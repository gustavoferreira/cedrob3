
// parser_B.c
// Cedro Socket BQT (B:) parser -> book top-N features + EMA + signal
//
// Modes:
//  - File: --file <YYYYMMDD_B.txt> --out <csv>
//  - Live: --live --input-dir <dir> --out-dir <dir>
//
// Notes:
//  - Reconstructs book by order-position with simple shifting (insert/delete/move).
//  - Tracks up to --book-cap positions per side (default 2000). Deeper positions are ignored.
//  - Aggregates per bar (--bar-sec, default 1) using write_ts (YYYYMMDD_HHMMSS) if present,
//    otherwise tries to use date from filename.
//
// Output: one line per (symbol, bar) with best bid/ask, spread, mid, microprice,
// depth sums, imbalance, OFI (top-of-book order flow imbalance), EMAs and signal.
//
// Build: gcc -O2 -std=c11 parser_B.c -o parser_B -lm
//
#define _GNU_SOURCE
#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define MAX_SYMS 64

static void die(const char *msg) { perror(msg); exit(1); }

static bool file_exists(const char *path) { return access(path, F_OK) == 0; }

static long long file_size(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    if (fseeko(f, 0, SEEK_END) != 0) { fclose(f); return -1; }
    off_t sz = ftello(f);
    fclose(f);
    return (long long)sz;
}

static double ema_alpha(int period) {
    if (period <= 1) return 1.0;
    return 2.0 / (period + 1.0);
}
static double ema_update(double prev, double x, double alpha, bool *inited) {
    if (!(*inited)) { *inited = true; return x; }
    return alpha * x + (1.0 - alpha) * prev;
}

// Parse "YYYYMMDD_HHMMSS" into ymd[9] and seconds since midnight.
static bool parse_write_ts(const char *s, char out_ymd[9], int *out_sec) {
    // expects 8 digits '_' 6 digits
    if (!s) return false;
    if (strlen(s) < 15) return false;
    for (int i=0;i<8;i++) if (!isdigit((unsigned char)s[i])) return false;
    if (s[8] != '_') return false;
    for (int i=9;i<15;i++) if (!isdigit((unsigned char)s[i])) return false;

    memcpy(out_ymd, s, 8); out_ymd[8]='\0';
    int hh = (s[9]-'0')*10 + (s[10]-'0');
    int mm = (s[11]-'0')*10 + (s[12]-'0');
    int ss = (s[13]-'0')*10 + (s[14]-'0');
    if (hh<0||hh>23||mm<0||mm>59||ss<0||ss>59) return false;
    *out_sec = hh*3600 + mm*60 + ss;
    return true;
}

static void sec_to_hhmmss(int sec, char out[9]) {
    if (sec < 0) sec = 0;
    int hh = sec/3600; sec%=3600;
    int mm = sec/60;   sec%=60;
    int ss = sec;
    snprintf(out, 9, "%02d%02d%02d", hh, mm, ss);
}

// Extract YYYYMMDD from any path by finding 8 consecutive digits.
static bool extract_ymd_from_path(const char *path, char out_ymd[9]) {
    if (!path) return false;
    const char *p = path;
    while (*p) {
        bool ok=true;
        for (int i=0;i<8;i++) { if (!isdigit((unsigned char)p[i])) { ok=false; break; } }
        if (ok) { memcpy(out_ymd,p,8); out_ymd[8]='\0'; return true; }
        p++;
    }
    return false;
}

// Split by ',' into up to max parts. Returns number of parts.
static int split_csv(char *s, char *out[], int max) {
    int n=0;
    char *p=s;
    out[n++]=p;
    while (*p && n<max) {
        if (*p==',') { *p='\0'; out[n++]=p+1; }
        p++;
    }
    return n;
}

// Split by ':' into up to max parts; last part gets rest.
static int split_colon(char *s, char *out[], int max) {
    int n=0;
    char *p=s;
    out[n++]=p;
    while (*p && n<max) {
        if (*p==':') { *p='\0'; out[n++]=p+1; }
        p++;
    }
    return n;
}

typedef struct {
    double price;
    double qty;
    int broker;
    long long order_id;
    char otype;   // order type (L/O/etc)
    char dh[9];   // DDMMHHMM
    bool valid;
} Order;

typedef struct {
    Order *arr;
    int cap;  // allocated/tracked capacity
    int len;  // current tracked length (0..cap)
} SideBook;

typedef struct {
    char symbol[32];
    SideBook bid; // direction 'A' = buy
    SideBook ask; // direction 'V' = sell

    // previous best for OFI calc
    bool prev_best_inited;
    double prev_bid_px, prev_bid_qty;
    double prev_ask_px, prev_ask_qty;

    // bar state
    bool bar_inited;
    int bar_start_sec; // aligned to bar_sec
    int events, adds, updates, d1, d2, d3, e_msgs;
    double ofi_sum; // accumulative OFI within bar

    // EMAs
    bool ema_fast_inited, ema_slow_inited, ema_imb_inited, ema_ofi_inited;
    double ema_fast, ema_slow, ema_imb, ema_ofi;
} SymState;

typedef struct {
    SymState syms[MAX_SYMS];
    int nsyms;
    int book_cap;
} SymBook;

static void side_init(SideBook *sb, int cap) {
    sb->cap=cap;
    sb->len=0;
    sb->arr=(Order*)calloc((size_t)cap, sizeof(Order));
    if (!sb->arr) die("calloc");
}
static void side_free(SideBook *sb) {
    free(sb->arr);
    sb->arr=NULL; sb->cap=0; sb->len=0;
}
static void side_clear(SideBook *sb) {
    sb->len=0;
    // don't memset entire array for performance; just mark valid false up to len previously?
    // len=0 is enough because we treat items >=len as absent.
}

static void side_insert(SideBook *sb, int pos, const Order *o) {
    if (pos < 0) return;
    if (pos > sb->len) pos = sb->len;
    if (pos >= sb->cap) return; // deeper than tracked range
    if (sb->len < sb->cap) {
        memmove(&sb->arr[pos+1], &sb->arr[pos], (size_t)(sb->len - pos) * sizeof(Order));
        sb->arr[pos] = *o;
        sb->arr[pos].valid = true;
        sb->len++;
    } else { // full, drop tail
        memmove(&sb->arr[pos+1], &sb->arr[pos], (size_t)(sb->cap - 1 - pos) * sizeof(Order));
        sb->arr[pos] = *o;
        sb->arr[pos].valid = true;
        sb->len = sb->cap;
    }
}

static void side_remove_at(SideBook *sb, int pos) {
    if (pos < 0 || pos >= sb->len) return;
    memmove(&sb->arr[pos], &sb->arr[pos+1], (size_t)(sb->len - pos - 1) * sizeof(Order));
    sb->len--;
    if (sb->len >= 0 && sb->len < sb->cap) {
        sb->arr[sb->len].valid = false;
        sb->arr[sb->len].price = 0; sb->arr[sb->len].qty = 0;
        sb->arr[sb->len].broker = 0; sb->arr[sb->len].order_id = 0;
        sb->arr[sb->len].otype = 0; sb->arr[sb->len].dh[0]='\0';
    }
}

static void side_remove_best_to(SideBook *sb, int pos_inclusive) {
    if (pos_inclusive < 0) return;
    int k = pos_inclusive + 1;
    if (k <= 0) return;
    if (k >= sb->len) { side_clear(sb); return; }
    memmove(&sb->arr[0], &sb->arr[k], (size_t)(sb->len - k) * sizeof(Order));
    int newlen = sb->len - k;
    // clear tail region (optional)
    for (int i=newlen;i<sb->len && i<sb->cap;i++) sb->arr[i].valid=false;
    sb->len = newlen;
}

static SymState* get_sym(SymBook *book, const char *sym) {
    for (int i=0;i<book->nsyms;i++) {
        if (strcmp(book->syms[i].symbol, sym)==0) return &book->syms[i];
    }
    if (book->nsyms >= MAX_SYMS) return NULL;
    SymState *st = &book->syms[book->nsyms++];
    memset(st, 0, sizeof(*st));
    snprintf(st->symbol, sizeof(st->symbol), "%s", sym);
    side_init(&st->bid, book->book_cap);
    side_init(&st->ask, book->book_cap);
    return st;
}

static void book_clear(SymState *st) {
    side_clear(&st->bid);
    side_clear(&st->ask);
    st->prev_best_inited = false;
    st->prev_bid_px = st->prev_bid_qty = st->prev_ask_px = st->prev_ask_qty = 0.0;
}

static bool has_best_bid(const SymState *st) { return st->bid.len > 0; }
static bool has_best_ask(const SymState *st) { return st->ask.len > 0; }

static double best_bid_px(const SymState *st) { return st->bid.arr[0].price; }
static double best_bid_qty(const SymState *st) { return st->bid.arr[0].qty; }
static double best_ask_px(const SymState *st) { return st->ask.arr[0].price; }
static double best_ask_qty(const SymState *st) { return st->ask.arr[0].qty; }

static double sum_qty_first(const SideBook *sb, int n) {
    if (n <= 0) return 0.0;
    int m = sb->len < n ? sb->len : n;
    double s = 0.0;
    for (int i=0;i<m;i++) s += sb->arr[i].qty;
    return s;
}

// Calculate top-of-book OFI increment given previous and current best quotes.
static double ofi_increment(double prev_bid_px, double prev_bid_qty,
                            double prev_ask_px, double prev_ask_qty,
                            double bid_px, double bid_qty,
                            double ask_px, double ask_qty) {
    double ofi = 0.0;

    // bid component
    if (bid_px > prev_bid_px) ofi += bid_qty;
    else if (bid_px == prev_bid_px) ofi += (bid_qty - prev_bid_qty);
    else ofi -= prev_bid_qty;

    // ask component
    if (ask_px < prev_ask_px) ofi -= ask_qty;
    else if (ask_px == prev_ask_px) ofi -= (ask_qty - prev_ask_qty);
    else ofi += prev_ask_qty;

    return ofi;
}

static void bar_reset(SymState *st, int bar_start_sec) {
    st->bar_inited = true;
    st->bar_start_sec = bar_start_sec;
    st->events = st->adds = st->updates = st->d1 = st->d2 = st->d3 = st->e_msgs = 0;
    st->ofi_sum = 0.0;
}

static void ensure_header(FILE *out) {
    long pos = ftell(out);
    if (pos == 0) {
        fprintf(out,
            "bar_ts,symbol,bar_sec,events,adds,updates,cancel1,cancel2,cancel3,e_msgs,"
            "best_bid_px,best_bid_qty,best_ask_px,best_ask_qty,spread,mid,microprice,"
            "bid_qty_L,ask_qty_L,imbalance_L,ofi,"
            "ema_fast,ema_slow,ema_imb,ema_ofi,ema_diff,signal,tracked_bid_len,tracked_ask_len\n"
        );
        fflush(out);
    }
}

static const char* signal_rule(double ema_fast, double ema_slow,
                               double ema_imb, double ema_ofi,
                               double imb_th, double ofi_th,
                               int min_events, int events) {
    if (events < min_events) return "FLAT";
    if (ema_fast > ema_slow && ema_imb > imb_th && ema_ofi > ofi_th) return "BUY";
    if (ema_fast < ema_slow && ema_imb < -imb_th && ema_ofi < -ofi_th) return "SELL";
    return "FLAT";
}

static void emit_bar(FILE *out, const char ymd[9], int bar_sec,
                     SymState *st,
                     int levels_L,
                     int ema_fast_p, int ema_slow_p, int ema_imb_p, int ema_ofi_p,
                     double imb_th, double ofi_th, int min_events) {
    if (!st->bar_inited) return;

    // Compute snapshot features from current book state
    bool bb = has_best_bid(st);
    bool ba = has_best_ask(st);

    double bb_px = bb ? best_bid_px(st) : NAN;
    double bb_q  = bb ? best_bid_qty(st) : NAN;
    double ba_px = ba ? best_ask_px(st) : NAN;
    double ba_q  = ba ? best_ask_qty(st) : NAN;

    double spread = (bb && ba) ? (ba_px - bb_px) : NAN;
    double mid = (bb && ba) ? (0.5 * (bb_px + ba_px)) : NAN;

    double micro = NAN;
    if (bb && ba) {
        double denom = bb_q + ba_q;
        if (denom > 0) micro = (bb_px * ba_q + ba_px * bb_q) / denom;
        else micro = mid;
    }

    double bidL = sum_qty_first(&st->bid, levels_L);
    double askL = sum_qty_first(&st->ask, levels_L);
    double imb = 0.0;
    double denom = bidL + askL;
    if (denom > 0.0) imb = (bidL - askL) / denom;

    // EMAs
    double a_fast = ema_alpha(ema_fast_p);
    double a_slow = ema_alpha(ema_slow_p);
    double a_imb  = ema_alpha(ema_imb_p);
    double a_ofi  = ema_alpha(ema_ofi_p);

    // price reference: micro if valid else mid
    double px_ref = (!isnan(micro) ? micro : mid);
    if (!isnan(px_ref)) {
        st->ema_fast = ema_update(st->ema_fast, px_ref, a_fast, &st->ema_fast_inited);
        st->ema_slow = ema_update(st->ema_slow, px_ref, a_slow, &st->ema_slow_inited);
    }
    st->ema_imb = ema_update(st->ema_imb, imb, a_imb, &st->ema_imb_inited);
    st->ema_ofi = ema_update(st->ema_ofi, st->ofi_sum, a_ofi, &st->ema_ofi_inited);

    double ema_diff = st->ema_fast - st->ema_slow;
    const char *sig = signal_rule(st->ema_fast, st->ema_slow, st->ema_imb, st->ema_ofi,
                                  imb_th, ofi_th, min_events, st->events);

    char hhmmss[9];
    sec_to_hhmmss(st->bar_start_sec, hhmmss);

    char bar_ts[32];
    snprintf(bar_ts, sizeof(bar_ts), "%s_%s", ymd, hhmmss);

    fprintf(out,
        "%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,"
        "%.10g,%.10g,%.10g,%.10g,%.10g,%.10g,%.10g,"
        "%.10g,%.10g,%.10g,%.10g,"
        "%.10g,%.10g,%.10g,%.10g,%.10g,%s,%d,%d\n",
        bar_ts, st->symbol, bar_sec,
        st->events, st->adds, st->updates, st->d1, st->d2, st->d3, st->e_msgs,
        bb_px, bb_q, ba_px, ba_q, spread, mid, micro,
        bidL, askL, imb, st->ofi_sum,
        st->ema_fast, st->ema_slow, st->ema_imb, st->ema_ofi, ema_diff, sig,
        st->bid.len, st->ask.len
    );
    fflush(out);
}

// Update OFI accumulator based on best quote changes after each event.
static void update_ofi_after_event(SymState *st) {
    if (!(has_best_bid(st) && has_best_ask(st))) {
        // can't compute OFI without both sides; still update prev if possible
        if (has_best_bid(st) && has_best_ask(st)) {
            // unreachable
        }
        st->prev_best_inited = false;
        return;
    }

    double bid_px = best_bid_px(st);
    double bid_qty = best_bid_qty(st);
    double ask_px = best_ask_px(st);
    double ask_qty = best_ask_qty(st);

    if (!st->prev_best_inited) {
        st->prev_best_inited = true;
        st->prev_bid_px = bid_px; st->prev_bid_qty = bid_qty;
        st->prev_ask_px = ask_px; st->prev_ask_qty = ask_qty;
        return;
    }

    double inc = ofi_increment(st->prev_bid_px, st->prev_bid_qty,
                              st->prev_ask_px, st->prev_ask_qty,
                              bid_px, bid_qty, ask_px, ask_qty);
    st->ofi_sum += inc;

    st->prev_bid_px = bid_px; st->prev_bid_qty = bid_qty;
    st->prev_ask_px = ask_px; st->prev_ask_qty = ask_qty;
}

// Parse & process one line. Returns true if processed any B message.
static bool process_line(SymBook *book, const char *line_in,
                         const char fallback_ymd[9],
                         int bar_sec, int levels_L,
                         FILE *out,
                         int ema_fast_p, int ema_slow_p, int ema_imb_p, int ema_ofi_p,
                         double imb_th, double ofi_th, int min_events) {
    const char *pb = strstr(line_in, "B:");
    if (!pb) return false;

    // Copy line (we'll mutate)
    char linebuf[4096];
    size_t n = strlen(line_in);
    if (n >= sizeof(linebuf)) n = sizeof(linebuf)-1;
    memcpy(linebuf, line_in, n);
    linebuf[n] = '\0';

    // trim end
    for (int i=(int)strlen(linebuf)-1;i>=0;i--) {
        if (linebuf[i]=='\n' || linebuf[i]=='\r') linebuf[i]='\0';
        else break;
    }

    // Find payload inside mutated copy
    char *pb_mut = strstr(linebuf, "B:");
    if (!pb_mut) return false;

    // Parse optional prefix: write_ts,buf_len,flag,...
    char ymd[9] = {0};
    int sec = -1;
    int bar_start = -1;

    // Try parse csv prefix by splitting linebuf by ',' first 4 fields
    // Example: 20251222_090054,4284,0,B:WING26:...
    char *csv_parts[5] = {0};
    int csv_n = split_csv(linebuf, csv_parts, 5);
    // After split_csv, pb_mut may be broken if it was after commas; so rebuild pb_mut:
    // payload is last part if prefix exists, else original.
    char *payload = NULL;

    if (csv_n >= 4 && strstr(csv_parts[3], "B:") == csv_parts[3]) {
        payload = csv_parts[3];
        if (!parse_write_ts(csv_parts[0], ymd, &sec)) {
            // fallback
            snprintf(ymd, sizeof(ymd), "%s", fallback_ymd);
        }
    } else {
        // no prefix; use fallback date; and we can't infer sec (so we skip bars)
        payload = pb_mut;
        snprintf(ymd, sizeof(ymd), "%s", fallback_ymd);
    }

    if (sec >= 0) {
        int bar_len = bar_sec;
        bar_start = (sec / bar_len) * bar_len;
    }

    // Now parse payload tokens
    char paybuf[2048];
    size_t pn = strlen(payload);
    if (pn >= sizeof(paybuf)) pn = sizeof(paybuf)-1;
    memcpy(paybuf, payload, pn);
    paybuf[pn] = '\0';

    char *parts[16] = {0};
    int np = split_colon(paybuf, parts, 16);
    if (np < 3) return false;
    if (strcmp(parts[0], "B") != 0) return false;

    const char *sym = parts[1];
    const char *op = parts[2];
    if (!sym || !*sym || !op || !*op) return false;

    SymState *st = get_sym(book, sym);
    if (!st) return false;

    // Bar handling: if we have time and bar moved forward, emit previous bar
    if (sec >= 0) {
        if (!st->bar_inited) bar_reset(st, bar_start);
        else if (bar_start > st->bar_start_sec) {
            emit_bar(out, ymd, bar_sec, st, levels_L,
                     ema_fast_p, ema_slow_p, ema_imb_p, ema_ofi_p,
                     imb_th, ofi_th, min_events);
            bar_reset(st, bar_start);
        } else if (bar_start < st->bar_start_sec) {
            // late line; ignore bar emission, but still apply book update
        }
    }

    st->events++;

    // Handle operations
    if (op[0] == 'E') {
        st->e_msgs++;
        // No state change
        update_ofi_after_event(st);
        return true;
    }

    if (op[0] == 'D') {
        // D:3 or D:<type>:<dir>:<pos>
        if (np >= 4 && strcmp(parts[3], "3") == 0) {
            st->d3++;
            book_clear(st);
            update_ofi_after_event(st);
            return true;
        }
        if (np < 6) return false;
        int ctype = atoi(parts[3]);
        char dir = parts[4][0];
        int pos = atoi(parts[5]);

        if (ctype == 1) st->d1++;
        else if (ctype == 2) st->d2++;
        else st->d1++; // fallback

        SideBook *sb = (dir == 'A') ? &st->bid : &st->ask;
        if (ctype == 1) {
            side_remove_at(sb, pos);
        } else if (ctype == 2) {
            side_remove_best_to(sb, pos);
        } else if (ctype == 3) {
            book_clear(st);
        } else {
            side_remove_at(sb, pos);
        }

        update_ofi_after_event(st);
        return true;
    }

    if (op[0] == 'A') {
        // A:<pos>:<dir>:<price>:<qty>:<broker>:<dh>:<orderid>:<otype>
        if (np < 11) return false;
        int pos = atoi(parts[3]);
        char dir = parts[4][0];

        char *endp=NULL;
        double price = strtod(parts[5], &endp);
        if (!endp || endp == parts[5]) return false;

        endp=NULL;
        double qty = strtod(parts[6], &endp);
        if (!endp || endp == parts[6]) return false;

        int broker = atoi(parts[7]);
        const char *dh = parts[8];
        long long oid = atoll(parts[9]);
        char otype = parts[10][0];

        Order o;
        memset(&o, 0, sizeof(o));
        o.price = price;
        o.qty = qty;
        o.broker = broker;
        o.order_id = oid;
        o.otype = otype;
        o.valid = true;
        if (dh && strlen(dh) >= 8) { memcpy(o.dh, dh, 8); o.dh[8]='\0'; }
        else o.dh[0]='\0';

        SideBook *sb = (dir == 'A') ? &st->bid : &st->ask;
        side_insert(sb, pos, &o);

        st->adds++;
        update_ofi_after_event(st);
        return true;
    }

    if (op[0] == 'U') {
        // U:<pos_new>:<pos_old>:<dir>:<price>:<qty>:<broker>:<dh>:<orderid>:<otype>
        if (np < 12) return false;
        int pos_new = atoi(parts[3]);
        int pos_old = atoi(parts[4]);
        char dir = parts[5][0];

        char *endp=NULL;
        double price = strtod(parts[6], &endp);
        if (!endp || endp == parts[6]) return false;

        endp=NULL;
        double qty = strtod(parts[7], &endp);
        if (!endp || endp == parts[7]) return false;

        int broker = atoi(parts[8]);
        const char *dh = parts[9];
        long long oid = atoll(parts[10]);
        char otype = parts[11][0];

        Order o;
        memset(&o, 0, sizeof(o));
        o.price = price;
        o.qty = qty;
        o.broker = broker;
        o.order_id = oid;
        o.otype = otype;
        o.valid = true;
        if (dh && strlen(dh) >= 8) { memcpy(o.dh, dh, 8); o.dh[8]='\0'; }
        else o.dh[0]='\0';

        SideBook *sb = (dir == 'A') ? &st->bid : &st->ask;

        if (pos_new == pos_old) {
            // simple in-place update if within tracked range
            if (pos_old >= 0 && pos_old < sb->len && pos_old < sb->cap) {
                sb->arr[pos_old] = o;
                sb->arr[pos_old].valid = true;
            } else {
                // treat as insert if we don't have it
                side_insert(sb, pos_new, &o);
            }
        } else {
            // remove old then insert new; adjust new if it was after old
            if (pos_old >= 0 && pos_old < sb->len) {
                side_remove_at(sb, pos_old);
                if (pos_new > pos_old) pos_new -= 1;
            }
            side_insert(sb, pos_new, &o);
        }

        st->updates++;
        update_ofi_after_event(st);
        return true;
    }

    // Unknown op; ignore
    update_ofi_after_event(st);
    return false;
}

typedef struct {
    bool live;
    char file[PATH_MAX];
    char out[PATH_MAX];
    char input_dir[PATH_MAX];
    char out_dir[PATH_MAX];

    int bar_sec;
    int levels_L;
    int book_cap;

    int ema_fast_p, ema_slow_p, ema_imb_p, ema_ofi_p;
    double imb_th;
    double ofi_th;
    int min_events;

    int poll_ms;
} Args;

static bool streq(const char *a, const char *b) { return strcmp(a,b)==0; }

static void usage(const char *argv0) {
    fprintf(stderr,
        "Uso:\n"
        "  %s --file <YYYYMMDD_B.txt> --out <saida.csv> [opcoes]\n"
        "  %s --live --input-dir <dir> --out-dir <dir> [opcoes]\n\n"
        "Opcoes:\n"
        "  --bar-sec N           (default 1)\n"
        "  --levels N            (somatorio qty nos primeiros N niveis por lado; default 20)\n"
        "  --book-cap N          (posicoes rastreadas por lado; default 2000)\n"
        "  --ema-fast N          (default 9)\n"
        "  --ema-slow N          (default 21)\n"
        "  --ema-imb N           (default 21)\n"
        "  --ema-ofi N           (default 21)\n"
        "  --imb-th X            (default 0.10)\n"
        "  --ofi-th X            (default 10)\n"
        "  --min-events N        (default 20)\n"
        "  --poll-ms N           (default 200) apenas live\n",
        argv0, argv0
    );
}

static Args parse_args(int argc, char **argv) {
    Args a; memset(&a, 0, sizeof(a));
    a.bar_sec = 1;
    a.levels_L = 20;
    a.book_cap = 2000;
    a.ema_fast_p = 9;
    a.ema_slow_p = 21;
    a.ema_imb_p = 21;
    a.ema_ofi_p = 21;
    a.imb_th = 0.10;
    a.ofi_th = 10.0;
    a.min_events = 20;
    a.poll_ms = 200;

    for (int i=1;i<argc;i++) {
        if (streq(argv[i],"--live")) a.live = true;
        else if (streq(argv[i],"--file") && i+1<argc) snprintf(a.file,sizeof(a.file),"%s",argv[++i]);
        else if (streq(argv[i],"--out") && i+1<argc) snprintf(a.out,sizeof(a.out),"%s",argv[++i]);
        else if (streq(argv[i],"--input-dir") && i+1<argc) snprintf(a.input_dir,sizeof(a.input_dir),"%s",argv[++i]);
        else if (streq(argv[i],"--out-dir") && i+1<argc) snprintf(a.out_dir,sizeof(a.out_dir),"%s",argv[++i]);
        else if (streq(argv[i],"--bar-sec") && i+1<argc) a.bar_sec = atoi(argv[++i]);
        else if (streq(argv[i],"--levels") && i+1<argc) a.levels_L = atoi(argv[++i]);
        else if (streq(argv[i],"--book-cap") && i+1<argc) a.book_cap = atoi(argv[++i]);
        else if (streq(argv[i],"--ema-fast") && i+1<argc) a.ema_fast_p = atoi(argv[++i]);
        else if (streq(argv[i],"--ema-slow") && i+1<argc) a.ema_slow_p = atoi(argv[++i]);
        else if (streq(argv[i],"--ema-imb") && i+1<argc) a.ema_imb_p = atoi(argv[++i]);
        else if (streq(argv[i],"--ema-ofi") && i+1<argc) a.ema_ofi_p = atoi(argv[++i]);
        else if (streq(argv[i],"--imb-th") && i+1<argc) a.imb_th = atof(argv[++i]);
        else if (streq(argv[i],"--ofi-th") && i+1<argc) a.ofi_th = atof(argv[++i]);
        else if (streq(argv[i],"--min-events") && i+1<argc) a.min_events = atoi(argv[++i]);
        else if (streq(argv[i],"--poll-ms") && i+1<argc) a.poll_ms = atoi(argv[++i]);
        else {
            fprintf(stderr, "Argumento invalido: %s\n", argv[i]);
            usage(argv[0]);
            exit(2);
        }
    }
    if (a.bar_sec <= 0) a.bar_sec = 1;
    if (a.levels_L <= 0) a.levels_L = 20;
    if (a.book_cap < 50) a.book_cap = 50;
    if (a.poll_ms < 10) a.poll_ms = 10;
    return a;
}

static void today_ymd(char out_ymd[9]) {
    time_t t = time(NULL);
    struct tm tm;
    localtime_r(&t, &tm);
    strftime(out_ymd, 9, "%Y%m%d", &tm);
}

static void build_live_paths(const Args *a, const char ymd[9],
                             char out_infile[PATH_MAX], char out_outfile[PATH_MAX]) {
    // Try both ..._B.txt and ..._B
    int n1 = snprintf(out_infile, PATH_MAX, "%s/%s_B.txt", a->input_dir, ymd);
    if (n1 < 0 || n1 >= PATH_MAX) { fprintf(stderr,"ERRO: input path grande\n"); exit(2); }

    if (!file_exists(out_infile)) {
        int n1b = snprintf(out_infile, PATH_MAX, "%s/%s_B", a->input_dir, ymd);
        if (n1b < 0 || n1b >= PATH_MAX) { fprintf(stderr,"ERRO: input path grande\n"); exit(2); }
    }

    int n2 = snprintf(out_outfile, PATH_MAX, "%s/%s_b_%ds.csv", a->out_dir, ymd, a->bar_sec);
    if (n2 < 0 || n2 >= PATH_MAX) { fprintf(stderr,"ERRO: output path grande\n"); exit(2); }
}

static void free_book(SymBook *book) {
    for (int i=0;i<book->nsyms;i++) {
        side_free(&book->syms[i].bid);
        side_free(&book->syms[i].ask);
    }
    book->nsyms = 0;
}

static void run_file_mode(const Args *a) {
    char ymd[9] = {0};
    if (!extract_ymd_from_path(a->file, ymd)) {
        fprintf(stderr, "Nao consegui extrair YYYYMMDD do nome do arquivo.\n");
        exit(2);
    }

    FILE *in = fopen(a->file, "rb");
    if (!in) die("fopen input");

    FILE *out = fopen(a->out, "wb");
    if (!out) die("fopen out");
    ensure_header(out);

    SymBook book; memset(&book, 0, sizeof(book));
    book.book_cap = a->book_cap;

    char *line=NULL;
    size_t cap=0;
    while (getline(&line, &cap, in) != -1) {
        process_line(&book, line, ymd, a->bar_sec, a->levels_L, out,
                     a->ema_fast_p, a->ema_slow_p, a->ema_imb_p, a->ema_ofi_p,
                     a->imb_th, a->ofi_th, a->min_events);
    }
    free(line);

    // flush last bars
    for (int i=0;i<book.nsyms;i++) {
        emit_bar(out, ymd, a->bar_sec, &book.syms[i], a->levels_L,
                 a->ema_fast_p, a->ema_slow_p, a->ema_imb_p, a->ema_ofi_p,
                 a->imb_th, a->ofi_th, a->min_events);
    }

    free_book(&book);
    fclose(in);
    fclose(out);
}

static void run_live_mode(const Args *a) {
    SymBook book; memset(&book, 0, sizeof(book));
    book.book_cap = a->book_cap;

    char cur_ymd[9] = {0};
    today_ymd(cur_ymd);

    char infile[PATH_MAX], outfile[PATH_MAX];
    build_live_paths(a, cur_ymd, infile, outfile);

    FILE *in=NULL;
    FILE *out=NULL;
    long long last_sz=-1;

    char *line=NULL;
    size_t cap=0;

    for (;;) {
        // rotate day
        char now_ymd[9] = {0};
        today_ymd(now_ymd);
        if (strcmp(now_ymd, cur_ymd)!=0) {
            if (out) {
                for (int i=0;i<book.nsyms;i++) {
                    emit_bar(out, cur_ymd, a->bar_sec, &book.syms[i], a->levels_L,
                             a->ema_fast_p, a->ema_slow_p, a->ema_imb_p, a->ema_ofi_p,
                             a->imb_th, a->ofi_th, a->min_events);
                }
                fclose(out); out=NULL;
            }
            if (in) { fclose(in); in=NULL; }

            free_book(&book);
            memset(&book, 0, sizeof(book));
            book.book_cap = a->book_cap;

            snprintf(cur_ymd,sizeof(cur_ymd),"%s", now_ymd);
            build_live_paths(a, cur_ymd, infile, outfile);
            last_sz=-1;
        }

        if (!out) {
            out = fopen(outfile, "ab+");
            if (!out) die("fopen live out");
            fseeko(out, 0, SEEK_END);
            ensure_header(out);
        }

        if (!in) {
            if (!file_exists(infile)) { usleep(a->poll_ms * 1000); continue; }
            in = fopen(infile, "rb");
            if (!in) { usleep(a->poll_ms * 1000); continue; }
            // tail mode: start at end
            fseeko(in, 0, SEEK_END);
            last_sz = file_size(infile);
        }

        long long sz = file_size(infile);
        if (sz >= 0 && last_sz >= 0 && sz < last_sz) {
            // truncated/rotated
            fclose(in);
            in = fopen(infile, "rb");
            if (!in) { usleep(a->poll_ms * 1000); continue; }
            fseeko(in, 0, SEEK_END);
            last_sz = sz;
        } else {
            last_sz = sz;
        }

        int got_any = 0;
        while (getline(&line, &cap, in) != -1) {
            got_any = 1;
            process_line(&book, line, cur_ymd, a->bar_sec, a->levels_L, out,
                         a->ema_fast_p, a->ema_slow_p, a->ema_imb_p, a->ema_ofi_p,
                         a->imb_th, a->ofi_th, a->min_events);
        }

        if (!got_any) {
            clearerr(in);
            usleep(a->poll_ms * 1000);
        }
    }

    free(line);
    free_book(&book);
}

int main(int argc, char **argv) {
    Args a = parse_args(argc, argv);

    if (!a.live) {
        if (a.file[0]=='\0' || a.out[0]=='\0') { usage(argv[0]); return 2; }
        run_file_mode(&a);
        return 0;
    }

    if (a.input_dir[0]=='\0' || a.out_dir[0]=='\0') { usage(argv[0]); return 2; }
    run_live_mode(&a);
    return 0;
}


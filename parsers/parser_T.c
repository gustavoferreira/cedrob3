// parser_T.c - C port of parser_T.py (Cedro log "T:" -> 1s bars per symbol)
// Build: gcc -O3 -march=native -std=c11 -Wall -Wextra -D_POSIX_C_SOURCE=200809L parser_T.c -o parser_T -lm

//gcc -O3 -march=native -std=c11 -Wall -Wextra -D_POSIX_C_SOURCE=200809L   parser_T_fixed.c -o parser_T -lm
////
///home/grao/cedrob3/parsers/parser_T   --input-template /home/grao/dados/cedro_files/{ymd}_T.txt   --output-template /home/grao/dados/sab/{ymd}_T_1s.csv   --symbols WING26,WDOF26   --session 09:00:00,18:30:00   --follow --rotate-daily --sleep-sec 0.25
////./parser_T   --input /home/grao/dados/cedro_files/20251222_T.txt   --output /home/grao/dados/sab/20251222_T_1s.csv   --symbols WING26,WDOF26   --session 09:00:00,18:30:00

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <math.h>
#include <limits.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>

#ifndef NAN
#define NAN (0.0/0.0)
#endif

#define STRLEN_MIN(a,b) ((a)<(b)?(a):(b))


static int parse_ndigits(const char *p, int n, int *out){
    if(!p || n<=0 || !out) return 0;
    int v = 0;
    for(int i=0;i<n;i++){
        unsigned char c = (unsigned char)p[i];
        if(!isdigit(c)) return 0;
        v = v*10 + (int)(c - '0');
    }
    *out = v;
    return 1;
}

static int is_missing_ll(long long v) { return v == LLONG_MIN; }
static int is_missing_d(double v) { return isnan(v); }

static void msleep_double(double sec){
    if(sec <= 0) return;
    // use nanosleep (POSIX) to avoid deprecated usleep warnings
    double s = sec;
    if(s < 0.0005) s = 0.0005;
    long sec_part = (long)s;
    long nsec_part = (long)((s - (double)sec_part) * 1000000000.0);
    if(nsec_part < 0) nsec_part = 0;
    if(nsec_part > 999999999) nsec_part = 999999999;

    struct timespec ts;
    ts.tv_sec = sec_part;
    ts.tv_nsec = nsec_part;
    // retry on EINTR
    while(nanosleep(&ts, &ts) == -1 && errno == EINTR) { /* continue */ }
}

static void iso_ms_from_time(time_t sec, int ms, char *out, size_t out_sz){
    struct tm tmv;
    localtime_r(&sec, &tmv);
    if(ms < 0) ms = 0;
    if(ms > 999) ms = 999;
    snprintf(out, out_sz, "%04d-%02d-%02dT%02d:%02d:%02d.%03d",
             tmv.tm_year+1900, tmv.tm_mon+1, tmv.tm_mday,
             tmv.tm_hour, tmv.tm_min, tmv.tm_sec, ms);
}

static int parse_write_ts_to_time(const char *s, time_t *out_sec){
    // accepts YYYYMMDD_HHMMSS or YYYYMMDD_HHMMSS.mmm
    if(!s || !out_sec) return 0;
    const char *t = s;
    while(*t && isspace((unsigned char)*t)) t++;

    // minimal length: 8 + '_' + 6 = 15
    if(strlen(t) < 15) return 0;
    if(t[8] != '_') return 0;

    int y=0,mn=0,d=0,hh=0,mm=0,ss=0;
    if(!parse_ndigits(t+0, 4, &y)) return 0;
    if(!parse_ndigits(t+4, 2, &mn)) return 0;
    if(!parse_ndigits(t+6, 2, &d)) return 0;
    if(!parse_ndigits(t+9, 2, &hh)) return 0;
    if(!parse_ndigits(t+11, 2, &mm)) return 0;
    if(!parse_ndigits(t+13, 2, &ss)) return 0;

    if(y < 1970 || y > 2100) return 0;
    if(mn < 1 || mn > 12) return 0;
    if(d < 1 || d > 31) return 0;
    if(hh < 0 || hh > 23 || mm < 0 || mm > 59 || ss < 0 || ss > 59) return 0;

    struct tm tmv;
    memset(&tmv, 0, sizeof(tmv));
    tmv.tm_year = y - 1900;
    tmv.tm_mon  = mn - 1;
    tmv.tm_mday = d;
    tmv.tm_hour = hh;
    tmv.tm_min  = mm;
    tmv.tm_sec  = ss;
    tmv.tm_isdst = -1;

    time_t tt = mktime(&tmv);
    if(tt == (time_t)-1) return 0;
    *out_sec = tt;
    return 1;
}


static int parse_hms_to_int(const char *s, int *out){
    if(!s) return 0;
    while(*s && isspace((unsigned char)*s)) s++;
    if(*s == '\0') return 0;

    if(strchr(s, ':')){
        int hh=0, mm=0, ss=0;
        // accept HH:MM:SS or HH:MM
        char *tmp = strdup(s);
        if(!tmp) return 0;
        char *save=NULL;
        char *p = strtok_r(tmp, ":", &save);
        if(p) hh = atoi(p);
        p = strtok_r(NULL, ":", &save);
        if(p) mm = atoi(p);
        p = strtok_r(NULL, ":", &save);
        if(p) ss = atoi(p);
        free(tmp);
        *out = hh*10000 + mm*100 + ss;
        return 1;
    }

    // digits
    for(const char *p=s; *p; ++p) if(!isdigit((unsigned char)*p)) return 0;
    size_t n = strlen(s);
    if(n==6){ *out = atoi(s); return 1; }
    if(n==4){ *out = atoi(s) * 100; return 1; }
    if(n<=2){ *out = atoi(s) * 10000; return 1; }
    return 0;
}

static int tick_dir_value(const char *s){
    if(!s) return 0;
    while(*s && isspace((unsigned char)*s)) s++;
    if(*s == '\0') return 0;
    if(strchr(s, '+')) return 1;
    if(strchr(s, '-')) return -1;
    return 0;
}

static int sgn(double x){
    if(isnan(x)) return 0;
    if(x > 0) return 1;
    if(x < 0) return -1;
    return 0;
}

static int compute_signal(double score, int last_signal, double enter_th, double keep_th){
    int s = sgn(score);
    if(s == 0) return 0;
    double th = enter_th;
    if(last_signal != 0 && s == last_signal) th = keep_th;
    if(fabs(score) >= th) return s;
    return 0;
}

typedef struct {
    double last, bid, ask;
    long long bid_qty1, ask_qty1;
    long long trade_qty_cur, trade_qty_last;

    long long status;
    char phase[32];
    char tick_dir[16];
    double variation;

    long long cum_trades, cum_vol;
    double cum_fin;

    double prev_last_for_tick;

    int last_signal;
    double last_score;
} SymbolState;

typedef struct {
    double last, bid, ask;
    long long bid_qty1, ask_qty1;
    long long trade_qty_cur, trade_qty_last;

    long long status;
    char phase[32];

    char tick_dir_last[16];
    int tick_dir_sum;
    int tick_dir_n;

    double variation;

    long long cum_trades, cum_vol;
    double cum_fin;

    char last_event_142[16];
    char last_trade_143[16];

    int n_events;
} Bucket;

static void init_state(SymbolState *st){
    st->last = NAN; st->bid = NAN; st->ask = NAN;
    st->bid_qty1 = LLONG_MIN; st->ask_qty1 = LLONG_MIN;
    st->trade_qty_cur = LLONG_MIN; st->trade_qty_last = LLONG_MIN;
    st->status = LLONG_MIN;
    st->phase[0] = '\0';
    st->tick_dir[0] = '\0';
    st->variation = NAN;
    st->cum_trades = LLONG_MIN; st->cum_vol = LLONG_MIN;
    st->cum_fin = NAN;
    st->prev_last_for_tick = NAN;
    st->last_signal = 0;
    st->last_score = 0.0;
}

static void init_bucket(Bucket *b){
    b->last = NAN; b->bid = NAN; b->ask = NAN;
    b->bid_qty1 = LLONG_MIN; b->ask_qty1 = LLONG_MIN;
    b->trade_qty_cur = LLONG_MIN; b->trade_qty_last = LLONG_MIN;
    b->status = LLONG_MIN;
    b->phase[0] = '\0';
    b->tick_dir_last[0] = '\0';
    b->tick_dir_sum = 0;
    b->tick_dir_n = 0;
    b->variation = NAN;
    b->cum_trades = LLONG_MIN; b->cum_vol = LLONG_MIN;
    b->cum_fin = NAN;
    b->last_event_142[0] = '\0';
    b->last_trade_143[0] = '\0';
    b->n_events = 0;
}

static double parse_double(const char *s, int *ok){
    if(ok) *ok = 0;
    if(!s) return NAN;
    while(*s && isspace((unsigned char)*s)) s++;
    if(*s=='\0') return NAN;
    char *end=NULL;
    errno = 0;
    double v = strtod(s, &end);
    if(end == s || errno != 0) return NAN;
    if(ok) *ok = 1;
    return v;
}

static long long parse_ll_from_any(const char *s, int *ok){
    // int(float(s)) style
    if(ok) *ok = 0;
    if(!s) return LLONG_MIN;
    while(*s && isspace((unsigned char)*s)) s++;
    if(*s=='\0') return LLONG_MIN;
    char *end=NULL;
    errno = 0;
    double v = strtod(s, &end);
    if(end == s || errno != 0) return LLONG_MIN;
    if(ok) *ok = 1;
    if(v > (double)LLONG_MAX) return LLONG_MAX;
    if(v < (double)LLONG_MIN) return LLONG_MIN;
    return (long long)(v);
}

static void csv_print_escaped(FILE *f, const char *s){
    if(!s) return;
    int need_quote = 0;
    for(const char *p=s; *p; ++p){
        if(*p==',' || *p=='"' || *p=='\n' || *p=='\r') { need_quote=1; break; }
    }
    if(!need_quote){
        fputs(s, f);
        return;
    }
    fputc('"', f);
    for(const char *p=s; *p; ++p){
        if(*p=='"') fputc('"', f);
        fputc(*p, f);
    }
    fputc('"', f);
}

static void csv_field_sep(FILE *f, int *first){
    if(*first) *first = 0; else fputc(',', f);
}

static void csv_put_str(FILE *f, int *first, const char *s){
    csv_field_sep(f, first);
    if(s && s[0]) csv_print_escaped(f, s);
}

static void csv_put_ll(FILE *f, int *first, long long v){
    csv_field_sep(f, first);
    if(!is_missing_ll(v)) fprintf(f, "%lld", v);
}

static void csv_put_int(FILE *f, int *first, int v){
    csv_field_sep(f, first);
    fprintf(f, "%d", v);
}

static void csv_put_double(FILE *f, int *first, double v){
    csv_field_sep(f, first);
    if(!is_missing_d(v)){
        // same spirit as Python: just output a reasonable decimal
        fprintf(f, "%.10g", v);
    }
}

static void compute_mid_spread(double bid, double ask, double *mid, double *spread){
    if(isnan(bid) || isnan(ask)) { *mid = NAN; *spread = NAN; return; }
    *mid = (bid + ask) / 2.0;
    *spread = (ask - bid);
}

static double safe_imb(long long bq, long long aq){
    if(is_missing_ll(bq) || is_missing_ll(aq)) return NAN;
    long long den = bq + aq;
    if(den <= 0) return NAN;
    return (double)(bq - aq) / (double)den;
}

static double safe_microprice(double bid, double ask, long long bq, long long aq){
    if(isnan(bid) || isnan(ask) || is_missing_ll(bq) || is_missing_ll(aq)) return NAN;
    long long den = bq + aq;
    if(den <= 0) return NAN;
    return (bid * (double)aq + ask * (double)bq) / (double)den;
}

static int hhmmssmmm_to_time(const struct tm *day, const char *hhmmssmmm, time_t *out_sec, int *out_ms){
    // hhmmssmmm: 9 digits
    if(!day || !hhmmssmmm) return 0;
    size_t n = strlen(hhmmssmmm);
    if(n != 9) return 0;
    for(size_t i=0;i<n;i++) if(!isdigit((unsigned char)hhmmssmmm[i])) return 0;

    int hh = (hhmmssmmm[0]-'0')*10 + (hhmmssmmm[1]-'0');
    int mm = (hhmmssmmm[2]-'0')*10 + (hhmmssmmm[3]-'0');
    int ss = (hhmmssmmm[4]-'0')*10 + (hhmmssmmm[5]-'0');
    int ms = (hhmmssmmm[6]-'0')*100 + (hhmmssmmm[7]-'0')*10 + (hhmmssmmm[8]-'0');

    if(hh<0||hh>23||mm<0||mm>59||ss<0||ss>59||ms<0||ms>999) return 0;

    struct tm tmv = *day;
    tmv.tm_hour = hh;
    tmv.tm_min  = mm;
    tmv.tm_sec  = ss;
    tmv.tm_isdst = -1;
    time_t sec = mktime(&tmv);
    if(sec == (time_t)-1) return 0;

    *out_sec = sec;
    *out_ms = ms;
    return 1;
}

// ---------------------- CLI options ----------------------

typedef struct {
    char input[1024];
    char output[1024];
    char input_template[1024];
    char output_template[1024];

    char symbols[512];
    char session[64];

    int follow;
    int rotate_daily;
    double sleep_sec;

    double max_spread;
    int require_trade;
    long long min_vol;

    double imb_th;
    double micro_dev_th;
    int tickdir_th;
    double enter_th;
    double keep_th;
    int bar_sec;
} Options;

static void opts_init(Options *o){
    memset(o, 0, sizeof(*o));
    o->symbols[0] = 0;
    strncpy(o->symbols, "WIN,WDO", sizeof(o->symbols)-1);
    o->sleep_sec = 0.25;
    o->bar_sec = 1;

    o->max_spread = 0.0;
    o->require_trade = 0;
    o->min_vol = 0;

    o->imb_th = 0.15;
    o->micro_dev_th = 0.0;
    o->tickdir_th = 2;
    o->enter_th = 2.0;
    o->keep_th = 1.0;
}

static void usage(const char *prog){
    fprintf(stderr,
        "Uso:\n"
        "  %s --input <YYYYMMDD_T.txt> --output <out.csv> [opções]\n"
        "  %s --input-template <.../{ymd}_T.txt> --output-template <.../{ymd}_T_1s.csv> --follow --rotate-daily [opções]\n\n"
        "Opções principais:\n"
        "  --symbols WING26,WDOF26\n"
        "  --session 09:00:00,18:30:00\n"
        "  --bar-sec 1 (segundos por barra)\n"
        "  --follow (tail -f)\n"
        "  --sleep-sec 0.25\n"
        "  --rotate-daily (reabre input/output templates ao virar o dia)\n\n"
        "Filtros/sinal (iguais ao Python):\n"
        "  --max-spread 0\n"
        "  --require-trade\n"
        "  --min-vol 0\n"
        "  --imb-th 0.15\n"
        "  --micro-dev-th 0\n"
        "  --tickdir-th 2\n"
        "  --enter-th 2.0\n"
        "  --keep-th 1.0\n",
        prog, prog);
}

static int streq(const char *a, const char *b){ return strcmp(a,b)==0; }

static int parse_args(int argc, char **argv, Options *o){
    for(int i=1;i<argc;i++){
        const char *a = argv[i];
        if(streq(a,"--input") && i+1<argc){ strncpy(o->input, argv[++i], sizeof(o->input)-1); }
        else if((streq(a,"--output")||streq(a,"--out")) && i+1<argc){ strncpy(o->output, argv[++i], sizeof(o->output)-1); }
        else if(streq(a,"--input-template") && i+1<argc){ strncpy(o->input_template, argv[++i], sizeof(o->input_template)-1); }
        else if(streq(a,"--output-template") && i+1<argc){ strncpy(o->output_template, argv[++i], sizeof(o->output_template)-1); }
        else if(streq(a,"--symbols") && i+1<argc){ strncpy(o->symbols, argv[++i], sizeof(o->symbols)-1); }
        else if(streq(a,"--session") && i+1<argc){ strncpy(o->session, argv[++i], sizeof(o->session)-1); }
        else if(streq(a,"--bar-sec") && i+1<argc){ o->bar_sec = atoi(argv[++i]); if(o->bar_sec < 1) o->bar_sec=1; }
        else if(streq(a,"--follow")){ o->follow = 1; }
        else if(streq(a,"--rotate-daily")){ o->rotate_daily = 1; }
        else if(streq(a,"--sleep-sec") && i+1<argc){ o->sleep_sec = atof(argv[++i]); }

        else if(streq(a,"--max-spread") && i+1<argc){ o->max_spread = atof(argv[++i]); }
        else if(streq(a,"--require-trade")){ o->require_trade = 1; }
        else if(streq(a,"--min-vol") && i+1<argc){ o->min_vol = parse_ll_from_any(argv[++i], NULL); }

        else if(streq(a,"--imb-th") && i+1<argc){ o->imb_th = atof(argv[++i]); }
        else if(streq(a,"--micro-dev-th") && i+1<argc){ o->micro_dev_th = atof(argv[++i]); }
        else if(streq(a,"--tickdir-th") && i+1<argc){ o->tickdir_th = atoi(argv[++i]); }
        else if(streq(a,"--enter-th") && i+1<argc){ o->enter_th = atof(argv[++i]); }
        else if(streq(a,"--keep-th") && i+1<argc){ o->keep_th = atof(argv[++i]); }

        else if(streq(a,"--help") || streq(a,"-h")){
            usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Parâmetro desconhecido: %s\n", a);
            usage(argv[0]);
            return 0;
        }
    }
    return 1;
}

// ---------------------- template helpers ----------------------

static void ymd_from_now(char *out, size_t out_sz){
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    snprintf(out, out_sz, "%04d%02d%02d", tmv.tm_year+1900, tmv.tm_mon+1, tmv.tm_mday);
}

static void apply_template(const char *templ, const char *ymd, char *out, size_t out_sz){
    // replace first occurrence of {ymd}
    const char *p = strstr(templ, "{ymd}");
    if(!p){
        strncpy(out, templ, out_sz-1);
        out[out_sz-1] = '\0';
        return;
    }
    size_t pre = (size_t)(p - templ);
    size_t post = strlen(p + 5);
    size_t need = pre + strlen(ymd) + post;
    if(need >= out_sz) need = out_sz-1;

    size_t npre = STRLEN_MIN(pre, out_sz-1);
    memcpy(out, templ, npre);
    out[npre] = '\0';

    strncat(out, ymd, out_sz-1 - strlen(out));
    strncat(out, p+5, out_sz-1 - strlen(out));
}

// ---------------------- symbol list ----------------------

typedef struct {
    char name[32];
    SymbolState st;
    Bucket b;
} SymSlot;

static int parse_symbols(const char *csv, SymSlot **out_slots){
    char *tmp = strdup(csv ? csv : "");
    if(!tmp) return 0;

    int count = 0;
    for(char *p=tmp; *p; ++p) if(*p==',') count++;
    count = count + 1;
    if(count <= 0) { free(tmp); return 0; }

    SymSlot *slots = (SymSlot*)calloc((size_t)count, sizeof(SymSlot));
    if(!slots){ free(tmp); return 0; }

    int n=0;
    char *save=NULL;
    char *tok = strtok_r(tmp, ",", &save);
    while(tok && n<count){
        while(*tok && isspace((unsigned char)*tok)) tok++;
        size_t len = strlen(tok);
        while(len>0 && isspace((unsigned char)tok[len-1])) tok[--len] = '\0';
        if(len>0){
            strncpy(slots[n].name, tok, sizeof(slots[n].name)-1);
            init_state(&slots[n].st);
            init_bucket(&slots[n].b);
            n++;
        }
        tok = strtok_r(NULL, ",", &save);
    }

    free(tmp);
    *out_slots = slots;
    return n;
}

static int find_symbol(SymSlot *slots, int n, const char *sym){
    for(int i=0;i<n;i++){
        // Check if slots[i].name is a prefix of sym
        size_t len = strlen(slots[i].name);
        if(strncmp(slots[i].name, sym, len)==0) return i;
    }
    return -1;
}

// ---------------------- output header ----------------------

static void write_header(FILE *out){
    const char *hdr =
        "read_ts,write_ts,symbol,"
        "event_ts_142,trade_ts_143,"
        "delay_ms,delay_src,"
        "last,best_bid,best_ask,spread,mid,"
        "bid_qty1,ask_qty1,imb1,microprice,microprice_dev,"
        "trade_qty_cur,trade_qty_last,"
        "cum_trades,cum_vol,cum_fin,"
        "d_trades_1s,d_vol_1s,d_fin_1s,d_fin_est_1s,"
        "tick_dir,variation,"
        "tick_dir_agg,tick_dir_sum,tick_dir_n,tick_dir_th,"
        "trade_sign_lr,trade_sign_tick,signed_vol_1s,"
        "t_signal_num,t_signal,had_trade_1s,"
        "status,phase,"
        "had_update_1s,carry_forward_1s,n_events_1s,reset_day\n";
    fputs(hdr, out);
}

// ---------------------- flush logic ----------------------

static int in_session_time(const struct tm *tmv, int sess_start, int sess_end, int sess_enabled){
    if(!sess_enabled) return 1;
    int hhmmss = tmv->tm_hour*10000 + tmv->tm_min*100 + tmv->tm_sec;
    return (sess_start <= hhmmss && hhmmss <= sess_end);
}

static void flush_second(time_t dt_sec, SymSlot *slots, int nslots, FILE *out,
                         int sess_start, int sess_end, int sess_enabled,
                         const Options *opt){

    struct tm tmv;
    localtime_r(&dt_sec, &tmv);

    if(!in_session_time(&tmv, sess_start, sess_end, sess_enabled)){
        for(int i=0;i<nslots;i++) init_bucket(&slots[i].b);
        return;
    }

    char write_ts[32];
    snprintf(write_ts, sizeof(write_ts), "%04d%02d%02d_%02d%02d%02d",
             tmv.tm_year+1900, tmv.tm_mon+1, tmv.tm_mday,
             tmv.tm_hour, tmv.tm_min, tmv.tm_sec);

    struct timeval tv;
    gettimeofday(&tv, NULL);
    time_t read_sec = tv.tv_sec;
    int read_ms = (int)(tv.tv_usec/1000);
    char read_ts[64];
    iso_ms_from_time(read_sec, read_ms, read_ts, sizeof(read_ts));

    for(int i=0;i<nslots;i++){
        Bucket *b = &slots[i].b;
        SymbolState *st = &slots[i].st;

        int had_update = (b->n_events > 0) ? 1 : 0;

        if(had_update){
            if(!isnan(b->last)) st->last = b->last;
            if(!isnan(b->bid))  st->bid  = b->bid;
            if(!isnan(b->ask))  st->ask  = b->ask;
            if(!is_missing_ll(b->bid_qty1)) st->bid_qty1 = b->bid_qty1;
            if(!is_missing_ll(b->ask_qty1)) st->ask_qty1 = b->ask_qty1;
            if(!is_missing_ll(b->trade_qty_cur)) st->trade_qty_cur = b->trade_qty_cur;
            if(!is_missing_ll(b->trade_qty_last)) st->trade_qty_last = b->trade_qty_last;
            if(!is_missing_ll(b->status)) st->status = b->status;
            if(b->phase[0]) strncpy(st->phase, b->phase, sizeof(st->phase)-1);
            if(b->tick_dir_last[0]) strncpy(st->tick_dir, b->tick_dir_last, sizeof(st->tick_dir)-1);
            if(!isnan(b->variation)) st->variation = b->variation;
        }

        int has_any_state = (!isnan(st->last) || !isnan(st->bid) || !isnan(st->ask));
        int carry_forward = (had_update==0 && has_any_state) ? 1 : 0;

        double mid=NAN, spread=NAN;
        compute_mid_spread(st->bid, st->ask, &mid, &spread);
        double imb1 = safe_imb(st->bid_qty1, st->ask_qty1);
        double microprice = safe_microprice(st->bid, st->ask, st->bid_qty1, st->ask_qty1);
        double microprice_dev = (!isnan(microprice) && !isnan(mid)) ? (microprice - mid) : NAN;

        int reset_day = 0;
        long long d_trades = 0;
        long long d_vol = 0;
        double d_fin = 0.0;

        if(had_update){
            if(!is_missing_ll(b->cum_trades)){
                long long prev = st->cum_trades;
                long long cur = b->cum_trades;
                if(is_missing_ll(prev)) d_trades = 0;
                else if(cur < prev){ reset_day = 1; d_trades = 0; }
                else d_trades = cur - prev;
                st->cum_trades = cur;
            }
            if(!is_missing_ll(b->cum_vol)){
                long long prev = st->cum_vol;
                long long cur = b->cum_vol;
                if(is_missing_ll(prev)) d_vol = 0;
                else if(cur < prev){ reset_day = 1; d_vol = 0; }
                else d_vol = cur - prev;
                st->cum_vol = cur;
            }
            if(!isnan(b->cum_fin)){
                double prev = st->cum_fin;
                double cur = b->cum_fin;
                if(isnan(prev)) d_fin = 0.0;
                else if(cur < prev){ reset_day = 1; d_fin = 0.0; }
                else d_fin = cur - prev;
                st->cum_fin = cur;
            }
        }

        int had_trade = (d_trades > 0 || d_vol > 0 || b->last_trade_143[0]) ? 1 : 0;

        int s_lr = 0;
        if(!isnan(st->last) && !isnan(mid)){
            if(st->last > mid) s_lr = 1;
            else if(st->last < mid) s_lr = -1;
        }

        int s_tick = 0;
        if(!isnan(st->last) && !isnan(st->prev_last_for_tick)){
            if(st->last > st->prev_last_for_tick) s_tick = 1;
            else if(st->last < st->prev_last_for_tick) s_tick = -1;
        }

        long long signed_vol = (long long)s_lr * (long long)d_vol;

        int tick_dir_sum = b->tick_dir_sum;
        int tick_dir_n = b->tick_dir_n;
        int tick_dir_agg = 0;
        if(abs(tick_dir_sum) >= opt->tickdir_th) tick_dir_agg = (tick_dir_sum > 0) ? 1 : -1;

        double score = 0.0;
        if(!isnan(imb1) && fabs(imb1) >= opt->imb_th) score += 1.0 * (imb1 > 0 ? 1 : -1);
        if(!isnan(microprice_dev)){
            if(opt->micro_dev_th <= 0.0 || fabs(microprice_dev) >= opt->micro_dev_th) score += 1.0 * (microprice_dev > 0 ? 1 : -1);
        }
        if(tick_dir_agg != 0) score += 0.8 * (double)tick_dir_agg;
        if(s_tick != 0) score += 0.6 * (double)s_tick;
        if(s_lr != 0) score += 0.6 * (double)s_lr;
        if(had_trade && d_vol != 0) score += 0.5 * (signed_vol > 0 ? 1 : -1);

        int allow_signal = 1;
        if(opt->max_spread > 0.0 && !isnan(spread)){
            if(spread > opt->max_spread) allow_signal = 0;
        }
        if(opt->require_trade && !had_trade) allow_signal = 0;
        if(opt->min_vol > 0 && d_vol < opt->min_vol) allow_signal = 0;
        if(isnan(mid) || isnan(st->last)) allow_signal = 0;

        int t_signal_num = 0;
        if(allow_signal) t_signal_num = compute_signal(score, st->last_signal, opt->enter_th, opt->keep_th);

        const char *t_signal = "HOLD";
        if(t_signal_num > 0) t_signal = "BUY";
        else if(t_signal_num < 0) t_signal = "SELL";

        if(t_signal_num != 0) st->last_signal = t_signal_num;
        else {
            if(fabs(score) < 0.2) st->last_signal = 0;
        }
        st->last_score = score;

        if(!isnan(st->last)) st->prev_last_for_tick = st->last;

        // event/trade ts and delay
        char event_ts_142[64] = {0};
        char trade_ts_143[64] = {0};
        time_t src_sec = dt_sec;
        int src_ms = 0;
        const char *delay_src = "write_ts";

        if(b->last_event_142[0]){
            time_t esec; int ems;
            if(hhmmssmmm_to_time(&tmv, b->last_event_142, &esec, &ems)){
                iso_ms_from_time(esec, ems, event_ts_142, sizeof(event_ts_142));
                src_sec = esec; src_ms = ems; delay_src = "142";
            }
        }
        if(strcmp(delay_src, "write_ts") == 0 && b->last_trade_143[0]){
            time_t tsec; int tms;
            if(hhmmssmmm_to_time(&tmv, b->last_trade_143, &tsec, &tms)){
                iso_ms_from_time(tsec, tms, trade_ts_143, sizeof(trade_ts_143));
                src_sec = tsec; src_ms = tms; delay_src = "143";
            }
        }

        long long read_total_ms = (long long)read_sec * 1000LL + (long long)read_ms;
        long long src_total_ms  = (long long)src_sec  * 1000LL + (long long)src_ms;
        long long delay_ms = read_total_ms - src_total_ms;

        double d_fin_est = NAN;
        if(d_fin != 0.0) d_fin_est = d_fin;
        else if(!isnan(st->last)) d_fin_est = (double)d_vol * st->last;

        // Write row
        int first = 1;
        csv_put_str(out, &first, read_ts);
        csv_put_str(out, &first, write_ts);
        csv_put_str(out, &first, slots[i].name);
        csv_put_str(out, &first, event_ts_142[0] ? event_ts_142 : "");
        csv_put_str(out, &first, trade_ts_143[0] ? trade_ts_143 : "");
        csv_put_ll(out, &first, delay_ms);
        csv_put_str(out, &first, delay_src);

        csv_put_double(out, &first, st->last);
        csv_put_double(out, &first, st->bid);
        csv_put_double(out, &first, st->ask);
        csv_put_double(out, &first, spread);
        csv_put_double(out, &first, mid);

        csv_put_ll(out, &first, st->bid_qty1);
        csv_put_ll(out, &first, st->ask_qty1);
        csv_put_double(out, &first, imb1);
        csv_put_double(out, &first, microprice);
        csv_put_double(out, &first, microprice_dev);

        csv_put_ll(out, &first, st->trade_qty_cur);
        csv_put_ll(out, &first, st->trade_qty_last);

        csv_put_ll(out, &first, st->cum_trades);
        csv_put_ll(out, &first, st->cum_vol);
        csv_put_double(out, &first, st->cum_fin);

        csv_put_ll(out, &first, d_trades);
        csv_put_ll(out, &first, d_vol);
        csv_put_double(out, &first, d_fin);
        csv_put_double(out, &first, d_fin_est);

        csv_put_str(out, &first, st->tick_dir);
        csv_put_double(out, &first, st->variation);

        csv_put_int(out, &first, tick_dir_agg);
        csv_put_int(out, &first, tick_dir_sum);
        csv_put_int(out, &first, tick_dir_n);
        csv_put_int(out, &first, opt->tickdir_th);

        csv_put_int(out, &first, s_lr);
        csv_put_int(out, &first, s_tick);
        csv_put_ll(out, &first, signed_vol);

        csv_put_int(out, &first, t_signal_num);
        csv_put_str(out, &first, t_signal);
        csv_put_int(out, &first, had_trade);

        csv_put_ll(out, &first, st->status);
        csv_put_str(out, &first, st->phase);

        csv_put_int(out, &first, had_update);
        csv_put_int(out, &first, carry_forward);
        csv_put_int(out, &first, b->n_events);
        csv_put_int(out, &first, reset_day);
        fputc('\n', out);

        init_bucket(b);
    }

    fflush(out);
}

// ---------------------- line parsing ----------------------

static int split_first3_commas(const char *line, char *write_ts, size_t write_ts_sz, const char **out_msg){
    // Mimics: parts = sline.lstrip().split(',', 3)
    // Returns 1 if ok and sets write_ts and msg ptr.
    if(!line) return 0;
    const char *p = line;
    while(*p && isspace((unsigned char)*p)) p++;
    const char *c1 = strchr(p, ',');
    if(!c1) return 0;
    size_t n = (size_t)(c1 - p);
    if(n >= write_ts_sz) n = write_ts_sz - 1;
    memcpy(write_ts, p, n);
    write_ts[n] = '\0';

    const char *c2 = strchr(c1+1, ',');
    if(!c2) return 0;
    const char *c3 = strchr(c2+1, ',');
    if(!c3) return 0;

    const char *msg = c3 + 1;
    while(*msg && isspace((unsigned char)*msg)) msg++;
    *out_msg = msg;
    return 1;
}

static int parse_T_message_and_update(const char *msg_in, SymSlot *slots, int nslots,
                                     long long *bad_lines, long long *parsed_lines, long long *ignored_symbols){
    (void)bad_lines; // mantido por compatibilidade com o contador do Python
    if(!msg_in) return 0;

    // trim leading spaces
    while(*msg_in && isspace((unsigned char)*msg_in)) msg_in++;

    if(strncmp(msg_in, "T:", 2) != 0) return 0;

    const char *bang = strchr(msg_in, '!');
    if(!bang) return 0;

    size_t mlen = (size_t)(bang - msg_in);
    if(mlen < 4) return 0;

    char *buf = (char*)malloc(mlen + 1);
    if(!buf) return 0;
    memcpy(buf, msg_in, mlen);
    buf[mlen] = '\0';

    // tokenize
    char *save=NULL;
    char *tok = strtok_r(buf, ":", &save);
    if(!tok || strcmp(tok, "T") != 0){ free(buf); return 0; }

    char *sym = strtok_r(NULL, ":", &save);
    if(!sym){ free(buf); return 0; }

    // skip parts[2]
    char *skip = strtok_r(NULL, ":", &save);
    if(!skip){ free(buf); return 0; }

    int idx_sym = find_symbol(slots, nslots, sym);
    (*parsed_lines)++;
    if(idx_sym < 0){
        (*ignored_symbols)++;
        free(buf);
        return 1;
    }

    Bucket *b = &slots[idx_sym].b;
    b->n_events += 1;

    // Now pairs: idx:value ... starting at parts[3]
    while(1){
        char *idx_s = strtok_r(NULL, ":", &save);
        if(!idx_s) break;
        char *val_s = strtok_r(NULL, ":", &save);
        if(!val_s) break;

        // idx must be digits
        int all_digits = 1;
        for(char *p=idx_s; *p; ++p){ if(!isdigit((unsigned char)*p)){ all_digits=0; break; } }
        if(!all_digits) continue;
        int idx = atoi(idx_s);

        int ok=0;
        switch(idx){
            case 2: {
                double v = parse_double(val_s, &ok);
                if(ok) b->last = v;
            } break;
            case 3: {
                double v = parse_double(val_s, &ok);
                if(ok) b->bid = v;
            } break;
            case 4: {
                double v = parse_double(val_s, &ok);
                if(ok) b->ask = v;
            } break;
            case 19: {
                long long v = parse_ll_from_any(val_s, &ok);
                if(ok) b->bid_qty1 = v;
            } break;
            case 20: {
                long long v = parse_ll_from_any(val_s, &ok);
                if(ok) b->ask_qty1 = v;
            } break;
            case 6: {
                long long v = parse_ll_from_any(val_s, &ok);
                if(ok) b->trade_qty_cur = v;
            } break;
            case 7: {
                long long v = parse_ll_from_any(val_s, &ok);
                if(ok) b->trade_qty_last = v;
            } break;
            case 8: {
                long long v = parse_ll_from_any(val_s, &ok);
                if(ok) b->cum_trades = v;
            } break;
            case 9: {
                long long v = parse_ll_from_any(val_s, &ok);
                if(ok) b->cum_vol = v;
            } break;
            case 10: {
                double v = parse_double(val_s, &ok);
                if(ok) b->cum_fin = v;
            } break;
            case 21: {
                double v = parse_double(val_s, &ok);
                if(ok) b->variation = v;
            } break;
            case 67: {
                long long v = parse_ll_from_any(val_s, &ok);
                if(ok) b->status = v;
            } break;
            case 88: {
                // phase
                while(*val_s && isspace((unsigned char)*val_s)) val_s++;
                strncpy(b->phase, val_s, sizeof(b->phase)-1);
                b->phase[sizeof(b->phase)-1] = '\0';
            } break;
            case 106: {
                while(*val_s && isspace((unsigned char)*val_s)) val_s++;
                strncpy(b->tick_dir_last, val_s, sizeof(b->tick_dir_last)-1);
                b->tick_dir_last[sizeof(b->tick_dir_last)-1] = '\0';
                int vdir = tick_dir_value(val_s);
                b->tick_dir_sum += vdir;
                b->tick_dir_n += 1;
            } break;
            case 142: {
                while(*val_s && isspace((unsigned char)*val_s)) val_s++;
                strncpy(b->last_event_142, val_s, sizeof(b->last_event_142)-1);
                b->last_event_142[sizeof(b->last_event_142)-1] = '\0';
            } break;
            case 143: {
                while(*val_s && isspace((unsigned char)*val_s)) val_s++;
                strncpy(b->last_trade_143, val_s, sizeof(b->last_trade_143)-1);
                b->last_trade_143[sizeof(b->last_trade_143)-1] = '\0';
            } break;
            default:
                break;
        }
    }

    free(buf);
    return 1;
}

// ---------------------- main loop ----------------------

static FILE* open_input_wait(const char *path, int follow, double sleep_sec){
    while(1){
        FILE *f = fopen(path, "r");
        if(f) return f;
        if(!follow) return NULL;
        msleep_double(sleep_sec);
    }
}

static FILE* open_output_new(const char *path){
    FILE *f = fopen(path, "w");
    if(!f) return NULL;
    setvbuf(f, NULL, _IOLBF, 0); // line-buffered
    write_header(f);
    return f;
}

int main(int argc, char **argv){
    Options opt; opts_init(&opt);
    if(!parse_args(argc, argv, &opt)) return 2;

    int use_templates = (opt.input_template[0] && opt.output_template[0]);

    if(!use_templates){
        if(!opt.input[0] || !opt.output[0]){
            fprintf(stderr, "ERRO: informe --input e --output (ou use --input-template/--output-template)\n");
            usage(argv[0]);
            return 2;
        }
    } else {
        if(!opt.follow){
            fprintf(stderr, "Aviso: usando templates sem --follow normalmente não faz sentido. Continuando mesmo assim.\n");
        }
    }

    SymSlot *slots=NULL;
    int nslots = parse_symbols(opt.symbols, &slots);
    if(nslots <= 0){
        fprintf(stderr, "ERRO: lista de symbols inválida\n");
        return 2;
    }

    int sess_enabled = 0;
    int sess_start = 0, sess_end = 0;
    if(opt.session[0]){
        char *tmp = strdup(opt.session);
        if(tmp){
            char *comma = strchr(tmp, ',');
            if(comma){
                *comma = '\0';
                int a=0,b=0;
                if(parse_hms_to_int(tmp, &a) && parse_hms_to_int(comma+1, &b)){
                    sess_start = a; sess_end = b; sess_enabled = 1;
                }
            }
            free(tmp);
        }
    }

    long long bad_lines = 0;
    long long parsed_lines = 0;
    long long ignored_symbols = 0;
    long long out_of_order = 0;

    char current_ymd[16] = {0};
    char in_path[1024] = {0};
    char out_path[1024] = {0};

    if(use_templates){
        ymd_from_now(current_ymd, sizeof(current_ymd));
        apply_template(opt.input_template, current_ymd, in_path, sizeof(in_path));
        apply_template(opt.output_template, current_ymd, out_path, sizeof(out_path));
    } else {
        strncpy(in_path, opt.input, sizeof(in_path)-1);
        strncpy(out_path, opt.output, sizeof(out_path)-1);
        // derive ymd from input name if possible, else leave blank
        if(strlen(in_path) >= 8){
            int ok=1;
            for(int i=0;i<8;i++) if(!isdigit((unsigned char)in_path[i])) ok=0;
            if(ok){
                strncpy(current_ymd, in_path, 8);
                current_ymd[8] = '\0';
            }
        }
    }

    FILE *fout = open_output_new(out_path);
    if(!fout){
        fprintf(stderr, "ERRO: não consegui abrir output: %s\n", out_path);
        return 1;
    }

    FILE *fin = open_input_wait(in_path, opt.follow, opt.sleep_sec);
    if(!fin){
        fprintf(stderr, "ERRO: input não existe: %s\n", in_path);
        fclose(fout);
        return 1;
    }

    // main processing state
    int have_current_dt = 0;
    time_t current_dt = 0;

    char line[65536];

    while(1){
        if(use_templates && opt.rotate_daily){
            char ymd_now[16];
            ymd_from_now(ymd_now, sizeof(ymd_now));
            if(strcmp(ymd_now, current_ymd) != 0){
                // switch day
                if(have_current_dt){
                    flush_second(current_dt, slots, nslots, fout, sess_start, sess_end, sess_enabled, &opt);
                    have_current_dt = 0;
                }
                fclose(fin);
                fclose(fout);

                strncpy(current_ymd, ymd_now, sizeof(current_ymd)-1);
                apply_template(opt.input_template, current_ymd, in_path, sizeof(in_path));
                apply_template(opt.output_template, current_ymd, out_path, sizeof(out_path));

                fout = open_output_new(out_path);
                if(!fout){
                    fprintf(stderr, "ERRO: não consegui abrir output: %s\n", out_path);
                    break;
                }
                fin = open_input_wait(in_path, opt.follow, opt.sleep_sec);
                if(!fin){
                    fprintf(stderr, "ERRO: input não existe: %s\n", in_path);
                    break;
                }
            }
        }

        if(!fgets(line, sizeof(line), fin)){
            if(!opt.follow) break;
            clearerr(fin);
            msleep_double(opt.sleep_sec);
            continue;
        }

        // strip newline
        size_t ln = strlen(line);
        while(ln>0 && (line[ln-1]=='\n' || line[ln-1]=='\r')) line[--ln]='\0';
        if(ln==0) continue;

        char write_ts_s[64];
        const char *msg=NULL;
        if(!split_first3_commas(line, write_ts_s, sizeof(write_ts_s), &msg)){
            bad_lines++;
            continue;
        }

        time_t dt_sec;
        if(!parse_write_ts_to_time(write_ts_s, &dt_sec)){
            bad_lines++;
            continue;
        }

        if(!have_current_dt){
            // align to bar start
            current_dt = (dt_sec / opt.bar_sec) * opt.bar_sec;
            have_current_dt = 1;
        } else {
            if(dt_sec < current_dt){
                out_of_order++;
                continue;
            }
        }

        while(have_current_dt && (current_dt + opt.bar_sec) <= dt_sec){
            flush_second(current_dt, slots, nslots, fout, sess_start, sess_end, sess_enabled, &opt);
            current_dt += opt.bar_sec;
        }

        // parse message
        int ok = parse_T_message_and_update(msg, slots, nslots, &bad_lines, &parsed_lines, &ignored_symbols);
        if(!ok){
            bad_lines++;
            continue;
        }
    }

    if(have_current_dt){
        flush_second(current_dt, slots, nslots, fout, sess_start, sess_end, sess_enabled, &opt);
    }

    fclose(fin);
    fclose(fout);

    fprintf(stdout, "OK\n");
    fprintf(stdout, "parsed_lines=%lld bad_lines=%lld ignored_symbols=%lld out_of_order=%lld\n",
            parsed_lines, bad_lines, ignored_symbols, out_of_order);
    fprintf(stdout, "out_csv=%s\n", out_path);

    free(slots);
    return 0;
}



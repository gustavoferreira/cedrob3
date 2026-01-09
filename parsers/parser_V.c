// v_gqt_parser.c
// //gcc -O2 -std=c11 parser_V.c -o parser_V -lm
// Parser/aggregator do Cedro GQT (V:) -> barras + EMA + sinal BUY/SELL/FLAT
// - Modo arquivo: --file <path> --out <csv>
// - Modo live:   --live --input-dir <dir> --out-dir <dir>
// Suporta prefixo opcional antes do payload (ex: "20251222_093004,1428,0,").
// Linhas truncadas/incompletas são ignoradas com segurança.

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

static void die(const char *msg) {
    perror(msg);
    exit(1);
}

static bool file_exists(const char *path) {
    return access(path, F_OK) == 0;
}

static long long file_size(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    if (fseeko(f, 0, SEEK_END) != 0) { fclose(f); return -1; }
    off_t sz = ftello(f);
    fclose(f);
    return (long long)sz;
}

static double ema_update(double prev, double x, double alpha, bool *inited) {
    if (!(*inited)) { *inited = true; return x; }
    return alpha * x + (1.0 - alpha) * prev;
}

static double ema_alpha(int period) {
    if (period <= 1) return 1.0;
    return 2.0 / (period + 1.0);
}

// HHMMSSmmm (ou HHMMSS) -> ms desde meia-noite. Retorna false se inválido.
static bool parse_hhmmssms_to_ms(const char *s, int *out_ms) {
    if (!s) return false;
    int len = (int)strlen(s);
    if (len < 6) return false;

    // pega HHMMSS
    int hh = 0, mm = 0, ss = 0, mmm = 0;
    for (int i = 0; i < 6; i++) if (!isdigit((unsigned char)s[i])) return false;

    hh = (s[0]-'0')*10 + (s[1]-'0');
    mm = (s[2]-'0')*10 + (s[3]-'0');
    ss = (s[4]-'0')*10 + (s[5]-'0');

    if (hh < 0 || hh > 23 || mm < 0 || mm > 59 || ss < 0 || ss > 59) return false;

    if (len >= 9) {
        for (int i = 6; i < 9; i++) if (!isdigit((unsigned char)s[i])) return false;
        mmm = (s[6]-'0')*100 + (s[7]-'0')*10 + (s[8]-'0');
    } else {
        mmm = 0;
    }

    *out_ms = ((hh*3600 + mm*60 + ss) * 1000) + mmm;
    return true;
}

// ms desde meia-noite -> HHMMSS
static void ms_to_hhmmss(int ms, char out[8]) {
    if (ms < 0) ms = 0;
    int sec = ms / 1000;
    int hh = sec / 3600; sec %= 3600;
    int mm = sec / 60;   sec %= 60;
    int ss = sec;
    // 6 chars + '\0' = 7; mas o fortify reclama, então damos folga
    snprintf(out, 8, "%02d%02d%02d", hh, mm, ss);
}
// tenta extrair YYYYMMDD do nome do arquivo (procura 8 dígitos seguidos).
static bool extract_ymd_from_path(const char *path, char out_ymd[9]) {
    if (!path) return false;
    const char *p = path;
    while (*p) {
        if (isdigit((unsigned char)p[0]) && isdigit((unsigned char)p[1]) &&
            isdigit((unsigned char)p[2]) && isdigit((unsigned char)p[3]) &&
            isdigit((unsigned char)p[4]) && isdigit((unsigned char)p[5]) &&
            isdigit((unsigned char)p[6]) && isdigit((unsigned char)p[7])) {
            memcpy(out_ymd, p, 8);
            out_ymd[8] = '\0';
            return true;
        }
        p++;
    }
    return false;
}

// Split por ':' em no máximo max_parts. A última parte pega o "resto".
static int split_colon_n(char *s, char *out[], int max_parts) {
    int n = 0;
    char *p = s;
    out[n++] = p;
    while (*p && n < max_parts) {
        if (*p == ':') {
            *p = '\0';
            out[n++] = p + 1;
        }
        p++;
    }
    return n;
}

typedef struct {
    char symbol[32];

    // bar state
    bool bar_inited;
    int bar_start_ms;       // ms do dia (alinhado em bar_sec)
    double o, h, l, c;      // OHLC
    double vwap_num;        // sum(price*qty)
    double vwap_den;        // sum(qty)
    double buy_vol;
    double sell_vol;
    double undef_vol;
    double trades;

    // EMAs
    bool ema_fast_inited;
    bool ema_slow_inited;
    bool ema_delta_inited;
    double ema_fast;
    double ema_slow;
    double ema_delta;

    // stats
    long long late_events;
    long long bad_lines;
} SymState;

typedef struct {
    SymState syms[MAX_SYMS];
    int nsyms;
} SymBook;

static SymState* get_sym(SymBook *book, const char *sym) {
    for (int i = 0; i < book->nsyms; i++) {
        if (strcmp(book->syms[i].symbol, sym) == 0) return &book->syms[i];
    }
    if (book->nsyms >= MAX_SYMS) return NULL;
    SymState *st = &book->syms[book->nsyms++];
    memset(st, 0, sizeof(*st));
    snprintf(st->symbol, sizeof(st->symbol), "%s", sym);
    return st;
}

static void reset_bar(SymState *st, int bar_start_ms, double first_price) {
    st->bar_inited = true;
    st->bar_start_ms = bar_start_ms;
    st->o = st->h = st->l = st->c = first_price;
    st->vwap_num = 0.0;
    st->vwap_den = 0.0;
    st->buy_vol = 0.0;
    st->sell_vol = 0.0;
    st->undef_vol = 0.0;
    st->trades = 0.0;
}

static void bar_update(SymState *st, double price, double qty, char aggressor) {
    if (price > st->h) st->h = price;
    if (price < st->l) st->l = price;
    st->c = price;

    st->vwap_num += price * qty;
    st->vwap_den += qty;
    st->trades += 1.0;

    if (aggressor == 'A') st->buy_vol += qty;
    else if (aggressor == 'V') st->sell_vol += qty;
    else st->undef_vol += qty;
}

static const char* signal_from_rules(double ema_fast, double ema_slow,
                                     double ema_delta, double imb,
                                     double delta_ema_th, double imb_th,
                                     int min_trades, int trades) {
    // Regras simples e tunáveis:
    // BUY: trend up (ema_fast>ema_slow) + fluxo comprador (ema_delta>th) + imbalance positivo
    // SELL: trend down + fluxo vendedor
    if (trades < min_trades) return "FLAT";
    if (ema_fast > ema_slow && ema_delta > delta_ema_th && imb > imb_th) return "BUY";
    if (ema_fast < ema_slow && ema_delta < -delta_ema_th && imb < -imb_th) return "SELL";
    return "FLAT";
}

static void ensure_header(FILE *out) {
    // se arquivo está vazio, imprime cabeçalho
    long pos = ftell(out);
    if (pos == 0) {
        fprintf(out,
            "bar_ts,symbol,bar_sec,trades,vol_total,buy_vol,sell_vol,undef_vol,delta,imbalance,"
            "open,high,low,close,vwap,ema_fast,ema_slow,ema_delta,ema_diff,signal\n"
        );
        fflush(out);
    }
}

static void emit_bar(FILE *out, const char ymd[9], int bar_sec,
                     SymState *st,
                     int ema_fast_p, int ema_slow_p, int ema_delta_p,
                     double delta_ema_th, double imb_th, int min_trades) {
    if (!st->bar_inited) return;
    if (st->vwap_den <= 0.0) return;

    double vwap = st->vwap_num / st->vwap_den;
    double vol_total = st->buy_vol + st->sell_vol + st->undef_vol;
    double delta = st->buy_vol - st->sell_vol;
    double denom = (st->buy_vol + st->sell_vol);
    double imb = (denom > 0.0) ? (delta / denom) : 0.0;

    // EMAs (em cima do VWAP e do delta)
    double a_fast = ema_alpha(ema_fast_p);
    double a_slow = ema_alpha(ema_slow_p);
    double a_del  = ema_alpha(ema_delta_p);

    st->ema_fast = ema_update(st->ema_fast, vwap, a_fast, &st->ema_fast_inited);
    st->ema_slow = ema_update(st->ema_slow, vwap, a_slow, &st->ema_slow_inited);
    st->ema_delta = ema_update(st->ema_delta, imb, a_del, &st->ema_delta_inited);

    double ema_diff = st->ema_fast - st->ema_slow;

    const char *sig = signal_from_rules(
        st->ema_fast, st->ema_slow, st->ema_delta, imb,
        delta_ema_th, imb_th, min_trades, (int)st->trades
    );

    char hhmmss[8];
    ms_to_hhmmss(st->bar_start_ms, hhmmss);

    char bar_ts[32];
    snprintf(bar_ts, sizeof(bar_ts), "%s_%s", ymd, hhmmss);

    fprintf(out,
        "%s,%s,%d,%d,%.0f,%.0f,%.0f,%.0f,%.0f,%.6f,%.10g,%.10g,%.10g,%.10g,%.10g,%.10g,%.10g,%.10g,%.10g,%s\n",
        bar_ts, st->symbol, bar_sec,
        (int)st->trades,
        vol_total, st->buy_vol, st->sell_vol, st->undef_vol,
        delta, imb,
        st->o, st->h, st->l, st->c,
        vwap,
        st->ema_fast, st->ema_slow, st->ema_delta, ema_diff,
        sig
    );
    fflush(out);
}

// Processa uma linha; retorna true se consumiu um trade A com sucesso.
static bool process_line(SymBook *book, const char *line_in,
                         const char ymd[9],
                         int bar_sec,
                         FILE *out,
                         int ema_fast_p, int ema_slow_p, int ema_delta_p,
                         double delta_ema_th, double imb_th, int min_trades) {
    const char *pv = strstr(line_in, "V:");
    if (!pv) return false;

    // copia payload (do V: até o fim)
    char buf[2048];
    size_t n = strlen(pv);
    if (n >= sizeof(buf)) n = sizeof(buf) - 1;
    memcpy(buf, pv, n);
    buf[n] = '\0';

    // trim \r\n
    for (int i = (int)strlen(buf) - 1; i >= 0; i--) {
        if (buf[i] == '\n' || buf[i] == '\r') buf[i] = '\0';
        else break;
    }

    // split por ':', a última parte pega resto (condição original pode ter espaços)
    char *parts[16] = {0};
    int np = split_colon_n(buf, parts, 16);
    if (np < 3) return false;

    // parts[0] deve ser "V"
    if (strcmp(parts[0], "V") != 0) return false;

    const char *sym = parts[1];
    const char *op  = parts[2];
    if (!sym || !*sym || !op || !*op) return false;

    SymState *st = get_sym(book, sym);
    if (!st) return false;

    // Remoção de negócio: V:<ativo>:D:<id>
    // Remoção de todos:   V:<ativo>:R
    if (op[0] == 'R') {
        // não temos book de trades aqui; apenas ignoramos e resetamos EMA/barras
        st->bar_inited = false;
        st->ema_fast_inited = st->ema_slow_inited = st->ema_delta_inited = false;
        return true;
    }
    if (op[0] == 'D') {
        // Sem bookkeeping por trade_id, só ignora.
        return true;
    }

    // Negócio (A): subscribe tem:
    // A:<horario>:<preco>:<broker_buy>:<broker_sell>:<qty>:<trade_id>:<cond>:<agressor>:<orig_cond>
    // snapshot pode ter request_id extra depois do trade_id.
    if (op[0] != 'A') return false;

    // precisa ter pelo menos: V sym A time price bb bs qty id cond aggressor orig
    // subscribe => np >= 12, snapshot => np >= 13
    bool is_snapshot = (np >= 13);

    const char *trade_time = parts[3];
    const char *price_s    = parts[4];
    const char *bb_s       = parts[5];
    const char *bs_s       = parts[6];
    const char *qty_s      = parts[7];
    const char *id_s       = parts[8];

    (void)bb_s; (void)bs_s; (void)id_s;

    int idx_cond = is_snapshot ? 10 : 9;
    int idx_aggr = is_snapshot ? 11 : 10;
    int idx_orig = is_snapshot ? 12 : 11;

    if (np <= idx_orig) { st->bad_lines++; return false; }

    const char *cond_s = parts[idx_cond];
    const char *aggr_s = parts[idx_aggr];
    const char *orig_s = parts[idx_orig];
    (void)cond_s; (void)orig_s;

    int t_ms = 0;
    if (!parse_hhmmssms_to_ms(trade_time, &t_ms)) { st->bad_lines++; return false; }

    char *endp = NULL;
    double price = strtod(price_s, &endp);
    if (!endp || endp == price_s) { st->bad_lines++; return false; }

    endp = NULL;
    double qty = strtod(qty_s, &endp);
    if (!endp || endp == qty_s) { st->bad_lines++; return false; }
    if (qty <= 0) { st->bad_lines++; return false; }

    char aggressor = (aggr_s && aggr_s[0]) ? aggr_s[0] : 'I';

    int bar_ms = bar_sec * 1000;
    int bar_start_ms = (t_ms / bar_ms) * bar_ms;

    if (!st->bar_inited) {
        reset_bar(st, bar_start_ms, price);
        bar_update(st, price, qty, aggressor);
        return true;
    }

    if (bar_start_ms == st->bar_start_ms) {
        bar_update(st, price, qty, aggressor);
        return true;
    }

    if (bar_start_ms > st->bar_start_ms) {
        // fecha bar atual e inicia novo
        emit_bar(out, ymd, bar_sec, st, ema_fast_p, ema_slow_p, ema_delta_p, delta_ema_th, imb_th, min_trades);
        reset_bar(st, bar_start_ms, price);
        bar_update(st, price, qty, aggressor);
        return true;
    }

    // evento atrasado (bar antigo)
    st->late_events++;
    return false;
}

typedef struct {
    bool live;
    char file[PATH_MAX];
    char out[PATH_MAX];
    char input_dir[PATH_MAX];
    char out_dir[PATH_MAX];

    int bar_sec;
    int ema_fast_p;
    int ema_slow_p;
    int ema_delta_p;

    double imb_th;
    double delta_ema_th;
    int min_trades;

    int poll_ms;
} Args;

static void usage(const char *argv0) {
    fprintf(stderr,
        "Uso:\n"
        "  %s --file <yyyymmdd_V.txt> --out <saida.csv> [opcoes]\n"
        "  %s --live --input-dir <dir> --out-dir <dir> [opcoes]\n\n"
        "Opcoes:\n"
        "  --bar-sec N           (default 1)\n"
        "  --ema-fast N          (default 9)\n"
        "  --ema-slow N          (default 21)\n"
        "  --ema-delta N         (default 21)\n"
        "  --imb-th X            (default 0.15)\n"
        "  --delta-ema-th X      (default 5)\n"
        "  --min-trades N        (default 3)\n"
        "  --poll-ms N           (default 200) apenas live\n",
        argv0, argv0
    );
}

static bool streq(const char *a, const char *b) { return strcmp(a,b)==0; }

static Args parse_args(int argc, char **argv) {
    Args a;
    memset(&a, 0, sizeof(a));
    a.bar_sec = 1;
    a.ema_fast_p = 9;
    a.ema_slow_p = 21;
    a.ema_delta_p = 21;
    a.imb_th = 0.15;
    a.delta_ema_th = 5.0;
    a.min_trades = 3;
    a.poll_ms = 200;

    for (int i = 1; i < argc; i++) {
        if (streq(argv[i], "--live")) a.live = true;
        else if (streq(argv[i], "--file") && i+1 < argc) snprintf(a.file, sizeof(a.file), "%s", argv[++i]);
        else if (streq(argv[i], "--out") && i+1 < argc)  snprintf(a.out, sizeof(a.out), "%s", argv[++i]);
        else if (streq(argv[i], "--input-dir") && i+1 < argc) snprintf(a.input_dir, sizeof(a.input_dir), "%s", argv[++i]);
        else if (streq(argv[i], "--out-dir") && i+1 < argc)   snprintf(a.out_dir, sizeof(a.out_dir), "%s", argv[++i]);
        else if (streq(argv[i], "--bar-sec") && i+1 < argc)   a.bar_sec = atoi(argv[++i]);
        else if (streq(argv[i], "--ema-fast") && i+1 < argc)  a.ema_fast_p = atoi(argv[++i]);
        else if (streq(argv[i], "--ema-slow") && i+1 < argc)  a.ema_slow_p = atoi(argv[++i]);
        else if (streq(argv[i], "--ema-delta") && i+1 < argc) a.ema_delta_p = atoi(argv[++i]);
        else if (streq(argv[i], "--imb-th") && i+1 < argc)    a.imb_th = atof(argv[++i]);
        else if (streq(argv[i], "--delta-ema-th") && i+1 < argc) a.delta_ema_th = atof(argv[++i]);
        else if (streq(argv[i], "--min-trades") && i+1 < argc) a.min_trades = atoi(argv[++i]);
        else if (streq(argv[i], "--poll-ms") && i+1 < argc)   a.poll_ms = atoi(argv[++i]);
        else {
            fprintf(stderr, "Argumento invalido: %s\n", argv[i]);
            usage(argv[0]);
            exit(2);
        }
    }

    if (a.bar_sec <= 0) a.bar_sec = 1;
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
    int n1 = snprintf(out_infile, PATH_MAX, "%s/%s_V.txt", a->input_dir, ymd);
    int n2 = snprintf(out_outfile, PATH_MAX, "%s/%s_v_%ds.csv", a->out_dir, ymd, a->bar_sec);

    if (n1 < 0 || n1 >= PATH_MAX) {
        fprintf(stderr, "ERRO: caminho input muito grande (PATH_MAX=%d)\n", PATH_MAX);
        exit(2);
    }
    if (n2 < 0 || n2 >= PATH_MAX) {
        fprintf(stderr, "ERRO: caminho output muito grande (PATH_MAX=%d)\n", PATH_MAX);
        exit(2);
    }
}


static void run_file_mode(const Args *a) {
    char ymd[9] = {0};
    if (!extract_ymd_from_path(a->file, ymd)) {
        fprintf(stderr, "Nao consegui extrair YYYYMMDD do nome do arquivo. Renomeie ou use um path com YYYYMMDD.\n");
        exit(2);
    }

    FILE *in = fopen(a->file, "rb");
    if (!in) die("fopen input");

    FILE *out = fopen(a->out, "wb");
    if (!out) die("fopen out");
    ensure_header(out);

    SymBook book; memset(&book, 0, sizeof(book));

    char *line = NULL;
    size_t cap = 0;
    while (getline(&line, &cap, in) != -1) {
        process_line(&book, line, ymd, a->bar_sec, out,
                     a->ema_fast_p, a->ema_slow_p, a->ema_delta_p,
                     a->delta_ema_th, a->imb_th, a->min_trades);
    }
    free(line);

    // flush final: fecha a última barra de cada símbolo
    for (int i = 0; i < book.nsyms; i++) {
        emit_bar(out, ymd, a->bar_sec, &book.syms[i],
                 a->ema_fast_p, a->ema_slow_p, a->ema_delta_p,
                 a->delta_ema_th, a->imb_th, a->min_trades);
    }

    fclose(in);
    fclose(out);
}

static void run_live_mode(const Args *a) {
    SymBook book; memset(&book, 0, sizeof(book));

    char cur_ymd[9] = {0};
    today_ymd(cur_ymd);

    char infile[PATH_MAX], outfile[PATH_MAX];
    build_live_paths(a, cur_ymd, infile, outfile);

    FILE *in = NULL;
    FILE *out = NULL;
    off_t last_off = 0;
    long long last_sz = -1;

    char *line = NULL;
    size_t cap = 0;

    for (;;) {
        // troca de dia?
        char now_ymd[9] = {0};
        today_ymd(now_ymd);
        if (strcmp(now_ymd, cur_ymd) != 0) {
            // flush e fecha
            if (out) {
                for (int i = 0; i < book.nsyms; i++) {
                    emit_bar(out, cur_ymd, a->bar_sec, &book.syms[i],
                             a->ema_fast_p, a->ema_slow_p, a->ema_delta_p,
                             a->delta_ema_th, a->imb_th, a->min_trades);
                }
                fclose(out);
                out = NULL;
            }
            if (in) { fclose(in); in = NULL; }

            memset(&book, 0, sizeof(book));
            snprintf(cur_ymd, sizeof(cur_ymd), "%s", now_ymd);
            build_live_paths(a, cur_ymd, infile, outfile);
            last_off = 0;
            last_sz = -1;
        }

        // garante output aberto
        if (!out) {
            out = fopen(outfile, "ab+");
            if (!out) die("fopen live out");
            fseeko(out, 0, SEEK_END);
            ensure_header(out);
        }

        // abre input quando existir
        if (!in) {
            if (!file_exists(infile)) {
                usleep(a->poll_ms * 1000);
                continue;
            }
            in = fopen(infile, "rb");
            if (!in) { usleep(a->poll_ms * 1000); continue; }
            // começa do final (tail) para live (pode mudar se quiser: troque SEEK_END por SEEK_SET)
            fseeko(in, 0, SEEK_END);
            last_off = ftello(in);
            last_sz = file_size(infile);
        }

        // detecta truncamento/rotacao do arquivo (tamanho diminuiu)
        long long sz = file_size(infile);
        if (sz >= 0 && last_sz >= 0 && sz < last_sz) {
            fclose(in);
            in = fopen(infile, "rb");
            if (!in) { usleep(a->poll_ms * 1000); continue; }
            fseeko(in, 0, SEEK_END);
            last_off = ftello(in);
            last_sz = sz;
        } else {
            last_sz = sz;
        }

        // tenta ler linhas novas
        int got_any = 0;
        while (getline(&line, &cap, in) != -1) {
            got_any = 1;
            process_line(&book, line, cur_ymd, a->bar_sec, out,
                         a->ema_fast_p, a->ema_slow_p, a->ema_delta_p,
                         a->delta_ema_th, a->imb_th, a->min_trades);
            last_off = ftello(in);
        }

        if (!got_any) {
            clearerr(in); // EOF
            usleep(a->poll_ms * 1000);
        }
    }

    free(line);
}

int main(int argc, char **argv) {
    Args a = parse_args(argc, argv);

    if (!a.live) {
        if (a.file[0] == '\0' || a.out[0] == '\0') {
            usage(argv[0]);
            return 2;
        }
        run_file_mode(&a);
        return 0;
    }

    // live
    if (a.input_dir[0] == '\0' || a.out_dir[0] == '\0') {
        usage(argv[0]);
        return 2;
    }
    run_live_mode(&a);
    return 0;
}


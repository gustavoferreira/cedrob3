//gcc -O3 -march=native -pipe -std=c11 parser_Z.c -o parser_Z -lm 
//
// parser_Z_signal_v3.c  (Linux, GCC 7.5+, Ubuntu 18.04 OK)
//
// Tail -f de arquivo Cedro *_Z.txt e gera snapshots 1s + sinal (BUY/SELL/HOLD)
// inspirado no parser_Z_signal_v3_py36.py
//
// Compilar:
//   gcc -O3 -std=c11 -Wall -Wextra -pedantic -o parser_Z_signal_v3 parser_Z_signal_v3.c -lm
//
// Exemplo (arquivo fixo):
//   ./parser_Z_signal_v3 
//     --input-template /home/grao/dados/cedro_files/20251222_Z.txt 
//     --out-csv /home/grao/dados/sab/20251222_ztop_signal_1s.csv 
//     --state-dir /home/grao/dados/sab/state_z 
//     --symbols WING26,WDOF26 
//     --depth 15 --topn 5 --snapshot-sec 1 
//     --min-warmup 60 --score-th 1.2 --persist 3 --cooldown-sec 30 --require-sign
//
// Exemplo (por dia):
//   ./parser_Z_signal_v3 
//     --input-template /home/grao/dados/cedro_files/{ymd}_Z.txt 
//     --out-template /home/grao/dados/sab/{ymd}_ztop_signal_1s.csv 
//     --state-dir /home/grao/dados/sab/state_z 
//     --symbols WING26,WDOF26 
//     --depth 15 --topn 5 --snapshot-sec 1 
//     --min-warmup 60 --score-th 1.2 --persist 3 --cooldown-sec 30 --require-sign
//
// Observações:
// - Lê em loop; quando chega EOF, dorme e continua.
// - Guarda offset em state-dir (arquivo .offset) para retomar.
// - Se {ymd} estiver no input-template, faz rollover diário automaticamente.
// - "delay_ms" em replay será enorme (ok).
//
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <math.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>

#ifndef NAN
#define NAN (0.0/0.0)
#endif

#define MAX_SYMS 16
#define MAX_PATH 4096
#define MID_RING 256

typedef struct {
  double price;
  int qty;
  int n_orders;
  int valid;
} Level;

typedef struct {
  int depth;
  Level *bids; // side 'A'
  Level *asks; // side 'V'
} OrderBook;

typedef struct {
  int window;
  double *buf;
  int head;
  int n;
  double sum;
  double sumsq;
} RollingZ;

typedef struct {
  // features
  double imb_ema_5;
  int imb_ema_init;
  double spread_ema_30;
  int spread_ema_init;
  double min_spread_seen;
  int min_spread_init;

  RollingZ rz_imb;
  RollingZ rz_mid;

  // mid ring for exact t-3 lookup
  int mid_sec[MID_RING];
  double mid_val[MID_RING];
  int mid_head;
  int mid_init;

  // D3 recent
  int last_d3_sec;     // sec_of_day or -1
  int last_d3_init;

  // persist + entry
  char persist_dir;    // 'B' or 'S' or 'H'
  int persist_count;
  int last_entry_sec;  // sec_of_day or -1
  char last_entry_dir; // 'B' or 'S' or 0
} FeatState;

typedef struct {
  int A, U, D1, D3, E, bad;
} Counters;

typedef struct {
  char symbol[32];
  OrderBook book;
  FeatState st;
  Counters ctr;
  int seen_any;
} SymCtx;

typedef struct {
  char input_template[MAX_PATH];
  char out_csv[MAX_PATH];
  char out_template[MAX_PATH];
  char state_dir[MAX_PATH];
  char symbols_csv[256];
  int depth;
  int topn;
  int snapshot_sec;
  double poll_sec;
  int start_at_end;
  char date_fixed[16]; // optional YYYYMMDD
  int cooldown_sec;
  int zwin;
  int min_warmup;
  double score_th;
  int require_sign;
  int persist_n;
  int ckpt_sec;
  int flush_sec;
  int batch_mode;
  int reset_state;
} Config;

// ---------- utils ----------

static void die(const char *msg) {
  fprintf(stderr, "ERRO: %s\n", msg);
  exit(2);
}

static int file_exists(const char *path) {
  struct stat st;
  return stat(path, &st) == 0;
}

static void ensure_dir(const char *path) {
  if (!path || !path[0]) return;
  struct stat st;
  if (stat(path, &st) == 0) {
    if (S_ISDIR(st.st_mode)) return;
    fprintf(stderr, "ERRO: %s existe e não é diretório\n", path);
    exit(2);
  }
  if (mkdir(path, 0775) != 0) {
    char tmp[MAX_PATH];
    strncpy(tmp, path, sizeof(tmp));
    tmp[sizeof(tmp)-1] = 0;
    char *slash = strrchr(tmp, '/');
    if (slash) {
      *slash = 0;
      if (tmp[0]) mkdir(tmp, 0775);
    }
    if (mkdir(path, 0775) != 0 && errno != EEXIST) {
      perror("mkdir");
      exit(2);
    }
  }
}

static void now_iso_ms(char out[64]) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  struct tm tmv;
  localtime_r(&tv.tv_sec, &tmv);
  int ms = (int)(tv.tv_usec / 1000);
  snprintf(out, 64, "%04d-%02d-%02dT%02d:%02d:%02d.%03d",
           tmv.tm_year+1900, tmv.tm_mon+1, tmv.tm_mday,
           tmv.tm_hour, tmv.tm_min, tmv.tm_sec, ms);
}

static void today_ymd(char out[16]) {
  time_t t = time(NULL);
  struct tm tmv;
  localtime_r(&t, &tmv);
  snprintf(out, 16, "%04d%02d%02d", tmv.tm_year+1900, tmv.tm_mon+1, tmv.tm_mday);
}

static int parse_write_ts_sec(const char *write_ts, int *sec_of_day_out) {
  // write_ts: YYYYMMDD_HHMMSS
  if (!write_ts || strlen(write_ts) < 15) return 0;
  if (write_ts[8] != '_') return 0;
  int hh = (write_ts[9]-'0')*10 + (write_ts[10]-'0');
  int mm = (write_ts[11]-'0')*10 + (write_ts[12]-'0');
  int ss = (write_ts[13]-'0')*10 + (write_ts[14]-'0');
  if (hh<0||hh>23||mm<0||mm>59||ss<0||ss>59) return 0;
  *sec_of_day_out = hh*3600 + mm*60 + ss;
  return 1;
}

static time_t parse_write_ts_time_t(const char *write_ts) {
  // local time; suficiente p/ delay_ms
  struct tm tmv;
  memset(&tmv, 0, sizeof(tmv));
  // YYYYMMDD_HHMMSS
  char buf[5];
  buf[4] = 0;
  memcpy(buf, write_ts, 4); tmv.tm_year = atoi(buf) - 1900;
  memcpy(buf, write_ts+4, 2); buf[2]=0; tmv.tm_mon  = atoi(buf) - 1;
  memcpy(buf, write_ts+6, 2); buf[2]=0; tmv.tm_mday = atoi(buf);
  memcpy(buf, write_ts+9, 2); buf[2]=0; tmv.tm_hour = atoi(buf);
  memcpy(buf, write_ts+11,2); buf[2]=0; tmv.tm_min  = atoi(buf);
  memcpy(buf, write_ts+13,2); buf[2]=0; tmv.tm_sec  = atoi(buf);
  tmv.tm_isdst = -1;
  return mktime(&tmv);
}

static double ema_update(double prev, int prev_init, double x, double alpha, int *out_init) {
  if (!prev_init) {
    *out_init = 1;
    return x;
  }
  *out_init = 1;
  return alpha * x + (1.0 - alpha) * prev;
}

// ---------- RollingZ ----------

static void rz_init(RollingZ *rz, int window) {
  rz->window = window;
  rz->buf = (double*)calloc((size_t)window, sizeof(double));
  rz->head = 0;
  rz->n = 0;
  rz->sum = 0.0;
  rz->sumsq = 0.0;
}

static void rz_free(RollingZ *rz) {
  free(rz->buf);
  rz->buf = NULL;
}

static void rz_push(RollingZ *rz, double x) {
  if (rz->n < rz->window) {
    rz->buf[rz->head] = x;
    rz->sum += x;
    rz->sumsq += x*x;
    rz->head = (rz->head + 1) % rz->window;
    rz->n++;
  } else {
    double old = rz->buf[rz->head];
    rz->sum -= old;
    rz->sumsq -= old*old;
    rz->buf[rz->head] = x;
    rz->sum += x;
    rz->sumsq += x*x;
    rz->head = (rz->head + 1) % rz->window;
  }
}

static void rz_mean_std(const RollingZ *rz, double *mean, double *std) {
  if (rz->n <= 1) { *mean = 0.0; *std = 0.0; return; }
  double m = rz->sum / (double)rz->n;
  double var = (rz->sumsq / (double)rz->n) - m*m;
  if (var < 0.0) var = 0.0;
  *mean = m;
  *std = sqrt(var);
}

static double rz_z(const RollingZ *rz, double x) {
  double mean, std;
  rz_mean_std(rz, &mean, &std);
  if (std <= 1e-12) return 0.0;
  return (x - mean) / std;
}

// ---------- OrderBook ----------

static void ob_init(OrderBook *ob, int depth) {
  ob->depth = depth;
  ob->bids = (Level*)calloc((size_t)depth, sizeof(Level));
  ob->asks = (Level*)calloc((size_t)depth, sizeof(Level));
}

static void ob_free(OrderBook *ob) {
  free(ob->bids); ob->bids = NULL;
  free(ob->asks); ob->asks = NULL;
}

static void ob_reset(OrderBook *ob) {
  memset(ob->bids, 0, (size_t)ob->depth * sizeof(Level));
  memset(ob->asks, 0, (size_t)ob->depth * sizeof(Level));
}

static void ob_shift_delete(Level *arr, int depth, int pos) {
  for (int i = pos; i < depth-1; i++) arr[i] = arr[i+1];
  arr[depth-1].valid = 0;
}

static void ob_apply(OrderBook *ob, char op, int cancel_type, char side, int pos, double price, int qty, int n_orders) {
  if (op == 'D' && cancel_type == 3) { ob_reset(ob); return; }
  if (op == 'D' && cancel_type == 1) {
    if (pos >= 0 && pos < ob->depth) {
      if (side == 'A') ob_shift_delete(ob->bids, ob->depth, pos);
      else if (side == 'V') ob_shift_delete(ob->asks, ob->depth, pos);
    }
    return;
  }
  if (op == 'A' || op == 'U') {
    if (pos >= 0 && pos < ob->depth) {
      Level *arr = (side == 'A') ? ob->bids : ob->asks;
      arr[pos].price = price;
      arr[pos].qty = qty;
      arr[pos].n_orders = n_orders;
      arr[pos].valid = 1;
    }
  }
}

typedef struct {
  double best_bid, best_ask, spread, mid;
  int bid_qty0, ask_qty0, bid_qty_topN, ask_qty_topN;
  double imb;
  int book_ready;
} Snap;

static Snap ob_snapshot(const OrderBook *ob, int topn) {
  Snap s;
  memset(&s, 0, sizeof(s));
  s.best_bid = NAN;
  s.best_ask = NAN;
  s.spread = NAN;
  s.mid = NAN;

  const Level *b0 = &ob->bids[0];
  const Level *a0 = &ob->asks[0];
  if (b0->valid) { s.best_bid = b0->price; s.bid_qty0 = b0->qty; }
  if (a0->valid) { s.best_ask = a0->price; s.ask_qty0 = a0->qty; }

  int n = topn < ob->depth ? topn : ob->depth;
  int bsum = 0, asum = 0;
  for (int i=0;i<n;i++) {
    if (ob->bids[i].valid) bsum += ob->bids[i].qty;
    if (ob->asks[i].valid) asum += ob->asks[i].qty;
  }
  s.bid_qty_topN = bsum;
  s.ask_qty_topN = asum;

  int denom = bsum + asum;
  s.imb = (denom > 0) ? ((double)(bsum - asum) / (double)denom) : 0.0;

  if (!isnan(s.best_bid) && !isnan(s.best_ask)) {
    s.spread = s.best_ask - s.best_bid;
    s.mid = (s.best_ask + s.best_bid) / 2.0;
    s.book_ready = 1;
  } else {
    s.book_ready = 0;
  }
  return s;
}

// ---------- mid ring ----------

static void midring_put(FeatState *st, int sec_of_day, double mid) {
  int idx = st->mid_head % MID_RING;
  st->mid_sec[idx] = sec_of_day;
  st->mid_val[idx] = mid;
  st->mid_head = (st->mid_head + 1) % MID_RING;
  st->mid_init = 1;
}

static int midring_get(const FeatState *st, int sec_of_day, double *mid_out) {
  if (!st->mid_init) return 0;
  for (int i=0;i<MID_RING;i++) {
    if (st->mid_sec[i] == sec_of_day) { *mid_out = st->mid_val[i]; return 1; }
  }
  return 0;
}

// ---------- symbol ctx ----------

static void sym_init(SymCtx *sc, const char *sym, int depth, int zwin) {
  memset(sc, 0, sizeof(*sc));
  strncpy(sc->symbol, sym, sizeof(sc->symbol)-1);
  ob_init(&sc->book, depth);
  FeatState *st = &sc->st;
  memset(st, 0, sizeof(*st));
  st->last_d3_sec = -1;
  st->last_entry_sec = -1;
  st->last_entry_dir = 0;
  st->persist_dir = 'H';
  st->persist_count = 0;
  st->mid_head = 0;
  st->mid_init = 0;
  for (int i=0;i<MID_RING;i++) st->mid_sec[i] = -999999;
  rz_init(&st->rz_imb, zwin);
  rz_init(&st->rz_mid, zwin);
}

static void sym_free(SymCtx *sc) {
  rz_free(&sc->st.rz_imb);
  rz_free(&sc->st.rz_mid);
  ob_free(&sc->book);
}

// ---------- config parsing ----------

static int has_ymd_placeholder(const char *s) {
  return (s && strstr(s, "{ymd}") != NULL);
}

static void format_template(const char *tmpl, const char *ymd, char out[MAX_PATH]) {
  const char *p = strstr(tmpl, "{ymd}");
  if (!p) { strncpy(out, tmpl, MAX_PATH-1); out[MAX_PATH-1]=0; return; }
  size_t pre = (size_t)(p - tmpl);
  snprintf(out, MAX_PATH, "%.*s%s%s", (int)pre, tmpl, ymd, p+5);
}

static void dirname_of(const char *path, char out[MAX_PATH]) {
  strncpy(out, path, MAX_PATH-1); out[MAX_PATH-1]=0;
  char *slash = strrchr(out, '/');
  if (slash) *slash = 0; else strcpy(out, ".");
}

static void state_path_for(const Config *cfg, const char *ymd, char out[MAX_PATH]) {
  char key[32];
  if (has_ymd_placeholder(cfg->input_template) || (cfg->date_fixed[0] != 0)) strncpy(key, ymd, sizeof(key)-1);
  else strcpy(key, "fixed");
  int n = snprintf(out, MAX_PATH, "%s/%s_Z.offset", cfg->state_dir, key);
  if (n < 0 || n >= MAX_PATH) { fprintf(stderr, "ERRO: state path muito longo\n"); exit(2); }
}

static long read_offset(const char *state_path) {
  FILE *f = fopen(state_path, "r");
  if (!f) return 0;
  char buf[64];
  if (!fgets(buf, sizeof(buf), f)) { fclose(f); return 0; }
  fclose(f);
  return strtol(buf, NULL, 10);
}

static void write_offset(const char *state_path, long off) {
  FILE *f = fopen(state_path, "w");
  if (!f) return;
  fprintf(f, "%ld", off);
  fclose(f);
}

// ---------- parsing line ----------

typedef struct {
  char write_ts[32];
  char symbol[32];
  char op;          // 'A','U','D','E'
  int cancel_type;  // for D
  char side;        // 'A' or 'V'
  int pos;
  double price;
  int qty;
  int n_orders;
} Event;

static int parse_event(char *line, Event *ev) {
  // line: write_ts,buf,src,payload
  char *p1 = strchr(line, ','); if (!p1) return 0;
  *p1 = 0;
  strncpy(ev->write_ts, line, sizeof(ev->write_ts)-1);
  ev->write_ts[sizeof(ev->write_ts)-1] = 0;

  char *p2 = strchr(p1+1, ','); if (!p2) return 0;
  char *p3 = strchr(p2+1, ','); if (!p3) return 0;
  char *payload = p3 + 1;
  size_t L = strlen(payload);
  while (L && (payload[L-1] == '\n' || payload[L-1] == '\r')) payload[--L] = 0;

  if (payload[0] != 'Z' || payload[1] != ':') return 0;

  char *save = NULL;
  char *tok = strtok_r(payload, ":", &save); // "Z"
  if (!tok) return 0;
  tok = strtok_r(NULL, ":", &save); // symbol
  if (!tok) return 0;
  strncpy(ev->symbol, tok, sizeof(ev->symbol)-1);
  ev->symbol[sizeof(ev->symbol)-1] = 0;
  tok = strtok_r(NULL, ":", &save); // op
  if (!tok) return 0;
  ev->op = tok[0];
  ev->cancel_type = 0;
  ev->side = 0;
  ev->pos = -1;
  ev->price = 0.0;
  ev->qty = 0;
  ev->n_orders = 0;

  if (ev->op == 'A' || ev->op == 'U') {
    tok = strtok_r(NULL, ":", &save); if (!tok) return 0; ev->pos = atoi(tok);
    tok = strtok_r(NULL, ":", &save); if (!tok) return 0; ev->side = tok[0];
    tok = strtok_r(NULL, ":", &save); if (!tok) return 0; ev->price = strtod(tok, NULL);
    tok = strtok_r(NULL, ":", &save); if (!tok) return 0; ev->qty = (int)strtod(tok, NULL);
    tok = strtok_r(NULL, ":", &save); if (!tok) return 0; ev->n_orders = (int)strtod(tok, NULL);
    return 1;
  }

  if (ev->op == 'D') {
    tok = strtok_r(NULL, ":", &save); if (!tok) return 0;
    ev->cancel_type = atoi(tok);
    if (ev->cancel_type == 3) return 1;
    if (ev->cancel_type == 1) {
      tok = strtok_r(NULL, ":", &save); if (!tok) return 0; ev->side = tok[0];
      tok = strtok_r(NULL, ":", &save); if (!tok) return 0; ev->pos = atoi(tok);
      return 1;
    }
    return 1;
  }

  if (ev->op == 'E') return 1;
  return 1;
}

// ---------- signal compute ----------

typedef struct {
  char signal[8];      // "BUY" "SELL" "HOLD"
  char entry[8];       // "BUY" "SELL" ""
  double conf;
  double score, z_imb, z_mid;
  int warmup_ok, spread_ok;
  char block_reason[32];
  double imb_ema_5;
  double mid_chg_3;
  int activity;
} SigOut;

static SigOut compute_signal(const Config *cfg, SymCtx *sc, int sec_of_day, const Snap *snap) {
  SigOut out;
  memset(&out, 0, sizeof(out));
  strcpy(out.signal, "HOLD");
  out.entry[0] = 0;
  out.conf = 0.0;
  out.score = 0.0;
  out.z_imb = 0.0;
  out.z_mid = 0.0;
  out.warmup_ok = 0;
  out.spread_ok = 0;
  strcpy(out.block_reason, "");
  out.activity = sc->ctr.A + sc->ctr.U;

  FeatState *st = &sc->st;

  if (!snap->book_ready || isnan(snap->mid) || isnan(snap->spread)) {
    strcpy(out.block_reason, "book_not_ready");
    out.imb_ema_5 = st->imb_ema_init ? st->imb_ema_5 : 0.0;
    out.mid_chg_3 = 0.0;
    return out;
  }

  double sp = snap->spread;
  if (sp > 0.0) {
    if (!st->min_spread_init || sp < st->min_spread_seen) {
      st->min_spread_seen = sp;
      st->min_spread_init = 1;
    }
  }
  st->spread_ema_30 = ema_update(st->spread_ema_30, st->spread_ema_init, sp, 2.0/(30.0+1.0), &st->spread_ema_init);

  st->imb_ema_5 = ema_update(st->imb_ema_5, st->imb_ema_init, snap->imb, 2.0/(5.0+1.0), &st->imb_ema_init);

  midring_put(st, sec_of_day, snap->mid);
  double mid_3;
  double mid_chg_3 = 0.0;
  if (midring_get(st, sec_of_day - 3, &mid_3)) mid_chg_3 = snap->mid - mid_3;

  rz_push(&st->rz_imb, st->imb_ema_5);
  rz_push(&st->rz_mid, mid_chg_3);

  out.imb_ema_5 = st->imb_ema_5;
  out.mid_chg_3 = mid_chg_3;

  if (st->rz_imb.n >= cfg->min_warmup) out.warmup_ok = 1;
  else { strcpy(out.block_reason, "warmup"); return out; }

  double sp_ema = st->spread_ema_init ? st->spread_ema_30 : sp;
  double min_sp = st->min_spread_init ? st->min_spread_seen : sp;
  double spread_th = fmax(sp_ema * 1.8, min_sp * 2.0);
  if (sp <= spread_th) out.spread_ok = 1;
  if (!out.spread_ok) { strcpy(out.block_reason, "spread"); return out; }

  if (sc->ctr.D3 > 0) { st->last_d3_sec = sec_of_day; st->last_d3_init = 1; }
  if (st->last_d3_init && (sec_of_day - st->last_d3_sec) < 2) { strcpy(out.block_reason, "d3_recent"); return out; }

  out.z_imb = rz_z(&st->rz_imb, st->imb_ema_5);
  out.z_mid = rz_z(&st->rz_mid, mid_chg_3);
  out.score = 0.75*out.z_imb + 0.25*out.z_mid;
  out.conf = fmin(1.0, fabs(out.score)/3.0);

  if (fabs(out.score) < cfg->score_th) { strcpy(out.block_reason, "score_th"); return out; }

  char decision = (out.score > 0.0) ? 'B' : 'S';

  if (cfg->require_sign) {
    if (decision == 'B' && !(st->imb_ema_5 > 0.0 && mid_chg_3 >= 0.0)) { strcpy(out.block_reason, "require_sign"); out.conf = 0.0; return out; }
    if (decision == 'S' && !(st->imb_ema_5 < 0.0 && mid_chg_3 <= 0.0)) { strcpy(out.block_reason, "require_sign"); out.conf = 0.0; return out; }
  }

  strcpy(out.signal, (decision=='B') ? "BUY" : "SELL");

  if (decision == st->persist_dir) st->persist_count += 1;
  else { st->persist_dir = decision; st->persist_count = 1; }

  if (st->persist_count >= cfg->persist_n) {
    if (st->last_entry_sec >= 0) {
      if ((sec_of_day - st->last_entry_sec) < cfg->cooldown_sec) { strcpy(out.block_reason, "cooldown"); out.entry[0]=0; return out; }
      if (st->last_entry_dir == decision) { strcpy(out.block_reason, "same_dir"); out.entry[0]=0; return out; }
    }
    strcpy(out.entry, (decision=='B') ? "BUY" : "SELL");
    st->last_entry_sec = sec_of_day;
    st->last_entry_dir = decision;
  }

  return out;
}

// ---------- CSV output ----------

static int csv_needs_header(const char *path) {
  struct stat st;
  return (stat(path, &st) != 0 || st.st_size == 0);
}

static void csv_write_header(FILE *out) {
  fprintf(out,
    "read_ts,write_ts,symbol,"
    "best_bid,best_ask,spread,mid,"
    "bid_qty0,ask_qty0,bid_qty_topN,ask_qty_topN,"
    "imb,imb_ema_5,mid_chg_3,activity,"
    "signal,entry_signal,signal_conf,"
    "score,z_imb,z_mid,"
    "warmup_ok,spread_ok,block_reason,"
    "book_ready,"
    "msg_A,msg_U,msg_D1,msg_D3,msg_E,msg_bad,"
    "delay_ms,file_offset,file_path\n"
  );
  fflush(out);
}

static void csv_write_row(FILE *out, const char *read_ts, const char *write_ts, const char *symbol,
                          const Snap *snap, const SigOut *sg, const Counters *ctr,
                          long delay_ms, long file_offset, const char *file_path) {
  fprintf(out,
    "%s,%s,%s,"
    "%.10g,%.10g,%.10g,%.10g,"
    "%d,%d,%d,%d,"
    "%.6f,%.6f,%.6f,%d,"
    "%s,%s,%.3f,"
    "%.6f,%.6f,%.6f,"
    "%d,%d,%s,"
    "%d,"
    "%d,%d,%d,%d,%d,%d,"
    "%ld,%ld,%s\n",
    read_ts, write_ts, symbol,
    (isnan(snap->best_bid)?0.0:snap->best_bid),
    (isnan(snap->best_ask)?0.0:snap->best_ask),
    (isnan(snap->spread)?0.0:snap->spread),
    (isnan(snap->mid)?0.0:snap->mid),
    snap->bid_qty0, snap->ask_qty0, snap->bid_qty_topN, snap->ask_qty_topN,
    snap->imb, sg->imb_ema_5, sg->mid_chg_3, sg->activity,
    sg->signal, sg->entry, sg->conf,
    sg->score, sg->z_imb, sg->z_mid,
    sg->warmup_ok, sg->spread_ok, (sg->block_reason[0]?sg->block_reason:""),
    snap->book_ready,
    ctr->A, ctr->U, ctr->D1, ctr->D3, ctr->E, ctr->bad,
    delay_ms, file_offset, file_path
  );
}

// ---------- main loop ----------

static int split_symbols(const char *csv, char syms[MAX_SYMS][32]) {
  int n = 0;
  const char *p = csv;
  while (*p) {
    while (*p && (*p==',' || isspace((unsigned char)*p))) p++;
    if (!*p) break;
    char tmp[32]; int k=0;
    while (*p && *p!=',' && !isspace((unsigned char)*p) && k<31) tmp[k++]=*p++;
    tmp[k]=0;
    if (k>0 && n<MAX_SYMS) { strncpy(syms[n], tmp, 31); syms[n][31]=0; n++; }
    while (*p && *p!=',') p++;
    if (*p==',') p++;
  }
  return n;
}

static SymCtx* find_sym(SymCtx *arr, int n, const char *sym) {
  for (int i=0;i<n;i++) if (strcmp(arr[i].symbol, sym)==0) return &arr[i];
  return NULL;
}

static void reset_counters(SymCtx *sc) {
  sc->ctr.A = sc->ctr.U = sc->ctr.D1 = sc->ctr.D3 = sc->ctr.E = sc->ctr.bad = 0;
}

static void usage() {
  fprintf(stderr,
    "Uso: parser_Z --input-template <path|tmpl> (--out-csv <file> | --out-template <tmpl>) --state-dir <dir> --symbols <CSV> [opções]\n"
    "Opções:\n"
    "  --depth N (15)\n"
    "  --topn N (5)\n"
    "  --snapshot-sec N (1)\n"
    "  --poll-sec S (0.05)\n"
    "  --ckpt-sec N (1)  (salva offset a cada N s)\n"
    "  --flush-sec N (1) (fflush a cada N s)\n"
    "  --batch           (sai ao chegar no EOF)\n"
    "  --reset-state     (ignora offset salvo e reprocessa do início)\n"
    "  --start-at-end\n"
    "  --date YYYYMMDD (fixa dia no template)\n"
    "  --cooldown-sec N (30)\n"
    "  --persist N (3)\n"
    "  --min-warmup N (60)\n"
    "  --zwin N (60)\n"
    "  --score-th X (1.2)\n"
    "  --require-sign (exige direção do mid junto)\n"
  );
  exit(2);
}

static int arg_eq(const char *a, const char *b) { return strcmp(a,b)==0; }

int main(int argc, char **argv) {
  Config cfg;
  memset(&cfg, 0, sizeof(cfg));
  cfg.depth = 15;
  cfg.topn = 5;
  cfg.snapshot_sec = 1;
  cfg.poll_sec = 0.05;
  cfg.start_at_end = 0;
  cfg.cooldown_sec = 30;
  cfg.zwin = 60;
  cfg.min_warmup = 60;
  cfg.score_th = 1.0;
  cfg.require_sign = 0;
  cfg.persist_n = 3;
  cfg.ckpt_sec = 1;
  cfg.flush_sec = 1;
  cfg.batch_mode = 0;
  cfg.reset_state = 0;

  for (int i=1;i<argc;i++) {
    if (arg_eq(argv[i], "--input-template") && i+1<argc) strncpy(cfg.input_template, argv[++i], MAX_PATH-1);
    else if (arg_eq(argv[i], "--out-csv") && i+1<argc) strncpy(cfg.out_csv, argv[++i], MAX_PATH-1);
    else if (arg_eq(argv[i], "--out-template") && i+1<argc) strncpy(cfg.out_template, argv[++i], MAX_PATH-1);
    else if (arg_eq(argv[i], "--state-dir") && i+1<argc) strncpy(cfg.state_dir, argv[++i], MAX_PATH-1);
    else if (arg_eq(argv[i], "--symbols") && i+1<argc) strncpy(cfg.symbols_csv, argv[++i], sizeof(cfg.symbols_csv)-1);
    else if (arg_eq(argv[i], "--depth") && i+1<argc) cfg.depth = atoi(argv[++i]);
    else if (arg_eq(argv[i], "--topn") && i+1<argc) cfg.topn = atoi(argv[++i]);
    else if (arg_eq(argv[i], "--snapshot-sec") && i+1<argc) cfg.snapshot_sec = atoi(argv[++i]);
    else if (arg_eq(argv[i], "--poll-sec") && i+1<argc) cfg.poll_sec = atof(argv[++i]);
    else if (arg_eq(argv[i], "--ckpt-sec") && i+1<argc) cfg.ckpt_sec = atoi(argv[++i]);
    else if (arg_eq(argv[i], "--flush-sec") && i+1<argc) cfg.flush_sec = atoi(argv[++i]);
    else if (arg_eq(argv[i], "--batch")) cfg.batch_mode = 1;
    else if (arg_eq(argv[i], "--reset-state")) cfg.reset_state = 1;
    else if (arg_eq(argv[i], "--start-at-end")) cfg.start_at_end = 1;
    else if (arg_eq(argv[i], "--date") && i+1<argc) strncpy(cfg.date_fixed, argv[++i], sizeof(cfg.date_fixed)-1);
    else if (arg_eq(argv[i], "--cooldown-sec") && i+1<argc) cfg.cooldown_sec = atoi(argv[++i]);
    else if (arg_eq(argv[i], "--zwin") && i+1<argc) cfg.zwin = atoi(argv[++i]);
    else if (arg_eq(argv[i], "--min-warmup") && i+1<argc) cfg.min_warmup = atoi(argv[++i]);
    else if (arg_eq(argv[i], "--score-th") && i+1<argc) cfg.score_th = atof(argv[++i]);
    else if (arg_eq(argv[i], "--persist") && i+1<argc) cfg.persist_n = atoi(argv[++i]);
    else if (arg_eq(argv[i], "--require-sign")) cfg.require_sign = 1;
    else {
      usage();
      fprintf(stderr, "Arg desconhecido: %s\n", argv[i]);
      return 2;
    }
  }

  if (cfg.input_template[0]==0) die("informe --input-template");
  if (cfg.state_dir[0]==0) die("informe --state-dir");
  if (cfg.symbols_csv[0]==0) die("informe --symbols");
  if (cfg.out_csv[0]==0 && cfg.out_template[0]==0) die("informe --out-csv OU --out-template");

  ensure_dir(cfg.state_dir);

  char syms[MAX_SYMS][32];
  int n_syms = split_symbols(cfg.symbols_csv, syms);
  if (n_syms <= 0) die("nenhum símbolo em --symbols");

  SymCtx ctx[MAX_SYMS];
  for (int i=0;i<n_syms;i++) sym_init(&ctx[i], syms[i], cfg.depth, cfg.zwin);

  char cur_ymd[16];
  if (cfg.date_fixed[0]) strncpy(cur_ymd, cfg.date_fixed, sizeof(cur_ymd)-1);
  else today_ymd(cur_ymd);

  char input_path[MAX_PATH], out_path[MAX_PATH], out_dir[MAX_PATH], state_path[MAX_PATH];
  format_template(cfg.input_template, cur_ymd, input_path);
  if (cfg.out_template[0]) format_template(cfg.out_template, cur_ymd, out_path);
  else strncpy(out_path, cfg.out_csv, MAX_PATH-1);
  dirname_of(out_path, out_dir);
  ensure_dir(out_dir);

  FILE *fin = NULL;
  FILE *fout = NULL;

  char last_write_ts[32] = {0};
  int last_sec_of_day = -1;
  time_t last_ckpt_t = 0;
  time_t last_flush_t = 0;
  long last_ckpt_off = -1;

  while (1) {
    if (!cfg.date_fixed[0] && has_ymd_placeholder(cfg.input_template)) {
      char ymd_now[16]; today_ymd(ymd_now);
      if (strcmp(ymd_now, cur_ymd) != 0) {
        strncpy(cur_ymd, ymd_now, sizeof(cur_ymd)-1);
        if (fin) { fclose(fin); fin = NULL; }
        if (fout) { fclose(fout); fout = NULL; }
        format_template(cfg.input_template, cur_ymd, input_path);
        if (cfg.out_template[0]) format_template(cfg.out_template, cur_ymd, out_path);
        else strncpy(out_path, cfg.out_csv, MAX_PATH-1);
        dirname_of(out_path, out_dir);
        ensure_dir(out_dir);
        last_write_ts[0] = 0;
        last_sec_of_day = -1;
        last_ckpt_t = 0;
        last_flush_t = 0;
        last_ckpt_off = -1;
        for (int i=0;i<n_syms;i++) reset_counters(&ctx[i]);
      }
    }

    if (!fin) {
      if (!file_exists(input_path)) {
        usleep((useconds_t)(cfg.poll_sec * 1000000.0));
        continue;
      }
      fin = fopen(input_path, "r");
      if (fin) setvbuf(fin, NULL, _IOFBF, 1<<20);
      if (!fin) { perror("fopen input"); usleep(200000); continue; }

      state_path_for(&cfg, cur_ymd, state_path);
      long last_offset = read_offset(state_path);
      if (cfg.reset_state) last_offset = 0;

      if (cfg.start_at_end && last_offset == 0) {
        fseek(fin, 0, SEEK_END);
      } else {
        if (last_offset > 0) fseek(fin, last_offset, SEEK_SET);
      }
    }

    if (!fout) {
      int need_header = csv_needs_header(out_path);
      fout = fopen(out_path, "a");
      if (fout) setvbuf(fout, NULL, _IOFBF, 1<<20);
      if (!fout) { perror("fopen out"); fclose(fin); fin=NULL; usleep(200000); continue; }
      if (need_header) csv_write_header(fout);
    }

    // getline buffer reaproveitado (evita malloc/free por linha)
    static char *line = NULL;
    static size_t cap = 0;
    ssize_t nread = getline(&line, &cap, fin);
    if (nread < 0) {
      long off = ftell(fin);
      // checkpoint final antes de dormir/sair
      if (off != last_ckpt_off) {
        write_offset(state_path, off);
        last_ckpt_off = off;
      }
      if (fout) fflush(fout);
      clearerr(fin);
      if (cfg.batch_mode) break;
      usleep((useconds_t)(cfg.poll_sec * 1000000.0));
      continue;
    }
    long file_off = ftell(fin);

    Event ev;
    if (!parse_event(line, &ev)) { continue; }

    SymCtx *sc = find_sym(ctx, n_syms, ev.symbol);
    if (!sc) { continue; }
    sc->seen_any = 1;

    if (ev.op == 'A') sc->ctr.A++;
    else if (ev.op == 'U') sc->ctr.U++;
    else if (ev.op == 'E') sc->ctr.E++;
    else if (ev.op == 'D') {
      if (ev.cancel_type == 1) sc->ctr.D1++;
      else if (ev.cancel_type == 3) sc->ctr.D3++;
    }

    if (ev.op == 'A' || ev.op == 'U' || ev.op == 'D') {
      ob_apply(&sc->book, ev.op, ev.cancel_type, ev.side, ev.pos, ev.price, ev.qty, ev.n_orders);
    }

    int sec_of_day;
    if (!parse_write_ts_sec(ev.write_ts, &sec_of_day)) { continue; }

    if (last_write_ts[0] == 0) {
      strncpy(last_write_ts, ev.write_ts, sizeof(last_write_ts)-1);
      last_sec_of_day = sec_of_day;
    } else if (strcmp(ev.write_ts, last_write_ts) != 0) {
      if (cfg.snapshot_sec <= 1 || ((last_sec_of_day % cfg.snapshot_sec) == 0)) {
        time_t dtw = parse_write_ts_time_t(last_write_ts);
        struct timeval tv; gettimeofday(&tv, NULL);
        long delay_ms = (long)((tv.tv_sec - dtw) * 1000L + tv.tv_usec/1000L);

        char read_ts[64]; now_iso_ms(read_ts);

        for (int i=0;i<n_syms;i++) {
          SymCtx *sci = &ctx[i];
          Snap snap = ob_snapshot(&sci->book, cfg.topn);
          SigOut sg;
          if (sci->seen_any) sg = compute_signal(&cfg, sci, last_sec_of_day, &snap);
          else {
            memset(&sg, 0, sizeof(sg));
            strcpy(sg.signal, "HOLD");
            sg.entry[0]=0;
            sg.conf=0.0;
            strcpy(sg.block_reason, "no_data");
            sg.imb_ema_5 = 0.0;
            sg.mid_chg_3 = 0.0;
            sg.activity = 0;
          }
          csv_write_row(fout, read_ts, last_write_ts, sci->symbol, &snap, &sg, &sci->ctr,
                        delay_ms, file_off, input_path);
          reset_counters(sci);
        }
        // checkpoint (offset) e flush em cadência (evita custo por linha)
        time_t now_t = time(NULL);
        if (cfg.ckpt_sec <= 0 || last_ckpt_t == 0 || (now_t - last_ckpt_t) >= cfg.ckpt_sec) {
          if (file_off != last_ckpt_off) {
            write_offset(state_path, file_off);
            last_ckpt_off = file_off;
          }
          last_ckpt_t = now_t;
        }
        if (cfg.flush_sec <= 0 || last_flush_t == 0 || (now_t - last_flush_t) >= cfg.flush_sec) {
          fflush(fout);
          last_flush_t = now_t;
        }
      }

      strncpy(last_write_ts, ev.write_ts, sizeof(last_write_ts)-1);
      last_sec_of_day = sec_of_day;
    }

  }

  for (int i=0;i<n_syms;i++) sym_free(&ctx[i]);
  return 0;
}


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
build_trendchop_ticks.py (Python 3.6.9)

Gera features de Trend/Chop por segundo (1s) para cada símbolo, usando preço base (mid)
preferindo Z, depois B, depois T. Produz CSV em /home/grao/dados/chope.

Exemplo:
  python3 build_trendchop_ticks.py --date 20251222 --symbols WING26,WDOF26
"""

import os
import sys
import math
import argparse
import csv
from collections import deque, defaultdict


# -----------------------------
# Utils
# -----------------------------

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYYMMDD")
    ap.add_argument("--symbols", default="WING26,WDOF26", help="CSV symbols")
    ap.add_argument("--z-dir", default="/home/grao/dados/z")
    ap.add_argument("--t-dir", default="/home/grao/dados/t")
    ap.add_argument("--v-dir", default="/home/grao/dados/v")
    ap.add_argument("--b-dir", default="/home/grao/dados/b")
    ap.add_argument("--out-dir", default="/home/grao/dados/chope")

    ap.add_argument("--z-template", default="{date}_ztop_signal_1s.csv")
    ap.add_argument("--t-template", default="{date}_t_1s.csv")
    ap.add_argument("--v-template", default="{date}_v_1s.csv")
    ap.add_argument("--b-template", default="{date}_b_1s.csv")

    # tick sizes default (WIN=5, WDO=0.5). Pode sobrescrever via CLI.
    ap.add_argument("--tick-win", type=float, default=5.0)
    ap.add_argument("--tick-wdo", type=float, default=0.5)

    # janelas
    ap.add_argument("--windows", default="30,120,300,900", help="comma windows in seconds")
    # thresholds do segmentador
    ap.add_argument("--er_enter", type=float, default=0.35)
    ap.add_argument("--er_exit", type=float, default=0.25)
    ap.add_argument("--ts_ref", type=float, default=2.0, help="TS_ref para normalizar TrendScore")
    ap.add_argument("--ts_enter", type=float, default=1.0, help="limiar TS_120 para entrar (ex: 1.0~2.0)")
    # EMA do tamanho/duração de trend
    ap.add_argument("--ema_alpha", type=float, default=0.05)

    ap.add_argument("--verbose", action="store_true")
    return ap.parse_args()


def log(msg):
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def safe_float(x, default=None):
    try:
        if x is None:
            return default
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return default
        return float(s)
    except Exception:
        return default


def sign(x):
    if x is None:
        return 0
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def ensure_dir(p):
    if not os.path.isdir(p):
        os.makedirs(p)


def file_path(dirpath, template, date):
    return os.path.join(dirpath, template.format(date=date))


def read_csv_rows(path):
    """Le CSV e retorna (header, iterator rows dict)."""
    f = open(path, "r", newline="")
    reader = csv.DictReader(f)
    return f, reader.fieldnames, reader


def choose_tick_size(symbol, tick_win, tick_wdo):
    # regra simples: se começa com WD -> WDO, senão WIN
    s = symbol.upper()
    if s.startswith("WDO"):
        return tick_wdo
    return tick_win


# -----------------------------
# Price loader (mid per second)
# -----------------------------

def load_mid_series(date, symbols, args):
    """
    Retorna dict: symbol -> list of (write_ts, mid_float)
    write_ts esperado como "YYYYMMDD_HHMMSS" (do seu formato).
    Prioridade: Z -> B -> T
    """

    symset = set([s.strip() for s in symbols if s.strip()])

    z_path = file_path(args.z_dir, args.z_template, date)
    b_path = file_path(args.b_dir, args.b_template, date)
    t_path = file_path(args.t_dir, args.t_template, date)

    src_used = None
    src_path = None

    if os.path.isfile(z_path):
        src_used = "Z"
        src_path = z_path
    elif os.path.isfile(b_path):
        src_used = "B"
        src_path = b_path
    elif os.path.isfile(t_path):
        src_used = "T"
        src_path = t_path

    if src_used is None:
        return None, None, None

    series = defaultdict(list)

    f, header, rows = read_csv_rows(src_path)
    try:
        # campos prováveis
        # Z: write_ts, symbol, mid
        # B: bar_ts, symbol, mid
        # T: write_ts, symbol, mid (às vezes)
        ts_key_candidates = ["write_ts", "bar_ts"]
        mid_key_candidates = ["mid", "microprice", "last"]

        # resolve keys
        ts_key = None
        for k in ts_key_candidates:
            if k in header:
                ts_key = k
                break
        if ts_key is None:
            # tenta qualquer coluna que contenha "ts"
            for k in header:
                if "ts" in k.lower():
                    ts_key = k
                    break

        mid_key = None
        for k in mid_key_candidates:
            if k in header:
                mid_key = k
                break

        if ts_key is None or mid_key is None or "symbol" not in header:
            return src_used, src_path, series  # vazio; vai dar ruim depois mas loga

        for r in rows:
            sym = (r.get("symbol") or "").strip()
            if sym not in symset:
                continue

            ts = (r.get(ts_key) or "").strip()
            if not ts:
                continue

            mid = safe_float(r.get(mid_key), None)
            if mid is None:
                continue

            series[sym].append((ts, mid))

    finally:
        f.close()

    # garante ordenação por timestamp string (funciona no formato YYYYMMDD_HHMMSS)
    for sym in list(series.keys()):
        series[sym].sort(key=lambda x: x[0])

    return src_used, src_path, series


# -----------------------------
# Trend/Chop core
# -----------------------------

def efficiency_ratio(prices, idx_now, n):
    """
    ER_n = abs(p[t]-p[t-n]) / sum(abs(diff)) no período
    prices: list float
    idx_now: índice atual (0..)
    n: janela (int)
    """
    if idx_now < n:
        return None
    p0 = prices[idx_now - n]
    p1 = prices[idx_now]
    net = abs(p1 - p0)

    denom = 0.0
    # somatório abs diffs
    j = idx_now - n + 1
    while j <= idx_now:
        denom += abs(prices[j] - prices[j - 1])
        j += 1

    if denom <= 1e-12:
        return 0.0
    return net / denom


def tstat_returns(prices, idx_now, n):
    """
    TS_n em retornos simples (diferença em ticks).
    TS = mean(ret) / (std(ret)/sqrt(n))  => mean * sqrt(n) / std
    """
    if idx_now < n:
        return None
    rets = []
    j = idx_now - n + 1
    while j <= idx_now:
        rets.append(prices[j] - prices[j - 1])
        j += 1

    if not rets:
        return 0.0

    # mean
    s = 0.0
    for x in rets:
        s += x
    mu = s / float(len(rets))

    # std
    v = 0.0
    for x in rets:
        d = x - mu
        v += d * d
    v = v / float(max(1, len(rets) - 1))
    sd = math.sqrt(v)

    if sd <= 1e-12:
        return 0.0
    return (mu * math.sqrt(float(len(rets)))) / sd


class TrendSegmentState(object):
    """
    Mantém estado do "segmento de trend" e EMAs de duração/tamanho.
    """
    def __init__(self, ema_alpha):
        self.ema_alpha = float(ema_alpha)

        self.active = False
        self.start_idx = None
        self.start_price = None
        self.dir = 0

        self.ema_dur = None
        self.ema_move = None

    def _ema_update(self, cur, x):
        if cur is None:
            return x
        a = self.ema_alpha
        return (1.0 - a) * cur + a * x

    def on_step(self, idx_now, price_now, er120, ts120, enter_er, exit_er, ts_enter):
        """
        Atualiza estado e, se um trend terminou, atualiza EMAs.
        Critérios:
          entra: ER_120 > enter_er e TS_120 > ts_enter
          sai:   ER_120 < exit_er  (ou fraqueza + dir invertendo)
        """
        # não decide se não tem ER/TS ainda
        if er120 is None or ts120 is None:
            return

        # direção local do 120 (se tiver start_idx, usa move)
        # aqui a direção do momento (curta) será baseada no sinal do tstat e no delta recente
        # mas um jeito estável: delta = price_now - price_120
        # (isso será calculado fora e passado via er/ts; aqui fazemos simples)
        # Para simplificar: dir "instant" = sign(ts120) (boa proxy).
        dir_now = sign(ts120)

        if not self.active:
            if (er120 > enter_er) and (abs(ts120) > ts_enter):
                self.active = True
                self.start_idx = idx_now
                self.start_price = price_now
                self.dir = dir_now if dir_now != 0 else 1
            return

        # se ativo: verifica saída
        # condição 1: ER caiu
        if er120 < exit_er:
            self._close(idx_now, price_now)
            return

        # condição 2: direção virou e está fraca (TS baixo)
        # "virou" = dir_now != dir (e não zero)
        if dir_now != 0 and dir_now != self.dir:
            # se TS está baixo (perdeu força), fecha
            if abs(ts120) < ts_enter:
                self._close(idx_now, price_now)
                return

        # mantém

    def _close(self, idx_now, price_now):
        if not self.active or self.start_idx is None or self.start_price is None:
            self.active = False
            self.start_idx = None
            self.start_price = None
            self.dir = 0
            return

        dur = float(max(1, idx_now - self.start_idx))
        move = abs(price_now - self.start_price)

        self.ema_dur = self._ema_update(self.ema_dur, dur)
        self.ema_move = self._ema_update(self.ema_move, move)

        self.active = False
        self.start_idx = None
        self.start_price = None
        self.dir = 0

    def maturity(self, idx_now, price_now):
        """
        Retorna: (age, age_ratio, move_ratio, maturity, ema_dur, ema_move)
        """
        if not self.active or self.start_idx is None or self.start_price is None:
            return 0.0, 0.0, 0.0, 0.0, self.ema_dur, self.ema_move

        age = float(max(0, idx_now - self.start_idx))
        move = abs(price_now - self.start_price)

        age_ratio = 0.0
        move_ratio = 0.0
        if self.ema_dur is not None and self.ema_dur > 1e-9:
            age_ratio = age / self.ema_dur
        if self.ema_move is not None and self.ema_move > 1e-9:
            move_ratio = move / self.ema_move

        mat = max(age_ratio, move_ratio)
        return age, age_ratio, move_ratio, mat, self.ema_dur, self.ema_move


def compute_features_for_symbol(sym, series_ts_mid, tick_size, windows, args):
    """
    Retorna lista de dicts (linhas) já prontas para escrever no CSV.
    """
    # extrai arrays
    ts_list = [x[0] for x in series_ts_mid]
    mid_list = [x[1] for x in series_ts_mid]

    # converte pra ticks (float)
    mid_ticks = []
    for m in mid_list:
        mid_ticks.append(m / float(tick_size))

    # estado de trend segment
    seg = TrendSegmentState(args.ema_alpha)

    out = []
    for i in range(len(ts_list)):
        row = {}
        row["write_ts"] = ts_list[i]
        row["symbol"] = sym
        row["mid"] = mid_list[i]
        row["tick_size"] = tick_size
        row["mid_ticks"] = mid_ticks[i]

        # janelas
        er = {}
        ts = {}
        for w in windows:
            er[w] = efficiency_ratio(mid_ticks, i, w)
            ts[w] = tstat_returns(mid_ticks, i, w)

            row["er_%d" % w] = er[w] if er[w] is not None else ""
            row["ts_%d" % w] = ts[w] if ts[w] is not None else ""

        # direções (usa 120 se existir, senão 30)
        w_dir = 120 if 120 in windows else (30 if 30 in windows else windows[0])
        trend_dir = None
        if i >= w_dir:
            trend_dir = sign(mid_ticks[i] - mid_ticks[i - w_dir])
        else:
            trend_dir = 0
        row["trend_dir_%d" % w_dir] = trend_dir

        # TrendScore / ChopScore (usa 120/300 por padrão se existirem)
        er120 = er.get(120)
        er300 = er.get(300)
        ts120 = ts.get(120)

        # normalização TS
        ts_norm = 0.0
        if ts120 is not None:
            ts_norm = min(1.0, abs(ts120) / float(args.ts_ref))

        # se não tiver 300, usa 120; se não tiver 120, usa menor
        if er120 is None:
            # pega qualquer ER disponível
            for w in windows:
                if er[w] is not None:
                    er120 = er[w]
                    break
        if er300 is None:
            er300 = er120

        if er120 is None:
            trend_score = ""
            chop_score = ""
        else:
            e120 = float(er120)
            e300 = float(er300) if er300 is not None else float(er120)
            trend_score = 0.5 * e120 + 0.3 * e300 + 0.2 * ts_norm
            chop_score = 1.0 - 0.5 * e120 - 0.5 * e300

        row["trend_score"] = trend_score
        row["chop_score"] = chop_score

        # breakout / exhaust (se 30 e 300 existirem; senão vazio)
        brk = ""
        exa = ""
        if (30 in windows) and (300 in windows) and (er.get(30) is not None) and (er.get(300) is not None):
            brk = float(er[30]) - float(er[300])
        if (120 in windows) and (300 in windows) and (er.get(120) is not None) and (er.get(300) is not None):
            exa = float(er[120]) - float(er[300])

        row["breakout_er30_er300"] = brk
        row["exhaust_er120_er300"] = exa

        # atualiza segmento de trend usando ER_120/TS_120 se existir
        seg.on_step(
            idx_now=i,
            price_now=mid_ticks[i],
            er120=er.get(120),
            ts120=ts.get(120),
            enter_er=args.er_enter,
            exit_er=args.er_exit,
            ts_enter=args.ts_enter,
        )

        age, age_ratio, move_ratio, mat, ema_dur, ema_move = seg.maturity(i, mid_ticks[i])

        row["trend_active"] = 1 if seg.active else 0
        row["trend_age_s"] = age
        row["ema_trend_dur_s"] = ema_dur if ema_dur is not None else ""
        row["ema_trend_move_ticks"] = ema_move if ema_move is not None else ""
        row["age_ratio"] = age_ratio
        row["move_ratio"] = move_ratio
        row["trend_maturity"] = mat

        row["trend_mature_08"] = 1 if mat >= 0.8 else 0
        row["trend_ok_entry_06"] = 1 if (mat < 0.6) else 0

        out.append(row)

    return out


def write_output_csv(path, rows, windows):
    # define header fixo
    base_cols = [
        "write_ts", "symbol",
        "mid", "tick_size", "mid_ticks",
    ]
    w_cols = []
    for w in windows:
        w_cols.append("er_%d" % w)
        w_cols.append("ts_%d" % w)

    other_cols = [
        "trend_dir_120",  # pode ficar vazio se não existir, ajustamos abaixo
        "trend_score", "chop_score",
        "breakout_er30_er300", "exhaust_er120_er300",
        "trend_active", "trend_age_s",
        "ema_trend_dur_s", "ema_trend_move_ticks",
        "age_ratio", "move_ratio", "trend_maturity",
        "trend_mature_08", "trend_ok_entry_06",
    ]

    # corrige nome do trend_dir para janela usada (se 120 não existir)
    # pega a chave real do primeiro row
    if rows:
        # encontra col que começa com trend_dir_
        tdir_key = None
        for k in rows[0].keys():
            if k.startswith("trend_dir_"):
                tdir_key = k
                break
        if tdir_key and tdir_key != "trend_dir_120":
            other_cols[0] = tdir_key

    header = base_cols + w_cols + other_cols

    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        wr.writeheader()
        for r in rows:
            wr.writerow(r)


# -----------------------------
# Main
# -----------------------------

def main():
    args = parse_args()
    date = args.date.strip()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    windows = [int(x.strip()) for x in args.windows.split(",") if x.strip()]
    windows.sort()

    src_used, src_path, series = load_mid_series(date, symbols, args)
    if src_used is None:
        log("[SKIP] %s sem Z/B/T em %s,%s,%s" % (date, args.z_dir, args.b_dir, args.t_dir))
        return 0

    # se achou fonte mas não leu nada, ainda assim reporta
    if series is None or len(series) == 0:
        log("[SKIP] %s fonte %s mas sem dados legíveis: %s" % (date, src_used, src_path))
        return 0

    out_rows_all = []
    for sym in symbols:
        if sym not in series or not series[sym]:
            if args.verbose:
                log("[WARN] %s sem dados para %s na fonte %s" % (date, sym, src_used))
            continue

        tick_size = choose_tick_size(sym, args.tick_win, args.tick_wdo)
        rows_sym = compute_features_for_symbol(sym, series[sym], tick_size, windows, args)
        out_rows_all.extend(rows_sym)

    if not out_rows_all:
        log("[SKIP] %s sem linhas (após filtros) usando %s: %s" % (date, src_used, src_path))
        return 0

    # ordena por write_ts e symbol
    out_rows_all.sort(key=lambda r: (r.get("write_ts", ""), r.get("symbol", "")))

    out_path = os.path.join(args.out_dir, "%s_trendchop.csv" % date)
    write_output_csv(out_path, out_rows_all, windows)

    log("[OK] %s (%s:%s) -> %s" % (date, src_used, os.path.basename(src_path), out_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())


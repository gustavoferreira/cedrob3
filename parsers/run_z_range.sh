#!/usr/bin/env bash
set -euo pipefail

PARSER="./parser_Z"
IN_DIR="/home/grao/dados/cedro_files"
OUT_DIR="/home/grao/dados/z"
STATE_DIR="/home/grao/dados/state"

SYMBOLS="WING26,WDOF26"
DEPTH=15
TOPN=5
SNAP=1
MIN_WARMUP=60
ZWIN=60
SCORE_TH=1.2
PERSIST=3
COOLDOWN=30
CKPT=5
FLUSH=5

START="20251201"
END="20251227"

# gera lista de datas (inclusive) sem depender do GNU date (mas ele normalmente existe no Ubuntu)
d="$START"
while [[ "$d" -le "$END" ]]; do
  in_file="${IN_DIR}/${d}_Z.txt"
  out_file="${OUT_DIR}/${d}_ztop_signal_1s.csv"

  if [[ ! -f "$in_file" ]]; then
    echo "[SKIP] Não existe: $in_file"
    d=$(date -d "${d:0:4}-${d:4:2}-${d:6:2} +1 day" +"%Y%m%d")
    continue
  fi

  echo "[RUN] $d  input=$in_file  output=$out_file"

  # limpa offset antigo desse dia pra não confundir (mesmo usando --reset-state)
  rm -f "${STATE_DIR}/${d}_Z.offset" 2>/dev/null || true

  "$PARSER" \
    --input-template "$in_file" \
    --out-csv "$out_file" \
    --state-dir "$STATE_DIR" \
    --symbols "$SYMBOLS" \
    --depth "$DEPTH" --topn "$TOPN" --snapshot-sec "$SNAP" \
    --min-warmup "$MIN_WARMUP" --zwin "$ZWIN" \
    --score-th "$SCORE_TH" --persist "$PERSIST" --cooldown-sec "$COOLDOWN" --require-sign \
    --reset-state --batch --ckpt-sec "$CKPT" --flush-sec "$FLUSH"

  d=$(date -d "${d:0:4}-${d:4:2}-${d:6:2} +1 day" +"%Y%m%d")
done

echo "OK. Finalizado."

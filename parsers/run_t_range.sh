#!/usr/bin/env bash
set -euo pipefail

PARSER="./parser_T"
IN_DIR="/home/grao/dados/cedro_files"
OUT_DIR="/home/grao/dados/t"
STATE_DIR="/home/grao/dados/state"

SYMBOLS="WIN,WDO"
DEPTH=15
TOPN=5
SNAP=5
MIN_WARMUP=60
ZWIN=60
SCORE_TH=1.2
PERSIST=3
COOLDOWN=30
CKPT=5
FLUSH=5

START="20251215"
END="20260107"

# gera lista de datas (inclusive) sem depender do GNU date (mas ele normalmente existe no Ubuntu)
d="$START"
while [[ "$d" -le "$END" ]]; do
  in_file="${IN_DIR}/${d}_T.txt"
  out_file="${OUT_DIR}/${d}_t_5s.csv"

  if [[ ! -f "$in_file" ]]; then
    echo "[SKIP] Não existe: $in_file"
    d=$(date -d "${d:0:4}-${d:4:2}-${d:6:2} +1 day" +"%Y%m%d")
    continue
  fi

  echo "[RUN] $d  input=$in_file  output=$out_file"

  # limpa offset antigo desse dia pra não confundir (mesmo usando --reset-state)
  rm -f "${STATE_DIR}/${d}_T.offset" 2>/dev/null || true

  "$PARSER" \
    --input "$in_file" \
    --output "${OUT_DIR}/${d}_t_1s.csv" \
    --symbols "$SYMBOLS" \
    --bar-sec 1  \


  "$PARSER" \
    --input "$in_file" \
    --output "${OUT_DIR}/${d}_t_5s.csv" \
    --symbols "$SYMBOLS" \
    --bar-sec 5  \


  "$PARSER" \
    --input "$in_file" \
    --output "${OUT_DIR}/${d}_t_10s.csv" \
    --symbols "$SYMBOLS" \
    --bar-sec 10  \


  "$PARSER" \
    --input "$in_file" \
    --output "${OUT_DIR}/${d}_t_30s.csv" \
    --symbols "$SYMBOLS" \
    --bar-sec 30  \


  "$PARSER" \
    --input "$in_file" \
    --output "${OUT_DIR}/${d}_t_60s.csv" \
    --symbols "$SYMBOLS" \
    --bar-sec 60  \


  "$PARSER" \
    --input "$in_file" \
    --output "${OUT_DIR}/${d}_t_120s.csv" \
    --symbols "$SYMBOLS" \
    --bar-sec 120  \

      "$PARSER" \
    --input "$in_file" \
    --output "${OUT_DIR}/${d}_t_300s.csv" \
    --symbols "$SYMBOLS" \
    --bar-sec 300  \

          "$PARSER" \
    --input "$in_file" \
    --output "${OUT_DIR}/${d}_t_600s.csv" \
    --symbols "$SYMBOLS" \
    --bar-sec 600  \

      "$PARSER" \
    --input "$in_file" \
    --output "${OUT_DIR}/${d}_t_900s.csv" \
    --symbols "$SYMBOLS" \
    --bar-sec 900  \






  d=$(date -d "${d:0:4}-${d:4:2}-${d:6:2} +1 day" +"%Y%m%d")
done

echo "OK. Finalizado."

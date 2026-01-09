#!/usr/bin/env bash
set -euo pipefail

PARSER="./parser_B"
IN_DIR="/home/grao/dados/cedro_files"
OUT_DIR="/home/grao/dados/b"
STATE_DIR="/home/grao/dados/state"

SYMBOLS="WING26,WDOF26"
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

START="20251216"
END="20260106"

# gera lista de datas (inclusive) sem depender do GNU date (mas ele normalmente existe no Ubuntu)
d="$START"
while [[ "$d" -le "$END" ]]; do
  in_file="${IN_DIR}/${d}_B.txt"
  out_file="${OUT_DIR}/${d}_b_5s.csv"

  if [[ ! -f "$in_file" ]]; then
    echo "[SKIP] Não existe: $in_file"
    d=$(date -d "${d:0:4}-${d:4:2}-${d:6:2} +1 day" +"%Y%m%d")
    continue
  fi

  echo "[RUN] $d  input=$in_file  output=$out_file"

  # limpa offset antigo desse dia pra não confundir (mesmo usando --reset-state)
  rm -f "${STATE_DIR}/${d}_B.offset" 2>/dev/null || true

  "$PARSER" \
    --file "$in_file" \
    --out "$out_file" \

  d=$(date -d "${d:0:4}-${d:4:2}-${d:6:2} +1 day" +"%Y%m%d")
done

echo "OK. Finalizado."

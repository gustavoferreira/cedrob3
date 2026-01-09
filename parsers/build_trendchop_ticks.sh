#!/usr/bin/env bash
set -euo pipefail

SCRIPT="/home/grao/cedrob3/parsers/build_trendchop_ticks.py"   # ajuste se estiver em outro caminho
OUT_DIR="/home/grao/dados/media/chope"
SYMBOLS="WING26,WDOF26"

START="20251216"
END="$(date +%Y%m%d)"   # hoje

mkdir -p "$OUT_DIR"

d="$START"
while [[ "$d" -le "$END" ]]; do
  echo "=== $d ==="
  python3 "$SCRIPT" --date "$d" --symbols "$SYMBOLS" || true
  d="$(date -d "${d:0:4}-${d:4:2}-${d:6:2} +1 day" +%Y%m%d)"
done

echo "DONE. Saídas em: $OUT_DIR"


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os


def split_line(raw):
    """
    Espera:
      YYYYMMDD_HHMMSS,<x>,<y>,<payload>

    Retorna (day, payload) ou None.
    """
    raw = raw.rstrip("\n")
    if not raw:
        return None

    parts = raw.split(",", 3)
    if len(parts) < 4:
        return None

    ts = parts[0].strip()      # ex: 20251216_183113
    payload = parts[3].strip() # ex: Z:...

    if "_" not in ts or len(ts) < 8:
        return None

    day = ts.split("_", 1)[0]  # ex: 20251216
    return (day, payload)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="arquivo de log bruto do dia")
    ap.add_argument("--out", dest="out", default=None,
                    help="arquivo de saída (se não passar, usa YYYYMMDD_Z.txt)")
    args = ap.parse_args()

    out_path = None
    out_f = None

    total = 0
    matched = 0

    try:
        with open(args.inp, "r", encoding="utf-8", errors="ignore") as fi:
            for line in fi:
                total += 1
                parsed = split_line(line)
                if parsed is None:
                    continue

                day, payload = parsed

                if not payload.startswith("Z:"):
                    continue

                if out_f is None:
                    out_path = args.out if args.out else (day + "_Z.txt")
                    out_f = open(out_path, "w", encoding="utf-8", buffering=1024 * 1024)

                out_f.write(line)
                matched += 1

    finally:
        if out_f is not None:
            out_f.close()

    print("Linhas lidas:", total)
    print("Linhas SAB (Z:) extraídas:", matched)
    if out_path:
        print("Saída:", os.path.abspath(out_path))
    else:
        print("Nenhuma linha Z: encontrada; nada foi gerado.")


if __name__ == "__main__":
    main()


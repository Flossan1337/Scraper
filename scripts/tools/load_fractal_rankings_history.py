#!/usr/bin/env python3
"""
load_fractal_rankings_history.py

Backfills fractal_rankings from data/fractal_rankings.xlsx directly
(append-only running log, same shape as plejd_sensortower_rankings). The
xlsx is wide (one column per product); this melts it to one row per
(snapshot_date, product), skipping "NA"/missing cells (product not found
on the ranked pages that day) rather than inventing a rank. Safe to
re-run: inserts use ON CONFLICT DO NOTHING.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core`

from core.db import insert_rows, get_connection

REPO_ROOT = SCRIPTS_DIR.parent
XLSX_PATH = REPO_ROOT / "data" / "fractal_rankings.xlsx"


def main():
    df = pd.read_excel(XLSX_PATH, sheet_name="Sheet1")
    products = [c for c in df.columns if c != "Date"]

    rows = []
    for _, row in df.iterrows():
        snapshot_date = str(row["Date"])
        for product in products:
            rank = row.get(product)
            if pd.isna(rank):
                continue
            rows.append((snapshot_date, product, int(rank)))

    print(f"Read {len(df)} rows from {XLSX_PATH}, {len(rows)} (date, product) rank observations")

    n_inserted = insert_rows(
        "fractal_rankings",
        ["snapshot_date", "product", "rank"],
        rows,
        conflict_columns=["snapshot_date", "product"],
    )
    print(f"fractal_rankings: {n_inserted} rows inserted")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*), count(DISTINCT snapshot_date), count(DISTINCT product),
                       min(snapshot_date), max(snapshot_date)
                FROM fractal_rankings;
            """)
            total, dates, prods, earliest, latest = cur.fetchone()
        print(f"Rows in table:      {total}")
        print(f"Distinct dates:     {dates}")
        print(f"Distinct products:  {prods}")
        print(f"Date range:         {earliest} .. {latest}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

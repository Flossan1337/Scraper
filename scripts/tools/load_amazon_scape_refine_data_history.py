#!/usr/bin/env python3
"""
load_amazon_scape_refine_data_history.py

Backfills amazon_scape_refine_data from data/fractal_scape_refine_data.xlsx
directly (append-only running log). The xlsx is wide (2 columns per
product-country pair: "<Product> <CC> Bought" / "<Product> <CC> Rank");
this melts it to one row per (snapshot_date, product, country). Safe to
re-run: inserts use ON CONFLICT DO NOTHING, which also resolves the one
known same-day double-run (2025-11-30) by keeping the first row.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core`

from core.db import insert_rows, get_connection

REPO_ROOT = SCRIPTS_DIR.parent
XLSX_PATH = REPO_ROOT / "data" / "fractal_scape_refine_data.xlsx"

COUNTRIES = ["US", "DE"]
PRODUCTS = [
    "Scape Dark", "Scape Light",
    "Refine Mesh Light", "Refine Fabric Light",
    "Refine Mesh Dark", "Refine Fabric Dark",
]


def main():
    df = pd.read_excel(XLSX_PATH, sheet_name="Tracking")

    rows = []
    for _, row in df.iterrows():
        snapshot_date = str(row["Date"])
        for product in PRODUCTS:
            for country in COUNTRIES:
                bought_col = f"{product} {country} Bought"
                rank_col = f"{product} {country} Rank"
                if bought_col not in row or rank_col not in row:
                    continue
                bought = row[bought_col]
                rank = row[rank_col]
                if pd.isna(bought) and pd.isna(rank):
                    continue
                rows.append((
                    snapshot_date,
                    product,
                    country,
                    None if pd.isna(bought) else int(bought),
                    None if pd.isna(rank) else int(rank),
                ))

    print(f"Read {len(df)} rows from {XLSX_PATH}, {len(rows)} (date, product, country) observations")

    n_inserted = insert_rows(
        "amazon_scape_refine_data",
        ["snapshot_date", "product", "country", "bought_past_month", "best_sellers_rank"],
        rows,
        conflict_columns=["snapshot_date", "product", "country"],
    )
    print(f"amazon_scape_refine_data: {n_inserted} rows inserted")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*), count(DISTINCT snapshot_date), count(DISTINCT product),
                       count(DISTINCT country), min(snapshot_date), max(snapshot_date)
                FROM amazon_scape_refine_data;
            """)
            total, dates, prods, countries, earliest, latest = cur.fetchone()
        print(f"Rows in table:      {total}")
        print(f"Distinct dates:     {dates}")
        print(f"Distinct products:  {prods}")
        print(f"Distinct countries: {countries}")
        print(f"Date range:         {earliest} .. {latest}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

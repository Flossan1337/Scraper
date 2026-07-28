#!/usr/bin/env python3
"""
load_anoto_amazon_data_history.py

Backfills anoto_amazon_data from data/anoto_amazon_data.xlsx directly
(append-only running log, same shape as kpi_history). Safe to re-run:
inserts use ON CONFLICT DO NOTHING.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core`

from core.db import insert_rows, get_connection

REPO_ROOT = SCRIPTS_DIR.parent
XLSX_PATH = REPO_ROOT / "data" / "anoto_amazon_data.xlsx"


def main():
    df = pd.read_excel(XLSX_PATH, sheet_name="Tracking")

    rows = [
        (str(row["Date"]), int(row["Bought Past Month"]), int(row["Best Sellers Rank"]))
        for _, row in df.iterrows()
    ]
    distinct_dates = len(set(r[0] for r in rows))
    print(f"Read {len(rows)} rows from {XLSX_PATH} ({distinct_dates} distinct dates)")

    n_inserted = insert_rows(
        "anoto_amazon_data",
        ["snapshot_date", "bought_past_month", "best_sellers_rank"],
        rows,
        conflict_columns=["snapshot_date"],
    )
    print(f"anoto_amazon_data: {n_inserted} rows inserted")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*), min(snapshot_date), max(snapshot_date)
                FROM anoto_amazon_data;
            """)
            total, earliest, latest = cur.fetchone()
        print(f"Rows in table: {total}")
        print(f"Date range:    {earliest} .. {latest}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

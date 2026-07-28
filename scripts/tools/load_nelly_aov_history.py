#!/usr/bin/env python3
"""
load_nelly_aov_history.py

Backfills nelly_aov from data/nelly_aov.xlsx directly (append-only running
log, same shape as rugvista_bestseller_prices). Safe to re-run: inserts
use ON CONFLICT DO NOTHING.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core`

from core.db import insert_rows, get_connection

REPO_ROOT = SCRIPTS_DIR.parent
XLSX_PATH = REPO_ROOT / "data" / "nelly_aov.xlsx"


def main():
    df = pd.read_excel(XLSX_PATH, sheet_name="Sheet1")

    rows = [
        (str(row["date"]), float(row["median_price"]), float(row["average_price"]))
        for _, row in df.iterrows()
    ]
    distinct_dates = len(set(r[0] for r in rows))
    print(f"Read {len(rows)} rows from {XLSX_PATH} ({distinct_dates} distinct dates)")

    n_inserted = insert_rows(
        "nelly_aov",
        ["snapshot_date", "median_price", "average_price"],
        rows,
        conflict_columns=["snapshot_date"],
    )
    print(f"nelly_aov: {n_inserted} rows inserted")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*), min(snapshot_date), max(snapshot_date)
                FROM nelly_aov;
            """)
            total, earliest, latest = cur.fetchone()
        print(f"Rows in table: {total}")
        print(f"Date range:    {earliest} .. {latest}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
load_rugvista_bestsellers_history.py

Backfills rugvista_bestseller_prices from data/rugvista_bestsellers.xlsx
directly. Like kpi_history, this doesn't need raw/ git-extracted snapshots:
the xlsx is itself an append-only running log (track_rugvista_bestsellers.py
only ever appends), so the current file already IS the full history. Safe
to re-run: inserts use ON CONFLICT DO NOTHING.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core`

from core.db import insert_rows, get_connection

REPO_ROOT = SCRIPTS_DIR.parent
XLSX_PATH = REPO_ROOT / "data" / "rugvista_bestsellers.xlsx"


def main():
    df = pd.read_excel(XLSX_PATH, sheet_name="Sheet1")

    rows = [
        (str(row["date"]), float(row["median_price"]), float(row["average_price"]))
        for _, row in df.iterrows()
    ]
    distinct_dates = len(set(r[0] for r in rows))
    print(f"Read {len(rows)} rows from {XLSX_PATH} ({distinct_dates} distinct dates)")

    n_inserted = insert_rows(
        "rugvista_bestseller_prices",
        ["snapshot_date", "median_price", "average_price"],
        rows,
        conflict_columns=["snapshot_date"],
    )
    print(f"rugvista_bestseller_prices: {n_inserted} rows inserted")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*), min(snapshot_date), max(snapshot_date)
                FROM rugvista_bestseller_prices;
            """)
            total, earliest, latest = cur.fetchone()
        print(f"Rows in table: {total}")
        print(f"Date range:    {earliest} .. {latest}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

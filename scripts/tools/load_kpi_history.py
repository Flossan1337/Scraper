#!/usr/bin/env python3
"""
load_kpi_history.py

Backfills kpi_history from data/kpi-history.xlsx directly. Unlike the
Rugvista/Ahlsell loaders, this doesn't need raw/ git-extracted snapshots:
kpi-history.xlsx is itself an append-only running log (fetch_kpi.py appends
one row per day, never overwrites), so the current file already IS the full
history. Safe to re-run: inserts use ON CONFLICT DO NOTHING, which also
collapses the known duplicate rows for 2025-10-03/2025-10-04
(see KNOWN_ISSUES.md #2) down to one row each.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core`

from core.db import insert_rows, get_connection

REPO_ROOT = SCRIPTS_DIR.parent
XLSX_PATH = REPO_ROOT / "data" / "kpi-history.xlsx"


def main():
    df = pd.read_excel(XLSX_PATH, sheet_name="kpi-history")

    rows = [
        (
            datetime.strptime(str(row["Date"]), "%Y-%m-%d").date().isoformat(),
            int(row["Konverteringar"]),
            int(row["Varumärken"]),
        )
        for _, row in df.iterrows()
    ]
    distinct_dates = len(set(r[0] for r in rows))
    print(f"Read {len(rows)} rows from {XLSX_PATH} ({distinct_dates} distinct dates)")

    n_inserted = insert_rows(
        "kpi_history",
        ["snapshot_date", "conversions", "brands"],
        rows,
        conflict_columns=["snapshot_date"],
    )
    print(f"kpi_history: {n_inserted} rows inserted")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*), min(snapshot_date), max(snapshot_date)
                FROM kpi_history;
            """)
            total, earliest, latest = cur.fetchone()
        print(f"Rows in table: {total}")
        print(f"Date range:    {earliest} .. {latest}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
load_plejd_sensortower_rankings_history.py

Backfills plejd_sensortower_rankings from data/plejd_sensortower_rankings.xlsx
directly (append-only running log, same shape as kpi_history). The xlsx is
wide (one column per country); this melts it to one row per
(snapshot_date, country), skipping cells with no rank (app unranked that day
in that country) rather than inventing a zero. Safe to re-run: inserts use
ON CONFLICT DO NOTHING.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core`

from core.db import insert_rows, get_connection

REPO_ROOT = SCRIPTS_DIR.parent
XLSX_PATH = REPO_ROOT / "data" / "plejd_sensortower_rankings.xlsx"
SHEET_NAME = "category_rankings"
COUNTRIES = ["SE", "NO", "FI", "NL", "DE", "DK", "ES"]


def main():
    df = pd.read_excel(XLSX_PATH, sheet_name=SHEET_NAME)

    rows = []
    for _, row in df.iterrows():
        snapshot_date = str(row["Date"])
        for country in COUNTRIES:
            rank = row.get(country)
            if pd.isna(rank):
                continue
            rows.append((snapshot_date, country, int(rank)))

    distinct_dates = len(set(r[0] for r in rows))
    print(f"Read {len(df)} rows ({distinct_dates} distinct dates) from {XLSX_PATH}, "
          f"{len(rows)} (date, country) rank observations")

    n_inserted = insert_rows(
        "plejd_sensortower_rankings",
        ["snapshot_date", "country", "rank"],
        rows,
        conflict_columns=["snapshot_date", "country"],
    )
    print(f"plejd_sensortower_rankings: {n_inserted} rows inserted")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*), count(DISTINCT snapshot_date), count(DISTINCT country),
                       min(snapshot_date), max(snapshot_date)
                FROM plejd_sensortower_rankings;
            """)
            total, dates, countries, earliest, latest = cur.fetchone()
        print(f"Rows in table:     {total}")
        print(f"Distinct dates:    {dates}")
        print(f"Distinct countries: {countries}")
        print(f"Date range:        {earliest} .. {latest}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

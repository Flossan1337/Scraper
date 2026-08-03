#!/usr/bin/env python3
"""
load_google_trends_history.py

One-off backfill for google_trends_monthly from the current committed xlsx
of each of the 8 Google Trends fetchers. Unlike the daily-snapshot pipelines,
each of these scripts rewrites its entire monthly series from FETCH_START on
every manual run - so the current xlsx already IS the full history, there's
no need for raw/ git-archaeology here.

Uses upsert_rows (latest-known-state), not insert_rows - safe to re-run.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import openpyxl

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core`

from core.db import upsert_rows, get_connection

REPO_ROOT = SCRIPTS_DIR.parent
DATA_DIR = REPO_ROOT / "data"

# (pipeline name, xlsx filename, [(sheet_name, sheet_label), ...])
# sheet_label becomes the "sheet" column - 'default' for single-sheet scripts,
# the real sheet name where a script's output distinguishes two scalings of
# the same terms (fetch_pierce_trends.py's "together"/"separate").
PIPELINES = [
    ("cheffelo", "cheffelo_trends_monthly.xlsx", [("Sheet1", "default")]),
    ("fractal", "fractal_trends_monthly.xlsx", [("fractal_trends_monthly", "default")]),
    ("nelly", "nelly_trends_monthly.xlsx", [("Sheet1", "default")]),
    ("pierce", "pierce_trends_monthly.xlsx", [("together", "together"), ("separate", "separate")]),
    ("plejd", "plejd_trends_monthly.xlsx", [("Sheet1", "default")]),
    ("plejd_vs_electrician", "plejd_vs_electrician_trends.xlsx", [("Sheet1", "default")]),
    ("revolutionrace", "revolutionrace_trends_monthly.xlsx", [("Sheet1", "default")]),
    ("rugvista", "rugvista_trends_monthly.xlsx", [("Sheet1", "default")]),
]


def load_sheet_rows(pipeline: str, sheet_label: str, path: Path, sheet_name: str, fetched_at: str) -> list[tuple]:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb[sheet_name]
    rows_iter = ws.iter_rows(values_only=True)
    header = next(rows_iter)
    date_col = header.index("Date")
    series_cols = [(i, name) for i, name in enumerate(header) if name != "Date" and name is not None]

    out = []
    for row in rows_iter:
        date_val = row[date_col]
        if date_val is None:
            continue
        month = date_val.date() if hasattr(date_val, "date") else date_val
        for i, series in series_cols:
            value = row[i]
            if value is None:
                continue
            out.append((pipeline, sheet_label, series, month, value, fetched_at))
    wb.close()
    return out


def main():
    fetched_at = datetime.now(timezone.utc).isoformat()
    total = 0
    for pipeline, filename, sheets in PIPELINES:
        path = DATA_DIR / filename
        if not path.exists():
            print(f"{pipeline}: SKIPPED - {path} not found")
            continue
        rows: list[tuple] = []
        for sheet_name, sheet_label in sheets:
            rows.extend(load_sheet_rows(pipeline, sheet_label, path, sheet_name, fetched_at))

        n = upsert_rows(
            "google_trends_monthly",
            ["pipeline", "sheet", "series", "month", "value", "fetched_at"],
            rows,
            conflict_columns=["pipeline", "sheet", "series", "month"],
        )
        print(f"{pipeline}: {len(rows)} rows read, {n} upserted")
        total += n

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*), min(month), max(month) FROM google_trends_monthly;")
            count, earliest, latest = cur.fetchone()
        print(f"\nTotal rows in table: {count}")
        print(f"Month range: {earliest} .. {latest}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

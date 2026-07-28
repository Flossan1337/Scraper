#!/usr/bin/env python3
"""
load_rvrc_sales_history.py

Backfills rvrc_sales_daily_summary from the single latest file in
raw/rvrc_sales_state/. Like Anoto/Neo, "daily_summaries" is already fully
cumulative (137 dates present in just the newest file, verified) - no
chronological merge across files needed.

Does NOT backfill rvrc_variant_snapshot: that per-product-colour
granularity was never persisted in the state file (only the aggregated
daily summary was) and the Excel "Latest Detail" sheet is overwritten
every run rather than appended, so there is no retrievable history for it.
That table only starts filling from the day track_rvrc_sales.py began
writing to it live.

Safe to re-run: inserts use ON CONFLICT DO NOTHING.
"""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core`

from core.db import insert_rows, get_connection

REPO_ROOT = SCRIPTS_DIR.parent
RAW_DIR = REPO_ROOT / "raw" / "rvrc_sales_state"


def main():
    import json

    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        raise SystemExit(f"No snapshot files found in {RAW_DIR}")
    latest = files[-1]
    data = json.loads(latest.read_text(encoding="utf-8"))
    summaries = data.get("daily_summaries", [])
    print(f"{latest.name}: {len(summaries)} daily summaries")

    rows = []
    for entry in summaries:
        fx = entry.get("fx_rates", {})
        rows.append((
            entry["date"],
            entry.get("slw_x_sell_eur"),
            entry.get("sld_x_sell_eur"),
            entry.get("slw_x_list_eur"),
            entry.get("sld_x_list_eur"),
            entry.get("product_colors_total"),
            entry.get("product_colors_with_sales"),
            fx.get("EUR"),
            fx.get("NOK"),
            fx.get("GBP"),
        ))

    n_inserted = insert_rows(
        "rvrc_sales_daily_summary",
        ["snapshot_date", "slw_x_sell_eur", "sld_x_sell_eur", "slw_x_list_eur",
         "sld_x_list_eur", "product_colors_total", "product_colors_with_sales",
         "fx_eur", "fx_nok", "fx_gbp"],
        rows,
        conflict_columns=["snapshot_date"],
    )
    print(f"rvrc_sales_daily_summary: {n_inserted} rows inserted")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*), min(snapshot_date), max(snapshot_date)
                FROM rvrc_sales_daily_summary;
            """)
            total, earliest, latest_date = cur.fetchone()
        print(f"Rows in table: {total}")
        print(f"Date range:    {earliest} .. {latest_date}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

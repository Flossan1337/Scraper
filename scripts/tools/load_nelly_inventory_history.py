#!/usr/bin/env python3
"""
load_nelly_inventory_history.py

Backfills nelly_daily_summary from the single latest file in
raw/nelly_inventory_state/ - like RVRC/Anoto, "daily_summary" is already
fully cumulative (131 dates present in just the newest file, verified).

Does NOT backfill nelly_variant_snapshot: per-product-colour detail was
never persisted anywhere (computed fresh each run, no state entry, no
leftover Excel sheet either) - that table only starts filling from the
day track_nelly_inventory.py began writing to it live.

Safe to re-run: inserts use ON CONFLICT DO NOTHING.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from psycopg2.extras import Json

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core`

from core.db import insert_rows, get_connection

REPO_ROOT = SCRIPTS_DIR.parent
RAW_DIR = REPO_ROOT / "raw" / "nelly_inventory_state"


def main():
    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        raise SystemExit(f"No snapshot files found in {RAW_DIR}")
    latest = files[-1]
    data = json.loads(latest.read_text(encoding="utf-8"))
    entries = data.get("daily_summary", [])
    print(f"{latest.name}: {len(entries)} daily summaries")

    rows = []
    for entry in entries:
        s = entry.get("summary", {})
        rows.append((
            entry["date"],
            s.get("total_products"),
            s.get("est_sold_today_units"),
            s.get("est_sold_today_sek"),
            s.get("est_sold_today_list_sek"),
            s.get("restocks"),
            s.get("returns"),
            Json(s.get("by_category") or {}),
            Json(s.get("by_brand") or {}),
            Json(s.get("by_site") or {}),
            Json(s.get("restock_events") or []),
            Json(s.get("return_events") or []),
        ))

    n_inserted = insert_rows(
        "nelly_daily_summary",
        ["snapshot_date", "total_products", "est_sold_today_units",
         "est_sold_today_sek", "est_sold_today_list_sek", "restocks", "returns",
         "by_category", "by_brand", "by_site", "restock_events", "return_events"],
        rows,
        conflict_columns=["snapshot_date"],
    )
    print(f"nelly_daily_summary: {n_inserted} rows inserted")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*), min(snapshot_date), max(snapshot_date)
                FROM nelly_daily_summary;
            """)
            total, earliest, latest_date = cur.fetchone()
        print(f"Rows in table: {total}")
        print(f"Date range:    {earliest} .. {latest_date}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

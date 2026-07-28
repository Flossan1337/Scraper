#!/usr/bin/env python3
"""
load_anoto_inventory_history.py

Backfills anoto_variant_snapshot from raw/anoto_inventory_state/ and
raw/neo_inventory_state/. Unlike Ahlsell, each state file's "daily_summary"
list is already fully cumulative (every day ever recorded, never pruned),
so only the LATEST raw file per store is needed - not a chronological
merge across all of them. Verified: the latest anoto file has all 94
distinct dates from day 1, the latest neo file has all 33.

Safe to re-run: inserts use ON CONFLICT DO NOTHING.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core`

from core.db import insert_rows, get_connection

REPO_ROOT = SCRIPTS_DIR.parent

STORES = [
    ("anoto", REPO_ROOT / "raw" / "anoto_inventory_state"),
    ("neo", REPO_ROOT / "raw" / "neo_inventory_state"),
]


def rows_for_store(store: str, raw_dir: Path) -> list[tuple]:
    files = sorted(raw_dir.glob("*.json"))
    if not files:
        print(f"  [{store}] no raw files found in {raw_dir}")
        return []
    latest = files[-1]
    data = json.loads(latest.read_text(encoding="utf-8"))

    rows = []
    for entry in data.get("daily_summary", []):
        snapshot_date = entry["date"]
        for row in entry.get("detail_rows", []):
            rows.append((
                snapshot_date,
                store,
                row["variant_id"],
                row.get("product_title"),
                row.get("variant_title"),
                row.get("sku"),
                row.get("price"),
                row.get("currency"),
                row.get("stock_curr"),
            ))
    print(f"  [{store}] {latest.name}: {len(data.get('daily_summary', []))} dates, {len(rows)} variant-day rows")
    return rows


def main():
    all_rows = []
    for store, raw_dir in STORES:
        all_rows.extend(rows_for_store(store, raw_dir))

    n_inserted = insert_rows(
        "anoto_variant_snapshot",
        ["snapshot_date", "store", "variant_id", "product_title", "variant_title",
         "sku", "price", "currency", "quantity"],
        all_rows,
        conflict_columns=["snapshot_date", "store", "variant_id"],
    )
    print(f"anoto_variant_snapshot: {n_inserted} rows inserted")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT store, count(*), count(DISTINCT snapshot_date), count(DISTINCT variant_id),
                       min(snapshot_date), max(snapshot_date)
                FROM anoto_variant_snapshot
                GROUP BY store
                ORDER BY store;
            """)
            for store, total, dates, variants, earliest, latest in cur.fetchall():
                print(f"  {store}: {total} rows, {dates} dates, {variants} variants, {earliest} .. {latest}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

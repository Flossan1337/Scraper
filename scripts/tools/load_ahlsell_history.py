#!/usr/bin/env python3
"""
load_ahlsell_history.py

Loads every extracted historical snapshot in raw/ahlsell_plejd_state/*.json into
ahlsell_article / ahlsell_warehouse / ahlsell_stock_snapshot in Postgres.

Each raw file's "snapshots" dict is itself cumulative (the live script never
prunes old dates), and "products"/"warehouses" are replaced wholesale on every
run, so this merges across ALL files chronologically (later files win on
conflicting metadata) rather than reading just the latest one, to avoid losing
articles/warehouses that dropped out of the catalog before the most recent run.

Safe to re-run: inserts use ON CONFLICT DO NOTHING.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core` and `track_ahlsell_plejd_inventory`

from core.db import insert_rows, get_connection
from track_ahlsell_plejd_inventory import categorize

REPO_ROOT = SCRIPTS_DIR.parent
RAW_DIR = REPO_ROOT / "raw" / "ahlsell_plejd_state"


def merge_history() -> tuple[dict, dict, dict]:
    products: dict = {}
    warehouses: dict = {}
    snapshots: dict = {}
    for path in sorted(RAW_DIR.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        products.update(data.get("products", {}))
        warehouses.update(data.get("warehouses", {}))
        snapshots.update(data.get("snapshots", {}))
    return products, warehouses, snapshots


def main():
    products, warehouses, snapshots = merge_history()
    print(f"Merged: {len(products)} articles, {len(warehouses)} warehouses, {len(snapshots)} snapshot dates")

    article_rows = [
        (art, meta.get("product_name"), meta.get("product_code"), meta.get("page_url"),
         categorize(art, meta.get("product_name", "")))
        for art, meta in products.items()
    ]
    warehouse_rows = [
        (wid, meta.get("name"), meta.get("city"), meta.get("address"))
        for wid, meta in warehouses.items()
    ]
    stock_rows = [
        (snapshot_date, art, wid, qty)
        for snapshot_date, arts in snapshots.items()
        for art, entry in arts.items()
        for wid, qty in entry.get("warehouses", {}).items()
    ]

    n_articles = insert_rows(
        "ahlsell_article",
        ["article", "product_name", "product_code", "page_url", "category"],
        article_rows, conflict_columns=["article"],
    )
    n_warehouses = insert_rows(
        "ahlsell_warehouse",
        ["warehouse_id", "name", "city", "address"],
        warehouse_rows, conflict_columns=["warehouse_id"],
    )
    n_stock = insert_rows(
        "ahlsell_stock_snapshot",
        ["snapshot_date", "article", "warehouse_id", "quantity"],
        stock_rows, conflict_columns=["snapshot_date", "article", "warehouse_id"],
    )

    print(f"ahlsell_article:        {n_articles} rows inserted")
    print(f"ahlsell_warehouse:      {n_warehouses} rows inserted")
    print(f"ahlsell_stock_snapshot: {n_stock} rows inserted")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*), count(DISTINCT snapshot_date), count(DISTINCT article),
                       min(snapshot_date), max(snapshot_date)
                FROM ahlsell_stock_snapshot;
            """)
            total, dates, articles, earliest, latest = cur.fetchone()
        print(f"Rows in table:       {total}")
        print(f"Distinct dates:      {dates}")
        print(f"Distinct articles:   {articles}")
        print(f"Earliest date:       {earliest}")
        print(f"Latest date:         {latest}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

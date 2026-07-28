#!/usr/bin/env python3
"""
load_ahlsell_led_panel_history.py

Loads every extracted historical snapshot in raw/ahlsell_led_panel_state/*.json
into ahlsell_led_panel_article / ahlsell_led_panel_stock_snapshot in Postgres.

Same shape as load_ahlsell_history.py: each raw file's "products" dict is
replaced wholesale on every run rather than merged, so this merges across
ALL files chronologically (later files win on conflicting metadata) rather
than reading just the latest one, to avoid losing articles that dropped out
of the catalog before the most recent run.

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
RAW_DIR = REPO_ROOT / "raw" / "ahlsell_led_panel_state"


def merge_history() -> tuple[dict, dict]:
    products: dict = {}
    snapshots: dict = {}
    for path in sorted(RAW_DIR.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        products.update(data.get("products", {}))
        snapshots.update(data.get("snapshots", {}))
    return products, snapshots


def main():
    products, snapshots = merge_history()
    print(f"Merged: {len(products)} articles, {len(snapshots)} snapshot dates")

    article_rows = [
        (art, meta.get("product_name"), meta.get("product_code"),
         meta.get("page_url"), meta.get("brand"))
        for art, meta in products.items()
    ]
    stock_rows = [
        (snapshot_date, art, qty)
        for snapshot_date, snap in snapshots.items()
        for art, qty in snap.get("by_article", {}).items()
    ]

    n_articles = insert_rows(
        "ahlsell_led_panel_article",
        ["article", "product_name", "product_code", "page_url", "brand"],
        article_rows, conflict_columns=["article"],
    )
    n_stock = insert_rows(
        "ahlsell_led_panel_stock_snapshot",
        ["snapshot_date", "article", "quantity"],
        stock_rows, conflict_columns=["snapshot_date", "article"],
    )

    print(f"ahlsell_led_panel_article:        {n_articles} rows inserted")
    print(f"ahlsell_led_panel_stock_snapshot: {n_stock} rows inserted")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT count(*), count(DISTINCT snapshot_date), count(DISTINCT article),
                       min(snapshot_date), max(snapshot_date)
                FROM ahlsell_led_panel_stock_snapshot;
            """)
            total, dates, articles, earliest, latest = cur.fetchone()
        print(f"Rows in table:     {total}")
        print(f"Distinct dates:    {dates}")
        print(f"Distinct articles: {articles}")
        print(f"Date range:        {earliest} .. {latest}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

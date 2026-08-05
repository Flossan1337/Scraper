#!/usr/bin/env python3
"""
fix_ahlsell_encoding.py

One-off fix for KNOWN_ISSUES.md #9: ahlsell_led_panel_article.product_name
(48 rows) and ahlsell_warehouse.name/city/address (67/48/75 rows) were
originally backfilled from raw/ahlsell_led_panel_state and
raw/ahlsell_plejd_state while extract_state_history.py's git() helper
mis-decoded non-ASCII bytes as cp1252 on Windows. Both raw/ trees have
since been regenerated correctly (encoding="utf-8" fix) - this re-merges
each history chronologically (same approach as the original loaders) and
upserts, overwriting the corrupted text. Small tables (~1200 and ~130
rows respectively) - one batch call each, unlike the row-by-row approach
needed for the much larger rugvista_variant_snapshot fix.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core`

from core.db import upsert_rows, get_connection

REPO_ROOT = SCRIPTS_DIR.parent


def merge_products(raw_dir: Path) -> dict:
    products: dict = {}
    for path in sorted(raw_dir.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        products.update(data.get("products", {}))
    return products


def merge_warehouses(raw_dir: Path) -> dict:
    warehouses: dict = {}
    for path in sorted(raw_dir.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        warehouses.update(data.get("warehouses", {}))
    return warehouses


def main():
    led_panel_dir = REPO_ROOT / "raw" / "ahlsell_led_panel_state"
    plejd_dir = REPO_ROOT / "raw" / "ahlsell_plejd_state"

    led_products = merge_products(led_panel_dir)
    led_rows = [
        (art, meta.get("product_name"), meta.get("product_code"),
         meta.get("page_url"), meta.get("brand"))
        for art, meta in led_products.items()
    ]
    n_led = upsert_rows(
        "ahlsell_led_panel_article",
        ["article", "product_name", "product_code", "page_url", "brand"],
        led_rows, conflict_columns=["article"],
    )
    print(f"ahlsell_led_panel_article: {n_led} rows upserted")

    warehouses = merge_warehouses(plejd_dir)
    wh_rows = [
        (wid, meta.get("name"), meta.get("city"), meta.get("address"))
        for wid, meta in warehouses.items()
    ]
    n_wh = upsert_rows(
        "ahlsell_warehouse",
        ["warehouse_id", "name", "city", "address"],
        wh_rows, conflict_columns=["warehouse_id"],
    )
    print(f"ahlsell_warehouse: {n_wh} rows upserted")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM ahlsell_led_panel_article WHERE product_name LIKE '%Ã%';")
            print(f"Remaining mojibake in ahlsell_led_panel_article: {cur.fetchone()[0]}")
            cur.execute("""SELECT count(*) FROM ahlsell_warehouse
                            WHERE name LIKE '%Ã%' OR city LIKE '%Ã%' OR address LIKE '%Ã%';""")
            print(f"Remaining mojibake in ahlsell_warehouse: {cur.fetchone()[0]}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

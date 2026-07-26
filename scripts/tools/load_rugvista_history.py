#!/usr/bin/env python3
"""
load_rugvista_history.py

Loads every extracted historical snapshot in raw/rugvista_state/*.json into
the rugvista_variant_snapshot table in Postgres (one row per product variant
per captured snapshot). Safe to re-run: inserts use ON CONFLICT DO NOTHING
keyed on (captured_at, product_id).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = REPO_ROOT / "raw" / "rugvista_state"
BATCH_SIZE = 1000

load_dotenv(REPO_ROOT / ".env")
DATABASE_URL = os.environ["DATABASE_URL"]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS rugvista_variant_snapshot (
    captured_at   timestamptz NOT NULL,
    product_id    bigint      NOT NULL,
    sku           text,
    parent_name   text,
    variant_name  text,
    size_label    text,
    length_cm     int,
    width_cm      int,
    price_sek     numeric,
    available     int,
    PRIMARY KEY (captured_at, product_id)
);
"""

INSERT_SQL = """
INSERT INTO rugvista_variant_snapshot
    (captured_at, product_id, sku, parent_name, variant_name,
     size_label, length_cm, width_cm, price_sek, available, snapshot_date)
VALUES %s
ON CONFLICT (captured_at, product_id) DO NOTHING;
"""


def fallback_captured_at(filename_stem: str) -> str:
    """Midnight UTC on the snapshot file's date, used only if snapshot_time is missing."""
    return datetime.strptime(filename_stem, "%Y-%m-%d").replace(tzinfo=timezone.utc).isoformat()


def rows_from_file(path: Path) -> list[tuple]:
    data = json.loads(path.read_text(encoding="utf-8"))
    default_captured_at = fallback_captured_at(path.stem)
    rows = []
    for product_id, v in data.items():
        captured_at = v.get("snapshot_time") or default_captured_at
        snapshot_date = datetime.fromisoformat(captured_at).date().isoformat()
        rows.append((
            captured_at,
            int(product_id),
            v.get("sku"),
            v.get("parent_name"),
            v.get("variant_name"),
            v.get("size_label"),
            v.get("length_cm"),
            v.get("width_cm"),
            v.get("price_SEK"),
            v.get("available"),
            snapshot_date,
        ))
    return rows


def main():
    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        raise SystemExit(f"No snapshot files found in {RAW_DIR}")

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()

        for path in files:
            rows = rows_from_file(path)
            with conn.cursor() as cur:
                for i in range(0, len(rows), BATCH_SIZE):
                    execute_values(cur, INSERT_SQL, rows[i:i + BATCH_SIZE])
            conn.commit()

        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    count(*),
                    count(DISTINCT captured_at::date),
                    count(DISTINCT product_id),
                    min(captured_at),
                    max(captured_at)
                FROM rugvista_variant_snapshot;
            """)
            total, distinct_dates, distinct_products, earliest, latest = cur.fetchone()

        print(f"Rows in table:        {total}")
        print(f"Distinct dates:        {distinct_dates}")
        print(f"Distinct product_ids:  {distinct_products}")
        print(f"Earliest captured_at:  {earliest}")
        print(f"Latest captured_at:    {latest}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

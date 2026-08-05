#!/usr/bin/env python3
"""
fix_rugvista_encoding.py

One-off fix for KNOWN_ISSUES.md #9: rugvista_variant_snapshot was originally
backfilled from raw/rugvista_state/*.json while extract_state_history.py's
git() helper mis-decoded non-ASCII bytes as cp1252 on Windows, corrupting
variant_name/parent_name text (e.g. "Röd" -> "RÃ¶d"). raw/ has since been
regenerated with the fix (encoding="utf-8" on the subprocess call) - this
script re-reads the now-correct raw files and overwrites every row via
upsert (not ON CONFLICT DO NOTHING like load_rugvista_history.py, since the
whole point is to correct rows that already exist).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core`

from core.db import get_connection

REPO_ROOT = SCRIPTS_DIR.parent
RAW_DIR = REPO_ROOT / "raw" / "rugvista_state"

UPDATE_SQL = """
UPDATE rugvista_variant_snapshot
SET sku = %s, parent_name = %s, variant_name = %s, size_label = %s
WHERE snapshot_date = %s AND product_id = %s
  AND (sku IS DISTINCT FROM %s OR parent_name IS DISTINCT FROM %s
       OR variant_name IS DISTINCT FROM %s OR size_label IS DISTINCT FROM %s);
"""


def fallback_captured_at(filename_stem: str) -> str:
    return datetime.strptime(filename_stem, "%Y-%m-%d").replace(tzinfo=timezone.utc).isoformat()


def rows_from_file(path: Path) -> list[tuple]:
    """Match existing rows by (snapshot_date, product_id) - not captured_at,
    which can differ by a couple of seconds between the value baked into an
    already-loaded row and this file's snapshot_time for the same date/
    product (e.g. from an earlier manual gap-fill) - only the corrupted text
    columns need correcting, not the row identity."""
    data = json.loads(path.read_text(encoding="utf-8"))
    default_captured_at = fallback_captured_at(path.stem)
    rows = []
    for product_id, v in data.items():
        captured_at = v.get("snapshot_time") or default_captured_at
        snapshot_date = datetime.fromisoformat(captured_at).date().isoformat()
        sku = v.get("sku")
        parent_name = v.get("parent_name")
        variant_name = v.get("variant_name")
        size_label = v.get("size_label")
        rows.append((
            sku, parent_name, variant_name, size_label,
            snapshot_date, int(product_id),
            sku, parent_name, variant_name, size_label,
        ))
    return rows


def main():
    files = sorted(RAW_DIR.glob("*.json"))
    if not files:
        raise SystemExit(f"No snapshot files found in {RAW_DIR}")

    conn = get_connection()
    total = 0
    try:
        with conn.cursor() as cur:
            for path in files:
                rows = rows_from_file(path)
                for row in rows:
                    cur.execute(UPDATE_SQL, row)
                    total += cur.rowcount
                conn.commit()
    finally:
        conn.close()
    print(f"{total} rows updated across {len(files)} files")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM rugvista_variant_snapshot WHERE variant_name LIKE '%Ã%' OR parent_name LIKE '%Ã%';")
            print(f"Remaining mojibake rows: {cur.fetchone()[0]}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

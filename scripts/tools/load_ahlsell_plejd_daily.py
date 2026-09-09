#!/usr/bin/env python3
"""
load_ahlsell_plejd_daily.py

One-off backfill of ahlsell_plejd_daily from an externally-sourced SQLite
database (`ahlsell_sales.db`) that tracked Ahlsell's Plejd assortment from an
*authenticated* session between 2026-05-28 and 2026-09-09.

Why this exists: the anonymous Ahlsell API exposes only per-branch stock. The
central warehouse -- 62% of Plejd inventory, and the half that actually moved
over this period -- plus prices require a login. This database is the only
record of that for the pre-2026-09 window, so it cannot be regenerated.

Validated against our own independent branch-level history before loading:
80/80 articles identical, 76 of 102 overlapping days match on every article,
92.8% of article-days match exactly. The residual is fetch-time drift (his
capture hour varied; stock moves during business hours). See KNOWN_ISSUES.md.

Loads only rows whose brand matches --brand (default Plejd); the source
database also holds Svedbergs/Ferroamp/CTEK, which are out of scope here.

`fetchDate` in the source has no timezone. It is interpreted as
Europe/Stockholm -- the minute-scatter (03:51, 03:59, 04:12, 04:44) looks like
a local scheduled task rather than a UTC CI cron -- but this is an assumption,
not a fact; the uncertainty is at most 2 hours. See KNOWN_ISSUES.md.

Fails loudly (insert_rows, not safe_insert): a silently partial backfill is
worse than a crash. Safe to re-run -- ON CONFLICT DO NOTHING on
(snapshot_date, variant_number).

Usage (from repo root):
    python ./scripts/tools/load_ahlsell_plejd_daily.py
    python ./scripts/tools/load_ahlsell_plejd_daily.py --db path/to.db --dry-run
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))  # for `core`

from core.db import insert_rows, upsert_rows  # noqa: E402

REPO_ROOT = SCRIPTS_DIR.parent
DEFAULT_DB = SCRIPTS_DIR / "ahlsell_sales.db"
STOCKHOLM = ZoneInfo("Europe/Stockholm")

SOURCE = "backfill"  # distinguishes third-party rows from our own captures;
                     # price_eff is account-specific and only comparable within one source


def read_rows(db_path: Path, brand: str) -> list[tuple]:
    """Reads the source database. Returns rows ready for ahlsell_plejd_daily."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cur = conn.execute(
            """
            SELECT date, variant_number, qty_lager, qty_butik, qty_inleverans,
                   price_eff, price_gnp, fetchDate
              FROM inventory
             WHERE brand LIKE ?
             ORDER BY date, variant_number
            """,
            (f"%{brand}%",),
        )
        rows = []
        for d, vn, lager, butik, inlev, eff, gnp, fetched in cur:
            # 'YYYY-MM-DD HH:MM' -> tz-aware Europe/Stockholm
            fetched_at = None
            if fetched:
                from datetime import datetime
                fetched_at = datetime.strptime(fetched, "%Y-%m-%d %H:%M").replace(tzinfo=STOCKHOLM)
            rows.append((d, str(vn), lager, butik, inlev, eff, gnp, fetched_at, SOURCE))
        return rows
    finally:
        conn.close()


def read_article_metadata(db_path: Path, brand: str) -> list[tuple]:
    """Latest-known sku / Ahlsell category per article.

    Both change over time (22 Plejd variants were recategorised during the
    window), so this is latest-known-state via upsert, not a history.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cur = conn.execute(
            """
            SELECT variant_number, sku, category
              FROM inventory
             WHERE brand LIKE ?
               AND date = (SELECT MAX(date) FROM inventory WHERE brand LIKE ?)
            """,
            (f"%{brand}%", f"%{brand}%"),
        )
        return [(str(vn), sku, cat) for vn, sku, cat in cur]
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", type=Path, default=DEFAULT_DB, help="source SQLite database")
    ap.add_argument("--brand", default="Plejd", help="brand to load (default: Plejd)")
    ap.add_argument("--dry-run", action="store_true", help="read and report, write nothing")
    args = ap.parse_args()

    if not args.db.exists():
        sys.exit(f"Source database not found: {args.db}")

    print(f"=== Backfill ahlsell_plejd_daily from {args.db.name} (brand={args.brand}) ===\n")

    rows = read_rows(args.db, args.brand)
    meta = read_article_metadata(args.db, args.brand)
    if not rows:
        sys.exit(f"No rows found for brand matching '{args.brand}'.")

    dates = sorted({r[0] for r in rows})
    print(f"  {len(rows):,} snapshot rows")
    print(f"  {len({r[1] for r in rows})} articles, {len(dates)} dates "
          f"({dates[0]} .. {dates[-1]})")
    print(f"  {len(meta)} articles with sku / category metadata")

    if args.dry_run:
        print("\n--dry-run: nothing written.")
        return

    n_meta = upsert_rows(
        table="ahlsell_article",
        columns=["article", "sku", "ahlsell_category"],
        rows=meta,
        conflict_columns=["article"],
    )
    print(f"\n  ahlsell_article    : {n_meta} rows upserted (sku / ahlsell_category)")

    n_daily = insert_rows(
        table="ahlsell_plejd_daily",
        columns=["snapshot_date", "variant_number", "qty_lager", "qty_butik",
                 "qty_inleverans", "price_eff", "price_gnp", "fetched_at", "source"],
        rows=rows,
        conflict_columns=["snapshot_date", "variant_number"],
    )
    print(f"  ahlsell_plejd_daily: {n_daily:,} rows inserted "
          f"({len(rows) - n_daily:,} already present, skipped)")
    print("\nKlart!")


if __name__ == "__main__":
    main()

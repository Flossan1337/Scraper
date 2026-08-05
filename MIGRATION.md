# Migration: Excel → Postgres

Status and plan for moving this repo's source of truth from `data/*.xlsx`
files to Postgres (Supabase). Written 2026-07-28. For open correctness
questions about specific pipelines, see `KNOWN_ISSUES.md` — this document
doesn't repeat them, only points at them.

## Next steps (as of 2026-08-05)

Raw capture is done for every active, correctly-shaped pipeline, including
all 8 Google Trends fetchers and TED lot-detail (added 2026-08-03). Tier 1
and both Tier 2/3 aggregate tables (`rvrc_sales_daily_summary`,
`nelly_daily_summary`) are fully validated against xlsx. Four views are
built and validated (`rugvista_daily_sales_v`, `ahlsell_plejd_sales_v`,
`ahlsell_led_panel_brand_stock_v`, `anoto_daily_sales_v`).

**Power Query repointing is fully done — all 22 queries, verified working
in the live `DATA_DASHBOARD.xlsx` (2026-08-05).** Batch 4 (the last 9 —
8 Google Trends tabs + `EQL Pharma AB Detail`) followed the same
`google_trends_monthly`/`ted_lot_tender` shape as the raw tables: the 8
Trends tabs pivot on `series` after filtering `pipeline`(+`sheet`) — same
`Table.Pivot`-after-`Table.SelectRows` pattern as `By Brand`/`By Category`
in batch 3 — and `EQL Pharma AB Detail` is a straight column select/rename
off `ted_lot_tender` filtered to that company. `DATA_DASHBOARD.xlsx` now
has zero local `.xlsx` dependencies for its Power Query sources — only the
one Postgres connection (confirmed via Data Source Settings, modulo a
stale/orphaned credential-list entry per old file path that Excel doesn't
auto-prune — harmless, doesn't affect what a query actually reads from).

Required one-time local setup discovered along the way: Excel's PostgreSQL
connector needs Npgsql **4.0.17 or earlier** specifically installed with the
GAC option (newer Npgsql versions aren't supported by the connector), and
Supabase's pooler requires the DB username in `postgres.<project-ref>` form
(not just `postgres`) since disabling TLS (needed to work around a
certificate-validation error) removes the SNI-based tenant routing
alternative.

**`KNOWN_ISSUES.md` #9 resolved 2026-08-05.** Fixed the `extract_state_history.py`
encoding bug, re-extracted all of `raw/`, and audited every text/JSONB
column in the schema (77 + 7 columns, all clean now). The bug turned out to
be much bigger than the original Nelly finding: it also corrupted
`rugvista_variant_snapshot.variant_name`/`.parent_name` (49,083 of 89,098
rows — the single largest data-quality bug found in this whole migration),
`ahlsell_led_panel_article.product_name` (48 rows — previously misdiagnosed
in `KNOWN_ISSUES.md` #7 as genuine source corruption; it wasn't), and
`ahlsell_warehouse.name`/`.city`/`.address` (67/48/75 of 110 rows). All four
reloaded via new one-off `upsert_rows`-based fix scripts in `scripts/tools/`
(`fix_rugvista_encoding.py`, `fix_ahlsell_encoding.py`) rather than the
original `insert_rows`/`ON CONFLICT DO NOTHING` loaders, since those skip
rows that already exist instead of correcting them.

**`KNOWN_ISSUES.md` #5 fixed 2026-08-05.** Removed the `quantity > 0` filter
in `fetch_stock()` — confirmed live via the API that zero-stock warehouse
entries genuinely exist and were being silently dropped (110 entries for a
sample article, 11 at zero). Takes effect from the next scheduled run.

**`KNOWN_ISSUES.md` #3 addressed (partially) 2026-08-05.** Added a shared
`core.cli.warn_if_gap()` helper, wired into the three scripts that actually
have this bug (`track_nelly_inventory.py`, `track_anoto_inventory.py` for
both Anoto and Neo, `track_ahlsell_plejd_inventory.py`) — prints a clear
`[VARNING]` in the run log when the previous snapshot is more than one
calendar day old, without changing the computed number (deliberately, to
preserve behavior already validated against xlsx history). Along the way,
found that `track_rvrc_sales.py` was wrongly included in the original list —
it uses the Voyado API's own `sale_last_week`/`sale_last_days` rolling
counters, not a self-computed stock delta, so it's immune by design.
Still open: `nelly_daily_summary`/`rvrc_sales_daily_summary` have no
SQL-view-level protection the way Rugvista/Anoto/Ahlsell do (see item 2
below) — a missed day still produces a wrong number there, just a visible
one now.

Remaining work, in priority order:

1. **Review the 8 Google Trends scripts** (`KNOWN_ISSUES.md` #11) — none
   were run live during migration (deliberately, to avoid 429/CAPTCHA risk);
   unknown whether they still work against Google's current defenses. Now
   that their Power Query tabs are live, a broken scraper means those tabs
   silently stop getting new months rather than just the raw table.
   **Explicitly deferred again — handle separately, last, per 2026-08-05
   decision.**
2. **Build `rvrc_variant_snapshot`/`nelly_variant_snapshot` delta views**
   once enough daily history has accumulated (both started empty at
   migration time, no backfill was possible — check back in a couple of
   weeks). This would also give Nelly/RVRC the same calendar-day-guard
   protection Rugvista/Anoto/Ahlsell already have at the view level.

**`KNOWN_ISSUES.md` #6 fixed 2026-08-05.** `track_nelly_aov.py`'s selector was
stale after a Nelly frontend rebuild (`text-sm text-darkGrey` → `text-subhead
leading-none text-darkGrey`); the old discount/regular price split via `<ins>`
no longer applies either, since the listing page now shows one price per
product. Confirmed live via Selenium DOM inspection, fixed, and ran
end-to-end (320 prices across 10 pages, sensible median/mean).

Separate from the migration itself, still open:
- `adtraction_epc_combined.py` and `track_tu_brands.py` are waiting on your
  call (see the inventory table below).

## Target architecture

- **Postgres is the source of truth for raw data**, at the finest
  granularity the source API gives us (one row per observation unit per
  snapshot — e.g. one row per product variant per day, not a pre-aggregated
  daily total).
- **Computed metrics are SQL views** (`sql/views/`), not Python. Fixing a
  calculation bug means changing the view — it then applies to the entire
  history immediately. A bug fixed in a Python script only applies to runs
  *after* the fix; the past stays wrong forever, which is the core problem
  this migration solves.
- **Reference/dimension data lives in tables, not hardcoded Python** (e.g.
  `ahlsell_article.category`, populated once from the existing `categorize()`
  function rather than re-checked against a hardcoded set on every run).
- **Excel stays** as the presentation layer. `DATA_DASHBOARD.xlsx`'s Power
  Query tabs get repointed from `data/*.xlsx` files to Postgres views, one
  tab at a time, only once each view's numbers have been diffed against the
  existing xlsx over the full available history. Nothing is deleted or
  repointed before that check passes — see the Rugvista precedent below.
- **GitHub Actions keeps its current shape.** Each migrated script's step
  just gains a `DATABASE_URL` env entry and the script gains a best-effort
  DB write; nothing about scheduling, `continue-on-error`, or the job
  summary changes as part of migration itself.

## The established pattern (Rugvista, Ahlsell/Plejd)

Checklist for migrating one more pipeline, in the order it was actually done:

1. **Extract history.** `scripts/tools/extract_state_history.py` walks the
   full git history of every `data/*_state.json` file and writes each daily
   version to `raw/<name>/<author-date>.json`, keyed on commit **author**
   date. It already ran once for every `_state.json` that existed at the
   time (11 files) — check `raw/<name>/` before assuming this step is
   needed; it's idempotent, safe to re-run if a new `_state.json` shows up.
   **~20 of the 38 scripts have no `_state.json` at all** (they write
   straight to `.xlsx`) — those don't get this head start. Backfill options
   for them, cheapest first: (a) if the script only ever *appends* a row per
   run and never rewrites the sheet (check for `excel_utils.append_row`/
   `append_df` or equivalent — `fetch_kpi.py` is the confirmed example), the
   **current** `.xlsx` file already *is* the full history; just read it
   directly with `pandas.read_excel`, no git involved
   (`scripts/tools/load_kpi_history.py` is the template); (b) otherwise,
   extract history from the xlsx files' own git-committed versions
   (`git show <rev>:path`, parse each with `pandas.read_excel` — more work,
   binary format); (c) accept the table starts empty from the day migration
   lands. Decide per pipeline; record the decision in the migration commit.
2. **One-off loader in `scripts/tools/`.** Reads every file under `raw/…`
   and backfills the table(s) via `core.db.insert_rows` (not `safe_insert` —
   a one-off backfill should fail loudly, not swallow errors). If the
   source state format replaces reference data wholesale each run rather
   than merging (Ahlsell's `products`/`warehouses` dicts do this), merge
   **chronologically across all files**, not just the newest — reading only
   the latest file silently drops metadata for anything that fell out of
   the catalog earlier. Run it, confirm the reported counts, run it again,
   confirm it's a no-op.
3. **Schema in `sql/`, never in Python.** New tables go in `sql/schema.sql`,
   which always describes the table's current full shape. A change to an
   *already-created* table (new column, new index) goes in a new numbered
   file under `sql/migrations/` (`00N_description.sql`), applied by hand
   against the database — `sql/schema.sql` is not a running changelog.
4. **Day-level dedup key on every snapshot table.** A `snapshot_date date
   NOT NULL` column, derived from the observation timestamp in
   `Europe/Stockholm`, with a `PRIMARY KEY`/`UNIQUE INDEX` on
   `(snapshot_date, <natural key>)` — never on the raw timestamp alone,
   since a same-day rerun has a slightly different captured-at but must
   never double-insert. (Rugvista: `(snapshot_date, product_id)`. Ahlsell:
   `(snapshot_date, article, warehouse_id)` was already the natural PK.)
5. **Wire the live script, additively.** After its existing collection
   step, call `core.db.safe_insert(...)` with the same conflict key as the
   table's unique index. Every existing state-file write, Excel write, and
   print statement stays exactly as it was — this is purely additive. If
   the script has an "already collected today, skip re-fetch" early-return
   branch (Ahlsell had one), rebuild the DB rows from the already-in-hand
   state data on that branch too — don't let the shortcut silently skip the
   database.
6. **Status line, never a crash.** `safe_insert` returns `(rows_inserted,
   error)`. The script's own summary print gets one line: rows written, or
   the error message. A DB problem is logged and visible in that run's
   output, but never causes the script to exit non-zero or drop its
   Excel/state output — this directly closes the blind spot `KNOWN_ISSUES.md`
   describes around `continue-on-error: true` swallowing failures silently.
7. **CI wiring.** Add `DATABASE_URL: ${{ secrets.DATABASE_URL }}` to that
   step's `env:` in `daily.yml` or `every-3-days.yml`.
8. **Validate before touching Excel/Power Query.** Write a throwaway
   comparison script (not committed — it did its job once; see how the
   Rugvista vs-xlsx diff was handled) that diffs the view's output against
   the existing xlsx row by row over the full history. Report exact
   matches vs. mismatches. If there are mismatches, they go in
   `KNOWN_ISSUES.md` with the hypotheses, and **the view stays flagged as
   not-for-analysis until resolved** — it does not get wired into Power
   Query in the meantime, even partially.

Two supporting reusable pieces already exist and should be reused, not
recreated: `scripts/core/db.py` (`get_connection`/`insert_rows`/`safe_insert`,
plus `upsert_rows`/`safe_upsert` for entity-shaped data — see below) and
`scripts/core/cli.py` (`add_no_side_effects_flag`/`log_skip`, for a script
that needs to test its DB write without touching local files).

**Not every pipeline is snapshot-shaped.** `fetch_ted_procurements.py` tracks
procurement *notices* whose fields change over time (status, winner, value) —
there's no meaningful "day" to key on, just a natural entity key
(`company`, `publication_number`). For pipelines like this, use
`upsert_rows`/`safe_upsert` (`ON CONFLICT DO UPDATE`) instead of steps 4/6
above, keyed on the entity's natural key, storing only its *latest known
state* rather than a daily history. Backfill may also be simpler than usual:
if the source system is itself the historical archive (TED's API has full
history back to 2021), you don't need `raw/` or git archaeology at all —
just run the live, DB-wired script once.

## Script inventory

Status legend: **Migrerad** = raw capture is live in Postgres (does *not*
imply the derived view, if any, is validated — check notes/`KNOWN_ISSUES.md`).
**Ej migrerad** = still Excel-only. **Avvecklad** = deliberately or silently
stopped; do not migrate without first deciding whether to revive or retire it.
**Engångsscript** = not a recurring pipeline at all, out of scope for migration.

| Script | State file(s) | Excel file(s) | Scheduled? | Status | Notes |
|---|---|---|---|---|---|
| `track_rugvista_daily_sales.py` | `rugvista_state.json` | `rugvista_daily_sales.xlsx` | daily | **Migrerad** | Raw capture live (`rugvista_variant_snapshot`); view `rugvista_daily_sales_v` validated against the full xlsx history (0 discrepancies across 286 days) after fixing the 48-hour gap guard — see `KNOWN_ISSUES.md` #1 (resolved 2026-07-29). Ready for Power Query. |
| `track_ahlsell_plejd_inventory.py` | `ahlsell_plejd_state.json` | `ahlsell_plejd_inventory.xlsx` | daily | **Migrerad** | Raw capture live (`ahlsell_stock_snapshot`/`ahlsell_article`/`ahlsell_warehouse`). `ahlsell_plejd_sales_v` built and validated against the full xlsx history (0 discrepancies, 315 observations) — found and fixed a real bug along the way: 39 `ahlsell_article.product_name` rows had mojibake from the original one-off load, silently miscategorizing 9 "Väggarmatur" articles as "Övrigt" (`KNOWN_ISSUES.md` #8, resolved). Known zero-stock-row gap still open — `KNOWN_ISSUES.md` #5. |
| `fetch_kpi.py` | — | `kpi-history.xlsx` | daily | **Migrerad** | Raw capture live (`kpi_history`, PK `snapshot_date`). No view needed — single scalar pair per day, no delta logic. Backfilled directly from `kpi-history.xlsx` (itself an append-only running log, not a snapshot file) rather than via git archaeology — `ON CONFLICT` also collapsed the known 2025-10-03/04 duplicates (`KNOWN_ISSUES.md` #2) to one row each. |
| `track_rugvista_bestsellers.py` | — | `rugvista_bestsellers.xlsx` | daily | **Migrerad** | Raw capture live (`rugvista_bestseller_prices`, PK `snapshot_date`). No view needed — single median/avg pair per day, no delta logic. Backfilled directly from the xlsx (append-only, no duplicates found — 300 rows, 300 distinct dates). |
| `fetch_ted_procurements.py` | — | `ted_procurements.xlsx` | daily | **Migrerad** | **Different shape from every other migration**: `ted_procurement_notice` holds the *latest known state* per `(company, publication_number)`, upserted via `core.db.upsert_rows`/`safe_upsert` (new — `ON CONFLICT DO UPDATE`, not `DO NOTHING`), not a daily snapshot history. Stores the full computed row set (Title/Description/Notice Type/Tenderers/Procedure Type/Won by Company were always computed but never written to the xlsx) and the rows the Excel export filters out for org-number-tracked companies (historical losses) — that filter is now applied only when building Excel (`filter_for_excel()`), not a reason to skip capturing the row. Tracked companies moved to a `ted_tracked_companies` data table (`company`, `search_terms`, `org_numbers` as JSONB) instead of the hardcoded `COMPANIES` list — `load_tracked_companies()` falls back to a small built-in list if the DB can't be read, so a DB problem never stops the fetch. No git archaeology needed for backfill: TED's own API is already the full historical archive back to 2021-01-01, so migrating just meant wiring the DB writes and running the existing fetch once (55 + 18 rows upserted live). **Lot-level detail added 2026-08-03**: `ted_lot_tender`, same upsert shape keyed on `(company, publication_number, lot_id)`, fed by the existing `fetch_lot_details()`/`parse_eforms_xml()` XML parsing (only for `DETAIL_COMPANIES = {"EQL Pharma AB"}`) — 65 rows upserted live. Along the way, fixed a pre-existing bug affecting both this table and `ted_procurement_notice`: pandas coerces a missing float to `NaN`, and psycopg2 was writing that through as a literal `NaN` numeric instead of `NULL` (34 + 37 rows respectively) — added a `_clean()` helper and re-ran to fix in place. |
| `fetch_plejd_sensortower_rankings.py` | — | `plejd_sensortower_rankings.xlsx` | daily | **Migrerad** | Raw capture live (`plejd_sensortower_rankings`, PK `(snapshot_date, country)`). Wide xlsx (one column per country) melted to long rows; missing ranks (app unranked that day) are skipped, not fabricated as zero. Backfilled directly from the xlsx — 212 rows/210 populated dates → 889 (date, country) observations. The script's existing "already written today" guard now also triggers a DB-only write (rebuilt from the existing xlsx row) instead of exiting early, so the database doesn't silently miss a day. |
| `track_fractal_rankings_playwright.py` | — | `fractal_rankings.xlsx` | daily | **Migrerad** | Raw capture live (`fractal_rankings`, PK `(snapshot_date, product)`). Same wide-to-long shape as the Plejd ranking: 6 products (2 headsets, 4 chairs), "NA"/not-found skipped rather than fabricated. Backfilled directly from the xlsx — 297 rows → 1219 (date, product) observations. |
| `fetch_anoto_amazon_data.py` | — | `anoto_amazon_data.xlsx` | daily | **Migrerad** | Raw capture live (`anoto_amazon_data`, PK `snapshot_date`). Backfilled directly from the xlsx (append-only, 95 rows, no duplicates). `0` is the script's own "not found that day" sentinel for both columns — kept as-is in the DB rather than converted to NULL, to match existing xlsx semantics exactly. |
| `amazon_scape_bought_playwright_us_de.py` | — | `fractal_scape_refine_data.xlsx` | daily | **Migrerad** | Raw capture live (`amazon_scape_refine_data`, PK `(snapshot_date, product, country)`). Wide xlsx (2 columns per product-country pair) melted to long rows. One known same-day double-run (2025-11-30, two rows with slightly different scrape results) resolved by `ON CONFLICT` keeping the first — 242 xlsx rows → 2892 of 2904 attempted observations inserted (12 skipped = that duplicate row × 6 products × 2 countries). Note: `daily.yml`'s job-summary `OUTFILE` map still points at the old `scape_bought_by_country.xlsx` path (stale since 2025-12-03) — cosmetic bug in the summary table, unrelated to this migration, worth a separate small fix. |
| `track_nelly_aov.py` | — | `nelly_aov.xlsx` | daily | **Migrerad** | Raw capture live (`nelly_aov`, PK `snapshot_date`). Backfilled directly from the xlsx (291 rows, no duplicates). Its stale-selector scraper bug (`KNOWN_ISSUES.md` #6, ~2.5 weeks of silent 0-row runs) was fixed 2026-08-05 — unrelated to this migration itself, but fixed in the same pass since it was blocking the table from getting fresh rows. |
| `track_ahlsell_led_panel_inventory.py` | `ahlsell_led_panel_state.json` | `ahlsell_led_panel_inventory.xlsx` | daily | **Migrerad** | Raw capture live (`ahlsell_led_panel_article`, `ahlsell_led_panel_stock_snapshot`). Simpler than Ahlsell/Plejd: this endpoint only returns per-article *totals* across all warehouses, no per-warehouse breakdown, so no `ahlsell_warehouse`-equivalent table. `ahlsell_led_panel_brand_stock_v` (per-brand daily totals) built and validated against the xlsx "Varumärken" sheet — 0 discrepancies, 551 observations. 48 mojibake rows in `ahlsell_led_panel_article.product_name` found during validation — originally believed to be genuine pre-existing source corruption, but turned out to be the same `extract_state_history.py` encoding bug as `KNOWN_ISSUES.md` #9; fixed 2026-08-05 via `scripts/tools/fix_ahlsell_encoding.py`. |
| `track_anoto_inventory.py` | `anoto_inventory_state.json`, `neo_inventory_state.json` | `anoto_inventory.xlsx` | daily | **Migrerad** | Raw capture live (`anoto_variant_snapshot`, PK `(snapshot_date, store, variant_id)`, `store` = `anoto`/`neo`). Denormalized like `rugvista_variant_snapshot` — price/title captured per snapshot, not a separate dimension table. `anoto_daily_sales_v` built and validated against both stores' "Daily Summary" sheets — required the same calendar-date gap guard as `rugvista_daily_sales_v` (a handful of inq.shop variants were transiently absent from single days' fetches); after that fix, 0 discrepancies except one deliberate divergence (2026-07-04, Neo) where the view correctly excludes a delta spanning a day the pipeline itself skipped, rather than reproducing that flaw like the xlsx does (`KNOWN_ISSUES.md` #3). |
| `track_rvrc_sales.py` | `rvrc_sales_state.json` | `rvrc_sales.xlsx` | daily | **Migrerad** | Two raw tables: `rvrc_sales_daily_summary` (aggregate, fully backfilled and **validated against the xlsx "Daily Summary" sheet — 0 discrepancies, 143 days**, after filling a 2026-07-27 gap the same migration-day timing gap seen elsewhere) and `rvrc_variant_snapshot` (per-product-colour, **not backfillable** — the "Latest Detail" xlsx sheet is replaced not appended each run, so no history was ever retrievable; starts from the day migration landed). The "already ran today" skip branch rebuilds the variant snapshot from that day's still-current "Latest Detail" sheet instead of skipping the database. No delta view yet — `rvrc_variant_snapshot` only has a few days of history so far, not enough to validate a view against. |
| `track_nelly_inventory.py` | `nelly_inventory_state.json` | `nelly_inventory.xlsx` | daily | **Migrerad** | Two raw tables, same split as RVRC: `nelly_daily_summary` (aggregate, fully backfilled and **validated against the xlsx "Daily Summary" sheet — 0 discrepancies, 137 days**, after filling the same 2026-07-27 gap; two of the earliest dates, 2026-03-17/18, predate the script's "returns" field entirely — stored as NULL, correctly treated as 0 to match the xlsx writer's own `.get("returns", 0)` default) and `nelly_variant_snapshot` (per-product-colour, **not backfillable at all** — unlike RVRC there's no leftover "Latest Detail" sheet either, so even the skip-branch can only rebuild the daily summary, not this table; starts from the day migration landed). Note: the docstring's Playwright cluster-ID auto-discovery is currently dead code — `main()` uses the hardcoded `ELEVATE_CLUSTER_ID` constant directly, so no browser automation was actually involved in this migration. No delta view yet — same reason as RVRC, insufficient history in `nelly_variant_snapshot` to validate against. |
| `adtraction_epc_combined.py` | root `adtraction_state.json` (Playwright storage state, not under `data/`) | `adtraction_epc_medians.xlsx` | every-3-days | Ej migrerad | Still scheduled in `every-3-days.yml`, but has deliberately produced no output since 2025-10-22 (~9 months) — a known, intentional state, not a bug. Low priority for migration until that changes. |
| `track_tu_brands.py` | `tu_brands_state.json` | `tu_brands.xlsx` | daily | Ej migrerad | State file has only 4 commits in ~9 months; `tu_brands.xlsx` has **never been created** (only appends a row when a genuinely new brand appears). Confirm this is expected (no new brands) vs. a broken append path before migrating. |
| `fetch_cheffelo_trends.py` | — | `cheffelo_trends_monthly.xlsx` | not scheduled | **Migrerad** | Google Trends, manual/ad-hoc only. Shares `google_trends_monthly` (long format: `pipeline`/`sheet`/`series`/`month`/`value`, upserted latest-known-state — see schema note) with the other 7 trends fetchers rather than 8 near-duplicate wide tables. Backfilled directly from the current xlsx (each run rewrites its full series from `FETCH_START`, so no `raw/` git archaeology needed) via `scripts/tools/load_google_trends_history.py`. DB write wired into the script via `core/trends.py`'s shared `write_trends_to_db()`, but **not live-tested** — see `KNOWN_ISSUES.md` #11. |
| `fetch_fractal_trends.py` | — | `fractal_trends_monthly.xlsx` | not scheduled | **Migrerad** | Same as `fetch_cheffelo_trends.py` above — shares `google_trends_monthly`, backfilled, wired, not live-tested (`KNOWN_ISSUES.md` #11). |
| `fetch_nelly_trends_v3.py` | `nelly_trends_cache.json` (transient) | `nelly_trends_monthly.xlsx` | not scheduled | **Migrerad** | Same as above. |
| `fetch_pierce_trends.py` | — | `pierce_trends_monthly.xlsx` | not scheduled | **Migrerad** | Same as above, except this script writes two sheets ("together"/"separate" scaling of the same 3 terms) — both map to `google_trends_monthly` via the `sheet` column instead of `'default'`. |
| `fetch_plejd_trends.py` | — | `plejd_trends_monthly.xlsx` | not scheduled | **Migrerad** | Same as `fetch_cheffelo_trends.py` above. |
| `fetch_plejd_vs_electrician_trends.py` | — | `plejd_vs_electrician_trends.xlsx` | not scheduled | **Migrerad** | Same as above. |
| `fetch_revolutionrace_trends.py` | — | `revolutionrace_trends_monthly.xlsx` | not scheduled | **Migrerad** | Same as above. |
| `fetch_rugvista_trends_v2.py` | `rugvista_trends_cache.json` (transient) | `rugvista_trends_monthly.xlsx` | not scheduled | **Migrerad** | Same as above. |
| `track_combined_prices.py` | — | `combined_prices.xlsx` | **removed 2026-06-22** | Avvecklad | Step deleted in commit `0e06ab3`; still listed (harmlessly) in `daily.yml`'s summary-table arrays. |
| `track_duroc_machines.py` | `duroc_machines_state.json` | `duroc_machines.xlsx` | **removed 2026-06-22** | Avvecklad | Step + summary-array entries cleanly removed in `0e06ab3`. |
| `track_revolutionrace_reviews.py` | `revolutionrace_state.json` | `revolutionrace_reviews.xlsx` | **removed 2026-06-22** | Avvecklad | Step + summary-array entries cleanly removed in `0e06ab3`. This is the script behind the already-known-stalled `revolutionrace_state.json` (`KNOWN_ISSUES.md` #4). |
| `youtube_diy_trends.py` | — | `youtube_diy_sentiment.xlsx` | **removed 2026-06-22** | Avvecklad | Step deleted in `0e06ab3`; still listed (harmlessly) in the summary-table arrays. |
| `track_rvrc_inventory.py` | `rvrc_inventory_state.json` | `rvrc_inventory.xlsx` | **removed ~2026-03-11** | Avvecklad | Superseded by `track_rvrc_sales.py` (uses the more reliable `sale_last_week` counter instead of raw stock deltas). |
| `adtraction_epc_by_country_sek.py` | root `adtraction_state.json`, `adtraction_category_cache.json` | `finance_median_epc_SEK_wide_v3.xlsx` | not scheduled | Avvecklad | Superseded by `adtraction_epc_combined.py` (Oct 2025). |
| `adtraction_epc_nonfinance_by_country_sek.py` | root `adtraction_state.json`, `adtraction_category_cache.json` | `nonfinance_median_epc_SEK_wide.xlsx` | not scheduled | Avvecklad | Superseded by `adtraction_epc_combined.py` (Oct 2025). |
| `amazon_refine_scape_ranking.py` | — | `fractal_scape_refine_ranks.xlsx` | not scheduled | Avvecklad | Superseded by `amazon_scape_bought_playwright_us_de.py`. |
| `fetch_tu_brands.py` | — | (`tu_brands.json`, `tu_brands_summary.txt` — no xlsx) | not scheduled | Avvecklad | Superseded by `track_tu_brands.py` (adds diffing/dedup this script lacks). |
| `_debug_tu.py` | — | — | not scheduled | Engångsscript | Prints matches only, writes nothing. |
| `backfill_plejd_sensortower_rankings.py` | — | `plejd_sensortower_rankings.xlsx` (shared) | not scheduled | Engångsscript | One-time 90-day backfill, by its own docstring. |
| `backfill_revolutionrace_history.py` | reads `revolutionrace_state.json` | `revolutionrace_reviews.xlsx` (shared) | not scheduled | Engångsscript | One-time monthly-histogram rebuild. |
| `fetch_rvrc_ski_product_reviews.py` | reads `revolutionrace_monthly_history.json` | `rvrc_ski_products_monthly.xlsx` | not scheduled | Engångsscript | One-off analysis, by its own docstring. |
| `excel_utils.py` | — | — | n/a | n/a | Shared helper module, not a script (no `__main__`). |

## Suggested migration order (simplest first)

Scored on: has a state file already extracted to `raw/` (head start), output
shape (single scalar vs. multi-dimensional), and whether delta/cross-day
math is involved (harder — gets it wrong silently, see `KNOWN_ISSUES.md` #1
and #5 for what that costs).

**Tier 1 — no state file, single/simple output, no delta math:**
1. ~~`fetch_kpi.py`~~ — migrated.
2. ~~`track_rugvista_bestsellers.py`~~ — migrated.
3. ~~`fetch_plejd_sensortower_rankings.py`~~ — migrated.
4. ~~`track_fractal_rankings_playwright.py`~~ — migrated.
5. ~~`fetch_anoto_amazon_data.py`~~ — migrated.
6. ~~`amazon_scape_bought_playwright_us_de.py`~~ — migrated.
7. ~~`track_nelly_aov.py`~~ — migrated. Tier 1 complete.

**Tier 2 — has a state file (raw/ history already extracted), same
snapshot+delta shape as Rugvista/Ahlsell:**

9. ~~`track_ahlsell_led_panel_inventory.py`~~ — migrated.
10. ~~`track_anoto_inventory.py`~~ — migrated. Tier 2 complete.

**Tier 3 — multi-market APIs, FX conversion, or first-run discovery logic:**

11. ~~`track_rvrc_sales.py`~~ — migrated.
12. ~~`track_nelly_inventory.py`~~ — migrated. Tier 3 complete.

**All active, correctly-shaped pipelines are now migrated**, including
`fetch_ted_procurements.py`'s lot-level detail (`ted_lot_tender`, added
2026-08-03) and all 8 Google Trends fetchers (`google_trends_monthly`,
added 2026-08-03 — backfilled and wired, but not live-tested against
Google's current API, see `KNOWN_ISSUES.md` #11). Only the two
"needs a decision" / discontinued groups below remain.

**Needs a decision before migrating:**
- `adtraction_epc_combined.py` — no output since 2025-10-22 by deliberate choice
  (confirmed by the repo owner), not a bug. Low priority until that changes.
- `track_tu_brands.py` — confirm whether the never-created xlsx is expected
  (no new brands found) or a broken append path, before migrating.

**Do not migrate** anything marked Avvecklad or Engångsscript above without
first deciding to revive (re-add the workflow step) or formally retire it.

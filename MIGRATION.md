# Migration: Excel → Postgres

Status and plan for moving this repo's source of truth from `data/*.xlsx`
files to Postgres (Supabase). Written 2026-07-28. For open correctness
questions about specific pipelines, see `KNOWN_ISSUES.md` — this document
doesn't repeat them, only points at them.

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
recreated: `scripts/core/db.py` (`get_connection`/`insert_rows`/`safe_insert`)
and `scripts/core/cli.py` (`add_no_side_effects_flag`/`log_skip`, for a
script that needs to test its DB write without touching local files).

## Script inventory

Status legend: **Migrerad** = raw capture is live in Postgres (does *not*
imply the derived view, if any, is validated — check notes/`KNOWN_ISSUES.md`).
**Ej migrerad** = still Excel-only. **Avvecklad** = deliberately or silently
stopped; do not migrate without first deciding whether to revive or retire it.
**Engångsscript** = not a recurring pipeline at all, out of scope for migration.

| Script | State file(s) | Excel file(s) | Scheduled? | Status | Notes |
|---|---|---|---|---|---|
| `track_rugvista_daily_sales.py` | `rugvista_state.json` | `rugvista_daily_sales.xlsx` | daily | **Migrerad** | Raw capture live (`rugvista_variant_snapshot`); view `rugvista_daily_sales_v` exists but disagrees with xlsx on 54/283 days — see `KNOWN_ISSUES.md` #1. Not for analysis yet. |
| `track_ahlsell_plejd_inventory.py` | `ahlsell_plejd_state.json` | `ahlsell_plejd_inventory.xlsx` | daily | **Migrerad** | Raw capture live (`ahlsell_stock_snapshot`/`ahlsell_article`/`ahlsell_warehouse`). No view yet. Known zero-stock-row gap — `KNOWN_ISSUES.md` #5. |
| `fetch_kpi.py` | — | `kpi-history.xlsx` | daily | **Migrerad** | Raw capture live (`kpi_history`, PK `snapshot_date`). No view needed — single scalar pair per day, no delta logic. Backfilled directly from `kpi-history.xlsx` (itself an append-only running log, not a snapshot file) rather than via git archaeology — `ON CONFLICT` also collapsed the known 2025-10-03/04 duplicates (`KNOWN_ISSUES.md` #2) to one row each. |
| `track_rugvista_bestsellers.py` | — | `rugvista_bestsellers.xlsx` | daily | **Migrerad** | Raw capture live (`rugvista_bestseller_prices`, PK `snapshot_date`). No view needed — single median/avg pair per day, no delta logic. Backfilled directly from the xlsx (append-only, no duplicates found — 300 rows, 300 distinct dates). |
| `fetch_ted_procurements.py` | — | `ted_procurements.xlsx` | daily | Ej migrerad | **Not a snapshot-shaped pipeline** — rewrites the whole workbook every run from a live TED query (one row per notice, keyed by `Publication Number`, not by day). Status/values can change for the same notice over time, so it wants upsert-on-conflict-**update** semantics, not the `ON CONFLICT DO NOTHING` snapshot pattern every other migrated script uses. Needs its own design (natural key = `(company, publication_number)`, plus a separate lot-level detail table) before migrating — don't force the day-snapshot checklist onto it. |
| `fetch_plejd_sensortower_rankings.py` | — | `plejd_sensortower_rankings.xlsx` | daily | **Migrerad** | Raw capture live (`plejd_sensortower_rankings`, PK `(snapshot_date, country)`). Wide xlsx (one column per country) melted to long rows; missing ranks (app unranked that day) are skipped, not fabricated as zero. Backfilled directly from the xlsx — 212 rows/210 populated dates → 889 (date, country) observations. The script's existing "already written today" guard now also triggers a DB-only write (rebuilt from the existing xlsx row) instead of exiting early, so the database doesn't silently miss a day. |
| `track_fractal_rankings_playwright.py` | — | `fractal_rankings.xlsx` | daily | **Migrerad** | Raw capture live (`fractal_rankings`, PK `(snapshot_date, product)`). Same wide-to-long shape as the Plejd ranking: 6 products (2 headsets, 4 chairs), "NA"/not-found skipped rather than fabricated. Backfilled directly from the xlsx — 297 rows → 1219 (date, product) observations. |
| `fetch_anoto_amazon_data.py` | — | `anoto_amazon_data.xlsx` | daily | **Migrerad** | Raw capture live (`anoto_amazon_data`, PK `snapshot_date`). Backfilled directly from the xlsx (append-only, 95 rows, no duplicates). `0` is the script's own "not found that day" sentinel for both columns — kept as-is in the DB rather than converted to NULL, to match existing xlsx semantics exactly. |
| `amazon_scape_bought_playwright_us_de.py` | — | `fractal_scape_refine_data.xlsx` | daily | **Migrerad** | Raw capture live (`amazon_scape_refine_data`, PK `(snapshot_date, product, country)`). Wide xlsx (2 columns per product-country pair) melted to long rows. One known same-day double-run (2025-11-30, two rows with slightly different scrape results) resolved by `ON CONFLICT` keeping the first — 242 xlsx rows → 2892 of 2904 attempted observations inserted (12 skipped = that duplicate row × 6 products × 2 countries). Note: `daily.yml`'s job-summary `OUTFILE` map still points at the old `scape_bought_by_country.xlsx` path (stale since 2025-12-03) — cosmetic bug in the summary table, unrelated to this migration, worth a separate small fix. |
| `track_nelly_aov.py` | — | `nelly_aov.xlsx` | daily | Ej migrerad | Single AOV row/day. ~8 days stale as of 2026-07-28 — under the "stalled" threshold but worth watching. |
| `track_ahlsell_led_panel_inventory.py` | `ahlsell_led_panel_state.json` | `ahlsell_led_panel_inventory.xlsx` | daily | Ej migrerad | Same site/shape as the already-migrated Ahlsell/Plejd pipeline — highest pattern reuse of anything left. |
| `track_anoto_inventory.py` | `anoto_inventory_state.json`, `neo_inventory_state.json` | `anoto_inventory.xlsx` | daily | Ej migrerad | One script, two shops (Anoto + Neo), two state files, one shared workbook. Stock-delta logic, same shape as Rugvista/Ahlsell. |
| `track_rvrc_sales.py` | `rvrc_sales_state.json` | `rvrc_sales.xlsx` | daily | Ej migrerad | Voyado Elevate API, 5 markets, FX conversion to EUR, `sale_last_week`/`sale_last_days` counters (different math than stock-delta — needs its own view logic). |
| `track_nelly_inventory.py` | `nelly_inventory_state.json` | `nelly_inventory.xlsx` | daily | Ej migrerad | ~20 markets, Voyado Elevate, Playwright-based first-run API discovery. Most complex active pipeline. |
| `adtraction_epc_combined.py` | root `adtraction_state.json` (Playwright storage state, not under `data/`) | `adtraction_epc_medians.xlsx` | every-3-days | Ej migrerad | Still scheduled in `every-3-days.yml`, but has deliberately produced no output since 2025-10-22 (~9 months) — a known, intentional state, not a bug. Low priority for migration until that changes. |
| `track_tu_brands.py` | `tu_brands_state.json` | `tu_brands.xlsx` | daily | Ej migrerad | State file has only 4 commits in ~9 months; `tu_brands.xlsx` has **never been created** (only appends a row when a genuinely new brand appears). Confirm this is expected (no new brands) vs. a broken append path before migrating. |
| `fetch_cheffelo_trends.py` | — | `cheffelo_trends_monthly.xlsx` | not scheduled | Ej migrerad | Google Trends, manual/ad-hoc only. |
| `fetch_fractal_trends.py` | — | `fractal_trends_monthly.xlsx` | not scheduled | Ej migrerad | Google Trends, manual/ad-hoc only. |
| `fetch_nelly_trends_v3.py` | `nelly_trends_cache.json` (transient) | `nelly_trends_monthly.xlsx` | not scheduled | Ej migrerad | Google Trends, manual/ad-hoc only. |
| `fetch_pierce_trends.py` | — | `pierce_trends_monthly.xlsx` | not scheduled | Ej migrerad | Google Trends, manual/ad-hoc only. |
| `fetch_plejd_trends.py` | — | `plejd_trends_monthly.xlsx` | not scheduled | Ej migrerad | Google Trends, manual/ad-hoc only. |
| `fetch_plejd_vs_electrician_trends.py` | — | `plejd_vs_electrician_trends.xlsx` | not scheduled | Ej migrerad | Google Trends, manual/ad-hoc only. |
| `fetch_revolutionrace_trends.py` | — | `revolutionrace_trends_monthly.xlsx` | not scheduled | Ej migrerad | Google Trends, manual/ad-hoc only. |
| `fetch_rugvista_trends_v2.py` | `rugvista_trends_cache.json` (transient) | `rugvista_trends_monthly.xlsx` | not scheduled | Ej migrerad | Google Trends, most recently manually run (2026-07-03) of the trend fetchers. |
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
7. `track_nelly_aov.py`

**Deferred — needs its own design, not a fit for the snapshot checklist:**
- `fetch_ted_procurements.py` (see inventory notes above).

**Tier 2 — has a state file (raw/ history already extracted), same
snapshot+delta shape as Rugvista/Ahlsell:**

9. `track_ahlsell_led_panel_inventory.py` — sibling of the already-migrated
   Ahlsell/Plejd pipeline, most reusable of anything left.
10. `track_anoto_inventory.py` — two shops, same stock-delta shape.

**Tier 3 — multi-market APIs, FX conversion, or first-run discovery logic:**

11. `track_rvrc_sales.py`
12. `track_nelly_inventory.py`

**Needs a decision before migrating:**
- `adtraction_epc_combined.py` — no output since 2025-10-22 by deliberate choice
  (confirmed by the repo owner), not a bug. Low priority until that changes.
- `track_tu_brands.py` — confirm whether the never-created xlsx is expected
  (no new brands found) or a broken append path, before migrating.

**Not scheduled — migrate only if/when rescheduled:** the 8 Google Trends
fetchers (`fetch_cheffelo_trends.py`, `fetch_fractal_trends.py`,
`fetch_nelly_trends_v3.py`, `fetch_pierce_trends.py`, `fetch_plejd_trends.py`,
`fetch_plejd_vs_electrician_trends.py`, `fetch_revolutionrace_trends.py`,
`fetch_rugvista_trends_v2.py`).

**Do not migrate** anything marked Avvecklad or Engångsscript above without
first deciding to revive (re-add the workflow step) or formally retire it.

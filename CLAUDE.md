# CLAUDE.md

Conventions for this repo. Read `MIGRATION.md` before migrating a script to
Postgres and `KNOWN_ISSUES.md` before trusting a number that comes out of
migrated data — don't re-discover or duplicate either.

## Running scripts

- Every script assumes CWD = repo root; relative paths like `data/x.json`
  resolve from there, matching `daily.yml` (`python ./scripts/foo.py`) and
  how a human runs them locally.
- Scripts directly under `scripts/` can `from core.db import ...` /
  `from core.cli import ...` with no path setup — Python puts a running
  script's own directory on `sys.path[0]`, and that directory is `scripts/`.
- Scripts under `scripts/tools/` are one level deeper and need
  `sys.path.insert(0, str(Path(__file__).resolve().parent.parent))` before
  importing `core` or a sibling module from `scripts/` — see
  `load_ahlsell_history.py`, which uses this to import both `core.db` and
  `categorize()` from `track_ahlsell_plejd_inventory` instead of copying it.

## Database access

- All Postgres access goes through `scripts/core/db.py`. Production scripts
  call `safe_insert()` (never raises, returns `(rows_inserted, error)`), not
  `insert_rows()` (raises) or `psycopg2.connect` directly. A one-off backfill
  script in `scripts/tools/` is the exception — it should use `insert_rows`
  and fail loudly, since a silent partial backfill is worse than a crash.
- Most tables are daily snapshots: `ON CONFLICT DO NOTHING` on
  `(snapshot_date, key)`, reruns are no-ops. Some pipelines track an *entity*
  whose fields change over time instead (e.g. `ted_procurement_notice` —
  a procurement's status/winner/value changes between runs) — for those,
  use `upsert_rows`/`safe_upsert` (`ON CONFLICT DO UPDATE`) keyed on the
  entity's natural key, storing latest-known-state rather than a history.
  Don't force the snapshot shape onto something that isn't one.
- `DATABASE_URL` comes from `.env` locally (gitignored, loaded via
  `python-dotenv`) or a workflow step's `env: DATABASE_URL: ${{ secrets.DATABASE_URL }}`
  in CI. Never hardcode it, commit it, or print it.
- Every snapshot table has a `snapshot_date date NOT NULL` (derived in
  `Europe/Stockholm`) with a unique index/PK on `(snapshot_date, <key>)`.
  Inserts use `ON CONFLICT (...) DO NOTHING` on that key — reruns are always
  no-ops, never duplicates.
- A database write is additive to an existing script, never a replacement:
  existing state-file/Excel writes and prints stay exactly as they were. A
  DB failure is caught, logged, and shown as one status line in the script's
  own summary — never a reason to exit non-zero or skip the rest of the run.
  See `write_snapshot_to_db()` in `track_rugvista_daily_sales.py` or
  `track_ahlsell_plejd_inventory.py` for the shape, including how the latter
  rebuilds DB rows on its "already collected today" early-return branch
  instead of skipping the database write there.

## Schema and classification data

- Table definitions live only in `sql/schema.sql`, which always describes
  the *current* full shape. A change to an already-created table goes in a
  new numbered file under `sql/migrations/` (`00N_description.sql`), applied
  by hand — not a rewrite of the existing `CREATE TABLE`.
- No hardcoded classification data in Python (e.g. the `LED_PANEL_ARTS`/
  `OVRIGT_ARTS` article-number sets that used to gate `categorize()`).
  Computing a category in Python is fine; once computed, the *result* is
  data and belongs in a table column, not something a SQL view re-derives
  from a hardcoded set.
- Computed/derived metrics (sales, deltas, aggregates) are SQL views under
  `sql/views/`, not Python — a fix there applies to the whole history at
  once.

## Historical backfill

- `git log`/`git show` on `data/*.json` and `data/*.xlsx` is the only source
  of pre-migration history — the nightly workflow has committed those files
  since the repo started. **Never rewrite git history**; it's the backfill
  source, not just a log.
- `scripts/tools/extract_state_history.py` extracts every `data/*_state.json`
  file's full history into `raw/<name>/<author-date>.json` in one generic,
  idempotent pass, keyed on commit **author** date. Check `raw/<name>/`
  before assuming it needs a re-run. `raw/` is gitignored — a derived cache,
  regenerate it, don't hand-edit it.
- Not every script has a `_state.json` — about half write straight to
  `.xlsx`. Those need a different backfill approach; see `MIGRATION.md`.

## Google Trends

- All Trends access goes through `core.trends.TrendReq` / `fetch_series()`.
  **Never `pytrends`** — it is gone from `requirements.txt` on purpose. Google
  answers the *first* request of any HTTP session with 429 (that response is
  what sets the `NID` cookie); pytrends builds a fresh session per request, so
  under pytrends every request fails. Measured 2026-09-04: 40 back-to-back
  calls on one reused session, no delays at all, gave 39x 200 in 2.1s.
- **Do not "fix" a 429 by adding sleeps, backoff, rotating User-Agents, or a
  fresh client per attempt.** That is what the old scripts did and it is what
  broke them — a new client per retry guarantees the next 429, and the 60s
  exponential backoff turned a 2-second job into a 45-minute one that still
  returned nothing. Retries in `core/trends.py` are seconds, deliberately.
- There is also a real per-IP quota, separate from the session handshake, and
  what it charges for is **new sessions, not requests**. Measured 2026-09-04:
  ~100 requests spread over many freshly-built sessions (each burning a
  handshake 429) locked the IP out for ~12 minutes, while all 8 scripts run
  back to back — ~156 requests on one warmed session — finished in 27s and
  tripped nothing. So a normal full run is nowhere near the limit; what costs
  you is rebuilding sessions in a loop, which is exactly what the old
  pytrends code did.
- When the quota does trip, it cannot be slept around inside a run.
  `core.trends` raises `TrendsQuotaError` after 3 consecutive failed requests
  so a script aborts in seconds instead of grinding; catch it, stop the loop,
  and write nothing.
- `fetch_series()` caches each series on disk under `.cache/trends/`, keyed on
  the exact request. A run killed by the quota is resumed by just running it
  again — only the missing series are refetched. `.cache/` is gitignored;
  it is derived, delete it to force a clean pull (`clear_series_cache()`).
- A script writes its xlsx and DB rows only when **every** series came back.
  A partial pull silently drops columns from the xlsx that Power Query reads.
- Don't chunk a long timeframe into small windows to avoid rate limits — the
  premise is false, Google returns 2016→today in one call. `fetch_nelly_trends_v3.py`
  and `fetch_rugvista_trends_v2.py` still chunk-and-stitch for historical
  reasons; that is a documented debt in `KNOWN_ISSUES.md`, not a pattern to copy.

## Excel / Power Query

- Never delete a `data/*.xlsx` file or repoint a Power Query source to a
  database view until that view's output has been diffed against the
  existing xlsx over the *entire* available history and the discrepancies
  are understood — zero, or explained and accepted in `KNOWN_ISSUES.md`.
  `rugvista_daily_sales_v` is the current example of a view that failed
  this check and is explicitly not-for-analysis until resolved.
- A one-off comparison script used only to validate one migration doesn't
  need to be committed. Reusable tooling (extractors, loaders) goes in
  `scripts/tools/` and is committed.

## General

- Don't add a flag, dependency, or abstraction a script doesn't need yet.
  `core.cli.add_no_side_effects_flag()` exists because two scripts needed to
  test a DB write without touching local files — reuse it instead of a new
  per-script dry-run flag.
- On Windows, printing ✅/❌ to a plain console can raise
  `UnicodeEncodeError` (cp1252) — set `PYTHONIOENCODING=utf-8` when running
  scripts locally. Not a bug in the scripts; GitHub Actions runs UTF-8.

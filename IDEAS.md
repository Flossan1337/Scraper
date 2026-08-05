# Ideas

Things worth trying or building when there's time — not commitments, not
scoped, not scheduled. Unlike `KNOWN_ISSUES.md` (documented bugs/anomalies
in current behavior) and `MIGRATION.md` (the Excel→Postgres migration plan),
this is a holding pen for "would be nice to check out" thoughts so they
don't get lost. Written 2026-08-05.

## 1. Morning summary of how all scripts ran

An agent or scheduled function that sends a daily summary of the previous
night's `daily.yml`/`every-3-days.yml` run: which scripts succeeded, which
failed, row counts written per table, any DB write failures. Right now this
information exists (each script prints a "Databas: N rader skrivna" status
line, GitHub Actions has the raw logs/job summary) but nobody looks at it
unless something's visibly wrong on the dashboard. A digest — email, Slack,
whatever — would close that loop.

## 2. Outlier/anomaly flagging on scraped data

An agent or function that reviews each day's freshly-written data and flags
two different failure modes:
- **Total failure** — a script that returned 0 rows / errored / didn't
  write at all (distinct from "genuinely 0 events today", see
  `KNOWN_ISSUES.md` #10 for a real example of that ambiguity).
- **Statistical outliers** — a fetched value wildly outside its own
  history (e.g. a price 10x normal, a stock delta an order of magnitude
  larger than any prior day) that might indicate a scraper reading the
  wrong field, a site redesign, or a genuine, worth-double-checking event.

Needs a definition of "outlier" per metric (rolling stats? fixed
thresholds? something dumber that's good enough?) — hasn't been thought
through yet.

## 3. Auto-investigation + proposed-fix agent for flagged issues

A follow-on to #2: when an anomaly/failure is flagged, an agent that digs
into *why* (inspects the scraper's actual HTTP response/HTML/JSON shape
against what the script expects, similar to how `track_nelly_aov.py` got
diagnosed and fixed on 2026-08-05) and proposes a concrete code fix — but
never applies it automatically. Surfaces a diff for manual review/approval
before anything touches a live script. The manual-approval step is the
whole point, not a nice-to-have to skip later.

## 4. Natural-language querying against the Postgres database

Try prompting directly against the Supabase database for ad-hoc insights —
"what were total Rugvista sales last month," "which Ahlsell category grew
fastest this quarter" — without going through Excel/Power Query at all.
Worth understanding what's actually useful here vs. what's better served by
the existing dashboard.

## 5. A reusable structure/pattern library for new scraper scripts

Codify the patterns this migration already established (see
`MIGRATION.md`'s "established pattern" checklist for the canonical
version) so that adding a *new* pipeline of a familiar shape — e.g. "track
inventory deltas for another company," which already has a concrete
template in `track_ahlsell_plejd_inventory.py`/`track_rugvista_daily_sales.py`
— doesn't require re-explaining the shape from scratch each time. The raw
material for this already exists in `MIGRATION.md` and `CLAUDE.md`; this
is really about whether it needs a more explicit/discoverable form (a
script template? a documented decision tree of "which existing script is
this most like"?) rather than living implicitly in prior conversations.

## 6. Prompt Claude from the mobile app to build scripts / check the database

Investigate a structure so that prompts sent through the Claude mobile app
can build scripts and check the database (running queries, checking script
status, etc.) against this repo, the same way this desktop/CLI session
does — without being at a computer.

## 7. Push notifications to the Claude mobile app for flagged issues

Once #2 (outlier/anomaly flagging) exists, get a push notification to the
phone whenever something flagged actually needs attention — an outlier,
a broken scraper, whatever the flagging logic decides is worth surfacing —
rather than having to go look for it.

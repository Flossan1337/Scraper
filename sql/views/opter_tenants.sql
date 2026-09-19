-- opter_tenants — analytical views over opter_tenant_snapshot
--
-- The snapshot table holds one row per live Opter cloud tenant per day. These
-- views turn that into the ARR-proxy series: how many customers there are, and
-- the daily net adds / losses. All churn logic keys on a tenant HOST being
-- present (or not) on consecutive snapshot dates. A tenant carried through a
-- transient DNS error still has a row (resolved=false), so it stays "present"
-- and does not read as a false loss.
--
-- Because runs can be missed (CI down for a day), "the previous snapshot" is
-- the most recent EARLIER snapshot_date, not literally yesterday — the same
-- gap-tolerant pattern as rugvista_daily_sales_v / anoto_daily_sales_v.

-- ── 1. Daily tenant count, per country and total ────────────────────────────
CREATE OR REPLACE VIEW opter_tenant_daily_v AS
SELECT
    snapshot_date,
    COUNT(*)                                            AS live_tenants,
    COUNT(*) FILTER (WHERE country = 'se')              AS live_se,
    COUNT(*) FILTER (WHERE country = 'no')              AS live_no,
    COUNT(*) FILTER (WHERE country = 'fi')              AS live_fi,
    COUNT(*) FILTER (WHERE country = 'dk')              AS live_dk,
    COUNT(*) FILTER (WHERE country = 'ee')              AS live_ee,
    COUNT(*) FILTER (WHERE country IS NULL
                        OR country NOT IN ('se','no','fi','dk','ee')) AS live_other
FROM opter_tenant_snapshot
GROUP BY snapshot_date
ORDER BY snapshot_date;

-- ── 2. Per-tenant lifecycle: first and last day each host was seen ───────────
CREATE OR REPLACE VIEW opter_tenant_lifecycle_v AS
SELECT
    host,
    MIN(slug)          AS slug,
    MIN(country)       AS country,
    MIN(snapshot_date) AS first_seen,
    MAX(snapshot_date) AS last_seen,
    MAX(snapshot_date) = (SELECT MAX(snapshot_date) FROM opter_tenant_snapshot)
                       AS currently_live
FROM opter_tenant_snapshot
GROUP BY host
ORDER BY first_seen, host;

-- ── 3. Daily net change: adds and losses vs the previous snapshot ────────────
-- An ADD = host present on this snapshot_date but not on the immediately
-- preceding one. A LOSS = host present on the preceding snapshot_date but not
-- on this one. Both exclude the very first snapshot (nothing to diff against),
-- and exclude 'seed' hosts from counting as adds on the baseline day.
CREATE OR REPLACE VIEW opter_tenant_changes_v AS
WITH present AS (
    SELECT DISTINCT snapshot_date, host FROM opter_tenant_snapshot
),
dates AS (
    SELECT snapshot_date,
           LAG(snapshot_date) OVER (ORDER BY snapshot_date) AS prev_date
    FROM (SELECT DISTINCT snapshot_date FROM opter_tenant_snapshot) d
),
counts AS (
    SELECT
        dt.snapshot_date,
        dt.prev_date,
        (SELECT COUNT(*) FROM present c
           WHERE c.snapshot_date = dt.snapshot_date
             AND NOT EXISTS (SELECT 1 FROM present p
                             WHERE p.snapshot_date = dt.prev_date
                               AND p.host = c.host))       AS adds,
        (SELECT COUNT(*) FROM present p
           WHERE p.snapshot_date = dt.prev_date
             AND NOT EXISTS (SELECT 1 FROM present c
                             WHERE c.snapshot_date = dt.snapshot_date
                               AND c.host = p.host))       AS losses
    FROM dates dt
    WHERE dt.prev_date IS NOT NULL
)
SELECT
    snapshot_date,
    prev_date,
    adds,
    losses,
    adds - losses AS net_change
FROM counts
ORDER BY snapshot_date;

-- ── 4. Explicit add/loss events, one row per tenant per event ────────────────
-- Use this to SEE which customers were won/lost, not just the counts.
CREATE OR REPLACE VIEW opter_tenant_events_v AS
WITH present AS (
    SELECT DISTINCT snapshot_date, host, slug, country FROM opter_tenant_snapshot
),
dates AS (
    SELECT snapshot_date,
           LAG(snapshot_date) OVER (ORDER BY snapshot_date) AS prev_date
    FROM (SELECT DISTINCT snapshot_date FROM opter_tenant_snapshot) d
)
-- adds
SELECT c.snapshot_date AS event_date, 'add'::text AS event, c.host, c.slug, c.country
FROM present c
JOIN dates dt ON dt.snapshot_date = c.snapshot_date
LEFT JOIN present p ON p.snapshot_date = dt.prev_date AND p.host = c.host
WHERE dt.prev_date IS NOT NULL AND p.host IS NULL
UNION ALL
-- losses
SELECT dt.snapshot_date AS event_date, 'loss'::text AS event, p.host, p.slug, p.country
FROM present p
JOIN dates dt ON dt.prev_date = p.snapshot_date
LEFT JOIN present c ON c.snapshot_date = dt.snapshot_date AND c.host = p.host
WHERE c.host IS NULL
ORDER BY event_date, event, host;

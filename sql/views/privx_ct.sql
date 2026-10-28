-- privx_ct — analytical views over privx_ct_cert + privx_ct_domain
--
-- The tables hold every CT certificate whose name begins "privx" and one
-- category per registrable domain. These views turn that into the
-- self-hosted PrivX series: which organisations held a valid certificate on a
-- given date, and when each first appeared or lapsed. Only customer + partner
-- orgs are counted; 'unreviewed' is shown separately until it is classified
-- in data/privx_ct_review.json.
--
-- ACTIVE on date D = any of the org's certificates has
-- valid_from <= D <= valid_to, using UTC dates. track_privx_ct.py builds its
-- 'Monthly' Excel sheet with the same definition (monthly_rows()), so the two
-- agree.
--
-- Churn reads late by construction: an org that stops using PrivX stays
-- active until its last certificate expires (~13 months for a 1-year cert,
-- ~3 for Let's Encrypt).

-- ── 1. One row per (org, category, certificate validity window) ─────────────
CREATE OR REPLACE VIEW privx_ct_org_cert_v AS
SELECT DISTINCT
    d.org,
    d.category,
    c.domain,
    c.host,
    (c.not_before AT TIME ZONE 'UTC')::date AS valid_from,
    (c.not_after  AT TIME ZONE 'UTC')::date AS valid_to
FROM privx_ct_cert c
JOIN privx_ct_domain d ON d.domain = c.domain;

-- ── 2. Per-org lifecycle ──────────────────────────────────────────────────────
CREATE OR REPLACE VIEW privx_ct_org_v AS
SELECT
    org,
    category,
    string_agg(DISTINCT domain, ', ' ORDER BY domain)          AS domains,
    MIN(valid_from)                                             AS first_seen,
    MAX(valid_to)                                               AS valid_until,
    bool_or(current_date BETWEEN valid_from AND valid_to)       AS active_now,
    COUNT(DISTINCT host)                                        AS n_hosts
FROM privx_ct_org_cert_v
GROUP BY org, category
ORDER BY category, first_seen, org;

-- ── 3. Monthly series — shaped for Excel/Power Query like the 'Monthly' sheet ─
-- One row per month from the first counted org's first month. Each month is
-- evaluated at its last day, or today for the current month ("As of").
-- New orgs   = counted orgs whose first certificate falls in the month.
-- Lapsed orgs = counted orgs active at the previous month's "As of" but not
--               at this one. An org that lapses and returns shows up in the
--               active count again without counting as new.
CREATE OR REPLACE VIEW privx_ct_monthly_v AS
WITH counted_first AS (
    SELECT org, MIN(valid_from) AS first_seen
    FROM privx_ct_org_cert_v
    WHERE category IN ('customer', 'partner')
    GROUP BY org
),
months AS (
    SELECT
        m::date                                                    AS month_start,
        LEAST((m + interval '1 month - 1 day')::date, current_date) AS as_of
    FROM generate_series(
        (SELECT date_trunc('month', MIN(first_seen))::timestamp FROM counted_first),
        date_trunc('month', current_date)::timestamp,
        interval '1 month') AS m
),
months_lag AS (
    SELECT month_start, as_of,
           LAG(as_of) OVER (ORDER BY month_start) AS prev_as_of
    FROM months
),
active AS (
    SELECT DISTINCT mo.month_start, oc.org, oc.category
    FROM months mo
    JOIN privx_ct_org_cert_v oc ON mo.as_of BETWEEN oc.valid_from AND oc.valid_to
),
active_counted AS (
    SELECT DISTINCT month_start, org FROM active
    WHERE category IN ('customer', 'partner')
),
lapsed AS (
    SELECT ml.month_start, p.org
    FROM months_lag ml
    JOIN months pm ON pm.as_of = ml.prev_as_of
    JOIN active_counted p ON p.month_start = pm.month_start
    LEFT JOIN active_counted c ON c.month_start = ml.month_start AND c.org = p.org
    WHERE c.org IS NULL
)
SELECT
    to_char(ml.month_start, 'YYYY-MM')                                   AS "Month",
    ml.as_of                                                             AS "As of",
    (SELECT COUNT(*) FROM active a
       WHERE a.month_start = ml.month_start AND a.category = 'customer')  AS "Active customers",
    (SELECT COUNT(*) FROM active a
       WHERE a.month_start = ml.month_start AND a.category = 'partner')   AS "Active partners",
    (SELECT COUNT(*) FROM active_counted a
       WHERE a.month_start = ml.month_start)                             AS "Active orgs",
    (SELECT COUNT(*) FROM counted_first f
       WHERE f.first_seen <= ml.as_of
         AND (ml.prev_as_of IS NULL OR f.first_seen > ml.prev_as_of))    AS "New orgs",
    (SELECT string_agg(f.org, ', ' ORDER BY f.org) FROM counted_first f
       WHERE f.first_seen <= ml.as_of
         AND (ml.prev_as_of IS NULL OR f.first_seen > ml.prev_as_of))    AS "New org names",
    (SELECT COUNT(*) FROM lapsed l WHERE l.month_start = ml.month_start) AS "Lapsed orgs",
    (SELECT string_agg(l.org, ', ' ORDER BY l.org) FROM lapsed l
       WHERE l.month_start = ml.month_start)                             AS "Lapsed org names",
    (SELECT COUNT(*) FROM active a
       WHERE a.month_start = ml.month_start AND a.category = 'unreviewed') AS "Active unreviewed"
FROM months_lag ml
ORDER BY ml.month_start;

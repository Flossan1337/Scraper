-- ahlsell_plejd_flows_v
--
-- Decomposes Plejd's movement through Ahlsell into the three physical flows,
-- per article per snapshot date, in units and in SEK.
--
--   Plejd ──(1) sell-in──▶ Centrallager ──(2) transfer──▶ Butiker ──(3)──▶ Kund
--                               │
--                               └────────(4) direktleverans───────────────▶ Kund
--
-- WHAT IS AND IS NOT IDENTIFIABLE
-- ------------------------------
-- Let C = central stock, B = branch stock, and per period
--   P   = Plejd -> central          T   = central -> branches
--   S_c = central -> customer       S_b = branches -> customer
-- Conservation gives only two equations for four unknowns:
--   ΔC = P - T - S_c        ΔB = T - S_b
-- so the flows are NOT separately identifiable from stock levels alone. One
-- combination is exact, and it is the number to trust:
--   ΔC + ΔB = P - (S_c + S_b)      i.e. inflow minus total customer offtake.
-- Everything else below is an estimator with a stated assumption.
--
-- ESTIMATORS
-- ----------
--  (1) sell_in_units     = max(ΔC, 0)
--      Central stock only rises when Plejd delivers, so positive ΔC is
--      sell-in. Understated when a delivery and outbound shipments land
--      between the same two snapshots and net off.
--  (2) branch_in_gross   = Σ over branches of max(Δ per branch, 0)
--      Arrivals at branches, which come from central. Measured per branch,
--      not from the national total, so a delivery to Malmö is not cancelled
--      by a sale in Umeå.
--  (3) branch_out_gross  = Σ over branches of max(-Δ per branch, 0)
--      Branch -> customer. Per-branch gross, for the same reason. This is
--      ~2.5x the national-net figure and is the better estimator: measured
--      2026-09, decreases average 2.3 units across ~78 branches/day (retail-
--      shaped), while increases average 7.7 units in blocks (pallet-shaped),
--      and only 7.6% of large decreases carry a branch-to-branch transfer
--      signature.
--  (4) direct_ship_units = max(central_out - branch_in, 0), AGGREGATE ONLY
--      Stock leaving central that did not arrive at a branch went straight
--      to a customer -- Ahlsell's direct-delivery channel, invisible to the
--      branch data. Deliberately NOT computed per article-day here: goods in
--      transit leave central on one day and arrive at a branch on the next,
--      so the per-article daily figure is noise and often negative. The
--      aggregate views compute it from summed components; use a weekly or
--      rolling window, never a single day.
--
-- VALUE
-- -----
-- price_eff is what an Ahlsell customer pays; price_gnp is list. Neither is
-- what Plejd invoices Ahlsell -- Plejd's revenue is Ahlsell's purchase cost,
-- which is below both and is not observable here. Value columns are therefore
-- a consistently-measured PROXY for mix and momentum, not Plejd revenue.
-- Priced at the closing day's price, so a price change is attributed to the
-- day it appears.
--
-- DATA QUALITY (see KNOWN_ISSUES.md #15)
-- --------------------------------------
--   - 2026-05-30's branch figure was a partial fetch and is nulled out here,
--     which also voids the deltas into and out of that date. Two of 103 days
--     lost, rather than one fabricated swing.
--   - days_covered exposes gaps (source data is missing 2026-07-03 and
--     2026-08-09). A row with days_covered > 1 spans more than 24h; do not
--     treat it as a daily rate without dividing.
--   - fetch_hour exposes capture time. Ten backfilled days were captured
--     during business hours, so their deltas do not span a clean day.
--     Filter on it when precision matters.
--   - stock_type / prev_stock_type: type 4 (Beställningsvara) reports the
--     -1 sentinel floored to 0. An article moving between type 4 and type 3
--     shows a stock swing that is a range change, not a sale. NULL on
--     backfilled rows -- it was never recorded and must not be guessed.

CREATE OR REPLACE VIEW ahlsell_plejd_flows_v AS
WITH daily AS (
    SELECT
        snapshot_date,
        variant_number,
        qty_lager,
        -- KNOWN_ISSUES.md #15.1: partial fetch, not a real level.
        CASE WHEN snapshot_date = DATE '2026-05-30' THEN NULL ELSE qty_butik END AS qty_butik,
        qty_inleverans,
        price_eff,
        price_gnp,
        stock_type,
        source,
        fetched_at
    FROM ahlsell_plejd_daily
),
lagged AS (
    SELECT
        d.*,
        LAG(qty_lager)     OVER w AS prev_lager,
        LAG(qty_butik)     OVER w AS prev_butik,
        LAG(stock_type)    OVER w AS prev_stock_type,
        LAG(snapshot_date) OVER w AS prev_date
    FROM daily d
    WINDOW w AS (PARTITION BY variant_number ORDER BY snapshot_date)
),

-- ── Per-branch gross flows, from the warehouse-level table ────────────────
-- Deliberately not derived from the national qty_butik total: netting a sale
-- in one branch against a delivery to another destroys most of the signal.
wh_dates AS (
    -- Deduplicate BEFORE the LAG. Applying LAG to the raw table evaluates it
    -- per row, and rows sharing a date resolve ties arbitrarily -- silently
    -- producing inconsistent prev_date values. Same trap as
    -- ahlsell_plejd_sales_v.
    SELECT DISTINCT snapshot_date FROM ahlsell_stock_snapshot
),
wh_pairs AS (
    SELECT snapshot_date,
           LAG(snapshot_date) OVER (ORDER BY snapshot_date) AS prev_date
    FROM wh_dates
),
wh_curr AS (
    SELECT p.snapshot_date AS d, s.article, s.warehouse_id, s.quantity
    FROM wh_pairs p
    JOIN ahlsell_stock_snapshot s ON s.snapshot_date = p.snapshot_date
    WHERE p.prev_date IS NOT NULL
),
wh_prev AS (
    SELECT p.snapshot_date AS d, s.article, s.warehouse_id, s.quantity AS prev_quantity
    FROM wh_pairs p
    JOIN ahlsell_stock_snapshot s ON s.snapshot_date = p.prev_date
    WHERE p.prev_date IS NOT NULL
),
wh_delta AS (
    SELECT
        COALESCE(c.d, p.d)             AS d,
        COALESCE(c.article, p.article) AS article,
        COALESCE(c.quantity, 0) - COALESCE(p.prev_quantity, 0) AS delta
    FROM wh_curr c
    FULL OUTER JOIN wh_prev p
        ON c.d = p.d AND c.article = p.article AND c.warehouse_id = p.warehouse_id
),
wh_gross AS (
    SELECT
        wd.d,
        wd.article,
        SUM(CASE WHEN wd.delta < 0 THEN -wd.delta ELSE 0 END) AS branch_out_gross,
        SUM(CASE WHEN wd.delta > 0 THEN  wd.delta ELSE 0 END) AS branch_in_gross,
        -- The branch table's date set is NOT the same as the daily table's
        -- (2026-08-25 exists centrally but was never collected per branch),
        -- so the branch delta can span a different number of days than the
        -- central one. Carried through rather than assumed equal.
        MAX(wp.prev_date)                                     AS branch_prev_date,
        MAX(wd.d - wp.prev_date)::int                         AS branch_days_covered
    FROM wh_delta wd
    JOIN wh_pairs wp ON wp.snapshot_date = wd.d
    GROUP BY wd.d, wd.article
)

SELECT
    l.snapshot_date,
    l.prev_date,
    (l.snapshot_date - l.prev_date)::int              AS days_covered,
    l.variant_number,
    a.sku,
    a.product_name,
    COALESCE(a.ahlsell_category, 'Okänd')             AS category,
    l.source,
    l.stock_type,
    l.prev_stock_type,
    EXTRACT(HOUR FROM l.fetched_at)::int              AS fetch_hour,

    -- levels
    l.qty_lager,
    l.qty_butik,
    l.qty_inleverans,
    (COALESCE(l.qty_lager, 0) + COALESCE(l.qty_butik, 0)) AS qty_total,

    -- raw movement
    l.qty_lager - l.prev_lager                        AS delta_central,
    l.qty_butik - l.prev_butik                        AS delta_branch,

    -- flow estimators, units
    GREATEST(l.qty_lager - l.prev_lager, 0)           AS sell_in_units,
    GREATEST(l.prev_lager - l.qty_lager, 0)           AS central_out_units,
    -- NULL, not 0, when this article has no branch presence at all: the 13
    -- central-only articles (SPD-01, CTR-01, ...) return an empty response
    -- from the per-branch API, so they have no rows in ahlsell_stock_snapshot.
    -- For those, ALL central outflow is direct-to-customer by construction.
    w.branch_in_gross                                 AS branch_in_units,
    w.branch_out_gross                                AS branch_out_units,
    w.branch_prev_date,
    w.branch_days_covered,

    -- prices carried through so aggregates can value their own sums
    l.price_eff,
    l.price_gnp,

    -- flow estimators, SEK
    GREATEST(l.qty_lager - l.prev_lager, 0) * l.price_eff AS sell_in_value_eff,
    GREATEST(l.prev_lager - l.qty_lager, 0) * l.price_eff AS central_out_value_eff,
    w.branch_out_gross * l.price_eff                      AS branch_out_value_eff,
    w.branch_out_gross * l.price_gnp                      AS branch_out_value_gnp
FROM lagged l
LEFT JOIN ahlsell_article a ON a.article = l.variant_number
LEFT JOIN wh_gross w        ON w.d = l.snapshot_date AND w.article = l.variant_number
WHERE l.prev_date IS NOT NULL
ORDER BY l.snapshot_date, l.variant_number;

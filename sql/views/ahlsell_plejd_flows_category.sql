-- ahlsell_plejd_flows_category_v
--
-- ahlsell_plejd_flows_daily_v split by Ahlsell's own product category, for
-- mix analysis: which parts of the Plejd range move, and what they are worth.
-- Read sql/views/ahlsell_plejd_flows.sql first -- the identifiability
-- argument and every data-quality caveat there apply here too.
--
-- Category comes from ahlsell_article.ahlsell_category (Ahlsell's taxonomy,
-- captured from the authenticated API), NOT the older `category` column that
-- categorize() infers from product names with hardcoded article sets.
--
-- HOW direct_ship_units IS SPLIT ACROSS CATEGORIES -- read this before using it
-- ---------------------------------------------------------------------------
-- The direct-shipping figure is a RESIDUAL: stock that left the central
-- warehouse and never showed up at a branch. A residual only exists at the
-- level it was computed. Flooring it per category first and summing does NOT
-- reconcile to the daily view -- it over-counts by ~6%, because a category
-- whose branch arrivals exceeded its central outflow gets clamped to zero
-- instead of offsetting a category that went the other way.
--
-- So this view computes the residual once per DAY, exactly as
-- ahlsell_plejd_flows_daily_v does, then ALLOCATES it across categories in
-- proportion to each category's share of that day's central outflow.
--
--   * SUM over categories == the daily view, exactly, by construction.
--   * The split is an ALLOCATION, not a measurement. A category's
--     direct_ship_units is "its pro-rata share of that day's unmatched
--     central outflow", not an observed shipment.
--   * branch_out_units, sell_in_units and every level column ARE measured
--     per category and carry no allocation assumption. If you need a figure
--     you can defend line by line, use those.
--   * customer_units mixes a measured part (branch_out) with an allocated
--     part (direct_ship). Fine for mix and trend; do not quote a single
--     category's customer_units as a hard number.

CREATE OR REPLACE VIEW ahlsell_plejd_flows_category_v AS
WITH per_cat AS (
    SELECT
        snapshot_date,
        category,
        MAX(days_covered)                                   AS days_covered,
        COUNT(*)                                            AS articles,
        COUNT(branch_in_units)                              AS articles_with_branch_data,

        SUM(qty_lager)                                      AS qty_lager,
        SUM(qty_butik)                                      AS qty_butik,
        SUM(qty_total)                                      AS qty_total,
        SUM(qty_inleverans)                                 AS qty_inleverans,

        SUM(sell_in_units)                                  AS sell_in_units,
        SUM(central_out_units)                              AS central_out_units,
        SUM(branch_in_units)                                AS branch_in_units,
        SUM(branch_out_units)                               AS branch_out_units,

        SUM(sell_in_value_eff)                              AS sell_in_value_eff,
        SUM(central_out_value_eff)                          AS central_out_value_eff,
        SUM(branch_out_value_eff)                           AS branch_out_value_eff,
        SUM(branch_out_value_gnp)                           AS branch_out_value_gnp
    FROM ahlsell_plejd_flows_v
    GROUP BY snapshot_date, category
),
per_day AS (
    -- The same residual the daily view reports, recomputed here so the two
    -- views cannot drift apart. NULL when no branch data exists at all that
    -- day (2026-08-25), which propagates to every category rather than
    -- silently becoming zero.
    SELECT
        snapshot_date,
        SUM(central_out_units)                              AS day_central_out,
        CASE WHEN SUM(articles_with_branch_data) = 0 THEN NULL
             ELSE GREATEST(SUM(central_out_units) - SUM(branch_in_units), 0)
        END                                                 AS day_direct_ship
    FROM per_cat
    GROUP BY snapshot_date
),
allocated AS (
    SELECT
        c.*,
        d.day_direct_ship
            * (c.central_out_units / NULLIF(d.day_central_out, 0)) AS direct_ship_units
    FROM per_cat c
    JOIN per_day d USING (snapshot_date)
)
SELECT
    snapshot_date,
    category,
    days_covered,
    articles,
    articles_with_branch_data,

    qty_lager,
    qty_butik,
    qty_total,
    qty_inleverans,

    -- measured per category
    sell_in_units,
    central_out_units,
    branch_in_units,
    branch_out_units,
    -- allocated (see header)
    ROUND(direct_ship_units)                                AS direct_ship_units,
    ROUND(COALESCE(branch_out_units, 0) + direct_ship_units) AS customer_units,

    ROUND(sell_in_value_eff)                                AS sell_in_value_eff,
    ROUND(branch_out_value_eff)                             AS branch_out_value_eff,
    ROUND(direct_ship_units
          * (central_out_value_eff / NULLIF(central_out_units, 0)))
                                                            AS direct_ship_value_eff,
    ROUND(COALESCE(branch_out_value_eff, 0)
          + direct_ship_units
            * (central_out_value_eff / NULLIF(central_out_units, 0)))
                                                            AS customer_value_eff,
    ROUND(branch_out_value_gnp)                             AS branch_out_value_gnp,

    -- 7-day rolling within the category. Sums the already-allocated daily
    -- values, so it stays consistent with the daily view's own rolling
    -- columns and needs no second flooring.
    SUM(sell_in_units)     OVER w7                          AS sell_in_units_7d,
    ROUND(SUM(direct_ship_units) OVER w7)                   AS direct_ship_units_7d,
    ROUND(COALESCE(SUM(branch_out_units) OVER w7, 0)
          + SUM(direct_ship_units) OVER w7)                 AS customer_units_7d
FROM allocated
WINDOW w7 AS (PARTITION BY category ORDER BY snapshot_date
              ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
ORDER BY snapshot_date, category;

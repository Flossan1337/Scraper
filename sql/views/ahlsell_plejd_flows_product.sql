-- ahlsell_plejd_flows_product_v
--
-- ahlsell_plejd_flows_daily_v split by individual product (Plejd's SKU:
-- DIM-01, LPN-01, ...) instead of by Ahlsell category. Same construction as
-- ahlsell_plejd_flows_category_v -- read that file's header, the allocation
-- caveat applies identically and matters MORE here, because the thinner the
-- slice the noisier a residual gets.
--
-- Why this exists alongside the category view: several categories currently
-- hold exactly one article, so a specific product can be tracked through the
-- category view today -- LPN-01 is alone in 'Infällda armaturer'. That is an
-- accident of Ahlsell's filing, not a guarantee. The moment Ahlsell files a
-- second recessed luminaire there, the category series silently becomes a
-- two-product blend and a ramp reads as flat. This view keys on the article
-- itself, so a product stays separable whatever Ahlsell does to its taxonomy.
--
-- Grain: one row per article per day. Aggregating several SKUs of the same
-- product family (all TRM-01 colours, say) is a job for the pivot, not for
-- this view -- it keeps the article-level detail so that choice stays open.
--
--   * branch_out_units, sell_in_units, and all level columns are MEASURED
--     per article -- defensible line by line.
--   * direct_ship_units is that article's pro-rata share of the day's
--     unmatched central outflow, ALLOCATED by its share of central outflow.
--     Exact only for the 13 articles with no branch presence at all, where
--     every unit leaving central necessarily went to a customer.
--   * SUM over articles == the daily view exactly, by construction.
--
-- For a single product, read the *_7d columns or a monthly roll-up. A single
-- article on a single day is mostly transit timing.

CREATE OR REPLACE VIEW ahlsell_plejd_flows_product_v AS
WITH per_day AS (
    -- the same day-level residual the daily and category views report
    SELECT
        snapshot_date,
        SUM(central_out_units)                              AS day_central_out,
        CASE WHEN COUNT(branch_in_units) = 0 THEN NULL
             ELSE GREATEST(SUM(central_out_units) - SUM(branch_in_units), 0)
        END                                                 AS day_direct_ship
    FROM ahlsell_plejd_flows_v
    GROUP BY snapshot_date
),
allocated AS (
    SELECT
        f.snapshot_date,
        f.variant_number,
        COALESCE(f.sku, f.variant_number)                   AS sku,
        f.product_name,
        f.category,
        f.days_covered,
        f.stock_type,

        f.qty_lager,
        f.qty_butik,
        f.qty_total,

        f.sell_in_units,
        f.central_out_units,
        f.branch_in_units,
        f.branch_out_units,
        f.price_eff,
        f.price_gnp,
        f.sell_in_value_eff,
        f.central_out_value_eff,
        f.branch_out_value_eff,

        -- ::numeric is load-bearing. central_out_units is integer and
        -- day_central_out is bigint, so without the cast Postgres does
        -- INTEGER division and every share truncates to 0 -- silently
        -- zeroing direct shipping for every product.
        d.day_direct_ship
            * (f.central_out_units::numeric
               / NULLIF(d.day_central_out, 0)) AS direct_ship_units
    FROM ahlsell_plejd_flows_v f
    JOIN per_day d USING (snapshot_date)
)
SELECT
    snapshot_date,
    variant_number,
    sku,
    product_name,
    category,
    days_covered,
    stock_type,

    qty_lager,
    qty_butik,
    qty_total,

    -- measured
    sell_in_units,
    central_out_units,
    branch_in_units,
    branch_out_units,
    -- allocated (see header)
    ROUND(direct_ship_units)                                AS direct_ship_units,
    ROUND(COALESCE(branch_out_units, 0) + direct_ship_units) AS customer_units,

    price_eff,
    price_gnp,
    ROUND(sell_in_value_eff)                                AS sell_in_value_eff,
    ROUND(branch_out_value_eff)                             AS branch_out_value_eff,
    ROUND(direct_ship_units * price_eff)                    AS direct_ship_value_eff,
    ROUND(COALESCE(branch_out_value_eff, 0)
          + direct_ship_units * price_eff)                  AS customer_value_eff,

    -- 7-day rolling within the article: read these, not the daily columns
    SUM(sell_in_units)   OVER w7                            AS sell_in_units_7d,
    SUM(branch_out_units) OVER w7                           AS branch_out_units_7d,
    ROUND(SUM(direct_ship_units) OVER w7)                   AS direct_ship_units_7d,
    ROUND(COALESCE(SUM(branch_out_units) OVER w7, 0)
          + SUM(direct_ship_units) OVER w7)                 AS customer_units_7d,
    ROUND(COALESCE(SUM(branch_out_value_eff) OVER w7, 0)
          + SUM(direct_ship_units * price_eff) OVER w7)     AS customer_value_eff_7d
FROM allocated
WINDOW w7 AS (PARTITION BY variant_number ORDER BY snapshot_date
              ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
ORDER BY snapshot_date, sku;

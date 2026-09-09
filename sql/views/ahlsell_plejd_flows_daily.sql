-- ahlsell_plejd_flows_daily_v
--
-- One row per snapshot date: the whole Plejd-through-Ahlsell chain in units
-- and SEK. Read sql/views/ahlsell_plejd_flows.sql first -- it carries the
-- identifiability argument and the data-quality caveats.
--
-- Built as a roll-up of ahlsell_plejd_flows_category_v rather than
-- independently off the per-article view. That is deliberate: computing the
-- two separately made them disagree by 1.6% on value, because a single
-- blended price for the day's direct-shipping residual is cruder than pricing
-- each category's share at its own average. Rolling up guarantees that
-- category totals equal daily totals equal whatever Excel pivots out of them.
--
-- THE COLUMNS THAT MATTER
-- -----------------------
--   sell_in_units       Plejd -> Ahlsell central. Measured.
--   branch_out_units    branches -> customers. Measured per branch.
--   direct_ship_units   central -> customers, bypassing branches. Residual.
--   customer_units      the headline: total Plejd units reaching end
--                       customers through Ahlsell = branch_out + direct_ship.
--
--   net_channel_change_units = sell_in - customer_units
--       The one EXACT identity in the whole model (it equals ΔC + ΔB, with
--       every unobservable flow cancelling). Positive = Ahlsell built stock.
--       Negative = Ahlsell drained the channel, and Plejd's invoiced volume
--       for the period understates what end customers actually bought.
--
-- Daily values are jumpy -- deliveries land in pallets and goods sit in
-- transit for a day. The *_7d columns are what to plot.
--
-- Value columns use price_eff, what an Ahlsell customer pays. That is NOT
-- Plejd's revenue: Plejd invoices Ahlsell at a wholesale cost below it, which
-- is not observable here. Use them for mix, momentum and relative sizing, not
-- as a revenue estimate.

CREATE OR REPLACE VIEW ahlsell_plejd_flows_daily_v AS
WITH capture AS (
    -- capture metadata, which is per-date and not carried by the category view
    SELECT
        snapshot_date,
        MAX(days_covered) AS days_covered,
        MIN(fetch_hour)   AS fetch_hour,
        MIN(source)       AS source
    FROM ahlsell_plejd_flows_v
    GROUP BY snapshot_date
),
rolled AS (
    SELECT
        snapshot_date,
        SUM(articles)                  AS articles,
        SUM(articles_with_branch_data) AS articles_with_branch_data,

        SUM(qty_lager)                 AS qty_lager,
        SUM(qty_butik)                 AS qty_butik,
        SUM(qty_total)                 AS qty_total,
        SUM(qty_inleverans)            AS qty_inleverans,

        SUM(sell_in_units)             AS sell_in_units,
        SUM(central_out_units)         AS central_out_units,
        SUM(branch_in_units)           AS branch_in_units,
        SUM(branch_out_units)          AS branch_out_units,
        SUM(direct_ship_units)         AS direct_ship_units,
        SUM(customer_units)            AS customer_units,

        SUM(sell_in_value_eff)         AS sell_in_value_eff,
        SUM(branch_out_value_eff)      AS branch_out_value_eff,
        SUM(direct_ship_value_eff)     AS direct_ship_value_eff,
        SUM(customer_value_eff)        AS customer_value_eff,
        SUM(branch_out_value_gnp)      AS branch_out_value_gnp
    FROM ahlsell_plejd_flows_category_v
    GROUP BY snapshot_date
)
SELECT
    r.snapshot_date,
    c.days_covered,
    c.fetch_hour,
    c.source,
    r.articles,
    r.articles_with_branch_data,

    -- stock levels at close of day
    r.qty_lager,
    r.qty_butik,
    r.qty_total,
    r.qty_inleverans,

    -- flows in units
    r.sell_in_units,
    r.central_out_units,
    r.branch_in_units,
    r.branch_out_units,
    r.direct_ship_units,
    r.customer_units,
    r.sell_in_units - r.customer_units          AS net_channel_change_units,

    -- flows in SEK (customer-facing prices -- NOT Plejd's revenue)
    r.sell_in_value_eff,
    r.branch_out_value_eff,
    r.direct_ship_value_eff,
    r.customer_value_eff,
    r.branch_out_value_gnp,

    -- 7-day rolling: plot these, not the daily columns
    SUM(r.sell_in_units)      OVER w7           AS sell_in_units_7d,
    SUM(r.direct_ship_units)  OVER w7           AS direct_ship_units_7d,
    SUM(r.customer_units)     OVER w7           AS customer_units_7d,
    SUM(r.customer_value_eff) OVER w7           AS customer_value_eff_7d,
    SUM(r.sell_in_units) OVER w7
        - SUM(r.customer_units) OVER w7         AS net_channel_change_units_7d
FROM rolled r
JOIN capture c USING (snapshot_date)
WINDOW w7 AS (ORDER BY r.snapshot_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
ORDER BY r.snapshot_date;

-- ahlsell_plejd_supply_v
--
-- The two SUPPLY-side flows, by product group, shaped for a date x category
-- pivot in Excel:
--
--   sell_in_*    Plejd  -> Ahlsell's central warehouse
--   branch_in_*  central warehouse -> Ahlsell's branches
--
-- Deliberately narrower than ahlsell_plejd_flows_category_v: it carries only
-- flows that are DIRECTLY MEASURED from stock movements. Nothing here is a
-- residual and nothing is allocated, so every cell can be traced to an
-- observed change in a stock level.
--
--   sell_in_units    = per article, sum of positive changes in central stock.
--                      Central stock only rises when Plejd delivers.
--   branch_in_units  = per article per BRANCH, sum of positive changes.
--                      Measured at the branches, so a delivery to Malmö is
--                      not cancelled by a sale in Umeå. Branch stock rises
--                      only on a delivery from central: measured 2026-09,
--                      just 2 of 67 articles show cumulative branch arrivals
--                      exceeding central departures, 114 units of 42,470
--                      (0.27%), so treating arrivals as central shipments is
--                      sound.
--
-- KNOWN AND ACCEPTED: same-day netting
-- ------------------------------------
-- One snapshot per day, so a delivery and an outbound shipment between the
-- same two snapshots net off and only the difference is seen. Measured
-- capture rates:
--
--   sell_in     ~94%  -- 369 delivery events; the delivery (median +52,
--                        up to +6,473) dwarfs that article's typical daily
--                        outflow (median -10), so little is lost.
--   branch_in   ~83%  -- arrivals average 7.7 units against ~2.3 units of
--                        same-day counter sales, so more is masked here.
--
-- Both under-report; neither over-reports. Levels are conservative, and the
-- bias is stable over time, so period-on-period comparison is unaffected.
--
-- VALUE
-- -----
-- price_eff is the price an Ahlsell customer pays. Plejd invoices Ahlsell at
-- a wholesale cost below it, which is not observable. Value columns are a
-- consistently-priced volume proxy for mix and momentum -- NOT Plejd revenue.
--
-- MISSING DAYS (branch flows only -- sell-in is unaffected)
-- --------------------------------------------------------
--   2026-05-30  branch snapshot was a partial fetch; nulled, which also
--               voids the 05-31 delta. KNOWN_ISSUES.md #15.1.
--   2026-08-25  central collected, branches never were.
-- Three of 102 days carry no branch_in. They are absent, not zero.
--
-- PRODUCT GROUPING
-- ----------------
-- One row per Ahlsell category per day, EXCEPT LPN-01, which is split out as
-- its own group so a ramping product cannot be hidden inside an aggregate.
-- sort_key keeps it adjacent to the category it came from. LPN-01 is
-- currently the only article in 'Infällda armaturer', so that category has no
-- other rows today -- if Ahlsell files a second recessed luminaire there it
-- appears as its own column automatically, beside LPN-01, with no double
-- counting in either.

CREATE OR REPLACE VIEW ahlsell_plejd_supply_v AS
WITH base AS (
    SELECT
        f.snapshot_date,
        COALESCE(a.ahlsell_category, 'Okänd')                  AS category,
        CASE WHEN f.variant_number = '7077777' THEN 'LPN-01'
             ELSE COALESCE(a.ahlsell_category, 'Okänd')
        END                                                    AS product_group,
        f.sell_in_units,
        f.sell_in_value_eff,
        f.branch_in_units,
        f.branch_in_units * f.price_eff                        AS branch_in_value_eff
    FROM ahlsell_plejd_flows_v f
    LEFT JOIN ahlsell_article a ON a.article = f.variant_number
)
SELECT
    snapshot_date,
    product_group,
    -- keeps LPN-01 next to 'Infällda armaturer' when the pivot is sorted
    category                                    AS sort_key,
    SUM(sell_in_units)                          AS sell_in_units,
    ROUND(SUM(sell_in_value_eff))               AS sell_in_value_eff,
    SUM(branch_in_units)                        AS branch_in_units,
    ROUND(SUM(branch_in_value_eff))             AS branch_in_value_eff
FROM base
GROUP BY snapshot_date, product_group, category;
-- NO ORDER BY here, deliberately. Power Query folds List.Distinct into
-- SELECT DISTINCT product_group over this view, and Postgres then rejects any
-- ORDER BY naming columns outside that select list:
--   42P10: for SELECT DISTINCT, ORDER BY expressions must appear in select list
-- Ordering is not a view's job anyway; the Excel queries sort on sort_key and
-- product_group before pivoting, which is what puts LPN-01 beside its category.

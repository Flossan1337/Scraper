-- ahlsell_plejd_supply_v
--
-- The two SUPPLY-side flows, by product group, shaped for a date x category
-- pivot in Excel:
--
--   sell_in_*      Plejd -> Ahlsell's central warehouse
--   central_out_*  everything leaving the central warehouse, whether it goes
--                  to a branch or straight to a customer
--
-- Deliberately narrower than ahlsell_plejd_flows_category_v: it carries only
-- flows that are DIRECTLY MEASURED from stock movements. Nothing here is a
-- residual and nothing is allocated, so every cell can be traced to an
-- observed change in a stock level.
--
--   sell_in_units      = per article, sum of POSITIVE changes in central
--                        stock. Central stock only rises when Plejd delivers.
--   central_out_units  = per article, sum of NEGATIVE changes in central
--                        stock. Everything that left, with no attempt to
--                        say where it went.
--
-- Both are computed PER ARTICLE per day, not per day overall, so an article
-- being replenished does not cancel a different article shipping out on the
-- same date. Within one article on one date the two are mutually exclusive
-- by construction: central stock either rose or fell, so a day either counts
-- as sell-in or as outflow for that article, never both.
--
-- central_out_units deliberately does NOT split branch replenishment from
-- direct-to-customer delivery. That split is not identifiable from stock
-- levels (see sql/views/ahlsell_plejd_flows.sql) and the estimate of it is a
-- residual; this column is a direct measurement instead. Roughly two thirds
-- of it ships straight from Hallsberg to the customer and one third restocks
-- branches, but that ratio is an estimate and is not applied here.
--
-- KNOWN AND ACCEPTED: same-day netting
-- ------------------------------------
-- One snapshot per day, so a delivery and an outbound shipment between the
-- same two snapshots net off and only the difference is seen. Measured
-- capture rates:
--
--   sell_in      ~94%  -- 369 delivery events; the delivery (median +52,
--                         up to +6,473) dwarfs that article's typical daily
--                         outflow (median -10), so little is lost.
--   central_out  ~high -- outflow is the frequent, small side of the ledger
--                         (3,890 article-days, median -10). Only the outflow
--                         landing on that article's ~4-5 delivery days per
--                         quarter is masked.
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
-- MISSING DAYS
-- ------------
-- None. Both columns read the central warehouse only, so the branch-side
-- gaps that affect other views (2026-05-30's partial branch fetch, and
-- 2026-08-25 where branches were never collected) do not apply here. Every
-- one of the 102 dates carries both measures.
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
        f.central_out_units,
        f.central_out_value_eff
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
    SUM(central_out_units)                      AS central_out_units,
    ROUND(SUM(central_out_value_eff))           AS central_out_value_eff
FROM base
GROUP BY snapshot_date, product_group, category;
-- NO ORDER BY here, deliberately. Power Query folds List.Distinct into
-- SELECT DISTINCT product_group over this view, and Postgres then rejects any
-- ORDER BY naming columns outside that select list:
--   42P10: for SELECT DISTINCT, ORDER BY expressions must appear in select list
-- Ordering is not a view's job anyway; the Excel queries sort on sort_key and
-- product_group before pivoting, which is what puts LPN-01 beside its category.

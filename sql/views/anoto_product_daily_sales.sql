-- anoto_product_daily_sales_v
--
-- Same stock-delta sales estimate as anoto_daily_sales_v (same LAG, same
-- calendar-day gap guard, same per-store price divisor - see that file
-- for the reasoning behind each), but grouped one level finer: per store,
-- per day, per product title. Replicates the `by_product` block in
-- track_anoto_inventory.py's compute_summary(), which feeds the "By
-- Product" sheets of data/anoto_inventory.xlsx.
--
-- product_title is taken from the current snapshot row (it is captured
-- per snapshot, not from a dimension table), so a product renamed in the
-- shop shows up as a new title from that day on - same as the xlsx.
--
-- Rows exist only for (day, product) pairs where at least one variant had
-- a preceding snapshot; a product with no sales that day appears with 0,
-- a product absent from the shop that day does not appear at all. The
-- dashboard's Power Query pivots this to one column pair per product.

CREATE OR REPLACE VIEW anoto_product_daily_sales_v AS
WITH lag AS (
    SELECT
        snapshot_date,
        store,
        variant_id,
        product_title,
        price / CASE store WHEN 'anoto' THEN 100 ELSE 1 END AS price,
        quantity,
        LAG(quantity)      OVER (PARTITION BY store, variant_id ORDER BY snapshot_date) AS prev_quantity,
        LAG(snapshot_date) OVER (PARTITION BY store, variant_id ORDER BY snapshot_date) AS prev_date
    FROM anoto_variant_snapshot
)
SELECT
    snapshot_date,
    store,
    product_title,
    SUM(CASE WHEN snapshot_date - prev_date <= 1
             THEN GREATEST(0, prev_quantity - quantity) ELSE 0 END)::bigint AS est_sold_units,
    ROUND(SUM(CASE WHEN snapshot_date - prev_date <= 1
                   THEN GREATEST(0, prev_quantity - quantity) * price ELSE 0 END), 2) AS est_sold_revenue,
    SUM(CASE WHEN snapshot_date - prev_date <= 1 AND quantity > prev_quantity
             THEN 1 ELSE 0 END)::bigint AS restocks
FROM lag
WHERE prev_quantity IS NOT NULL
GROUP BY snapshot_date, store, product_title
ORDER BY snapshot_date, store, product_title;

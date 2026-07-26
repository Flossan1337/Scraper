-- rugvista_daily_sales_v
--
-- Replicates the sales-from-availability-delta logic in
-- scripts/track_rugvista_daily_sales.py, but computed from the full
-- rugvista_variant_snapshot history instead of a single day/prev-state pair:
--
--   - Per product_id, compare `available` to the immediately preceding
--     snapshot (LAG ordered by captured_at).
--   - A decrease in `available` is counted as units sold; an increase
--     (restock) is ignored.
--   - Revenue = units sold * price_sek from the CURRENT snapshot (matches
--     the Python script, which prices sales at the post-change price).
--   - Products with no preceding snapshot are skipped (first sighting).
--   - Deltas are only counted when the preceding snapshot is less than 48
--     hours earlier, so a product that disappears and reappears days later
--     doesn't produce a false delta.
--   - Day is the snapshot's calendar date in Europe/Stockholm, matching
--     today_stockholm_date_str() in the Python script.

CREATE OR REPLACE VIEW rugvista_daily_sales_v AS
WITH day_snapshots AS (
    SELECT
        (captured_at AT TIME ZONE 'Europe/Stockholm')::date AS day,
        MIN(captured_at) AS day_captured_at
    FROM rugvista_variant_snapshot
    GROUP BY 1
),
day_gaps AS (
    SELECT
        day,
        day_captured_at,
        EXTRACT(EPOCH FROM (
            day_captured_at - LAG(day_captured_at) OVER (ORDER BY day_captured_at)
        )) / 3600.0 AS hours_since_prev_snapshot
    FROM day_snapshots
),
variant_lag AS (
    SELECT
        (captured_at AT TIME ZONE 'Europe/Stockholm')::date AS day,
        product_id,
        captured_at,
        available,
        price_sek,
        LAG(available)   OVER (PARTITION BY product_id ORDER BY captured_at) AS prev_available,
        LAG(captured_at) OVER (PARTITION BY product_id ORDER BY captured_at) AS prev_captured_at
    FROM rugvista_variant_snapshot
),
variant_metrics AS (
    SELECT
        day,
        prev_available IS NULL AS is_new_variant,
        CASE
            WHEN prev_available IS NOT NULL
             AND price_sek IS NOT NULL
             AND EXTRACT(EPOCH FROM (captured_at - prev_captured_at)) / 3600.0 < 48
             AND available < prev_available
            THEN prev_available - available
            ELSE 0
        END AS units_sold,
        CASE
            WHEN prev_available IS NOT NULL
             AND price_sek IS NOT NULL
             AND EXTRACT(EPOCH FROM (captured_at - prev_captured_at)) / 3600.0 < 48
             AND available < prev_available
            THEN (prev_available - available) * price_sek
            ELSE 0
        END AS revenue_sek
    FROM variant_lag
)
SELECT
    d.day,
    COALESCE(SUM(vm.units_sold), 0)::bigint AS units_sold,
    ROUND(COALESCE(SUM(vm.revenue_sek), 0), 2) AS revenue_sek,
    CASE
        WHEN COALESCE(SUM(vm.units_sold), 0) > 0
        THEN ROUND(SUM(vm.revenue_sek) / SUM(vm.units_sold), 2)
        ELSE NULL
    END AS aov,
    g.hours_since_prev_snapshot,
    COUNT(*) FILTER (WHERE vm.is_new_variant) AS new_variants
FROM day_snapshots d
JOIN day_gaps g ON g.day = d.day
LEFT JOIN variant_metrics vm ON vm.day = d.day
GROUP BY d.day, g.hours_since_prev_snapshot
ORDER BY d.day;

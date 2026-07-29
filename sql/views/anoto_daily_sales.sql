-- anoto_daily_sales_v
--
-- Replicates the stock-delta sales estimate in track_anoto_inventory.py's
-- compute_summary(), per store (anoto = inq.shop, neo = Neo Smart Pen):
--
--   - Per variant, compare `quantity` to the immediately preceding
--     snapshot (LAG ordered by snapshot_date).
--   - A decrease is counted as units sold; an increase (restock) or no
--     change contributes zero (est_sold = GREATEST(0, prev - curr),
--     matching the Python `max(0, -delta)`).
--   - Variants with no preceding snapshot are skipped (first sighting).
--   - Revenue = units sold * price from the CURRENT snapshot.
--   - Deltas only count when the preceding snapshot's date is exactly one
--     day earlier. A variant can be transiently absent from a single
--     day's fetch (e.g. 2026-05-08 for several inq.shop variants) even
--     though the pipeline itself ran that day - LAG() would otherwise
--     skip back two days and misread the gap as a single day's sale.
--     Same failure class and same fix as rugvista_daily_sales_v (see
--     KNOWN_ISSUES.md #1) - confirmed concretely on 2026-05-09, where 9
--     variants missing only from 2026-05-08 inflated that day's estimate
--     from 127 to 381 units before this guard was added.
--
-- One known, deliberate divergence from data/anoto_inventory.xlsx: the Neo
-- pipeline itself skipped 2026-07-03 entirely (missing from its own
-- daily_summary, not an extraction gap). The Python script's state file
-- only ever holds one prior value, so its 2026-07-04 run silently compared
-- against the 2026-07-02 snapshot and counted a 2-day-old change as that
-- day's sale (KNOWN_ISSUES.md #3). This view's calendar-day guard
-- correctly excludes that cross-gap delta instead of reproducing the
-- flaw, so it reports 0 units that day where the xlsx reports 1.

CREATE OR REPLACE VIEW anoto_daily_sales_v AS
WITH lag AS (
    SELECT
        snapshot_date,
        store,
        variant_id,
        price,
        quantity,
        LAG(quantity)      OVER (PARTITION BY store, variant_id ORDER BY snapshot_date) AS prev_quantity,
        LAG(snapshot_date) OVER (PARTITION BY store, variant_id ORDER BY snapshot_date) AS prev_date
    FROM anoto_variant_snapshot
)
SELECT
    snapshot_date,
    store,
    SUM(CASE WHEN snapshot_date - prev_date <= 1
             THEN GREATEST(0, prev_quantity - quantity) ELSE 0 END)::bigint AS est_sold_units,
    ROUND(SUM(CASE WHEN snapshot_date - prev_date <= 1
                   THEN GREATEST(0, prev_quantity - quantity) * price ELSE 0 END), 2) AS est_sold_revenue
FROM lag
WHERE prev_quantity IS NOT NULL
GROUP BY snapshot_date, store
ORDER BY snapshot_date, store;

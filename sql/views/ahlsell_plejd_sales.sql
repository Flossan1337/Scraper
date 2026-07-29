-- ahlsell_plejd_sales_v
--
-- Replicates compute_deltas() in track_ahlsell_plejd_inventory.py: daily
-- sales-out (customers buying from Ahlsell) and sales-in (Plejd restocking
-- Ahlsell) per category, from warehouse-level stock deltas.
--
--   - Compares each snapshot date to the immediately preceding date that
--     actually has data (matching the Python script's `sorted_dates[i-1]`
--     - if a whole day's snapshot was skipped, both this view and the
--     original script silently span the gap; see KNOWN_ISSUES.md #3).
--   - Within a compared date pair, a (article, warehouse) combination
--     missing from either side is treated as 0 stock (FULL OUTER JOIN +
--     COALESCE), matching the Python script's `set(prev_wh) | set(curr_wh)`
--     with `.get(wid, 0.0)`.
--   - A stock decrease is sales-out; an increase is sales-in.
--   - Known limitation (KNOWN_ISSUES.md #5, not fixed by this view):
--     fetch_stock() only returns warehouse rows with quantity > 0, so a
--     warehouse hitting exactly zero disappears from the raw data instead
--     of being captured as 0. If that gap spans more than one snapshot
--     date, this view (like the Python script) has no way to tell "sold
--     out same day" from "sold out over several days" and will misstate
--     the day the swing is attributed to.

CREATE OR REPLACE VIEW ahlsell_plejd_sales_v AS
WITH distinct_dates AS (
    -- Must deduplicate BEFORE applying LAG: computing LAG(snapshot_date)
    -- directly against the raw (non-aggregated) table, even with SELECT
    -- DISTINCT, evaluates the window function per row first - with many
    -- rows sharing the same date, ties resolve arbitrarily and silently
    -- produce inconsistent prev_date values (some rows in the same date
    -- pointing at that same date instead of the true previous one),
    -- roughly doubling every total. Aggregate to one row per date first.
    SELECT DISTINCT snapshot_date FROM ahlsell_stock_snapshot
),
dates AS (
    SELECT
        snapshot_date,
        LAG(snapshot_date) OVER (ORDER BY snapshot_date) AS prev_date
    FROM distinct_dates
),
curr_rows AS (
    SELECT d.snapshot_date AS curr_date, s.article, s.warehouse_id, s.quantity
    FROM dates d
    JOIN ahlsell_stock_snapshot s ON s.snapshot_date = d.snapshot_date
    WHERE d.prev_date IS NOT NULL
),
prev_rows AS (
    SELECT d.snapshot_date AS curr_date, s.article, s.warehouse_id, s.quantity AS prev_quantity
    FROM dates d
    JOIN ahlsell_stock_snapshot s ON s.snapshot_date = d.prev_date
    WHERE d.prev_date IS NOT NULL
),
joined AS (
    SELECT
        COALESCE(c.curr_date, p.curr_date)       AS curr_date,
        COALESCE(c.article, p.article)           AS article,
        COALESCE(c.warehouse_id, p.warehouse_id) AS warehouse_id,
        COALESCE(c.quantity, 0)      AS curr_qty,
        COALESCE(p.prev_quantity, 0) AS prev_qty
    FROM curr_rows c
    FULL OUTER JOIN prev_rows p
        ON c.curr_date = p.curr_date
       AND c.article = p.article
       AND c.warehouse_id = p.warehouse_id
),
with_category AS (
    SELECT
        j.curr_date,
        COALESCE(a.category, 'Övrigt') AS category,
        j.curr_qty - j.prev_qty AS delta
    FROM joined j
    LEFT JOIN ahlsell_article a ON a.article = j.article
)
SELECT
    curr_date AS snapshot_date,
    category,
    SUM(CASE WHEN delta < 0 THEN -delta ELSE 0 END) AS sales_out,
    SUM(CASE WHEN delta > 0 THEN delta  ELSE 0 END) AS sales_in
FROM with_category
GROUP BY curr_date, category
ORDER BY curr_date, category;

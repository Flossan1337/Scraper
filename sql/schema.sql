-- rugvista_variant_snapshot
-- One row per product variant per snapshot capture: historical backfill from
-- raw/rugvista_state/*.json (scripts/tools/load_rugvista_history.py) plus
-- live daily runs (scripts/track_rugvista_daily_sales.py).
CREATE TABLE IF NOT EXISTS rugvista_variant_snapshot (
    captured_at    timestamptz NOT NULL,
    product_id     bigint      NOT NULL,
    sku            text,
    parent_name    text,
    variant_name   text,
    size_label     text,
    length_cm      int,
    width_cm       int,
    price_sek      numeric,
    available      int,
    snapshot_date  date        NOT NULL,
    PRIMARY KEY (captured_at, product_id)
);

-- Day-level dedup key: lets the live script use ON CONFLICT (snapshot_date, product_id)
-- so re-running it the same day doesn't create a second row per variant, even
-- though captured_at differs slightly between runs.
CREATE UNIQUE INDEX IF NOT EXISTS rugvista_variant_snapshot_day_uk
    ON rugvista_variant_snapshot (snapshot_date, product_id);

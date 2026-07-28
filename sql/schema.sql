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

-- ahlsell_stock_snapshot
-- One row per article per warehouse per day (Ahlsell/Plejd inventory tracking).
CREATE TABLE IF NOT EXISTS ahlsell_stock_snapshot (
    snapshot_date  date NOT NULL,
    article        text NOT NULL,
    warehouse_id   text NOT NULL,
    quantity       numeric,
    PRIMARY KEY (snapshot_date, article, warehouse_id)
);

-- ahlsell_article
-- Article metadata. category is categorized once via
-- track_ahlsell_plejd_inventory.categorize() at load/write time and stored as
-- data, rather than re-derived from hardcoded article numbers in Python.
CREATE TABLE IF NOT EXISTS ahlsell_article (
    article       text PRIMARY KEY,
    product_name  text,
    product_code  text,
    page_url      text,
    category      text
);

-- ahlsell_warehouse
-- Store/warehouse metadata.
CREATE TABLE IF NOT EXISTS ahlsell_warehouse (
    warehouse_id  text PRIMARY KEY,
    name          text,
    city          text,
    address       text
);

-- kpi_history
-- Daily Adtraction platform KPIs (total conversions, brand count) scraped by
-- fetch_kpi.py. data/kpi-history.xlsx is itself the full append-only
-- history (not a "latest snapshot" state file), so this table was
-- backfilled by reading that file directly instead of via git archaeology.
CREATE TABLE IF NOT EXISTS kpi_history (
    snapshot_date  date NOT NULL,
    conversions    bigint,
    brands         int,
    PRIMARY KEY (snapshot_date)
);

-- rugvista_bestseller_prices
-- Daily median/average price across Rugvista's bestseller list pages,
-- scraped by track_rugvista_bestsellers.py. data/rugvista_bestsellers.xlsx
-- is itself the full append-only history, backfilled by reading that file
-- directly instead of via git archaeology.
CREATE TABLE IF NOT EXISTS rugvista_bestseller_prices (
    snapshot_date   date NOT NULL,
    median_price    numeric,
    average_price   numeric,
    PRIMARY KEY (snapshot_date)
);

-- plejd_sensortower_rankings
-- Daily Plejd App Store category ranking (Lifestyle, Top Free iPhone) per
-- country, scraped by fetch_plejd_sensortower_rankings.py. Backfilled by
-- melting the wide data/plejd_sensortower_rankings.xlsx (one column per
-- country, append-only) into one row per (snapshot_date, country); missing
-- ranks (app unranked that day in that country) are simply absent rows,
-- not fabricated zeros.
CREATE TABLE IF NOT EXISTS plejd_sensortower_rankings (
    snapshot_date  date NOT NULL,
    country        text NOT NULL,
    rank           int,
    PRIMARY KEY (snapshot_date, country)
);

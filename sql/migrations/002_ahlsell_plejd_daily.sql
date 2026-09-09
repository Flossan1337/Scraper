-- 002_ahlsell_plejd_daily.sql
--
-- Adds the central-warehouse / price view of Ahlsell's Plejd assortment,
-- which the anonymous API never exposed (see KNOWN_ISSUES.md).
--
-- Shape follows the externally-sourced SQLite database this table was
-- backfilled from (scripts/ahlsell_sales.db), deliberately: it is one row
-- per article per day with the quantities already split central/branch/
-- inbound, which is the grain Ahlsell's authenticated product API returns.
-- The per-warehouse breakdown lives on in ahlsell_stock_snapshot and is
-- NOT replaced by this table -- qty_butik here equals the sum of that
-- table's quantities for the same day (validated: 92.8% of article-days
-- match exactly, remainder explained by fetch-time drift).

CREATE TABLE IF NOT EXISTS ahlsell_plejd_daily (
    snapshot_date   date NOT NULL,
    variant_number  text NOT NULL,
    qty_lager       integer,          -- central warehouse (authenticated only)
    qty_butik       integer,          -- sum across branch network
    qty_inleverans  integer,          -- inbound to central, not yet received
    price_eff       numeric,          -- account-specific net price -- see `source`
    price_gnp       numeric,          -- list price, account-independent
    fetched_at      timestamptz,      -- capture instant; stock drifts during
                                      -- business hours, so this is load-bearing
    source          text NOT NULL,    -- 'backfill' (third-party) | 'own'
    PRIMARY KEY (snapshot_date, variant_number)
);

CREATE INDEX IF NOT EXISTS ahlsell_plejd_daily_variant_idx
  ON ahlsell_plejd_daily (variant_number);

-- Ahlsell's own product taxonomy and the Plejd product code, both from the
-- authenticated API. Preferred over track_ahlsell_plejd_inventory.categorize(),
-- which infers a category from the product name with hardcoded article sets.
ALTER TABLE ahlsell_article ADD COLUMN IF NOT EXISTS sku              text;
ALTER TABLE ahlsell_article ADD COLUMN IF NOT EXISTS ahlsell_category text;

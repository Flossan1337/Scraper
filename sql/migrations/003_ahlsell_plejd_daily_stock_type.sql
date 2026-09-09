-- 003_ahlsell_plejd_daily_stock_type.sql
--
-- Ahlsell's authenticated API returns globalStock as {quantity, type}. The
-- quantity alone is ambiguous, because type 4 (Beställningsvara -- an order-in
-- item Ahlsell does not stock centrally) reports quantity -1.0 as a SENTINEL,
-- not a real level, while type 1 (Restnoterad -- stocked but currently empty)
-- reports a genuine 0.
--
-- Measured 2026-09-09 across all 80 Plejd articles: 64 x type 3 (I lager,
-- 37..3517), 2 x type 1 (quantity 0), 14 x type 4 (all quantity -1.0).
--
-- qty_lager stores -1 floored to 0, matching the backfilled source's
-- convention so the series stays continuous; stock_type carries the
-- distinction the flooring destroys. NULL on backfilled rows -- the source
-- database did not record it, and it must not be invented.

ALTER TABLE ahlsell_plejd_daily ADD COLUMN IF NOT EXISTS stock_type integer;

COMMENT ON COLUMN ahlsell_plejd_daily.stock_type IS
  'Ahlsell globalStock.type: 1=Restnoterad, 2=Delvis i lager, 3=I lager, '
  '4=Beställningsvara (qty_lager floored from sentinel -1), 6=Osäker leveranstid. '
  'NULL for source=''backfill''.';

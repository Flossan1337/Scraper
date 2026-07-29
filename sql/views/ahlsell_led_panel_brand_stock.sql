-- ahlsell_led_panel_brand_stock_v
--
-- Replicates the "Varumärken" sheet in track_ahlsell_led_panel_inventory.py:
-- total recessed-LED-panel stock per brand per day, summed across all
-- articles for that brand. This is a straight aggregation, not a delta -
-- unlike Ahlsell/Plejd, this pipeline was never computing sales, only
-- current stock levels.

CREATE OR REPLACE VIEW ahlsell_led_panel_brand_stock_v AS
SELECT
    s.snapshot_date,
    a.brand,
    SUM(s.quantity) AS total_quantity
FROM ahlsell_led_panel_stock_snapshot s
JOIN ahlsell_led_panel_article a ON a.article = s.article
GROUP BY s.snapshot_date, a.brand
ORDER BY s.snapshot_date, a.brand;

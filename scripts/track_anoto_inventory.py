#!/usr/bin/env python3
"""
track_anoto_inventory.py

Tracks inq.shop (Anoto/Inq) per-variant inventory and estimates daily sales
via stock deltas, writing results to a state JSON file and an Excel workbook.

How inventory data is obtained
-------------------------------
inq.shop runs on Shopify.  Every product page contains a:

    <script type="application/json" data-section-type="product" …>
      {
        "product":         { …variants with price, SKU, title… },
        "variantInventory": {
          "<variant_id>": {"inventory_quantity": 444, "inventory_management": "shopify"},
          …
        },
        …
      }
    </script>

This block exposes real inventory counts (not just available/unavailable).
The script fetches each product page as plain HTML, extracts this JSON block
with a regex, and reads inventory_quantity per variant.

Sales estimation methodology
-----------------------------
- Negative stock delta (curr < prev) → estimated units sold.
- Positive delta                     → restock / return (ignored for revenue).
- Day 1 has no prior snapshot → all deltas are zero (first-run baseline).
- est_revenue = est_sold_units × variant_price  (currency = whatever the page
  serves, typically USD for US-session or SEK for European-session fetches).

Note on currency
----------------
Shopify may geo-price the store.  To get consistent prices regardless of where
the script runs, the FORCE_CURRENCY and FORCE_COUNTRY params below can be set.
The store's base currency is USD (Anoto/Inq is a US company); if you see SEK
prices, add "?currency=USD" to requests or set the params below.

Products tracked
----------------
Discovered automatically from /products.json on every run.
The Route shipping-protection product is filtered out (SKU = "ROUTEINS").

State file  : data/anoto_inventory_state.json
Excel output: data/anoto_inventory.xlsx
"""

import json
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

# ── Configuration ──────────────────────────────────────────────────────────────
SHOP_BASE_URL   = "https://inq.shop"

# Add ?currency=USD to page fetches to force USD pricing regardless of IP geo.
# Set to "" to use whatever the server returns.
FORCE_CURRENCY  = "USD"

SCRIPT_DIR      = Path(__file__).resolve().parent
STATE_FILE      = (SCRIPT_DIR / ".." / "data" / "anoto_inventory_state.json").resolve()
XLSX_PATH       = (SCRIPT_DIR / ".." / "data" / "anoto_inventory.xlsx").resolve()

# Pause between product-page fetches to avoid rate-limiting.
REQUEST_DELAY   = 1.5   # seconds

# Products whose ALL variants match this SKU string are skipped entirely.
SKIP_SKU        = "ROUTEINS"
# Product titles containing any of these strings are also skipped.
SKIP_TITLES     = ["Shipping Protection"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Compiled regex to extract the product section JSON from the page HTML.
_SECTION_RE = re.compile(
    r'<script[^>]+type=["\']application/json["\'][^>]+'
    r'data-section-type=["\']product["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)


# ── State I/O ──────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8-sig"))
    return {
        "daily_summary":   [],
        "last_snapshot":   {},   # {variant_id_str: stock_int}
        "product_catalog": {},   # {variant_id_str: {product_title, variant_title, sku, price, currency}}
    }


def save_state(state: dict) -> None:
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ── Product discovery ──────────────────────────────────────────────────────────

def fetch_product_handles() -> list[dict]:
    """
    Fetch all product handles from /products.json, filtering out Route and
    any other non-trackable products.

    Returns list of {id, handle, title} dicts.
    """
    url  = f"{SHOP_BASE_URL}/products.json"
    resp = requests.get(url, params={"limit": 250}, headers=HEADERS, timeout=(10, 30))
    resp.raise_for_status()
    raw = resp.json().get("products", [])

    result: list[dict] = []
    for p in raw:
        title    = p.get("title", "")
        variants = p.get("variants", [])

        # Skip shipping protection by title
        if any(pat.lower() in title.lower() for pat in SKIP_TITLES):
            continue
        # Skip if every variant has the Route SKU
        if variants and all(SKIP_SKU in (v.get("sku") or "") for v in variants):
            continue

        result.append({"id": p["id"], "handle": p["handle"], "title": title})

    return result


# ── Per-product inventory fetch ────────────────────────────────────────────────

def fetch_product_inventory(handle: str) -> tuple[dict[str, int], dict[str, dict]]:
    """
    Fetch a single product page and extract per-variant inventory + metadata.

    Returns
    -------
    inventory : {variant_id_str: inventory_quantity_int}
    catalog   : {variant_id_str: {product_title, variant_title, sku, price, currency}}
    """
    params = {}
    if FORCE_CURRENCY:
        params["currency"] = FORCE_CURRENCY

    url = f"{SHOP_BASE_URL}/products/{handle}"
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=(10, 30))
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"    [WARN] {handle}: {exc}")
        return {}, {}

    m = _SECTION_RE.search(resp.text)
    if not m:
        print(f"    [WARN] {handle}: product JSON block not found")
        return {}, {}

    try:
        data = json.loads(m.group(1))
    except json.JSONDecodeError as exc:
        print(f"    [WARN] {handle}: JSON parse error — {exc}")
        return {}, {}

    product       = data.get("product") or {}
    variant_inv   = data.get("variantInventory") or {}
    product_title = product.get("title", handle)
    currency      = FORCE_CURRENCY or "?"

    # Build variant metadata map from the embedded product object.
    variant_meta: dict[str, dict] = {}
    for v in product.get("variants") or []:
        vid = str(v.get("id", ""))
        try:
            price = float(v.get("price") or 0)
        except (TypeError, ValueError):
            price = 0.0
        variant_meta[vid] = {
            "variant_title": v.get("title", ""),
            "sku":           v.get("sku", ""),
            "price":         price,
            "available":     bool(v.get("available")),
        }

    inventory: dict[str, int]  = {}
    catalog:   dict[str, dict] = {}

    for vid_str, inv_data in variant_inv.items():
        qty = inv_data.get("inventory_quantity")
        if qty is None:
            continue
        inventory[vid_str] = int(qty)

        meta = variant_meta.get(vid_str, {})
        catalog[vid_str] = {
            "product_title": product_title,
            "variant_title": meta.get("variant_title", ""),
            "sku":           meta.get("sku", ""),
            "price":         meta.get("price", 0.0),
            "currency":      currency,
            "handle":        handle,
        }

    return inventory, catalog


def fetch_all_inventory(
    handles: list[dict],
) -> tuple[dict[str, int], dict[str, dict]]:
    """Fetch inventory for every product, returning merged dicts."""
    all_inv:     dict[str, int]  = {}
    all_catalog: dict[str, dict] = {}

    for p in handles:
        handle = p["handle"]
        title  = p["title"]
        print(f"  [{title}]  ({handle})")
        inv, cat = fetch_product_inventory(handle)
        if inv:
            print(f"    → {len(inv)} variant(s) with inventory data")
        else:
            print("    → no inventory data found")
        all_inv.update(inv)
        all_catalog.update(cat)
        time.sleep(REQUEST_DELAY)

    return all_inv, all_catalog


# ── Delta computation ──────────────────────────────────────────────────────────

def compute_summary(
    curr_inv:      dict[str, int],
    last_snapshot: dict[str, int],
    catalog:       dict[str, dict],
) -> tuple[dict, list[dict]]:
    """
    Compare current inventory to the previous snapshot and produce a summary.

    Returns
    -------
    summary     : aggregated metrics for this run
    detail_rows : one dict per variant, sorted by product title / variant title
    """
    is_first_run  = not last_snapshot
    detail_rows:  list[dict] = []
    by_product:   dict[str, dict] = {}

    total_est_units = 0
    total_est_rev   = 0.0
    total_restocks  = 0

    for vid, curr_stock in curr_inv.items():
        meta   = catalog.get(vid, {})
        ptitle = meta.get("product_title", "Unknown")
        vtitle = meta.get("variant_title", "")
        sku    = meta.get("sku", "")
        price  = meta.get("price", 0.0)

        prev_stock = last_snapshot.get(vid)
        if prev_stock is None or is_first_run:
            delta    = None   # no comparison available
            est_sold = 0
        else:
            delta    = curr_stock - prev_stock
            est_sold = max(0, -delta)   # negative delta = sold units

        est_rev    = round(est_sold * price, 2)
        is_restock = delta is not None and delta > 0

        total_est_units += est_sold
        total_est_rev   += est_rev
        if is_restock:
            total_restocks += 1

        if ptitle not in by_product:
            by_product[ptitle] = {
                "est_sold_units": 0,
                "est_rev":        0.0,
                "restocks":       0,
            }
        by_product[ptitle]["est_sold_units"] += est_sold
        by_product[ptitle]["est_rev"]        += est_rev
        if is_restock:
            by_product[ptitle]["restocks"]   += 1

        detail_rows.append({
            "variant_id":    vid,
            "product_title": ptitle,
            "variant_title": vtitle,
            "sku":           sku,
            "price":         price,
            "currency":      meta.get("currency", "?"),
            "stock_prev":    "" if (prev_stock is None or is_first_run) else prev_stock,
            "stock_curr":    curr_stock,
            "delta":         "" if (delta is None)                       else delta,
            "est_sold":      est_sold,
            "est_rev":       est_rev,
            "is_restock":    is_restock,
        })

    # Round the summary revenue
    by_product = {k: {**v, "est_rev": round(v["est_rev"], 2)} for k, v in by_product.items()}

    summary = {
        "total_variants": len(curr_inv),
        "est_sold_units": total_est_units,
        "est_revenue":    round(total_est_rev, 2),
        "currency":       FORCE_CURRENCY or "local",
        "restocks":       total_restocks,
        "by_product":     by_product,
    }

    detail_rows.sort(key=lambda r: (r["product_title"], r["variant_title"]))
    return summary, detail_rows


# ── Excel writer ───────────────────────────────────────────────────────────────

_HDR_FILL = PatternFill("solid", fgColor="1F497D")
_HDR_FONT = Font(bold=True, color="FFFFFF")


def _write_headers(ws, headers: list[str]) -> None:
    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.fill = _HDR_FILL
        cell.font = _HDR_FONT
        cell.alignment = Alignment(horizontal="center")


def _autofit(ws) -> None:
    for col in ws.columns:
        max_len = max(
            (len(str(c.value)) for c in col if c.value is not None), default=8
        )
        ws.column_dimensions[get_column_letter(col[0].column)].width = min(
            max_len + 4, 55
        )


def write_excel(state: dict) -> None:
    if XLSX_PATH.exists():
        wb = load_workbook(XLSX_PATH)
    else:
        wb = Workbook()
        for name in list(wb.sheetnames):
            del wb[name]

    all_entries = state.get("daily_summary", [])

    # ── Sheet 1: Daily Summary ────────────────────────────────────────────────
    ds = "Daily Summary"
    if ds in wb.sheetnames:
        del wb[ds]
    ws_ds = wb.create_sheet(ds, 0)
    currency_label = state.get("product_catalog", {})
    # Pull currency from catalog if available
    curr_label = "USD"
    if state.get("product_catalog"):
        first = next(iter(state["product_catalog"].values()), {})
        curr_label = first.get("currency", "USD")
    _write_headers(ws_ds, [
        "Date",
        f"Est. Sold (units)",
        f"Est. Revenue ({curr_label})",
        "Restocks",
    ])
    for entry in all_entries:
        s = entry.get("summary", {})
        ws_ds.append([
            entry.get("date", ""),
            s.get("est_sold_units", 0),
            s.get("est_revenue", 0.0),
            s.get("restocks", 0),
        ])
    _autofit(ws_ds)

    # ── Sheet 2: By Product (wide, one column-pair per product) ──────────────
    pn = "By Product"
    if pn in wb.sheetnames:
        del wb[pn]
    ws_p = wb.create_sheet(pn)

    all_prods: set[str] = set()
    for entry in all_entries:
        all_prods.update(entry.get("summary", {}).get("by_product", {}).keys())

    latest_bp = (
        all_entries[-1].get("summary", {}).get("by_product", {}) if all_entries else {}
    )
    sorted_prods = sorted(
        all_prods, key=lambda p: -latest_bp.get(p, {}).get("est_rev", 0)
    )
    prod_cols = (
        [f"{p} (Units)" for p in sorted_prods]
        + [f"{p} (Rev {curr_label})" for p in sorted_prods]
    )
    _write_headers(ws_p, ["Date"] + prod_cols)
    for entry in all_entries:
        by_p = entry.get("summary", {}).get("by_product", {})
        row: list = [entry.get("date", "")]
        for p in sorted_prods:
            row.append(by_p.get(p, {}).get("est_sold_units", 0))
        for p in sorted_prods:
            row.append(by_p.get(p, {}).get("est_rev", 0.0))
        ws_p.append(row)
    _autofit(ws_p)

    # ── Sheet 3: Latest Snapshot (per-variant detail, rebuilt from state) ─────
    snap = "Latest Snapshot"
    if snap in wb.sheetnames:
        del wb[snap]
    ws_s = wb.create_sheet(snap)
    _write_headers(ws_s, [
        "Product", "Variant", "SKU",
        f"Price ({curr_label})", "Stock Today", "Stock Yesterday",
        "Delta", "Est. Sold", f"Est. Revenue ({curr_label})",
    ])

    # Rebuild latest detail from last_snapshot + catalog for freshness
    if all_entries:
        last_detail = all_entries[-1].get("detail_rows", [])
        for row_d in last_detail:
            ws_s.append([
                row_d.get("product_title", ""),
                row_d.get("variant_title", ""),
                row_d.get("sku", ""),
                row_d.get("price", 0.0),
                row_d.get("stock_curr", ""),
                row_d.get("stock_prev", ""),
                row_d.get("delta", ""),
                row_d.get("est_sold", 0),
                row_d.get("est_rev", 0.0),
            ])
    _autofit(ws_s)

    # ── Sheet 4: Full History Detail (one row per variant per day) ────────────
    hist = "History Detail"
    if hist in wb.sheetnames:
        del wb[hist]
    ws_h = wb.create_sheet(hist)
    _write_headers(ws_h, [
        "Date", "Product", "Variant", "SKU",
        f"Price ({curr_label})", "Stock", "Delta",
        "Est. Sold", f"Est. Revenue ({curr_label})",
    ])
    for entry in all_entries:
        d = entry.get("date", "")
        for row_d in entry.get("detail_rows", []):
            ws_h.append([
                d,
                row_d.get("product_title", ""),
                row_d.get("variant_title", ""),
                row_d.get("sku", ""),
                row_d.get("price", 0.0),
                row_d.get("stock_curr", ""),
                row_d.get("delta", ""),
                row_d.get("est_sold", 0),
                row_d.get("est_rev", 0.0),
            ])
    _autofit(ws_h)

    wb.save(XLSX_PATH)
    print(f"  Saved -> {XLSX_PATH}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    today = date.today().isoformat()
    now   = datetime.now().isoformat(timespec="seconds")

    print(f"[{now}] Anoto/inq inventory tracker")
    print("=" * 60)

    state = load_state()

    # Guard: skip if already ran today.
    if state.get("daily_summary") and state["daily_summary"][-1]["date"] == today:
        print(
            f"Already ran today ({today}). Delete the last entry in daily_summary "
            f"from {STATE_FILE.name} to re-run."
        )
        return

    # ── Step 1: Discover all trackable products ───────────────────────────────
    print("\nDiscovering products ...")
    handles = fetch_product_handles()
    if not handles:
        print("No trackable products found — check shop URL.")
        return
    print(f"  {len(handles)} product(s) to track:")
    for p in handles:
        print(f"    {p['handle']}  (id={p['id']})")

    # ── Step 2: Fetch inventory from each product page ────────────────────────
    print(f"\nFetching inventory (currency={FORCE_CURRENCY or 'geo-default'}) ...")
    curr_inv, catalog = fetch_all_inventory(handles)
    print(f"\n  Total variants with inventory data: {len(curr_inv)}")

    if not curr_inv:
        print("No inventory data fetched — aborting.")
        return

    # ── Step 3: Compute stock deltas and revenue estimates ────────────────────
    print("\nComputing deltas ...")
    last_snapshot = state.get("last_snapshot") or {}
    is_first_run  = not last_snapshot

    summary, detail_rows = compute_summary(curr_inv, last_snapshot, catalog)

    if is_first_run:
        print("  (First run — no prior snapshot, all deltas are zero / baseline only.)")

    curr_label = summary.get("currency", "USD")
    print(f"  Variants tracked         : {summary['total_variants']}")
    print(f"  Est. sold today (units)  : {summary['est_sold_units']}")
    print(f"  Est. revenue today       : {summary['est_revenue']:,.2f} {curr_label}")
    print(f"  Restocks detected        : {summary['restocks']}")

    print(f"\n  By product (est. revenue {curr_label}):")
    for ptitle, pdata in sorted(
        summary["by_product"].items(), key=lambda x: -x[1]["est_rev"]
    ):
        print(
            f"    [{ptitle}]  {pdata['est_rev']:,.2f} {curr_label}"
            f"  ({pdata['est_sold_units']} units)"
            f"  restocks={pdata['restocks']}"
        )

    # ── Step 4: Update state ──────────────────────────────────────────────────
    if not isinstance(state.get("daily_summary"), list):
        state["daily_summary"] = []

    state["daily_summary"].append({
        "date":        today,
        "timestamp":   now,
        "summary":     summary,
        "detail_rows": detail_rows,
    })
    state["last_snapshot"]   = curr_inv
    # Merge catalog (keeps metadata for variants that may have disappeared)
    state["product_catalog"] = {**state.get("product_catalog", {}), **catalog}

    save_state(state)
    print(f"\n  State saved -> {STATE_FILE.name}")

    # ── Step 5: Write Excel ───────────────────────────────────────────────────
    print("Writing Excel ...")
    write_excel(state)

    print("\nDone.")


if __name__ == "__main__":
    main()

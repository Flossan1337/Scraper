#!/usr/bin/env python3
"""
track_rvrc_inventory.py

Tracks RevolutionRace inventory at variant (size × colour) level across multiple
markets (SE, DE, NO, UK, COM) using the Voyado Elevate API directly.

Methodology
-----------
Each daily run:
  1. Queries the Voyado Elevate storefront API for every top-level product
     category (clothing, accessories, shoes) in each market, paginating with
     limit=600 until all products are retrieved.
  2. Extracts per-variant stockNumber, sellingPrice, and listPrice from the
     structured JSON response (no HTML parsing or payload decoding required).
  3. Compares against the previous snapshot stored in the state file.
  4. Estimated units sold per variant = max(0, prev_stock − curr_stock).
     Stock increases (restocks / new colours) are excluded from the estimate.
  5. Revenue is calculated twice per variant:
       - sell revenue ≈ units × sellingPrice   (customer-facing / post-discount)
       - list revenue ≈ units × listPrice      (full / pre-discount price)
     Revenue is denominated in EUR using DE market prices as the primary source
     (DACH accounts for ~60 % of RVRC sales).  The live EUR/SEK rate is stored
     separately so the user can convert to SEK if desired.
  6. Tracks all five markets to maximise variant coverage and catch products
     listed only in DE, NO, UK, or COM.

How the data is fetched
-----------------------
The RVRC site uses Voyado Elevate (Apptus) as its product search/catalog backend.
The storefront API is publicly accessible without authentication — customerKey
and sessionKey are random UUIDs generated fresh each run.

Endpoint: GET https://{cluster}.api.esales.apptus.cloud/api/storefront/v3/queries/landing-page
Required param: pageReference (e.g. "clothing", "accessories", "shoes")
Optional params: limit (max 600), skip (pagination offset), market, locale

The response structure is:
  {primaryList: {productGroups: [{products: [{variants: [{key, stockNumber,
   sellingPrice, listPrice, size, label}]}]}], totalHits: N}}

Pagination uses skip (offset by productGroup count) until skip >= totalHits.
Typically 5–15 total requests cover all 5 markets, completing in ~30 seconds.

State file  : data/rvrc_inventory_state.json
Excel output: data/rvrc_inventory.xlsx
"""

import json
import time
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

# ── Elevate API configuration ──────────────────────────────────────────────────
ELEVATE_CLUSTER_ID = "wA4BFC9F5"
ELEVATE_BASE_URL   = f"https://{ELEVATE_CLUSTER_ID}.api.esales.apptus.cloud"
ELEVATE_ENDPOINT   = "/api/storefront/v3/queries/landing-page"
ELEVATE_LIMIT      = 600  # max allowed by API (1000 returns 400 error)

# Top-level Elevate category pageReference values covering all RVRC products.
# clothing ~1,700 products, accessories ~180, shoes ~70 (SE market; similar for others).
ELEVATE_CATEGORIES = ["clothing", "accessories", "shoes"]

# ── Market configuration ───────────────────────────────────────────────────────
# Each market is fetched independently for pricing/currency purposes.
# NOTE: RVRC uses a single shared central inventory pool via Voyado Elevate —
# the same variant key with the same stockNumber appears in every market.
# Sales deltas are therefore deduplicated by variant key in compute_sales()
# (priority: DE > SE > NO > UK > COM).  DE is listed first so that revenue
# is naturally denominated in EUR for the vast majority of variants.
MARKETS: dict[str, dict] = {
    "SE":  {"elevate_market": "SE",  "locale": "sv-SE",  "currency": "SEK"},
    "DE":  {"elevate_market": "DE",  "locale": "de-DE",  "currency": "EUR"},
    "NO":  {"elevate_market": "NO",  "locale": "nb-NO",  "currency": "NOK"},
    "UK":  {"elevate_market": "UK",  "locale": "en-GB",  "currency": "GBP"},
    "COM": {"elevate_market": "EU",  "locale": "en-001", "currency": "EUR"},
}

# FX fallback rates (local currency → SEK) used if the Frankfurter API is down
FX_FALLBACKS: dict[str, float] = {
    "SEK": 1.0,
    "EUR": 11.5,
    "NOK": 0.97,
    "GBP": 13.0,
}

SCRIPT_DIR = Path(__file__).resolve().parent
STATE_FILE = (SCRIPT_DIR / ".." / "data" / "rvrc_inventory_state.json").resolve()
XLSX_PATH  = (SCRIPT_DIR / ".." / "data" / "rvrc_inventory.xlsx").resolve()

# ── State I/O ──────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        raw = json.loads(STATE_FILE.read_text(encoding="utf-8-sig"))
        # Migrate old single-market format (has "variants" key instead of "markets")
        snap = raw.get("last_snapshot") or {}
        if snap and "variants" in snap and "markets" not in snap:
            print("  [state] Old single-market format detected — treating as no prior snapshot.")
            raw["last_snapshot"] = None
        return raw
    return {"last_snapshot": None, "daily_sales": []}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ── FX rates ───────────────────────────────────────────────────────────────────

def fetch_fx_rates() -> dict[str, float]:
    """
    Fetch current exchange rates to SEK for all non-SEK market currencies via
    the Frankfurter API (free, ECB-sourced).  Falls back to FX_FALLBACKS.

    Returns {currency_code: sek_per_unit}, e.g. {"SEK": 1.0, "EUR": 11.5, ...}
    """
    non_sek = sorted({cfg["currency"] for cfg in MARKETS.values() if cfg["currency"] != "SEK"})
    rates: dict[str, float] = {"SEK": 1.0}
    if not non_sek:
        return rates
    try:
        resp = requests.get(
            f"https://api.frankfurter.app/latest?from=EUR&to=SEK,{','.join(non_sek)}",
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()["rates"]
        eur_sek = float(data["SEK"])
        rates["EUR"] = eur_sek
        for ccy in non_sek:
            if ccy == "EUR":
                continue
            if ccy in data:
                # Cross-rate: 1 ccy = (EUR/SEK) / (EUR/ccy) SEK
                rates[ccy] = eur_sek / float(data[ccy])
            else:
                rates[ccy] = FX_FALLBACKS.get(ccy, 1.0)
                print(f"  [FX] {ccy} not in API response — using fallback {rates[ccy]}")
        print(f"  [FX] {rates}")
    except Exception as e:
        print(f"  [FX] API error ({e}) — using fallbacks")
        for ccy in non_sek:
            rates[ccy] = FX_FALLBACKS.get(ccy, 1.0)
    return rates


# ── Elevate API helpers ────────────────────────────────────────────────────────

def _get_price(price_raw) -> float:
    """Parse a price that may be a float or {'min': x, 'max': x} dict."""
    if isinstance(price_raw, dict):
        try:
            return float(price_raw.get("min") or 0.0)
        except (TypeError, ValueError):
            return 0.0
    try:
        return float(price_raw) if price_raw is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


# Ordered keyword → display-name mapping for _parse_category().
# Matches against the second segment of the Elevate categorybreadcrumb id,
# e.g. "Men>Jackets>Waterproof Jackets" → "Jackets" → "Jackets & Vests".
_CATEGORY_KEYWORDS: list[tuple[str, str]] = [
    ("jacket",      "Jackets & Vests"),
    ("vest",        "Jackets & Vests"),
    ("trouser",     "Trousers & Pants"),
    ("pant",        "Trousers & Pants"),
    ("tight",       "Trousers & Pants"),
    ("fleece",      "Fleece & Midlayer"),
    ("midlayer",    "Fleece & Midlayer"),
    ("sweater",     "Fleece & Midlayer"),
    ("base layer",  "Base Layer"),
    ("baselayer",   "Base Layer"),
    ("t-shirt",     "T-Shirts & Tops"),
    ("shirt",       "T-Shirts & Tops"),
    ("top",         "T-Shirts & Tops"),
    ("hoodie",      "Hoodies & Sweatshirts"),
    ("sweatshirt",  "Hoodies & Sweatshirts"),
    ("shoe",        "Shoes"),
    ("boot",        "Shoes"),
    ("sock",        "Accessories"),
    ("cap",         "Accessories"),
    ("hat",         "Accessories"),
    ("glove",       "Accessories"),
    ("bag",         "Accessories"),
    ("backpack",    "Accessories"),
    ("accessori",   "Accessories"),
]


def _parse_category(breadcrumb_id: str, page_ref: str) -> str:
    """
    Derive a clean product category label from an Elevate categorybreadcrumb id
    (e.g. "Men>Jackets>Waterproof Jackets") or fall back to the top-level page
    reference ("clothing", "accessories", "shoes").
    """
    if breadcrumb_id:
        parts = [p.strip() for p in breadcrumb_id.split(">")]
        raw   = parts[1] if len(parts) >= 2 else parts[0]
        lower = raw.lower()
        for kw, label in _CATEGORY_KEYWORDS:
            if kw in lower:
                return label
        return raw  # preserve unrecognised breadcrumb segment as-is
    return {"clothing": "Clothing", "accessories": "Accessories", "shoes": "Shoes"}.get(
        page_ref, page_ref.capitalize()
    )


def fetch_elevate_page(
    elevate_market: str, locale: str, page_ref: str,
    skip: int = 0, customer_key: str = "", session_key: str = "",
) -> Optional[dict]:
    """Fetch one page from the Elevate landing-page API."""
    params = {
        "market":       elevate_market,
        "locale":       locale,
        "customerKey":  customer_key,
        "sessionKey":   session_key,
        "touchpoint":   "desktop",
        "pageReference": page_ref,
        "limit":        ELEVATE_LIMIT,
        "skip":         skip,
    }
    try:
        resp = requests.get(
            ELEVATE_BASE_URL + ELEVATE_ENDPOINT,
            params=params,
            timeout=(10, 60),
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        print(f"    [WARN] Elevate {page_ref} skip={skip}: {exc}")
        return None


def extract_variants_from_elevate(data: dict, page_ref: str = "") -> tuple[dict[str, dict], int, int]:
    """
    Extract variant data from an Elevate landing-page response.

    Returns
    -------
    (variants_dict, total_hits, group_count)
    variants_dict: {variant_key: {"stock", "sell_price", "list_price", "title", "size", "category"}}
    total_hits:    totalHits from the API response (used for pagination)
    group_count:   number of productGroups in this page (pagination offset increment)
    """
    variants: dict[str, dict] = {}
    pl = data.get("primaryList") or {}
    total_hits = int(pl.get("totalHits") or 0)
    groups = pl.get("productGroups") or []
    for group in groups:
        for product in group.get("products") or []:
            title  = str(product.get("title") or product.get("name") or "")
            p_sell = _get_price(product.get("sellingPrice"))
            p_list = _get_price(product.get("listPrice")) or p_sell
            # Derive product category from Elevate categorybreadcrumb attribute
            custom      = product.get("custom") or {}
            breadcrumbs = custom.get("categorybreadcrumb") or []
            bc_id       = breadcrumbs[0].get("id", "") if breadcrumbs else ""
            category    = _parse_category(bc_id, page_ref)
            for variant in product.get("variants") or []:
                key = variant.get("key")
                if not key or not isinstance(key, str):
                    continue
                try:
                    stock = int(variant.get("stockNumber") or 0)
                except (TypeError, ValueError):
                    stock = 0
                sell_price = _get_price(variant.get("sellingPrice")) or p_sell
                list_price = _get_price(variant.get("listPrice")) or p_list or sell_price
                size = str(variant.get("size") or variant.get("label") or "")
                variants[key] = {
                    "stock":      stock,
                    "sell_price": sell_price,
                    "list_price": list_price,
                    "title":      title,
                    "size":       size,
                    "category":   category,
                }
    return variants, total_hits, len(groups)


# Minimum expected unique variants per market.  If a market returns fewer than
# this after fetching all categories, a loud warning is printed.  The Elevate
# API typically returns ~13,000–16,000 variants per market; 10,000 is a safe
# lower threshold that catches silent failures.
MIN_EXPECTED_VARIANTS: dict[str, int] = {
    "SE":  10000,
    "DE":  10000,
    "NO":  10000,
    "UK":  10000,
    "COM": 10000,
}


def fetch_all_by_market() -> dict[str, dict[str, dict]]:
    """
    Fetch variants for every market using the Voyado Elevate API directly.
    ~5–15 total requests across all markets (<30s) vs old ~600 requests (~40min).

    Returns
    -------
    {market_code: {variant_key: {"stock", "sell_price", "list_price", "title", "size"}}}
    """
    result: dict[str, dict[str, dict]] = {}

    for market_code, cfg in MARKETS.items():
        elevate_market = cfg["elevate_market"]
        locale         = cfg["locale"]
        customer_key   = str(uuid.uuid4())
        session_key    = str(uuid.uuid4())
        print(f"\n[{market_code}] Elevate API (market={elevate_market}, locale={locale})")

        market_variants: dict[str, dict] = {}

        for cat in ELEVATE_CATEGORIES:
            skip, page = 0, 1
            while True:
                data = fetch_elevate_page(
                    elevate_market, locale, cat, skip, customer_key, session_key
                )
                if data is None:
                    break
                new_v, total_hits, pg_count = extract_variants_from_elevate(data, cat)
                market_variants.update(new_v)
                print(
                    f"  {cat} p{page}: +{len(new_v)} variants "
                    f"[{skip}–{skip + pg_count}/{total_hits}]"
                )
                skip += pg_count
                if skip >= total_hits or pg_count == 0:
                    break
                page += 1

        variant_count = len(market_variants)
        min_expected  = MIN_EXPECTED_VARIANTS.get(market_code, 5000)
        status = "OK" if variant_count >= min_expected else "LOW"
        print(f"  [{market_code}] {variant_count:,} unique variants | status: {status}")
        if status == "LOW":
            print(
                f"  *** WARNING [{market_code}]: Only {variant_count:,} variants "
                f"(expected >= {min_expected:,}). Possible API change or fetch failure. ***"
            )

        result[market_code] = market_variants

    return result


# ── Sales delta calculation ────────────────────────────────────────────────────

def compute_sales(
    prev_snapshot: Optional[dict],
    curr_by_market: dict[str, dict[str, dict]],
    fx_rates: dict[str, float],
) -> tuple[list[dict], dict, int, float, float, int, int]:
    """
    Compare current per-market stock snapshots to the previous ones.

    Deduplication: RVRC uses a single shared central inventory pool via Voyado
    Elevate, so the same variant key appears in every market.  Markets are
    processed in priority order (DE first) and each variant key is counted at
    most once.  Using DE as the top priority means revenue is denominated in
    EUR by default; the rare SE-only or NOK/GBP-priced variants are converted
    to EUR via cross-rates.

    Revenue is in EUR for all variants:
      - DE / COM variants (EUR): fx_to_eur = 1.0
      - Other-currency fallbacks: local_price × (local_ccy/SEK) / (EUR/SEK)

    Returns
    -------
    per_variant        : list of per-variant sale dicts (only variants with sales)
    by_category        : {category: {units, revenue_sell_eur, revenue_list_eur,
                           variants_with_sales}}
    total_units        : aggregate deduplicated units
    total_rev_sell_eur : aggregate sell revenue in EUR
    total_rev_list_eur : aggregate list revenue in EUR
    total_tracked      : unique variant keys across all markets
    total_restocks     : total restock events observed
    """
    eur_sek = fx_rates.get("EUR", 11.5)

    if prev_snapshot is None:
        print("  No previous snapshot - recording baseline (sales = 0 today).")
        total_tracked = len({k for m in curr_by_market.values() for k in m})
        return [], {}, 0, 0.0, 0.0, total_tracked, 0

    prev_markets: dict = prev_snapshot.get("markets", {})
    per_variant: list[dict] = []
    by_category: dict[str, dict] = {}

    # DE first so revenue is in EUR for the overwhelming majority of variants.
    MARKET_PRIORITY = ["DE", "SE", "NO", "UK", "COM"]
    counted_keys: set[str] = set()
    total_restocks = 0

    for market_code in MARKET_PRIORITY:
        if market_code not in curr_by_market:
            continue
        curr_variants = curr_by_market[market_code]
        currency  = MARKETS[market_code]["currency"]
        # Multiply local price by fx_to_eur to get EUR amount
        fx_to_eur = fx_rates.get(currency, 1.0) / eur_sek
        prev_variants = prev_markets.get(market_code, {})

        for key, curr in curr_variants.items():
            if key in counted_keys:
                continue
            prev = prev_variants.get(key)
            if prev is None:
                continue
            delta = prev["stock"] - curr["stock"]
            if delta < 0:
                total_restocks += 1
                continue
            if delta == 0:
                continue

            counted_keys.add(key)
            rev_sell_eur = delta * curr["sell_price"] * fx_to_eur
            rev_list_eur = delta * curr["list_price"] * fx_to_eur
            discount_pct = (
                round(100.0 * (1.0 - curr["sell_price"] / curr["list_price"]), 1)
                if curr["list_price"] > 0 else 0.0
            )
            category = curr.get("category", "Other")
            per_variant.append({
                "key":              key,
                "category":         category,
                "title":            curr["title"],
                "size":             curr["size"],
                "units":            delta,
                "sell_price_eur":   round(curr["sell_price"] * fx_to_eur, 2),
                "list_price_eur":   round(curr["list_price"] * fx_to_eur, 2),
                "sell_revenue_eur": round(rev_sell_eur, 2),
                "list_revenue_eur": round(rev_list_eur, 2),
                "discount_pct":     discount_pct,
            })

            cat = by_category.setdefault(category, {
                "units": 0, "revenue_sell_eur": 0.0,
                "revenue_list_eur": 0.0, "variants_with_sales": 0,
            })
            cat["units"]               += delta
            cat["revenue_sell_eur"]    += rev_sell_eur
            cat["revenue_list_eur"]    += rev_list_eur
            cat["variants_with_sales"] += 1

    for cat_data in by_category.values():
        cat_data["revenue_sell_eur"] = round(cat_data["revenue_sell_eur"], 2)
        cat_data["revenue_list_eur"] = round(cat_data["revenue_list_eur"], 2)

    total_units    = sum(v["units"]            for v in by_category.values())
    total_rev_sell = sum(v["revenue_sell_eur"] for v in by_category.values())
    total_rev_list = sum(v["revenue_list_eur"] for v in by_category.values())
    total_tracked  = len({k for m in curr_by_market.values() for k in m})

    return per_variant, by_category, total_units, total_rev_sell, total_rev_list, total_tracked, total_restocks


# ── Excel output ───────────────────────────────────────────────────────────────

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
        max_len = max((len(str(c.value)) for c in col if c.value is not None), default=8)
        ws.column_dimensions[get_column_letter(col[0].column)].width = min(max_len + 4, 55)


def write_excel(state: dict, per_variant_today: list[dict]) -> None:
    if XLSX_PATH.exists():
        wb = load_workbook(XLSX_PATH)
    else:
        wb = Workbook()
        for name in list(wb.sheetnames):
            del wb[name]

    today_row   = state["daily_sales"][-1] if state["daily_sales"] else {}
    fx_snapshot = today_row.get("fx_rates", {})

    # ── Sheet 1: Daily Summary (one row appended per run) ────────────────────
    summary_name = "Daily Summary"
    if summary_name in wb.sheetnames:
        ws_sum = wb[summary_name]
    else:
        ws_sum = wb.create_sheet(summary_name)
        _write_headers(ws_sum, [
            "Date",
            "Est. Units Sold",
            "Est. Revenue Sell (EUR)",
            "Est. Revenue List (EUR)",
            "Avg Discount %",
            "Variants Tracked",
            "Variants w/ Sales",
            "Restock Events",
            "EUR/SEK",
        ])

    rev_sell = today_row.get("estimated_revenue_sell_eur", 0.0)
    rev_list = today_row.get("estimated_revenue_list_eur", 0.0)
    avg_disc = (
        round(100.0 * (1.0 - rev_sell / rev_list), 1)
        if rev_list > 0 else 0.0
    )
    ws_sum.append([
        today_row.get("date", ""),
        today_row.get("estimated_units", 0),
        round(rev_sell, 0),
        round(rev_list, 0),
        avg_disc,
        today_row.get("variants_tracked", 0),
        today_row.get("variants_with_sales", 0),
        today_row.get("restock_events", 0),
        fx_snapshot.get("EUR", ""),
    ])
    _autofit(ws_sum)

    # ── Sheet 2: By Category (one row per category per run, appended) ────────
    by_cat_name = "By Category"
    if by_cat_name in wb.sheetnames:
        ws_cat = wb[by_cat_name]
    else:
        ws_cat = wb.create_sheet(by_cat_name)
        _write_headers(ws_cat, [
            "Date",
            "Category",
            "Est. Units",
            "Revenue Sell (EUR)",
            "Revenue List (EUR)",
            "Variants w/ Sales",
        ])

    for cat_name, cdata in sorted(today_row.get("by_category", {}).items()):
        ws_cat.append([
            today_row.get("date", ""),
            cat_name,
            cdata.get("units", 0),
            round(cdata.get("revenue_sell_eur", 0.0), 0),
            round(cdata.get("revenue_list_eur", 0.0), 0),
            cdata.get("variants_with_sales", 0),
        ])
    _autofit(ws_cat)

    # ── Sheet 3: Latest Detail (replaced each run) ────────────────────────────
    detail_name = "Latest Detail"
    if detail_name in wb.sheetnames:
        del wb[detail_name]
    ws_det = wb.create_sheet(detail_name)
    _write_headers(ws_det, [
        "Variant Key",
        "Product Title",
        "Category",
        "Size",
        "Units Sold",
        "Sell Price (EUR)",
        "List Price (EUR)",
        "Sell Revenue (EUR)",
        "List Revenue (EUR)",
        "Discount %",
    ])
    for row in sorted(per_variant_today, key=lambda x: -x["sell_revenue_eur"]):
        ws_det.append([
            row["key"],
            row["title"],
            row["category"],
            row["size"],
            row["units"],
            row["sell_price_eur"],
            row["list_price_eur"],
            round(row["sell_revenue_eur"], 0),
            round(row["list_revenue_eur"], 0),
            row["discount_pct"],
        ])
    _autofit(ws_det)

    wb.save(XLSX_PATH)
    print(f"  Saved -> {XLSX_PATH}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    today = date.today().isoformat()
    now   = datetime.now().isoformat(timespec="seconds")

    print(f"[{now}] RevolutionRace multi-market inventory tracker")
    print("=" * 60)

    state = load_state()

    # Guard: skip if already ran today
    if state["daily_sales"] and state["daily_sales"][-1]["date"] == today:
        print(f"Already ran today ({today}). Delete the last entry in daily_sales "
              f"from {STATE_FILE.name} to re-run.")
        return

    print("\nFetching live FX rates ...")
    fx_rates = fetch_fx_rates()

    print("\nFetching inventory across all markets and categories ...")
    curr_by_market = fetch_all_by_market()

    total_variants = sum(len(v) for v in curr_by_market.values())
    print(f"\nTotal variants fetched across all markets: {total_variants:,}")

    if total_variants == 0:
        print("No variants fetched — check URLs and network connectivity.")
        return

    print("\nComputing sales delta vs. previous snapshot ...")
    per_variant, by_category, total_units, total_rev_sell, total_rev_list, total_tracked, total_restocks = compute_sales(
        state.get("last_snapshot"),
        curr_by_market,
        fx_rates,
    )
    print(f"  Est. units sold       : {total_units:,}")
    print(f"  Est. sell revenue     : {total_rev_sell:,.0f} EUR")
    print(f"  Est. list revenue     : {total_rev_list:,.0f} EUR")
    print(f"  EUR/SEK               : {fx_rates.get('EUR', 'n/a')}")
    print(f"  Variants w/ sales     : {len(per_variant):,} / {total_tracked:,}")
    print(f"  Restock events        : {total_restocks:,}")
    for cat_name, cdata in sorted(by_category.items(), key=lambda x: -x[1]["revenue_sell_eur"]):
        print(f"  [{cat_name}] {cdata['units']:,} units | "
              f"{cdata['revenue_sell_eur']:,.0f} EUR sell | "
              f"{cdata['revenue_list_eur']:,.0f} EUR list")

    # Persist new snapshot
    state["last_snapshot"] = {
        "date":      today,
        "timestamp": now,
        "markets": {
            mkt: {
                k: {
                    "stock":      v["stock"],
                    "sell_price": v["sell_price"],
                    "list_price": v["list_price"],
                }
                for k, v in variants.items()
            }
            for mkt, variants in curr_by_market.items()
        },
    }
    state["daily_sales"].append({
        "date":                      today,
        "estimated_units":           total_units,
        "estimated_revenue_sell_eur": round(total_rev_sell, 2),
        "estimated_revenue_list_eur": round(total_rev_list, 2),
        "variants_tracked":          total_tracked,
        "variants_with_sales":       len(per_variant),
        "restock_events":            total_restocks,
        "fx_rates":                  {k: round(v, 4) for k, v in fx_rates.items()},
        "by_category":               by_category,
    })

    save_state(state)
    print(f"\n  State saved -> {STATE_FILE.name}")

    print("Writing Excel ...")
    write_excel(state, per_variant)

    print("\nDone.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
track_rvrc_inventory.py

Tracks RevolutionRace inventory at variant (size × colour) level across multiple
markets (SE, DE, NO, UK) by fetching the Nuxt payload JSON from each category
listing page.

Methodology
-----------
Each daily run:
  1. Fetches all configured category pages (paginated) for every market via the
     /_payload.json endpoint (Nuxt 3 SSR data endpoint — no browser required).
  2. Decodes the Nuxt flat-array (@ nuxt/devalue) payload to extract per-variant
     stockNumber, sellingPrice, and listPrice.
  3. Compares against the previous snapshot stored in the state file.
  4. Estimated units sold per variant = max(0, prev_stock − curr_stock).
     Stock increases (restocks / new colours) are excluded from the estimate.
  5. Revenue is calculated twice per variant:
       - sell revenue ≈ units × sellingPrice   (customer-facing / post-discount)
       - list revenue ≈ units × listPrice      (full / pre-discount price)
     Both are converted to SEK via live FX rates (Frankfurter / ECB API).
  6. Tracks all four markets so products that exist only in DE, NO, or UK are
     not missed (confirmed: DE /bekleidung/hosen has 621 products vs SE's 610).

How the data is fetched
-----------------------
The RVRC site is Nuxt 3.  Each category page exposes its full product data at:
  https://www.revolutionrace.{tld}{category-path}/_payload.json
The payload is a flat-array (@ nuxt/devalue format).  flat[2] is a nav dict that
contains a key "Elevate Category Products <english-name>" pointing to the index
of the page object, which holds primaryList → productGroups → products → variants.

Category discovery
------------------
Rather than maintaining hardcoded category lists, the script auto-discovers
category paths at runtime via discover_category_paths().  For each market a
set of top-level "path roots" is configured (e.g. "/klader", "/skor",
"/accessoarer" for SE).  The function fetches /{base_url}/_payload.json and
regex-extracts all direct child paths under each root, so new categories RVRC
adds (new product lines, shoe launches, etc.) are automatically included the
next day.  Hardcoded fallback_paths are used only if discovery fails.

Configured path roots per market:
  SE : /klader, /skor, /accessoarer
  DE : /bekleidung, /schuhe, /accessoires
  NO : /klaer, /sko, /tilbehor
  UK : /clothing, /footwear, /accessories
  COM: /clothing, /footwear, /accessories

Products appearing in multiple categories (e.g. a fleece jacket in both
/jackor and /lager-pa-lager, or in both SE and DE) are keyed by variant_key so
they cause no double-counting in the delta calculation within a market.
Cross-market inventory is independent (same SKU, separate stock pools per market).

State file  : data/rvrc_inventory_state.json
Excel output: data/rvrc_inventory.xlsx
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

# ── Market configuration ───────────────────────────────────────────────────────
# Each market is scraped independently; variant keys are shared across markets
# but inventory pools are market-specific (same SKU has separate stock per market).

MARKETS: dict[str, dict] = {
    "SE": {
        "base_url":       "https://www.revolutionrace.se",
        "currency":       "SEK",
        "lang":           "sv-SE",
        # Top-level site sections to search for sub-categories.
        # discover_category_paths() extracts all 2nd-level paths under each root
        # from the live nav payload — picks up new categories automatically.
        "path_roots":     ["/klader", "/skor", "/accessoarer"],
        "fallback_paths": [
            "/klader/byxor",
            "/klader/jackor",
            "/klader/trojor",
            "/klader/lager-pa-lager",
            "/klader/regnklader",
            "/klader/vinterklader",
            "/klader/understall",
            "/klader/underklader-strumpor",
        ],
    },
    "DE": {
        "base_url":       "https://www.revolutionrace.de",
        "currency":       "EUR",
        "lang":           "de-DE",
        "path_roots":     ["/bekleidung", "/schuhe", "/accessoires"],
        "fallback_paths": [
            "/bekleidung/hosen",
            "/bekleidung/jacken",
            "/bekleidung/regenbekleidung",
            "/bekleidung/oberteile",
        ],
    },
    "NO": {
        "base_url":       "https://www.revolutionrace.no",
        "currency":       "NOK",
        "lang":           "nb-NO",
        "path_roots":     ["/klaer", "/sko", "/tilbehor"],
        "fallback_paths": [
            "/klaer/bukser",
            "/klaer/jakker",
            "/klaer/regntoy",
            "/klaer/superundertoy-ullundertoy",
        ],
    },
    "UK": {
        "base_url":       "https://www.revolutionrace.co.uk",
        "currency":       "GBP",
        "lang":           "en-GB",
        "path_roots":     ["/clothing", "/footwear", "/accessories"],
        "fallback_paths": [
            "/clothing/trousers",
            "/clothing/jackets",
            "/clothing/waterproofs",
            "/clothing/tops",
        ],
    },
    # 31 remaining countries (AU, CH, FI, DK, FR, NL, CA, US, JP, etc.) all
    # shop via revolutionrace.com — a single shared stock pool for all of them.
    "COM": {
        "base_url":       "https://www.revolutionrace.com",
        "currency":       "EUR",
        "lang":           "en-US",
        "path_roots":     ["/clothing", "/footwear", "/accessories"],
        "fallback_paths": [
            "/clothing/jackets",
            "/clothing/tops",
            "/clothing/trousers",
            "/clothing/base-layers",
        ],
    },
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

REQUEST_DELAY_S = 2.0  # polite wait between HTTP requests (seconds)
MAX_PAGES       = 30   # safety cap on pagination depth per category
MIN_GROUPS      = 3    # if a page returns fewer productGroups, treat as last page

# ── State I/O ──────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
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


# ── Nuxt payload decoding ──────────────────────────────────────────────────────

def _make_headers(lang: str) -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json",
        "Accept-Language": f"{lang},{lang.split('-')[0]};q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
    }

def _deref(flat: list, value):
    """
    Dereference a single value from the Nuxt flat-array payload.

    The @nuxt/devalue format stores all values in a flat list where objects and
    arrays reference other entries by integer index.  Primitive values (strings,
    booleans, null) are stored directly at their index; structured values
    (objects/arrays) may themselves contain integer references.

    One level of dereferencing is sufficient for all the fields we care about
    (stockNumber, sellingPrice, key, size, title) because the Nuxt encoder
    stores primitives at the leaf level.  We do NOT recurse further to avoid
    touching the entire 46 k-entry payload.
    """
    if isinstance(value, int) and 0 <= value < len(flat):
        return flat[value]
    return value


def _extract_primary_list(flat: list) -> Optional[dict]:
    """
    Navigate the Nuxt flat-array payload to find the Elevate Category Products
    entry that contains primaryList → productGroups → products → variants.

    The top-level data-keys object is always at flat[2].  One of its keys
    matches 'Elevate Category Products <english-path>' and its value is an
    index into the flat array pointing to the landing-page-style object
    {primaryList, recommendationLists, seo, published, contentLists, customData}.
    """
    if len(flat) < 3 or not isinstance(flat[2], dict):
        return None

    nav: dict = flat[2]
    elevate_key = next(
        (k for k in nav if k.startswith("Elevate Category Products") and not k.endswith("_Facets")),
        None,
    )
    if elevate_key is None:
        return None

    page_obj = _deref(flat, nav[elevate_key])
    if not isinstance(page_obj, dict) or "primaryList" not in page_obj:
        return None

    primary_list = _deref(flat, page_obj["primaryList"])
    return primary_list if isinstance(primary_list, dict) else None


def fetch_category_page(base_url: str, lang: str, path: str, page: int) -> Optional[dict]:
    """
    Fetch one paginated listing page via the Nuxt /_payload.json endpoint and
    return the primaryList dict (with _flat attached), or None on failure.
    """
    url = base_url + path + "/_payload.json" + (f"?page={page}" if page > 1 else "")
    try:
        resp = requests.get(url, headers=_make_headers(lang), timeout=(10, 30))
        resp.raise_for_status()
        flat = resp.json()
    except requests.RequestException as e:
        print(f"    [WARN] {url}: {e}")
        return None
    except ValueError:
        print(f"    [WARN] {url}: response is not valid JSON")
        return None

    if not isinstance(flat, list):
        print(f"    [WARN] {url}: expected flat array, got {type(flat).__name__}")
        return None

    primary_list = _extract_primary_list(flat)
    if primary_list is None:
        print(f"    [WARN] No Elevate primaryList found in {url}")
        return None

    # Re-attach the flat array so extract_variants can dereference values
    primary_list["_flat"] = flat
    return primary_list


def extract_variants(primary_list: dict) -> dict[str, dict]:
    """
    Parse productGroups → products → variants from a page's primaryList.

    Returns
    -------
    {variant_key: {"stock": int, "sell_price": float, "list_price": float,
                   "title": str, "size": str}}
    where variant_key is the RVRC key e.g. "10004_2243-XS".
    sell_price = sellingPrice (customer-facing / post-discount price).
    list_price = listPrice    (full / pre-discount price).
    Both prices are in the market's local currency.
    """
    flat: list = primary_list.get("_flat", [])

    def d(v):
        return _deref(flat, v)

    results: dict[str, dict] = {}

    pg_ref = primary_list.get("productGroups")
    product_groups = d(pg_ref) if pg_ref is not None else []
    if not isinstance(product_groups, list):
        return results

    for group_ref in product_groups:
        group = d(group_ref)
        if not isinstance(group, dict):
            continue

        for product_ref in d(group.get("products")) or []:
            product = d(product_ref)
            if not isinstance(product, dict):
                continue

            title = d(product.get("title")) or d(product.get("name")) or ""

            # Product-level prices as fallback (used when variant lacks its own)
            def _parse_price(raw) -> float:
                raw = d(raw)
                if isinstance(raw, dict):
                    return float(d(raw.get("min")) or 0.0)
                try:
                    return float(raw) if raw is not None else 0.0
                except (TypeError, ValueError):
                    return 0.0

            p_sell = _parse_price(product.get("sellingPrice"))
            p_list = _parse_price(product.get("listPrice"))
            if p_list == 0.0:
                p_list = p_sell  # fallback: treat list = sell when unavailable

            for variant_ref in d(product.get("variants")) or []:
                variant = d(variant_ref)
                if not isinstance(variant, dict):
                    continue

                key = d(variant.get("key"))
                if not key or not isinstance(key, str):
                    continue

                stock_raw = d(variant.get("stockNumber"))
                try:
                    stock = int(stock_raw) if stock_raw is not None else 0
                except (TypeError, ValueError):
                    stock = 0

                sell_raw = d(variant.get("sellingPrice"))
                try:
                    sell_price = float(sell_raw) if sell_raw is not None else p_sell
                except (TypeError, ValueError):
                    sell_price = p_sell

                list_raw = d(variant.get("listPrice"))
                try:
                    list_price = float(list_raw) if list_raw is not None else p_list
                except (TypeError, ValueError):
                    list_price = p_list
                if list_price == 0.0:
                    list_price = sell_price  # last resort fallback

                results[key] = {
                    "stock":      stock,
                    "sell_price": sell_price,
                    "list_price": list_price,
                    "title":      title if isinstance(title, str) else str(title),
                    "size":       str(d(variant.get("size")) or d(variant.get("label")) or ""),
                }

    return results


def discover_category_paths(base_url: str, lang: str, path_roots: list[str]) -> list[str]:
    """
    Auto-discover product-listing category paths for a market.

    For each root in path_roots (e.g. '/klader', '/skor') fetches that root's
    own page payload ({root}/_payload.json) and regex-extracts all direct child
    paths of the form '{root}/{slug}'.  Fetching the root page (rather than the
    homepage) ensures even low-prominence categories are found — the homepage
    nav omits some sub-categories while the root category page links to all of
    them in its sidebar/facet nav.

    Returns a deduplicated list of paths, ordered by root then alphabetically.
    Returns [] on complete failure so the caller can fall back to hardcoded paths.
    """
    seen: set[str] = set()
    paths: list[str] = []

    for root in path_roots:
        url = f"{base_url}{root}/_payload.json?__idx=0"
        try:
            resp = requests.get(url, headers=_make_headers(lang), timeout=(10, 30))
            resp.raise_for_status()
        except Exception as exc:
            print(f"  [discover] {root} payload failed: {exc}")
            continue

        raw = resp.text
        escaped = re.escape(root)
        # Match "{root}/{slug}" with no further slashes — direct children only.
        pattern = rf'"{escaped}/([a-z0-9][a-z0-9-]*)"'
        for slug in sorted(set(re.findall(pattern, raw))):
            full = f"{root}/{slug}"
            if full not in seen:
                paths.append(full)
                seen.add(full)

        time.sleep(REQUEST_DELAY_S)

    return paths


def fetch_all_by_market() -> dict[str, dict[str, dict]]:
    """
    Fetch variants for every configured market and category, handling pagination.

    Category paths are discovered dynamically from the live site nav payload
    (discover_category_paths) so new categories are picked up automatically.
    Falls back to hardcoded fallback_paths if discovery fails.

    Returns
    -------
    {market_code: {variant_key: {"stock", "sell_price", "list_price", "title", "size"}}}
    """
    result: dict[str, dict[str, dict]] = {}

    for market_code, cfg in MARKETS.items():
        base_url = cfg["base_url"]
        lang     = cfg["lang"]

        paths = discover_category_paths(base_url, lang, cfg["path_roots"])
        if paths:
            print(f"\n[{market_code}] Discovered {len(paths)} categories from {base_url} ...")
        else:
            paths = cfg["fallback_paths"]
            print(f"\n[{market_code}] Discovery failed — using {len(paths)} fallback categories from {base_url} ...")

        market_variants: dict[str, dict] = {}

        for path in paths:
            print(f"  {path} ...")
            seen_in_category: set[str] = set()

            for page in range(1, MAX_PAGES + 1):
                primary_list = fetch_category_page(base_url, lang, path, page)
                if primary_list is None:
                    break

                flat = primary_list.get("_flat", [])
                pg_ref = primary_list.get("productGroups")
                groups = _deref(flat, pg_ref) if pg_ref is not None else []
                if not isinstance(groups, list) or not groups:
                    break

                page_variants = extract_variants(primary_list)
                new_keys = set(page_variants) - seen_in_category
                if page > 1 and not new_keys:
                    print(f"    page {page}: no new keys - stopping pagination")
                    break

                seen_in_category.update(page_variants)
                market_variants.update(page_variants)

                total_hits = _deref(flat, primary_list.get("totalHits"))
                print(
                    f"    page {page}: +{len(new_keys)} new "
                    f"(category: {len(seen_in_category)}, totalHits: {total_hits})"
                )

                if len(groups) < MIN_GROUPS:
                    print(f"    page {page}: only {len(groups)} groups — last page")
                    break

                time.sleep(REQUEST_DELAY_S)

            time.sleep(REQUEST_DELAY_S)

        print(f"  [{market_code}] Total unique variants: {len(market_variants):,}")
        result[market_code] = market_variants

    return result


# ── Sales delta calculation ────────────────────────────────────────────────────

def compute_sales(
    prev_snapshot: Optional[dict],
    curr_by_market: dict[str, dict[str, dict]],
    fx_rates: dict[str, float],
) -> tuple[list[dict], dict, int, float, float, int]:
    """
    Compare current per-market stock snapshots to the previous ones.

    Logic per variant (within each market):
    - New variant (not in prev): skip — first sighting, no delta possible.
    - delta = prev_stock − curr_stock
      - delta > 0  → estimated units sold = delta
      - delta <= 0 → restock or unchanged (excluded from sales)

    Revenue is estimated at both selling price (post-discount) and list price
    (full price), then converted to SEK using the supplied FX rates.

    Returns
    -------
    per_variant  : list of per-variant sale dicts (only variants with sales)
    market_totals: {market_code: {units, revenue_sell_sek, revenue_list_sek, ...}}
    total_units  : sum across all markets
    total_rev_sell_sek: sum across all markets
    total_rev_list_sek: sum across all markets
    total_tracked: total variants tracked across all markets
    """
    if prev_snapshot is None:
        print("  No previous snapshot - recording baseline (sales = 0 today).")
        total_tracked = sum(len(v) for v in curr_by_market.values())
        empty_totals = {
            m: {"units": 0, "revenue_sell_sek": 0.0, "revenue_list_sek": 0.0,
                "variants_tracked": len(curr_by_market.get(m, {})), "variants_with_sales": 0,
                "restocks": 0}
            for m in MARKETS
        }
        return [], empty_totals, 0, 0.0, 0.0, total_tracked

    prev_markets: dict = prev_snapshot.get("markets", {})
    per_variant: list[dict] = []
    market_totals: dict[str, dict] = {}

    for market_code, curr_variants in curr_by_market.items():
        currency = MARKETS[market_code]["currency"]
        fx = fx_rates.get(currency, 1.0)
        prev_variants = prev_markets.get(market_code, {})

        m_units = 0
        m_rev_sell = 0.0
        m_rev_list = 0.0
        m_restocks = 0

        for key, curr in curr_variants.items():
            prev = prev_variants.get(key)
            if prev is None:
                continue
            delta = prev["stock"] - curr["stock"]
            if delta < 0:
                # Stock went up — restock event
                m_restocks += 1
                continue
            if delta == 0:
                continue

            rev_sell = delta * curr["sell_price"] * fx
            rev_list = delta * curr["list_price"] * fx
            m_units    += delta
            m_rev_sell += rev_sell
            m_rev_list += rev_list

            discount_pct = (
                round(100.0 * (1.0 - curr["sell_price"] / curr["list_price"]), 1)
                if curr["list_price"] > 0 else 0.0
            )
            per_variant.append({
                "key":              key,
                "market":           market_code,
                "title":            curr["title"],
                "size":             curr["size"],
                "units":            delta,
                "sell_price_local": curr["sell_price"],
                "list_price_local": curr["list_price"],
                "currency":         currency,
                "sell_revenue_sek": round(rev_sell, 2),
                "list_revenue_sek": round(rev_list, 2),
                "discount_pct":     discount_pct,
            })

        market_totals[market_code] = {
            "units":              m_units,
            "revenue_sell_sek":   round(m_rev_sell, 2),
            "revenue_list_sek":   round(m_rev_list, 2),
            "variants_tracked":   len(curr_variants),
            "variants_with_sales": sum(
                1 for v in per_variant if v["market"] == market_code
            ),
            "restocks":           m_restocks,
        }

    total_units     = sum(m["units"] for m in market_totals.values())
    total_rev_sell  = sum(m["revenue_sell_sek"] for m in market_totals.values())
    total_rev_list  = sum(m["revenue_list_sek"] for m in market_totals.values())
    total_tracked   = sum(m["variants_tracked"] for m in market_totals.values())

    return per_variant, market_totals, total_units, total_rev_sell, total_rev_list, total_tracked


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
            "Est. Revenue Sell (SEK)",
            "Est. Revenue List (SEK)",
            "Avg Discount %",
            "Variants Tracked",
            "Variants w/ Sales",
            "Restock Events",
            "EUR/SEK",
            "NOK/SEK",
            "GBP/SEK",
        ])

    rev_sell = today_row.get("estimated_revenue_sell_sek", 0.0)
    rev_list = today_row.get("estimated_revenue_list_sek", 0.0)
    avg_disc = (
        round(100.0 * (1.0 - rev_sell / rev_list), 1)
        if rev_list > 0 else 0.0
    )
    total_restocks = sum(
        mdata.get("restocks", 0) for mdata in today_row.get("by_market", {}).values()
    )
    ws_sum.append([
        today_row.get("date", ""),
        today_row.get("estimated_units", 0),
        round(rev_sell, 0),
        round(rev_list, 0),
        avg_disc,
        today_row.get("variants_tracked", 0),
        today_row.get("variants_with_sales", 0),
        total_restocks,
        fx_snapshot.get("EUR", ""),
        fx_snapshot.get("NOK", ""),
        fx_snapshot.get("GBP", ""),
    ])
    _autofit(ws_sum)

    # ── Sheet 2: By Market (one row per market per run, appended) ────────────
    by_market_name = "By Market"
    if by_market_name in wb.sheetnames:
        ws_mkt = wb[by_market_name]
    else:
        ws_mkt = wb.create_sheet(by_market_name)
        _write_headers(ws_mkt, [
            "Date",
            "Market",
            "Est. Units",
            "Revenue Sell (SEK)",
            "Revenue List (SEK)",
            "Variants Tracked",
            "Variants w/ Sales",
            "Restock Events",
        ])

    for mkt, mdata in today_row.get("by_market", {}).items():
        ws_mkt.append([
            today_row.get("date", ""),
            mkt,
            mdata.get("units", 0),
            round(mdata.get("revenue_sell_sek", 0.0), 0),
            round(mdata.get("revenue_list_sek", 0.0), 0),
            mdata.get("variants_tracked", 0),
            mdata.get("variants_with_sales", 0),
            mdata.get("restocks", 0),
        ])
    _autofit(ws_mkt)

    # ── Sheet 3: Latest Detail (replaced each run) ────────────────────────────
    detail_name = "Latest Detail"
    if detail_name in wb.sheetnames:
        del wb[detail_name]
    ws_det = wb.create_sheet(detail_name)
    _write_headers(ws_det, [
        "Variant Key",
        "Product Title",
        "Size",
        "Market",
        "Units Sold",
        "Sell Price (local)",
        "List Price (local)",
        "Currency",
        "Sell Revenue (SEK)",
        "List Revenue (SEK)",
        "Discount %",
    ])
    for row in sorted(per_variant_today, key=lambda x: -x["sell_revenue_sek"]):
        ws_det.append([
            row["key"],
            row["title"],
            row["size"],
            row["market"],
            row["units"],
            row["sell_price_local"],
            row["list_price_local"],
            row["currency"],
            round(row["sell_revenue_sek"], 0),
            round(row["list_revenue_sek"], 0),
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
    per_variant, market_totals, total_units, total_rev_sell, total_rev_list, total_tracked = compute_sales(
        state.get("last_snapshot"),
        curr_by_market,
        fx_rates,
    )
    print(f"  Est. units sold       : {total_units:,}")
    print(f"  Est. sell revenue     : {total_rev_sell:,.0f} SEK")
    print(f"  Est. list revenue     : {total_rev_list:,.0f} SEK")
    print(f"  Variants w/ sales     : {len(per_variant):,} / {total_tracked:,}")
    for mkt, mdata in market_totals.items():
        print(f"  [{mkt}] {mdata['units']:,} units | "
              f"{mdata['revenue_sell_sek']:,.0f} SEK sell | "
              f"{mdata['revenue_list_sek']:,.0f} SEK list | "
              f"{mdata['variants_tracked']:,} variants")

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
        "estimated_revenue_sell_sek": round(total_rev_sell, 2),
        "estimated_revenue_list_sek": round(total_rev_list, 2),
        "variants_tracked":          total_tracked,
        "variants_with_sales":       len(per_variant),
        "fx_rates":                  {k: round(v, 4) for k, v in fx_rates.items()},
        "by_market":                 market_totals,
    })

    save_state(state)
    print(f"\n  State saved -> {STATE_FILE.name}")

    print("Writing Excel ...")
    write_excel(state, per_variant)

    print("\nDone.")


if __name__ == "__main__":
    main()

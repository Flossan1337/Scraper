# scripts/track_rugvista_daily_sales.py
import os
import sys
import time
import json
import argparse
from datetime import datetime
from typing import Dict, Any, List, Tuple

import requests
import pandas as pd

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None

API_URL = "https://www.rugvista.se/api/productlist"

STATE_PATH = "data/rugvista_state.json"        # previous snapshot
XLSX_PATH  = "data/rugvista_daily_sales.xlsx"  # daily totals

STHLM_TZ = ZoneInfo("Europe/Stockholm") if ZoneInfo else None

def ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def now_local_iso():
    if STHLM_TZ:
        return datetime.now(STHLM_TZ).isoformat(timespec="seconds")
    return datetime.now().astimezone().isoformat(timespec="seconds")

def today_stockholm_date_str():
    if STHLM_TZ:
        return datetime.now(STHLM_TZ).date().isoformat()
    return datetime.now().astimezone().date().isoformat()

def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"),
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.rugvista.se/",
    })
    return s

def build_params(offset=0, limit=49, currency="SEK", language="sv", locale="sv-SE", top_seller=True):
    p = {
        "currency": currency,
        "defect": "false",
        "from": str(offset),
        "language": language,
        "limit": str(limit),
        "locale": locale,
        "productType": "rug",
        "sort": "relevance",
        "sort_dir": "desc",
    }
    if top_seller:
        p["topSeller"] = "true"
    return p

def fetch_page(session, params, retries=3, backoff=1.6):
    for i in range(retries):
        r = session.get(API_URL, params=params, timeout=25)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff ** (i + 1))
            continue
        r.raise_for_status()
    raise RuntimeError(f"Failed after {retries} attempts")

def iterate_parents(session, limit=49, max_pages=None, top_seller=True):
    offset, page = 0, 0
    while True:
        if max_pages is not None and page >= max_pages:
            break
        data = fetch_page(session, build_params(offset=offset, limit=limit, top_seller=top_seller))
        parents = data.get("products") or data.get("items") or data.get("data") or []
        if not parents:
            break
        yield parents
        offset += limit
        page += 1
        total = data.get("total") or data.get("totalCount")
        if total is not None and offset >= int(total):
            break
        if len(parents) < limit:
            break

def _coerce_int(x):
    try:
        return int(x) if x is not None else None
    except Exception:
        return None

def _coerce_float(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None

def current_price_sek(v: Dict[str, Any]) -> float | None:
    """
    Prefer 'price'; else fall back to sale_prices['SEK'] then regular_prices['SEK'].
    """
    p = _coerce_float(v.get("price"))
    if p is not None:
        return p
    sale_prices = v.get("sale_prices") or {}
    if isinstance(sale_prices, dict):
        p = _coerce_float(sale_prices.get("SEK"))
        if p is not None:
            return p
    regular_prices = v.get("regular_prices") or {}
    if isinstance(regular_prices, dict):
        p = _coerce_float(regular_prices.get("SEK"))
        if p is not None:
            return p
    return None

def explode_variants(parents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for parent in parents:
        parent_name = None
        dn = parent.get("display_names")
        if isinstance(dn, dict):
            parent_name = dn.get("sv") or dn.get("en") or next(iter(dn.values()), None)

        variants = parent.get("products") or []
        for v in variants:
            pid = v.get("product_id")
            if pid is None:
                continue
            rows.append({
                "parent_name": parent_name,
                "variant_name": v.get("name"),
                "product_id": _coerce_int(pid),
                "sku": v.get("sku"),
                "length_cm": _coerce_int(v.get("length")),
                "width_cm": _coerce_int(v.get("width")),
                "size_label": v.get("size"),
                "price_SEK": current_price_sek(v),
                "available": _coerce_int(v.get("available")),
                "eta": v.get("eta"),
                "url": v.get("url"),
            })
    return rows

def load_state() -> Dict[str, Any]:
    if not os.path.isfile(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state: Dict[str, Any]):
    ensure_dir(STATE_PATH)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def compute_sales_from_deltas(
    rows: List[Dict[str, Any]], prev_state: Dict[str, Any]
) -> Tuple[int, float, Dict[str, Any], Dict[str, int]]:
    """
    Returns:
      total_units_sold, total_revenue, new_state_dict, metrics
    metrics includes:
      variants_seen, matched_prev, new_variants, restock_events, restock_units,
      no_change_events, sold_units_missing_price
    """
    total_units = 0
    total_revenue = 0.0
    now_iso = now_local_iso()

    metrics = {
        "variants_seen": 0,
        "matched_prev": 0,
        "new_variants": 0,
        "restock_events": 0,
        "restock_units": 0,
        "no_change_events": 0,
        "sold_units_missing_price": 0,
    }

    new_state = {}
    for r in rows:
        metrics["variants_seen"] += 1

        pid = str(r["product_id"])
        curr_avail = r["available"]
        price = r["price_SEK"]

        new_state[pid] = {
            "available": curr_avail,
            "price_SEK": price,
            "parent_name": r.get("parent_name"),
            "variant_name": r.get("variant_name"),
            "length_cm": r.get("length_cm"),
            "width_cm": r.get("width_cm"),
            "size_label": r.get("size_label"),
            "sku": r.get("sku"),
            "url": r.get("url"),
            "snapshot_time": now_iso,
        }

        prev = prev_state.get(pid)
        if prev is None:
            metrics["new_variants"] += 1
            continue

        metrics["matched_prev"] += 1
        prev_avail = prev.get("available")

        if not (isinstance(prev_avail, int) and isinstance(curr_avail, int)):
            continue

        delta = curr_avail - prev_avail
        if delta == 0:
            metrics["no_change_events"] += 1
        elif delta > 0:
            # Restock: availability increased; not counted as sales
            metrics["restock_events"] += 1
            metrics["restock_units"] += delta
        else:
            # Sales: availability decreased
            units_sold = -delta
            if isinstance(price, (int, float)):
                total_units += units_sold
                total_revenue += units_sold * float(price)
            else:
                metrics["sold_units_missing_price"] += units_sold

    return total_units, round(total_revenue, 2), new_state, metrics

def append_daily_row_to_excel(date_str: str, total_units: int, total_revenue: float):
    ensure_dir(XLSX_PATH)
    aov = round(total_revenue / total_units, 2) if total_units > 0 else None

    new_row = pd.DataFrame([{
        "Date": date_str,
        "Total rugs sold": total_units,
        "Total sales amount (SEK)": total_revenue,
        "Average order value (SEK)": aov
    }])

    if os.path.exists(XLSX_PATH):
        existing = pd.read_excel(XLSX_PATH)
        df = pd.concat([existing, new_row], ignore_index=True)
    else:
        df = new_row

    df = df[["Date", "Total rugs sold", "Total sales amount (SEK)", "Average order value (SEK)"]]
    df.to_excel(XLSX_PATH, index=False)

def main():
    ap = argparse.ArgumentParser(description="Track Rugvista daily sold units and revenue from availability deltas.")
    ap.add_argument("--limit", type=int, default=49, help="Page size")
    ap.add_argument("--max-pages", type=int, default=None, help="Limit pages (for testing)")
    ap.add_argument("--all-products", action="store_true", help="Unset topSeller flag to fetch all rugs")
    args = ap.parse_args()

    try:
        session = make_session()

        # 1) Fetch & explode variants
        rows: List[Dict[str, Any]] = []
        for parents in iterate_parents(session, limit=args.limit, max_pages=args.max_pages, top_seller=(not args.all_products)):
            rows.extend(explode_variants(parents))

        if not rows:
            print("❌ No products/variants returned. Try --all-products or check the endpoint.", file=sys.stderr)
            sys.exit(2)

        # 2) Load previous state
        prev_state = load_state()

        # 3) Compute totals & metrics
        total_units, total_revenue, new_state, metrics = compute_sales_from_deltas(rows, prev_state)
        aov = round(total_revenue / total_units, 2) if total_units > 0 else None

        # 4) Append daily summary row to Excel
        run_date = today_stockholm_date_str()
        append_daily_row_to_excel(run_date, total_units, total_revenue)

        # 5) Save new state
        save_state(new_state)

        # 6) Terminal summary
        print("\n" + "="*64)
        print("✅ Rugvista Daily Sales — Run Summary")
        print("-"*64)
        print(f"Date (Europe/Stockholm): {run_date}")
        print(f"Variants seen:           {metrics['variants_seen']}")
        print(f"Matched previous state:  {metrics['matched_prev']}")
        print(f"New variants (no prev):  {metrics['new_variants']}")
        print(f"No-change events:        {metrics['no_change_events']}")
        print(f"Restock events:          {metrics['restock_events']}  "
              f"(units +{metrics['restock_units']})")
        print("-"*64)
        print(f"Units sold:              {total_units}")
        print(f"Total sales (SEK):       {total_revenue}")
        print(f"Average order value:     {aov if aov is not None else 'N/A'}")
        if metrics["sold_units_missing_price"] > 0:
            print(f"⚠️  Units sold with missing price: {metrics['sold_units_missing_price']} (revenue not counted)")
        print("-"*64)
        print(f"Wrote daily row to:      {XLSX_PATH}")
        print(f"Updated state file:      {STATE_PATH}")
        print("="*64 + "\n")

        sys.exit(0)

    except Exception as e:
        print(f"❌ Failure: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

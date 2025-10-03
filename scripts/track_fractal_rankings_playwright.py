# track_fractal_rankings_playwright.py
from datetime import datetime
import csv, re
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# NEW: Excel
from openpyxl import Workbook, load_workbook

HEADSET_URL = "https://www.newegg.com/Gaming-Headsets/SubCategory/ID-3767?Order=3&View=96"
CHAIR_URL   = "https://www.newegg.com/Gaming-Chairs/SubCategory/ID-3628?Order=3&View=96"

HEADSET_PRODUCTS = {
    "Fractal Design Scape Dark RGB Wireless Gaming Headset": [
        "fractal design scape dark",
        "fd-hs-sca1-01"
    ],
    "Fractal Design Scape Light RGB Wireless Gaming Headset": [
        "fractal design scape light",
        "fd-hs-sca1-02"
    ],
}

CHAIR_PRODUCTS = {
    "Fractal Design Refine Gaming Chair (Fabric Dark)": [
        "fractal design refine fabric dark",
        "refine fabric dark"
    ],
    "Fractal Design Refine Gaming Chair (Mesh Dark)": [
        "fractal design refine mesh dark",
        "refine mesh dark"
    ],
    "Fractal Design Refine Gaming Chair (Fabric Light)": [
        "fractal design refine fabric light",
        "refine fabric light"
    ],
    "Fractal Design Refine Gaming Chair (Mesh Light)": [
        "fractal design refine mesh light",
        "refine mesh light"
    ],
}

# OLD: CSV_PATH = Path("fractal_rankings.csv")
# NEW: Spara i ../data som xlsx
SCRIPT_DIR = Path(__file__).resolve().parent
XLSX_PATH = (SCRIPT_DIR / ".." / "data" / "fractal_rankings.xlsx").resolve()

def canon(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[\s\-\(\)\[\],.:/®™]+", " ", s)
    return " ".join(s.split())

def wait_page_ready(page):
    try:
        page.wait_for_load_state("domcontentloaded", timeout=15000)
    except PWTimeoutError:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=20000)
    except PWTimeoutError:
        pass
    try:
        page.wait_for_selector("a.item-title:visible", timeout=20000)
    except PWTimeoutError:
        pass

def lazy_scroll(page, steps=10, wait_ms=600):
    for _ in range(steps):
        page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        page.wait_for_timeout(wait_ms)

def collect_visible_unique_titles(page):
    """
    Returns ordered list of (href, title) for visible product titles only,
    de-duplicated by href and restricted to real product pages ('/p/').
    """
    links = page.locator("a.item-title:visible")
    n = links.count()
    seen = set()
    ordered = []
    for i in range(n):
        el = links.nth(i)
        href = (el.get_attribute("href") or "").strip()
        if not href or "/p/" not in href:
            continue
        if href in seen:
            continue
        title = el.inner_text().strip()
        if not title:
            continue
        seen.add(href)
        ordered.append((href, title))
    return ordered

def paginate_and_rank(page, url, targets_aliases, max_pages=6, debug_name=""):
    alias_map = {k: [canon(k)] + [canon(a) for a in v] for k, v in targets_aliases.items()}
    out = {k: "NA" for k in alias_map.keys()}

    page.goto(url)
    wait_page_ready(page)

    global_rank = 0
    page_idx = 0
    seen_hrefs_global = set()
    debug_seen = []

    while page_idx < max_pages:
        page_idx += 1

        lazy_scroll(page, steps=10, wait_ms=500)

        items = collect_visible_unique_titles(page)
        page_hrefs = [h for h, _ in items]
        filtered = [(h, t) for (h, t) in items if h not in seen_hrefs_global]
        seen_hrefs_global.update(h for h, _ in filtered)

        for href, title in filtered:
            debug_seen.append(title)
            global_rank += 1
            ct = canon(title)
            for canonical_name, aliases in alias_map.items():
                if out[canonical_name] != "NA" and isinstance(out[canonical_name], int):
                    continue
                if any(a in ct for a in aliases):
                    out[canonical_name] = global_rank

        if all(isinstance(v, int) for v in out.values()):
            break

        next_clicked = False
        for sel in ["a[aria-label='Next']", "a:has-text('Next')", "button:has-text('Next')"]:
            try:
                loc = page.locator(sel).first
                if loc and loc.is_enabled() and loc.is_visible():
                    href = (loc.get_attribute("href") or "")
                    if href and "javascript:void(0)" not in href:
                        loc.click()
                        wait_page_ready(page)
                        next_clicked = True
                        break
            except Exception:
                continue
        if not next_clicked:
            break

    if debug_name:
        print(f"[DEBUG] {debug_name}: counted {len(debug_seen)} visible unique items. First 10:")
        for t in debug_seen[:10]:
            print(" -", t)

    return out

# ──────────────────────────────────────
# Excel helpers (ersätter CSV)

def ensure_header_xlsx():
    XLSX_PATH.parent.mkdir(parents=True, exist_ok=True)
    if XLSX_PATH.exists():
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    header = ["Date"] + list(HEADSET_PRODUCTS.keys()) + list(CHAIR_PRODUCTS.keys())
    ws.append(header)
    wb.save(XLSX_PATH)

def append_row(all_ranks):
    header = ["Date"] + list(HEADSET_PRODUCTS.keys()) + list(CHAIR_PRODUCTS.keys())
    row = [datetime.now().strftime("%Y-%m-%d")] + [all_ranks.get(k, "NA") for k in header[1:]]
    if not XLSX_PATH.exists():
        ensure_header_xlsx()
    wb = load_workbook(XLSX_PATH)
    ws = wb.active
    ws.append(row)
    wb.save(XLSX_PATH)
    print(f"✓ Appended {row[0]} → {XLSX_PATH}")

def main():
    ensure_header_xlsx()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ))
        page = context.new_page()

        headsets = paginate_and_rank(page, HEADSET_URL, HEADSET_PRODUCTS, max_pages=3, debug_name="Headsets")
        chairs   = paginate_and_rank(page, CHAIR_URL,   CHAIR_PRODUCTS,   max_pages=6, debug_name="Chairs")

        all_ranks = {}
        all_ranks.update(headsets)
        all_ranks.update(chairs)

        append_row(all_ranks)
        print("Saved rankings:", all_ranks)

        context.close()
        browser.close()

if __name__ == "__main__":
    main()

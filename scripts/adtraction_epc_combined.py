# scripts/adtraction_epc_combined.py
# Samkör Finans + Non-Finance med robust kategorifångst och paginering.
# Kräver env: ADTRACTION_EMAIL, ADTRACTION_PASSWORD
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from statistics import median
from datetime import date
import os, re, time, json, argparse, requests, urllib.parse as _url
from openpyxl import Workbook, load_workbook

BASE_ROOT = "https://secure.adtraction.com"
BASE = f"{BASE_ROOT}/partner"
STATE_PATH = "adtraction_state.json"

DATA_DIR = "data"
XLSX_PATH = os.path.join(DATA_DIR, "adtraction_epc_medians.xlsx")
SHEET_FIN = "Finance"
SHEET_NON = "Non-Finance"

COUNTRY_URLS = {
    "Sweden":       f"{BASE}/programs.htm?cid=1&asonly=false",
    "Denmark":      f"{BASE}/programs.htm?cid=12&asonly=false",
    "Finland":      f"{BASE}/programs.htm?cid=14&asonly=false",
    "Norway":       f"{BASE}/programs.htm?cid=33&asonly=false",
    "France":       f"{BASE}/programs.htm?cid=15&asonly=false",
    "Germany":      f"{BASE}/programs.htm?cid=16&asonly=false",
    "Italy":        f"{BASE}/programs.htm?cid=22&asonly=false",
    "Spain":        f"{BASE}/programs.htm?cid=42&asonly=false",
    "Netherlands":  f"{BASE}/programs.htm?cid=32&asonly=false",
    "Poland":       f"{BASE}/programs.htm?cid=34&asonly=false",
    "Switzerland":  f"{BASE}/programs.htm?cid=44&asonly=false",
}

COUNTRY_CCY = {
    "Sweden": "SEK","Denmark": "DKK","Finland": "EUR","Norway": "NOK",
    "France": "EUR","Germany": "EUR","Italy": "EUR","Spain": "EUR",
    "Netherlands": "EUR","Poland": "PLN","Switzerland": "CHF",
}

COUNTRY_ORDER = [
    "Sweden","Denmark","Finland","Norway",
    "France","Germany","Italy","Spain",
    "Netherlands","Poland","Switzerland"
]

# --- parsning av tal/valutor ---
NUMBER_RE   = re.compile(r"(\d[\d\s\u00A0]*[.,]\d+|\d[\d\s\u00A0]*)")
CURR_CODE_RE = re.compile(r"\b(SEK|EUR|DKK|NOK|PLN|CHF)\b", re.IGNORECASE)
HAS_EUR_SYM  = re.compile(r"€")
HAS_PLN_SYM  = re.compile(r"zł", re.IGNORECASE)
HAS_CHF_SYM  = re.compile(r"\bCHF\b|(?<!\w)Fr(?!\w)")
HAS_KR_SYM   = re.compile(r"\bkr\.?\b", re.IGNORECASE)

def parse_number(text: str):
    if not text: return None
    t = " ".join(text.split()).strip().lower()
    if t in {"ingen data","no data","-","—",""}: return None
    m = NUMBER_RE.search(t)
    if not m: return None
    raw = m.group(1).replace("\u00A0"," ").replace(" ","")
    if "," in raw and "." in raw: raw = raw.replace(".","")  # 1.234,56
    try: val = float(raw.replace(",", "."))
    except ValueError: return None
    return val if abs(val) >= 1e-12 else None

def detect_currency(cell_text: str, country_name: str):
    if not cell_text: return None
    t = " ".join(cell_text.split()).strip()
    m = CURR_CODE_RE.search(t)
    if m: return m.group(1).upper()
    if HAS_EUR_SYM.search(t): return "EUR"
    if HAS_PLN_SYM.search(t): return "PLN"
    if HAS_CHF_SYM.search(t): return "CHF"
    if HAS_KR_SYM.search(t) or "SEK" in t.upper():
        if "SEK" in t.upper(): return "SEK"
        if country_name == "Sweden": return "SEK"
        if country_name == "Denmark": return "DKK"
        if country_name == "Norway":  return "NOK"
        return "SEK"
    return None

def scrape_epc_values_from_table(page, country_name: str):
    """Returnerar lista [(värde, CCY), ...] från tabeller som innehåller en 'EPC'-kolumn."""
    out = []
    tables = page.locator("table")
    for i in range(tables.count()):
        table = tables.nth(i)
        headers = table.locator("thead tr th")
        if not headers.count(): continue
        heads = [headers.nth(j).inner_text().strip().lower() for j in range(headers.count())]
        if not any("epc" in h for h in heads): continue
        epc_idx = next(idx for idx, h in enumerate(heads) if "epc" in h)
        try: table.locator("tbody tr").first.wait_for(state="visible", timeout=6000)
        except PWTimeout: pass
        rows = table.locator("tbody tr")
        for r in range(rows.count()):
            cells = rows.nth(r).locator("td")
            if cells.count() <= epc_idx: continue
            txt = cells.nth(epc_idx).inner_text()
            val = parse_number(txt)
            if val is None: continue
            ccy = detect_currency(txt, country_name)
            out.append((val, ccy))
    return out

# --- kategorier från tiles ---
FINANCE_LABELS = ["finans", "finance", "finanzen", "finanza", "finanze", "financiën", "finanse", "finanzas", "rahoitus"]

def extract_categories_via_dom(page):
    """Returnerar [(label_lower, abs_url), ...] från kategorikorten (tiles)."""
    data = page.evaluate("""
(() => {
  const out = [];
  document.querySelectorAll('a[href]').forEach(a => {
    const href = a.getAttribute('href') || '';
    const text = (a.textContent || '').trim().replace(/\\s+/g, ' ');
    if (/listadvertprograms\\.htm/i.test(href)) out.push({text, href});
  });
  return out;
})()
""") or []
    items = []
    for d in data:
        label = (d.get("text") or "").strip().lower()
        href  = (d.get("href") or "").strip()
        if not href: continue
        if href.startswith("/"): href = BASE_ROOT + href
        elif not href.startswith("http"): href = BASE_ROOT + "/" + href.lstrip("./")
        items.append((label, href))
    return items

def pick_finance_url(cat_items):
    for label, url in cat_items:
        if any(tok in label for tok in FINANCE_LABELS):
            return url
    return None

# --- robust paginering ---
def discover_pagination_urls(page, any_list_url):
    """Returnera alla sid-URL:er för samma kategori (cid/cId; page/p)."""
    parsed = _url.urlparse(any_list_url)
    q = _url.parse_qs(parsed.query)
    cid_key = "cId" if "cId" in q else ("cid" if "cid" in q else None)
    cid_val = (q.get(cid_key, [""])[0] if cid_key else "")
    hrefs = page.evaluate("""() => Array.from(document.querySelectorAll('a[href]'), a => a.getAttribute('href'))""") or []
    urls = set([any_list_url])

    def abs_url(h):
        if not h: return None
        if h.startswith("http"): return h
        if h.startswith("/"):    return BASE_ROOT + h
        return BASE_ROOT + "/" + h.lstrip("./")

    for h in hrefs:
        u = abs_url(h)
        if not u or "listadvertprograms.htm" not in u.lower(): 
            continue
        pq = _url.parse_qs(_url.urlparse(u).query)
        if cid_key and pq.get(cid_key, [""])[0] != cid_val: 
            continue
        if "page" in pq or "p" in pq:
            urls.add(u)

    def page_key(u):
        pq = _url.parse_qs(_url.urlparse(u).query)
        for k in ("page", "p"):
            if k in pq:
                try: return int(pq[k][0])
                except: pass
        return 1

    return sorted(urls, key=page_key)

# --- loginhjälp ---
def looks_like_login(page):
    url = page.url.lower()
    if "login" in url or "signin" in url: return True
    if page.locator('input[type="password"]').count() > 0: return True
    if page.locator('text=/logga in|sign in|log in/i').count() > 0: return True
    return False

def auto_login_and_save_state(p, email: str, password: str, headless: bool = True):
    browser = p.chromium.launch(headless=headless)
    context = browser.new_context(
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    )
    page = context.new_page()
    page.goto(f"{BASE}/programs.htm?asonly=false", wait_until="domcontentloaded")

    if not looks_like_login(page):
        context.storage_state(path=STATE_PATH)
        context.close(); browser.close()
        return True

    def fill(sel, val):
        loc = page.locator(sel)
        if loc.count(): loc.first.fill(val); return True
        return False

    # prova två vägar till login
    try: page.locator('a:has-text("Partner")').first.click(timeout=2000)
    except Exception: pass

    ok_email = (fill('input[type="email"]', email) or fill('input[name="email"]', email) or
                fill('input#email', email) or fill('input[id*="email" i]', email) or
                fill('input[name*="user" i]', email))
    ok_pwd = (fill('input[type="password"]', password) or fill('input[name="password"]', password) or
              fill('input#password', password) or fill('input[id*="pass" i]', password))
    if not (ok_email and ok_pwd):
        page.goto(f"{BASE}/login.htm", wait_until="domcontentloaded")
        ok_email = (fill('input[type="email"]', email) or fill('input[name="email"]', email) or
                    fill('input#email', email) or fill('input[id*="email" i]', email) or
                    fill('input[name*="user" i]', email))
        ok_pwd = (fill('input[type="password"]', password) or fill('input[name="password"]', password) or
                  fill('input#password', password) or fill('input[id*="pass" i]', password))

    clicked = False
    for sel in ['button[type="submit"]','input[type="submit"]',
                'button:has-text("Logga in")','button:has-text("Sign in")',
                'text=Logga in','text=Sign in']:
        try: page.locator(sel).first.click(timeout=1500); clicked = True; break
        except Exception: continue
    if not clicked:
        try: page.keyboard.press("Enter")
        except Exception: pass

    try: page.wait_for_load_state("networkidle", timeout=10000)
    except PWTimeout: pass

    page.goto(f"{BASE}/programs.htm?asonly=false", wait_until="domcontentloaded")
    ok = not looks_like_login(page)
    if ok: context.storage_state(path=STATE_PATH)
    context.close(); browser.close()
    return ok

# --- FX SEK ---
def fetch_fx_local_to_sek(currencies):
    need = sorted({c for c in currencies if c and c != "SEK"})
    out = {c: 1.0 for c in currencies}
    if not need: return out
    try:
        r = requests.get("https://open.er-api.com/v6/latest/SEK", timeout=15)
        r.raise_for_status(); data = r.json()
        if data.get("result") == "success":
            rates = data.get("rates", {})
            for c in need:
                per_sek = float(rates[c]); out[c] = 1.0/per_sek if per_sek else 1.0
            print("\nFX (open.er-api.com):")
            for c in need: print(f"  {c}→SEK = {out[c]:.4f}")
            return out
    except Exception as e:
        print(f"FX provider 1 failed ({e}); trying fallback…")
    symbols = ",".join(need)
    r = requests.get(f"https://api.exchangerate.host/latest?base=SEK&symbols={symbols}", timeout=15)
    r.raise_for_status(); rates = r.json().get("rates", {})
    for c in need:
        per_sek = float(rates[c]); out[c] = 1.0/per_sek if per_sek else 1.0
    print("\nFX (exchangerate.host):")
    for c in need: print(f"  {c}→SEK = {out[c]:.4f}")
    return out

# --- Excel helpers ---
def ensure_book_and_sheets(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        wb = Workbook()
        ws1 = wb.active
        ws1.title = SHEET_FIN
        ws2 = wb.create_sheet(SHEET_NON)
        header = ["date", "All (SEK)"] + [f"{c} (SEK)" for c in COUNTRY_ORDER]
        ws1.append(header); ws2.append(header)
        wb.save(path)
    else:
        wb = load_workbook(path)
        if SHEET_FIN not in wb.sheetnames:
            ws = wb.create_sheet(SHEET_FIN)
            ws.append(["date","All (SEK)"]+[f"{c} (SEK)" for c in COUNTRY_ORDER])
        if SHEET_NON not in wb.sheetnames:
            ws = wb.create_sheet(SHEET_NON)
            ws.append(["date","All (SEK)"]+[f"{c} (SEK)" for c in COUNTRY_ORDER])
        wb.save(path)

def append_row(path, sheet_name, dt, all_median, per_country):
    wb = load_workbook(path)
    ws = wb[sheet_name]
    row = [dt, (None if all_median is None else round(all_median,2))]
    for c in COUNTRY_ORDER:
        v = per_country.get(c)
        row.append(None if v is None else round(v,2))
    ws.append(row); wb.save(path)

# --- main ---
def run_for_country(page, country, fx, headful=False):
    """Skrapar både Finance & Non-Finance för ett land. Returnerar dictar med medianer i SEK och 'n'."""
    results = {"finance": {"n":0, "median": None}, "non": {"n":0, "median": None}}
    ccy_expected = COUNTRY_CCY[country]

    # Landingssida
    page.goto(COUNTRY_URLS[country], wait_until="domcontentloaded")
    try: page.wait_for_load_state("networkidle", timeout=8000)
    except PWTimeout: pass

    cats = extract_categories_via_dom(page)
    fin_url = pick_finance_url(cats)
    non_links = [url for label,url in cats if url != fin_url]

    # --- Finance ---
    if fin_url:
        values_local = []
        page.goto(fin_url, wait_until="domcontentloaded")
        try: page.wait_for_selector("table", timeout=8000)
        except PWTimeout: pass
        for u in discover_pagination_urls(page, fin_url):
            page.goto(u, wait_until="domcontentloaded")
            try: page.wait_for_selector("table", timeout=8000)
            except PWTimeout: continue
            values_local.extend(scrape_epc_values_from_table(page, country))

        if values_local:
            values_sek = [val * fx.get((ccy or ccy_expected).upper(), 1.0) for val, ccy in values_local]
            results["finance"]["n"] = len(values_local)
            results["finance"]["median"] = median(values_sek)
            # print i samma format som gamla step summary för kompatibilitet
            print(f"[{country}] n={results['finance']['n']}  median={results['finance']['median']:.2f} SEK")
        else:
            print(f"[{country}] No EPC values found (Finance).")
    else:
        print(f"[{country}] No Finance category link found.")

    # --- Non-Finance ---
    values_local_nf = []
    for link in non_links:
        page.goto(link, wait_until="domcontentloaded")
        try: page.wait_for_selector("table", timeout=8000)
        except PWTimeout: continue
        for u in discover_pagination_urls(page, link):
            page.goto(u, wait_until="domcontentloaded")
            try: page.wait_for_selector("table", timeout=8000)
            except PWTimeout: continue
            values_local_nf.extend(scrape_epc_values_from_table(page, country))

    if values_local_nf:
        values_sek_nf = [val * fx.get((ccy or ccy_expected).upper(), 1.0) for val, ccy in values_local_nf]
        results["non"]["n"] = len(values_local_nf)
        results["non"]["median"] = median(values_sek_nf)
        # kompatibel rad för din YAML-parser
        print(f"[{country}] n={results['non']['n']} median_ex_fin={results['non']['median']:.2f} SEK")
    else:
        print(f"[{country}] No EPC values found (Non-Finance).")

    return results

def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("countries", nargs="*", help="Optional filter: run only matching countries (partial ok)")
    parser.add_argument("--headful", action="store_true")
    args = parser.parse_args()

    # urval
    if args.countries:
        sel = []
        for r in args.countries:
            rlow = r.lower()
            sel.extend([c for c in COUNTRY_ORDER if rlow in c.lower()])
        seen=set(); countries=[c for c in sel if not (c in seen or seen.add(c))]
        if not countries: countries = COUNTRY_ORDER
    else:
        countries = COUNTRY_ORDER

    # FX
    fx = fetch_fx_local_to_sek({COUNTRY_CCY[c] for c in countries})

    ensure_book_and_sheets(XLSX_PATH)
    today = date.today().isoformat()

    finance_by_country = {}
    non_by_country = {}
    all_fin_vals, all_non_vals = [], []

    with sync_playwright() as p:
        # state check / login
        need_login = not os.path.exists(STATE_PATH)
        if not need_login:
            tb = p.chromium.launch(headless=True)
            tc = tb.new_context(storage_state=STATE_PATH)
            tp = tc.new_page()
            tp.goto(f"{BASE}/programs.htm?asonly=false", wait_until="domcontentloaded")
            need_login = looks_like_login(tp)
            tc.close(); tb.close()
        if need_login:
            email = os.environ.get("ADTRACTION_EMAIL","")
            pwd   = os.environ.get("ADTRACTION_PASSWORD","")
            if not (email and pwd) or not auto_login_and_save_state(p, email, pwd, headless=True):
                print("Auto-login failed or credentials missing. Please set ADTRACTION_EMAIL and ADTRACTION_PASSWORD.")
                return

        browser = p.chromium.launch(headless=not args.headful)
        context = browser.new_context(storage_state=STATE_PATH, viewport={"width":1440,"height":900} if args.headful else None)
        page = context.new_page()

        for country in countries:
            try:
                res = run_for_country(page, country, fx, headful=args.headful)
                # Finance
                if res["finance"]["n"] > 0 and res["finance"]["median"] is not None:
                    finance_by_country[country] = res["finance"]["median"]
                    all_fin_vals.extend([res["finance"]["median"]])  # eller alla values? behåll median per land för "All"? vi tar alla värden:
                # Lägg istället till alla underliggande värden för riktig "All":
                # men vi har inte listan här, så vi låter "All" bli median av ländernas medianer:
                # (vill du använda alla datapunkter globalt, säg till så sparar vi dem mellansteg)
                # Non-Fin
                if res["non"]["n"] > 0 and res["non"]["median"] is not None:
                    non_by_country[country] = res["non"]["median"]
            except Exception as e:
                print(f"[{country}] Error: {e}")

        context.close(); browser.close()

    # "All (SEK)" – median av ländernas medianer (robust; kan bytas till global median om vi behåller alla datapunkter)
    fin_all = median(list(finance_by_country.values())) if finance_by_country else None
    non_all = median(list(non_by_country.values())) if non_by_country else None

    append_row(XLSX_PATH, SHEET_FIN, today, fin_all, finance_by_country)
    append_row(XLSX_PATH, SHEET_NON, today, non_all, non_by_country)
    print(f"\nWrote {XLSX_PATH} → sheets [{SHEET_FIN}] & [{SHEET_NON}] for {today}")

if __name__ == "__main__":
    main()

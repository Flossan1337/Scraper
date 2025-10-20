# litium_radar.py
# End-to-end "Litium Radar":
# 1) Hämtar kandidater från Common Crawl per månad (Jan-2024 → nu)
# 2) Validerar Litium-fingeravtryck mot HTML/JS
# 3) Skriver/uppdaterar Excel: data/litium_radar.xlsx
#
# Kör-exempel:
#   python litium_radar.py
#   python litium_radar.py --since 2024-01 --until 2025-10 --tlds .se .no .dk .fi
#
# Not: Scriptet använder CC collinfo.json för att hitta månadsindex.
#      Du styr mängd via MAX_* konstanter nedan.

import os, sys, re, json, math, time, asyncio, argparse
from datetime import datetime, date
from collections import OrderedDict, defaultdict
from urllib.parse import urlparse, urljoin

import requests
import async_timeout
import aiohttp
from bs4 import BeautifulSoup

import pandas as pd

# ----------------- CONFIG -----------------
OUTPUT_XLSX = os.path.join("data", "litium_radar.xlsx")

# Common Crawl söktermer (unika för Litium)
TERMS = [
    "data-litium-block-id",
    "window._litium",
    "litium-request-context",
    "litium.constants"
]

# Filtrera TLD (default Norden). Tom lista => inga filter (globalt).
DEFAULT_TLDS = [".se", ".no", ".dk", ".fi"]

# Begränsningar för CC-frågor (balans mellan fart & träff)
PAGE_SIZE = 1000            # rader per CDX-sida
MAX_PAGES_PER_TERM = 5      # per term per index (t.ex. 5 * 1000 = 5k rader)
MAX_HOSTS_PER_INDEX = 8000  # hårt tak per index (alla termer tillsammans)

# Validering/fingerprinting
CONCURRENCY = 20
TIMEOUT = 12
MAX_JS_FETCH = 5            # hur många JS-filer per sajt vi kikar i

# Poängregler
SIGNATURES = [
    (r"window\._?litium", 3, "js_window._litium"),
    (r"\blitium\.constants\b", 2, "js_litium.constants"),
    (r"\blitium\.cache\b", 2, "js_litium.cache"),
    (r"\blitium\.bootstrapComponent\b", 3, "js_litium.bootstrapComponent"),
    (r"data-litium-block-id", 2, "attr_data-litium-block-id"),
    (r"litium-request-context", 2, "header_or_js_litium-request-context"),
]

RES_NAME_POINTS = 2  # om 'litium' finns i resursnamn (JS/CSS/img)

# ------------------------------------------

def yyyymm(dt: date) -> str:
    return f"{dt.year:04d}-{dt.month:02d}"

def parse_month(s: str) -> date:
    return datetime.strptime(s, "%Y-%m").date().replace(day=1)

def month_range(start: date, end: date):
    cur = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    while cur <= last:
        yield cur
        # next month
        y, m = cur.year, cur.month
        if m == 12: cur = date(y+1, 1, 1)
        else: cur = date(y, m+1, 1)

def ensure_data_dir():
    os.makedirs("data", exist_ok=True)

def load_existing_book():
    if not os.path.exists(OUTPUT_XLSX):
        return None, None
    try:
        xls = pd.ExcelFile(OUTPUT_XLSX)
        counts = pd.read_excel(xls, "monthly_counts")
    except Exception:
        counts = pd.DataFrame(columns=["Month","High","Medium","Low","Candidates"])
    try:
        newd = pd.read_excel(xls, "new_domains")
    except Exception:
        newd = pd.DataFrame()
    return counts, newd

def save_book(counts_df: pd.DataFrame, new_domains_df: pd.DataFrame):
    ensure_data_dir()
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl", mode="w") as writer:
        counts_df.sort_values("Month").to_excel(writer, index=False, sheet_name="monthly_counts")
        # för "kolumn per månad"-layout: skriv som brett df
        new_domains_df.to_excel(writer, index=False, sheet_name="new_domains")

# --- Common Crawl helpers ---

def fetch_collinfo():
    # Hämtar alla index-metadata och returnerar lista.
    url = "http://index.commoncrawl.org/collinfo.json"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def pick_indexes_for_months(since: date, until: date):
    # Välj CC-MAIN index som matchar efterfrågade månader.
    # collinfo har fält: id (CC-MAIN-YYYY-WW), name, timegate m.m.
    info = fetch_collinfo()
    # Skapa en approx-karta: månad -> lista av index-id som publicerats den månaden
    # (vi tar en per månad: den senaste som hör till den månaden)
    month_to_ids = defaultdict(list)
    for entry in info:
        cid = entry.get("id","")
        if not cid.startswith("CC-MAIN-"):
            continue
        # entry["name"] har ofta ett datumintervall; vi använder 'cdx-api' tillgänglig oavsett.
        # Vi approximerar med publiceringsdatum från 'cdx-api' länk om finns, annars hoppar vi.
        # Fallback: gissa månad från id:s "YYYY-WW" => konvertera vecka till månad ungefärligt.
        try:
            yy = int(cid.split("-")[2])
            ww = int(cid.split("-")[3])
            # approx: veckonummer -> månad (lite grovt men funkar för vårt syfte)
            approx = datetime.strptime(f"{yy}-W{ww}-1", "%Y-W%W-%w").date()
            month_key = yyyymm(approx)
            month_to_ids[month_key].append(cid)
        except Exception:
            continue

    desired = [yyyymm(m) for m in month_range(since, until)]
    chosen = []
    for mk in desired:
        ids = sorted(month_to_ids.get(mk, []))
        if ids:
            chosen.append((mk, ids[-1]))  # ta "senaste" den månaden
    return chosen  # lista av (YYYY-MM, CC-MAIN-YYYY-WW-id)

def cdx_query(index_id: str, term: str, page: int, page_size: int):
    base = f"http://index.commoncrawl.org/{index_id}"
    params = {
        "url": f"*{term}*",
        "output": "json",
        "filter": "status:200",
        "page": page,
        "limit": page_size
    }
    r = requests.get(base, params=params, timeout=45)
    if r.status_code != 200:
        return []
    return r.text.splitlines()

def extract_host(line: str):
    try:
        j = json.loads(line)
        url = j.get("url")
        if not url: return None
        host = urlparse(url).netloc.lower()
        if ":" in host: host = host.split(":")[0]
        # filtrera självklara skräpvärden
        if not host or "." not in host: return None
        return host
    except Exception:
        return None

def collect_hosts_for_index(index_id: str, terms, tlds, page_size, max_pages, hard_cap):
    seen = OrderedDict()
    total_req = 0
    for term in terms:
        for p in range(max_pages):
            lines = cdx_query(index_id, term, p, page_size)
            total_req += 1
            if not lines: break
            got = 0
            for L in lines:
                h = extract_host(L)
                if not h: continue
                if tlds:
                    if not any(h.endswith(t) for t in tlds):
                        continue
                if h not in seen:
                    seen[h] = None
                    got += 1
                    if len(seen) >= hard_cap: break
            if len(seen) >= hard_cap: break
            # liten paus för att vara snäll
            time.sleep(0.8)
        if len(seen) >= hard_cap: break
    return list(seen.keys())

# --- Validation (fingerprinting) ---

async def fetch_text(session, url):
    try:
        with async_timeout.timeout(TIMEOUT):
            async with session.get(url, allow_redirects=True) as resp:
                txt = await resp.text(errors="ignore")
                return resp.status, dict(resp.headers), txt, resp.cookies
    except Exception:
        return None, {}, "", {}

async def analyze_domain(session, domain):
    url = "https://" + domain if not domain.startswith("http") else domain
    out = {"domain": domain, "score": 0, "evidence": [], "status": None, "confidence": "low"}
    st, headers, html, cookies = await fetch_text(session, url)
    out["status"] = st
    if not st or not html:
        return out

    soup = BeautifulSoup(html, "html.parser")
    blobs = [html]

    # hämta några JS-resurser från samma origin
    js_urls = []
    for s in soup.find_all("script", src=True):
        src = s.get("src") or ""
        if not src: continue
        u = urljoin(url, src)
        try:
            if urlparse(u).netloc == urlparse(url).netloc:
                js_urls.append(u)
        except Exception:
            pass
    js_urls = js_urls[:MAX_JS_FETCH]

    for u in js_urls:
        st2, _, js_txt, _ = await fetch_text(session, u)
        if st2 and js_txt:
            blobs.append(js_txt)
        if "litium" in u.lower():
            out["score"] += RES_NAME_POINTS
            out["evidence"].append(f"res_name: {os.path.basename(urlparse(u).path)}")

    combined = "\n".join(blobs)
    for pat, pts, tag in SIGNATURES:
        if re.search(pat, combined, re.IGNORECASE):
            out["score"] += pts
            out["evidence"].append(tag)

    # headers/cookies
    if any("litium" in (v or "").lower() for v in headers.values()):
        out["score"] += 2
        out["evidence"].append("header_contains_litium")
    if any("litium" in (k or "").lower() or "litium" in (getattr(v, "value", str(v)) or "").lower()
           for k, v in (cookies or {}).items()):
        out["score"] += 2
        out["evidence"].append("cookie_contains_litium")

    if soup.select("[data-litium-block-id]"):
        out["score"] += 2
        out["evidence"].append("dom_attr_data-litium-block-id")

    out["confidence"] = "high" if out["score"] >= 7 else ("medium" if out["score"] >= 4 else "low")
    return out

async def validate_hosts(hosts):
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [analyze_domain(session, h) for h in hosts]
        return await asyncio.gather(*tasks)

# --- Excel helpers ---

def build_new_domains_wide(existing_wide: pd.DataFrame, month_key: str, new_list: list):
    """Returnerar uppdaterad 'wide' df där varje kolumn = månad, rader = domäner (olika längd tillåts)."""
    col = pd.Series(new_list, name=month_key, dtype="object")
    if existing_wide is None or existing_wide.empty:
        return pd.DataFrame({month_key: col})
    # align columns: lägg ny kolumn sist
    out = existing_wide.copy()
    # fyll ned till max längd
    max_rows = max(len(out), len(col))
    out = out.reindex(range(max_rows))
    newcol = pd.Series(col.values, index=range(len(col)))
    out[month_key] = newcol
    return out

def existing_domains_from_wide(wide_df: pd.DataFrame):
    if wide_df is None or wide_df.empty:
        return set()
    vals = set()
    for c in wide_df.columns:
        vals.update([str(x).strip().lower() for x in wide_df[c].dropna().tolist()])
    return vals

# ----------------- MAIN -----------------

def main():
    parser = argparse.ArgumentParser(description="Litium Radar (Common Crawl + validation -> Excel)")
    parser.add_argument("--since", type=str, default="2024-01", help="Startmånad YYYY-MM (default 2024-01)")
    parser.add_argument("--until", type=str, default=None, help="Slutmånad YYYY-MM (default = nu)")
    parser.add_argument("--tlds", type=str, nargs="*", default=DEFAULT_TLDS, help="Filtrera TLD, ex: .se .no .dk .fi (tom lista = globalt)")
    parser.add_argument("--max-pages", type=int, default=MAX_PAGES_PER_TERM)
    parser.add_argument("--page-size", type=int, default=PAGE_SIZE)
    parser.add_argument("--max-hosts-per-index", type=int, default=MAX_HOSTS_PER_INDEX)
    args = parser.parse_args()

    since = parse_month(args.since)
    until = parse_month(args.until) if args.until else date.today().replace(day=1)
    tlds = args.tlds if args.tlds else []

    print(f"[i] Period: {yyyymm(since)} → {yyyymm(until)} | TLD-filter: {tlds if tlds else 'GLOBAL'}")

    counts_df, new_wide_df = load_existing_book()
    already = existing_domains_from_wide(new_wide_df)  # set av historiska "nya"

    # Hämta CC-index per månad
    month_indexes = pick_indexes_for_months(since, until)
    if not month_indexes:
        print("Hittade inga CC-index för perioden.")
        return

    monthly_rows = [] if counts_df is None or counts_df.empty else counts_df.to_dict("records")
    cumulative_known = set(already)

    for month_key, index_id in month_indexes:
        print(f"\n== {month_key} | CC index: {index_id} ==")
        # 1) Hämta kandidater från CC
        hosts = collect_hosts_for_index(
            index_id=index_id,
            terms=TERMS,
            tlds=tlds,
            page_size=args.page_size,
            max_pages=args.max_pages,
            hard_cap=args.max_hosts_per_index,
        )
        print(f"  Kandidater (raw, dedup): {len(hosts)}")

        if not hosts:
            # skriv rad med 0
            monthly_rows.append({"Month": month_key, "High": 0, "Medium": 0, "Low": 0, "Candidates": 0})
            continue

        # 2) Validera
        results = asyncio.run(validate_hosts(hosts))
        high = [r for r in results if r["confidence"] == "high"]
        med  = [r for r in results if r["confidence"] == "medium"]
        low  = [r for r in results if r["confidence"] == "low"]

        print(f"  High: {len(high)} | Medium: {len(med)} | Low: {len(low)}")

        monthly_rows.append({
            "Month": month_key,
            "High": len(high),
            "Medium": len(med),
            "Low": len(low),
            "Candidates": len(hosts),
        })

        # 3) Nya domäner denna månad (ta endast High för tydlighet)
        new_this_month = []
        for r in high:
            d = r["domain"].lower().strip()
            if d not in cumulative_known:
                cumulative_known.add(d)
                new_this_month.append(d)

        # uppdatera wide-tabellen där varje kolumn = månad
        new_wide_df = build_new_domains_wide(new_wide_df, month_key, new_this_month)

    # 4) Spara Excel
    counts_df = pd.DataFrame(monthly_rows, columns=["Month","High","Medium","Low","Candidates"])
    save_book(counts_df, new_wide_df)

    print(f"\n✔ Klart. Excel uppdaterad: {OUTPUT_XLSX}")
    print("   - Flik 'monthly_counts': per månad (High/Medium/Low/Candidates)")
    print("   - Flik 'new_domains'   : nya high-confidence domäner per månad (en kolumn per månad)")
    print("\nTips: kör samma kommando nästa månad – filen appendas/uppdateras automatiskt.")
    print("      Ändra TLD-filter med --tlds för att t.ex. köra GLOBALT (ange inga TLDs).")

if __name__ == "__main__":
    main()

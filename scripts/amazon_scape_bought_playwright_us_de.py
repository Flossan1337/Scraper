import asyncio, os, random, re
from datetime import date
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

from excel_utils import append_row  # <-- skriver till Excel

# ── Spara i repo-root/data oavsett var scriptet körs ifrån ────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = (SCRIPT_DIR / ".." / "data").resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
XLSX_PATH = str((DATA_DIR / "scape_bought_by_country.xlsx").resolve())  # <-- rätt data-mapp
SHEET_NAME = "scape_bought_by_country"
# ───────────────────────────────────────────────────────────────────────────

DUMP_DIR = Path("html_dumps"); DUMP_DIR.mkdir(exist_ok=True)

# Products (ASINs)
PRODUCTS = [
    ("Scape Dark",  "B0D5HGK3C2"),
    ("Scape Light", "B0D5HK6JRS"),
]

# Only US and DE
COUNTRIES = [
    # code, domain, Accept-Language, gl, locale, cookie_name, cookie_value
    ("US","amazon.com", "en-US,en;q=0.9", "US","en-US","i18n-prefs","USD"),
    ("DE","amazon.de",  "de-DE,de;q=0.9,en;q=0.6","DE","de-DE","lc-acbde","de_DE"),
]

UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

HEADLESS = True

SELECTORS = [
    "#social-proofing-faceout-title-tk_bought span.a-text-bold",
    "#social-proofing-faceout-title-tk_bought",
    "div.social-proofing-faceout span.a-text-bold",
    'div[data-cel-widget="social-proofing-faceout"] span.a-text-bold',
    "div.social-proofing-faceout",
]

def parse_number(text: str) -> int:
    m = re.search(r"([0-9][0-9.,]*)", text)
    if not m: return 0
    return int(re.sub(r"[^\d]", "", m.group(1)) or "0")

async def get_bought_count(context, domain: str, gl: str, asin: str, code: str) -> int:
    page = await context.new_page()
    url = f"https://{domain}/dp/{asin}?th=1&psc=1&gl={gl}"
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        found_text = ""
        for sel in SELECTORS:
            try:
                el = await page.wait_for_selector(sel, state="attached", timeout=4000)
                if el:
                    found_text = (await el.inner_text()).strip()
                    if found_text: break
            except PWTimeout:
                continue

        value = parse_number(found_text) if found_text else 0

        if value == 0:
            dump = DUMP_DIR / f"{code}_{asin}.html"
            dump.write_text(await page.content(), encoding="utf-8", errors="ignore")

        return value

    except Exception:
        return 0
    finally:
        await page.close()

def build_header():
    header = ["date"]
    for code, *_ in COUNTRIES:
        header += [f"Scape Dark - {code}", f"Scape Light - {code}"]
    return header

async def run_once():
    today = date.today().isoformat()
    row_values = [today]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        for code, domain, accept_lang, gl, locale, ck_name, ck_value in COUNTRIES:
            context = await browser.new_context(
                locale=locale,
                user_agent=random.choice(UAS),
                java_script_enabled=True,
                viewport={"width": 1366, "height": 850},
                extra_http_headers={"Accept-Language": accept_lang},
            )
            await context.add_cookies([{
                "name": ck_name, "value": ck_value,
                "domain": f".{domain}", "path": "/",
                "secure": True, "httpOnly": False
            }])

            results_for_market = []
            for _label, asin in PRODUCTS:
                val = await get_bought_count(context, domain, gl, asin, code)
                results_for_market.append(val)
            await context.close()

            row_values.extend(results_for_market)
            print(f"{code}: Dark={results_for_market[0]}  Light={results_for_market[1]}")

        await browser.close()

    # --- skriv/append till Excel i ../data ---
    header = build_header()
    row_dict = dict(zip(header, row_values))
    append_row(XLSX_PATH, SHEET_NAME, row_dict)
    print(f"Appended row to {XLSX_PATH} [{SHEET_NAME}]:", row_values)

if __name__ == "__main__":
    asyncio.run(run_once())

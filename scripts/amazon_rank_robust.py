import asyncio
import re
import os
from datetime import date
from pathlib import Path
from playwright.async_api import async_playwright

# Behövs för Excel
try:
    import openpyxl
except ImportError:
    print("Du måste installera openpyxl: pip install openpyxl")
    exit()

# ── KONFIGURATION ──────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = (SCRIPT_DIR / ".." / "data").resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
XLSX_PATH = str((DATA_DIR / "fractal_rank_tracking.xlsx").resolve())
SHEET_NAME = "Rankings_Pro"

TARGETS = [
    # --- USA (amazon.com) ---
    {
        "name": "Scape US",
        "asin": "B0D5HK6JRS", 
        "domain": "amazon.com",
        "locale": "en-US",
        "keywords": ["Best Sellers Rank"]
    },
    {
        "name": "Refine US",
        "asin": "B0CSYWWRSV",
        "domain": "amazon.com",
        "locale": "en-US",
        "keywords": ["Best Sellers Rank"]
    },
    # --- TYSKLAND (amazon.de) ---
    {
        "name": "Scape DE",
        "asin": "B0D5HK6JRS",
        "domain": "amazon.de",
        "locale": "de-DE",
        "keywords": ["Bestseller-Rang", "Best Sellers Rank"] 
    },
    {
        "name": "Refine DE",
        "asin": "B0CSYWWRSV",
        "domain": "amazon.de",
        "locale": "de-DE",
        "keywords": ["Bestseller-Rang", "Best Sellers Rank"]
    },
]

HEADLESS = True 

# ───────────────────────────────────────────────────────────────────────────

def extract_smart_rank(text: str, keywords: list) -> int:
    """
    Hittar ranking men ignorerar 'Top 100' länkar.
    1. Letar upp nyckelordet.
    2. Loopar igenom alla träffar av 'siffra + in'.
    3. Kollar texten INNAN siffran. Står det 'Top', ignorera.
    4. Returnerar lägsta giltiga siffra.
    """
    found_keyword = False
    relevant_chunk = ""

    # Hitta startpunkt
    for kw in keywords:
        idx = text.lower().find(kw.lower())
        if idx != -1:
            start_pos = idx + len(kw)
            # Vi tar en rejäl bit text för att vara säkra
            relevant_chunk = text[start_pos : start_pos + 1500]
            found_keyword = True
            break
    
    if not found_keyword:
        return 0

    # Hitta alla matchningar MED position (så vi kan se vad som står innan)
    # Regex: Siffror följt av 'in'
    iterator = re.finditer(r"([0-9,.]+)\s+in\s+", relevant_chunk, re.IGNORECASE)
    
    candidates = []
    
    for match in iterator:
        number_str = match.group(1)
        start_index = match.start()
        
        # --- ANTI-TOP-100 LOGIK ---
        # Titta på de 15 tecknen precis INNAN siffran
        # Vi använder max(0, ...) för att inte krascha om siffran är allra först
        preceding_text = relevant_chunk[max(0, start_index - 15) : start_index]
        
        # Om det står "Top" precis innan (t.ex. "See Top 100"), så hoppar vi över denna!
        if "top" in preceding_text.lower():
            # print(f"      🗑️ Ignorerar '{number_str}' eftersom det står 'Top' innan.")
            continue 

        # Rensa och spara siffran
        clean_str = number_str.replace(",", "").replace(".", "")
        if clean_str.isdigit():
            val = int(clean_str)
            if 0 < val < 10000000:
                candidates.append(val)

    if candidates:
        lowest = min(candidates)
        print(f"      🔍 Hittade kandidater: {candidates} -> Valde: {lowest}")
        return lowest
        
    return 0

async def handle_blockers(page):
    try:
        if await page.locator("#sp-cc-accept").is_visible(timeout=2000):
            await page.click("#sp-cc-accept")
            await page.wait_for_timeout(500)
    except:
        pass

    upsells = ['text="Continue shopping"', 'text="Weiter shoppen"', 'text="Weiter einkaufen"']
    for sel in upsells:
        try:
            if await page.locator(sel).is_visible(timeout=500):
                await page.click(sel)
                await page.wait_for_timeout(500)
        except:
            pass

async def get_rank(context, item) -> int:
    page = await context.new_page()
    url = f"https://www.{item['domain']}/dp/{item['asin']}?th=1&psc=1"
    print(f"  Fetching {item['name']} ({item['domain']})...")

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await handle_blockers(page)

        text_content = ""
        selectors = [
            "#prodDetails", 
            "#detailBullets_feature_div", 
            "#productDescription", 
            "table", 
            "ul",
            "div#centerCol",
        ]

        for sel in selectors:
            if await page.locator(sel).count() > 0:
                elements = await page.locator(sel).all_inner_texts()
                for txt in elements:
                    text_content += txt + "\n"
        
        if len(text_content) < 500:
            text_content = await page.inner_text("body")

        # Anropa den nya smarta funktionen
        rank = extract_smart_rank(text_content, item['keywords'])

        if rank > 0:
            print(f"    ✅ Rank: #{rank}")
        else:
            print(f"    ⚠️ Kunde inte hitta rank för {item['name']}")
        
        return rank

    except Exception as e:
        print(f"    ❌ Error: {e}")
        return 0
    finally:
        await page.close()

def append_to_excel(data_dict):
    file_exists = os.path.exists(XLSX_PATH)
    headers = ["Date"] + [t["name"] for t in TARGETS]

    if not file_exists:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = SHEET_NAME
        ws.append(headers)
    else:
        wb = openpyxl.load_workbook(XLSX_PATH)
        if SHEET_NAME in wb.sheetnames:
            ws = wb[SHEET_NAME]
        else:
            ws = wb.create_sheet(SHEET_NAME)
            ws.append(headers)

    row = [data_dict["Date"]]
    for t in TARGETS:
        row.append(data_dict.get(t["name"], 0))
    
    ws.append(row)
    wb.save(XLSX_PATH)
    print(f"💾 Data saved to {XLSX_PATH}")

async def run():
    today = date.today().isoformat()
    results = {"Date": today}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        
        print(f"--- Starting Final V4 Rank Scraping ({today}) ---")

        for item in TARGETS:
            context = await browser.new_context(
                locale=item['locale'],
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            )
            
            rank = await get_rank(context, item)
            results[item['name']] = rank
            await context.close()
        
        await browser.close()

    append_to_excel(results)

if __name__ == "__main__":
    asyncio.run(run())
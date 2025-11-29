import asyncio
import re
import os
from datetime import date
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
import openpyxl

# ── KONFIGURATION ──────────────────────────────────────────────────────────
# Spara i repo-root/data
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = (SCRIPT_DIR / ".." / "data").resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
XLSX_PATH = str((DATA_DIR / "fractal_amazon_ranks_us.xlsx").resolve())
SHEET_NAME = "Rankings_US"

# Produkter och vilken kategori vi ska leta efter ranking i
PRODUCTS = [
    # Headsets
    {"name": "Scape Dark",          "asin": "B0D5HGK3C2", "category": "Computer Headsets"},
    {"name": "Scape Light",         "asin": "B0D5HK6JRS", "category": "Computer Headsets"},
    
    # Gaming Chairs (Refine)
    {"name": "Refine Mesh Light",   "asin": "B0CSYXY8FD", "category": "Computer Gaming Chairs"},
    {"name": "Refine Fabric Light", "asin": "B0CSYXYX39", "category": "Computer Gaming Chairs"},
    {"name": "Refine Mesh Dark",    "asin": "B0CSYYMTT4", "category": "Computer Gaming Chairs"},
    {"name": "Refine Fabric Dark",  "asin": "B0CSYWWRSV", "category": "Computer Gaming Chairs"},
]

HEADLESS = True  # Sätt till False om du vill se webbläsaren jobba

# ───────────────────────────────────────────────────────────────────────────

def parse_rank(text: str, category: str) -> int:
    """
    Letar efter mönstret '#123 in Category Name' och returnerar siffran.
    """
    # Regex för att hitta rankingen kopplad till den specifika kategorin
    # Exempel: #45 in Computer Gaming Chairs
    # Vi använder re.IGNORECASE för att vara säkra
    pattern = r"#([0-9,]+)\s+in\s+" + re.escape(category)
    match = re.search(pattern, text, re.IGNORECASE)
    
    if match:
        # Ta bort kommatecken (t.ex. 151,380 -> 151380) och konvertera till int
        clean_number = match.group(1).replace(",", "")
        return int(clean_number)
    return 0

async def handle_blockers(page):
    """Klickar bort eventuella popups/blockers"""
    try:
        # Kollar efter "Continue shopping" eller liknande
        blockers = ['text="Continue shopping"', 'input.a-button-input']
        for sel in blockers:
            if await page.locator(sel).is_visible(timeout=1000):
                await page.click(sel)
                await page.wait_for_timeout(1000)
                return
    except:
        pass

async def get_product_rank(context, asin: str, category: str, name: str) -> int:
    page = await context.new_page()
    url = f"https://www.amazon.com/dp/{asin}?th=1&psc=1&language=en_US"
    print(f"  Fetching rank for {name} ({category})...")

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await handle_blockers(page)

        # Hämta texten från hela sidan.
        # Det är enklare än att leta efter specifika div-taggar eftersom Amazon
        # flyttar runt "Product Details" hela tiden.
        content_text = await page.inner_text("body")
        
        # Leta efter "Best Sellers Rank" sektionen för säkerhets skull
        # (För att undvika att matcha fel text någon annanstans)
        # Men vi söker i hela texten först, det brukar räcka.
        
        rank = parse_rank(content_text, category)
        
        if rank > 0:
            print(f"    ✅ Rank: #{rank}")
        else:
            print(f"    ⚠️ Could not find rank for '{category}'.")
            # Debug: Spara html om vi misslyckas
            # await page.screenshot(path=f"debug_{asin}.png")
        
        return rank

    except Exception as e:
        print(f"    ❌ Error: {e}")
        return 0
    finally:
        await page.close()

def append_to_excel(data_dict):
    """
    Skapar filen om den saknas, annars lägger till en rad.
    Använder openpyxl direkt för att slippa externa beroenden.
    """
    file_exists = os.path.exists(XLSX_PATH)
    
    if not file_exists:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = SHEET_NAME
        # Skapa header
        headers = ["Date"] + [p["name"] for p in PRODUCTS]
        ws.append(headers)
    else:
        wb = openpyxl.load_workbook(XLSX_PATH)
        if SHEET_NAME in wb.sheetnames:
            ws = wb[SHEET_NAME]
        else:
            ws = wb.create_sheet(SHEET_NAME)
            headers = ["Date"] + [p["name"] for p in PRODUCTS]
            ws.append(headers)

    # Skapa raden
    row = [data_dict["Date"]]
    for p in PRODUCTS:
        row.append(data_dict.get(p["name"], 0))
    
    ws.append(row)
    wb.save(XLSX_PATH)
    print(f"💾 Data saved to {XLSX_PATH}")

async def run():
    today = date.today().isoformat()
    results = {"Date": today}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 850},
            locale="en-US"
        )
        
        print(f"--- Starting Rank Scraping ({today}) ---")

        for prod in PRODUCTS:
            rank = await get_product_rank(context, prod["asin"], prod["category"], prod["name"])
            results[prod["name"]] = rank
        
        await browser.close()

    # Spara till Excel
    append_to_excel(results)

if __name__ == "__main__":
    asyncio.run(run())
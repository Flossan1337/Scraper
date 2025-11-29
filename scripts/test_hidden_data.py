import asyncio, re, json
from playwright.async_api import async_playwright

# --- HÄR ÄR DIN SPECIFIKA STOL ---
ASIN = "B0CSYXY8FD" # Fractal Refine Mesh Light
# Vi testar på Amazon US (.com) först eftersom datan oftast syns där först
URL = f"https://www.amazon.com/dp/{ASIN}?th=1&psc=1&language=en_US"

async def spy_on_hidden_data():
    async with async_playwright() as pw:
        print(f"🕵️  Startar detektivarbete på {URL}...")
        
        # Vi kör headless=False så du kan se vad som händer (valfritt)
        browser = await pw.chromium.launch(headless=True) 
        page = await browser.new_page()
        
        # Gå till sidan
        try:
            await page.goto(URL, wait_until="domcontentloaded", timeout=30000)
        except Exception as e:
            print(f"❌ Kunde inte ladda sidan: {e}")
            await browser.close()
            return

        # 1. Hämta all HTML
        content = await page.content()

        print("🔍 Analyserar dolda JSON-objekt...")

        # 2. Leta i Amazons "data-a-state" block (där de gömmer config)
        hidden_data = await page.evaluate('''() => {
            const dataBlocks = [];
            // Hämta alla script-taggar som Amazon använder för data
            const scripts = document.querySelectorAll('script[type="a-state"]');
            
            scripts.forEach(s => {
                const text = s.innerText;
                // Om blocket verkar handla om social proofing eller bought count
                if (text.includes("bought") || text.includes("social-proofing")) {
                    dataBlocks.push(text);
                }
            });
            return dataBlocks;
        }''')

        found_something = False

        if hidden_data:
            print(f"   -> Hittade {len(hidden_data)} intressanta datablock.")
            for i, data_str in enumerate(hidden_data):
                try:
                    # Försök tolka det som JSON
                    data_json = json.loads(data_str)
                    
                    # Skriv ut en del av JSON för att se struktur
                    print(f"\n--- Block {i+1} (JSON-analys) ---")
                    json_dump = json.dumps(data_json, indent=2)
                    
                    # Leta efter specifika nyckelord i JSON
                    if "count" in json_dump or "value" in json_dump:
                         print(json_dump[:600] + "...\n(visar första 600 tecknen)")
                    else:
                        print("(Inga uppenbara räkneverk i detta block)")

                    # Regex-sökning inuti JSON-strängen efter siffror kopplade till "bought"
                    match = re.search(r'("count":\s*"?\d+"?)|("value":\s*"?\d+"?)', data_str)
                    if match:
                        print(f"   👉 HITTADE MÖJLIG SIFFRA: {match.group(0)}")
                        found_something = True
                except:
                    # Om det inte är ren JSON
                    pass

        # 3. Leta i den vanliga HTML-koden med Regex som sista utväg
        print("\n🔍 Scannar rå HTML efter mönster...")
        regex_patterns = [
            r'bought_count.*?(\d+)',
            r'recent_sales.*?(\d+)',
            r'social_proofing_count.*?(\d+)',
            r'(\d+)\+\s*bought',  # T.ex. "50+ bought"
            r'past_month_count.*?(\d+)'
        ]
        
        for pattern in regex_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            if matches:
                # Filtrera bort falska positiver (t.ex. årtal eller småsiffror)
                valid_matches = [m for m in matches if len(m) < 5] 
                if valid_matches:
                    print(f"   👉 Regex träff på '{pattern}': {valid_matches}")
                    found_something = True

        if not found_something:
            print("\n❌ Slutsats: Inga dolda siffror hittades i koden.")
            print("   Det betyder troligen att Amazon INTE skickar datan till webbläsaren för denna produkt.")
        else:
            print("\n✅ Slutsats: Vi hittade spår! Kolla loggarna ovan.")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(spy_on_hidden_data())
import asyncio, re, json
from playwright.async_api import async_playwright

# Fractal Refine Fabric Dark (ASIN från din bild tidigare)
ASIN = "B0D1G7XYZ" # OBS: Dubbelkolla att detta är rätt ASIN för Fractal-stolen!
URL = f"https://www.amazon.com/dp/{ASIN}?th=1&psc=1&language=en_US"

async def spy_on_hidden_data():
    async with async_playwright() as pw:
        print(f"🕵️  Startar detektivarbete på {URL}...")
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Gå till sidan
        await page.goto(URL, wait_until="domcontentloaded")
        
        # 1. Hämta all HTML
        content = await page.content()

        print("🔍 Analyserar dolda JSON-objekt...")

        # 2. Leta i Amazons "data-a-state" block (där de gömmer config)
        # Vi letar specifikt efter block som innehåller ordet "bought"
        hidden_data = await page.evaluate('''() => {
            const dataBlocks = [];
            // Hämta alla script-taggar som Amazon använder för data
            const scripts = document.querySelectorAll('script[type="a-state"]');
            
            scripts.forEach(s => {
                const text = s.innerText;
                // Om blocket verkar handla om social proofing
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
                    print(f"\n--- Block {i+1} (JSON) ---")
                    print(json.dumps(data_json, indent=2)[:500] + "...") # Skriv ut första 500 tecknen
                    
                    # Leta efter siffror i JSON-strukturen
                    match = re.search(r'("count":\s*"?\d+"?)|("value":\s*"?\d+"?)', data_str)
                    if match:
                        print(f"   👉 HITTADE MÖJLIG SIFFRA: {match.group(0)}")
                        found_something = True
                except:
                    # Om det inte är ren JSON, skriv ut råtexten om den innehåller siffror
                    print(f"\n--- Block {i+1} (Raw Text) ---")
                    snippet = data_str[max(0, data_str.find("bought")-50) : min(len(data_str), data_str.find("bought")+100)]
                    print(f"...{snippet}...")

        # 3. Leta i den vanliga HTML-koden med Regex som sista utväg
        print("\n🔍 Scannar rå HTML...")
        regex_patterns = [
            r'bought_count.*?(\d+)',
            r'recent_sales.*?(\d+)',
            r'social_proofing_count.*?(\d+)'
        ]
        
        for pattern in regex_patterns:
            matches = re.findall(pattern, content)
            if matches:
                print(f"   👉 Regex träff på '{pattern}': {matches}")
                found_something = True

        if not found_something:
            print("\n❌ Slutsats: Inga dolda siffror hittades.")
            print("   Det betyder att Amazon rensar bort datan på servern för denna produkt.")
        else:
            print("\n✅ Slutsats: Vi hittade spår! Om du ser en siffra ovan, kan vi extrahera den.")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(spy_on_hidden_data())
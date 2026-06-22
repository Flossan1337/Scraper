import requests, re

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}

for url in [
    "https://www.c.technischeunie.nl/merken-overzicht.html",
    "https://www.c.technischeunie.nl/merken-overzicht-lijst.html",
]:
    r = requests.get(url, headers=HEADERS, timeout=30)
    html = r.text
    print(f"\n=== {url} ({len(html)} chars) ===")

    old = re.findall(r'zoeken\?brand=([^"&\s]+)', html)
    print(f"  Old pattern (zoeken?brand=): {len(old)} matches")

    # Look for any href containing brand-related terms
    all_hrefs = re.findall(r'href="([^"]+)"', html)
    brand_hrefs = [h for h in all_hrefs if "brand" in h.lower() or "merk" in h.lower()]
    print(f"  Brand/merk hrefs: {brand_hrefs[:20]}")

    # Search for any JSON-like brand arrays
    json_brands = re.findall(r'"brand[^"]*"\s*:\s*"([^"]+)"', html, re.IGNORECASE)
    print(f"  JSON brand fields: {json_brands[:10]}")

    # Look for data attributes
    data_brands = re.findall(r'data-brand[^=]*="([^"]+)"', html, re.IGNORECASE)
    print(f"  data-brand attrs: {data_brands[:10]}")

    # Any /search or /zoeken patterns
    search_links = [h for h in all_hrefs if "zoek" in h.lower() or "search" in h.lower()]
    print(f"  Search links sample: {search_links[:10]}")

    # Check list page for list items with brand names
    if "lijst" in url:
        # Look for <a href> links in list sections
        list_links = re.findall(r'<a[^>]+href="([^"]+)"[^>]*>([^<]+)</a>', html)
        print(f"  List page anchor links (first 20): {list_links[:20]}")
        # Dump 3000 chars of raw html
        print("\n  Raw HTML snippet (chars 5000-8000):")
        print(repr(html[5000:8000]))

import urllib.request
import re
import json
import urllib.parse

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

req = urllib.request.Request(
    "https://www.c.technischeunie.nl/merken-overzicht.html",
    headers=headers
)

with urllib.request.urlopen(req) as response:
    html = response.read().decode("utf-8")

# Extract brands from the full A-Z list using the search URL format
brands_full_raw = sorted(set(re.findall(r'zoeken\?brand=([^"&]+)', html)))
brands_full = [urllib.parse.unquote_plus(b) for b in brands_full_raw]

# Also extract the featured brand pages
brands_featured = sorted(set(re.findall(r'/merken-overzicht/([A-Za-z0-9][A-Za-z0-9-]+)(?:\.html)', html)))

output = {
    "featured_brands_count": len(brands_featured),
    "featured_brands": brands_featured,
    "full_brands_count": len(brands_full),
    "full_brands": brands_full,
}

with open("data/tu_brands.json", "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

# Also write a simple text summary
with open("data/tu_brands_summary.txt", "w", encoding="utf-8") as f:
    f.write(f"Technische Unie - Brand Analysis\n")
    f.write(f"=================================\n\n")
    f.write(f"Featured brands with dedicated pages: {len(brands_featured)}\n")
    f.write(f"Full A-Z stocked brands: {len(brands_full)}\n\n")
    f.write(f"--- FEATURED BRANDS ---\n")
    for i, b in enumerate(brands_featured, 1):
        f.write(f"{i:3}. {b}\n")
    f.write(f"\n--- FULL A-Z BRAND LIST ---\n")
    for i, b in enumerate(brands_full, 1):
        f.write(f"{i:3}. {b}\n")

print(f"Done! Featured: {len(brands_featured)}, Full list: {len(brands_full)}")
print("Results saved to data/tu_brands.json and data/tu_brands_summary.txt")

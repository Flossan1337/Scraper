"""
fetch_cint_panelbook.py
Hämtar antal genomförda enkäter per land från Cints panelbook-sida
och lägger till en rad i en Excel-fil med datum + antal per land.

Källa: https://www.cint.com/products/cint-exchange/panelbook/
"""

from __future__ import annotations

import html
import json
import re
from datetime import date
from pathlib import Path

import requests

from excel_utils import append_row

# ── Paths ────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
XLSX_PATH = REPO_ROOT / "data" / "cint_panelbook.xlsx"
SHEET     = "Completed Surveys"

# ── Scraping config ──────────────────────────────────────────────────
URL = "https://www.cint.com/products/cint-exchange/panelbook/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


def fetch_page(url: str) -> str:
    """Download the panelbook page HTML."""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_panelbook(page_html: str) -> dict[str, int]:
    """
    Parse the panelbook HTML and return {country: completed_surveys}.

    The data is embedded in a data-wp-context attribute as JSON with structure:
    { "controlData": [
        { "id": "0",
          "properties": [
            {"label": "country",           "value": "Albania"},
            {"label": "Completed Surveys", "value": "2173"},
            ...
          ]
        },
        ...
      ]
    }
    The JSON is HTML-entity-encoded inside the attribute.
    """
    # Extract the data-wp-context attribute value
    match = re.search(r'data-wp-context="([^"]*controlData[^"]*)"', page_html)
    if not match:
        return {}

    raw = html.unescape(match.group(1))
    context = json.loads(raw)
    entries = context.get("controlData", [])

    data: dict[str, int] = {}
    for entry in entries:
        props = {p["label"]: p["value"] for p in entry.get("properties", [])}
        country = props.get("country")
        surveys = props.get("Completed Surveys")
        if country and surveys:
            data[country] = int(surveys)

    return data


def main() -> None:
    print(f"Fetching Cint panelbook data from {URL} ...")
    html = fetch_page(URL)

    data = parse_panelbook(html)
    if not data:
        print("✗ No country data found — page structure may have changed.")
        return

    print(f"✓ Parsed {len(data)} countries.")

    # Build row: Date first, then countries in alphabetical order
    row: dict[str, object] = {"Date": date.today().isoformat()}
    for country in sorted(data):
        row[country] = data[country]

    # Append to Excel
    append_row(str(XLSX_PATH), SHEET, row)
    print(f"✓ Row appended to {XLSX_PATH.name}  (sheet: '{SHEET}')")

    # Print a few samples for quick verification
    samples = ["United States", "United Kingdom", "Germany", "Sweden", "France"]
    for s in samples:
        if s in data:
            print(f"   {s}: {data[s]:,}")


if __name__ == "__main__":
    main()

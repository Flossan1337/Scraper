import requests
from bs4 import BeautifulSoup
import re
import os
from datetime import datetime
from pathlib import Path  # <-- added

from excel_utils import append_row  # <-- NY import

URL = "https://adtraction.com/se/om-adtraction/"

# --- Spara i repo-root/data oavsett var scriptet körs från ---
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = (SCRIPT_DIR / ".." / "data").resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
XLSX_FILE = str((DATA_DIR / "kpi-history.xlsx").resolve())  # <-- ändrat till rätt data-mapp
# ------------------------------------------------------------

SHEET_NAME = "kpi-history"            # flikens namn

def fetch_stats():
    resp = requests.get(URL, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    strings = list(soup.stripped_strings)
    try:
        start = strings.index("Vår plattform")
    except ValueError:
        raise RuntimeError("Couldn't locate the 'Vår plattform' section.")
    stats = {}
    i = start + 1
    while i < len(strings) - 1:
        label = strings[i].strip()
        value = strings[i+1].strip()
        if re.fullmatch(r"[\d\s]+", value):
            stats[label] = int(value.replace(" ", ""))
            i += 2
        else:
            i += 1
    return stats

if __name__ == "__main__":
    stats = fetch_stats()
    conv   = stats.get("Konverteringar", 0)
    brands = stats.get("Varumärken",    0)

    # 1) Print to console
    print(f"Konverteringar: {conv:,}")
    print(f"Varumärken:     {brands:,}")

    # 2) Append till Excel i ../data
    row = {
        "Date": datetime.now().strftime("%Y-%m-%d"),
        "Konverteringar": conv,
        "Varumärken": brands,
    }
    append_row(XLSX_FILE, SHEET_NAME, row)

    print(f"Appended to {XLSX_FILE} [{SHEET_NAME}]")

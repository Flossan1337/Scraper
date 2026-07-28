import requests
from bs4 import BeautifulSoup
import re
import os
from datetime import datetime
from pathlib import Path  # <-- added

from excel_utils import append_row  # <-- NY import
from core.db import safe_insert

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

    today = datetime.now().strftime("%Y-%m-%d")

    # 2) Append till Excel i ../data
    row = {
        "Date": today,
        "Konverteringar": conv,
        "Varumärken": brands,
    }
    append_row(XLSX_FILE, SHEET_NAME, row)

    print(f"Appended to {XLSX_FILE} [{SHEET_NAME}]")

    # 3) Best-effort: also write to Postgres (must not break the script)
    db_rows_written, db_error = safe_insert(
        table="kpi_history",
        columns=["snapshot_date", "conversions", "brands"],
        rows=[(today, conv, brands)],
        conflict_columns=["snapshot_date"],
    )
    if db_error is not None:
        print(f"Databas: MISSLYCKADES – {db_error}")
    else:
        print(f"Databas: {db_rows_written} rader skrivna")

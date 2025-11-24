# fetch_fractal_trends.py
# Robust mot 429 (rate limit): backoff + paus mellan grupper.
# Skriver ALLTID om hela historiken i Excel (ingen append).

import os
import time
import random
from datetime import datetime
from pathlib import Path

import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError
from openpyxl import load_workbook

from excel_utils import append_df

# ── KONFIG VIA ENV (valfritt) ───────────────────────────────────────────────
# Bas-sömn för backoff (sek), max antal försök, och paus mellan grupper.
BASE_SLEEP = int(os.getenv("TRENDS_BASE_SLEEP", "20"))
MAX_TRIES = int(os.getenv("TRENDS_MAX_TRIES", "7"))
PAUSE_BETWEEN_GROUPS = int(os.getenv("TRENDS_PAUSE", "35"))

# Extra ”stealth”-sömn innan varje grupps API-call (tyst, ingen print)
PRE_SLEEP_MIN = int(os.getenv("TRENDS_PRE_SLEEP_MIN", "8"))
PRE_SLEEP_MAX = int(os.getenv("TRENDS_PRE_SLEEP_MAX", "20"))

# Proxy kan sättas via env, t.ex.:
# TRENDS_PROXY="http://USER:PASS@HOST:PORT"
PROXY = os.getenv("TRENDS_PROXY", "").strip() or None

# ── DEFINE YOUR GROUPS ──
GROUPS = [
    ["Fractal North", "Fractal Define", "Fractal Core", "Fractal Node", "Fractal Meshify"],
    ["Fractal North", "Fractal Focus", "Fractal Vector", "Fractal Era", "Fractal Torrent"],
    ["Fractal North", "Fractal Pop", "Fractal Ridge", "Fractal Terra", "Fractal Mood"],
    ["Fractal North", "Fractal Epoch", "Fractal Refine", "Fractal Scape"],
]

# ── OUTPUT ──
# Se till att alltid skriva till Scraper/data (en nivå upp från denna fil)
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

XLSX_PATH = DATA_DIR / "fractal_trends_monthly.xlsx"
SHEET_NAME = "fractal_trends_monthly"


def iot_with_retry(py: TrendReq) -> pd.DataFrame:
    """
    interest_over_time() med exponentiell backoff.
    Hanterar 429 och tillfälliga nätverksfel.
    """
    last_err = None
    for attempt in range(MAX_TRIES):
        try:
            df = py.interest_over_time()
            return df
        except TooManyRequestsError as e:
            last_err = e
            sleep = BASE_SLEEP * (2 ** attempt) + random.uniform(0, BASE_SLEEP)
            print(f"[429] Försök {attempt+1}/{MAX_TRIES} – väntar {sleep:.0f}s.")
            time.sleep(sleep)
        except Exception as e:
            # Andra transienta fel: testa med mild backoff
            last_err = e
            sleep = BASE_SLEEP + random.uniform(0, BASE_SLEEP)
            print(f"[Varning] {type(e).__name__}: {e} – väntar {sleep:.0f}s och försöker igen.")
            time.sleep(sleep)
    raise last_err if last_err else RuntimeError("Okänt fel i iot_with_retry")


def fetch_group(py: TrendReq, keywords):
    """
    Hämtar månatlig Google Trends för upp till 5 sökord
    från 2016-01-01 till idag. Returnerar DataFrame indexerad per månad.
    """
    # Tyst, slumpad väntan innan vi pingar Google (”stealth mode”)
    if PRE_SLEEP_MAX > 0 and PRE_SLEEP_MAX >= PRE_SLEEP_MIN >= 0:
        pre_sleep = random.uniform(PRE_SLEEP_MIN, PRE_SLEEP_MAX)
        time.sleep(pre_sleep)

    tf = f"2016-01-01 {datetime.now():%Y-%m-%d}"
    py.build_payload(keywords, timeframe=tf)
    df = iot_with_retry(py).drop(columns=["isPartial"], errors="ignore")
    return df.resample("M").mean()  # månadsmedel


def main():
    # TrendReq med egna retrier + högre timeout.
    py = TrendReq(
        hl="en-US",
        tz=120,  # Stockholm sommar = UTC+2 (pytrends använder minuter)
        timeout=(10, 60),  # connect, read
        retries=0,  # vi sköter retrier själva
        backoff_factor=0,  # (inaktivt, eftersom retries=0)
        proxies={"https": PROXY} if PROXY else {},  # alltid en dict, aldrig None
    )

    master = None
    for i, grp in enumerate(GROUPS, start=1):
        # Viktigt: behåll exakt din print-struktur
        print(f"Kör grupp {i}/{len(GROUPS)}: {grp}")
        df = fetch_group(py, grp)
        if master is None:
            master = df
        else:
            # Ta bort ”Fractal North” så den inte dupliceras vid join
            df = df.drop(columns=["Fractal North"], errors="ignore")
            master = master.join(df, how="outer")

        # Paus mellan grupper för att undvika 429-spikar
        if i < len(GROUPS):
            sleep = PAUSE_BETWEEN_GROUPS + random.uniform(0, 10)
            # Samma format som du hade, med punkt
            print(f"Paus {sleep:.0f}s innan nästa grupp.")
            time.sleep(sleep)

    # Sortera datum och gör om indexet till kolumn "Date"
    out = master.sort_index().reset_index().rename(columns={"date": "Date"})
    out["Date"] = pd.to_datetime(out["Date"])

    # Ta bort gammal flik (om finns) så vi skriver om HELA historiken
    if XLSX_PATH.exists():
        try:
            wb = load_workbook(XLSX_PATH)
            if SHEET_NAME in wb.sheetnames:
                ws = wb[SHEET_NAME]
                wb.remove(ws)
                wb.save(XLSX_PATH)
        except Exception as e:
            print(f"Varning: kunde inte ta bort gammal flik ({SHEET_NAME}): {e}")

    # Skriv hela datasetet på nytt
    append_df(str(XLSX_PATH), SHEET_NAME, out)
    print(f"Skrev om hela historiken ({len(out)} rader) till {XLSX_PATH} [{SHEET_NAME}].")


if __name__ == "__main__":
    main()

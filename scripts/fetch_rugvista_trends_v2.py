# fetch_rugvista_trends_v2.py

import time, random
from datetime import datetime
import pandas as pd
from pytrends.request import TrendReq
from requests.exceptions import RequestException
import os  # <— keep
from pathlib import Path  # <— added

# ── OUTPUT to Scripts/data (one level up from this file) ──
REPO_ROOT   = Path(__file__).resolve().parent.parent
DATA_DIR    = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_XLSX = DATA_DIR / "rugvista_trends_monthly.xlsx"  # <— changed

SEARCH_TERM = "rugvista"

# Tunables
BASE_SLEEP = 3.0       # pause before building payload
MAX_RETRIES = 8
BACKOFF_START = 5.0
BACKOFF_MULT = 1.8

def mk_client():
    return TrendReq(
        hl="en-US",
        tz=120,                 # Stockholm
        retries=0,              # we'll handle retries
        backoff_factor=0,
        timeout=(10, 30),
        requests_args={
            "headers": {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            }
        },
    )

def fetch_world_monthly(py: TrendReq) -> pd.DataFrame:
    timeframe = f"2016-01-01 {datetime.now():%Y-%m-%d}"
    attempt, backoff = 0, BACKOFF_START

    while True:
        try:
            # small courtesy pause to avoid burst token fetches
            time.sleep(BASE_SLEEP + random.uniform(0, 2))

            # empty geo => worldwide
            py.build_payload([SEARCH_TERM], timeframe=timeframe, geo="")
            df = py.interest_over_time().drop(columns=["isPartial"], errors="ignore")
            df = df.rename(columns={SEARCH_TERM: "Rugvista"})
            return df.resample("M").mean()
        except RequestException as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                raise
            wait = backoff + random.uniform(0, 3)
            print(f"[WORLD] Network error ({e}). Retry {attempt}/{MAX_RETRIES} in {wait:.1f}s…")
            time.sleep(wait)
            backoff *= BACKOFF_MULT
        except Exception as e:
            msg = str(e)
            if "429" in msg or "TooManyRequests" in msg:
                attempt += 1
                if attempt > MAX_RETRIES:
                    raise
                wait = backoff + random.uniform(1, 6)
                print(f"[WORLD] 429 rate-limited. Retry {attempt}/{MAX_RETRIES} in {wait:.1f}s…")
                time.sleep(wait)
                backoff *= BACKOFF_MULT
            else:
                raise

def main():
    py = mk_client()
    df = fetch_world_monthly(py)
    out = df.sort_index().reset_index().rename(columns={"date": "Date"})
    # folder ensured above via DATA_DIR.mkdir()
    out.to_excel(str(OUTPUT_XLSX), index=False, engine="openpyxl")  # <— writes to Scripts/data
    print(f"Wrote {len(out)} months to {OUTPUT_XLSX}")
    print(f"Columns: {list(out.columns)}")

if __name__ == "__main__":
    main()

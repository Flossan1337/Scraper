# fetch_nelly_trends_v3.py

import time, random
from datetime import datetime
import pandas as pd
from pytrends.request import TrendReq
from requests.exceptions import RequestException
import os  # <— keep
from pathlib import Path  # <— added

# ── OUTPUT to Scripts/data (one level up from this file) ──
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR  = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_CSV = DATA_DIR / "nelly_trends_monthly.xlsx"  # <— changed (target folder fixed)

COUNTRIES = [("SE","SE"), ("NO","NO"), ("DK","DK"), ("FI","FI")]
SEARCH_TERM = "nelly"

# Tunables
BASE_SLEEP = 3.0
MAX_RETRIES = 8
BACKOFF_START = 5.0
BACKOFF_MULT = 1.8

def mk_client():
    # Move timeout here (NOT in requests_args) to avoid duplicate timeout error
    return TrendReq(
        hl="en-US",
        tz=120,
        retries=0,
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

def fetch_country_monthly(py: TrendReq, geo_code: str, col_suffix: str) -> pd.DataFrame:
    timeframe = f"2016-01-01 {datetime.now():%Y-%m-%d}"
    attempt, backoff = 0, BACKOFF_START

    while True:
        try:
            py.build_payload([SEARCH_TERM], timeframe=timeframe, geo=geo_code)
            df = py.interest_over_time().drop(columns=["isPartial"], errors="ignore")
            df = df.rename(columns={SEARCH_TERM: f"Nelly_{col_suffix}"})
            return df.resample("M").mean()
        except RequestException as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                raise
            sleep_s = backoff + random.uniform(0, 3)
            print(f"[{geo_code}] Network error ({e}). Retry {attempt}/{MAX_RETRIES} in {sleep_s:.1f}s…")
            time.sleep(sleep_s)
            backoff *= BACKOFF_MULT
        except Exception as e:
            msg = str(e)
            if "429" in msg or "TooManyRequests" in msg:
                attempt += 1
                if attempt > MAX_RETRIES:
                    raise
                sleep_s = backoff + random.uniform(1, 6)
                print(f"[{geo_code}] 429 rate-limited. Retry {attempt}/{MAX_RETRIES} in {sleep_s:.1f}s…")
                time.sleep(sleep_s)
                backoff *= BACKOFF_MULT
            else:
                raise

def main():
    py = mk_client()
    master = None

    for geo, suffix in COUNTRIES:
        time.sleep(BASE_SLEEP + random.uniform(0, 2))
        df = fetch_country_monthly(py, geo, suffix)
        master = df if master is None else master.join(df, how="outer")

    out = master.sort_index().reset_index().rename(columns={"date": "Date"})
    # Folder already ensured above
    out.to_excel(str(OUTPUT_CSV), index=False, engine="openpyxl")  # <— writes to Scripts/data
    print(f"Wrote {len(out)} months to {OUTPUT_CSV}")
    print(f"Columns: {list(out.columns)}")

if __name__ == "__main__":
    main()

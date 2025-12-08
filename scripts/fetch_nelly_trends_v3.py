# fetch_nelly_trends_v4.py

import time, random
from datetime import datetime
import pandas as pd
from pytrends.request import TrendReq
from requests.exceptions import RequestException
import os
from pathlib import Path

# ── OUTPUT to Scripts/data ──
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR  = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_CSV = DATA_DIR / "nelly_trends_monthly.xlsx"

COUNTRIES = [("SE","SE"), ("NO","NO"), ("DK","DK"), ("FI","FI")]
SEARCH_TERM = "nelly"

# ── TUNABLES (More conservative settings) ──
BASE_SLEEP = 15.0       # Ökat från 3.0 till 15.0
MAX_RETRIES = 5
BACKOFF_START = 60.0    # Ökat från 5.0 till 60.0 (Google straffar hårt)
BACKOFF_MULT = 1.5

def mk_client():
    """Creates a fresh client to reset cookies/session."""
    # En lista med moderna User-Agents för att variera lite
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0"
    ]
    
    return TrendReq(
        hl="en-US",
        tz=120,
        retries=0,        # Vi hanterar retries själva
        backoff_factor=0, 
        timeout=(10, 30),
        requests_args={
            "headers": {
                "User-Agent": random.choice(user_agents)
            }
        },
    )

def fetch_country_monthly(geo_code: str, col_suffix: str) -> pd.DataFrame:
    timeframe = f"2016-01-01 {datetime.now():%Y-%m-%d}"
    attempt = 0
    current_backoff = BACKOFF_START

    while True:
        try:
            # VIKTIGT: Skapa ny klient varje försök för att rensa cookies
            py = mk_client()
            
            print(f"[{geo_code}] Requesting data...")
            py.build_payload([SEARCH_TERM], timeframe=timeframe, geo=geo_code)
            
            df = py.interest_over_time()
            if df.empty:
                print(f"[{geo_code}] Warning: Empty response from Google.")
                # Return empty DF with correct index if possible, or retry?
                # For now, let's treat empty as a valid result (0 traffic) or retry.
                # Often empty means bad params, but here likely rate limit ghosting.
                raise Exception("Empty DataFrame returned")

            df = df.drop(columns=["isPartial"], errors="ignore")
            df = df.rename(columns={SEARCH_TERM: f"Nelly_{col_suffix}"})
            
            # Success!
            return df.resample("M").mean()

        except Exception as e:
            attempt += 1
            msg = str(e)
            is_rate_limit = "429" in msg or "TooManyRequests" in msg

            if attempt > MAX_RETRIES:
                print(f"[{geo_code}] GIVING UP after {MAX_RETRIES} retries. Error: {msg}")
                raise e

            # Om det är rate limit, vänta betydligt längre
            if is_rate_limit:
                sleep_s = current_backoff + random.uniform(5, 15)
                print(f"[{geo_code}] RATE LIMITED (429). Cleaning cookies & sleeping {sleep_s:.1f}s...")
                current_backoff *= BACKOFF_MULT
            else:
                # Vanligt nätverksfel
                sleep_s = random.uniform(5, 10)
                print(f"[{geo_code}] Error ({msg}). Retry {attempt}/{MAX_RETRIES} in {sleep_s:.1f}s...")
            
            time.sleep(sleep_s)

def main():
    master = None

    for geo, suffix in COUNTRIES:
        # Initial sleep mellan länder för att inte stressa API:et
        sleep_initial = BASE_SLEEP + random.uniform(2, 8)
        if master is not None: # Vänta inte före första requesten
            print(f"Sleeping {sleep_initial:.1f}s before next country...")
            time.sleep(sleep_initial)

        try:
            df = fetch_country_monthly(geo, suffix)
            master = df if master is None else master.join(df, how="outer")
        except Exception as e:
            print(f"CRITICAL FAILURE for {geo}: {e}")
            # Fortsätt med nästa land eller avbryt? Här fortsätter vi.
            continue

    if master is not None:
        out_df = master.sort_index().reset_index().rename(columns={"date": "Date"})
        out_df.to_excel(str(OUTPUT_CSV), index=False, engine="openpyxl")
        print(f"SUCCESS! Wrote {len(out_df)} months to {OUTPUT_CSV}")
        print(f"Columns: {list(out_df.columns)}")
    else:
        print("No data fetched.")

if __name__ == "__main__":
    main()
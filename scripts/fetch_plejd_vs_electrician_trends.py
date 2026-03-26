# fetch_plejd_vs_electrician_trends.py
#
# Fetches Google Trends for "Plejd" vs local word for "electrician"
# for SE, NO, FI, NL, DE. Outputs all countries in one sheet.

import time, random
from datetime import datetime
import pandas as pd
from pytrends.request import TrendReq
from pathlib import Path

# ── OUTPUT ──
REPO_ROOT  = Path(__file__).resolve().parent.parent
DATA_DIR   = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT     = DATA_DIR / "plejd_vs_electrician_trends.xlsx"

# (geo_code, local_electrician_word, col_suffix, start_date)
COUNTRIES = [
    ("SE", "elektriker",     "SE", "2016-01-01"),
    ("NO", "elektriker",     "NO", "2016-01-01"),
    ("FI", "sähköasentaja",  "FI", "2016-01-01"),
    ("NL", "elektricien",    "NL", "2021-01-01"),
    ("DE", "Elektriker",     "DE", "2016-01-01"),
]
PLEJD = "Plejd"

# ── TUNABLES ──
BASE_SLEEP    = 15.0
MAX_RETRIES   = 5
BACKOFF_START = 60.0
BACKOFF_MULT  = 1.5


def mk_client():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    ]
    return TrendReq(
        hl="en-US",
        tz=120,
        retries=0,
        backoff_factor=0,
        timeout=(10, 30),
        requests_args={"headers": {"User-Agent": random.choice(user_agents)}},
    )


def fetch_pair(geo_code: str, electrician_word: str, start_date: str) -> pd.DataFrame:
    """Fetches Plejd + electrician_word together so they share the same scale."""
    timeframe = f"{start_date} {datetime.now():%Y-%m-%d}"
    keywords  = [PLEJD, electrician_word]
    attempt   = 0
    current_backoff = BACKOFF_START

    while True:
        try:
            py = mk_client()
            print(f"[{geo_code}] Requesting: {keywords} ...")
            py.build_payload(keywords, timeframe=timeframe, geo=geo_code)

            df = py.interest_over_time()
            if df.empty:
                raise Exception("Empty DataFrame returned")

            df = df.drop(columns=["isPartial"], errors="ignore")
            return df.resample("ME").mean()

        except Exception as e:
            attempt += 1
            msg = str(e)
            is_rate_limit = "429" in msg or "TooManyRequests" in msg

            if attempt > MAX_RETRIES:
                print(f"[{geo_code}] GIVING UP after {MAX_RETRIES} retries. Error: {msg}")
                raise

            if is_rate_limit:
                sleep_s = current_backoff + random.uniform(5, 15)
                print(f"[{geo_code}] RATE LIMITED (429). Sleeping {sleep_s:.1f}s...")
                current_backoff *= BACKOFF_MULT
            else:
                sleep_s = random.uniform(5, 10)
                print(f"[{geo_code}] Error ({msg}). Retry {attempt}/{MAX_RETRIES} in {sleep_s:.1f}s...")

            time.sleep(sleep_s)


COUNTRY_NAMES = {
    "SE": "Sweden",
    "NO": "Norway",
    "FI": "Finland",
    "NL": "Netherlands",
    "DE": "Germany",
}


def main():
    master = None
    first  = True

    for geo, elec_word, suffix, start_date in COUNTRIES:
        if not first:
            sleep_s = BASE_SLEEP + random.uniform(2, 8)
            print(f"Sleeping {sleep_s:.1f}s before next country...")
            time.sleep(sleep_s)
        first = False

        country = COUNTRY_NAMES.get(suffix, suffix)
        try:
            df = fetch_pair(geo, elec_word, start_date)
            df = df.rename(columns={
                PLEJD:     f"{country}_Plejd",
                elec_word: f"{country}_{elec_word}",
            })
            master = df if master is None else master.join(df, how="outer")
            print(f"[{geo}] OK — {len(df)} months")
        except Exception as e:
            print(f"CRITICAL FAILURE for {geo}: {e}")
            continue

    if master is not None:
        out_df = master.sort_index().reset_index().rename(columns={"date": "Date"})
        out_df.to_excel(str(OUTPUT), index=False, engine="openpyxl")
        print(f"\nSUCCESS! Wrote {len(out_df)} rows to {OUTPUT}")
        print(f"Columns: {list(out_df.columns)}")
    else:
        print("No data fetched.")


if __name__ == "__main__":
    main()

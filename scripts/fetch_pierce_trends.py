# fetch_pierce_trends.py
# Fetches Google Trends for Pierce Group brands: 24mx, xlmoto, sledstore
# Sheet "together" : all three terms in one search (relative 0-100)
# Sheet "separate" : each term fetched independently (own 0-100 scale)

import time, random
from datetime import datetime
import pandas as pd
from pytrends.request import TrendReq
from pathlib import Path

# ── OUTPUT ──
REPO_ROOT   = Path(__file__).resolve().parent.parent
DATA_DIR    = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_XLSX = DATA_DIR / "pierce_trends_monthly.xlsx"

SEARCH_TERMS = ["24mx", "xlmoto", "sledstore"]
TIMEFRAME    = f"2016-01-01 {datetime.now():%Y-%m-%d}"

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
        requests_args={
            "headers": {"User-Agent": random.choice(user_agents)}
        },
    )


def fetch_together() -> pd.DataFrame:
    """Fetch all three terms in one request so they are scaled relative to each other."""
    attempt = 0
    current_backoff = BACKOFF_START

    while True:
        try:
            py = mk_client()
            print(f"[TOGETHER] Requesting data for {SEARCH_TERMS}...")
            py.build_payload(SEARCH_TERMS, timeframe=TIMEFRAME, geo="")
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
                print(f"[TOGETHER] GIVING UP after {MAX_RETRIES} retries. Error: {msg}")
                raise

            if is_rate_limit:
                sleep_s = current_backoff + random.uniform(5, 15)
                print(f"[TOGETHER] RATE LIMITED (429). Sleeping {sleep_s:.1f}s...")
                current_backoff *= BACKOFF_MULT
            else:
                sleep_s = random.uniform(5, 10)
                print(f"[TOGETHER] Error ({msg}). Retry {attempt}/{MAX_RETRIES} in {sleep_s:.1f}s...")

            time.sleep(sleep_s)


def fetch_single(term: str) -> pd.DataFrame:
    """Fetch a single term independently so it gets its own 0-100 scale."""
    attempt = 0
    current_backoff = BACKOFF_START

    while True:
        try:
            py = mk_client()
            print(f"[SEPARATE] Requesting data for '{term}'...")
            py.build_payload([term], timeframe=TIMEFRAME, geo="")
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
                print(f"[{term}] GIVING UP after {MAX_RETRIES} retries. Error: {msg}")
                raise

            if is_rate_limit:
                sleep_s = current_backoff + random.uniform(5, 15)
                print(f"[{term}] RATE LIMITED (429). Sleeping {sleep_s:.1f}s...")
                current_backoff *= BACKOFF_MULT
            else:
                sleep_s = random.uniform(5, 10)
                print(f"[{term}] Error ({msg}). Retry {attempt}/{MAX_RETRIES} in {sleep_s:.1f}s...")

            time.sleep(sleep_s)


def main():
    # ── Sheet 1: together ──
    print("=== Fetching TOGETHER data ===")
    together_df = fetch_together()
    together_out = (
        together_df
        .sort_index()
        .reset_index()
        .rename(columns={"date": "Date"})
    )

    # ── Sheet 2: separate ──
    print("\n=== Fetching SEPARATE data ===")
    separate_frames = []
    for i, term in enumerate(SEARCH_TERMS):
        if i > 0:
            sleep_s = BASE_SLEEP + random.uniform(2, 8)
            print(f"Sleeping {sleep_s:.1f}s before next term...")
            time.sleep(sleep_s)
        separate_frames.append(fetch_single(term))

    separate_master = separate_frames[0]
    for df in separate_frames[1:]:
        separate_master = separate_master.join(df, how="outer")

    separate_out = (
        separate_master
        .sort_index()
        .reset_index()
        .rename(columns={"date": "Date"})
    )

    # ── Write Excel ──
    with pd.ExcelWriter(str(OUTPUT_XLSX), engine="openpyxl") as writer:
        together_out.to_excel(writer, sheet_name="together", index=False)
        separate_out.to_excel(writer, sheet_name="separate", index=False)

    print(f"\nSUCCESS! Wrote {OUTPUT_XLSX}")
    print(f"  'together' sheet : {len(together_out)} rows, columns: {list(together_out.columns)}")
    print(f"  'separate' sheet : {len(separate_out)} rows, columns: {list(separate_out.columns)}")


if __name__ == "__main__":
    main()

# fetch_cheffelo_trends.py

import time, random
from datetime import datetime
import pandas as pd
from pytrends.request import TrendReq
from pathlib import Path

# ── OUTPUT ──
REPO_ROOT   = Path(__file__).resolve().parent.parent
DATA_DIR    = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_XLSX = DATA_DIR / "cheffelo_trends_monthly.xlsx"

START_DATE = "2022-01-01"

# Each query: search term (or topic ID), geo code, output column name.
# Sweden uses the Google Knowledge Graph topic ID for "Linas Matkasse"
# to match the subject-based search (same as the /g/... URL param).
QUERIES = [
    {"term": "Linas Matkasse", "geo": "SE", "col": "Linas_Matkasse_SE"},
    {"term": "Godtlevert",     "geo": "NO", "col": "Godtlevert_NO"},
    {"term": "Adams matkasse", "geo": "NO", "col": "Adams_Matkasse_NO"},
    {"term": "retnemt",        "geo": "DK", "col": "Retnemt_DK"},
]

# ── TUNABLES ──
BASE_SLEEP    = 15.0
MAX_RETRIES   = 5
BACKOFF_START = 60.0
BACKOFF_MULT  = 1.5

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
]


def mk_client():
    """Creates a fresh TrendReq client (resets cookies/session)."""
    return TrendReq(
        hl="en-US",
        tz=120,
        retries=0,
        backoff_factor=0,
        timeout=(10, 30),
        requests_args={"headers": {"User-Agent": random.choice(USER_AGENTS)}},
    )


def fetch_monthly(term: str, geo: str, col: str) -> pd.DataFrame:
    timeframe = f"{START_DATE} {datetime.now():%Y-%m-%d}"
    attempt = 0
    current_backoff = BACKOFF_START

    while True:
        try:
            py = mk_client()
            print(f"[{geo}] Fetching '{col}'...")
            py.build_payload([term], timeframe=timeframe, geo=geo)

            df = py.interest_over_time()
            if df.empty:
                raise Exception("Empty DataFrame returned")

            df = df.drop(columns=["isPartial"], errors="ignore")
            df = df.rename(columns={term: col})
            return df.resample("ME").mean()

        except Exception as e:
            attempt += 1
            msg = str(e)
            is_rate_limit = "429" in msg or "TooManyRequests" in msg

            if attempt > MAX_RETRIES:
                print(f"[{geo}] GIVING UP after {MAX_RETRIES} retries. Error: {msg}")
                raise

            if is_rate_limit:
                sleep_s = current_backoff + random.uniform(5, 15)
                print(f"[{geo}] RATE LIMITED (429). Retry {attempt}/{MAX_RETRIES} in {sleep_s:.1f}s...")
                current_backoff *= BACKOFF_MULT
            else:
                sleep_s = random.uniform(5, 10)
                print(f"[{geo}] Error ({msg}). Retry {attempt}/{MAX_RETRIES} in {sleep_s:.1f}s...")

            time.sleep(sleep_s)


def main():
    master = None

    for i, q in enumerate(QUERIES):
        if i > 0:
            sleep_s = BASE_SLEEP + random.uniform(2, 8)
            print(f"Sleeping {sleep_s:.1f}s before next query...")
            time.sleep(sleep_s)

        try:
            df = fetch_monthly(q["term"], q["geo"], q["col"])
            master = df if master is None else master.join(df, how="outer")
        except Exception as e:
            print(f"CRITICAL FAILURE for {q['col']}: {e}")
            continue

    if master is not None:
        out_df = master.sort_index().reset_index().rename(columns={"date": "Date"})
        out_df.to_excel(str(OUTPUT_XLSX), index=False, engine="openpyxl")
        print(f"\nSUCCESS! Wrote {len(out_df)} months to {OUTPUT_XLSX}")
        print(f"Columns: {list(out_df.columns)}")
    else:
        print("No data fetched.")


if __name__ == "__main__":
    main()

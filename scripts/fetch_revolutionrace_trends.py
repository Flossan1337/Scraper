# fetch_revolutionrace_trends.py

import time, random
from datetime import datetime
import pandas as pd
from pytrends.request import TrendReq
from requests.exceptions import RequestException
from pathlib import Path

# ── OUTPUT ──────────────────────────────────────────────────────────────────
REPO_ROOT   = Path(__file__).resolve().parent.parent
DATA_DIR    = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_XLSX = DATA_DIR / "revolutionrace_trends_monthly.xlsx"

# RevolutionRace topic entity (Google Knowledge Graph ID)
SEARCH_TERM = "/g/11c6t_2ld2"
START_DATE  = "2019-01-01"

# ── TUNABLES ─────────────────────────────────────────────────────────────────
BASE_SLEEP    = 15.0
MAX_RETRIES   = 8
BACKOFF_START = 60.0
BACKOFF_MULT  = 1.5

def mk_client():
    """Creates a fresh TrendReq client (resets cookies/session)."""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    ]
    return TrendReq(
        hl="en-US",
        tz=120,           # Stockholm
        retries=0,        # handled manually below
        backoff_factor=0,
        timeout=(10, 30),
        requests_args={
            "headers": {"User-Agent": random.choice(user_agents)}
        },
    )

def fetch_world_monthly() -> pd.DataFrame:
    timeframe = f"{START_DATE} {datetime.now():%Y-%m-%d}"
    attempt, backoff = 0, BACKOFF_START

    while True:
        try:
            py = mk_client()
            time.sleep(BASE_SLEEP + random.uniform(2, 8))

            print(f"[WORLD] Requesting data (timeframe: {timeframe})…")
            # geo="" → worldwide; SEARCH_TERM is a topic entity, not a plain keyword
            py.build_payload([SEARCH_TERM], timeframe=timeframe, geo="")
            df = py.interest_over_time()

            if df.empty:
                raise Exception("Empty DataFrame returned – possible rate-limit ghost")

            df = df.drop(columns=["isPartial"], errors="ignore")
            df = df.rename(columns={SEARCH_TERM: "RevolutionRace"})
            return df.resample("ME").mean()

        except RequestException as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                raise
            wait = backoff + random.uniform(0, 5)
            print(f"[WORLD] Network error ({e}). Retry {attempt}/{MAX_RETRIES} in {wait:.1f}s…")
            time.sleep(wait)
            backoff *= BACKOFF_MULT

        except Exception as e:
            msg = str(e)
            attempt += 1
            if attempt > MAX_RETRIES:
                raise

            if "429" in msg or "TooManyRequests" in msg:
                wait = backoff + random.uniform(5, 15)
                print(f"[WORLD] Rate-limited (429). Retry {attempt}/{MAX_RETRIES} in {wait:.1f}s…")
            else:
                wait = random.uniform(5, 10)
                print(f"[WORLD] Error ({msg}). Retry {attempt}/{MAX_RETRIES} in {wait:.1f}s…")

            time.sleep(wait)
            backoff *= BACKOFF_MULT

def main():
    df = fetch_world_monthly()
    out = df.sort_index().reset_index().rename(columns={"date": "Date"})
    out.to_excel(str(OUTPUT_XLSX), index=False, engine="openpyxl")
    print(f"Wrote {len(out)} months to {OUTPUT_XLSX}")
    print(f"Columns: {list(out.columns)}")

if __name__ == "__main__":
    main()

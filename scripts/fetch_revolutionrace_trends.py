# fetch_revolutionrace_trends.py

from datetime import datetime
from pathlib import Path

import pandas as pd

# Not pytrends: see the TrendReq comment in core/trends.py. pytrends builds a
# fresh session per request, and Google answers a session's first request with
# a 429, so under pytrends every request fails.
from core.trends import TrendReq, fetch_series, write_trends_to_db

# ── OUTPUT ──────────────────────────────────────────────────────────────────
REPO_ROOT   = Path(__file__).resolve().parent.parent
DATA_DIR    = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_XLSX = DATA_DIR / "revolutionrace_trends_monthly.xlsx"

# RevolutionRace topic entity (Google Knowledge Graph ID)
SEARCH_TERM = "/g/11c6t_2ld2"
START_DATE  = "2019-01-01"


def main():
    py = TrendReq(hl="en-US", tz=120)
    timeframe = f"{START_DATE} {datetime.now():%Y-%m-%d}"

    print(f"[WORLD] Requesting data (timeframe: {timeframe})…")
    # geo="" → worldwide; SEARCH_TERM is a topic entity, not a plain keyword
    df = fetch_series([SEARCH_TERM], timeframe=timeframe, geo="", client=py)

    df = df.drop(columns=["isPartial"], errors="ignore")
    df = df.rename(columns={SEARCH_TERM: "RevolutionRace"})
    df = df.resample("ME").mean()

    out = df.sort_index().reset_index().rename(columns={"date": "Date"})
    out.to_excel(str(OUTPUT_XLSX), index=False, engine="openpyxl")
    print(f"Wrote {len(out)} months to {OUTPUT_XLSX}")
    print(f"Columns: {list(out.columns)}")

    db_rows, db_error = write_trends_to_db("revolutionrace", {"default": out})
    if db_error is not None:
        print(f"Databas: MISSLYCKADES - {db_error}")
    else:
        print(f"Databas: {db_rows} rader uppserta")


if __name__ == "__main__":
    main()

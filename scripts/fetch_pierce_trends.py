# fetch_pierce_trends.py
# Fetches Google Trends for Pierce Group brands: 24mx, xlmoto, sledstore
# Sheet "together" : all three terms in one search (relative 0-100)
# Sheet "separate" : each term fetched independently (own 0-100 scale)

from datetime import datetime
from pathlib import Path

import pandas as pd

# Not pytrends: see the TrendReq comment in core/trends.py. pytrends builds a
# fresh session per request, and Google answers a session's first request with
# a 429, so under pytrends every request fails.
from core.trends import TrendReq, fetch_series, write_trends_to_db

# ── OUTPUT ──
REPO_ROOT   = Path(__file__).resolve().parent.parent
DATA_DIR    = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_XLSX = DATA_DIR / "pierce_trends_monthly.xlsx"

SEARCH_TERMS = ["24mx", "xlmoto", "sledstore"]
TIMEFRAME    = f"2016-01-01 {datetime.now():%Y-%m-%d}"


def main():
    # One client for the whole run - the shared warmed session is the point.
    py = TrendReq(hl="en-US", tz=120)

    # ── Sheet 1: together (one request, so the terms share a scale) ──
    print(f"=== Fetching TOGETHER data for {SEARCH_TERMS} ===")
    together_df = fetch_series(SEARCH_TERMS, timeframe=TIMEFRAME, geo="", client=py)
    together_df = together_df.drop(columns=["isPartial"], errors="ignore")
    together_out = (
        together_df.resample("ME").mean()
        .sort_index()
        .reset_index()
        .rename(columns={"date": "Date"})
    )

    # ── Sheet 2: separate (one request each, own 0-100 scale) ──
    print("\n=== Fetching SEPARATE data ===")
    separate_master = None
    for term in SEARCH_TERMS:
        print(f"[SEPARATE] {term}")
        df = fetch_series([term], timeframe=TIMEFRAME, geo="", client=py)
        df = df.drop(columns=["isPartial"], errors="ignore").resample("ME").mean()
        separate_master = df if separate_master is None else separate_master.join(df, how="outer")

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

    db_rows, db_error = write_trends_to_db(
        "pierce", {"together": together_out, "separate": separate_out}
    )
    if db_error is not None:
        print(f"Databas: MISSLYCKADES - {db_error}")
    else:
        print(f"Databas: {db_rows} rader uppserta")


if __name__ == "__main__":
    main()

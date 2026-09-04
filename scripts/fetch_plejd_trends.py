# fetch_plejd_trends.py

from datetime import datetime
from pathlib import Path

import pandas as pd

# Not pytrends: see the TrendReq comment in core/trends.py. pytrends builds a
# fresh session per request, and Google answers a session's first request with
# a 429, so under pytrends every request fails.
from core.trends import (
    TrendReq,
    TrendsQuotaError,
    fetch_series,
    write_trends_to_db,
)

# -- OUTPUT to Scripts/data --
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR  = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_CSV = DATA_DIR / "plejd_trends_monthly.xlsx"

COUNTRIES = [
    ("SE", "SE"),
    ("NO", "NO"),
    ("FI", "FI"),
    ("NL", "NL"),
    ("ES", "ES"),
    ("DE", "DE"),
    ("CH", "CH"),
    ("IS", "IS"),
    ("DK", "DK"),
]
SEARCH_TERM = "Plejd"
FETCH_START = "2016-01-01"


def main():
    # One client for the whole run - the shared warmed session is the point.
    py = TrendReq(hl="en-US", tz=120)
    timeframe = f"{FETCH_START} {datetime.now():%Y-%m-%d}"

    master = None
    failed = []
    quota_hit = False

    for geo, suffix in COUNTRIES:
        try:
            df = fetch_series([SEARCH_TERM], timeframe=timeframe, geo=geo, client=py)
            df = df.drop(columns=["isPartial"], errors="ignore")
            df = df.rename(columns={SEARCH_TERM: f"Plejd_{suffix}"})
            df = df.resample("ME").mean()
            master = df if master is None else master.join(df, how="outer")
            print(f"[{geo}] OK - {len(df)} months")
        except TrendsQuotaError as e:
            # Every remaining country would fail the same way. Stop now and
            # keep what we have; a rerun resumes from the cache.
            print(f"[{geo}] {e}")
            failed.extend(g for g, _ in COUNTRIES[COUNTRIES.index((geo, suffix)):])
            quota_hit = True
            break
        except Exception as e:
            failed.append(geo)
            print(f"[{geo}] FAILED: {e}")

    if master is None:
        print("\nNo data fetched - nothing written.")
        if quota_hit:
            print("Rate limited. Wait, then rerun; cached countries are reused.")
        return

    out_df = master.sort_index().reset_index().rename(columns={"date": "Date"})

    if failed:
        # A partial pull would silently drop columns from the xlsx and leave
        # stale months in the DB. Write only when every country is present.
        print(f"\nINCOMPLETE - missing {', '.join(failed)}. Nothing written.")
        print("Rerun to fetch only the missing countries (the rest are cached).")
        return

    out_df.to_excel(str(OUTPUT_CSV), index=False, engine="openpyxl")
    print(f"\nSUCCESS! Wrote {len(out_df)} months to {OUTPUT_CSV}")
    print(f"Columns: {list(out_df.columns)}")

    db_rows, db_error = write_trends_to_db("plejd", {"default": out_df})
    if db_error is not None:
        print(f"Databas: MISSLYCKADES - {db_error}")
    else:
        print(f"Databas: {db_rows} rader uppserta")


if __name__ == "__main__":
    main()

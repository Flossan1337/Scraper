# fetch_cheffelo_trends.py

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

# ── OUTPUT ──
REPO_ROOT   = Path(__file__).resolve().parent.parent
DATA_DIR    = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_XLSX = DATA_DIR / "cheffelo_trends_monthly.xlsx"

# Fetching from 2019 guarantees a ~7-year window → Google Trends returns
# native monthly integers (no weekly resampling artefacts, no decimal averages).
FETCH_START = "2019-01-01"

# Each query: search term (or topic ID), geo code, output column name.
# Sweden uses the Google Knowledge Graph topic ID for "Linas Matkasse"
# to match the subject-based search (same as the /g/... URL param).
QUERIES = [
    {"term": "Linas Matkasse", "geo": "SE", "col": "Linas_Matkasse_SE"},
    {"term": "Godtlevert",     "geo": "NO", "col": "Godtlevert_NO"},
    {"term": "Adams matkasse", "geo": "NO", "col": "Adams_Matkasse_NO"},
    {"term": "retnemt",        "geo": "DK", "col": "Retnemt_DK"},
]


def main():
    # One client for the whole run - the shared warmed session is the point.
    py = TrendReq(hl="en-US", tz=120)
    timeframe = f"{FETCH_START} {datetime.now():%Y-%m-%d}"

    master = None
    failed = []

    for i, q in enumerate(QUERIES):
        try:
            df = fetch_series([q["term"]], timeframe=timeframe, geo=q["geo"], client=py)
            df = df.drop(columns=["isPartial"], errors="ignore")
            df = df.rename(columns={q["term"]: q["col"]})
            # Google returns native monthly integers for a 7-year window.
            # resample is a no-op on monthly data but harmless; round+int
            # eliminates any floating-point edge case.
            df = df.resample("ME").mean().round(0).astype(int)
            master = df if master is None else master.join(df, how="outer")
            print(f"[{q['geo']}] OK - {q['col']}, {len(df)} months")
        except TrendsQuotaError as e:
            # Every remaining query would fail the same way. Stop now; a
            # rerun resumes from the cache instead of starting over.
            print(f"[{q['geo']}] {e}")
            failed.extend(x["col"] for x in QUERIES[i:])
            break
        except Exception as e:
            failed.append(q["col"])
            print(f"[{q['geo']}] FAILED {q['col']}: {e}")

    if master is None:
        print("\nNo data fetched - nothing written.")
        return

    out_df = master.sort_index().reset_index().rename(columns={"date": "Date"})

    if failed:
        # A partial pull would silently drop columns from the xlsx. Write
        # only when every query came back.
        print(f"\nINCOMPLETE - missing {', '.join(failed)}. Nothing written.")
        print("Rerun to fetch only what is missing (the rest are cached).")
        return

    out_df.to_excel(str(OUTPUT_XLSX), index=False, engine="openpyxl")
    print(f"\nSUCCESS! Wrote {len(out_df)} months to {OUTPUT_XLSX}")
    print(f"Columns: {list(out_df.columns)}")

    db_rows, db_error = write_trends_to_db("cheffelo", {"default": out_df})
    if db_error is not None:
        print(f"Databas: MISSLYCKADES - {db_error}")
    else:
        print(f"Databas: {db_rows} rader uppserta")


if __name__ == "__main__":
    main()

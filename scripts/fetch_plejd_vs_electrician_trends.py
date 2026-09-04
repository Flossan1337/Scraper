# fetch_plejd_vs_electrician_trends.py
#
# Fetches Google Trends for "Plejd" vs local word for "electrician"
# for SE, NO, FI, NL, DE. Outputs all countries in one sheet.

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

COUNTRY_NAMES = {
    "SE": "Sweden",
    "NO": "Norway",
    "FI": "Finland",
    "NL": "Netherlands",
    "DE": "Germany",
}


def main():
    # One client for the whole run - the shared warmed session is the point.
    py = TrendReq(hl="en-US", tz=120)

    master = None
    failed = []

    for i, (geo, elec_word, suffix, start_date) in enumerate(COUNTRIES):
        country = COUNTRY_NAMES.get(suffix, suffix)
        timeframe = f"{start_date} {datetime.now():%Y-%m-%d}"
        try:
            # Both terms in one request so they share the same 0-100 scale.
            df = fetch_series([PLEJD, elec_word], timeframe=timeframe, geo=geo, client=py)
            df = df.drop(columns=["isPartial"], errors="ignore").resample("ME").mean()
            df = df.rename(columns={
                PLEJD:     f"{country}_Plejd",
                elec_word: f"{country}_{elec_word}",
            })
            master = df if master is None else master.join(df, how="outer")
            print(f"[{geo}] OK — {len(df)} months")
        except TrendsQuotaError as e:
            # Every remaining country would fail the same way. Stop now; a
            # rerun resumes from the cache instead of starting over.
            print(f"[{geo}] {e}")
            failed.extend(g for g, _, _, _ in COUNTRIES[i:])
            break
        except Exception as e:
            failed.append(geo)
            print(f"[{geo}] FAILED: {e}")

    if master is None:
        print("\nNo data fetched - nothing written.")
        return

    out_df = master.sort_index().reset_index().rename(columns={"date": "Date"})

    if failed:
        # A partial pull would silently drop columns from the xlsx. Write
        # only when every country came back.
        print(f"\nINCOMPLETE - missing {', '.join(failed)}. Nothing written.")
        print("Rerun to fetch only the missing countries (the rest are cached).")
        return

    out_df.to_excel(str(OUTPUT), index=False, engine="openpyxl")
    print(f"\nSUCCESS! Wrote {len(out_df)} rows to {OUTPUT}")
    print(f"Columns: {list(out_df.columns)}")

    db_rows, db_error = write_trends_to_db("plejd_vs_electrician", {"default": out_df})
    if db_error is not None:
        print(f"Databas: MISSLYCKADES - {db_error}")
    else:
        print(f"Databas: {db_rows} rader uppserta")


if __name__ == "__main__":
    main()

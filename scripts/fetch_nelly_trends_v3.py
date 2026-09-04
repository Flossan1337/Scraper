# fetch_nelly_trends_v3.py
#
# NOTE on the chunking below: it splits 2016→today into 12-month windows with
# a 2-month overlap and stitches them, which costs ~13 requests per country
# (~52 for all four, ~104 HTTP calls). That was built to dodge 429s, but the
# 429s never came from request size - see the TrendReq comment in
# core/trends.py. Google returns the whole 2016→today window in ONE call.
#
# The chunking is kept for now because switching to a single call changes
# every value in the output (Google's own normalisation vs. our stitched
# reconstruction), and that needs a diff against the full xlsx history first.
# See KNOWN_ISSUES.md. Of all the trends scripts this is the one most likely
# to exhaust the per-IP quota on a single run.

from datetime import datetime
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta

# Not pytrends: see the TrendReq comment in core/trends.py. Retries and the
# per-chunk cache live in core.trends now - no sleeps needed here.
from core.trends import TrendReq, TrendsQuotaError, fetch_series, write_trends_to_db

# ── OUTPUT ──
REPO_ROOT  = Path(__file__).resolve().parent.parent
DATA_DIR   = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_CSV = DATA_DIR / "nelly_trends_monthly.xlsx"

COUNTRIES   = [("SE", "SE"), ("NO", "NO"), ("DK", "DK"), ("FI", "FI")]
SEARCH_TERM = "nelly"
START_DATE  = datetime(2016, 1, 1)

CHUNK_MONTHS   = 12    # months per request
OVERLAP_MONTHS = 2     # overlap used for inter-chunk normalisation


def _normalise_and_stitch(chunks: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Stitch overlapping chunks into a single continuous normalised series.
    Each chunk is scaled so its overlap window matches the previous chunk.
    """
    if not chunks:
        raise ValueError("No chunks to stitch")

    result = chunks[0].copy()

    for nxt in chunks[1:]:
        overlap_idx = result.index.intersection(nxt.index)
        if len(overlap_idx) == 0:
            # No overlap – just append as-is (shouldn't happen with correct settings)
            result = pd.concat([result, nxt[~nxt.index.isin(result.index)]])
            continue

        prev_mean = result.loc[overlap_idx, SEARCH_TERM].mean()
        nxt_mean  = nxt.loc[overlap_idx, SEARCH_TERM].mean()

        scale = (prev_mean / nxt_mean) if nxt_mean > 0 else 1.0

        nxt_scaled = nxt.copy()
        nxt_scaled[SEARCH_TERM] = nxt_scaled[SEARCH_TERM] * scale

        # Keep previous values in the overlap window; append the new tail
        new_rows = nxt_scaled[~nxt_scaled.index.isin(result.index)]
        result = pd.concat([result, new_rows])

    return result.sort_index()


def fetch_country_monthly(py: TrendReq, geo: str, col_suffix: str) -> pd.DataFrame:
    """
    Fetches the full history for one country by requesting ~CHUNK_MONTHS-sized
    windows with OVERLAP_MONTHS overlap, then normalises and stitches them.
    """
    end_date = datetime.now()
    chunks: list[pd.DataFrame] = []

    chunk_start = START_DATE
    while chunk_start < end_date:
        chunk_end = min(chunk_start + relativedelta(months=CHUNK_MONTHS), end_date)
        timeframe = f"{chunk_start:%Y-%m-%d} {chunk_end:%Y-%m-%d}"
        print(f"  [{geo}] Fetching {timeframe} …")

        # Cached per chunk in core.trends - an interrupted run resumes.
        df = fetch_series([SEARCH_TERM], timeframe=timeframe, geo=geo, client=py)
        chunks.append(df[[SEARCH_TERM]])

        # Termination guard. Without it this loop never ends: once chunk_end
        # clamps to end_date, subtracting the overlap rewinds chunk_start to
        # the same date every iteration, and the same window is fetched
        # forever. (The pre-2026-09 version had this bug too - it just span
        # slowly, behind sleeps and 429s, so it looked like a rate-limit hang.
        # build_chunks() in fetch_rugvista_trends_v2.py has the same guard.)
        if chunk_end >= end_date:
            break
        next_start = chunk_end - relativedelta(months=OVERLAP_MONTHS)
        if next_start <= chunk_start:
            break

        # Move start forward, keeping overlap for normalisation
        chunk_start = next_start

    stitched = _normalise_and_stitch(chunks)
    stitched = stitched.rename(columns={SEARCH_TERM: f"Nelly_{col_suffix}"})
    return stitched.resample("ME").mean()


def main():
    # One client for the whole run - the shared warmed session is the point.
    py = TrendReq(hl="en-US", tz=120)

    master = None
    failed = []

    for i, (geo, suffix) in enumerate(COUNTRIES):
        try:
            print(f"[{geo}] Starting chunked fetch …")
            df = fetch_country_monthly(py, geo, suffix)
            master = df if master is None else master.join(df, how="outer")
            print(f"[{geo}] Done – {len(df)} months fetched.")
        except TrendsQuotaError as e:
            # Every remaining country would fail the same way. Stop now; a
            # rerun resumes from the cached chunks.
            print(f"[{geo}] {e}")
            failed.extend(g for g, _ in COUNTRIES[i:])
            break
        except Exception as exc:
            failed.append(geo)
            print(f"[{geo}] FAILED: {exc}")

    if master is None:
        print("\nNo data fetched - nothing written.")
        return

    out_df = master.sort_index().reset_index().rename(columns={"date": "Date"})

    if failed:
        # A partial pull would silently drop columns from the xlsx. Write
        # only when every country came back.
        print(f"\nINCOMPLETE - missing {', '.join(failed)}. Nothing written.")
        print("Rerun to fetch only what is missing (the rest are cached).")
        return

    out_df.to_excel(str(OUTPUT_CSV), index=False, engine="openpyxl")
    print(f"\nSUCCESS – wrote {len(out_df)} months → {OUTPUT_CSV}")
    print(f"Columns: {list(out_df.columns)}")

    db_rows, db_error = write_trends_to_db("nelly", {"default": out_df})
    if db_error is not None:
        print(f"Databas: MISSLYCKADES - {db_error}")
    else:
        print(f"Databas: {db_rows} rader uppserta")


if __name__ == "__main__":
    main()

# fetch_nelly_trends_v4.py

import time
import random
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from pytrends.request import TrendReq
from pathlib import Path

# ── OUTPUT ──
REPO_ROOT  = Path(__file__).resolve().parent.parent
DATA_DIR   = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_CSV   = DATA_DIR / "nelly_trends_monthly.xlsx"
CACHE_FILE   = DATA_DIR / "nelly_trends_cache.json"   # partial-result cache

COUNTRIES   = [("SE", "SE"), ("NO", "NO"), ("DK", "DK"), ("FI", "FI")]
SEARCH_TERM = "nelly"
START_DATE  = datetime(2016, 1, 1)

# ── TUNABLES ──
CHUNK_MONTHS   = 12      # months per request (shorter = less suspicious)
OVERLAP_MONTHS = 2       # overlap used for inter-chunk normalisation
BASE_SLEEP     = 60.0    # seconds between chunks/countries
MAX_RETRIES    = 6
BACKOFF_START  = 120.0   # first 429 back-off in seconds
BACKOFF_MAX    = 900.0   # cap at 15 minutes
BACKOFF_MULT   = 2.0

# Optional proxy list – leave empty to use no proxy.
# Format: ["http://user:pass@host:port", ...]
PROXIES: list[str] = []

# ── USER-AGENTS ──
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]


def _is_rate_limit(msg: str) -> bool:
    triggers = ["429", "toomanyrequest", "response code 429", "too many requests"]
    return any(t in msg.lower() for t in triggers)


def mk_client() -> TrendReq:
    """Fresh client per attempt – resets cookies and session state."""
    kwargs: dict = dict(
        hl="en-US",
        tz=120,
        retries=0,
        backoff_factor=0,
        timeout=(10, 45),
        requests_args={"headers": {"User-Agent": random.choice(_USER_AGENTS)}},
    )
    if PROXIES:
        proxy = random.choice(PROXIES)
        kwargs["proxies"] = {"https": proxy, "http": proxy}
    return TrendReq(**kwargs)


def _fetch_single_chunk(geo: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch one time-window with retry/backoff. Returns raw weekly DataFrame."""
    timeframe = f"{start:%Y-%m-%d} {end:%Y-%m-%d}"
    attempt = 0
    backoff = BACKOFF_START

    while True:
        try:
            py = mk_client()
            print(f"  [{geo}] Fetching {timeframe} …")
            py.build_payload([SEARCH_TERM], timeframe=timeframe, geo=geo)
            df = py.interest_over_time()

            if df.empty:
                raise RuntimeError("Empty DataFrame – possible silent 429")

            df = df.drop(columns=["isPartial"], errors="ignore")
            return df[[SEARCH_TERM]]

        except Exception as exc:
            attempt += 1
            msg = str(exc)
            if attempt > MAX_RETRIES:
                print(f"  [{geo}] GIVING UP after {MAX_RETRIES} retries: {msg}")
                raise

            if _is_rate_limit(msg):
                sleep_s = min(backoff, BACKOFF_MAX) + random.uniform(10, 30)
                print(f"  [{geo}] 429 – sleeping {sleep_s:.0f}s (attempt {attempt}/{MAX_RETRIES}) …")
                backoff = min(backoff * BACKOFF_MULT, BACKOFF_MAX)
            else:
                sleep_s = random.uniform(15, 30)
                print(f"  [{geo}] Error '{msg}' – retry {attempt}/{MAX_RETRIES} in {sleep_s:.0f}s …")

            time.sleep(sleep_s)


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

        if nxt_mean > 0:
            scale = prev_mean / nxt_mean
        else:
            scale = 1.0

        nxt_scaled = nxt.copy()
        nxt_scaled[SEARCH_TERM] = nxt_scaled[SEARCH_TERM] * scale

        # Keep previous values in the overlap window; append the new tail
        new_rows = nxt_scaled[~nxt_scaled.index.isin(result.index)]
        result = pd.concat([result, new_rows])

    return result.sort_index()


def fetch_country_monthly(geo: str, col_suffix: str) -> pd.DataFrame:
    """
    Fetches the full history for one country by requesting ~CHUNK_MONTHS-sized
    windows with OVERLAP_MONTHS overlap, then normalises and stitches them.
    """
    end_date = datetime.now()
    chunks: list[pd.DataFrame] = []

    chunk_start = START_DATE
    chunk_index = 0

    while chunk_start < end_date:
        chunk_end = min(chunk_start + relativedelta(months=CHUNK_MONTHS), end_date)

        if chunk_index > 0:
            sleep_s = BASE_SLEEP + random.uniform(10, 30)
            print(f"  [{geo}] Sleeping {sleep_s:.0f}s before next chunk …")
            time.sleep(sleep_s)

        raw = _fetch_single_chunk(geo, chunk_start, chunk_end)
        chunks.append(raw)
        chunk_index += 1

        # Move start forward, keeping overlap for normalisation
        chunk_start = chunk_end - relativedelta(months=OVERLAP_MONTHS)

    stitched = _normalise_and_stitch(chunks)
    stitched = stitched.rename(columns={SEARCH_TERM: f"Nelly_{col_suffix}"})
    return stitched.resample("ME").mean()


# ── Disk cache helpers ──────────────────────────────────────────────────────

def _load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text())
        except Exception:
            pass
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_FILE.write_text(json.dumps(cache))


def _df_to_cache(df: pd.DataFrame) -> dict:
    return df.reset_index().rename(columns={"date": "Date"}).to_dict(orient="list")


def _df_from_cache(data: dict) -> pd.DataFrame:
    df = pd.DataFrame(data)
    df["Date"] = pd.to_datetime(df["Date"])
    return df.set_index("Date")


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    cache = _load_cache()
    master: pd.DataFrame | None = None

    for geo, suffix in COUNTRIES:
        col = f"Nelly_{suffix}"

        # Resume from cache if available
        if col in cache:
            print(f"[{geo}] Loaded from cache – skipping fetch.")
            df = _df_from_cache(cache[col])
        else:
            if master is not None:
                sleep_s = BASE_SLEEP + random.uniform(15, 45)
                print(f"[{geo}] Sleeping {sleep_s:.0f}s before next country …")
                time.sleep(sleep_s)

            try:
                print(f"[{geo}] Starting chunked fetch …")
                df = fetch_country_monthly(geo, suffix)
                cache[col] = _df_to_cache(df)
                _save_cache(cache)
                print(f"[{geo}] Done – {len(df)} months fetched.")
            except Exception as exc:
                print(f"[{geo}] CRITICAL FAILURE: {exc}")
                continue

        master = df if master is None else master.join(df, how="outer")

    if master is not None:
        out_df = master.sort_index().reset_index().rename(columns={"date": "Date"})
        out_df.to_excel(str(OUTPUT_CSV), index=False, engine="openpyxl")
        print(f"\nSUCCESS – wrote {len(out_df)} months → {OUTPUT_CSV}")
        print(f"Columns: {list(out_df.columns)}")
        # Clean up cache after a successful full run
        if CACHE_FILE.exists():
            CACHE_FILE.unlink()
    else:
        print("No data fetched.")

if __name__ == "__main__":
    main()
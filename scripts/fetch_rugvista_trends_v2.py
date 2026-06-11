# fetch_rugvista_trends_v2.py
#
# Robustare hämtning av Google Trends för Rugvista.
#
# Varför vi fick 429 förut:
#   – En enda stor förfrågan (2016→idag, ~10 år) är mycket dyr för Googles API
#     och triggar rate-limiting nästan alltid.
#
# Lösning:
#   1. Dela upp i ~2-åriga CHUNK:ar med 3 månaders ÖVERLAPP.
#      Varje chunk är en kort, billig förfrågan.
#   2. Normalisera ihop chunks via överlappet (Google returnerar relativa värden
#      0-100 inom varje chunk – vi skalar ihop dem med överlappets medelvärde).
#   3. Ny TrendReq-instans per request → färska cookies/session.
#   4. Mycket längre pauser: 15 s base, 60+ s vid 429.

import json
import random
import time
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

import pandas as pd
from pytrends.request import TrendReq
from requests.exceptions import RequestException

# ── Paths ────────────────────────────────────────────────────────────────────
REPO_ROOT    = Path(__file__).resolve().parent.parent
DATA_DIR     = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_XLSX  = DATA_DIR / "rugvista_trends_monthly.xlsx"
CACHE_FILE   = DATA_DIR / "rugvista_trends_cache.json"   # sparar chunk-progress

# ── Inställningar ─────────────────────────────────────────────────────────────
SEARCH_TERM   = "rugvista"
START_DATE    = date(2016, 1, 1)
CHUNK_YEARS   = 2          # varje förfrågan täcker ~2 år
OVERLAP_MONTHS = 3         # överlapp för normalisering

BASE_SLEEP    = 15.0       # sekunder före varje request (var 3 – för lite)
MAX_RETRIES   = 6
BACKOFF_429   = 60.0       # startfördröjning vid 429 (var 5 – för lite)
BACKOFF_MULT  = 2.0
INTER_CHUNK   = 20.0       # extra paus mellan chunks


# ── Google Trends-klient ──────────────────────────────────────────────────────

def mk_client() -> TrendReq:
    """Ny instans per anrop – återställer session och cookies."""
    return TrendReq(
        hl="en-US",
        tz=120,
        retries=0,
        backoff_factor=0,
        timeout=(10, 45),
        requests_args={
            "headers": {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            }
        },
    )


# ── Hämta en enskild chunk ────────────────────────────────────────────────────

def fetch_chunk(start: date, end: date, label: str) -> pd.Series:
    """
    Hämtar dagliga trenddata för [start, end] och returnerar
    månatligt medel som pd.Series med DatetimeIndex.

    Ny TrendReq-instans per försök; aggressiv backoff vid 429.
    """
    timeframe = f"{start:%Y-%m-%d} {end:%Y-%m-%d}"
    attempt, backoff = 0, BACKOFF_429

    while True:
        try:
            print(f"  [{label}] Hämtar {timeframe}…", flush=True)
            time.sleep(BASE_SLEEP + random.uniform(0, 5))

            py = mk_client()
            py.build_payload([SEARCH_TERM], timeframe=timeframe, geo="")
            df = py.interest_over_time()

            if df.empty:
                raise ValueError(f"Tomt svar för {timeframe}")

            df = df.drop(columns=["isPartial"], errors="ignore")
            series = df[SEARCH_TERM].astype(float)
            monthly = series.resample("ME").mean()
            print(f"  [{label}] OK – {len(monthly)} månader", flush=True)
            return monthly

        except RequestException as exc:
            attempt += 1
            if attempt > MAX_RETRIES:
                raise
            wait = backoff + random.uniform(0, 10)
            print(f"  [{label}] Nätverksfel: {exc}. Retry {attempt}/{MAX_RETRIES} om {wait:.0f}s…")
            time.sleep(wait)
            backoff *= BACKOFF_MULT

        except Exception as exc:
            msg = str(exc)
            is_429 = "429" in msg or "TooManyRequests" in msg or "response" in msg.lower()
            if is_429:
                attempt += 1
                if attempt > MAX_RETRIES:
                    raise
                wait = backoff + random.uniform(5, 20)
                print(f"  [{label}] 429 rate-limit. Retry {attempt}/{MAX_RETRIES} om {wait:.0f}s…")
                time.sleep(wait)
                backoff *= BACKOFF_MULT
            else:
                raise


# ── Normalisera och foga ihop chunks ─────────────────────────────────────────

def stitch(chunks: list[pd.Series]) -> pd.Series:
    """
    Fogar ihop chunks med överlappsnormalisering.

    Varje chunk returneras av Google med värden 0-100 relativt
    sin egen period. Vi skalar varje ny chunk mot den föregående
    via medelvärdet i överlappet.
    """
    result = chunks[0].copy()

    for chunk in chunks[1:]:
        overlap = result.index.intersection(chunk.index)
        if len(overlap) == 0:
            # Inget överlapp – konkatenera direkt (ska inte hända)
            new = chunk[chunk.index > result.index.max()]
            result = pd.concat([result, new])
            continue

        ref_mean   = result.loc[overlap].mean()
        chunk_mean = chunk.loc[overlap].mean()

        scale = (ref_mean / chunk_mean) if chunk_mean > 0 else 1.0

        new = chunk[chunk.index > result.index.max()] * scale
        result = pd.concat([result, new])

    return result.sort_index()


# ── Bygg chunk-intervall ──────────────────────────────────────────────────────

def build_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """Returnerar lista med (chunk_start, chunk_end) med överlapp."""
    chunks: list[tuple[date, date]] = []
    cur = start
    while cur < end:
        chunk_end = min(cur + relativedelta(years=CHUNK_YEARS), end)
        chunks.append((cur, chunk_end))
        # Nästa chunk börjar OVERLAP_MONTHS före slutet av denna
        cur = chunk_end - relativedelta(months=OVERLAP_MONTHS)
        if cur <= chunks[-1][0]:
            break
    return chunks


# ── Cache-hantering (sparar hämtade chunks som JSON) ─────────────────────────

def load_cache() -> dict[str, list]:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_cache(cache: dict[str, list]) -> None:
    CACHE_FILE.write_text(json.dumps(cache, indent=2), encoding="utf-8")


def cache_key(start: date, end: date) -> str:
    return f"{start:%Y-%m-%d}__{end:%Y-%m-%d}"


# ── Huvudfunktion ─────────────────────────────────────────────────────────────

def main() -> None:
    today   = date.today()
    chunks  = build_chunks(START_DATE, today)
    cache   = load_cache()

    print(f"Hämtar '{SEARCH_TERM}' i {len(chunks)} chunks med {OVERLAP_MONTHS} mån överlapp.")
    print(f"Cache-fil: {CACHE_FILE}\n")

    series_list: list[pd.Series] = []

    for i, (cs, ce) in enumerate(chunks, 1):
        key = cache_key(cs, ce)
        label = f"chunk {i}/{len(chunks)}"

        if key in cache:
            print(f"  [{label}] Redan cachad – hoppar över API-anrop.")
            records = cache[key]
            s = pd.Series(
                {pd.Timestamp(r["date"]): r["value"] for r in records},
                dtype=float,
            )
        else:
            s = fetch_chunk(cs, ce, label)
            cache[key] = [
                {"date": str(dt.date()), "value": v}
                for dt, v in s.items()
            ]
            save_cache(cache)

        series_list.append(s)

        if i < len(chunks):
            wait = INTER_CHUNK + random.uniform(0, 10)
            print(f"  Pausar {wait:.0f}s innan nästa chunk…\n")
            time.sleep(wait)

    # Foga ihop och normalisera
    combined = stitch(series_list)
    combined.name = "Rugvista"

    # Klipp bort framtida "isPartial"-månader
    combined = combined[combined.index <= pd.Timestamp(today)]

    out = combined.reset_index().rename(columns={"index": "Date", "date": "Date"})
    out.columns = ["Date", "Rugvista"]

    out.to_excel(str(OUTPUT_XLSX), index=False, engine="openpyxl")
    print(f"\nKlar! {len(out)} månader sparade → {OUTPUT_XLSX}")

    # Rensa cache när allt lyckats
    if CACHE_FILE.exists():
        CACHE_FILE.unlink()
        print("Cache-fil borttagen.")


if __name__ == "__main__":
    main()

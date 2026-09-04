# fetch_rugvista_trends_v2.py
#
# Hämtning av Google Trends för Rugvista.
#
# OBS om chunkningen nedan: den byggdes på antagandet att en enda stor
# förfrågan (2016→idag) "triggar rate-limiting nästan alltid". Det stämmer
# inte - mätt 2026-09-04 returnerar Google hela 2016→idag i ETT anrop utan
# problem (se kommentaren i core/trends.py om vad 429:orna faktiskt beror på).
# Chunkningen kostar alltså ~6x fler requests än nödvändigt OCH ger ett
# hopsytt värde i stället för Googles egen månadsnormalisering.
#
# Den behålls ändå tills värdena har diffats mot hela historiken i xlsx:en -
# att byta till ett enda anrop ändrar varenda siffra i utdatan. Se
# KNOWN_ISSUES.md.

from datetime import date, datetime
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta

# Inte pytrends: se TrendReq-kommentaren i core/trends.py. Retrier och
# chunk-cache ligger numera i core.trends - inga sleeps behövs här.
from core.trends import TrendReq, TrendsQuotaError, fetch_series, write_trends_to_db

# ── Paths ────────────────────────────────────────────────────────────────────
REPO_ROOT    = Path(__file__).resolve().parent.parent
DATA_DIR     = REPO_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_XLSX  = DATA_DIR / "rugvista_trends_monthly.xlsx"

# ── Inställningar ─────────────────────────────────────────────────────────────
SEARCH_TERM   = "rugvista"
START_DATE    = date(2016, 1, 1)
CHUNK_YEARS   = 2          # varje förfrågan täcker ~2 år
OVERLAP_MONTHS = 3         # överlapp för normalisering


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


# ── Huvudfunktion ─────────────────────────────────────────────────────────────

def main() -> None:
    today   = date.today()
    chunks  = build_chunks(START_DATE, today)

    # En klient för hela körningen - den delade uppvärmda sessionen är poängen.
    py = TrendReq(hl="en-US", tz=120)

    print(f"Hämtar '{SEARCH_TERM}' i {len(chunks)} chunks med {OVERLAP_MONTHS} mån överlapp.\n")

    series_list: list[pd.Series] = []

    for i, (cs, ce) in enumerate(chunks, 1):
        label = f"chunk {i}/{len(chunks)}"
        timeframe = f"{cs:%Y-%m-%d} {ce:%Y-%m-%d}"
        print(f"  [{label}] Hämtar {timeframe}…", flush=True)
        try:
            # Cachad per chunk i core.trends - en avbruten körning återupptas.
            df = fetch_series([SEARCH_TERM], timeframe=timeframe, geo="", client=py)
        except TrendsQuotaError as e:
            # Utan alla chunks går serien inte att sy ihop. Avbryt; en ny
            # körning återanvänder de chunks som redan hämtats.
            print(f"  [{label}] {e}")
            print("\nAvbruten - inget skrivet. Kör igen senare, redan hämtade chunks återanvänds.")
            return

        s = df[SEARCH_TERM].astype(float).resample("ME").mean()
        print(f"  [{label}] OK – {len(s)} månader", flush=True)
        series_list.append(s)

    # Foga ihop och normalisera
    combined = stitch(series_list)
    combined.name = "Rugvista"

    # Klipp bort framtida "isPartial"-månader
    combined = combined[combined.index <= pd.Timestamp(today)]

    out = combined.reset_index()
    out.columns = ["Date", "Rugvista"]

    out.to_excel(str(OUTPUT_XLSX), index=False, engine="openpyxl")
    print(f"\nKlar! {len(out)} månader sparade → {OUTPUT_XLSX}")

    db_rows, db_error = write_trends_to_db("rugvista", {"default": out})
    if db_error is not None:
        print(f"Databas: MISSLYCKADES - {db_error}")
    else:
        print(f"Databas: {db_rows} rader uppserta")


if __name__ == "__main__":
    main()

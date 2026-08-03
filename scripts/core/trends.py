"""
core/trends.py

Shared DB-write helper for the 8 Google Trends fetchers, all of which
produce the same shape (a wide DataFrame: a "Date" column plus one column
per search term/geo series) and share one table, google_trends_monthly.
See sql/schema.sql for why this is upserted (latest-known-state) rather
than snapshot-inserted.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from core.db import safe_upsert


def write_trends_to_db(
    pipeline: str,
    sheets: dict[str, pd.DataFrame],
) -> tuple[int | None, str | None]:
    """
    Best-effort: upsert one script's full monthly trends output.

    sheets: {sheet_label: df}, where df has a "Date" column plus one column
    per series. Use {"default": df} for a script with a single sheet.
    """
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for sheet_label, df in sheets.items():
        if df is None or df.empty:
            continue
        series_cols = [c for c in df.columns if c != "Date"]
        for _, row in df.iterrows():
            date_val = row["Date"]
            month = date_val.date() if hasattr(date_val, "date") else date_val
            for series in series_cols:
                value = row[series]
                if pd.isna(value):
                    continue
                rows.append((pipeline, sheet_label, series, month, float(value), fetched_at))

    if not rows:
        return 0, None

    return safe_upsert(
        table="google_trends_monthly",
        columns=["pipeline", "sheet", "series", "month", "value", "fetched_at"],
        rows=rows,
        conflict_columns=["pipeline", "sheet", "series", "month"],
    )

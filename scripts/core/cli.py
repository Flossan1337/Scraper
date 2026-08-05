"""
core/cli.py

Shared CLI helpers for scripts in this repo.
"""
from __future__ import annotations

import argparse
from datetime import date

NO_SIDE_EFFECTS_HELP = (
    "Fetch data and write to the database as usual, but skip writing local "
    "output files (state JSON, Excel report)."
)


def add_no_side_effects_flag(parser: argparse.ArgumentParser) -> None:
    """Adds --no-side-effects to an argparse parser with a consistent name/help
    text across scripts. Use args.no_side_effects to gate local file writes."""
    parser.add_argument(
        "--no-side-effects",
        action="store_true",
        help=NO_SIDE_EFFECTS_HELP,
    )


def log_skip(description: str) -> None:
    """Consistent log line for a local write skipped under --no-side-effects."""
    print(f"[--no-side-effects] Skipping: {description}")


def warn_if_gap(prev_date_str: str | None, today_str: str, metric_label: str = "sold/restock/return") -> None:
    """
    Warn (never raise, never change any computed number) when a delta script's
    previous snapshot is more than one calendar day before today - see
    KNOWN_ISSUES.md #3. These scripts attribute the full stock change since
    the previous snapshot to "today" with no gap check; if a nightly run was
    missed, that silently folds N missed days of real change into one day's
    number. This only makes the gap visible in the run log - by design it
    does not suppress or zero the computed delta, to keep behavior identical
    to what's already been validated against historical xlsx data.
    """
    if not prev_date_str:
        return
    gap_days = (date.fromisoformat(today_str) - date.fromisoformat(prev_date_str)).days
    if gap_days > 1:
        print(f"  [VARNING] Föregående snapshot är från {prev_date_str}, {gap_days} dagar sedan "
              f"(inte 1) — dagens {metric_label}-siffror slår ihop alla mellanliggande dagars "
              f"förändring till en enda dag. Se KNOWN_ISSUES.md #3.")

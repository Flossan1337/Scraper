"""
core/cli.py

Shared CLI helpers for scripts in this repo.
"""
from __future__ import annotations

import argparse

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

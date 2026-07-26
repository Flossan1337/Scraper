#!/usr/bin/env python3
"""
One-off script: walk the full git history of every data/*_state.json file
and dump each historical version to raw/<name>/<author-date>.json.

Uses the commit's AUTHOR date (not today's date, not commit/committer date).
Safe to re-run: it just overwrites files for dates it re-derives.
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from collections import defaultdict

REPO_ROOT = Path(
    subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
)
DATA_DIR = REPO_ROOT / "data"
RAW_DIR = REPO_ROOT / "raw"


def git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {result.stderr}")
    return result.stdout


def get_state_files() -> list[str]:
    return sorted(p.name for p in DATA_DIR.glob("*_state.json"))


def get_history(filename: str) -> list[tuple[str, str, str]]:
    """Return (commit_hash, author_date_iso, path_at_commit) oldest -> newest."""
    rel_path = f"data/{filename}"
    out = git(
        "log", "--follow", "--format=COMMIT\x1f%H\x1f%aI", "--name-only",
        "--", rel_path,
    )
    entries: list[tuple[str, str, str]] = []
    current: tuple[str, str] | None = None
    for line in out.splitlines():
        if line.startswith("COMMIT\x1f"):
            _, h, adate = line.split("\x1f")
            current = (h, adate)
        elif line.strip():
            assert current is not None
            entries.append((current[0], current[1], line.strip()))
    entries.reverse()  # oldest first
    return entries


def extract_file(filename: str) -> dict:
    stem = filename[: -len(".json")]
    out_dir = RAW_DIR / stem
    out_dir.mkdir(parents=True, exist_ok=True)

    history = get_history(filename)
    dates_seen: dict[str, str] = {}  # date -> commit hash (last wins, chronological)
    skipped = []

    for commit_hash, author_date, path_at_commit in history:
        date_only = author_date[:10]
        try:
            content = git("show", f"{commit_hash}:{path_at_commit}")
        except RuntimeError:
            skipped.append(commit_hash)
            continue
        (out_dir / f"{date_only}.json").write_text(content, encoding="utf-8", newline="")
        dates_seen[date_only] = commit_hash

    return {
        "commits_seen": len(history),
        "snapshots_written": len(dates_seen),
        "dates": sorted(dates_seen.keys()),
        "skipped_commits": skipped,
    }


def missing_dates(dates: list[str]) -> list[str]:
    if len(dates) < 2:
        return []
    from datetime import date, timedelta

    parsed = [date.fromisoformat(d) for d in dates]
    start, end = parsed[0], parsed[-1]
    have = set(parsed)
    missing = []
    d = start
    while d <= end:
        if d not in have:
            missing.append(d.isoformat())
        d += timedelta(days=1)
    return missing


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    summary = {}
    for filename in get_state_files():
        info = extract_file(filename)
        info["missing"] = missing_dates(info["dates"])
        summary[filename] = info
        print(f"{filename}: {info['snapshots_written']} snapshots "
              f"({info['dates'][0] if info['dates'] else '-'} .. "
              f"{info['dates'][-1] if info['dates'] else '-'}), "
              f"{len(info['missing'])} missing")

    return summary


if __name__ == "__main__":
    main()

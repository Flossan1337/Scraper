"""
core/db.py

Shared Postgres helpers for scripts that persist snapshots to the database.
Assumes target tables already exist (see sql/schema.sql and sql/migrations/).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Optional, Sequence

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(REPO_ROOT / ".env")  # no-op locally if absent; DATABASE_URL comes from CI env otherwise


def get_connection():
    """Connect to Postgres using DATABASE_URL (.env locally, CI env var in GitHub Actions)."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL not set. Locally: add it to .env. In CI: pass it as a "
            "step/job env var backed by a secret."
        )
    return psycopg2.connect(database_url)


def insert_rows(
    table: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    conflict_columns: Sequence[str],
    batch_size: int = 1000,
) -> int:
    """Batched INSERT ... ON CONFLICT (conflict_columns) DO NOTHING.
    Returns the total number of rows actually inserted (skipped conflicts don't count)."""
    if not rows:
        return 0

    insert_sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({', '.join(conflict_columns)}) DO NOTHING;"
    )

    conn = get_connection()
    try:
        rows_written = 0
        with conn.cursor() as cur:
            for i in range(0, len(rows), batch_size):
                execute_values(cur, insert_sql, rows[i:i + batch_size])
                rows_written += cur.rowcount
        conn.commit()
        return rows_written
    finally:
        conn.close()


def safe_insert(
    table: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    conflict_columns: Sequence[str],
    batch_size: int = 1000,
) -> Optional[int]:
    """Same as insert_rows, but never raises: logs the error to stderr and returns
    None on failure instead. The error text is also left on safe_insert.last_error."""
    try:
        result = insert_rows(table, columns, rows, conflict_columns, batch_size=batch_size)
        safe_insert.last_error = None
        return result
    except Exception as e:
        safe_insert.last_error = str(e)
        print(f"DB insert into {table} failed (continuing anyway): {e}", file=sys.stderr)
        return None


safe_insert.last_error = None

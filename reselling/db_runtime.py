"""Runtime DB connection adapter (SQLite / PostgreSQL)."""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Iterable


ROOT_DIR = Path(__file__).resolve().parents[1]
POSTGRES_SCHEMA_PATH = ROOT_DIR / "docs" / "sql" / "reseller_supabase_schema.sql"
_POSTGRES_SCHEMA_APPLIED = False


def _normalize_backend(raw: str) -> str:
    key = (raw or "").strip().lower()
    if key in {"postgres", "postgresql", "supabase"}:
        return "postgres"
    if key in {"sqlite", "local"}:
        return "sqlite"
    return ""


def resolve_backend() -> str:
    forced = _normalize_backend(os.getenv("DB_BACKEND", ""))
    if forced:
        return forced
    if (os.getenv("SUPABASE_DB_URL", "") or "").strip():
        return "postgres"
    return "sqlite"


def resolve_postgres_url() -> str:
    url = (os.getenv("SUPABASE_DB_URL", "") or "").strip()
    if url:
        return url
    forced = _normalize_backend(os.getenv("DB_BACKEND", ""))
    if forced == "postgres":
        fallback = (os.getenv("DATABASE_URL", "") or "").strip()
        if fallback:
            return fallback
    return ""


def _translate_params_sql(sql_text: str) -> str:
    # Convert sqlite-style '?' placeholders to psycopg '%s', skipping quoted strings.
    out: list[str] = []
    in_single = False
    in_double = False
    i = 0
    while i < len(sql_text):
        ch = sql_text[i]
        if ch == "'" and not in_double:
            if in_single and i + 1 < len(sql_text) and sql_text[i + 1] == "'":
                out.append("''")
                i += 2
                continue
            in_single = not in_single
            out.append(ch)
        elif ch == '"' and not in_single:
            in_double = not in_double
            out.append(ch)
        elif ch == "?" and not in_single and not in_double:
            out.append("%s")
        else:
            out.append(ch)
        i += 1
    return "".join(out)


class PostgresCursorAdapter:
    def __init__(self, cursor: Any):
        self._cursor = cursor

    @property
    def rowcount(self) -> int:
        return int(getattr(self._cursor, "rowcount", -1))

    @property
    def lastrowid(self) -> Any:
        return getattr(self._cursor, "lastrowid", None)

    def fetchone(self) -> Any:
        return self._cursor.fetchone()

    def fetchall(self) -> list[Any]:
        return self._cursor.fetchall()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


class DbConnection:
    """Tiny compatibility wrapper to share sqlite-style call sites."""

    def __init__(self, raw_conn: Any, *, backend: str):
        self._conn = raw_conn
        self.backend = backend

    def execute(self, sql_text: str, params: Iterable[Any] = ()) -> Any:
        if self.backend == "postgres":
            from psycopg.rows import dict_row

            translated = _translate_params_sql(sql_text)
            cur = self._conn.cursor(row_factory=dict_row)
            cur.execute(translated, tuple(params))
            return PostgresCursorAdapter(cur)
        return self._conn.execute(sql_text, tuple(params))

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "DbConnection":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        try:
            if exc_type is not None:
                self.rollback()
            else:
                self.commit()
        finally:
            self.close()
        return False


def is_postgres_connection(conn: Any) -> bool:
    return str(getattr(conn, "backend", "sqlite")).lower() == "postgres"


def connect_db(db_path: Path) -> DbConnection:
    backend = resolve_backend()
    if backend == "postgres":
        url = resolve_postgres_url()
        if not url:
            raise RuntimeError("postgres backend selected but SUPABASE_DB_URL is not set")
        try:
            import psycopg
        except ImportError as exc:
            venv_lib = ROOT_DIR / ".venv" / "lib"
            for site_dir in sorted(venv_lib.glob("python*/site-packages")):
                path_text = str(site_dir)
                if path_text not in sys.path:
                    sys.path.insert(0, path_text)
            try:
                import psycopg  # type: ignore[no-redef]
            except ImportError as re_exc:
                raise RuntimeError(
                    "psycopg is required for postgres backend: install with `.venv/bin/python -m pip install \"psycopg[binary]\"`"
                ) from re_exc
        return DbConnection(psycopg.connect(url), backend="postgres")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    sqlite_conn = sqlite3.connect(str(db_path))
    sqlite_conn.row_factory = sqlite3.Row
    return DbConnection(sqlite_conn, backend="sqlite")


def ensure_postgres_schema(conn: DbConnection) -> None:
    global _POSTGRES_SCHEMA_APPLIED
    if _POSTGRES_SCHEMA_APPLIED:
        return
    if not is_postgres_connection(conn):
        return
    if not POSTGRES_SCHEMA_PATH.exists():
        raise FileNotFoundError(f"postgres schema file not found: {POSTGRES_SCHEMA_PATH}")

    raw = POSTGRES_SCHEMA_PATH.read_text(encoding="utf-8")
    lines: list[str] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        if stripped.upper() in {"BEGIN", "BEGIN;", "COMMIT", "COMMIT;"}:
            continue
        lines.append(line)
    sql_text = "\n".join(lines)
    statements = [part.strip() for part in sql_text.split(";") if part.strip()]
    for stmt in statements:
        conn.execute(stmt)
    conn.commit()
    _POSTGRES_SCHEMA_APPLIED = True

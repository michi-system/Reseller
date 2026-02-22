"""DB models for Operator runtime (SQLite / PostgreSQL)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from reselling.db_runtime import DbConnection, connect_db, ensure_postgres_schema, is_postgres_connection


def connect(db_path: Path) -> DbConnection:
    return connect_db(db_path)


def init_db(conn: DbConnection) -> None:
    if is_postgres_connection(conn):
        ensure_postgres_schema(conn)
        return

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS approved_listing_inbox (
            approved_id TEXT PRIMARY KEY,
            approved_at TEXT NOT NULL,
            approved_by TEXT NOT NULL,
            sku_key TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            ingest_run_id TEXT NOT NULL,
            ingested_at TEXT NOT NULL,
            source_file_path TEXT NOT NULL DEFAULT '',
            source_file_hash TEXT NOT NULL DEFAULT '',
            listing_status TEXT NOT NULL DEFAULT 'ready',
            last_error TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS operator_listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            approved_id TEXT NOT NULL UNIQUE,
            channel TEXT NOT NULL DEFAULT 'ebay',
            channel_account_id TEXT NOT NULL DEFAULT '',
            channel_listing_id TEXT NOT NULL DEFAULT '',
            listing_state TEXT NOT NULL DEFAULT 'ready',
            title TEXT NOT NULL DEFAULT '',
            sku_key TEXT NOT NULL DEFAULT '',
            source_market TEXT NOT NULL DEFAULT '',
            target_market TEXT NOT NULL DEFAULT 'ebay',
            source_price_jpy REAL NOT NULL DEFAULT 0.0,
            target_price_usd REAL NOT NULL DEFAULT 0.0,
            fx_rate REAL NOT NULL DEFAULT 0.0,
            estimated_profit_jpy REAL NOT NULL DEFAULT 0.0,
            estimated_profit_rate REAL NOT NULL DEFAULT 0.0,
            current_source_price_jpy REAL NOT NULL DEFAULT 0.0,
            current_target_price_usd REAL NOT NULL DEFAULT 0.0,
            current_fx_rate REAL NOT NULL DEFAULT 0.0,
            current_profit_jpy REAL NOT NULL DEFAULT 0.0,
            current_profit_rate REAL NOT NULL DEFAULT 0.0,
            source_in_stock INTEGER NOT NULL DEFAULT 1,
            low_profit_streak INTEGER NOT NULL DEFAULT 0,
            low_stock_streak INTEGER NOT NULL DEFAULT 0,
            needs_review INTEGER NOT NULL DEFAULT 0,
            next_light_check_at TEXT,
            next_heavy_check_at TEXT,
            last_light_checked_at TEXT,
            last_heavy_checked_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(approved_id) REFERENCES approved_listing_inbox(approved_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS monitor_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id INTEGER NOT NULL,
            check_type TEXT NOT NULL,
            source_price_jpy REAL NOT NULL DEFAULT 0.0,
            target_price_usd REAL NOT NULL DEFAULT 0.0,
            fx_rate REAL NOT NULL DEFAULT 0.0,
            source_in_stock INTEGER NOT NULL DEFAULT 1,
            profit_jpy REAL NOT NULL DEFAULT 0.0,
            profit_rate REAL NOT NULL DEFAULT 0.0,
            decision TEXT NOT NULL,
            reason_code TEXT NOT NULL DEFAULT '',
            captured_at TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY(listing_id) REFERENCES operator_listings(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS listing_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            actor_type TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            reason_code TEXT NOT NULL DEFAULT '',
            note TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY(listing_id) REFERENCES operator_listings(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS job_runs (
            run_id TEXT PRIMARY KEY,
            job_type TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            processed_count INTEGER NOT NULL DEFAULT 0,
            success_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            error_summary TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS operator_config_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            config_version TEXT NOT NULL UNIQUE,
            min_profit_jpy REAL NOT NULL,
            min_profit_rate REAL NOT NULL,
            stop_consecutive_fail_count INTEGER NOT NULL,
            light_interval_new_hours INTEGER NOT NULL,
            light_interval_stable_hours INTEGER NOT NULL,
            light_interval_stopped_hours INTEGER NOT NULL,
            heavy_interval_days INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_operator_listings_state
        ON operator_listings(listing_state, updated_at DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_operator_listings_next_light
        ON operator_listings(next_light_check_at)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_operator_listings_next_heavy
        ON operator_listings(next_heavy_check_at)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_monitor_snapshots_listing_id
        ON monitor_snapshots(listing_id, captured_at DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_listing_events_listing_id
        ON listing_events(listing_id, created_at DESC)
        """
    )
    conn.commit()


def insert_job_run(conn: DbConnection, run_id: str, job_type: str, started_at: str) -> None:
    conn.execute(
        """
        INSERT INTO job_runs (
            run_id, job_type, started_at, status
        ) VALUES (?, ?, ?, 'running')
        """,
        (run_id, job_type, started_at),
    )
    conn.commit()


def finish_job_run(
    conn: DbConnection,
    *,
    run_id: str,
    finished_at: str,
    status: str,
    processed_count: int,
    success_count: int,
    error_count: int,
    error_summary: str = "",
) -> None:
    conn.execute(
        """
        UPDATE job_runs
        SET finished_at = ?,
            status = ?,
            processed_count = ?,
            success_count = ?,
            error_count = ?,
            error_summary = ?
        WHERE run_id = ?
        """,
        (
            finished_at,
            status,
            processed_count,
            success_count,
            error_count,
            error_summary,
            run_id,
        ),
    )
    conn.commit()


def seed_config_if_missing(
    conn: DbConnection,
    *,
    config_version: str,
    min_profit_jpy: float,
    min_profit_rate: float,
    stop_consecutive_fail_count: int,
    light_interval_new_hours: int,
    light_interval_stable_hours: int,
    light_interval_stopped_hours: int,
    heavy_interval_days: int,
    created_at: str,
    created_by: str,
) -> None:
    conn.execute(
        """
        INSERT INTO operator_config_versions (
            config_version,
            min_profit_jpy,
            min_profit_rate,
            stop_consecutive_fail_count,
            light_interval_new_hours,
            light_interval_stable_hours,
            light_interval_stopped_hours,
            heavy_interval_days,
            created_at,
            created_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(config_version) DO NOTHING
        """,
        (
            config_version,
            min_profit_jpy,
            min_profit_rate,
            stop_consecutive_fail_count,
            light_interval_new_hours,
            light_interval_stable_hours,
            light_interval_stopped_hours,
            heavy_interval_days,
            created_at,
            created_by,
        ),
    )
    conn.commit()


def latest_config(conn: DbConnection) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT *
        FROM operator_config_versions
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return dict(row)


def fetch_listings_by_state(conn: DbConnection, states: Iterable[str], limit: int) -> list[Any]:
    state_list = [str(s) for s in states]
    placeholders = ",".join("?" for _ in state_list)
    if not placeholders:
        return []
    query = f"""
        SELECT *
        FROM operator_listings
        WHERE listing_state IN ({placeholders})
        ORDER BY updated_at ASC, id ASC
        LIMIT ?
    """
    params = state_list + [max(1, int(limit))]
    return conn.execute(query, params).fetchall()

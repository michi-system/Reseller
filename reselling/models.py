"""SQLite helpers for runtime state."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fx_rate_states (
            pair TEXT PRIMARY KEY,
            rate REAL NOT NULL,
            source TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            next_refresh_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS review_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_site TEXT NOT NULL,
            market_site TEXT NOT NULL,
            source_item_id TEXT,
            market_item_id TEXT,
            source_title TEXT NOT NULL,
            market_title TEXT NOT NULL,
            condition TEXT NOT NULL DEFAULT 'new',
            match_level TEXT NOT NULL DEFAULT 'L2_precise',
            match_score REAL NOT NULL DEFAULT 0.0,
            expected_profit_usd REAL NOT NULL DEFAULT 0.0,
            expected_margin_rate REAL NOT NULL DEFAULT 0.0,
            fx_rate REAL NOT NULL DEFAULT 0.0,
            fx_source TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            listing_state TEXT NOT NULL DEFAULT 'dummy_pending',
            listing_reference TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            approved_at TEXT,
            rejected_at TEXT,
            listed_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS review_rejections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_id INTEGER NOT NULL,
            issue_targets_json TEXT NOT NULL,
            reason_text TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(candidate_id) REFERENCES review_candidates(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS liquidity_signals (
            signal_key TEXT PRIMARY KEY,
            sold_90d_count INTEGER NOT NULL DEFAULT -1,
            active_count INTEGER NOT NULL DEFAULT -1,
            sell_through_90d REAL NOT NULL DEFAULT -1.0,
            sold_price_median REAL NOT NULL DEFAULT -1.0,
            sold_price_currency TEXT NOT NULL DEFAULT 'USD',
            source TEXT NOT NULL DEFAULT '',
            confidence REAL NOT NULL DEFAULT 0.0,
            unavailable_reason TEXT NOT NULL DEFAULT '',
            fetched_at TEXT NOT NULL,
            next_refresh_at TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_review_candidates_status_created_at
        ON review_candidates(status, created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_review_rejections_candidate_id
        ON review_rejections(candidate_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_liquidity_signals_next_refresh_at
        ON liquidity_signals(next_refresh_at)
        """
    )
    conn.commit()


def get_fx_rate_state(conn: sqlite3.Connection, pair: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT pair, rate, source, fetched_at, next_refresh_at
        FROM fx_rate_states
        WHERE pair = ?
        """,
        (pair,),
    ).fetchone()
    if row is None:
        return None
    return dict(row)


def upsert_fx_rate_state(
    conn: sqlite3.Connection,
    *,
    pair: str,
    rate: float,
    source: str,
    fetched_at: str,
    next_refresh_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO fx_rate_states (pair, rate, source, fetched_at, next_refresh_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(pair) DO UPDATE SET
            rate = excluded.rate,
            source = excluded.source,
            fetched_at = excluded.fetched_at,
            next_refresh_at = excluded.next_refresh_at
        """,
        (pair, rate, source, fetched_at, next_refresh_at),
    )
    conn.commit()

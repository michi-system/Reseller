"""DB helpers for runtime state (SQLite / PostgreSQL)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from .db_runtime import DbConnection, connect_db, ensure_postgres_schema, is_postgres_connection


def connect(db_path: Path) -> DbConnection:
    return connect_db(db_path)


def _table_exists(conn: DbConnection, table_name: str) -> bool:
    if is_postgres_connection(conn):
        row = conn.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = ?
            ) AS exists
            """,
            (table_name,),
        ).fetchone()
        return bool(row["exists"]) if row is not None else False
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _rename_table(conn: DbConnection, old_name: str, new_name: str) -> None:
    conn.execute(f'ALTER TABLE "{old_name}" RENAME TO "{new_name}"')


def _migrate_legacy_review_tables(conn: DbConnection) -> None:
    if _table_exists(conn, "review_candidates") and not _table_exists(conn, "miner_candidates"):
        _rename_table(conn, "review_candidates", "miner_candidates")
    if _table_exists(conn, "review_rejections") and not _table_exists(conn, "miner_rejections"):
        _rename_table(conn, "review_rejections", "miner_rejections")


def init_db(conn: DbConnection) -> None:
    if is_postgres_connection(conn):
        ensure_postgres_schema(conn)
        _migrate_legacy_review_tables(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS miner_seed_pool (
                id BIGSERIAL PRIMARY KEY,
                category_key TEXT NOT NULL,
                seed_query TEXT NOT NULL,
                seed_key TEXT NOT NULL,
                source_title TEXT NOT NULL DEFAULT '',
                source_item_url TEXT NOT NULL DEFAULT '',
                source_page INTEGER NOT NULL DEFAULT 1,
                source_offset INTEGER NOT NULL DEFAULT 0,
                source_rank INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                consumed_at TEXT,
                last_used_at TEXT,
                use_count INTEGER NOT NULL DEFAULT 0,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(category_key, seed_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS miner_seed_refill_state (
                category_key TEXT PRIMARY KEY,
                last_refill_at TEXT NOT NULL,
                last_refill_status TEXT NOT NULL DEFAULT '',
                last_refill_message TEXT NOT NULL DEFAULT '',
                last_rank_checked INTEGER NOT NULL DEFAULT 0,
                cooldown_until TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS miner_seed_refill_pages (
                category_key TEXT NOT NULL,
                query_key TEXT NOT NULL,
                page_offset INTEGER NOT NULL,
                page_size INTEGER NOT NULL DEFAULT 50,
                fetched_at TEXT NOT NULL,
                result_count INTEGER NOT NULL DEFAULT 0,
                new_seed_count INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(category_key, query_key, page_offset)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_miner_candidates_status_created_at
            ON miner_candidates(status, created_at DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_miner_rejections_candidate_id
            ON miner_rejections(candidate_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_miner_seed_pool_category_rank
            ON miner_seed_pool(category_key, source_rank ASC, id ASC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_miner_seed_pool_category_expiry
            ON miner_seed_pool(category_key, expires_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_miner_seed_refill_pages_category_query
            ON miner_seed_refill_pages(category_key, query_key, page_offset)
            """
        )
        conn.commit()
        return

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
    _migrate_legacy_review_tables(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS miner_candidates (
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
        CREATE TABLE IF NOT EXISTS miner_rejections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_id INTEGER NOT NULL,
            issue_targets_json TEXT NOT NULL,
            reason_text TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(candidate_id) REFERENCES miner_candidates(id)
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
        CREATE TABLE IF NOT EXISTS miner_seed_pool (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_key TEXT NOT NULL,
            seed_query TEXT NOT NULL,
            seed_key TEXT NOT NULL,
            source_title TEXT NOT NULL DEFAULT '',
            source_item_url TEXT NOT NULL DEFAULT '',
            source_page INTEGER NOT NULL DEFAULT 1,
            source_offset INTEGER NOT NULL DEFAULT 0,
            source_rank INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            consumed_at TEXT,
            last_used_at TEXT,
            use_count INTEGER NOT NULL DEFAULT 0,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(category_key, seed_key)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS miner_seed_refill_state (
            category_key TEXT PRIMARY KEY,
            last_refill_at TEXT NOT NULL,
            last_refill_status TEXT NOT NULL DEFAULT '',
            last_refill_message TEXT NOT NULL DEFAULT '',
            last_rank_checked INTEGER NOT NULL DEFAULT 0,
            cooldown_until TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS miner_seed_refill_pages (
            category_key TEXT NOT NULL,
            query_key TEXT NOT NULL,
            page_offset INTEGER NOT NULL,
            page_size INTEGER NOT NULL DEFAULT 50,
            fetched_at TEXT NOT NULL,
            result_count INTEGER NOT NULL DEFAULT 0,
            new_seed_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(category_key, query_key, page_offset)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_miner_candidates_status_created_at
        ON miner_candidates(status, created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_miner_rejections_candidate_id
        ON miner_rejections(candidate_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_liquidity_signals_next_refresh_at
        ON liquidity_signals(next_refresh_at)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_miner_seed_pool_category_rank
        ON miner_seed_pool(category_key, source_rank ASC, id ASC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_miner_seed_pool_category_expiry
        ON miner_seed_pool(category_key, expires_at)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_miner_seed_refill_pages_category_query
        ON miner_seed_refill_pages(category_key, query_key, page_offset)
        """
    )
    conn.commit()


def get_fx_rate_state(conn: DbConnection, pair: str) -> Optional[Dict[str, Any]]:
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
    conn: DbConnection,
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

-- Reseller (Miner + Operator) PostgreSQL schema for Supabase
-- Generated for big-bang migration from local SQLite.

BEGIN;

CREATE TABLE IF NOT EXISTS fx_rate_states (
    pair TEXT PRIMARY KEY,
    rate DOUBLE PRECISION NOT NULL,
    source TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    next_refresh_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS miner_candidates (
    id BIGSERIAL PRIMARY KEY,
    source_site TEXT NOT NULL,
    market_site TEXT NOT NULL,
    source_item_id TEXT,
    market_item_id TEXT,
    source_title TEXT NOT NULL,
    market_title TEXT NOT NULL,
    condition TEXT NOT NULL DEFAULT 'new',
    match_level TEXT NOT NULL DEFAULT 'L2_precise',
    match_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    expected_profit_usd DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    expected_margin_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    fx_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0,
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
);

CREATE TABLE IF NOT EXISTS miner_rejections (
    id BIGSERIAL PRIMARY KEY,
    candidate_id BIGINT NOT NULL REFERENCES miner_candidates(id),
    issue_targets_json TEXT NOT NULL,
    reason_text TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS liquidity_signals (
    signal_key TEXT PRIMARY KEY,
    sold_90d_count INTEGER NOT NULL DEFAULT -1,
    active_count INTEGER NOT NULL DEFAULT -1,
    sell_through_90d DOUBLE PRECISION NOT NULL DEFAULT -1.0,
    sold_price_median DOUBLE PRECISION NOT NULL DEFAULT -1.0,
    sold_price_currency TEXT NOT NULL DEFAULT 'USD',
    source TEXT NOT NULL DEFAULT '',
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    unavailable_reason TEXT NOT NULL DEFAULT '',
    fetched_at TEXT NOT NULL,
    next_refresh_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}'
);

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
);

CREATE TABLE IF NOT EXISTS operator_listings (
    id BIGSERIAL PRIMARY KEY,
    approved_id TEXT NOT NULL UNIQUE REFERENCES approved_listing_inbox(approved_id),
    channel TEXT NOT NULL DEFAULT 'ebay',
    channel_account_id TEXT NOT NULL DEFAULT '',
    channel_listing_id TEXT NOT NULL DEFAULT '',
    listing_state TEXT NOT NULL DEFAULT 'ready',
    title TEXT NOT NULL DEFAULT '',
    sku_key TEXT NOT NULL DEFAULT '',
    source_market TEXT NOT NULL DEFAULT '',
    target_market TEXT NOT NULL DEFAULT 'ebay',
    source_price_jpy DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    target_price_usd DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    fx_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    estimated_profit_jpy DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    estimated_profit_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    current_source_price_jpy DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    current_target_price_usd DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    current_fx_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    current_profit_jpy DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    current_profit_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    source_in_stock INTEGER NOT NULL DEFAULT 1,
    low_profit_streak INTEGER NOT NULL DEFAULT 0,
    low_stock_streak INTEGER NOT NULL DEFAULT 0,
    needs_review INTEGER NOT NULL DEFAULT 0,
    next_light_check_at TEXT,
    next_heavy_check_at TEXT,
    last_light_checked_at TEXT,
    last_heavy_checked_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS monitor_snapshots (
    id BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL REFERENCES operator_listings(id),
    check_type TEXT NOT NULL,
    source_price_jpy DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    target_price_usd DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    fx_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    source_in_stock INTEGER NOT NULL DEFAULT 1,
    profit_jpy DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    profit_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    decision TEXT NOT NULL,
    reason_code TEXT NOT NULL DEFAULT '',
    captured_at TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS listing_events (
    id BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL REFERENCES operator_listings(id),
    event_type TEXT NOT NULL,
    actor_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    reason_code TEXT NOT NULL DEFAULT '',
    note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}'
);

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
);

CREATE TABLE IF NOT EXISTS operator_config_versions (
    id BIGSERIAL PRIMARY KEY,
    config_version TEXT NOT NULL UNIQUE,
    min_profit_jpy DOUBLE PRECISION NOT NULL,
    min_profit_rate DOUBLE PRECISION NOT NULL,
    stop_consecutive_fail_count INTEGER NOT NULL,
    light_interval_new_hours INTEGER NOT NULL,
    light_interval_stable_hours INTEGER NOT NULL,
    light_interval_stopped_hours INTEGER NOT NULL,
    heavy_interval_days INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_miner_candidates_status_created_at
    ON miner_candidates(status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_miner_rejections_candidate_id
    ON miner_rejections(candidate_id);

CREATE INDEX IF NOT EXISTS idx_liquidity_signals_next_refresh_at
    ON liquidity_signals(next_refresh_at);

CREATE INDEX IF NOT EXISTS idx_operator_listings_state
    ON operator_listings(listing_state, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_operator_listings_next_light
    ON operator_listings(next_light_check_at);

CREATE INDEX IF NOT EXISTS idx_operator_listings_next_heavy
    ON operator_listings(next_heavy_check_at);

CREATE INDEX IF NOT EXISTS idx_monitor_snapshots_listing_id
    ON monitor_snapshots(listing_id, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_listing_events_listing_id
    ON listing_events(listing_id, created_at DESC);

COMMIT;

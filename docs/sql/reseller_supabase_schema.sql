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
);

CREATE TABLE IF NOT EXISTS miner_seed_refill_state (
    category_key TEXT PRIMARY KEY,
    last_refill_at TEXT NOT NULL,
    last_refill_status TEXT NOT NULL DEFAULT '',
    last_refill_message TEXT NOT NULL DEFAULT '',
    last_rank_checked INTEGER NOT NULL DEFAULT 0,
    cooldown_until TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);

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
);

CREATE TABLE IF NOT EXISTS miner_seed_liquidity_cooldowns (
    id BIGSERIAL PRIMARY KEY,
    category_key TEXT NOT NULL,
    seed_key TEXT NOT NULL,
    seed_query TEXT NOT NULL DEFAULT '',
    reason_code TEXT NOT NULL DEFAULT '',
    sold_90d_count INTEGER NOT NULL DEFAULT -1,
    min_required INTEGER NOT NULL DEFAULT 0,
    blocked_until TEXT NOT NULL,
    last_rejected_at TEXT NOT NULL,
    reject_count INTEGER NOT NULL DEFAULT 1,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(category_key, seed_key)
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

CREATE INDEX IF NOT EXISTS idx_miner_seed_pool_category_rank
    ON miner_seed_pool(category_key, source_rank ASC, id ASC);

CREATE INDEX IF NOT EXISTS idx_miner_seed_pool_category_expiry
    ON miner_seed_pool(category_key, expires_at);

CREATE INDEX IF NOT EXISTS idx_miner_seed_refill_pages_category_query
    ON miner_seed_refill_pages(category_key, query_key, page_offset);

CREATE INDEX IF NOT EXISTS idx_miner_seed_liquidity_cooldowns_active
    ON miner_seed_liquidity_cooldowns(category_key, blocked_until);

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

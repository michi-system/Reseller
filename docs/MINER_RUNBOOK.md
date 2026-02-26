# Miner Runbook (Unified)

最終更新: 2026-02-26

この文書は Miner 実運用の手順書です。A/B/C 実行、UI設定、確認コマンドを最新実装に合わせています。

## 1. 事前準備
- `.env.local` に秘密情報を設定し、Git管理しない
- 主要キー:
  - `EBAY_CLIENT_ID`
  - `EBAY_CLIENT_SECRET`
  - `YAHOO_APP_ID`（または `YAHOO_CLIENT_ID`）
  - `RAKUTEN_APPLICATION_ID`

推奨:
- `DB_BACKEND=sqlite`（ローカル検証時）
- `LIQUIDITY_PROVIDER_MODE=rpa_json`

## 2. API起動
```bash
python3 scripts/run_api.py --host 127.0.0.1 --port 8000
```

ヘルスチェック:
```bash
curl -sS http://127.0.0.1:8000/healthz
```

## 3. Miner UI設定（永続化）
取得:
```bash
curl -sS http://127.0.0.1:8000/v1/miner/settings
```

保存:
```bash
curl -sS -X POST http://127.0.0.1:8000/v1/miner/settings \
  -H 'Content-Type: application/json' \
  -d '{
    "requireInStock": true,
    "limitPerSite": 20,
    "maxCandidates": 20,
    "stageABigWordLimit": 0,
    "stageAMinimizeTransitions": true,
    "stageBQueryMode": "seed_only",
    "stageBMaxQueriesPerSite": 1,
    "stageBTopMatchesPerSeed": 3,
    "stageBApiMaxCallsPerRun": 0,
    "stageCMinSold90d": 10,
    "stageCLiquidityRefreshEnabled": true,
    "stageCLiquidityRefreshBudget": 12,
    "stageCAllowMissingSoldSample": false,
    "stageCEbayItemDetailEnabled": true,
    "stageCEbayItemDetailMaxFetch": 30,
    "minMatchScore": 0.72,
    "minProfitUsd": 0.01,
    "minMarginRate": 0.03
  }'
```

## 4. 探索実行（A→B→C）
```bash
curl -sS -X POST http://127.0.0.1:8000/v1/miner/fetch \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "watch",
    "source_sites": ["rakuten", "yahoo"],
    "timed_mode": true,
    "target_min_candidates": 3,
    "fetch_timebox_sec": 300,
    "fetch_max_passes": 20,
    "continue_after_target": true,
    "require_in_stock": true,
    "limit_per_site": 20,
    "max_candidates": 20,
    "stage_a_big_word_limit": 0,
    "stage_a_minimize_transitions": true,
    "stage_b_query_mode": "seed_only",
    "stage_b_max_queries_per_site": 1,
    "stage_b_top_matches_per_seed": 3,
    "stage_b_api_max_calls_per_run": 0,
    "stage_c_min_sold_90d": 10,
    "stage_c_liquidity_refresh_on_miss_enabled": true,
    "stage_c_liquidity_refresh_on_miss_budget": 12,
    "stage_c_allow_missing_sold_sample": false,
    "stage_c_ebay_item_detail_enabled": true,
    "stage_c_ebay_item_detail_max_fetch_per_run": 30,
    "min_match_score": 0.72,
    "min_profit_usd": 0.01,
    "min_margin_rate": 0.03
  }'
```

## 5. 実行中の確認
進捗:
```bash
curl -sS http://127.0.0.1:8000/v1/system/fetch-progress
```

seed pool:
```bash
curl -sS 'http://127.0.0.1:8000/v1/miner/seed-pool-status?category=watch'
```

RPA進捗:
```bash
curl -sS http://127.0.0.1:8000/v1/system/rpa-progress
```

## 6. 結果確認
レビュー待ち:
```bash
curl -sS 'http://127.0.0.1:8000/v1/miner/queue?status=pending&limit=50'
```

レビュー済み:
```bash
curl -sS 'http://127.0.0.1:8000/v1/miner/queue?status=reviewed&limit=50'
```

## 7. Product Research（A/C向け）運用要点
- URL先入れで `condition/minPrice/offset/sorting` を試行
- `format=fixed_price` はUI確定が必要なため、URLのみを信用しない
- `Date last sold` ヘッダーで新しい順を最終確定
- 本番では `--screenshot-after-filters` / `--html-after-filters` は無効（速度優先）

## 8. テスト
主要回帰（SQLite固定）:
```bash
DB_BACKEND=sqlite python3 -m unittest -q \
  tests.test_miner_seed_pool \
  tests.test_liquidity_rpa_guard \
  tests.test_api_miner_settings \
  tests.test_api_fetch_progress \
  tests.test_miner_queue_status
```

## 9. 障害時ガイド
- `rpa_daily_limit_reached`: 当日停止。翌日に再実行
- `seed_pool_empty`: A段階補充条件/カテゴリ設定を確認
- `skipped_liquidity_unavailable` 多発: RPA JSONの更新状態、DOM変化、PR上限を確認
- `skipped_source_variant_unresolved` 多発: 複数SKUページの型番解決失敗。サイト別fallback条件を再確認

## 10. 禁止事項
- `docs/*_latest.json`, `docs/autonomous_*`, `docs/cycle_diagnostics/*` の手編集禁止
- `data/miner_*`, `data/liquidity_*` の手編集禁止

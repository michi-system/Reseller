# Local API

## Start server

```bash
python3 scripts/run_api.py --host 127.0.0.1 --port 8000
```

## Endpoints

### Health

```bash
curl -sS http://127.0.0.1:8000/healthz
```

### Current FX

```bash
curl -sS http://127.0.0.1:8000/v1/system/fx-rate
```

### Refresh FX

Normal (refresh only if due):

```bash
curl -sS -X POST http://127.0.0.1:8000/v1/system/fx-rate/refresh
```

Force refresh:

```bash
curl -sS -X POST "http://127.0.0.1:8000/v1/system/fx-rate/refresh?force=true"
```

### Profit calc

```bash
curl -sS -X POST http://127.0.0.1:8000/v1/profit/calc \
  -H "Content-Type: application/json" \
  -d '{
    "sale_price_usd": 420,
    "purchase_price_jpy": 42000,
    "domestic_shipping_jpy": 1200,
    "international_shipping_usd": 28,
    "customs_usd": 8,
    "packaging_usd": 3,
    "refresh_fx": true
  }'
```

### Create review candidate

```bash
curl -sS -X POST http://127.0.0.1:8000/v1/review/candidates \
  -H "Content-Type: application/json" \
  -d '{
    "source_site": "rakuten",
    "market_site": "ebay",
    "source_item_id": "rakuten-123",
    "market_item_id": "ebay-456",
    "source_title": "SEIKO SBGA211 新品",
    "market_title": "Grand Seiko SBGA211 NEW",
    "condition": "new",
    "match_level": "L2_precise",
    "match_score": 0.93,
    "expected_profit_usd": 62.5,
    "expected_margin_rate": 0.18
  }'
```

### Review queue

```bash
curl -sS "http://127.0.0.1:8000/v1/review/queue?status=pending&limit=50"
```

Profit-positive / precision-first queue (recommended for manual review):

```bash
curl -sS "http://127.0.0.1:8000/v1/review/queue?status=pending&limit=50&min_profit_usd=0.01&min_margin_rate=0.03&min_match_score=0.75&condition=new"
```

自動承認（最終確認待ち）キュー:

```bash
curl -sS "http://127.0.0.1:8000/v1/review/queue?status=approved&limit=50"
```

### Review fetch (live APIs -> candidate auto-create)

```bash
curl -sS -X POST http://127.0.0.1:8000/v1/review/fetch \
  -H "Content-Type: application/json" \
  -d '{
    "query": "watch",
    "source_sites": ["rakuten", "yahoo"],
    "market_site": "ebay",
    "require_in_stock": true,
    "limit_per_site": 20,
    "max_candidates": 20,
    "min_match_score": 0.75,
    "min_profit_usd": 0.01,
    "min_margin_rate": 0.03
  }'
```

レスポンスの `fetched` にはサイト別の実行詳細が含まれます（`calls_made`, `max_calls`, `target_items`, `stop_reason`, `queries[]`）。
`queries[]` で「どの検索語を何回使ったか」を確認できます。
`query_cache_skip=true` の場合は、同一条件で直近完走済み・新規0件のため API 呼び出しを省略しています。
`query` にカテゴリ名（例: `watch`, `腕時計`, `trading cards`）を入れた場合は、
`data/category_knowledge_seeds_v1.json` を使ってメーカー/シリーズ/型番クエリへ自動展開します。
適用時はレスポンス `hints` と `fetched.<site>.knowledge` に反映されます。

流動性Gate（90日回転）:
- `LIQUIDITY_GATE_ENABLED=1` で有効
- `LIQUIDITY_MIN_SOLD_90D` / `LIQUIDITY_MIN_SELL_THROUGH_90D` で閾値調整
- `LIQUIDITY_REQUIRE_SIGNAL=1` の場合、流動性データ未取得候補を除外
- `LIQUIDITY_PROVIDER_MODE=ebay_marketplace_insights` で eBay側売却履歴APIを優先
- `LIQUIDITY_PROVIDER_MODE=rpa_json` で Product Research RPAの出力JSONを使用
- Insights失敗時の代替は `LIQUIDITY_FALLBACK_MODE`（`none` / `http_json` / `rpa_json` / `mock`）
- 候補メタデータ `metadata.liquidity` に判定根拠が保存されます

Product Research RPA収集:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install playwright
.venv/bin/playwright install chromium

.venv/bin/python scripts/rpa_market_research.py \
  --query "seiko sbdc101" \
  --query "citizen nb1050" \
  --lookback-days 90 \
  --condition new \
  --strict-condition \
  --fixed-price-only \
  --signal-key-map "seiko sbdc101=model:SBDC101" \
  --output data/liquidity_rpa_signals.jsonl
```

初回はブラウザでログイン（2FA含む）し、保存プロファイルを使って再利用します。
収集後に以下を設定すると、レビュー作成時にRPA値を使って流動性判定します。

```bash
LIQUIDITY_PROVIDER_MODE=rpa_json
LIQUIDITY_RPA_JSON_PATH=data/liquidity_rpa_signals.jsonl
LIQUIDITY_RPA_MAX_AGE_SECONDS=604800
LIQUIDITY_REQUIRE_SIGNAL=1
```

自動実行フロー（`run_review_cycle.py` 内で起動）:
- `LIQUIDITY_PROVIDER_MODE=rpa_json` かつ `LIQUIDITY_RPA_AUTO_REFRESH=1` のとき、fetch前にRPA収集を自動実行
- 1st pass: `condition=new`（厳格）
- 2nd pass: 1st passで `sold_90d_count` 未取得クエリのみ `condition=any` で再収集（欠損埋め）
- 3rd pass: 2nd pass後も未取得なら、モデルコード抽出クエリで再収集（`LIQUIDITY_RPA_ENABLE_MODEL_CODE_BACKFILL=1`）
- 既定は `LIQUIDITY_RPA_MODEL_CODE_BACKFILL_SOURCE=unavailable_reason_only` で、前サイクルで `liquidity_unavailable_required` になったモデルコードのみ再収集
- 理由別ターゲットは `data/liquidity_backfill_targets.json` に保存され、TTL（`LIQUIDITY_BACKFILL_TARGET_TTL_SECONDS`）で自動整理
- `skipped_liquidity_unavailable` が出たのにモデルコード抽出できない場合は、検索クエリから型番を補完してターゲット化
- `sold_price_min` が `sold_price_median` に対して極端に低い場合（既定 ratio `< 0.35`）は外れ値として除外
  - しきい値: `LIQUIDITY_SOLD_PRICE_MIN_OUTLIER_RATIO`
  - 外れ値時は `metadata.sold_price_min_outlier=true` と `sold_price_min_raw` を保持
- 設定は `.env.local` の `LIQUIDITY_RPA_*` で調整

`sold_90d_count` の意味:
- `>=0`: 取得成功（`0` は「90日内売却なし」）
- `-1`: 未取得/判定不能（UI取得失敗、アクセス制約、要素未検出など）

自動レビュー固定ポリシー（`scripts/auto_review_cycle.py`）:
- `AUTO_REVIEW_REQUIRE_LIQUIDITY_SIGNAL=1` なら `sold_90d_count=-1` を自動承認しない
- `AUTO_REVIEW_BLOCK_COLOR_MISSING_MARKET=1` なら eBay側色情報欠損マッチを自動承認しない
- `AUTO_REVIEW_ALLOW_FALLBACK_ANY=1` でも、`fallback_any` は追加条件を満たす場合のみ承認
- 追加条件:
  - `AUTO_REVIEW_FALLBACK_MIN_SOLD_90D` 以上の売却件数
  - `AUTO_REVIEW_FALLBACK_MIN_PROFIT_USD` 以上の期待利益
  - `sold_price_min` が取得済み

EV90（90日期待値）:
- 候補作成時に `metadata.ev90` を算出して保存
- `EV90_MIN_USD` 未満は `skipped_low_ev90` として除外
- `EV90_ENFORCE_WITHOUT_LIQUIDITY=0`（既定）なら流動性未取得時はEV90で除外しない
- 自動レビューでも `--min-ev90-usd` 閾値で再判定可能

API節約重視のデフォルト方針:
- 1クエリ内では `results/hits/limit` をサイト上限に寄せる（eBay最大200, Yahoo最大50, Rakuten最大30）
- 同一クエリ内でページングしてから次のクエリに進む
- `target_items` 到達か低歩留まりで早期停止
- 直近完走済みで新規0件のクエリはTTL中スキップ（`REVIEW_QUERY_SKIP_TTL_SECONDS`）
- 1回の取得で重複比率が高いクエリは同一サイクル内で早期クールダウン（既定ON）
  - `--duplicate-heavy-ratio-threshold`（既定 `0.70`）
  - `--duplicate-heavy-min-evaluated`（既定 `12`）
  - `--duplicate-heavy-min-duplicates`（既定 `8`）
  - `--disable-duplicate-heavy-cooldown` で無効化可能
- 型番入りクエリは毎回 narrow 先頭クエリから開始（`REVIEW_FETCH_FORCE_EXACT_FOR_MODEL_QUERY=1`）
- eBay側の色情報欠損でも、識別子/型番が強一致する場合は候補化して取りこぼしを抑制（既定ON）
  - `REVIEW_MATCH_ALLOW_COLOR_MISSING_WITH_IDENTIFIER=1`
  - `REVIEW_MATCH_ALLOW_COLOR_MISSING_WITH_MODEL_CODE=1`
  - 自動承認では色リスク理由として保持し、最終はレビューで確認
- `review_cycle_report` の `low_match_reason_counts` / `low_match_samples` で一致不足の主因を確認可能
- `skipped_ambiguous_model_title` で複数型番列挙タイトルの除外数を確認可能
- `review_cycle_report` の `liquidity_backfill` で、reason-target更新有無（`updated/added_entries/touched_entries`）を確認可能

## Cycle scripts

Build queue up to target size (example: 20 candidates):

```bash
python3 scripts/run_review_cycle.py --target-count 20
```

Recommended precision-first cycle (24 candidates + fixed manifest):

```bash
python3 scripts/run_review_cycle.py --target-count 24 --hard-cap 30 --min-profit-usd 0.01 --min-margin-rate 0.03 --min-match-score 0.75 --require-full-batch
```

API制限を意識した検証モード（推奨）:

```bash
python3 scripts/run_review_cycle.py \
  --target-count 24 \
  --hard-cap 30 \
  --max-zero-gain-strikes 2 \
  --daily-budget-ebay 120 \
  --daily-budget-rakuten 120 \
  --daily-budget-yahoo 120
```

低収穫クエリは履歴学習で自動スキップされます（`data/query_efficiency_stats.json`）。
再評価周期は `--historical-retry-every-runs` で調整できます。

完全オフライン検証（キャッシュのみ利用）:

```bash
python3 scripts/run_review_cycle.py --target-count 24 --cache-only --cache-ttl-seconds 86400
```

`review_cycle_report_latest.json` には `api_efficiency_summary`（network/cache比率）が出力されます。

Generate close report for the active cycle:

```bash
python3 scripts/close_review_cycle.py --reject-floor 10 --min-reviewed-ratio 1.0 --min-reject-rate 0.10
```

Get active cycle manifest:

```bash
curl -sS http://127.0.0.1:8000/v1/review/cycle/active
```

Apply logic improvements from rejected items in active cycle:

```bash
python3 scripts/apply_cycle_improvements.py
```

Issue target が空の否認を `other` として取り込みたい場合:

```bash
python3 scripts/apply_cycle_improvements.py --allow-empty-issue-target
```

Run one full autonomous cycle (start + auto-review + close + improvements):

```bash
python3 scripts/run_autonomous_cycle.py --target-count 24 --hard-cap 30 --min-ev90-usd 0
```

Notes:
- デフォルトはフルバッチ必須（`--require-full-batch` 相当）です。部分バッチで回す場合は `--allow-partial-batch` を付けます。
- 自動レビューは `run_review_cycle` の保存スコアに加えて再マッチ判定を行い、同一商品根拠が弱いものは自動承認しません。
- `--skip-apply-when-not-ready` を付けると `ready_for_tuning=false` のとき通常は改善適用をスキップします。
- ただし `ready_for_light_tuning=true` かつ `rejected_with_issue_count` が閾値以上なら light mode で改善適用します（既定ON）。
- light mode を無効化する場合は `--disable-light-tuning` を指定します。
- 既定で `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/OPERATION_POLICY.json` を読み、閾値と運用条件（新品固定・流動性必須）を事前検証します。
- 実験でのみ外す場合は `--skip-policy-check` を使います（本番運用では非推奨）。
- 実行後に `review/auto/close` の整合性検証を行い、`docs/review_cycle_validation_latest.json` を出力します。
- 履歴スキップを一時的に外して検証したい場合は `--historical-min-attempts 999 --historical-retry-every-runs 1` を指定します。

Guarded multi-cycle (fail-fast) 実行:

```bash
python3 scripts/run_autonomous_cycles_guarded.py \
  --cycles 5 \
  --stagnation-limit 2 \
  --cycle-args "--target-count 24 --hard-cap 30"
```

Guarded 実行の仕様:
- 各周で `run_autonomous_cycle.py` を実行し、検証結果 (`validation_ok`) を即チェック
- `batch_size=0` / `progressed=false` が連続したら早期停止（`--stagnation-limit`）
- 周ごとのログと JSON を `docs/autonomous_guarded_runs/` に保存
- `summary_latest.json` に `aggregate_review_kpi`（低流動性除外率 / EV90除外率）を出力
- フルバッチ厳格のまま回す場合はそのまま、部分バッチ許容にする場合は `--allow-partial-batch` を付与

Cleanup pending queue by auto-rejecting obvious outliers:

```bash
python3 scripts/cleanup_pending_queue.py --apply
```

### Review approve (dummy listing)

```bash
curl -sS -X POST http://127.0.0.1:8000/v1/review/candidates/1/approve
```

### Review reject (targets + reason)

```bash
curl -sS -X POST http://127.0.0.1:8000/v1/review/candidates/1/reject \
  -H "Content-Type: application/json" \
  -d '{
    "issue_targets": ["model", "price", "shipping"],
    "reason_text": "型番末尾が異なる。送料想定が甘い。"
  }'
```

`reason_text` は任意です。`issue_targets` のみでも保存できます。

### Review candidate detail

```bash
curl -sS http://127.0.0.1:8000/v1/review/candidates/1
```

## Scheduler

```bash
python3 scripts/run_scheduler.py --interval-seconds 30
```

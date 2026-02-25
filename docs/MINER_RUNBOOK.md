# Miner Runbook (Unified)

最終更新: 2026-02-25

この文書は、Minerの実行手順（事前チェック / API / サイクル運用 / 利益計算）を1本に統合した運用手順書です。

## 1. 事前準備
`.env.local` を使用し、機密情報はgit管理しない。

必須（preflight）:
- `EBAY_CLIENT_ID`
- `EBAY_CLIENT_SECRET`
- `YAHOO_APP_ID`（または `YAHOO_CLIENT_ID`）
- `RAKUTEN_APPLICATION_ID`

推奨ランタイム:
- `TARGET_MARKETPLACE=EBAY_US`
- `TARGET_CATEGORY=watch_new`
- `ITEM_CONDITION=new`
- `DEFAULT_QUERY=seiko watch`
- `EBAY_RATE_LIMIT_RPS=2`
- `YAHOO_RATE_LIMIT_RPS=1`
- `RAKUTEN_RATE_LIMIT_RPS=1`

## 2. Preflight
ネットワークなし:
```bash
python3 scripts/preflight.py
```

オンライン最小確認（各サイト1リクエスト）:
```bash
python3 scripts/preflight.py --online --query "seiko sbga211"
```

FX必須で確認:
```bash
python3 scripts/preflight.py --online --require-fx --query "seiko sbga211"
```

## 3. APIサーバ起動
```bash
python3 scripts/run_api.py --host 127.0.0.1 --port 8000
```

ヘルスチェック:
```bash
curl -sS http://127.0.0.1:8000/healthz
```

代表エンドポイント:
- `GET /v1/system/fx-rate`
- `POST /v1/system/fx-rate/refresh`
- `POST /v1/profit/calc`
- `POST /v1/miner/fetch`
- `GET /v1/miner/queue`
- `GET /v1/miner/cycle/active`

## 4. Fetchと流動性判定
推奨キュー確認（利益・精度優先）:
```bash
curl -sS "http://127.0.0.1:8000/v1/miner/queue?status=pending&limit=50&min_profit_usd=0.01&min_margin_rate=0.03&min_match_score=0.75&condition=new"
```

流動性Gateの主要設定:
- `LIQUIDITY_GATE_ENABLED=1`
- `LIQUIDITY_REQUIRE_SIGNAL=1`
- `LIQUIDITY_MIN_SOLD_90D`
- `LIQUIDITY_MIN_SELL_THROUGH_90D`
- `LIQUIDITY_PROVIDER_MODE=rpa_json` または `ebay_marketplace_insights`

`sold_90d_count` の意味:
- `>=0`: 取得成功（`0` は90日売却なし）
- `-1`: 未取得/判定不能

## 5. サイクル実行コマンド
24件バッチ（精度優先）:
```bash
python3 scripts/run_miner_cycle.py --target-count 24 --hard-cap 30 --min-profit-usd 0.01 --min-margin-rate 0.03 --min-match-score 0.75 --require-full-batch
```

API制限を意識した検証モード:
```bash
python3 scripts/run_miner_cycle.py \
  --target-count 24 \
  --hard-cap 30 \
  --max-zero-gain-strikes 2 \
  --daily-budget-ebay 120 \
  --daily-budget-rakuten 120 \
  --daily-budget-yahoo 120
```

クローズレポート:
```bash
python3 scripts/close_miner_cycle.py --reject-floor 10 --min-reviewed-ratio 1.0 --min-reject-rate 0.10
```

## 6. Query Widthの運用基準
推奨waterfall:
1. eBay: `L1_precise_new` 開始、不足時のみ `L2_precise`
2. Yahoo: `L2_precise` 開始、不足時 `L3_mid`
3. Rakuten: `L2_precise` 開始、不足時 `L3_mid`

補助レポート:
- `docs/query_width_report*.json`
- `docs/query_width_summary.json`
- `docs/query_width_strategy.md`

## 7. FX + Profit運用
為替参照順序:
1. プロセスキャッシュ
2. DB `fx_rate_states`
3. `FX_USD_JPY`（env fallback）

利益試算:
```bash
python3 scripts/profit_demo.py --refresh-fx --sale-usd 420 --purchase-jpy 42000 --domestic-shipping-jpy 1200 --international-shipping-usd 28 --customs-usd 8 --packaging-usd 3
```

## 8. 事故防止
- `docs/miner_cycle_*_latest.json` は手編集しない
- `data/miner_*` / `data/liquidity_*` は手編集しない
- 設定変更時は `docs/WORKBOARD.md` の Decision Log を更新

## 9. 統合前の原本（退避先）
- `docs/archive/miner_legacy/API_LOCAL.md`
- `docs/archive/miner_legacy/PREVALIDATION.md`
- `docs/archive/miner_legacy/FX_PROFIT_FLOW.md`

## 10. Product Research RPA（Phase A運用）
腕時計カテゴリの seed 補充で使う `scripts/rpa_market_research.py` の推奨例。

初回ページ（offset=0）:
```bash
python3 scripts/rpa_market_research.py \
  --query "G-SHOCK" \
  --condition new \
  --sold-sort recently_sold \
  --fixed-price-only \
  --lookback-days 90 \
  --min-price-usd 100 \
  --result-limit 50 \
  --result-offset 0 \
  --output data/liquidity_rpa_signals.jsonl \
  --headless
```

2ページ目（offset=50）:
```bash
python3 scripts/rpa_market_research.py \
  --query "G-SHOCK" \
  --condition new \
  --sold-sort recently_sold \
  --fixed-price-only \
  --lookback-days 90 \
  --min-price-usd 100 \
  --result-limit 50 \
  --result-offset 50 \
  --output data/liquidity_rpa_signals.jsonl \
  --headless
```

実装前提（2026-02-25時点）:
- `--pause-for-login` の既定値は `0`（本番での待機時間なし）。
- `conditionId` / `minPrice` / `offset` / `sorting=datelastsold` は URL先入れを試行し、未反映分はUI操作で補完する。
- `fixed_price` は URLのみだと不安定なため、UIで選択状態を確認する。
- `recently_sold` は `Date last sold` ヘッダー操作で確定する（`metadata.filter_state.sort_selection_source` に記録）。
- `result_offset` はフィルタ適用後に戻る場合があるため、コード側で再適用・再確認する。
- `Lock selected filters` は有効化を試行する（状態は `metadata.filter_state` に保存）。

検証時のみ使うオプション:
- `--screenshot-after-filters` / `--html-after-filters` を付ける。
- 本番運用では不要なため付けない（速度優先）。

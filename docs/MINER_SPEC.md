# Miner Specification (Unified)

最終更新: 2026-02-26

この文書は Miner の正本仕様です。A/B/C 各段階、UI設定、候補化条件、受け入れ条件をこの1本に集約します。

## 1. 目的
- カテゴリ起点で seed を補充し、日本側最安候補と eBay 実売データを突合して、レビュー候補を自動生成する。
- 同一商品性・流動性・利益を満たす候補のみを `pending` に投入する。

## 2. スコープ
- 売り先: US eBay
- 仕入れ先: Rakuten / Yahoo
- 基本条件: 新品、在庫あり（UIで変更可）

## 3. 処理の全体像
1. A段階で `seed A` を補充（Product Research）
2. B段階で `seed A` を使って日本側最安候補を抽出（`seed B` 行を生成）
3. C段階で `seed B` を eBay 90日データで再判定し、レビュー候補を作成

## 4. A/B/C仕様
### 4.1 A段階（seed補充）
- 目的: B/Cで使う seed を増やす
- 入力: big word（カテゴリ知識で事前管理）
- Product Research 条件:
  - Sold / Last 90 days
  - Condition = New
  - Format = Fixed price
  - min price（カテゴリ別）
  - Date last sold = 新しい順（URL先入れ + UI確定）
- 取得: 1ページ50件、ページ送り対応
- 保存最小項目: `title`, `sold_price`, `item_url`, `item_id`
- 重複排除: `category_key + seed_key` で一意化
- ページ解放:
  - big wordごとに `hours/page` を使って許可ページ数を算出
  - 主カテゴリが `page_unlock_wait` のときは他カテゴリにフォールバックして目標補充数を達成

### 4.2 B段階（日本側一次探索）
- 目的: `seed A` から日本側候補を抽出して `seed B` を作る
- 実行順:
  - `seed A` は古い順で処理
  - 日本側候補は `source_total_jpy = price + shipping` の昇順で評価
- 既定:
  - `stage_b_query_mode = seed_only`
  - `stage_b_max_queries_per_site = 1`
  - `stage_b_top_matches_per_seed = 3`
  - `stage_b_api_max_calls_per_run = 0`（自動）
- 複数SKU対応:
  - 型番解決できない候補は除外
  - 一部条件でのみ listing price fallback を許可
- 出力:
  - `stage_b.rows` に候補行を保存（`stage1_rank` 含む）
  - `seed A` は消費しない（B/Cで再利用可能）

### 4.3 C段階（eBay最終再判定）
- 目的: 90日売却実績で最終利益判定を更新
- 入力: `seed B` 行（+ `seed A` 情報）
- 判定データ:
  - sold 90d 件数
  - sold 90d 最低価格（`sold_price_min_90d`）
  - active 件数
  - active 最低価格
  - sold 最低価格の参照URL/サンプル
- 既定:
  - `stage_c_min_sold_90d = 10`
  - `stage_c_liquidity_refresh_on_miss_enabled = true`
  - `stage_c_liquidity_refresh_on_miss_budget = 12`
  - `stage_c_retry_missing_active_enabled = false`（PR節約のため既定OFF）
  - `stage_c_allow_missing_sold_sample = false`
  - `stage_c_ebay_item_detail_enabled = true`
  - `stage_c_ebay_item_detail_max_fetch_per_run = 30`
- 再取得戦略（RPA JSON）:
  - `sold_90d_count` 欠損時: `on_miss_retry`
  - soldサンプル欠損時: `on_missing_sample_retry`
  - active 欠損時: `on_missing_active_retry`
  - PR上限到達時は即停止
- 重複排除:
  - C段階実行重複: `site + item_id + item_url + source_total_jpy`
  - 候補重複: `source side + sold side` の署名で抑止

## 5. 候補化ルール（pending投入条件）
以下をすべて満たす候補のみ投入する。
1. 同一商品判定を通過
2. 新品条件を通過
3. 日本側在庫条件を通過
4. 流動性シグナル取得済み（`sold_90d_count >= 0`）
5. `sold_90d_count >= stage_c_min_sold_90d`
6. 期待利益 `>= min_profit_usd`
7. 期待粗利率 `>= min_margin_rate`
8. `source_total_usd < sold_price_min_90d`

## 6. データ契約（要点）
### 6.1 seed A（DB: `miner_seed_pool`）
- `seed_query`, `seed_key`, `source_title`, `source_item_url`, `source_rank`, `metadata_json`

### 6.2 seed B（レスポンス: `stage_b.rows`）
- `seed_id`, `stage1_rank`, `stage1_query`
- `source_site`, `source_item_id`, `source_item_url`, `source_title`
- `source_price_jpy`, `source_shipping_jpy`, `source_total_jpy`
- `source_price_basis_type`, `stage1_match_score`, `stage1_match_reason`

### 6.3 C段階候補（`miner_candidates.metadata_json`）
- eBay: `ebay_sold_*`, `ebay_active_*`, `market_item_url`, `market_item_url_active`
- 日本側: `source_*`, `source_variant_price_resolution`
- 判定根拠: `liquidity_query`, `liquidity`, `seed_pool`, `seed_jp`, `calc_*`

## 7. Miner UI設定（永続化）
- エンドポイント:
  - `GET /v1/miner/settings`
  - `POST /v1/miner/settings`
- 保存先: `miner_ui_settings` テーブル（`settings_key = miner_fetch_settings_v1`）
- リセット:
  - UIの「詳細設定 > リセット」でデフォルトに戻し、DBへ保存
- 保存対象:
  - A段階: 在庫条件、件数、big word制限、遷移最小化
  - B段階: query mode、query上限、top matches、API上限
  - C段階: sold基準、再取得ON/OFF、再取得予算、sample欠損許容、詳細取得ON/OFF、詳細取得上限
  - 共通: `min_match_score`, `min_profit_usd`, `min_margin_rate`

## 8. 受け入れ条件（DoD）
### 8.1 Phase A
1. 条件設定再現（Sold/90days/New/Fixed/minPrice/新しい順）
2. 50件取得できる
3. ページ送りで内容が入れ替わる
4. seed最小項目を保存できる
5. 重複投入を抑止できる

### 8.2 Phase B
1. `source_total_jpy` 昇順で上位候補が取れる
2. `stage_b.rows` に `stage1_rank` と価格情報が入る
3. 複数SKU誤取得を抑止できる

### 8.3 Phase C
1. sold 90d 最低価格で利益再計算できる
2. sold件数 / active件数 / active最低価格が取得できる
3. sold/active欠損時に予算内で再取得できる
4. PR上限時に停止理由を明示して安全停止する

## 9. 運用ガード
- 生成物・状態ファイルは手編集しない
- PR上限・API制限を前提に低リスク実行を優先する
- DOM変更等で取得不能になった場合は無限再試行せず停止してUIへ理由を出す

## 10. 更新ルール
要件変更時は最低限、以下を同時更新する。
1. `docs/MINER_SPEC.md`
2. `docs/MINER_RUNBOOK.md`
3. `web/miner.html`, `web/miner.js`, `web/miner.css`（表示変更がある場合）
4. `docs/STATUS_CURRENT.md`
5. `docs/WORKBOARD.md`

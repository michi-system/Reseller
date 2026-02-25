# Miner Specification (Unified)

最終更新: 2026-02-25

この文書は、Minerの要件・探索フロー・完了条件（DoD）を1本に統合した正本です。

## 1. 目的
- 一般カテゴリ名だけで探索を開始し、レビュー候補まで自動生成する。
- US eBayの90日売却実績と日本側の最安在庫を突合し、利益見込みのある同一新品商品のみを候補化する。

## 2. スコープ
- 売り先: US eBay
- 仕入れ先: Yahooショッピング / 楽天
- 既定条件: 新品のみ、在庫あり必須（必要時のみ解除）

## 3. ユーザ操作要件
1. 一般カテゴリ名を入力する。
2. 開始ボタンを押す。
3. システムが自動で以下を実行する。
- カテゴリ展開（メーカー/シリーズ/型番）
- A/B/C探索
- 同一商品判定
- 90日流動性判定
- 利益算出
- レビュー待ちキュー投入

## 4. A/B/C探索フロー
### A段階（補充）
- 目的: カテゴリseedの補充
- 条件: `active_seed_count` が補充閾値以下で起動
- 実行: 事前準備済みbig wordを使い、Product Research の 90 days sold / New / Fixed Price でseed抽出
- big word作成/更新は運用前工程（Codex + 調査）で行い、A段階は読み込みのみ行う
- 既定ガード:
  - `MINER_SEED_POOL_REFILL_THRESHOLD=0`
  - `MINER_SEED_POOL_REFILL_TIMEBOX_SEC=300`
  - `MINER_SEED_POOL_MAX_TIMEOUT_PAGES_PER_RUN=2`
  - ビッグワード別ページ解放:
    - `MINER_STAGEA_QUERY_PAGE_UNLOCK_ENABLED=1`
    - `MINER_STAGEA_QUERY_PAGE_UNLOCK_HOURS_DEFAULT=24`
    - `MINER_STAGEA_QUERY_PAGE_UNLOCK_MIN_PAGES=1`
    - `MINER_STAGEA_QUERY_PAGE_UNLOCK_INITIAL_PAGES`（履歴未作成時の初期ページ数）
    - `MINER_STAGEA_QUERY_PAGE_UNLOCK_HOURS_JSON`（ビッグワード別 `hours/page` 上書き）
  - `page_unlock_wait` 時のカテゴリフォールバック:
    - `MINER_STAGEA_FALLBACK_ON_PAGE_UNLOCK_WAIT=1`
    - `MINER_STAGEA_FALLBACK_MAX_CATEGORIES=8`
    - 主カテゴリが待機中の場合、他カテゴリを順次補充して合計新規seedが目標件数に達したらA段階を終了

### B段階（日本側一次取得）
- 目的: 古いseedから20件を順に消化し、日本側最安本体候補を取得
- 実行: 新品・価格昇順・seed上限価格付きでYahoo/Rakuten検索
- 既定ガード:
  - `MINER_STAGE1_API_MAX_CALLS_PER_RUN`（1実行あたりの日本側API呼び出し上限）
  - `MINER_STAGE1_MULTI_SKU_STRICT=true`（複数型番ページで対象型番価格を解決できない候補を除外）
- 出力:
  - B段階の取得結果は `stage_b.rows` として保持し、`seed A` を消費せずに `seed B` 情報としてC段階へ引き継ぐ
- 補足: ヒットなしでも理由ログを必ず残す

### C段階（最終再判定）
- 目的: 日本側価格とeBay最新90日実績の再突合
- 実行: 日本産seedでeBay 90days soldを再取得
- 既定ガード:
  - `MINER_STAGE2_ALLOW_MISSING_SOLD_SAMPLE=false`
  - sold根拠URL欠損候補はpending化しない

## 5. 候補化ルール（レビュー待ち投入条件）
以下をすべて満たす候補のみレビュー待ちへ投入する。
1. 同一商品判定を通過（識別子/型番/ブランド/バリアント）
2. 新品条件を通過
3. 日本側在庫あり（既定）
4. 90日流動性シグナル取得済み（`sold_count_90d != -1`）
5. 90日売却件数が基準以上
6. 期待利益が基準以上
7. 期待利益率が基準以上

期待利益には以下を含める。
- 仕入れ価格
- 国内送料
- 国際送料
- eBay/決済手数料
- 為替レート
- 安全マージン

## 6. レビュー表示要件
- 左: eBay（90日売却実績、最低成約価格）
- 右: 日本側（現在の最安在庫）
- 必須表示項目:
  - `expected_profit_usd`
  - `expected_profit_jpy`
  - `sold_count_90d`
  - `sold_price_min_90d`
  - `source_price_jpy`
  - `fx_rate_used`
  - 商品リンク
  - 商品画像

## 7. DoD（完了条件）
### 7.1 1サイクル完了条件
- `reviewed_count=24`
- `unresolved_count=0`
- `miner_cycle_report` / `auto_miner_report` / `close_report` / `validation_report` が揃う

### 7.2 運用ガード
- `.env.local` は `ITEM_CONDITION=new`
- `LIQUIDITY_REQUIRE_SIGNAL=1`
- `AUTO_MINER_REQUIRE_LIQUIDITY_SIGNAL=1`
- `LIQUIDITY_PROVIDER_MODE` は `rpa_json` または `ebay_marketplace_insights`
- 閾値の正本は `docs/OPERATION_POLICY.json`

### 7.3 逸脱時
- サイクル開始前に停止
- `docs/miner_cycle_validation_latest.json` で原因を確認してから再開

## 8. 非機能要件
- API節約:
  - レート制限順守
  - 重複取得抑制
  - 低収率クエリ停止
- 鮮度:
  - 商品情報は短TTL
  - カテゴリ傾向は中長期TTL
- 監査可能性:
  - 候補ごとに採用根拠を保存
  - 否認理由を改善サイクルへ還元

## 9. 変更ルール
要件・完了条件・閾値を変更する場合は次を同時更新する。
1. `docs/MINER_SPEC.md`
2. `docs/OPERATION_POLICY.json`
3. `web/miner.*`（表示項目・文言に影響がある場合）
4. `docs/WORKBOARD.md`（Decision Log）
5. `docs/STATUS_CURRENT.md`（必要時）

## 10. 統合前の原本（退避先）
- `docs/archive/miner_legacy/REQUIREMENTS.md`
- `docs/archive/miner_legacy/DEFINITION_OF_DONE.md`
- `docs/archive/miner_legacy/miner_explore_to_review_flow.md`

## 11. Phase A受け入れ条件（腕時計プロファイル）
腕時計カテゴリでは、次を満たしたとき Phase A を完了とする。
1. 条件設定が再現できること（`Sold` / `Last 90 days` / `Condition=New` / `Format=Fixed price` / `minPrice` / `Date last sold=新しい順`）。
2. 1ページ50件のタイトルと価格を取得できること（`result_limit=50`）。
3. ページ送りで一覧が切り替わること（`result_offset=0` と `result_offset=50` の重複率が低いこと）。
4. seed抽出に必要な最小情報を保存できること（`title`, `sold_price`, `item_url`, `item_id`）。
5. 重複処理が効くこと（同一seedの再投入時に追加0件になること）。

運用補足:
- `format=fixed_price` は URL だけでは不安定なため、UIでの確定操作を前提にする。
- `conditionId` / `minPrice` / `offset` / `sorting=datelastsold` は URL先入れを試行し、反映不足はUIで補正する。
- 新しい順運用では、前回取得時刻からの経過時間に応じてビッグワードごとの許可ページ数を計算する（短時間の再探索で深いページを掘りすぎない）。

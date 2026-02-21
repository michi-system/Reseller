# Definition Of Done (MVP運用)

## 1. 1サイクル完了条件
- 24件レビュー完了（`reviewed_count=24`）
- 未解決0件（`unresolved_count=0`）
- `review_cycle_report` / `auto_review_report` / `close_report` / `validation_report` が揃っている

## 2. 候補採用条件
- 同一商品の根拠がある（型番/識別子/スコア）
- 新品かつ在庫あり
- 90日売却シグナルが取得済み（`sold_90d_count != -1`）
- 期待利益が正（`expected_profit_usd > 0`）

## 3. 運用ガード
- `.env.local` が `ITEM_CONDITION=new` を維持
- `LIQUIDITY_REQUIRE_SIGNAL=1`
- `AUTO_REVIEW_REQUIRE_LIQUIDITY_SIGNAL=1`
- `LIQUIDITY_PROVIDER_MODE` は `rpa_json` か `ebay_marketplace_insights`
- 閾値は `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/OPERATION_POLICY.json` より緩くしない

## 4. 逸脱時
- サイクル開始前に停止
- `review_cycle_validation_latest.json` を確認して原因を修正してから再開

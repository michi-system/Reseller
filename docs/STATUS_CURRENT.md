# Project Whole Picture (2026-02-20)

## 1. 目的（不変）
- 日本仕入れ（Rakuten / Yahoo）と US eBay を突合し、同一新品商品のみを高精度抽出
- 利益・流動性（90日売却）を満たす候補だけをレビュー対象にする
- 最終承認は人間、否認理由はDB蓄積して次サイクル改善に還元
- API使用量とレビュー時間を同時最適化する

## 2. 現在の全体フロー
1. `scripts/run_review_cycle.py`
- サイト別最適化探索（重複抑制、履歴学習スキップ、同一モデル上限）
- 新品/中古・在庫・一致スコア・利益・EV90・流動性ゲートを通過した候補のみ作成
- `liquidity_unavailable_required` をモデルコード単位で収集し、backfillターゲット化

2. `scripts/auto_review_cycle.py`
- 自動承認は厳格条件のみ
- 流動性未取得(-1)・fallback弱条件・色情報欠損マッチを自動承認しない
- 自動承認後も `approved` で止め、人間の最終承認待ち

3. `web/review.html` + `web/review.js`
- 左eBay/右日本カード + 為替反映価格比較 + 算出根拠
- 否認は「指摘箇所のみ」でも送信可（理由テキスト任意）
- 色欠損マッチを「色要確認」として可視化

4. `scripts/close_review_cycle.py` / `scripts/apply_cycle_improvements.py`
- サイクル締めで否認統計を集計
- issue targetベースで改善を適用（空振り防止ガードあり）

5. `scripts/run_autonomous_cycle.py`
- policy guard + validationで「2周目以降に実は失敗」を防止

## 3. 現在地（最新スナップショット）
- 最新サイクル: `cycle-20260220-031359`
- `cycle_ready=false`, `batch_size=0`（検証用 cache-only 実行）
- API効率: cache hit 100%（network call 0）
- DBステータス: `listed=364`, `rejected=110`, `approved=1`, `pending(レビュー閾値通過)=0`
- 否認上位: `price`, `color`, `condition`, `accessories`

## 4. 実装済みの重要改善
- 90日流動性の reason-based model-code backfill
- 重複過多クエリの同一サイクル内クールダウン
- 曖昧型番タイトル除外
- 色欠損でも強一致時は候補化（取りこぼし抑制）
- ただし色欠損は自動承認で必ずブロック（安全側）
- レビューUIに「色要確認」警告を表示

## 5. いま残っている本質課題
1. 候補枯渇
- 現在の探索集合では「新規でレビューに回せる pending」が薄い

2. 流動性データの欠損/偏り
- query依存で取得ムラがあり、fallback設計の継続調整が必要

3. 色・型番の境界ケース
- 「色欠損だが同一」ケースを増やすと誤一致リスクも上がるため、閾値運用が要管理

## 6. 次の進行順（推奨）
1. 候補補充フェーズ
- 未重複シリーズ中心にクエリ再編し、24件バッチの再確保を優先

2. 色要確認ワークフロー
- UIに「色要確認のみ表示」フィルタを追加
- 人間レビューでの最終判定速度を上げる

3. 改善サイクル再開
- 24件単位で human/auto を回し、否認統計で閾値更新

4. 出品自動化（ダミー解除）前の最終ゲート
- DoDに「90日売却条件・最低利益・一致根拠」の固定化チェックを追加

## 7. 参照ファイル
- `scripts/run_review_cycle.py`
- `scripts/auto_review_cycle.py`
- `scripts/run_autonomous_cycle.py`
- `reselling/live_review_fetch.py`
- `reselling/liquidity.py`
- `web/review.js`
- `docs/OPERATION_POLICY.json`
- `docs/DEFINITION_OF_DONE.md`
- `docs/REQUIREMENTS.md`
- `docs/KNOWLEDGE_LIBRARY.md`
- `data/category_knowledge_seeds_v1.json`

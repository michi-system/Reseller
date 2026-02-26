# STATUS CURRENT

最終更新: 2026-02-26 (JST)  
このファイルは「現時点の運用スナップショット」専用です。  
計画/工数/実装タスクは `docs/WORKBOARD.md` に集約します。

## 1. 目的（固定）
- 日本仕入れ（Rakuten / Yahoo）と US eBay を突合し、同一新品商品のみ候補化する。
- 90日売却情報と利益条件を満たした案件だけをレビュー待ちへ投入する。
- 人間レビュー結果を学習材料として次サイクルに還元する。
- API消費とレビュー工数のバランスを最適化する。

## 2. 現在の処理フロー（運用実体）
1. `scripts/run_miner_cycle.py`
2. `scripts/auto_miner_cycle.py`
3. `scripts/close_miner_cycle.py`
4. `scripts/apply_cycle_improvements.py`
5. `scripts/run_autonomous_cycle.py` / `scripts/run_autonomous_cycles_guarded.py`
6. UI: `web/miner.html` + `web/miner.js`

## 3. 現在地スナップショット
- Phase A: 腕時計カテゴリで条件設定・50件取得・ページ送り・重複抑止まで実装済み
- Phase B: `seed_only` 既定、`source_total_jpy` 昇順、`stage1_rank` 付きで `stage_b.rows` を生成
- Phase C: sold/activeの再判定、欠損時再取得（miss/sample/active）、eBay詳細補完を実装済み
- Miner UI:
  - ヘッダー進捗に A/B/C 段階と pool状態を表示
  - seed pool要約（補充理由・cooldown・正規化/重複整理）を表示
  - 詳細設定をDB永続化し、リセットボタンで既定値へ復帰可能
- 主要既定:
  - `stage_b_query_mode=seed_only`
  - `stage_b_top_matches_per_seed=3`
  - `stage_c_min_sold_90d=10`
  - `stage_c_liquidity_refresh_on_miss_budget=12`
  - `stage_c_ebay_item_detail_max_fetch_per_run=30`

注記:
- 上記数値は実行により変動するため、最新値は `docs/miner_cycle_report_latest.json` と DB を正とする。

## 4. 稼働中の主要ガード
- 新品固定 (`ITEM_CONDITION=new`)
- 流動性シグナル必須 (`LIQUIDITY_REQUIRE_SIGNAL=1`)
- 自動レビューで流動性欠損/色欠損を保守的にブロック
- Query重複スキップ/クールダウン/履歴ベース再試行制御
- Product Research Phase Aは URL先入れ + UI最小操作 + offset再適用でPR消費を抑制
- `recently_sold` は `sorting=datelastsold` 先入れ + `Date last sold` ヘッダー操作で最終確定（`metadata.filter_state.sort_selection_source` で検証）

## 5. 既知課題（運用観点）
1. カテゴリ/ビッグワードにより seed 補充速度が偏る（hours/page の継続最適化が必要）
2. マルチSKU商品ページの型番解決失敗は引き続き発生しうる
3. eBay側DOM変更時に RPA抽出が不安定化する可能性がある
4. 候補流入量はカテゴリごとの差が大きく、運用側で最小件数保証の監視が必要

## 6. 参照先
- 作業計画: `docs/WORKBOARD.md`
- 記録先ルール: `docs/DOCS_GOVERNANCE.md`
- 生成物の所在: `docs/RECORDS_REGISTRY.md`
- 日次レポート: `docs/daily_reports/`

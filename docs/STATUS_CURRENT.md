# STATUS CURRENT

最終更新: 2026-02-22 (JST)  
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
- 最新サイクルID: `cycle-20260220-031359`
- 直近確認値: `cycle_ready=false`, `batch_size=0`
- API効率（直近検証）: cache hit 100%, network call 0
- DBステータス（直近確認値）: `listed=364`, `rejected=110`, `approved=1`, `pending=0`
- 否認上位傾向: `price`, `color`, `condition`, `accessories`

注記:
- 上記数値は実行により変動するため、最新値は `docs/miner_cycle_report_latest.json` と DB を正とする。

## 4. 稼働中の主要ガード
- 新品固定 (`ITEM_CONDITION=new`)
- 流動性シグナル必須 (`LIQUIDITY_REQUIRE_SIGNAL=1`)
- 自動レビューで流動性欠損/色欠損を保守的にブロック
- Query重複スキップ/クールダウン/履歴ベース再試行制御

## 5. 既知課題（運用観点）
1. レビュー候補の流入不足（カテゴリ・型番によっては枯渇）
2. 流動性データ欠損時の歩留まり低下
3. 型番/色/付属品境界の誤判定リスク
4. UI可読性・操作速度の継続改善余地

## 6. 参照先
- 作業計画: `docs/WORKBOARD.md`
- 記録先ルール: `docs/DOCS_GOVERNANCE.md`
- 生成物の所在: `docs/RECORDS_REGISTRY.md`
- 日次レポート: `docs/daily_reports/`

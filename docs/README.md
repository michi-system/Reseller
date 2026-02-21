# Docs Guide

この `docs/` は「常用ドキュメント」「生成ログ」「アーカイブ」に分離しています。

## 1. まず読む（常用）
- `docs/REQUIREMENTS.md`
  - 最終要件（カテゴリ入力 -> 自動探索 -> 90日売却基準 -> レビュー投入）
- `docs/DEFINITION_OF_DONE.md`
  - 1サイクルの完了条件
- `docs/OPERATION_POLICY.json`
  - 閾値と運用ガードの正本
- `docs/STATUS_CURRENT.md`
  - 現在地と残課題
- `docs/KNOWLEDGE_LIBRARY.md`
  - カテゴリ -> メーカー/シリーズ/型番の知識設計
- `docs/API_LOCAL.md`
  - ローカルAPIと運用コマンド
- `docs/PREVALIDATION.md`
  - 実行前チェック
- `docs/FX_PROFIT_FLOW.md`
  - 為替と利益計算フロー
- `docs/RESEARCH_NOTES.md`
  - 追加調査ノート（公式+コミュニティ）

## 2. 実データ（機械可読）
- `data/category_knowledge_seeds_v1.json`
  - カテゴリ展開用のナレッジ辞書（v1）

## 3. 自動生成ログ（読み物ではない）
- `docs/autonomous_runs/`
- `docs/autonomous_runs_v2/`
- `docs/autonomous_guarded_runs/`
- `docs/cycle_diagnostics/`
- `docs/review_cycle_*_latest.json`
- `docs/query_width_*.json`
- `docs/min_new_autotune_*.json`
- `docs/fetch_autotune_report_latest.json`

## 4. アーカイブ（旧レポート）
- `docs/archive/manual/PROJECT_SUPREME_REPORT_2026-02-19.md`
- `docs/archive/manual/CYCLE_BOTTLENECK_REPORT_2026-02-20.md`

## 5. 更新ルール
1. 要件変更時は `REQUIREMENTS.md` -> `DEFINITION_OF_DONE.md` -> `OPERATION_POLICY.json` の順で同期。
2. 調査メモは `RESEARCH_NOTES.md` に追記し、確定知識は `KNOWLEDGE_LIBRARY.md` と `data/category_knowledge_seeds_v1.json` に反映。
3. 生成ログは編集せず、必要なら新規実行で再生成。

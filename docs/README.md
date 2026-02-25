# Docs Index

このファイルは `docs/` の総合索引です。  
最初の入口は必ず `docs/START_HERE.md` に固定します。

## 1. 最初に読む
1. `docs/START_HERE.md`
2. `docs/MINER_SPEC.md`
3. `docs/PHASE_A_GUIDE.md`
4. `docs/OPERATION_POLICY.json`
5. `docs/MINER_RUNBOOK.md`
6. `docs/STATUS_CURRENT.md`
7. `docs/WORKBOARD.md`
8. `docs/PROGRAM_OVERVIEW.md`
9. `docs/OPERATIONS_MANUAL.md`
10. `docs/GITHUB_PROJECT_SETUP.md`
11. `docs/INTERNAL_NAME_MIGRATION.md`
12. `docs/daily_reports/TEMPLATE.md`
13. `docs/OPERATOR_OPERATIONS_SPEC.md`
14. `docs/OPERATOR_EFFORT_TABLE.md`
15. `docs/SUPABASE_BIG_BANG_MIGRATION.md`
16. `docs/CODEX_COLLAB_ONBOARDING.md`

## 2. 正本（Single Source of Truth）
| 種別 | 正本 | 用途 |
|---|---|---|
| Miner要件/DoD | `docs/MINER_SPEC.md` | Minerが満たすべき条件と完了判定 |
| 閾値/運用ガード | `docs/OPERATION_POLICY.json` | しきい値と運用制約 |
| Miner実行手順 | `docs/MINER_RUNBOOK.md` | 事前チェック・API・サイクル運用手順 |
| 現在地 | `docs/STATUS_CURRENT.md` | 最新スナップショット |
| 計画/工数/進行 | `docs/WORKBOARD.md` | タスク・工数・決定ログ |
| 全体設計 | `docs/PROGRAM_OVERVIEW.md` | Reseller (Miner + Operator) の境界とロードマップ |
| 運用手順 | `docs/OPERATIONS_MANUAL.md` | Codex/GitHubの進め方 |
| GitHub初期設定 | `docs/GITHUB_PROJECT_SETUP.md` | 画面操作ベースのセットアップ |
| 日報テンプレ | `docs/daily_reports/TEMPLATE.md` | 日次レポートの記録フォーマット |
| Operator運用仕様 | `docs/OPERATOR_OPERATIONS_SPEC.md` | 出品/監視/停止判定の運用定義 |
| Operator工数表 | `docs/OPERATOR_EFFORT_TABLE.md` | 0->1開発の工数見積と分解 |
| DB戦略 | `docs/LOCAL_DB_STRATEGY.md` | ローカルDB継続と移行条件 |
| Supabase一括移行 | `docs/SUPABASE_BIG_BANG_MIGRATION.md` | 一括移行とローカル即時ロールバック手順 |
| 共同開発参加手順 | `docs/CODEX_COLLAB_ONBOARDING.md` | 別Mac/別アカウントCodexの参加手順 |
| internal名移行 | `docs/INTERNAL_NAME_MIGRATION.md` | 新旧internal名の段階移行計画 |
| データ契約 | `docs/DATA_CONTRACT_APPROVED_LISTING.md` | Tool間の受け渡し仕様 |
| 記録先ルール | `docs/DOCS_GOVERNANCE.md` | 散乱防止ルール |
| 生成物索引 | `docs/RECORDS_REGISTRY.md` | 生成物の参照先と意味 |
| 引き継ぎマニフェスト | `docs/HANDOVER_MANIFEST.json` | 機械可読の引き継ぎ定義 |

## 3. 参照ドキュメント（補助）
- `docs/MINER_RUNBOOK.md`: ローカル実行コマンドと運用手順
- `docs/PHASE_A_GUIDE.md`: Phase A（seed補充）の平易な運用解説
- `docs/KNOWLEDGE_LIBRARY.md`: カテゴリ展開ナレッジ
- `docs/RESEARCH_NOTES.md`: 調査メモ（未確定情報）
- `docs/query_width_strategy.md`: API探索幅の戦略サマリ
- `docs/RPA_SPEEDUP_NOTES_2026-02-21.md`: RPA高速化メモ
- `data/rpa_training/phasea_acceptance/`: Product Research Phase A受け入れ試験のレポート/スクショ
- `docs/LOCAL_DB_STRATEGY.md`: ローカルDB運用と移行計画
- `docs/SUPABASE_BIG_BANG_MIGRATION.md`: Supabase一括移行とロールバック手順
- `docs/CODEX_COLLAB_ONBOARDING.md`: 別Mac/別アカウントCodexの参加手順
- `docs/sql/reseller_supabase_schema.sql`: Supabase投入用の初期スキーマSQL
- `docs/DATA_CONTRACT_APPROVED_LISTING.md`: 承認済み商品の契約定義

## 4. 生成物・状態ファイル
生成物本体の一覧は `docs/RECORDS_REGISTRY.md` を参照。  
`docs/miner_cycle_*_latest.json` や `docs/autonomous_*` などは手編集禁止です。

## 5. アーカイブ
- `docs/archive/manual/PROJECT_SUPREME_REPORT_2026-02-19.md`
- `docs/archive/manual/CYCLE_BOTTLENECK_REPORT_2026-02-20.md`
- `docs/archive/miner_legacy/*.md`（統合前のMiner要件/手順原本）

## 6. 変更時の最小更新セット
要件や運用を変更した場合は、最低限次を同時更新します。
1. `docs/MINER_SPEC.md`
2. `docs/OPERATION_POLICY.json`
3. `docs/MINER_RUNBOOK.md`（実行手順に影響する場合）
4. `docs/WORKBOARD.md`
5. `docs/STATUS_CURRENT.md`

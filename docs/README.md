# Docs Index

このファイルは `docs/` の総合索引です。  
最初の入口は必ず `docs/START_HERE.md` に固定します。

## 1. 最初に読む
1. `docs/START_HERE.md`
2. `docs/REQUIREMENTS.md`
3. `docs/DEFINITION_OF_DONE.md`
4. `docs/OPERATION_POLICY.json`
5. `docs/STATUS_CURRENT.md`
6. `docs/WORKBOARD.md`
7. `docs/PROGRAM_OVERVIEW.md`
8. `docs/OPERATIONS_MANUAL.md`
9. `docs/GITHUB_PROJECT_SETUP.md`
10. `docs/INTERNAL_NAME_MIGRATION.md`

## 2. 正本（Single Source of Truth）
| 種別 | 正本 | 用途 |
|---|---|---|
| 要件 | `docs/REQUIREMENTS.md` | プロジェクトが満たすべき条件 |
| 完了定義 | `docs/DEFINITION_OF_DONE.md` | 1サイクル完了判定 |
| 閾値/運用ガード | `docs/OPERATION_POLICY.json` | しきい値と運用制約 |
| 現在地 | `docs/STATUS_CURRENT.md` | 最新スナップショット |
| 計画/工数/進行 | `docs/WORKBOARD.md` | タスク・工数・決定ログ |
| 全体設計 | `docs/PROGRAM_OVERVIEW.md` | Reseller (Miner + Operator) の境界とロードマップ |
| 運用手順 | `docs/OPERATIONS_MANUAL.md` | Codex/GitHubの進め方 |
| GitHub初期設定 | `docs/GITHUB_PROJECT_SETUP.md` | 画面操作ベースのセットアップ |
| DB戦略 | `docs/LOCAL_DB_STRATEGY.md` | ローカルDB継続と移行条件 |
| internal名移行 | `docs/INTERNAL_NAME_MIGRATION.md` | 新旧internal名の段階移行計画 |
| データ契約 | `docs/DATA_CONTRACT_APPROVED_LISTING.md` | Tool間の受け渡し仕様 |
| 記録先ルール | `docs/DOCS_GOVERNANCE.md` | 散乱防止ルール |
| 生成物索引 | `docs/RECORDS_REGISTRY.md` | 生成物の参照先と意味 |
| 引き継ぎマニフェスト | `docs/HANDOVER_MANIFEST.json` | 機械可読の引き継ぎ定義 |

## 3. 参照ドキュメント（補助）
- `docs/API_LOCAL.md`: ローカル API と実行コマンド
- `docs/PREVALIDATION.md`: 実行前チェック
- `docs/FX_PROFIT_FLOW.md`: 為替・利益計算フロー
- `docs/KNOWLEDGE_LIBRARY.md`: カテゴリ展開ナレッジ
- `docs/RESEARCH_NOTES.md`: 調査メモ（未確定情報）
- `docs/query_width_strategy.md`: API探索幅の戦略サマリ
- `docs/RPA_SPEEDUP_NOTES_2026-02-21.md`: RPA高速化メモ
- `docs/LOCAL_DB_STRATEGY.md`: ローカルDB運用と移行計画
- `docs/DATA_CONTRACT_APPROVED_LISTING.md`: 承認済み商品の契約定義

## 4. 生成物・状態ファイル
生成物本体の一覧は `docs/RECORDS_REGISTRY.md` を参照。  
`docs/review_cycle_*_latest.json` や `docs/autonomous_*` などは手編集禁止です。

## 5. アーカイブ
- `docs/archive/manual/PROJECT_SUPREME_REPORT_2026-02-19.md`
- `docs/archive/manual/CYCLE_BOTTLENECK_REPORT_2026-02-20.md`

## 6. 変更時の最小更新セット
要件や運用を変更した場合は、最低限次を同時更新します。
1. `docs/REQUIREMENTS.md`
2. `docs/DEFINITION_OF_DONE.md`
3. `docs/OPERATION_POLICY.json`
4. `docs/WORKBOARD.md`
5. `docs/STATUS_CURRENT.md`

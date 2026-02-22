# Start Here (Handover Entry)

このファイルを、引き継ぎ時の最初の入口に固定する。  
リポジトリ直下の `README.md` からもこのファイルへ到達できる。

## 1. 目的
- 日本仕入れ（Yahoo / Rakuten）と US eBay を突合し、同一新品商品のみを候補化する。
- 90日売却情報と利益条件を満たすものだけをレビュー対象にする。
- 人間レビュー結果（承認/否認）を次サイクル改善に還元する。

## 2. 最短オンボーディング（10分）
1. `docs/REQUIREMENTS.md` を読む（最終要件の正本）。
2. `docs/DEFINITION_OF_DONE.md` を読む（完了条件の正本）。
3. `docs/OPERATION_POLICY.json` を読む（閾値・運用ガードの正本）。
4. `docs/STATUS_CURRENT.md` を読む（最新スナップショット）。
5. `docs/WORKBOARD.md` を読む（工数・計画・実装タスクの正本）。
6. `docs/PROGRAM_OVERVIEW.md` を読む（Reseller全体像）。
7. `docs/OPERATIONS_MANUAL.md` を読む（Codex/GitHubの運用手順）。
8. `docs/GITHUB_PROJECT_SETUP.md` を読む（GitHub画面の初期設定手順）。
9. `docs/INTERNAL_NAME_MIGRATION.md` を読む（internal名の段階移行手順）。
10. `docs/daily_reports/TEMPLATE.md` を読む（日報フォーマット）。
11. `docs/OPERATOR_OPERATIONS_SPEC.md` を読む（Operator運用仕様）。
12. `docs/OPERATOR_EFFORT_TABLE.md` を読む（Operator工数見積）。
13. `docs/SUPABASE_BIG_BANG_MIGRATION.md` を読む（一括移行とローカル復帰手順）。
14. `docs/CODEX_COLLAB_ONBOARDING.md` を読む（別Mac/別アカウント参加手順）。

## 3. 正本の責務（Single Source of Truth）
| 種別 | 正本ファイル | 役割 |
|---|---|---|
| 要件 | `docs/REQUIREMENTS.md` | 何を満たすべきか |
| 完了定義 | `docs/DEFINITION_OF_DONE.md` | 何をもって完了とするか |
| 閾値/ガード | `docs/OPERATION_POLICY.json` | 実行時パラメータの基準 |
| 現在地 | `docs/STATUS_CURRENT.md` | 最新の実運用スナップショット |
| 計画/工数/実装管理 | `docs/WORKBOARD.md` | backlog, effort, next actions |
| 全体設計 | `docs/PROGRAM_OVERVIEW.md` | Reseller (Miner + Operator) の境界と計画 |
| 運用手順 | `docs/OPERATIONS_MANUAL.md` | GitHub/Codexの作業フロー |
| GitHub設定 | `docs/GITHUB_PROJECT_SETUP.md` | リポジトリ/Projectの初期設定手順 |
| 日報テンプレ | `docs/daily_reports/TEMPLATE.md` | 日次レポートの記録フォーマット |
| Operator運用仕様 | `docs/OPERATOR_OPERATIONS_SPEC.md` | 出品/監視/停止判定の運用定義 |
| Operator工数表 | `docs/OPERATOR_EFFORT_TABLE.md` | 0->1開発の工数見積と分解 |
| DB方針 | `docs/LOCAL_DB_STRATEGY.md` | ローカルDB継続と移行基準 |
| Supabase一括移行 | `docs/SUPABASE_BIG_BANG_MIGRATION.md` | 一括移行とローカル即時ロールバック手順 |
| 共同開発参加手順 | `docs/CODEX_COLLAB_ONBOARDING.md` | 別Mac/別アカウントCodexの参加手順 |
| internal名移行 | `docs/INTERNAL_NAME_MIGRATION.md` | 新旧internal名の段階移行計画 |
| データ契約 | `docs/DATA_CONTRACT_APPROVED_LISTING.md` | 承認済みデータ受け渡し定義 |
| ナレッジ | `docs/KNOWLEDGE_LIBRARY.md` + `data/category_knowledge_seeds_v1.json` | カテゴリ展開知識 |
| 記録先ルール | `docs/DOCS_GOVERNANCE.md` | どこに何を書くか |
| 生成物一覧 | `docs/RECORDS_REGISTRY.md` | 自動生成ログの参照先 |
| 機械可読引き継ぎ | `docs/HANDOVER_MANIFEST.json` | 新規エージェント向け定義 |

## 4. 実行コマンド入口
- API: `docs/API_LOCAL.md`
- 事前チェック: `docs/PREVALIDATION.md`
- 為替・利益式: `docs/FX_PROFIT_FLOW.md`
- Supabase初期スキーマ: `docs/sql/reseller_supabase_schema.sql`
- 生成物の所在確認: `docs/RECORDS_REGISTRY.md`

## 5. 引き継ぎ時の禁止事項
- 生成物（`docs/*_latest.json`, `docs/autonomous_*`, `docs/cycle_diagnostics`）を手編集しない。
- 実行状態（`data/review_*`, `data/liquidity_*`）を手編集しない。
- 正本を増殖させない（同じ責務の新規mdを作らない）。
- 要件変更時に `REQUIREMENTS -> DoD -> OPERATION_POLICY` の同期を省略しない。

## 6. 次エージェント向け作業開始手順
1. `docs/STATUS_CURRENT.md` と `docs/WORKBOARD.md` の `Now` セクションを確認。
2. `docs/OPERATION_POLICY.json` の閾値が作業意図と矛盾しないか確認。
3. 変更後は `docs/WORKBOARD.md` のタスク状態と実装ログを更新。
4. 必要なら `docs/STATUS_CURRENT.md` のスナップショットを更新。

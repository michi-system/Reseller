# WORKBOARD

このファイルは、計画・工数・実装進行・意思決定の正本です。  
タスク状態の更新先をこの1ファイルに固定します。

## 1. 運用ルール
- `Now` は常に最新化し、完了タスクは `Recently Completed` へ移す。
- 要件/閾値に関わる変更は `Decision Log` に理由を残す。
- `STATUS_CURRENT.md` は運用スナップショット専用。計画はここに書かない。

## 2. Now
| ID | 優先 | 種別 | 状態 | 内容 | 完了条件 | 目安工数 |
|---|---|---|---|---|---|---|
| WB-P0-001 | P0 | ロジック | in_progress | レビュー候補流入を安定化（sold-first起点 + 日本側照会順最適化） | 24件バッチを再現可能、0件停滞が連続しない | 1-2日 |
| WB-P0-002 | P0 | 品質 | in_progress | 90日最低価格の異常安値混入を抑制（本体/付属品判定強化） | 明らかな異常安値候補がレビュー待ちに残らない | 0.5-1日 |
| WB-P0-003 | P0 | UI/UX | in_progress | 探索進捗ゲージの一貫性改善（戻り/急完了表示の抑制） | 進捗表示が単調増加に近く、状態文言が段階一致 | 0.5日 |
| WB-P0-004 | P0 | 体制 | done | Reseller (Miner + Operator) のGitHub Project運用を日報中心へ切替（Today/Doing/Done/Reported） | 週次レビューで日報カードがProject管理される | 0.5日 |
| WB-P0-005 | P0 | 体制 | done | 承認済み商品のデータ契約実装（JSONL最小出力, Issue #3） | Operatorで取り込み可能な契約データが固定化 | 1-2日 |
| WB-P0-006 | P0 | 体制 | todo | internal名の段階移行（DB/識別子/ヘッダ互換） | `INTERNAL_NAME_MIGRATION` のPhase 1-2が完了 | 1-2日 |
| WB-P0-007 | P0 | 実装 | done | Operator 0->1骨格 + Miner JSONL取り込み | `apps/operator` の最小実行系と `latest.jsonl` 取り込みが通る | 2-3日 |
| WB-P0-008 | P0 | 実装 | done | Operator 自動出品ワーカー実装（dry-run優先） | dry-runでキュー消化と出品API呼び出し模擬が再現できる | 2-3日 |
| WB-P0-009 | P0 | 実装 | done | Operator 監視ジョブ + 利益低下時の自動停止 | 閾値割れ連続時に停止イベントが記録される | 2-4日 |
| WB-P0-010 | P0 | UI/運用 | done | Operator専用UI + 手動介入API（停止/要確認/継続/ready復帰） | `/operator` 画面で日次運用と手動操作が完結できる | 1-2日 |
| WB-P1-001 | P1 | 実装 | todo | `active_count` 取得実装（EFF-001） | 流動性にアクティブ件数が保存される | 3-5h |
| WB-P1-002 | P1 | 実装 | todo | STR算出実装（EFF-002） | `sell_through_90d` が実値計算される | 1-2h |
| WB-P1-003 | P1 | UI/判定 | todo | アクティブ/STRのUI連携（EFF-003） | UI表示と閾値判定が連動する | 1-2h |
| WB-P1-004 | P1 | 運用 | todo | 「取得ゼロ」時の自動診断出力（原因内訳） | ゼロ件時に停止理由が即時把握できる | 2-4h |
| WB-P1-005 | P1 | 移行 | done | legacy internal alias のサンセット実施（Issue #4） | 期限までに wrapper/互換ヘッダの撤去可否を確定 | 0.5-1日 |
| WB-P0-011 | P0 | 移行 | done | Supabase一括移行向けローカル復帰チェックポイント運用を実装 | 切替前チェックポイント作成と復帰コマンドが固定化される | 0.5日 |
| WB-P0-012 | P0 | 移行 | done | Supabase投入の初期スキーマSQLとSQLite CSV一括エクスポートを実装 | SQL適用とCSV出力手順が固定化される | 0.5日 |
| WB-P0-013 | P0 | 移行 | done | CSVバンドルをSupabaseへ一括投入する実行スクリプトを実装 | dry-run/本実行の再現可能な投入コマンドが固定化される | 0.5日 |
| WB-P0-014 | P0 | 移行 | done | API/OperatorのDB接続をSQLite/PostgreSQL両対応化し、Supabase運用モードを実装 | `.env.local` で `DB_BACKEND=postgres` の実運用起動が可能 | 1日 |

## 3. Recently Completed
| 日付 | ID | 内容 | 反映先 |
|---|---|---|---|
| 2026-02-21 | WB-DOC-001 | ドキュメント導線の一本化（`START_HERE` 追加） | `docs/START_HERE.md` |
| 2026-02-21 | WB-DOC-002 | docs運用ルールの明文化 | `docs/DOCS_GOVERNANCE.md` |
| 2026-02-21 | WB-DOC-003 | docs索引/状態ファイル整理の開始 | `docs/README.md`, `docs/STATUS_CURRENT.md` |
| 2026-02-22 | WB-DOC-004 | 2ツール全体像/運用手順/DB方針/データ契約ドキュメント追加 | `docs/PROGRAM_OVERVIEW.md`, `docs/OPERATIONS_MANUAL.md`, `docs/LOCAL_DB_STRATEGY.md`, `docs/DATA_CONTRACT_APPROVED_LISTING.md` |
| 2026-02-22 | WB-DOC-005 | GitHub運用テンプレ導入（Issue/PR/CODEOWNERS） | `.github/ISSUE_TEMPLATE/*`, `.github/PULL_REQUEST_TEMPLATE.md`, `.github/CODEOWNERS` |
| 2026-02-22 | WB-OPS-001 | ローカルDBバックアップ運用スクリプト追加 | `scripts/backup_local_db.py`, `docs/LOCAL_DB_STRATEGY.md` |
| 2026-02-22 | WB-NAME-001 | 表示名を Reseller / Miner / Operator に統一（internal名は維持） | `README.md`, `docs/*.md`, `.github/ISSUE_TEMPLATE/*` |
| 2026-02-22 | WB-NAME-002 | internal名移行のPhase 1実装（DB canonical化 + legacy fallback） | `reselling/config.py`, `scripts/sync_rejected_blocklist.py`, `scripts/backup_local_db.py`, `docs/INTERNAL_NAME_MIGRATION.md` |
| 2026-02-22 | WB-NAME-003 | internal名移行のPhase 2実装（レスポンスヘッダ新旧互換） | `reselling/live_review_fetch.py`, `docs/INTERNAL_NAME_MIGRATION.md` |
| 2026-02-22 | WB-NAME-004 | internal名移行のPhase 3実装（RPAスクリプトcanonical入口追加） | `scripts/rpa_market_research.py`, `scripts/run_review_cycle.py`, `reselling/live_review_fetch.py` |
| 2026-02-22 | WB-NAME-005 | internal名移行のPhase 4実装（DB実体を `data/reseller.db` へ移行） | `scripts/migrate_db_to_reseller.py`, `.env.local`, `reselling/config.py` |
| 2026-02-22 | WB-P0-005 | 承認済み出品データJSONLエクスポートを実装（Issue #3） | `reselling/approved_export.py`, `scripts/export_approved_listings.py`, `tests/test_approved_listing_export.py`, `data/approved_listing_exports/latest.jsonl` |
| 2026-02-22 | WB-P1-005 | legacy internal aliasサンセット方針を実装/文書化（Issue #4） | `scripts/rpa_ebay_product_research.py`, `scripts/rpa_market_research.py`, `reselling/live_review_fetch.py`, `docs/INTERNAL_NAME_MIGRATION.md` |
| 2026-02-22 | WB-P0-004 | GitHub運用をIssue最小・日報中心へ切替 | `docs/OPERATIONS_MANUAL.md`, `docs/GITHUB_PROJECT_SETUP.md`, `docs/daily_reports/TEMPLATE.md`, `scripts/generate_daily_report.py` |
| 2026-02-22 | WB-OPS-003 | Reseller Roadmapを工数表運用へ整備（Track/担当/見積/実績、未来+過去タスク登録） | `https://github.com/users/michi-system/projects/4` |
| 2026-02-22 | WB-OPS-004 | Operator運用仕様v1と開発工数表を策定 | `docs/OPERATOR_OPERATIONS_SPEC.md`, `docs/OPERATOR_EFFORT_TABLE.md` |
| 2026-02-22 | WB-OPS-005 | Operatorコアロジック実装（取込/出品サイクル/監視判定/設定バージョン管理/API追加） | `listing_ops/*`, `scripts/operator_*.py`, `reselling/api_server.py`, `tests/test_operator_logic.py` |
| 2026-02-22 | WB-MIG-001 | Supabase一括移行の事前安全策を実装（ローカル復帰チェックポイント作成/復元スクリプト + 手順書） | `scripts/create_local_checkpoint.py`, `scripts/restore_local_checkpoint.py`, `docs/SUPABASE_BIG_BANG_MIGRATION.md` |
| 2026-02-22 | WB-MIG-002 | Supabase投入準備を実装（初期スキーマSQL + SQLite CSV一括エクスポート） | `docs/sql/reseller_supabase_schema.sql`, `scripts/export_sqlite_bundle.py`, `docs/SUPABASE_BIG_BANG_MIGRATION.md` |
| 2026-02-22 | WB-MIG-003 | CSVバンドル投入スクリプトを実装（dry-run + truncate投入 + シーケンス同期） | `scripts/import_csv_bundle_to_postgres.py`, `docs/SUPABASE_BIG_BANG_MIGRATION.md` |
| 2026-02-22 | WB-MIG-004 | DBランタイムをSQLite/PostgreSQL両対応化し、Supabase切替運用を有効化 | `reselling/db_runtime.py`, `reselling/models.py`, `listing_ops/models.py`, `reselling/review.py`, `listing_ops/ingest.py`, `reselling/env.py` |
| 2026-02-22 | WB-OPS-006 | Operator運用UIと手動介入APIを実装（一覧/詳細/ジョブ実行/手動状態遷移） | `web/operator.*`, `listing_ops/manual_actions.py`, `reselling/api_server.py`, `tests/test_operator_manual_actions.py` |
| 2026-02-22 | WB-OPS-007 | 別Mac/別アカウントCodex向けの共同開発オンボーディング手順を整備 | `docs/CODEX_COLLAB_ONBOARDING.md`, `docs/README.md`, `docs/START_HERE.md`, `docs/OPERATIONS_MANUAL.md` |
| 2026-02-22 | WB-SEC-001 | `.env`運用を安全化し、未使用のSupabase関連env要素を削除 | `.gitignore`, `.env.example`, `scripts/import_csv_bundle_to_postgres.py`, `reselling/db_runtime.py`, `docs/SUPABASE_BIG_BANG_MIGRATION.md`, `docs/CODEX_COLLAB_ONBOARDING.md` |
| 2026-02-22 | WB-SEC-002 | API接続envの棚卸しを実施し、未使用の `EBAY_DEV_ID` を削除 | `.env.example` |
| 2026-02-22 | WB-OPS-008 | 別アカウント参加設定を1コマンド化（`setup_collab.py`） | `scripts/setup_collab.py`, `docs/CODEX_COLLAB_ONBOARDING.md`, `docs/OPERATIONS_MANUAL.md` |
| 2026-02-22 | WB-MIG-005 | 運用経路（API/エクスポート/否認同期）のPostgreSQL対応を完了し、ローカルDB依存を除去 | `reselling/approved_export.py`, `scripts/export_approved_listings.py`, `scripts/sync_rejected_blocklist.py`, `reselling/db_runtime.py` |

## 4. Backlog
| ID | 優先 | 種別 | 内容 | 着手条件 |
|---|---|---|---|---|
| WB-BK-001 | P1 | 自動化 | カテゴリ別ナレッジ更新の半自動化（鮮度管理） | P0課題の流入安定後 |
| WB-BK-002 | P1 | 運用 | 生成物ローテーション（古い履歴の圧縮/退避） | レポート参照需要の棚卸し後 |
| WB-BK-003 | P2 | UI | レビュー済み分析画面の高度化（否認理由ヒートマップ） | 主要ロジック安定後 |
| WB-BK-004 | P2 | 品質 | 各カテゴリでの季節性係数の運用チューニング | データ母数が十分になった後 |

## 5. Effort Ledger
| ID | 項目 | 内容 | 目安工数 | 状態 |
|---|---|---|---|---|
| EFF-001 | アクティブ件数取得 | Product Research結果から `active_count` を安定抽出し `liquidity` へ保存 | 3-5h | 未実装 |
| EFF-002 | STR算出実装 | `sell_through_90d = sold_90d_count / (sold_90d_count + active_count)` 実装 | 1-2h | 未実装 |
| EFF-003 | UI表示/判定連携 | 「アクティブ / STR」実値表示と閾値判定連動 | 1-2h | 未実装 |

## 6. Decision Log (append only)
| 日付 | 決定ID | 決定内容 | 理由 |
|---|---|---|---|
| 2026-02-19 | DEC-001 | レビュー候補は新品固定・在庫あり必須を既定化 | 誤検知と無駄レビューを減らすため |
| 2026-02-20 | DEC-002 | 90日売却シグナル未取得 (`-1`) は原則除外 | 流動性不明候補の混入防止 |
| 2026-02-20 | DEC-003 | 自動レビュー承認後も最終は人間確認 | 出品誤りリスクを運用で抑えるため |
| 2026-02-21 | DEC-004 | ドキュメント正本を固定し、記録先の分散を禁止 | 引き継ぎコストと判断ブレを抑えるため |
| 2026-02-22 | DEC-005 | Miner + Operator は当面モノレポで運用する | 共通ドメインロジックの再利用と仕様ズレ防止のため |
| 2026-02-22 | DEC-006 | 共有DB移行前はローカルDB + 契約データ出力で連携する | いきなりDB移行せず、安全に段階導入するため |
| 2026-02-22 | DEC-007 | 表示名は Reseller / Miner / Operator を採用し、internal名は段階移行で後追い変更する | 既存コード互換を保ちながら運用認知を統一するため |
| 2026-02-22 | DEC-008 | DB internal名は `data/reseller.db` をcanonicalとし、`data/ebayminer.db` / `reselling.db` をlegacy fallbackで吸収する | 無停止で移行するため |
| 2026-02-22 | DEC-009 | APIレスポンスヘッダは `x-reseller-*` をcanonical化し、`x-ebayminer-*` は env で互換併記を有効化する | クライアント無停止で段階移行するため |
| 2026-02-22 | DEC-010 | RPAスクリプト入口は `scripts/rpa_market_research.py` をcanonicalとし、legacyスクリプトは互換のため維持する | 段階移行中の実行互換を保つため |
| 2026-02-22 | DEC-011 | `.env.local` の `DB_PATH` を canonical (`data/reseller.db`) に更新し、実行時の優先DBを新名へ固定する | 設定値による逆戻りを防ぐため |
| 2026-02-22 | DEC-012 | legacyスクリプト `scripts/rpa_ebay_product_research.py` は canonical 実装への互換ラッパーに変更する | 既存ジョブの参照切れを防ぐため |
| 2026-02-22 | DEC-013 | legacyラッパー `scripts/rpa_ebay_product_research.py` は 2026-03-31 以降に削除判断、互換ヘッダ設定は 2026-04-15 までに最終判断する | 日付付きで撤去条件を固定し、だらだら残る状態を防ぐため |
| 2026-02-22 | DEC-014 | GitHub運用は Issue駆動を常用せず、ProjectのDraftカード + 日報 (`docs/daily_reports`) を正本にする | 実装速度を優先しつつ、作業履歴と作業者同定を日次で残すため |
| 2026-02-22 | DEC-015 | ロードマップは `Reseller 工数表` で管理し、`Status=Todo` を未来、`Status=Done` を過去として扱う | 未来タスクと完了履歴を1枚で俯瞰するため |
| 2026-02-22 | DEC-016 | 担当の初期割当は `michi-system: 全体リード + Miner/Shared`, `a28ngi: Operator` とする | 並列開発で詰まりを減らし、責任境界を明確にするため |
| 2026-02-22 | DEC-017 | 停止済み商品の復帰は自動再開せず、`manual-resume-ready` で再出品キューへ戻す運用に固定 | 自動再開の誤復帰リスクを抑えつつ、再出品判断を人間で管理するため |
| 2026-02-22 | DEC-018 | Supabase移行は一括切替で実施し、PITR必須ではなくローカルチェックポイント復元をロールバック正本とする | 「最終的にMacへ戻せる」を主目的に、コストと運用速度を優先するため |
| 2026-02-22 | DEC-019 | Supabase移行入力はSQLite全表のCSVバンドルを正本にし、手動テーブル投入手順を固定する | DB間差分を可視化しつつ、移行時の再実行性を確保するため |
| 2026-02-22 | DEC-020 | CSV投入は `import_csv_bundle_to_postgres.py` を正本にし、`--dry-run` と `--truncate --apply` の2段運用に固定する | 投入ミスを防ぎ、毎回同じ手順で再現可能にするため |
| 2026-02-22 | DEC-021 | DB接続は共通ラッパー (`reselling/db_runtime.py`) でSQLite/PG差分を吸収し、`.env.local` の `DB_BACKEND` / `SUPABASE_DB_URL` で切替する | 実装速度を維持したまま本番移行とローカル復帰を両立するため |
| 2026-02-22 | DEC-022 | 共同開発の初回参加手順は `docs/CODEX_COLLAB_ONBOARDING.md` に一本化する | 別環境の参加準備を標準化し、設定漏れとアカウント混在を減らすため |
| 2026-02-22 | DEC-023 | 接続envは `SUPABASE_DB_URL` に一本化し、未使用の `SUPABASE_ANON_KEY` / `SUPABASE_SERVICE_ROLE_KEY` / `DATABASE_URL` を撤去する | 共有時の誤設定と秘匿情報混在リスクを下げ、運用理解を単純化するため |
| 2026-02-22 | DEC-024 | eBay接続envから未使用の `EBAY_DEV_ID` を削除し、実運用で使うキーを `EBAY_CLIENT_ID` / `EBAY_CLIENT_SECRET` に限定する | 設定ミスを減らし、共同作業者の初期設定を単純化するため |
| 2026-02-22 | DEC-025 | 共同作業者の初期設定は `scripts/setup_collab.py` に集約し、手動設定手順を縮小する | 別アカウント参加の立ち上げ時間を短縮し、環境差分による詰まりを減らすため |
| 2026-02-22 | DEC-026 | 運用スクリプトはPostgreSQLを正本とし、SQLite直接参照は移行/バックアップ用途の補助スクリプトに限定する | 本番運用のDB依存を一元化しつつ、復旧手段を維持するため |

## 7. 次エージェント向け起点
1. `Now` の `in_progress` を上から順に処理。
2. 実装変更時は `Decision Log` と `STATUS_CURRENT.md` を同期。
3. 生成物パスを増やしたら `RECORDS_REGISTRY.md` を更新。

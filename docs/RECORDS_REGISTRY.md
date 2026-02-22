# Records Registry

このファイルは、プロジェクト内の「生成物」「状態ファイル」「履歴ログ」の保存先を一元管理する索引です。  
新しい出力先を追加したら、このファイルを必ず更新します。

## 1. ルール
- 生成物 (`docs/*.json`, `docs/autonomous_*`, `docs/cycle_diagnostics`) は手編集禁止。
- 状態ファイル (`data/review_*`, `data/liquidity_*`) は原則手編集禁止。
- パス変更時はコード変更と同じコミットで本ファイルを更新する。

## 2. サイクル系の最新ポインタ
| パス | 生成元 | 用途 |
|---|---|---|
| `docs/review_cycle_active.json` | `scripts/run_review_cycle.py` | 現在アクティブなサイクル情報 |
| `docs/review_cycle_report_latest.json` | `scripts/run_review_cycle.py` | 最新の探索/候補化レポート |
| `docs/review_cycle_auto_review_latest.json` | `scripts/auto_review_cycle.py` | 最新の自動レビュー結果 |
| `docs/review_cycle_close_report_latest.json` | `scripts/close_review_cycle.py` | 最新の締めレポート |
| `docs/review_cycle_validation_latest.json` | `scripts/run_autonomous_cycle.py` | 最新の整合性検証レポート |

## 3. 自律実行の履歴
| パス | 生成元 | 用途 |
|---|---|---|
| `docs/autonomous_guarded_runs/` | `scripts/run_autonomous_cycles_guarded.py` | fail-fast 実行の周回ログ・成果物 |
| `docs/autonomous_guarded_runs/summary_latest.json` | `scripts/run_autonomous_cycles_guarded.py` | guarded実行の集計サマリ |
| `docs/autonomous_runs/` | 過去運用スクリプト | 旧自律実行履歴 |
| `docs/autonomous_runs_v2/` | 過去運用スクリプト | 旧v2履歴 |

## 4. 診断/検証レポート
| パス | 生成元 | 用途 |
|---|---|---|
| `docs/cycle_diagnostics/*.json` | 検証スクリプト群 | 個別シナリオ診断 |
| `docs/review_cycle_report_probe_*.json` | 検証実行 | プローブ結果 |
| `docs/review_cycle_report_run_*.json` | 単発サイクル実行 | run単位の保存 |
| `docs/query_width_report*.json` | `scripts/query_width_pilot.py` | API探索幅検証 |
| `docs/query_width_summary.json` | `scripts/summarize_query_width.py` | 幅検証の集約 |
| `docs/fetch_autotune_report_latest.json` | fetch autotune処理 | 取得チューニング結果 |
| `docs/min_new_autotune_batch_latest.json` | fetch autotune処理 | min_new_items バッチ調整結果 |
| `docs/min_new_autotune_validation_latest.json` | fetch autotune処理 | min_new_items 検証結果 |
| `docs/rpa_speed_benchmark_*` | RPAベンチ実行 | RPA速度比較 |
| `docs/fetch_run_v16_summary.json` | 単発検証 | fetchラン集計 |

## 5. 実行状態ファイル（`data/`）
| パス | 更新元 | 用途 |
|---|---|---|
| `data/reseller.db` | API/スクリプト全体 | 候補・レビュー・設定の主DB (canonical) |
| `data/ebayminer.db` | API/スクリプト全体 | 旧DBパス (互換読み込み対象) |
| `reselling.db` | API/スクリプト全体 | 旧ローカルDBパス (互換読み込み対象) |
| `data/category_knowledge_seeds_v1.json` | 手動更新 + fetch参照 | カテゴリ展開ナレッジ正本 |
| `data/review_blocklist.json` | `scripts/sync_rejected_blocklist.py`, `scripts/apply_cycle_improvements.py` | 否認由来の除外対象 |
| `data/review_query_cache.json` | `reselling/live_review_fetch.py` | クエリ結果キャッシュ |
| `data/review_query_skip.json` | `reselling/live_review_fetch.py` | 低収率スキップ状態 |
| `data/review_fetch_cursor.json` | `reselling/live_review_fetch.py` | サイト別ページング継続位置 |
| `data/review_fetch_tuner.json` | `reselling/live_review_fetch.py` | 取得チューニング状態 |
| `data/review_api_usage.json` | `reselling/live_review_fetch.py` | API利用状況 |
| `data/query_efficiency_stats.json` | `scripts/run_review_cycle.py` | クエリ歩留まり統計 |
| `data/liquidity_backfill_targets.json` | `scripts/run_review_cycle.py` | 流動性欠損埋めターゲット |
| `data/liquidity_rpa_progress.json` | `reselling/live_review_fetch.py` | Product Research進行状態 |
| `data/liquidity_rpa_fetch_state.json` | `reselling/live_review_fetch.py` | RPA呼び出し間隔/状態 |
| `data/liquidity_rpa_signals.jsonl` | `scripts/rpa_market_research.py` | 90日売却シグナル入力 |
| `data/rpa/ebay_profile/` | Playwright persistent profile | eBayログインセッション保持 |

## 6. その他の運用記録
| パス | 用途 |
|---|---|
| `docs/fx_rate_cache.json` | FX最新取得キャッシュ |
| `docs/daily_reports/*.md` | 日次作業レポート（`scripts/generate_daily_report.py` で生成） |
| `logs/` | 実行ログ置き場 |
| `backups/` | 手動バックアップ置き場 |
| `docs/archive/manual/` | 手動作成レポートの退避先 |

## 7. 参照優先順
1. 仕様確認: `docs/REQUIREMENTS.md` / `docs/DEFINITION_OF_DONE.md` / `docs/OPERATION_POLICY.json`
2. 現在地確認: `docs/STATUS_CURRENT.md`
3. 進行確認: `docs/WORKBOARD.md`
4. 実行結果確認: 本ファイルの各出力先

## 8. 変更チェックリスト
新しい生成物パスを追加した場合は、次を同時実施する。
1. 生成スクリプトの出力先を明示
2. 本ファイルに行を追加
3. `docs/README.md` または `docs/START_HERE.md` に導線を追加（必要時）
4. `docs/HANDOVER_MANIFEST.json` の該当配列を更新

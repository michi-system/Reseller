# Docs Governance

このファイルは「どこに何を書くか」を固定する運用ルールです。

## 1. 文書クラス定義
| クラス | 編集可否 | 例 | 用途 |
|---|---|---|---|
| 正本 | 手動更新 | `docs/REQUIREMENTS.md`, `docs/OPERATION_POLICY.json` | 要件・基準・運用判断の正本 |
| 補助資料 | 手動更新 | `docs/RESEARCH_NOTES.md`, `docs/API_LOCAL.md` | 調査・運用補助 |
| 生成物 | 手動編集禁止 | `docs/review_cycle_*_latest.json`, `docs/autonomous_*` | 実行結果の証跡 |
| 実行状態 | 原則手動編集禁止 | `data/review_query_cache.json` など | ランタイム継続状態 |
| アーカイブ | 原則固定 | `docs/archive/**` | 旧資料保管 |

## 2. 情報の正規書き先
| 情報の種類 | 書き先 |
|---|---|
| 要件変更 | `docs/REQUIREMENTS.md` |
| 完了条件変更 | `docs/DEFINITION_OF_DONE.md` |
| 閾値/ガード変更 | `docs/OPERATION_POLICY.json` |
| 進行管理・工数・決定事項 | `docs/WORKBOARD.md` |
| 現在の運用スナップショット | `docs/STATUS_CURRENT.md` |
| ナレッジ確定版 | `docs/KNOWLEDGE_LIBRARY.md` + `data/category_knowledge_seeds_v1.json` |
| 未確定調査メモ | `docs/RESEARCH_NOTES.md` |
| 生成物の所在/意味 | `docs/RECORDS_REGISTRY.md` |

## 3. 更新トランザクション（必須手順）
### 3.1 要件・運用変更時
1. `docs/REQUIREMENTS.md` を更新
2. `docs/DEFINITION_OF_DONE.md` を同期
3. `docs/OPERATION_POLICY.json` を同期
4. `docs/WORKBOARD.md` の Decision Log に理由を追記
5. `docs/STATUS_CURRENT.md` を必要箇所だけ更新

### 3.2 実装変更時
1. `docs/WORKBOARD.md` のタスク状態を更新
2. 運用影響があるなら `docs/STATUS_CURRENT.md` 更新
3. 出力パスを追加/変更したら `docs/RECORDS_REGISTRY.md` 更新

### 3.3 新規ドキュメント追加時
1. 既存正本で代替不可であることを確認
2. 追加後に `docs/README.md` へ索引追記
3. 必要なら `docs/START_HERE.md` に導線追記
4. 機械参照が必要なら `docs/HANDOVER_MANIFEST.json` を更新

## 4. 生成物と状態ファイルの扱い
手編集禁止:
- `docs/review_cycle_*_latest.json`
- `docs/autonomous_runs*/`
- `docs/autonomous_guarded_runs/`
- `docs/cycle_diagnostics/`
- `data/review_query_cache.json`
- `data/review_query_skip.json`
- `data/review_fetch_cursor.json`
- `data/review_fetch_tuner.json`
- `data/liquidity_rpa_progress.json`
- `data/liquidity_rpa_fetch_state.json`

修正が必要な場合は手編集せず、再実行または専用メンテナンススクリプトで更新する。

## 5. 散乱防止の禁止事項
- 同じ責務の md/json を複数作らない
- 「最新状態」を複数ファイルに同時記載しない
- 固定出力パスを無断で変更しない
- 運用判断をコミットメッセージだけに残して docs に反映しない

## 6. 引き継ぎチェック
引き継ぎ前に最低限確認する。
1. `docs/START_HERE.md` の導線が有効
2. `docs/WORKBOARD.md` の `Now` が更新済み
3. `docs/STATUS_CURRENT.md` が最新状態を反映
4. `docs/RECORDS_REGISTRY.md` が現実の出力先と一致

## 7. 廃止・移管
- 手動ドキュメントを廃止するときは `docs/archive/` に移してから参照を更新する。
- 生成物の置き場を変える場合は、先にコードと `docs/RECORDS_REGISTRY.md` を同時更新する。

# Internal Name Migration Plan

この文書は internal 名の段階移行計画です。  
display名は `Reseller / Miner / Operator` を採用済み、internal名は互換を保ちながら移行します。

## 1. 命名マップ
| 区分 | 旧 | 新 (canonical) | 互換方針 |
|---|---|---|---|
| プロジェクト表示名 | ebayminer | Reseller | 即時切替済み |
| Tool 1 display | ebayminer | Miner | 即時切替済み |
| Tool 2 display | listing-ops | Operator | 即時切替済み |
| 主DBパス | `data/ebayminer.db` | `data/reseller.db` | 自動フォールバック |
| API識別子 | `ebayminer-api/0.1` | `reseller-api/0.1` | 識別子更新済み |

## 2. 2026-02-22 までの実装済み
- `reselling/config.py`
  - `data/reseller.db` を canonical に設定
  - `data/ebayminer.db`, `reselling.db` を legacy fallback として自動解決
- `scripts/sync_rejected_blocklist.py`
  - DBパスを `load_settings().db_path` に統一
- `scripts/backup_local_db.py`
  - canonical/legacy DBを自動検出してバックアップ
- `reselling/api_server.py`
  - `server_version` を `reseller-api/0.1` へ更新
- `reselling/live_miner_fetch.py`
  - レスポンスヘッダを `x-reseller-*` をcanonical出力に変更
  - `INTERNAL_EMIT_LEGACY_EBAYMINER_HEADERS=1` のときだけ `x-ebayminer-*` を併記
  - 読み取り側も新旧ヘッダ両対応に統一
- `scripts/rpa_market_research.py`
  - canonical実装ファイルとして移行済み
  - 呼び出し元 (`run_miner_cycle`, `live_miner_fetch`) を新入口へ切替
- `scripts/rpa_ebay_product_research.py`
  - legacy互換ラッパーとして維持（canonicalを再export）
- `scripts/migrate_db_to_reseller.py`
  - `data/ebayminer.db` -> `data/reseller.db` 移行
  - `.env.local` の `DB_PATH` を canonical へ更新

## 3. 未着手（次フェーズ）
1. スクリプト名の最終収束
- 呼び出し元は新名へ移行済み。legacyラッパーの削除時期だけを判断する。
- `scripts/rpa_ebay_product_research.py` は deprecation 警告付きで暫定維持。

2. DBファイル名の最終収束
- 運用DBは `data/reseller.db` へ移行済み。
- legacy DBは当面 fallback として保持する（削除は別フェーズ）。
- 固定後に `docs/HANDOVER_MANIFEST.json` と `docs/RECORDS_REGISTRY.md` から legacy を段階除去する。

3. 参照パスの正規化
- docs内の絶対パス `/.../ebayminer/...` はリポジトリ名変更時に崩れる。
- 相対パス中心へ段階置換する。

## 4. サンセット計画
- 2026-03-15 まで: 既存ジョブを `scripts/rpa_market_research.py` へ切替完了。
- 2026-03-31 以降: legacyラッパー (`scripts/rpa_ebay_product_research.py`) 削除可。
- 2026-04-15 まで: `INTERNAL_EMIT_LEGACY_EBAYMINER_HEADERS` の運用要否を最終判断。

## 5. 実行コマンド（完了済み）
```bash
python3 scripts/migrate_db_to_reseller.py
```

## 6. 移行ルール
- 一度に全部置換しない。PRを小分けにする。
- 互換レイヤーを先に入れてから呼び出し元を置換する。
- 各ステップで最低限の疎通確認を行う。

## 7. ロールバック方針
- DB指定が原因で起動失敗した場合:
  1. `DB_PATH` を明示設定して起動確認
  2. legacy DBへ一時戻す
  3. `docs/WORKBOARD.md` の Decision Log に理由記録

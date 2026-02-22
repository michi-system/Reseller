# Local DB Strategy

この文書は、現状のローカルDB運用と将来の共有DB移行方針を定義します。

## 1. 現在の状態
- canonical DB: `data/reseller.db` (SQLite)
- legacy DB: `data/ebayminer.db`, `reselling.db` (自動互換対象)
- バックアップ保存先: `backups/`
- 実行状態ファイル: `data/` 配下の json

## 2. 方針
- 当面はローカルDB継続でよい。
- ただし、Operator連携に必要な「承認済み商品の共有」はDB直接共有ではなく、契約済みデータ形式で行う。
- 共有データを安定運用できたら、共有DB (PostgreSQLなど) へ移行する。

## 3. 段階計画
1. Phase A: ローカルDB固定
- Miner内の承認情報をSQLiteに保持。
- バックアップを定期取得する。

2. Phase B: 共有出力追加
- Minerが承認済み商品を契約形式で出力する。
- 例: `data/approved_listing_exports/latest.jsonl`
- Operatorはこの契約データだけを読む。

3. Phase C: 共有DB移行
- 契約形式をそのまま共有DBへ保存。
- Operatorは共有DBから直接取得。
- 切替期間は JSONL とDBを並行運用し、差分を比較する。

## 4. バックアップ/復旧ルール
- 毎日: SQLiteバックアップを1世代追加。
- 毎週: 直近7日分を保持、古い世代を圧縮退避。
- 実行コマンド:
  - 作成 + 7日超過削除: `python3 scripts/backup_local_db.py`
  - 確認のみ: `python3 scripts/backup_local_db.py --dry-run`
- 出力形式: `backups/<db_filename>.auto_YYYYMMDD_HHMMSS.bak`
- 復旧手順:
  1. 現行DB（`data/reseller.db` または legacy DB）を退避。
  2. `backups/` から最新正常版を復元。
  3. `docs/WORKBOARD.md` の `Decision Log` に復旧理由を記録。

## 5. 移行判断のゲート
次を満たしたら共有DBへ移行する。
- 承認済み出力が2週間安定（欠損なし）
- Operatorが契約形式で問題なく出品処理できる
- 停止ロジックの判定ログが追跡可能

## 6. 注意点
- `data/reseller.db` / legacy DB を複数PCで同時共有しない。
- 実行中にDBファイルを手編集しない。
- スキーマ変更時は `docs/DATA_CONTRACT_APPROVED_LISTING.md` を先に更新する。

## 7. 移行コマンド
初回移行時は次を実行する。

```bash
python3 scripts/migrate_db_to_reseller.py
```

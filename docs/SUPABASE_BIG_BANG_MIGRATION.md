# Supabase一括移行手順（ローカル即時ロールバック対応）

最終更新: 2026-02-22 (JST)

この文書は、ResellerのDBをローカルSQLiteからSupabase (PostgreSQL) へ一括移行し、必要時にMacローカル運用へ即時復帰できるようにするための実行手順です。

## 1. 方針
- 移行は一括で実施する（段階的並行は行わない）。
- ただし、切替前チェックポイントを必ず作成し、ローカル復帰手順を固定する。
- PITRは必須ではない（「最終的にMacへ戻せる」が目的のため）。

## 2. あなたが最低限やること
1. Supabaseプロジェクトを作成する。
2. `Project URL` を控える。
3. `service_role key` を控える。
4. `Postgres接続文字列 (DATABASE_URL)` を控える。
5. 移行時間を決め、その時間は作業更新を止める。

上記5点以外はCodex側で実行可能です。

## 3. 移行直前チェック（必須）
1. ローカルAPI/ジョブを停止する（書き込み停止）。
2. 次を実行してチェックポイントを作成する。

```bash
python3 scripts/create_local_checkpoint.py --tag pre-supabase-bigbang
```

3. 作成先例: `backups/checkpoints/pre-supabase-bigbang_YYYYMMDD_HHMMSS/`
4. `manifest.json` があることを確認する。

## 4. Codex側で実施する作業（移行本体）
1. Supabase SQL Editor で `docs/sql/reseller_supabase_schema.sql` を実行する。
2. 次でCSVバンドルを作る（SQLite全表を一括エクスポート）。

```bash
python3 scripts/export_sqlite_bundle.py --tag pre-supabase-bigbang
```

3. 生成先 `backups/sqlite_exports/pre-supabase-bigbang_YYYYMMDD_HHMMSS/` からCSVをSupabase各テーブルへ投入する。
4. APIの読み書き先をSupabaseへ切替。
5. 主要API (`/healthz`, review, operator) の疎通確認。
6. 監視サイクル・出品サイクルをdry-runで再実行し、整合性確認。

## 5. ローカル復帰（ロールバック）手順
不具合時は次で即時復帰できます。

1. API/ジョブを停止する。
2. 最新チェックポイントを確認する（dry-run）。

```bash
python3 scripts/restore_local_checkpoint.py --dry-run
```

3. 復元を実行する。

```bash
python3 scripts/restore_local_checkpoint.py --apply
```

4. ローカル設定でAPI再起動し、疎通確認する。
5. `docs/WORKBOARD.md` の `Decision Log` に復帰理由を記録する。

## 6. 注意点
- 復帰時点以降のSupabase側最新データは、同期していなければローカルへ自動反映されません。
- 「データ欠損なしで戻す」必要がある場合は、移行後に定期エクスポート運用を追加してください。

## 7. 関連コマンド
```bash
# 既存ローカルDBバックアップ（従来運用）
python3 scripts/backup_local_db.py

# 切替前チェックポイント作成（推奨）
python3 scripts/create_local_checkpoint.py --tag pre-supabase-bigbang

# SQLite全表をCSVバンドル化（移行投入用）
python3 scripts/export_sqlite_bundle.py --tag pre-supabase-bigbang

# 復帰確認
python3 scripts/restore_local_checkpoint.py --dry-run

# 復帰実行
python3 scripts/restore_local_checkpoint.py --apply
```

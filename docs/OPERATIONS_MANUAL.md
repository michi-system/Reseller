# Operations Manual (Codex + GitHub)

この文書は、GitHubに不慣れなメンバーでも同じ運用で進めるための手順書です。

## 0. 命名ルール（表示名）
- Project: `Reseller`
- Tool 1: `Miner` (internal: `ebayminer`)
- Tool 2: `Operator` (internal: `listing-ops`, planned)

## 1. 使う画面と役割
- Codex: 実装、ファイル編集、ローカル実行、テスト。
- GitHub Web: PRレビュー、日報管理、最終マージ判断。

## 2. 毎日の進め方（Issue最小 + 日報中心）
1. GitHub Projectで「今日のDraftカード」を `Doing` に移動。
2. Codexで実装を進める（Issueは通常作らない）。
3. 必要なPRを作る（Issue紐付けは必須ではない）。
4. その日の終わりに日報を生成する。
5. 日報リンクをProjectカードへ追記し、`Reported` へ移動。

## 3. Codexで必ず開く場所
- 作業フォルダ: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer`
- 最初に読む: `docs/START_HERE.md`
- 進行管理の正本: `docs/WORKBOARD.md`
- 日報テンプレ: `docs/daily_reports/TEMPLATE.md`

## 4. 作業者同定ルール（必須）
作業者の特定は、次の3点を同時に記録する。

1. `GitHub Actor`
- pushやPRを実行したアカウント（GitHub上の実行者）。

2. `Git Author`
- commitの `user.name` / `user.email`（実作業者の署名）。

3. `Daily Report Owner`
- 日報の `operator` フィールド（`@github_id`）。

この3点が一致しない場合は、日報の `Risks / Follow-ups` に理由を残す。

## 5. GitHub Projectでの管理単位
- 基本: `Draft item` だけを使う（Issueを通常運用で使わない）。
- 1日1カード: `YYYY-MM-DD @operator` 形式で作る。
- 列: `Today` / `Doing` / `Done` / `Reported`
- `Reported` に移す条件: 日報ファイルのパスをカードに追記済み。

## 6. 日報生成手順（作業終了時）
1. 次を実行する。

```bash
python3 scripts/generate_daily_report.py --date YYYY-MM-DD --operator @github_id
```

2. 生成先: `docs/daily_reports/YYYY-MM-DD.md`
3. 必ず埋める項目:
- `Validation`
- `Risks / Follow-ups`
4. PRがある日は、日報内の `Commits` とPR差分が矛盾しないか確認する。

## 7. 作業開始テンプレ（Codex向け）
次をそのままCodexに渡す。

```text
今日の作業を実装して。最後に日報を生成できるように、
変更ファイル・検証結果・残課題をまとめて報告して。
```

## 8. Issueを作る条件（例外運用）
通常はIssueを作らない。次の場合だけIssue化する。
1. 本番障害/ブロッカー（復旧トラッキングが必要）
2. 外部依頼を明示的に残す必要がある
3. 2日以上またぐ大型作業で、完了条件の合意が必要

## 9. PRレビューで見るポイント
1. 仕様影響: 要件や閾値が変わるか
2. データ影響: DB項目や保存先が変わるか
3. 運用影響: 停止ロジックや監視ジョブに副作用があるか
4. 検証: テスト or 実行ログの証跡があるか

## 10. 週次運用（30分）
1. KPI確認（`docs/PROGRAM_OVERVIEW.md` の6指標）
2. 日報（`docs/daily_reports/*.md`）を見て停止理由トップ3を決める
3. `docs/WORKBOARD.md` の `Now` 並べ替え
4. `Decision Log` に方針変更を追記
5. `python3 scripts/backup_local_db.py --dry-run` でバックアップ運用を確認

## 11. 保存場所ルール
- 要件/運用ルール: `docs/REQUIREMENTS.md`, `docs/OPERATION_POLICY.json`
- プロジェクト全体像: `docs/PROGRAM_OVERVIEW.md`
- internal名移行方針: `docs/INTERNAL_NAME_MIGRATION.md`
- 日々の作業管理: `docs/WORKBOARD.md`
- 日報: `docs/daily_reports/`
- 生成物ログ: `docs/RECORDS_REGISTRY.md` で参照
- 実行状態/キャッシュ: `data/`（手編集しない）
- ローカルDBバックアップ: `backups/`（`scripts/backup_local_db.py` で管理）

## 12. 事故を避ける禁止事項
- `docs/review_cycle_*_latest.json` を手編集しない。
- `data/*.json` の実行状態ファイルを手編集しない。
- 1つの変更で要件だけ更新して、運用閾値を更新し忘れない。
- 仕様変更をPR本文だけに残し、`docs/WORKBOARD.md` へ残さない運用をしない。
- 日報なしで作業を終えた扱いにしない。

## 13. 2人チームのおすすめ担当
- オーナー: 要件決定、承認基準、最終マージ判断。
- メンバー: 実装、テスト、PR作成、ログ整理。
- 共通: `docs/WORKBOARD.md` 更新、日報作成、週次レビュー参加。

## 14. 一括移行運用（Supabase）
- 一括移行時は `docs/SUPABASE_BIG_BANG_MIGRATION.md` を正本にする。
- スキーマ初期化は `docs/sql/reseller_supabase_schema.sql` を使う。
- 切替前に必ず次を実行する。

```bash
python3 scripts/create_local_checkpoint.py --tag pre-supabase-bigbang
```

- データ投入用CSVを次で一括出力する。

```bash
python3 scripts/export_sqlite_bundle.py --tag pre-supabase-bigbang
```

- CSVバンドルをPostgreSQLへ投入する（本実行時）。

```bash
python3 scripts/import_csv_bundle_to_postgres.py --bundle-dir backups/sqlite_exports/pre-supabase-bigbang_YYYYMMDD_HHMMSS --truncate --apply
```

- 不具合時の即時復帰は次を実行する。

```bash
python3 scripts/restore_local_checkpoint.py --apply
```

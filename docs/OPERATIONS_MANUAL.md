# Operations Manual (Codex + GitHub)

この文書は、GitHubに不慣れなメンバーでも同じ運用で進めるための手順書です。

## 0. 命名ルール（表示名）
- Project: `Reseller`
- Tool 1: `Miner` (internal: `ebayminer`)
- Tool 2: `Operator` (internal: `listing-ops`, planned)

## 1. 使う画面と役割
- Codex: 実装、ファイル編集、ローカル実行、テスト。
- GitHub Web: タスク管理（Issue/Project）、PRレビュー、マージ判断。

## 2. 毎日の進め方（最小ループ）
1. GitHub Projectで今日のカードを1つ `Doing` に移動。
2. Issue本文の「完了条件」を確認。
3. Codexでリポジトリを開く。
4. 変更する。
5. PRを作る。
6. レビューし、OKならマージ。
7. Projectカードを `Done` に移す。

## 3. Codexで必ず開く場所
- 作業フォルダ: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer`
- 最初に読む: `docs/START_HERE.md`
- 進行管理の正本: `docs/WORKBOARD.md`

## 4. GitHubでの管理単位
- Issue: 1つの作業単位（必ず完了条件を書く）。
- PR: 変更のレビュー単位（Issue番号を紐付ける）。
- Project: 進行の可視化（Backlog/Doing/Review/Done）。

## 5. 作業開始テンプレ（Codex向け）
Issueを開いて、次をそのままCodexに渡す。

```text
このIssueを実装して。完了条件を満たすこと。変更したファイルと確認結果を最後に報告して。
Issue: <URL>
```

## 6. PRレビューで見るポイント
1. 仕様影響: 要件や閾値が変わるか
2. データ影響: DB項目や保存先が変わるか
3. 運用影響: 停止ロジックや監視ジョブに副作用があるか
4. 検証: テスト or 実行ログの証跡があるか

## 7. 週次運用（30分）
1. KPI確認（`docs/PROGRAM_OVERVIEW.md` の6指標）
2. 停止理由トップ3の対策決め
3. `docs/WORKBOARD.md` の `Now` 並べ替え
4. `Decision Log` に方針変更を追記
5. `python3 scripts/backup_local_db.py --dry-run` でバックアップ運用を確認

## 8. 保存場所ルール
- 要件/運用ルール: `docs/REQUIREMENTS.md`, `docs/OPERATION_POLICY.json`
- プロジェクト全体像: `docs/PROGRAM_OVERVIEW.md`
- internal名移行方針: `docs/INTERNAL_NAME_MIGRATION.md`
- 日々の作業管理: `docs/WORKBOARD.md`
- 生成物ログ: `docs/RECORDS_REGISTRY.md` で参照
- 実行状態/キャッシュ: `data/`（手編集しない）
- ローカルDBバックアップ: `backups/`（`scripts/backup_local_db.py` で管理）

## 9. 事故を避ける禁止事項
- `docs/review_cycle_*_latest.json` を手編集しない。
- `data/*.json` の実行状態ファイルを手編集しない。
- 1つの変更で要件だけ更新して、運用閾値を更新し忘れない。
- 仕様変更をPR本文だけに残し、`docs/WORKBOARD.md` へ残さない運用をしない。

## 10. 2人チームのおすすめ担当
- オーナー: 要件決定、承認基準、最終マージ判断。
- メンバー: 実装、テスト、PR作成、ログ整理。
- 共通: `docs/WORKBOARD.md` 更新と週次レビュー参加。

# GitHub Project Setup Checklist

この文書は、GitHub Webで最初に行う設定を順番にまとめたチェックリストです。

## 1. リポジトリ基本設定（1回だけ）
1. `Settings -> General` を開く
2. Default branch を運用ブランチに統一する（`master` か `main`）
3. Pull Requests のテンプレ利用を有効にする（本リポジトリは自動反映済み）

## 2. ブランチ保護（推奨）
1. `Settings -> Branches -> Add rule`
2. 対象ブランチ: default branch
3. 次を有効化:
   - Require a pull request before merging
   - Require approvals (1以上)
   - Dismiss stale approvals when new commits are pushed

## 3. Labels整備（最小）
- `feature`
- `bug`
- `miner`
- `operator`
- `shared-contract`
- `priority:P0`
- `priority:P1`

移行期間は旧ラベル（`research`, `listing-ops`）が残っていてもよい。
Issueは例外運用なので、通常日はこのラベルを使わない。

新規Issueで領域/優先度ラベルが未設定だった場合は、`.github/workflows/issue-default-labels.yml` が
`shared-contract` と `priority:P1` を自動補完する。

## 4. GitHub Project作成（Daily Report運用）
1. `Projects -> New project -> Board`
2. 列を4つにする:
   - Today
   - Doing
   - Done
   - Reported
3. アイテムは `Draft item` を使う（Issue連携を前提にしない）。
4. カード命名規則:
   - `YYYY-MM-DD @operator`
5. カードに最低限残す情報:
   - その日のPRリンク
   - 日報ファイルパス（`docs/daily_reports/YYYY-MM-DD.md`）

## 5. Project列の運用ルール
1. 朝: `Today` -> `Doing`
2. 実装完了: `Doing` -> `Done`
3. 日報追記完了: `Done` -> `Reported`
4. 翌週のレビューで `Reported` を確認し、次週分を `Today` に補充

## 6. PR運用ルール
- Issue紐付けは任意（必要時のみ）
- テンプレの `Impact Check` を埋める
- 仕様変更なら `docs/WORKBOARD.md` の Decision Log を更新
- マージ前に日報に反映できる情報（検証結果、残課題）をPR内に残す

## 7. 作業者同定のチェック項目
日報レビュー時に次を照合する。
1. `operator`（日報）
2. `GitHub Actor`（push/PR実行者）
3. `Git Author`（commit著者）

不一致があれば、日報に理由を追記する。

## 8. 週次ルーティン
1. `Reported` 列のカードを確認
2. 停止理由や不具合を必要時のみ `bug` で起票
3. 次週の `Today` を2-3件に絞る
4. `docs/WORKBOARD.md` とProjectの整合を取る

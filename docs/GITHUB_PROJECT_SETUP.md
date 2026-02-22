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

移行期間は旧ラベル（`research`, `listing-ops`）が残っていてもよい。新規Issueは `miner` / `operator` を使う。

## 4. GitHub Project作成
1. `Projects -> New project -> Board`
2. 列を4つにする:
   - Backlog
   - Doing
   - Review
   - Done
3. 自動化（任意）:
   - PRがOpenになったら `Review`
   - PRがMergeされたら `Done`

## 5. Issue運用ルール（2人チーム）
- 1Issue = 1成果物
- 完了条件を必ず記載
- ラベルは最低2つ（領域 + 優先度）

## 6. PR運用ルール
- 必ずIssueを紐付ける
- テンプレの `Impact Check` を埋める
- 仕様変更なら `docs/WORKBOARD.md` の Decision Log を更新

## 7. 週次ルーティン
1. Done列のカードを確認
2. 停止理由や不具合を `bug` で再起票
3. 次週のDoingを2-3件に絞る
4. `docs/WORKBOARD.md` とProjectの整合を取る

# Daily Report Template

日次の作業終了時に、1日1ファイルで記録する。

## 1. Metadata
- date: `YYYY-MM-DD`
- operator: `@github_id`
- github_actor: `@github_id` (push実行アカウント)
- git_author_default: `Name <email>`
- generated_at: `ISO8601`

## 2. Commit Summary
- commit_count: `N`
- commit_range: `oldest_short..latest_short` (なければ `none`)
- authors:
  - `Name <email>`
- primary_scope:
  - `Miner`
  - `Operator`
  - `Shared`
  - `Docs`

## 3. Commits
- `hash` `author` - `subject`

## 4. Changed Files
- `path/to/file`

## 5. Validation
- tests:
  - `command -> result`
- manual_checks:
  - `check -> result`

## 6. Risks / Follow-ups
- `明日の着手点 / 残課題`

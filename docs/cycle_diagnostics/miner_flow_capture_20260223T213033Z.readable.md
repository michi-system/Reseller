# Miner探索 実測レポート（可読版）: 20260223T213033Z

## 実行サマリ
- カテゴリ: `watch`
- 実行時間: `183 ms`
- 開始(UTC): `2026-02-23T21:30:40.015000+00:00`
- 終了(UTC): `2026-02-23T21:30:40.198000+00:00`
- 作成候補数: `0`
- 停止理由: ``
- 実行パス数: `0`
- Seed数: `0`

## フロー概要（バックエンド）
1. `timed_fetch_start` で探索開始
2. `seed_pool_ready` でSeed数確定
3. `stage1_running` で一次判定をSeedごとに進行
4. `stage2_running` で最終再判定を実行
5. `pass_completed` → `completed` で終了

## バックエンド進捗の主要遷移
| sec | status | phase | progress% | pass | created | seed_count | message |
|---:|---|---|---:|---|---:|---:|---|

## UIゲージ遷移（表示の変化）
| sec | ラベル | 表示% | ヘッドライン | 詳細 |
|---:|---|---|---|---|

## 結果の読解ポイント
- `一次通過`: `0`
- `最終再判定`: `0`
- `除外(低利益)`: `0`
- `除外(一致不足)`: `0`
- `除外(低粗利率)`: `0`
- `除外(低流動性)`: `0`

## 補足
- hints:
- 実行後 pending total: `0`

## 参照ファイル
- 完全版レポート: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/cycle_diagnostics/miner_flow_capture_20260223T213033Z.raw.md`
- result json: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/cycle_diagnostics/miner_flow_capture_20260223T213033Z.result.json`
- ui jsonl: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/cycle_diagnostics/miner_flow_capture_20260223T213033Z.ui.jsonl`
- backend jsonl: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/cycle_diagnostics/miner_flow_capture_20260223T213033Z.backend.jsonl`

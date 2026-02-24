# Miner探索 実測レポート（可読版）: 20260224T001718Z

## 実行サマリ
- カテゴリ: `watch`
- 実行時間: `14627 ms`
- 開始(UTC): `2026-02-24T00:17:24.596000+00:00`
- 終了(UTC): `2026-02-24T00:17:39.223000+00:00`
- 作成候補数: `0`
- 停止理由: `seed_batch_completed`
- 実行パス数: `20`
- Seed数: `20`

## フロー概要（バックエンド）
1. `timed_fetch_start` で探索開始
2. `seed_pool_ready` でSeed数確定
3. `stage1_running` で一次判定をSeedごとに進行
4. `stage2_running` で最終再判定を実行
5. `pass_completed` → `completed` で終了

## バックエンド進捗の主要遷移
| sec | status | phase | progress% | pass | created | seed_count | message |
|---:|---|---|---:|---|---:|---:|---|
| 2.104 | running | timed_fetch_start | 5 | 0/20 | 0 | 0 | 腕時計のSeedプールを確認しています |
| 7.564 | running | stage1_running | 26.4 | 5/20 | 0 | 20 | 一次判定 5/20: GW-M5610U-1JF NEW |
| 7.852 | running | stage1_running | 62.4 | 15/20 | 0 | 20 | 一次判定 15/20: SPB143 |
| 8.143 | completed | completed | 100 | 20/20 | 0 | 20 | 探索完了: 候補 0 件 |

## UIゲージ遷移（表示の変化）
| sec | ラベル | 表示% | ヘッドライン | 詳細 |
|---:|---|---|---|---|
| 2.104 | C: eBay最終再判定 / 準備中 (20/20) | 3% | 探索中 3% /  20/20 | 段階:C: eBay最終再判定 / 探索語:watch / 除外トップ:曖昧型番タイトル 165件 / 経過:330s |
| 3.153 | A: Seed補充 / 探索開始 | 5% | 探索中 5% | 段階:A: Seed補充 / 探索語:watch / 経過:1s |
| 3.931 | A: Seed補充 / 探索開始 | 6% | 探索中 6% | 段階:A: Seed補充 / 探索語:watch / 経過:2s |
| 4.705 | A: Seed補充 / 探索開始 | 8% | 探索中 8% | 段階:A: Seed補充 / 探索語:watch / 経過:3s |
| 5.745 | A: Seed補充 / 探索開始 | 10% | 探索中 10% | 段階:A: Seed補充 / 探索語:watch / 経過:4s |
| 6.525 | A: Seed補充 / 探索開始 | 11% | 探索中 11% | 段階:A: Seed補充 / 探索語:watch / 経過:4s |
| 7.308 | A: Seed補充 / 探索開始 | 13% | 探索中 13% | 段階:A: Seed補充 / 探索語:watch / 経過:5s |
| 8.143 | C: eBay最終再判定 / 結果集計中 | 96% | 今回の探索: 追加 0件 / 探索完走 | 段階:C: eBay最終再判定 |
| 11.265 | C: eBay最終再判定 / 完了 | 100% | 今回の探索: 追加 0件 / 探索完走 | 段階:C: eBay最終再判定 / 更新:0s |

## 結果の読解ポイント
- `一次通過`: `0`
- `最終再判定`: `0`
- `除外(低利益)`: `0`
- `除外(一致不足)`: `0`
- `除外(低粗利率)`: `0`
- `除外(低流動性)`: `0`

## 補足
- hints:
  - 腕時計は深掘り上限に到達したため、2026-03-02T23:55:37Z まで補充を停止しています。
  - 一致不足の主因: mod_conflict
  - 複数型番が列挙された曖昧タイトルを除外しました。
  - 90日売却流動性の閾値で除外されています。回転率の高い型番で再検索してください。
  - 否認済みブロックに該当しています。別商品の検索を推奨します。
  - 一致不足の主因: model_code_conflict
  - 複数型番が列挙された曖昧タイトルを除外しました。
  - 否認済みブロックに該当しています。別商品の検索を推奨します。
  - 複数型番が列挙された曖昧タイトルを除外しました。
  - この検索ワードは現在設定の探索範囲を完走済みです。
  - 複数型番が列挙された曖昧タイトルを除外しました。
  - 一致不足の主因: model_code_conflict
  - 複数型番が列挙された曖昧タイトルを除外しました。
  - 90日売却流動性の閾値で除外されています。回転率の高い型番で再検索してください。
  - 否認済みブロックに該当しています。別商品の検索を推奨します。
  - 複数型番が列挙された曖昧タイトルを除外しました。
  - 90日売却済み商品の参照URL/画像が取得できない候補を除外しました。
  - 一致不足の主因: mod_conflict
  - 複数型番が列挙された曖昧タイトルを除外しました。
  - 90日売却済み商品の参照URL/画像が取得できない候補を除外しました。
- 実行後 pending total: `0`

## 参照ファイル
- 完全版レポート: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/cycle_diagnostics/miner_flow_capture_20260224T001718Z.raw.md`
- result json: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/cycle_diagnostics/miner_flow_capture_20260224T001718Z.result.json`
- ui jsonl: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/cycle_diagnostics/miner_flow_capture_20260224T001718Z.ui.jsonl`
- backend jsonl: `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/cycle_diagnostics/miner_flow_capture_20260224T001718Z.backend.jsonl`

# Cycle Bottleneck Report (2026-02-20)

## 対象
- `seiko sbdc101 watch`
- `citizen nb1050 watch`
- `casio gwm5610 watch`

## 実装反映
- 型番クエリは毎回 narrow 先頭から探索（`REVIEW_FETCH_FORCE_EXACT_FOR_MODEL_QUERY=1`）
- 型番表記ゆれ（`SBDC-101` / `SBDC101`）を正規化一致として判定
- `review_cycle_report` に `low_match_reason_counts` / `low_match_samples` を追加
- 複数型番列挙タイトルを除外（`skipped_ambiguous_model_title`）
- `run_autonomous_cycle.py` に historical-skip 制御オプションを追加

## 診断結果（1ラウンド, target=12）
| query | created | skipped_duplicates | skipped_low_match | skipped_ambiguous_model_title | skipped_liquidity_unavailable | 主因 |
|---|---:|---:|---:|---:|---:|---|
| seiko sbdc101 watch | 0 | 5 | 2 | 0 | 0 | 既存重複 + 部分型番一致 |
| citizen nb1050 watch | 0 | 37 | 5 | 0 | 38 | 流動性未取得 + 既存重複 |
| casio gwm5610 watch | 0 | 9 | 36 | 25 | 0 | 色バリアント不一致 + 曖昧型番列挙 |

## 主要ボトルネック
1. **既存重複の比率が高い**（新規性不足）
2. **流動性未取得**（特に `citizen nb1050 watch`）
3. **色バリアント不一致**（`variant_color_missing_market`）
4. **複数型番列挙タイトル**（曖昧ソース）

## 次アクション（完成に向けた優先順）
1. RPA流動性の欠損埋めを「query単位」だけでなく「モデルコード単位」で再収集する
2. 既存重複が多いクエリは自動で探索を浅く止め、未探索シリーズへ枠を回す
3. `variant_color_missing_market` のうち、同一モデルコード強一致時は再評価キューに回す（即除外しない）
4. 24件達成用にクエリセットを「未重複シリーズ中心」に再編成し、guarded 連続運転へ移行

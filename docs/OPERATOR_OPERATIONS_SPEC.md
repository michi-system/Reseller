# Operator運用仕様 v1

最終更新: 2026-02-22 (JST)

この文書は、Resellerにおける Operator の運用仕様を定義する。
対象読者は `michi-system` (全体リード/ Miner) と `a28ngi` (Operator担当)。

## 1. 役割定義
- Operatorは「承認済み商品の出品実行」と「出品後の利益保全」を担う。
- 具体的には次を実施する:
  - Miner承認済みデータの取り込み
  - eBayへの自動出品
  - 定期監視（価格・在庫・利益）
  - 停止/要確認の自動判定
  - 手動介入を含む履歴管理とUI可視化

## 2. 非対象
- 新規商品の発掘
- 同一商品マッチングの新規実装（ただしロジック共有は実施）
- 最終承認判断（人間レビュー）

## 3. 連携方針（MVP）
- 橋渡しは `data/approved_listing_exports/latest.jsonl` を一次ソースにする。
- Operatorは取り込み時に `approved_id` で冪等化し、二重取り込みを防止する。
- 将来の共有DB移行は別判断とし、MVPでは実施しない。

## 4. 出品方針
### 4.1 自動出品
- eBay Inventory API を使用して出品する。
- 基本フロー:
  1. `createOffer`
  2. `publishOffer`
- まずは `dry-run` モードを優先し、検証後に本番 publish を有効化する。

### 4.2 「Sell Similar」相当
- 公開APIに「Sell Similar」の専用エンドポイントは前提にしない。
- 代替として以下で疑似再現する:
  - 過去売却データからテンプレを生成
  - 同一商品判定ロジック（Miner共有）で不一致を除外
  - 説明文/画像は自前作成を原則とする

## 5. 監視仕様（2レイヤー）
### 5.1 軽量監視（高頻度）
- 目的: 在庫切れ・仕入れ価格急変・利益割れを早期検知
- 監視対象:
  - 仕入れ側: 在庫/価格
  - 出品側: 出品状態/価格
- 初期頻度:
  - 新規出品から72時間: 6時間ごと
  - 安定出品: 24時間ごと
  - 停止中: 72時間ごと（再開候補の確認のみ）

### 5.2 重量監視（低頻度）
- 目的: 市況変動の再評価（売却最安値の再取得）
- 実行頻度: 7日ごと
- 対象: 出品中 + 停止中のうち再開候補

## 6. 判定仕様（MVP）
### 6.1 即停止（auto_stop）
- 仕入れ在庫なしが連続2回
- 想定利益率が `min_profit_rate` 未満かつ連続2回
- 想定利益額が `min_profit_jpy` 未満かつ連続2回

### 6.2 要確認（alert_review）
- 7日再評価で売却最安値が急落
- 利益率は閾値近傍でブレており、誤停止リスクが高い
- 同一商品一致スコアが境界値近傍

### 6.3 再開
- MVPでは「自動再開しない」。
- 手動再開のみ許可し、再開理由を必須ログ化する。

## 7. API利用枠の運用値（初期）
### 7.1 eBay API既定上限（公開値）
- Inventory API: 2,000,000 calls/day
- Trading API: 5,000 calls/day

### 7.2 内部予算（安全側）
- 日次の内部利用上限を次に固定する:
  - Inventory API: 40,000 calls/day（既定上限の2%）
  - Trading API: 1,000 calls/day（既定上限の20%）
- 理由:
  - 障害時の再試行・突発対応の余白を残す
  - Miner側のAPI消費に干渉しない

### 7.3 監視頻度からの概算（500商品例）
- 仮定:
  - 新規100件（6h）
  - 安定300件（24h）
  - 停止100件（72h）
- 軽量監視コール概算:
  - `2 * (100*4 + 300*1 + 100*(1/3)) = 約1466 calls/day`
- 重量監視（7日再評価）概算:
  - `400/7 = 約57 calls/day`
- 合計:
  - `約1523 calls/day`（安全係数3倍でも約4569/day）

## 8. UI要件（Operator）
- 画面:
  1. 取込キュー
  2. 出品中
  3. 要確認
  4. 停止中
  5. イベント履歴
  6. 設定（頻度/閾値）
- 必須操作:
  - 手動停止
  - 手動再開
  - 理由付き無視
  - 監視頻度変更
- 必須可視化:
  - 現在利益
  - 停止理由
  - 最終監視時刻
  - 次回監視予定

## 9. 監査ログ要件
- すべての自動判定に `reason_code` を残す。
- 停止/再開/手動介入は `actor` と `timestamp` を必須保存。
- API失敗は `job_run_id` 単位で集計し、日報へ反映する。

## 10. 初期担当
- `michi-system`: 全体リード + Miner/Shared判断 + 停止基準の最終決定
- `a28ngi`: Operator実装・UI実装・監視運用実装

## 11. 参照ソース（2026-02-22確認）
- eBay API call limits:
  - https://developer.ebay.com/develop/get-started/api-call-limits
- Inventory API:
  - https://developer.ebay.com/api-docs/sell/inventory/resources/offer/methods/createOffer
  - https://developer.ebay.com/api-docs/sell/inventory/resources/offer/methods/publishOffer
- Rate limit監視API:
  - https://developer.ebay.com/api-docs/developer/analytics/static/overview.html
- Relist系:
  - https://developer.ebay.com/Devzone/XML/docs/reference/ebay/RelistFixedPriceItem.html
- ポリシー:
  - https://www.ebay.com/help/duplicate-listings-policy/policies/duplicate-listings-policy?id=4255
  - https://www.ebay.com/sellercenter/resources/intellectual-property

## 12. 実装済みMVPコマンド（2026-02-22）
```bash
# 1) DB初期化（config seed含む）
python3 scripts/operator_init_db.py

# 2) Miner承認JSONL取り込み
python3 scripts/operator_ingest_approved.py --input-path data/approved_listing_exports/latest.jsonl

# 3) 出品サイクル（既定: dry-run）
python3 scripts/operator_run_listing_cycle.py --limit 20

# 4) 監視サイクル（軽量）
python3 scripts/operator_run_monitor_cycle.py --check-type light --limit 300

# 5) 監視サイクル（重量）
python3 scripts/operator_run_monitor_cycle.py --check-type heavy --observation-jsonl path/to/obs.jsonl

# 6) 閾値/頻度設定を新バージョンで反映
python3 scripts/operator_set_config.py --min-profit-jpy 2000 --min-profit-rate 0.1

# 7) 現在の状態確認
python3 scripts/operator_status_summary.py

# 8) 監視入力テンプレを出力（手入力/外部取得連携の土台）
python3 scripts/operator_export_observation_targets.py --check-type light
```

## 13. 実装済みUI/API補足（2026-02-22）
- Operator専用UI: `http://127.0.0.1:8000/operator`
- 実装済み手動操作:
  - `manual-stop`: 手動停止
  - `manual-alert`: 要確認化
  - `manual-keep-listed`: 出品継続判定
  - `manual-resume-ready`: 再出品キュー（ready）へ戻す
- 運用上の扱い:
  - 停止済みの自動再開は引き続き無効（MVP方針維持）
  - 再開時は `ready` へ戻し、Listing Cycleで再出品する
  - すべて `listing_events` に `actor_id/reason_code/note` を保存

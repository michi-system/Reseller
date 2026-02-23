# Miner 探索フロー（探索開始 → レビュー待ち一覧）

## 1. CUI風フロー全体

```text
[USER]
  |
  | click: 探索開始
  v
[WEB: miner.js]
  |
  | POST /v1/miner/fetch
  v
[API: /v1/miner/fetch]
  |
  |-- 進捗を running に更新
  |
  |-- [STEP A] seed_pool 整理
  |     |
  |     |-- 期限切れ seed を削除（expires_at < now）
  |     `-- 有効 seed のみ残す
  |
  |-- [STEP B] 補充判定
  |     |
  |     |-- if usable_seed > 補充しきい値
  |     |      `-- 補充スキップ
  |     |
  |     `-- if usable_seed <= 補充しきい値
  |            |
  |            |-- Product Research 補充開始
  |            |    条件: 90日Sold / 新品 / Fixed Price
  |            |
  |            |-- page=1 (offset=0, limit=50)
  |            |-- タイトル群 -> seed抽出 -> 正規化 -> 重複除去
  |            |-- seed_pool に追加（expires_at=+7日）
  |            |
  |            |-- if 新規seed >= 80件 (目標100の80%)
  |            |      `-- 補充終了
  |            |
  |            |-- else if page < 10
  |            |      `-- 次ページへ（offset += 50）
  |            |
  |            `-- else (page==10)
  |                   `-- 補充打ち切り + 「再実行推奨」通知
  |
  |-- [STEP C] 探索バッチ作成
  |     |
  |     `-- 有効 seed から 20件選定
  |
  |-- [STEP D] 20 seed を探索実行
  |     |
  |     |-- Rakuten 検索
  |     |-- Yahoo 検索
  |     |-- eBay側情報とマッチング
  |     |-- 利益/流動性/EV90 など判定
  |     `-- 通過候補だけ DB に pending で保存
  |
  |-- [STEP E] レスポンス返却
  |     |
  |     |-- created_count / skipped内訳 / 補充情報 / 通知
  |     `-- 進捗を completed に更新
  v
[WEB: miner.js]
  |
  | GET /v1/miner/queue?status=pending...
  v
[UI]
  |
  `-- レビュー待ち一覧に反映
```

## 2. 分岐詳細（補充ロジック）

```text
[補充開始]
  |
  | target_new_seed = 100
  | soft_target     = 80   (80%)
  | max_pages       = 10
  | limit_per_page  = 50
  |
  v
for page in 1..10:
  1) Product Research を実行
  2) タイトルから seed 抽出
  3) 重複除去
     - 既存の「有効 seed_pool」
     - 今回補充ジョブで既に追加済みの seed
  4) 新規 seed を保存（expires_at = now + 7日）
  5) if new_seed_count >= 80:
       break

if new_seed_count < 80 after page 10:
  - 補充終了（未達）
  - カテゴリ再実行推奨通知を返す
```

## 3. 重要ルール

- `既存プール` の定義は **有効期限内 seed のみ**。
- 期限切れ seed は探索前に削除（または無効化）する。
- 重複判定に期限切れ seed は使わない。
- 1探索あたりの実行 seed 数は `20` 固定。
- 補充検索の最大順位は `500位`（50件 × 10ページ）。

## 4. 探索開始からレビュー待ち反映までのI/O

```text
INPUT
  - category_key (例: watch)
  - 探索パラメータ（min_match_score, min_profit, min_margin など）

PROCESS
  - seed_pool 整理
  - 必要時 Product Research 補充
  - seed 20件で Rakuten/Yahoo 探索
  - 比較/判定
  - pending 保存

OUTPUT
  - fetch result payload
    - created_count
    - created_ids
    - skipped_* counters
    - 補充関連情報
    - 通知（再実行推奨など）
  - UI の pending 一覧更新
```


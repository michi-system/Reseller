# Phase A（seed補充）わかりやすい解説

最終更新: 2026-02-25

このドキュメントは、Miner の `Phase A` を「何をしているか」「どこを見れば正常か」を日本語で短く把握するための実務ガイドです。  
正本仕様は `docs/MINER_SPEC.md`、閾値の正本は `docs/OPERATION_POLICY.json` です。

## 1. Phase A とは
- 役割: `seed`（次の探索に使う検索語）を eBay Product Research から補充する段階。
- 目的: B/C段階で回すための「売れている型番候補」を安定して増やすこと。
- 発火条件: seedプールの有効件数がしきい値以下になったとき。

## 2. 何を入力して、何を出すか
- 入力:
  - カテゴリ（例: `watch`）
  - カテゴリに紐づく big word（例: `G-SHOCK`, `Prospex`）
  - Product Research の固定条件（Sold / Last 90 days / New / Fixed price / min price / 新しい順）
- 出力:
  - `miner_seed_pool` に seed 行を追加
  - `miner_seed_refill_pages` にページ取得履歴を保存
  - `refill` サマリ（件数、停止理由、診断情報）

## 3. Phase A の処理フロー
1. 事前に準備したカテゴリの big word リストを読み込む（重複は正規化して除外）。
2. 各 big word で Product Research を検索する。
3. 一覧行から `title`, `sold_price`, `item_url`, `item_id` を取り出す。
4. タイトルから seed 候補（型番中心）を抽出する。
5. 重複やノイズ（アクセサリ、価格下限未満、cooldown中）を除外する。
6. 残った seed を DB に保存する。

注記:
- big word の「作成・見直し」は運用前工程（Codex + Web調査）で行う。
- Phase A 実行時は、ローカルのカテゴリ知識に保存済みの big word を使用する。

## 4. ビッグワードとページの考え方（重要）
Phase A は「毎回全ページ」を掘るのではなく、big word ごとにページ上限を動的に決めます。

- 基本式:
  - `unlocked_pages = floor(経過秒 / (hours_per_page * 3600))`
  - ただし `min_pages` と `max_pages` で上下限をかける
  - 履歴がない初回は `initial_pages` を使う
- 意味:
  - 前回探索から時間が経っていない big word は浅く探索
  - 時間が経った big word は深いページまで許可
  - 重複回収を抑えて PR 消費効率を上げる

### 既定値
- `MINER_STAGEA_QUERY_PAGE_UNLOCK_ENABLED=1`
- `MINER_STAGEA_QUERY_PAGE_UNLOCK_HOURS_DEFAULT=24`
- `MINER_STAGEA_QUERY_PAGE_UNLOCK_MIN_PAGES=1`
- `MINER_STAGEA_QUERY_PAGE_UNLOCK_INITIAL_PAGES`（未設定時は `max_pages`）
- `MINER_STAGEA_FALLBACK_ON_PAGE_UNLOCK_WAIT=1`（`page_unlock_wait` 時に他カテゴリを回す）
- `MINER_STAGEA_FALLBACK_MAX_CATEGORIES=8`（1回のフォールバック上限カテゴリ数）

### big word ごとの上書き
- `MINER_STAGEA_QUERY_PAGE_UNLOCK_HOURS_JSON` に JSON で指定
- 例:
```json
{
  "g-shock": 12,
  "prospex": 24,
  "watch:promaster": 18
}
```
- キー優先順位:
  - `category_key:query_key`（例 `watch:promaster`）
  - `query_key`（例 `promaster`）
  - default

## 5. 停止理由の読み方（Phase A）
- `target_reached`: 目標seed件数に到達
- `partial_refill`: 追加はあったが目標未達
- `all_big_words_exhausted`: 全big wordを回したが増分が少ない
- `fresh_window_skip`: 履歴が新しすぎてページ取得をスキップ
- `page_unlock_wait`: ページ解放待ち（経過時間不足）
- `daily_limit_reached`: RPA/PRの日次上限
- `rpa_timeout_guard`: タイムアウト多発で安全停止
- `target_reached_with_fallback`: 主カテゴリが `page_unlock_wait` だったため他カテゴリを順次補充し、合計新規seedが目標件数に到達

### `page_unlock_wait` のときの動作
- 主カテゴリが `page_unlock_wait` になった場合、設定が有効なら他カテゴリへ順に移る。
- 各カテゴリで追加された `added_count` を合算し、合計が目標（既定100）に達したらA段階を終了する。
- 日次上限に到達した場合はその時点で停止する。

## 6. 腕時計カテゴリでの成功条件
Phase A 完了判定は、`docs/MINER_SPEC.md` の受け入れ条件に従います。

1. 条件設定が再現できる  
2. 1ページ50件のタイトル/価格を取得できる  
3. ページ送りで一覧が切り替わる  
4. seed最小情報（title/sold_price/item_url/item_id）を保存できる  
5. 重複処理が効く  

## 7. 実運用でまず見る場所
- 実装:
  - `reselling/miner_seed_pool.py`
  - `scripts/rpa_market_research.py`
- 正本:
  - `docs/MINER_SPEC.md`
  - `docs/OPERATION_POLICY.json`
- テスト:
  - `tests/test_miner_seed_pool.py`
  - `tests/test_rpa_product_research_filter.py`

## 8. すぐ使える確認コマンド
```bash
python3 -m unittest -q tests.test_miner_seed_pool tests.test_rpa_product_research_filter tests.test_big_word_normalization
```

実行後は `seed_pool.refill.reason` と `diagnostics` を見て、  
「条件不整合で止まっていないか」「重複だけ拾っていないか」を確認します。

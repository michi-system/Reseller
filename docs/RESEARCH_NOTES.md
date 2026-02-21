# Web調査レポート（追加10分・公式+コミュニティ均衡）

## 0. 実施ログ
- 開始: 2026-02-21 06:03:31 JST
- 終了: 2026-02-21 06:13:43 JST
- 実測: 約10分12秒
- 目的:
  - Web探索だけでどこまで「学習データ化」できるかを再評価
  - 季節性を判定ロジックに実装可能な形で紐づける
  - 古い情報の混入を防ぐため、公式の最新仕様とコミュニティ実務知見を分離して扱う

## 1. 結論（先に）
- Web探索だけでも、以下は高確度で作れる。
  1. カテゴリ/ブランド/シリーズの探索優先度
  2. 季節性タグ（いつ強いか）の事前重み
  3. API節約のための取得順序と停止条件
- ただし、最終の可否判定（本当に売れる/利益が残る）は、Web由来の一般知見だけでは不足。
  - 90日実売（件数・価格）
  - 自前レビューの否認理由
  - 直近在庫/送料/手数料
  を必ず組み合わせる必要がある。

## 2. 公式ソースで確定した事項（運用に直結）

### 2.1 YahooショッピングAPI
- `ItemSearch` は `condition`（new/used）や `availability`（在庫）で絞り込み可能。
- `results` は 1-100。
- `start + results <= 1000` の制約がある。
- レート制御は `Client ID` 単位で `1秒1リクエスト`（超過で制限対象）。

示唆:
- 1クエリで最大100件を取りつつ、ページングは進捗連動で実行。
- 同義語で無限掘りせず、`min_new_items` と重複率で停止させる。

### 2.2 YahooランキングAPIの扱い
- ショッピングWeb API `v2` の `categoryRanking` / `queryRanking` は終了済み（2023-11-30）。

示唆:
- Yahoo側はランキング前提での季節推定をやめる。
- 季節性は eBay実売 + 楽天ランキング + 自前実績で補完する。

### 2.3 Rakuten Web Service
- `Ichiba Item Search` で `availability=1`（在庫あり）や `condition=1`（新品）を使える。

示唆:
- デフォルトは在庫あり必須・新品必須。
- カスタムで外せるが、既定は品質優先。

### 2.4 eBay（季節性/市場トレンド）
- Terapeak Product Research は最大3年の販売データ分析に対応。
- eBay公式のWatchlist/トレンド系ページは「カテゴリ横断の需要変化」を定期配信。

示唆:
- 季節性の基準窓は `90日` と `365日` の二重管理。
- 90日を優先、365日は補助（同月比/季節要因の補正）に使う。

## 3. コミュニティ情報（採用ルール付き）

### 3.1 観測できた傾向
- eBay Community では「Terapeakと実売表示の見え方差」や「売れ行き評価の実務運用」が継続議論。
- Reddit（r/Flipping, r/eBay）でも、粗利より sell-through を優先する運用が反復して語られる。

### 3.2 取り込み方（重要）
- コミュニティ情報は `ハード判定` には使わない。
- 使うのは次のみ:
  1. 仮説生成（例: このカテゴリは年末強い）
  2. 閾値の初期値提案（例: STR最低ライン）
  3. 例外ルール候補（例: 付属品欠損の価格崩れ）
- 採用条件:
  - 公式仕様と矛盾しない
  - 直近90日の自前データで再現する

## 4. 季節性を実装へ落とす最小スキーマ

### 4.1 追加プロパティ
- `season_tag`:
  - `spring`, `summer`, `fall`, `holiday`, `back_to_school`, `tax_refund`
- `seasonality_index`:
  - カテゴリ×月の需要倍率（基準=1.0）
- `sold_count_90d`
- `min_sold_price_90d`
- `median_sold_price_90d`
- `freshness_days`（取得後経過日数）
- `data_source_tier`（official/community/internal）
- `confidence`（0-1）

### 4.2 判定ロジック（最小）
1. `liquidity_gate`: `sold_count_90d >= N` を満たさないものは除外
2. `price_gate`: `expected_profit_jpy > 0` かつ `safety_margin` を上回る
3. `season_gate`: `seasonality_index` が低すぎる場合はスコア減点
4. `freshness_gate`: 商品レベル情報がTTL超過なら再取得

## 5. API節約に効く探索設計（更新版）
1. 取得順序
- 広いカテゴリ語 -> メーカー -> シリーズ -> 型番 の段階展開
- 各段で `new_unique_rate` を測り、低下したら打ち切り

2. クエリ継続条件
- 継続: `min_new_items` を満たす
- 停止: 重複率高止まり + 低収率が連続

3. 重複節約
- `site + item_id` の厳密重複排除
- 同一商品指紋（brand/model/variant）をキャッシュして再照会を抑制

4. 評価母集団の安定化
- 1周の評価対象数を固定（例: 24件）
- 分母が足りない場合は「探索不足」と明示（完走扱いにしない）

## 6. 次の10分でやる価値が高い調査
1. カテゴリ別の季節イベント辞書（US）を作る
2. 返品理由の上位パターン（色違い/サイズ違い/付属欠品）をカテゴリ別に整理
3. 90日売却件数と利益率の相関を、既存DBでカテゴリ別に可視化

## 7. 参照リンク

### 公式
- Yahoo ItemSearch v3
  - https://developer.yahoo.co.jp/webapi/shopping/v3/itemsearch.html
- Yahoo 利用制限（FAQ）
  - https://developer.yahoo.co.jp/faq/
- Yahoo Shopping API v2 終了告知
  - https://developer.yahoo.co.jp/changelog/v2/2023-09-13-shoppin-api-v2-closed.html
- Rakuten Ichiba Item Search
  - https://webservice.rakuten.co.jp/documentation/ichiba-item-search
- Rakuten Ichiba Item Ranking
  - https://webservice.rakuten.co.jp/documentation/ichiba-item-ranking
- eBay Terapeak Product Research
  - https://www.ebay.com/help/selling/selling-tools/terapeak-product-research?id=4853
- eBay Seller Center Watchlist
  - https://www.ebay.com/sellercenter/growth/ebay-watchlist-report
- eBay 2025 Product Trends
  - https://www.ebay.com/sellercenter/resources/seller-updates/2025-product-trends
- Shopify（米国向け販促カレンダー）
  - https://www.shopify.com/blog/sales-events-calendar
- NRF（米国小売トレンド）
  - https://nrf.com/

### コミュニティ
- eBay Community（Terapeak関連ディスカッション）
  - https://community.ebay.com/t5/Selling/What-happened-to-sales-data-in-the-Product-Research-tab/td-p/35079571
- eBay Community（Sold-through議論）
  - https://community.ebay.com/t5/Selling/sold-through-rates/td-p/34796544
- Reddit r/Flipping（90日STR議論）
  - https://www.reddit.com/r/Flipping/comments/1nxj5ft/what_does_your_90_day_sell_through_rate_look_like/
- Reddit r/eBay（販売評価系ディスカッション）
  - https://www.reddit.com/r/Ebay/comments/1l14f4w/what_should_my_90day_total_sales_be_for_a_newer/

## 8. 注意（鮮度）
- 商品レベル情報は陳腐化が早い。TTLを短く運用する（7-30日）。
- カテゴリ傾向は比較的安定だが、年次で変動するため四半期ごとに再学習する。
- 「コミュニティで見た」は根拠階層を下げ、最終判定に直結させない。

# ebayminer 最強化レポート（自由調査版 / 2026-02-19）

## 0. この文書の目的
- せどり一般の実務知見と、eBay/Yahoo/Rakutenの最新仕様を合わせて、プロジェクト全体を「売れる・守れる・回る」形に再設計する。
- いま話している論点（90日売却）を中核に置きつつ、見落とすと負ける要素を網羅する。
- API上限がある前提で、検索効率とレビュー効率を同時最大化する。

## 1. 先に結論（実装方針）
1. 目標関数を `期待利益` から `EV90` に変更する。  
`EV90 = P(90日内売却) * 純利益 - (1-P) * 在庫拘束コスト - リスク期待損失`
2. フローを「利益先行」から「需要先行（流動性先行）」に切り替える。  
先に売れる証拠を取れない候補には、国内APIコストを使わない。
3. API最適化は「大量取得」か「小分け取得」かの二択ではなく、`段階探索 + キャッシュ + 重複排除 + 停止条件` の組み合わせで行う。
4. 自動改善サイクルは `前提検証フェーズ` を毎周必須化し、2周目以降の隠れ失敗を防ぐ。

## 2. Web調査で確認した事実（一次情報中心）
### 2.1 eBay（API・販売運用）
- Finding API は `2025-02-04` に decommission、Browse API への移行が前提。
- Browse search は `limit<=200`、`offset<=10000`。
- eBay Buy APIs（Browse等）はデフォルトで `5,000 calls/day`（Application Growth Check前）。
- Marketplace Insights API は公開情報上 `Limited Release` 扱い。
- Terapeak Product Research は最大3年の履歴分析に対応（季節性や需要確認の基礎になる）。
- Product identifiers（UPC/EAN/GTIN/MPN）は可視性改善に重要と明示。

### 2.2 YahooショッピングAPI
- 商品検索v3で `condition`（`new`/`used`）と `in_stock`（在庫有無）を指定可能。
- `start` と `results` の合計値に上限（`start + results <= 1000`）。
- `results` は `1-100`。
- 同一URLへの短時間大量アクセスは `429` や利用制限の要因になる旨の注意がある。
- 基本制限として `1クエリ/秒` の記載あり。

### 2.3 楽天API（Ichiba Item Search）
- `hits` は最大30、`page` は最大100。
- `availability=1`（在庫あり）や `condition=1`（新品）で絞り込み可能。
- 同一URLへの短時間大量アクセスで一定時間利用不能になる注意が明記。

### 2.4 越境物流・規制
- 日本郵便の米国向け案内で、`2025-08-29` 以降の通関制度変更（de minimis優遇の終了）に伴う申告要件強化が案内されている。
- eBayはProhibited/Restricted Items、VeRO（知財）を明示しており、カテゴリによっては利益以前に出品継続リスクが高い。

### 2.5 同様の運用者ケース（参考）
- コミュニティ上では、単純粗利より `sell-through（回転）` を主要判断にしている運用者が多い。
- 実務では「利幅があるのに売れ残る」ケースが最も資金効率を落とすという報告が繰り返し見られる。
- コミュニティ情報はノイズがあるため、モデルには直接入れず、閾値仮説の初期値に限定して使うのが安全。

## 3. 90日売却以外で、見落とすと負ける論点
1. 規約リスク  
売れてもVeROや禁止カテゴリでアカウント健全性を損なうと長期EVが崩壊する。
2. 返品率リスク  
同一商品でもバリエーション違い、型番サフィックス違いで返品率が跳ねる。
3. 物流リスク  
配送遅延・関税説明不足・輸入要件の変更は評価悪化に直結。
4. 資金繰りリスク  
入金タイムラグと在庫拘束の二重負担で、回っていてもキャッシュが詰まる。
5. 価格改定リスク  
アクティブ価格参照のみだと、成約価格との乖離で見込み利益が崩れる。
6. データ品質リスク  
「新品ではありません」の否定文を新品誤判定すると精度が下がる。

## 4. 最強化アーキテクチャ（需要先行）
### Gate 0: Demand/Liquidity Gate（最優先）
- キーは `JAN/UPC/MPN/型番` を第一にする。
- 指標は `sold_90d_count`、`active_count`、`sell_through_90d`、`sold_price_median`。
- 初期閾値案  
`sold_90d_count >= 3`  
`sell_through_90d >= 0.15`
- ここを通過しない候補は国内検索を打たない。

### Gate 1: JP Sourcing（Yahoo/Rakuten/Amazon）
- Gate 0通過商品のみ国内探索。
- `condition=new`、`availability=1` 相当のAPIフィルタを必須化。
- 商品説明文は新品判定に使わず、タイトルと条件属性を主に使う。

### Gate 2: Identity Matching（同一商品判定）
- strict一致を維持。  
`ブランド + 型番 + 仕様キー（色/サイズ/容量）` を必須要素にする。
- 汎用トークンのみ一致（例: `2100`）はreject。
- 付属品、ジャンク、部品取り語彙は強制reject。

### Gate 3: Unit Economics（純利益）
- 成約側価格を優先（アクティブ価格は補助）。
- FXはリアルタイム取得を主、失敗時はキャッシュと安全マージンで保守化。
- 利益は `USD` と `JPY` の両表示を標準化。

### Gate 4: Risk/Compliance
- VeRO関連ブランド、禁制品、返品多発パターンをスコア化して減点。
- 国際配送可否と関税説明リスクを候補ごとにタグ付け。

### Gate 5: Human Final Review
- 自動承認済み候補のみ最終確認。
- UIには `EV90`、`売却件数`、`STR`、`リスクタグ`、`利益(JPY/USD)` を表示。
- 否認は「指摘箇所選択のみ」で送信可能（自由記述は任意）。

## 5. API節約とヒット最大化を両立する検索設計
### 5.1 基本戦略
1. 段階探索  
`Exact ID` -> `ブランド+型番` -> `近傍表記` の順で広げる。
2. 重複回避  
`source + item_id + normalized_title + price + condition` で重複キーを作り、再取得を抑止。
3. クエリキャッシュ  
`marketplace + normalized_query + filters + page` をTTL管理。
4. 停止条件  
連続ゼロ進捗、重複率上昇、API残量低下で探索停止。

### 5.2 「100件一括 vs 20件x5回」の判断
- 一括はAPI効率は良いがノイズを抱え込みやすい。
- 小分けは調整しやすいが、同一URLや類似クエリの連打制限に触れやすい。
- 実務最適は `中間`。  
`20-40件単位 + quality check + 継続可否判定` が、精度と節約のバランスが高い。

### 5.3 サイト別推奨
- eBay  
Demand先行。売却履歴データ（Insights/Terapeak）を先に取り、通過商品のみBrowseで補完。
- Yahoo  
`condition=new`、`in_stock=true`、`results`を中量で回し、`start+results<=1000` を遵守。
- Rakuten  
`availability=1` と `condition=1` を固定。`hits<=30` 前提でページ深掘りは進捗連動で実施。

## 6. 自動改善サイクル（失敗を翌周へ持ち越さない）
1. Preflight（新設必須）  
APIキー疎通、日次残量、DB書込、否認理由保存、レビュー遷移を最初に検査。
2. Harvest  
Gate 0 -> Gate 4までで候補生成。
3. Auto Review  
高信頼のみ自動承認、境界値は人手へ。
4. Human Review  
最終承認/否認を収集。
5. Learn  
否認理由をルールと重みへ反映。
6. Close Report  
「失敗なしで1周完了」を明示し、未完了なら次周開始禁止。

運用単位は `1周=24件レビュー完了` を基準にしつつ、母数不足時はタイムアウト終了（例: 24時間）を併用する。

## 7. 実装優先順位（この順で着手）
1. Gate 0の本実装（Demand/Liquidity DB + スコア算出）
2. eBay側を「売却履歴優先」の取得経路へ変更
3. Yahoo/Rakutenの新品・在庫フィルタ強制化と中古語彙reject強化
4. Query重複回避（指紋キャッシュ + item重複抑止）
5. Preflightフェーズを自動サイクルに統合
6. Review UIに `EV90` と `Liquidity` を追加
7. モデル改善のA/B運用（閾値と重みの比較）

## 8. KPI（勝っているかを判定する指標）
- `EV90 > 0` の候補率
- 90日内売却率
- API 1,000call あたりの有効候補数
- 自動承認 -> 最終否認率
- 在庫回転日数（DOH）
- 返品率・キャンセル率・規約違反率

## 9. 参考リンク
### 公式（一次）
- eBay API deprecation status  
https://developer.ebay.com/develop/get-started/api-deprecation-status
- eBay Browse API search  
https://developer.ebay.com/api-docs/buy/browse/resources/item_summary/methods/search
- eBay API call limits  
https://developer.ebay.com/develop/get-started/api-call-limits
- eBay Marketplace Insights API overview  
https://www.edp.ebay.com/api-docs/buy/marketplace-insights/static/overview.html
- eBay Terapeak Product Research  
https://www.ebay.com/help/selling/selling-tools/terapeak-product-research?id=4853
- eBay Product identifiers  
https://www.ebay.com/sellercenter/listings/product-identifiers
- eBay Prohibited & restricted items  
https://www.ebay.com/help/policies/prohibited-restricted-items/prohibited-restricted-items?id=4207
- eBay VeRO program  
https://www.ebay.com/help/policies/listing-policies/verified-rights-owner-program?id=4349
- Yahoo 商品検索API（v3）  
https://developer.yahoo.co.jp/webapi/shopping/itemsearch/v3/itemsearch.html
- Rakuten Ichiba Item Search  
https://webservice.rakuten.co.jp/documentation/ichiba-item-search
- Japan Post（米国向け通関制度変更案内）  
https://www.post.japanpost.jp/int/information/2025/0825_01_en.html

### 参考（コミュニティ・事例観測）
- Reddit r/Flipping（90日STR議論）  
https://www.reddit.com/r/Flipping/comments/1nxj5ft/what_does_your_90_day_sell_through_rate_look_like/
- Reddit r/Flipping（STR実務議論）  
https://www.reddit.com/r/Flipping/comments/1ewc1g3/sell_through_rates_what_are_they/
- eBay Community（売れ筋設計の実務相談）  
https://community.ebay.com/t5/Selling/90-day-total/td-p/35141495

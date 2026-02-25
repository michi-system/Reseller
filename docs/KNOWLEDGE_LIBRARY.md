# Category Knowledge Library (2026-02-21)

## 1. 現状評価
- 以前: 実運用シードは `watch` 中心で、カテゴリ横断ナレッジは未充足。
- 今回: 追加収集により、カテゴリ入力からメーカー/シリーズ/型番へ展開するための初期辞書を作成。
- 保存先:
  - `/Users/tadamichikimura/Downloads/dev-HQ/Reseller/data/category_knowledge_seeds_v1.json`

## 2. この辞書でできること
1. 一般カテゴリ名から、優先ブランド・シリーズ・型番候補へ段階展開。
2. 季節性タグを使ったクエリ優先度調整。
3. 収集すべきプロパティを統一し、レビュー判定までつなぐ。

実装連携（有効化済み）:
- `reselling/live_miner_fetch.py` で `query` がカテゴリ名と判定されたとき、
  `data/category_knowledge_seeds_v1.json` を参照してサイト別クエリへ自動展開する。
- 適用状況は API レスポンス `fetched.<site>.knowledge` と `hints` に出力される。

## 3. 収集カテゴリ（v1）
- `watch`
- `sneakers`
- `streetwear`
- `trading_cards`
- `toys_collectibles`
- `video_game_consoles`
- `camera_lenses`

## 4. 季節性の紐づけ方
- `holiday_peak`（10-12月）
- `back_to_school`（7-9月）
- `tax_refund_window`（1-4月）
- `set_release_peak`（カード新弾時期）
- `major_launch_window`（ハード/大型新製品時期）

運用ルール:
1. 季節タグ一致カテゴリは探索優先度を上げる。
2. 一致しないカテゴリでも除外はせず、`min_new_items` と重複率で自動停止。
3. モデル単位は入れ替わりが速いため短TTLで更新。

## 5. 鮮度ルール（古い情報回避）
- カテゴリ: 180日
- シリーズ: 90日
- 型番: 30日
- コミュニティ由来ヒント: 30日（ハード判定禁止）

## 6. 追加で紐づけるべきプロパティ（必須）
- 識別子: `jan/upc/ean/mpn/model_code`
- 商品状態: `condition`
- 在庫: `in_stock`
- 流動性: `sold_count_90d`, `sold_price_min_90d`, `sold_price_median_90d`
- 原価系: `source_price_jpy`, 送料、手数料、FX
- 鮮度: `freshness_days`
- リスク: 偽造/VeRO/付属欠品/中古混入

推奨追加（v1.1）:
- `compliance_risk_level`（prohibited/restricted該当可能性）
- `ip_risk_level`（VeRO/ブランド知財警戒）
- `counterfeit_risk_level`（高リスクブランドの真贋注意）
- `listing_format`（auction/fixed price）
- `return_policy_signal`（返品条件差異）

## 7. 注意
- コミュニティ情報は仮説生成のみ。
- レビュー待ち候補の採用条件は、既存ポリシー（新品・在庫あり・流動性あり・利益正）を維持。

## 8. 主要ソース（今回の追加探索）

### 公式・一次情報
- eBay Watches Authenticity Guarantee（対象ブランド/モデル）
  - https://www.ebay.com/e/fashion/watches-authenticity-guarantee
- eBay Sneakers Authenticity Guarantee（主要ブランド）
  - https://www.ebay.com/e/fashion/sneaker-authenticity-guarantee
- eBay Streetwear Authenticity Guarantee（主要ブランド）
  - https://www.ebay.com/e/fashion/streetwear-authenticity-guarantee
- eBay Trading Cards Authenticity Guarantee（TCG/スポーツカード）
  - https://www.ebay.com/e/collectibles/trading-cards
- eBay Seller Update 2025 Holiday shipping prep（Q4運用）
  - https://www.ebay.com/sellercenter/resources/seller-updates/2025-holiday-shipping-prep
- eBay Collected report（2025年コレクティブル動向）
  - https://www.ebay.com/sellercenter/growth/collected-report
- eBay game console best-seller page（モデル例）
  - https://www.ebay.com/b/Video-Game-Consoles/139971/bn_7116517165
- eBay camera lens best-seller page（モデル例）
  - https://www.ebay.com/b/Camera-Lenses/3323/bn_727958
- eBay Prohibited and restricted items policy
  - https://www.ebay.com/help/policies/prohibited-restricted-items/prohibited-restricted-items?id=4207
- eBay Verified Rights Owner (VeRO)
  - https://www.ebay.com/sellercenter/ebay-for-business/verified-rights-owner-program
- IRS 2025 filing season dates（tax refund season）
  - https://www.irs.gov/newsroom/irs-kicks-off-2025-tax-filing-season-encourages-taxpayers-to-file-electronically-and-choose-direct-deposit-for-faster-refunds
- NRF Back-to-School 2025（季節需要の根拠）
  - https://nrf.com/media-center/press-releases/back-school-shoppers-prioritize-essentials-while-managing-costs
- NRF Holiday 2025 forecast（Q4需要の根拠）
  - https://nrf.com/media-center/press-releases/consumers-remain-engaged-holiday-spending-after-strong-year-so-far
- Pokemon Press（2025 TCG release dates）
  - https://press.pokemon.com/en/releases
- MTG official release schedule
  - https://magic.wizards.com/en/products
- One Piece Card Game official events/schedule
  - https://en.onepiece-cardgame.com/events/

### コミュニティ（補助）
- eBay Community（Terapeak表示差分の実務議論）
  - https://community.ebay.com/t5/Selling/What-happened-to-sales-data-in-the-Product-Research-tab/td-p/35079571
- Reddit r/Flipping（90日STR運用議論）
  - https://www.reddit.com/r/Flipping/comments/1nxj5ft/what_does_your_90_day_sell_through_rate_look_like/

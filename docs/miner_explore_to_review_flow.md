# Miner 探索フロー（A/B/C 正規版）

最終更新: 2026-02-24

この文書は、探索開始からレビュー待ち反映までを「カテゴリ指定」「A/B/C段階」「記録重視」で再整理した仕様です。
実装値が未確定の箇所は「要確認」に分離し、矛盾が出ないように暫定値を明記しています。

## 1. フロー全体（カテゴリ指定 → レビュー待ち）

```text
[USER]
  |
  | カテゴリ指定 + 探索開始
  v
[API]
  |
  |-- プール残数確認
  |     if active_seed_count > threshold -> B
  |     else -> A
  |
  |-- A: eBay 90days sold で seed 補充
  |-- B: 日本側検索で一次候補抽出
  |-- C: eBay 90days sold 再取得で最終判定
  |
  `-- pending 保存 + UI反映
```

## 2. A 段階（補充）

### 2.1 目的
- カテゴリプールの seed を不足なく補充する。
- 「探索に使える seed と価格（送料込み）」を揃える。

### 2.2 入力
- カテゴリキー
- ビッグワード群（カテゴリ名、主要メーカー名など）
- ビッグワード順（ナレッジ順）

### 2.3 実行条件
- プール有効数が補充閾値以下の場合に実行。
- 補充閾値の既定値は `0`（`MINER_SEED_POOL_REFILL_THRESHOLD` で変更可能）。

### 2.4 補充手順
1. ビッグワードを順番に選ぶ。
2. そのビッグワードで、7日以内に未スキャンのページを上位から走査。
3. eBay Product Research 条件:
- 90 days sold
- Condition: New
- Fixed Price
- カテゴリ選択
- カテゴリ別の最低価格フィルタ（安すぎる価格帯を除外）
  - 実装既定値（2026-02-24 時点）:
    - watch: `$8`
    - sneakers: `$20`
    - streetwear: `$15`
    - trading_cards: `$10`
    - toys_collectibles: `$12`
    - video_game_consoles: `$60`
    - camera_lenses: `$25`
  - 上記は `MINER_SEED_POOL_MIN_SOLD_PRICE_USD_<CATEGORY_KEY>` でカテゴリ単位上書き可能
  - 全体共通の既定上書きは `MINER_SEED_POOL_MIN_SOLD_PRICE_USD_DEFAULT`
4. 1ページ50件から seed 抽出:
- 特定商品をヒットできる語を seed として抽出。
- 付属品のみ（カバー等）の場合は本体seedを記録しない。
- 「付属品付き本体」は除外しない。
- seed には「seed価格(送料込み)」を紐付ける。
5. 重複除去:
- 今回探索内で既出の seed
- 同一カテゴリの有効プール内 seed
6. ページ走査は、新規seed獲得目標100件達成まで継続。
7. 目標達成後も、現在読み込み中ページは最後まで抽出して終了。
- 例: 99件の状態で次ページへ進んだ場合、当該50件ページは最後まで処理する。
- そのため、1探索での新規seed獲得数は最大149件まで許容する。
8. スキャン済みページにタイムスタンプを記録。
9. 2000件到達までに新規seedが十分に取れない場合は次ビッグワードへ切替。
10. 全ビッグワード消化後、各ビッグワード停止タイムスタンプから7日待機。

### 2.5 A段階の記録
- ビッグワード単位:
  - 開始/終了時刻
  - 最終ページオフセット
  - 新規seed獲得数
  - 停止理由（目標達成/上限到達/結果枯渇/クールダウン）
- ページ単位:
  - page, offset, raw件数, 新規seed件数
  - スキャン時刻
- seed単位:
  - seed文字列
  - seed価格（送料込み）
  - 抽出元タイトル
  - 抽出元URL
  - ビッグワード
  - 抽出方式（タイトル抽出 / API補完）

## 3. B 段階（日本側一次取得）

### 3.1 目的
- 古い seed から 20件を消化し、日本側の最安本体候補を取る。
- seed精度改善用ログを残す。

### 3.2 手順
1. カテゴリプールの有効seedを古い順で20件選定。
2. 各 seed で日本側サイト API 検索:
- カテゴリ指定（本体）
- 新品
- 価格昇順
- eBay seed 送料込み価格以下
3. 検索結果そのものを記録（seed精度評価用）。
4. 同一商品本体と思しき最安候補を seed ごとに最大2件採用。
5. ヒットなしでも詳細ログを残す（追加API再調査はしない）。
6. 20件すべて試行したら、日本側結果から「日本産seed」を抽出。

### 3.3 B段階の記録
- seed試行ログ:
  - seed
  - リクエスト条件
  - 総ヒット件数
  - 採用件数（0/1/2）
  - 採用理由/除外理由
- 無ヒットログ:
  - API応答の要約
  - フィルタ条件
  - 検索語
- 日本産seedログ:
  - 生成seed
  - 生成元（タイトル/API）
  - 元候補ID
  - 信頼度

## 4. C 段階（最終再判定）

### 4.1 目的
- B段階で得た日本側価格と、最新eBay 90日実績を突き合わせる。
- 古いseed価格参照による誤判定を回避する。

### 4.2 手順
1. 日本産seedで eBay Product Research を再実行:
- 90 days sold
- New
- Fixed Price
- カテゴリ指定
2. 同一商品本体ページ、90日最低価格（送料込み）、販売件数を取得。
3. 日本側販売価格と比較。
4. 利益条件を満たす候補のみ pending へ保存。
5. sold sample URL/価格を取得できない場合は既定で除外する。
  - `MINER_STAGE2_ALLOW_MISSING_SOLD_SAMPLE=false`（既定）
  - 例外運用を行う場合のみ `true` に切り替える。

### 4.3 C段階の記録
- 日本産seedごと:
  - eBay再取得時刻
  - sold最低価格（送料込み）
  - sold件数
  - 判定結果（通過/除外）
  - 除外理由（利益不足・一致不足・流動性不足など）

## 5. UI仕様（今回の基準）

### 5.1 ゲージ
- ヘッダに A/B/C の3段階を常時表示。
- 進行中段階を強調、完了段階は完了表示、未着手は待機表示。
- パーセントは全体進行率として表示しつつ、段階名を必ず併記。

### 5.2 表示テキスト方針
- カジュアル文（〜してね、更新したよ）は使わない。
- 丁寧語で統一。

### 5.3 探索ログ表示
- 上段: 現在段階、進捗率、現在seed/現在ビッグワード
- 下段: 除外トップ理由、経過秒、PR進捗（有効時のみ）

## 6. API運用設計（Web調査反映）

### 6.1 公式情報ベースの上限/制約
- eBay Browse API: 5,000 calls/day（標準上限）
- eBay Taxonomy API: 5,000 calls/day（標準上限）
- eBay Buy系は本番利用に審査・制約あり。Marketplace Insights APIは新規開放停止中。
- Rakuten Web Service: 1 application_id あたり 1 request/second
- YahooショッピングAPI: 同一URLへの短時間大量アクセスは制限対象（目安 1 query/second）

### 6.2 実用上の呼び分け方針
1. seed抽出はタイトル抽出を第一優先にする。
2. API補完は「抽出不能または曖昧」の場合のみ実行する。
3. 1 seed あたりのAPI補完呼び出しは上限1回に制限する。
4. カテゴリ/ビッグワード単位でAPI補完予算を持つ。
5. 予算超過時は補完を止め、タイトル抽出のみで継続する。
6. 付属品タイトル（例: band/strap/case only）はSeed記録対象から除外する。

### 6.3 日次予算（暫定）
- eBay Browse: 1,200 calls/day（5,000の24%）
- eBay Taxonomy: 300 calls/day（5,000の6%）
- Rakuten: 1 rpsを超えない。日次は上限未公開のため保守運用。
- Yahoo: 1 qpsを超えない。日次は固定値を前提にしない。
- Seed API補完（A段階）:
  - `MINER_SEED_API_SUPPLEMENT_DAILY_BUDGET=300`
  - `MINER_SEED_API_SUPPLEMENT_HOURLY_BUDGET=80`
  - `MINER_SEED_API_SUPPLEMENT_PER_RUN_BUDGET=40`
  - `MINER_SEED_API_SUPPLEMENT_ENABLED=1`
- A段階の実行ガード（補充が長時間固まる場合の保護）:
  - `MINER_SEED_POOL_REFILL_TIMEBOX_SEC=300`
  - `MINER_SEED_POOL_MAX_TIMEOUT_PAGES_PER_RUN=2`
  - `MINER_SEED_POOL_TIMEOUT_COOLDOWN_HOURS=1`
- C段階 sold sample ポリシー:
  - `MINER_STAGE2_ALLOW_MISSING_SOLD_SAMPLE=false`（既定）
  - 根拠URLが欠落する候補は pending 化しない

### 6.4 timed fetch の実行優先順
- B段階は「古い順20件消化」を優先し、timeboxは保護目的で後段適用する。
- 最低実行件数の既定:
  - `MINER_TIMED_FETCH_MIN_STAGE1_ATTEMPTS=20`
- UI/API既定:
  - `fetch_timebox_sec=300`
  - `fetch_max_passes=20`

### 6.5 カテゴリ指定の堅牢化方針（実装時の既定値）
- eBay: Taxonomy APIのカテゴリ提案を優先し、カテゴリIDを確定する。
- Rakuten: Genre Search APIで genreId を確定し、Item Searchへ渡す。
- Yahoo: カテゴリID取得APIで category_id を確定し、商品検索へ渡す。
- いずれも「カテゴリマップキャッシュ」を持ち、短時間再計算を避ける。
- 既定キャッシュTTL: 7日（カテゴリ変化が少ないため）。
- 実装補足:
  - B/C段階実行時は `MINER_ACTIVE_CATEGORY_KEY` を渡してカテゴリ文脈を固定する。
  - 日本側検索は新品固定 + 価格昇順 + seed上限価格（`MINER_ACTIVE_SEED_MAX_PRICE_JPY`）を適用。
  - `MINER_SOURCE_CATEGORY_FILTERS_JSON` がある場合はそれを最優先で使う。

## 7. 不明点・要確認（実装前に固定すべき）

1. B段階の「カテゴリ指定（本体）」をサイト横断でどう定義するか。
- 要確認: Rakuten/Yahoo でのカテゴリマッピングルール。
- 暫定: カテゴリID確定を必須化し、カテゴリ未解決の場合は探索対象外としてログに残す。
2. 日本産seedの抽出方法（タイトル抽出 vs API属性取得）の採用基準。
- 要確認: APIコスト上限（日次/1探索あたり）をどの値に固定するか。
- 暫定: タイトル抽出を優先し、抽出不能時のみAPI属性補完を1seedあたり1回まで実行。

## 8. 矛盾点と暫定解

- 矛盾: 20件固定運用と timebox 停止が競合。
  - 暫定: 「20件消化優先」、timebox は保護的上限として使用。

## 9. ログの過不足レビュー

### 9.1 不足しているログ
- ビッグワード別のスキャン履歴（どの語で何件取れたか）。
- seed抽出方式（タイトル抽出/API補完）と成功率。
- B段階の「生検索結果の要約ログ」（精度改善用）。
- C段階で使った最終 sold 根拠（URL/画像/件数）の監査ログ。

### 9.2 過剰化に注意するログ
- 全件生レスポンス全文保存は容量増大リスクが高い。
- 対策: 重要フィールドの構造化保存 + 代表サンプル + 再現キー（query/offset/id）を保存。
- 重要項目（判定理由、価格、候補ID、API条件）は要約せず保持する。

## 10. 記録ファイル命名ルール（提案）

- `docs/cycle_diagnostics/seed_refill_{category}_{yyyymmdd_hhmmss}.json`
- `docs/cycle_diagnostics/seed_trial_{category}_{yyyymmdd_hhmmss}.json`
- `docs/cycle_diagnostics/final_judgement_{category}_{yyyymmdd_hhmmss}.json`
- `docs/cycle_diagnostics/ui_progress_{category}_{yyyymmdd_hhmmss}.jsonl`

## 11. 公式参照URL

- eBay API call limits: https://developer.ebay.com/develop/get-started/api-call-limits
- eBay Browse API overview: https://developer.ebay.com/api-docs/buy/browse/static/overview.html
- eBay Buy filters（Marketplace Insights制約注記）: https://developer.ebay.com/api-docs/buy/static/ref-buy-browse-filters.html
- eBay Taxonomy getCategorySuggestions: https://developer.ebay.com/api-docs/commerce/taxonomy/resources/category_tree/methods/getCategorySuggestions
- Rakuten request limit FAQ: https://webservice.faq.rakuten.net/hc/ja/articles/900001974383-%E5%90%84API%E3%81%AE%E5%88%A9%E7%94%A8%E5%88%B6%E9%99%90%E3%82%92%E6%95%99%E3%81%88%E3%81%A6%E3%81%8F%E3%81%A0%E3%81%95%E3%81%84
- Rakuten Ichiba Item Search: https://webservice.rakuten.co.jp/documentation/ichiba-item-search
- Rakuten Ichiba Genre Search: https://webservice.rakuten.co.jp/documentation/ichiba-genre-search
- Yahoo Shopping API top: https://developer.yahoo.co.jp/webapi/shopping/
- Yahoo Shopping v3 API: https://developer.yahoo.co.jp/webapi/shopping/v3/
- Yahoo Shopping help: https://developer.yahoo.co.jp/webapi/shopping/help/

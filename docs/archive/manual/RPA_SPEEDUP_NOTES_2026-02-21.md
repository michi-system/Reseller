# RPA高速化メモ (2026-02-21)

## Web調査で採用したポイント

1. `networkidle` は推奨されないため、`goto(wait_until="commit")` + 明示待機へ寄せる。  
   出典: Playwright Python `page.goto` (`networkidle` DISCOURAGED, `commit` サポート)
2. `wait_for_response` で必要なレスポンス到着を待つ。  
   出典: Playwright Network Guide / Python API
3. Service Worker があると network interception/event の見え方が変わるため、`service_workers='block'` を使う。  
   出典: Playwright Network Guide / Service Workers docs
4. `route()` は HTTP キャッシュを無効化するため、速度目的だけでの常時ルーティングは避ける。  
   出典: Playwright `page.route` note

## 実装反映

- `LIQUIDITY_RPA_GOTO_WAIT_UNTIL=commit` を追加
- 固定 `wait_for_timeout` を短縮
- `page.wait_for_response(...)` を追加
- `page.set_default_timeout()` / `page.set_default_navigation_timeout()` を導入
- `service_workers='block'` を設定可能化（既定ON）
- HTML抽出で十分な場合は `inner_text("body")` をスキップ

## 追加した環境変数

- `LIQUIDITY_RPA_GOTO_WAIT_UNTIL`
- `LIQUIDITY_RPA_ACTION_TIMEOUT_MS`
- `LIQUIDITY_RPA_NAV_TIMEOUT_MS`
- `LIQUIDITY_RPA_BLOCK_SERVICE_WORKERS`


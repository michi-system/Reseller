# Data Contract: Approved Listing

この文書は、MinerからOperatorへ渡す「承認済み商品データ」の契約です。

## 1. レコード単位
- 1レコード = 1商品 (1出品候補)
- 形式: JSON Lines (`.jsonl`) または共有DBの1行

## 2. 必須フィールド
| フィールド | 型 | 説明 |
|---|---|---|
| `approved_id` | string | 承認イベントの一意ID |
| `approved_at` | string (ISO8601) | 承認日時 |
| `approved_by` | string | 承認者 |
| `sku_key` | string | 型番・規格を正規化した照合キー |
| `title` | string | 商品名 |
| `brand` | string | ブランド |
| `model` | string | 型番/モデル |
| `source_market` | string | 仕入れ市場（jp_ecなど） |
| `source_price_jpy` | number | 仕入れ価格（JPY） |
| `target_market` | string | 販売市場（ebay） |
| `target_price_usd` | number | 販売想定価格（USD） |
| `fx_rate` | number | 利用為替レート |
| `estimated_profit_jpy` | number | 見込み利益（JPY） |
| `estimated_profit_rate` | number | 見込み利益率（0-1） |
| `risk_flags` | array[string] | 承認時点の注意フラグ |
| `listing_status` | string | `ready` / `listed` / `paused` / `stopped` |

## 3. 任意フィールド
| フィールド | 型 | 説明 |
|---|---|---|
| `notes` | string | 人間レビューの補足 |
| `category_hint` | string | カテゴリ推定 |
| `image_url` | string | 代表画像 |
| `shipping_cost_jpy` | number | 想定送料 |
| `fee_total_jpy` | number | 想定手数料 |

## 4. ステータス遷移
```text
ready -> listed -> paused -> listed
ready -> listed -> stopped
paused -> stopped
```

`stopped` は終端扱い。再開時は新規承認イベントで `ready` を作る。

## 5. JSONL例
```json
{
  "approved_id": "apr_20260222_0001",
  "approved_at": "2026-02-22T10:35:00+09:00",
  "approved_by": "tad",
  "sku_key": "CASIO_GW-M5610U-1JF",
  "title": "Casio G-SHOCK GW-M5610U-1JF",
  "brand": "Casio",
  "model": "GW-M5610U-1JF",
  "source_market": "rakuten",
  "source_price_jpy": 14980,
  "target_market": "ebay",
  "target_price_usd": 179.0,
  "fx_rate": 149.2,
  "estimated_profit_jpy": 5210,
  "estimated_profit_rate": 0.257,
  "risk_flags": [],
  "listing_status": "ready",
  "notes": "箱/保証書あり"
}
```

## 6. 契約変更ルール
- フィールド追加: 後方互換を維持する（任意で追加）。
- フィールド削除/型変更: メジャー変更として事前合意する。
- 変更時は必ず次を同時更新する。
  - `docs/PROGRAM_OVERVIEW.md`
  - `docs/WORKBOARD.md` (Decision Log)
  - Miner / Operator 実装

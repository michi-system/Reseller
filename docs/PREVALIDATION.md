# Pre-validation setup (eBay / Yahoo / Rakuten)

This repo is prepared to run a low-cost preflight before live search validation.

## 1) Env setup

Use `.env.local` and keep it git-ignored.

Required for preflight:
- `EBAY_CLIENT_ID`
- `EBAY_CLIENT_SECRET`
- `YAHOO_APP_ID` (or `YAHOO_CLIENT_ID`)
- `RAKUTEN_APPLICATION_ID`

Recommended runtime parameters:
- `TARGET_MARKETPLACE=EBAY_US`
- `TARGET_CATEGORY=watch_new`
- `ITEM_CONDITION=new`
- `DEFAULT_QUERY=seiko watch`
- `EBAY_RATE_LIMIT_RPS=2`
- `YAHOO_RATE_LIMIT_RPS=1`
- `RAKUTEN_RATE_LIMIT_RPS=1`
- `RAKUTEN_API_BASE_URL=https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20220601`
- `EBAY_MIN_CANDIDATES=20`
- `EBAY_MAX_CANDIDATES=2000`
- `YAHOO_MIN_CANDIDATES=20`
- `YAHOO_MAX_CANDIDATES=1000`
- `RAKUTEN_MIN_CANDIDATES=10`
- `RAKUTEN_MAX_CANDIDATES=1000`

Optional now, required before profit validation:
- `FX_RATE_PROVIDER_URL` (keyless mode, recommended)
- `FX_BASE_CCY` (example: `USD`)
- `FX_QUOTE_CCY` (example: `JPY`)
- `FX_RATE_JSON_PATH` (example: `rates.{QUOTE}`)
- `FX_PROVIDER` (example: `open_er_api`)
- `FX_CACHE_SECONDS` (example: `900`)
- `FX_USD_JPY` (fallback default, example: `150`)

Recommended default (keyless mode, same style as previous project):

```dotenv
FX_PROVIDER=open_er_api
FX_RATE_PROVIDER_URL=https://open.er-api.com/v6/latest/USD
FX_BASE_CCY=USD
FX_QUOTE_CCY=JPY
FX_RATE_JSON_PATH=rates.{QUOTE}
FX_CACHE_SECONDS=900
FX_USD_JPY=150
```

Alternative (API key mode):
- `FX_API_KEY`
- `FX_RATE_URL_TEMPLATE` (use placeholders `{FX_API_KEY}`, `{BASE}`, `{QUOTE}`)

## 2) Dry run (no network)

```bash
python3 scripts/preflight.py
```

This checks env completeness, runtime parameter visibility, and fixed policy constraints from:
- `/Users/tadamichikimura/Downloads/dev-HQ/ebayminer/docs/OPERATION_POLICY.json`

Skip policy check only for explicit experiments:

```bash
python3 scripts/preflight.py --skip-policy-check
```

## 3) Online smoke checks (1 request per site)

```bash
python3 scripts/preflight.py --online --query "seiko sbga211"
```

What it does:
- eBay: OAuth token issue + Browse search (`limit=1`)
- Yahoo: ItemSearch v3 (`results=1`)
- Rakuten: IchibaItemSearch (`hits=1`)

Require FX too:

```bash
python3 scripts/preflight.py --online --require-fx --query "seiko sbga211"
```

FX URL placeholders:
- `{FX_API_KEY}`
- `{BASE}`
- `{QUOTE}`

Standalone FX check:

```bash
python3 scripts/fx_quote.py --base USD --quote JPY
```

`fx_quote.py` behavior:
- Provider fetch succeeds: `source=api`
- Provider fetch fails and `FX_USD_JPY` is set: `source=fallback_env`
- Both fail: non-zero exit

Bypass cache:

```bash
python3 scripts/fx_quote.py --base USD --quote JPY --no-cache
```

## 4) Interpretation

- `PASS`: ready to move into rate/breadth validation
- `FAIL`: read the HTTP status and payload summary, then fix credentials or endpoint assumptions
- `POLICY:* NG`: startup guard is preventing drift from the fixed completion policy. Align `.env.local` before running cycles.

## 5) Next phase (not yet in this repo)

Run staged query-width pilot:

```bash
python3 scripts/query_width_pilot.py --brand seiko --model sbga211 --noun watch
```

The script saves a report at `docs/query_width_report.json` with:
- HTTP status per stage
- hit count per stage
- marginal count delta as the query broadens
- recommended stage per site (narrowest stage with enough candidates)

Profit dry run with FX state:

```bash
python3 scripts/profit_demo.py --refresh-fx --sale-usd 420 --purchase-jpy 42000
```

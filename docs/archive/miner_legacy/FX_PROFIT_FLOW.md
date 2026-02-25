# FX + Profit Flow

## Overview

Profit uses USD/JPY in this order:

1. Process cache (`reselling.fx_rate`)
2. SQLite `fx_rate_states` (`reselling.models`)
3. `FX_USD_JPY` fallback from env

FX refresh is separated and called only when needed:

- `maybe_refresh_usd_jpy_rate(force=False)`
- Refresh is skipped until `next_refresh_at`
- Default provider mode is keyless: `FX_RATE_PROVIDER_URL=https://open.er-api.com/v6/latest/USD`

## Files

- `reselling/config.py`
- `reselling/models.py`
- `reselling/fx_rate.py`
- `reselling/profit.py`
- `reselling/api_server.py`
- `reselling/scheduler.py`
- `scripts/profit_demo.py`
- `scripts/run_api.py`
- `scripts/run_scheduler.py`

## Commands

Refresh only if due, then calculate profit:

```bash
python3 scripts/profit_demo.py \
  --refresh-fx \
  --sale-usd 420 \
  --purchase-jpy 42000 \
  --domestic-shipping-jpy 1200 \
  --international-shipping-usd 28 \
  --customs-usd 8 \
  --packaging-usd 3
```

Force refresh, then calculate:

```bash
python3 scripts/profit_demo.py \
  --force-refresh-fx \
  --sale-usd 420 \
  --purchase-jpy 42000
```

## Notes

- If provider fetch fails and DB has old rate, DB rate is reused.
- If DB is empty too, `FX_USD_JPY` is used.
- DB path is controlled by `DB_PATH`.

#!/usr/bin/env python3
"""CLI demo for FX refresh + profit calculation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.local"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from reselling.env import load_dotenv


def main() -> int:
    load_dotenv(ENV_PATH)

    from reselling.fx_rate import maybe_refresh_usd_jpy_rate
    from reselling.profit import ProfitInput, calculate_profit

    parser = argparse.ArgumentParser(description="Profit demo using current USD/JPY state.")
    parser.add_argument("--sale-usd", type=float, required=True)
    parser.add_argument("--purchase-jpy", type=float, required=True)
    parser.add_argument("--domestic-shipping-jpy", type=float, default=0.0)
    parser.add_argument("--international-shipping-usd", type=float, default=0.0)
    parser.add_argument("--customs-usd", type=float, default=0.0)
    parser.add_argument("--packaging-usd", type=float, default=0.0)
    parser.add_argument("--misc-cost-jpy", type=float, default=0.0)
    parser.add_argument("--misc-cost-usd", type=float, default=0.0)
    parser.add_argument("--marketplace-fee-rate", type=float, default=0.13)
    parser.add_argument("--payment-fee-rate", type=float, default=0.03)
    parser.add_argument("--fixed-fee-usd", type=float, default=0.0)
    parser.add_argument("--refresh-fx", action="store_true")
    parser.add_argument("--force-refresh-fx", action="store_true")
    args = parser.parse_args()

    refresh_info = None
    if args.refresh_fx or args.force_refresh_fx:
        refresh_info = maybe_refresh_usd_jpy_rate(force=args.force_refresh_fx)

    payload = calculate_profit(
        ProfitInput(
            sale_price_usd=args.sale_usd,
            purchase_price_jpy=args.purchase_jpy,
            domestic_shipping_jpy=args.domestic_shipping_jpy,
            international_shipping_usd=args.international_shipping_usd,
            customs_usd=args.customs_usd,
            packaging_usd=args.packaging_usd,
            misc_cost_jpy=args.misc_cost_jpy,
            misc_cost_usd=args.misc_cost_usd,
            marketplace_fee_rate=args.marketplace_fee_rate,
            payment_fee_rate=args.payment_fee_rate,
            fixed_fee_usd=args.fixed_fee_usd,
        )
    )
    if refresh_info is not None:
        payload["fx_refresh"] = refresh_info

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

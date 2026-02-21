"""Profit calculation that depends on current USD/JPY state."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

from .config import Settings, load_settings
from .fx_rate import get_current_usd_jpy_snapshot


@dataclass(frozen=True)
class ProfitInput:
    sale_price_usd: float
    purchase_price_jpy: float
    domestic_shipping_jpy: float = 0.0
    international_shipping_usd: float = 0.0
    customs_usd: float = 0.0
    packaging_usd: float = 0.0
    misc_cost_jpy: float = 0.0
    misc_cost_usd: float = 0.0
    marketplace_fee_rate: float = 0.13
    payment_fee_rate: float = 0.03
    fixed_fee_usd: float = 0.0


def calculate_profit(input_data: ProfitInput, settings: Optional[Settings] = None) -> Dict[str, Any]:
    settings = settings or load_settings()
    fx = get_current_usd_jpy_snapshot(settings)

    jpy_cost_total = (
        float(input_data.purchase_price_jpy)
        + float(input_data.domestic_shipping_jpy)
        + float(input_data.misc_cost_jpy)
    )
    jpy_cost_total_usd = jpy_cost_total / fx.rate
    variable_fee_usd = (
        float(input_data.sale_price_usd)
        * (float(input_data.marketplace_fee_rate) + float(input_data.payment_fee_rate))
    )
    usd_cost_total = (
        jpy_cost_total_usd
        + float(input_data.international_shipping_usd)
        + float(input_data.customs_usd)
        + float(input_data.packaging_usd)
        + float(input_data.misc_cost_usd)
        + float(input_data.fixed_fee_usd)
        + variable_fee_usd
    )
    revenue_usd = float(input_data.sale_price_usd)
    profit_usd = revenue_usd - usd_cost_total
    margin_rate = (profit_usd / revenue_usd) if revenue_usd > 0 else 0.0

    return {
        "input": asdict(input_data),
        "fx": {
            "pair": fx.pair,
            "rate": fx.rate,
            "source": fx.source,
            "fetched_at": fx.fetched_at,
            "next_refresh_at": fx.next_refresh_at,
            "provenance": fx.provenance,
        },
        "breakdown": {
            "revenue_usd": revenue_usd,
            "jpy_cost_total": jpy_cost_total,
            "jpy_cost_total_usd": jpy_cost_total_usd,
            "variable_fee_usd": variable_fee_usd,
            "usd_cost_total": usd_cost_total,
            "profit_usd": profit_usd,
            "margin_rate": margin_rate,
        },
    }


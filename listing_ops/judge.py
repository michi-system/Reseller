"""Profit and stock judge for Operator monitoring."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class JudgeInput:
    source_price_jpy: float
    target_price_usd: float
    fx_rate: float
    source_in_stock: bool
    low_profit_streak: int
    low_stock_streak: int
    min_profit_jpy: float
    min_profit_rate: float
    stop_consecutive_fail_count: int
    heavy_price_drop: bool = False


@dataclass(frozen=True)
class JudgeResult:
    decision: str  # keep | alert_review | stop
    reason_code: str
    profit_jpy: float
    profit_rate: float
    next_low_profit_streak: int
    next_low_stock_streak: int
    needs_review: bool


def _profit(source_price_jpy: float, target_price_usd: float, fx_rate: float) -> tuple[float, float]:
    source = max(0.0, float(source_price_jpy))
    target = max(0.0, float(target_price_usd))
    rate = max(0.0, float(fx_rate))
    profit_jpy = target * rate - source
    if source <= 0:
        return profit_jpy, 0.0
    return profit_jpy, profit_jpy / source


def evaluate(input_data: JudgeInput) -> JudgeResult:
    profit_jpy, profit_rate = _profit(
        input_data.source_price_jpy,
        input_data.target_price_usd,
        input_data.fx_rate,
    )

    low_profit = (profit_jpy < float(input_data.min_profit_jpy)) or (
        profit_rate < float(input_data.min_profit_rate)
    )
    out_of_stock = not bool(input_data.source_in_stock)

    next_low_profit_streak = int(input_data.low_profit_streak) + 1 if low_profit else 0
    next_low_stock_streak = int(input_data.low_stock_streak) + 1 if out_of_stock else 0
    fail_limit = max(1, int(input_data.stop_consecutive_fail_count))

    if next_low_stock_streak >= fail_limit:
        return JudgeResult(
            decision="stop",
            reason_code="source_out_of_stock_consecutive",
            profit_jpy=round(profit_jpy, 2),
            profit_rate=round(profit_rate, 6),
            next_low_profit_streak=next_low_profit_streak,
            next_low_stock_streak=next_low_stock_streak,
            needs_review=False,
        )
    if next_low_profit_streak >= fail_limit:
        return JudgeResult(
            decision="stop",
            reason_code="profit_below_threshold_consecutive",
            profit_jpy=round(profit_jpy, 2),
            profit_rate=round(profit_rate, 6),
            next_low_profit_streak=next_low_profit_streak,
            next_low_stock_streak=next_low_stock_streak,
            needs_review=False,
        )
    if input_data.heavy_price_drop:
        return JudgeResult(
            decision="alert_review",
            reason_code="heavy_price_drop_detected",
            profit_jpy=round(profit_jpy, 2),
            profit_rate=round(profit_rate, 6),
            next_low_profit_streak=next_low_profit_streak,
            next_low_stock_streak=next_low_stock_streak,
            needs_review=True,
        )
    if out_of_stock:
        return JudgeResult(
            decision="alert_review",
            reason_code="source_out_of_stock_once",
            profit_jpy=round(profit_jpy, 2),
            profit_rate=round(profit_rate, 6),
            next_low_profit_streak=next_low_profit_streak,
            next_low_stock_streak=next_low_stock_streak,
            needs_review=True,
        )
    if low_profit:
        return JudgeResult(
            decision="alert_review",
            reason_code="profit_below_threshold_once",
            profit_jpy=round(profit_jpy, 2),
            profit_rate=round(profit_rate, 6),
            next_low_profit_streak=next_low_profit_streak,
            next_low_stock_streak=next_low_stock_streak,
            needs_review=True,
        )
    return JudgeResult(
        decision="keep",
        reason_code="healthy",
        profit_jpy=round(profit_jpy, 2),
        profit_rate=round(profit_rate, 6),
        next_low_profit_streak=next_low_profit_streak,
        next_low_stock_streak=next_low_stock_streak,
        needs_review=False,
    )


def judge_input_from_listing(
    listing_row: Dict[str, Any],
    *,
    source_price_jpy: float,
    target_price_usd: float,
    fx_rate: float,
    source_in_stock: bool,
    min_profit_jpy: float,
    min_profit_rate: float,
    stop_consecutive_fail_count: int,
    heavy_price_drop: bool = False,
) -> JudgeInput:
    return JudgeInput(
        source_price_jpy=source_price_jpy,
        target_price_usd=target_price_usd,
        fx_rate=fx_rate,
        source_in_stock=source_in_stock,
        low_profit_streak=int(listing_row.get("low_profit_streak", 0)),
        low_stock_streak=int(listing_row.get("low_stock_streak", 0)),
        min_profit_jpy=min_profit_jpy,
        min_profit_rate=min_profit_rate,
        stop_consecutive_fail_count=stop_consecutive_fail_count,
        heavy_price_drop=heavy_price_drop,
    )

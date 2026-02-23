"""Shared numeric helpers for metrics/reporting."""

from __future__ import annotations


def safe_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)

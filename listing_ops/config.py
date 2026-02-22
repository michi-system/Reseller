"""Operator settings loader."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OPERATOR_DB_PATH = ROOT_DIR / "data" / "operator.db"


@dataclass(frozen=True)
class OperatorSettings:
    db_path: Path
    min_profit_jpy: float
    min_profit_rate: float
    stop_consecutive_fail_count: int
    light_interval_new_hours: int
    light_interval_stable_hours: int
    light_interval_stopped_hours: int
    heavy_interval_days: int
    default_actor_id: str


def _get_float(name: str, default: float) -> float:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _get_int(name: str, default: int, min_value: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(min_value, value)


def load_operator_settings() -> OperatorSettings:
    raw_db_path = (os.getenv("OPERATOR_DB_PATH", "") or "").strip()
    db_path = Path(raw_db_path) if raw_db_path else DEFAULT_OPERATOR_DB_PATH
    return OperatorSettings(
        db_path=db_path,
        min_profit_jpy=_get_float("OPERATOR_MIN_PROFIT_JPY", 1500.0),
        min_profit_rate=_get_float("OPERATOR_MIN_PROFIT_RATE", 0.08),
        stop_consecutive_fail_count=_get_int("OPERATOR_STOP_FAIL_COUNT", 2, 1),
        light_interval_new_hours=_get_int("OPERATOR_LIGHT_NEW_HOURS", 6, 1),
        light_interval_stable_hours=_get_int("OPERATOR_LIGHT_STABLE_HOURS", 24, 1),
        light_interval_stopped_hours=_get_int("OPERATOR_LIGHT_STOPPED_HOURS", 72, 1),
        heavy_interval_days=_get_int("OPERATOR_HEAVY_DAYS", 7, 1),
        default_actor_id=(os.getenv("OPERATOR_DEFAULT_ACTOR", "system") or "system").strip(),
    )

"""Shared coercion helpers for env/query/body values."""

from __future__ import annotations

import os
from typing import Any

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    if isinstance(value, str):
        norm = value.strip().lower()
        if not norm:
            return default
        if norm in _TRUE_VALUES:
            return True
        if norm in _FALSE_VALUES:
            return False
    return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    return to_bool(raw, default)


def env_int(name: str, default: int = 0) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    return to_int(raw, default)


def env_float(name: str, default: float = 0.0) -> float:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    return to_float(raw, default)

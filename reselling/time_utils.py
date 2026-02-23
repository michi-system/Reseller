"""Shared UTC time helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def utc_iso(ts: Optional[float] = None) -> str:
    if ts is None:
        dt = datetime.now(timezone.utc)
    else:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def iso_to_epoch(text: str) -> int:
    raw = str(text or "").strip()
    if not raw:
        return 0
    try:
        normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        return int(datetime.fromisoformat(normalized).timestamp())
    except ValueError:
        return 0

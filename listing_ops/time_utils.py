"""Time helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_iso(raw: str) -> datetime:
    return datetime.fromisoformat(raw)


def add_hours(iso_ts: str, hours: int) -> str:
    return (parse_iso(iso_ts) + timedelta(hours=hours)).isoformat(timespec="seconds")


def add_days(iso_ts: str, days: int) -> str:
    return (parse_iso(iso_ts) + timedelta(days=days)).isoformat(timespec="seconds")

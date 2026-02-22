"""Runtime configuration helpers."""

from __future__ import annotations

from typing import Any, Dict

from .config import OperatorSettings
from .models import latest_config, seed_config_if_missing


def ensure_and_get_active_config(conn, settings: OperatorSettings) -> Dict[str, Any]:
    current = latest_config(conn)
    if current is not None:
        return current
    config_version = "v1-default"
    from .time_utils import utcnow_iso

    seed_config_if_missing(
        conn,
        config_version=config_version,
        min_profit_jpy=settings.min_profit_jpy,
        min_profit_rate=settings.min_profit_rate,
        stop_consecutive_fail_count=settings.stop_consecutive_fail_count,
        light_interval_new_hours=settings.light_interval_new_hours,
        light_interval_stable_hours=settings.light_interval_stable_hours,
        light_interval_stopped_hours=settings.light_interval_stopped_hours,
        heavy_interval_days=settings.heavy_interval_days,
        created_at=utcnow_iso(),
        created_by=settings.default_actor_id,
    )
    current = latest_config(conn)
    if current is None:
        raise RuntimeError("failed to seed operator config")
    return current

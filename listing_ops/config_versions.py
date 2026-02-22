"""Config version management for Operator."""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Dict

from .config import load_operator_settings
from .models import connect, init_db, latest_config
from .time_utils import utcnow_iso


def create_config_version(
    *,
    db_path: Path,
    min_profit_jpy: float,
    min_profit_rate: float,
    stop_consecutive_fail_count: int,
    light_interval_new_hours: int,
    light_interval_stable_hours: int,
    light_interval_stopped_hours: int,
    heavy_interval_days: int,
    created_by: str,
    config_version: str = "",
) -> Dict[str, Any]:
    version = (config_version or "").strip() or f"v1-{uuid.uuid4().hex[:8]}"
    conn = connect(db_path)
    init_db(conn)
    now_iso = utcnow_iso()
    conn.execute(
        """
        INSERT INTO operator_config_versions (
            config_version,
            min_profit_jpy,
            min_profit_rate,
            stop_consecutive_fail_count,
            light_interval_new_hours,
            light_interval_stable_hours,
            light_interval_stopped_hours,
            heavy_interval_days,
            created_at,
            created_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            version,
            float(min_profit_jpy),
            float(min_profit_rate),
            max(1, int(stop_consecutive_fail_count)),
            max(1, int(light_interval_new_hours)),
            max(1, int(light_interval_stable_hours)),
            max(1, int(light_interval_stopped_hours)),
            max(1, int(heavy_interval_days)),
            now_iso,
            str(created_by or "system").strip() or "system",
        ),
    )
    conn.commit()
    active = latest_config(conn)
    conn.close()
    if active is None:
        raise RuntimeError("failed to create config version")
    return active


def load_or_default(db_path: Path) -> Dict[str, Any]:
    settings = load_operator_settings()
    conn = connect(db_path)
    init_db(conn)
    row = latest_config(conn)
    conn.close()
    if row is not None:
        return row
    return {
        "config_version": "env-default",
        "min_profit_jpy": settings.min_profit_jpy,
        "min_profit_rate": settings.min_profit_rate,
        "stop_consecutive_fail_count": settings.stop_consecutive_fail_count,
        "light_interval_new_hours": settings.light_interval_new_hours,
        "light_interval_stable_hours": settings.light_interval_stable_hours,
        "light_interval_stopped_hours": settings.light_interval_stopped_hours,
        "heavy_interval_days": settings.heavy_interval_days,
    }

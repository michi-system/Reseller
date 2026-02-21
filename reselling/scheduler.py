"""Simple loop scheduler for FX refresh."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Dict

from .fx_rate import maybe_refresh_usd_jpy_rate


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def run_scheduler(interval_seconds: int = 30) -> None:
    interval_seconds = max(5, int(interval_seconds))
    print(f"[scheduler] started interval={interval_seconds}s")
    try:
        while True:
            result: Dict[str, Any] = maybe_refresh_usd_jpy_rate(force=False)
            print(
                json.dumps(
                    {
                        "at": _ts(),
                        "event": "fx_refresh_tick",
                        "result": result,
                    },
                    ensure_ascii=False,
                )
            )
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("[scheduler] stopped")


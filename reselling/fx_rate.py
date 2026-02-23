"""FX state retrieval and refresh logic."""

from __future__ import annotations

import json
import os
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from .config import Settings, load_settings
from .http_json import request_json as _http_request_json
from .json_utils import extract_json_path as _extract_json_path
from .models import connect, get_fx_rate_state, init_db, upsert_fx_rate_state
from .time_utils import utc_iso


@dataclass(frozen=True)
class FxRateSnapshot:
    pair: str
    rate: float
    source: str
    fetched_at: str
    next_refresh_at: Optional[str]
    provenance: str


_PROCESS_CACHE: Dict[str, FxRateSnapshot] = {}


def _from_iso(value: str) -> datetime:
    cleaned = value.replace("Z", "+00:00")
    return datetime.fromisoformat(cleaned).astimezone(timezone.utc)


def _request_json(url: str, timeout: int = 20) -> Tuple[int, Dict[str, str], Dict[str, Any]]:
    return _http_request_json(url, timeout=timeout, raw_limit=500)

def _pair(settings: Settings) -> str:
    return f"{settings.fx_base_ccy}{settings.fx_quote_ccy}"


def _resolve_fx_url(settings: Settings) -> Tuple[str, str]:
    base = settings.fx_base_ccy
    quote = settings.fx_quote_ccy
    if settings.fx_rate_provider_url:
        raw = settings.fx_rate_provider_url
        mode = "provider_url"
    else:
        raw = settings.fx_rate_url_template
        mode = "template"
    if not raw:
        raise ValueError("missing FX provider URL/template")

    return (
        raw.replace("{FX_API_KEY}", urllib.parse.quote_plus(settings.fx_api_key))
        .replace("{BASE}", urllib.parse.quote_plus(base))
        .replace("{QUOTE}", urllib.parse.quote_plus(quote)),
        mode,
    )


def _default_snapshot(settings: Settings) -> FxRateSnapshot:
    now_iso = utc_iso()
    return FxRateSnapshot(
        pair=_pair(settings),
        rate=float(settings.fx_usd_jpy_default),
        source="env:FX_USD_JPY",
        fetched_at=now_iso,
        next_refresh_at=None,
        provenance="env_default",
    )


def get_current_usd_jpy_snapshot(settings: Optional[Settings] = None) -> FxRateSnapshot:
    settings = settings or load_settings()
    pair = _pair(settings)

    cached = _PROCESS_CACHE.get(pair)
    if cached is not None:
        return FxRateSnapshot(
            pair=cached.pair,
            rate=cached.rate,
            source=cached.source,
            fetched_at=cached.fetched_at,
            next_refresh_at=cached.next_refresh_at,
            provenance="process_cache",
        )

    with connect(settings.db_path) as conn:
        init_db(conn)
        row = get_fx_rate_state(conn, pair)
        if row:
            snap = FxRateSnapshot(
                pair=row["pair"],
                rate=float(row["rate"]),
                source=str(row["source"]),
                fetched_at=str(row["fetched_at"]),
                next_refresh_at=str(row["next_refresh_at"]),
                provenance="db",
            )
            _PROCESS_CACHE[pair] = snap
            return snap

    snap = _default_snapshot(settings)
    _PROCESS_CACHE[pair] = snap
    return snap


def get_current_usd_jpy_rate(settings: Optional[Settings] = None) -> float:
    return get_current_usd_jpy_snapshot(settings).rate


def maybe_refresh_usd_jpy_rate(
    settings: Optional[Settings] = None,
    *,
    force: bool = False,
    timeout: int = 20,
) -> Dict[str, Any]:
    settings = settings or load_settings()
    pair = _pair(settings)
    now = datetime.now(timezone.utc)
    now_iso = utc_iso(now.timestamp())

    with connect(settings.db_path) as conn:
        init_db(conn)
        row = get_fx_rate_state(conn, pair)
        if row and not force:
            next_refresh_at = _from_iso(str(row["next_refresh_at"]))
            if next_refresh_at > now:
                snap = FxRateSnapshot(
                    pair=row["pair"],
                    rate=float(row["rate"]),
                    source=str(row["source"]),
                    fetched_at=str(row["fetched_at"]),
                    next_refresh_at=str(row["next_refresh_at"]),
                    provenance="db",
                )
                _PROCESS_CACHE[pair] = snap
                return {
                    "updated": False,
                    "reason": "not_due",
                    "pair": pair,
                    "rate": snap.rate,
                    "source": snap.source,
                    "fetched_at": snap.fetched_at,
                    "next_refresh_at": snap.next_refresh_at,
                }

        try:
            url, mode = _resolve_fx_url(settings)
            status, _, payload = _request_json(url, timeout=timeout)
            path = settings.fx_rate_json_path.replace("{BASE}", settings.fx_base_ccy).replace(
                "{QUOTE}", settings.fx_quote_ccy
            )
            raw_rate = _extract_json_path(payload, path)
            if status != 200 or not isinstance(raw_rate, (int, float)) or raw_rate <= 0:
                raise RuntimeError(
                    f"fx_fetch_failed http={status} mode={mode} path={path} raw_rate={raw_rate}"
                )
            rate = float(raw_rate)
            source = urlparse(url).netloc or settings.fx_provider
            next_refresh_at = utc_iso((now + timedelta(seconds=settings.fx_refresh_seconds)).timestamp())
            upsert_fx_rate_state(
                conn,
                pair=pair,
                rate=rate,
                source=source,
                fetched_at=now_iso,
                next_refresh_at=next_refresh_at,
            )
            snap = FxRateSnapshot(
                pair=pair,
                rate=rate,
                source=source,
                fetched_at=now_iso,
                next_refresh_at=next_refresh_at,
                provenance="provider",
            )
            _PROCESS_CACHE[pair] = snap
            return {
                "updated": True,
                "reason": "refreshed",
                "pair": pair,
                "rate": rate,
                "source": source,
                "fetched_at": now_iso,
                "next_refresh_at": next_refresh_at,
            }
        except Exception as err:
            if row:
                snap = FxRateSnapshot(
                    pair=row["pair"],
                    rate=float(row["rate"]),
                    source=str(row["source"]),
                    fetched_at=str(row["fetched_at"]),
                    next_refresh_at=str(row["next_refresh_at"]),
                    provenance="db_on_error",
                )
                _PROCESS_CACHE[pair] = snap
                return {
                    "updated": False,
                    "reason": "fetch_error_used_db",
                    "error": str(err),
                    "pair": pair,
                    "rate": snap.rate,
                    "source": snap.source,
                    "fetched_at": snap.fetched_at,
                    "next_refresh_at": snap.next_refresh_at,
                }

            snap = _default_snapshot(settings)
            _PROCESS_CACHE[pair] = snap
            return {
                "updated": False,
                "reason": "fetch_error_used_env_default",
                "error": str(err),
                "pair": snap.pair,
                "rate": snap.rate,
                "source": snap.source,
                "fetched_at": snap.fetched_at,
                "next_refresh_at": snap.next_refresh_at,
            }


def clear_process_cache() -> None:
    _PROCESS_CACHE.clear()


if __name__ == "__main__":
    # Lightweight manual check
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    print(json.dumps(maybe_refresh_usd_jpy_rate(), ensure_ascii=False, indent=2))
    print(json.dumps(get_current_usd_jpy_snapshot().__dict__, ensure_ascii=False, indent=2))

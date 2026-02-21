"""Runtime settings loader."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    db_path: Path
    fx_provider: str
    fx_rate_provider_url: str
    fx_rate_url_template: str
    fx_rate_json_path: str
    fx_api_key: str
    fx_base_ccy: str
    fx_quote_ccy: str
    fx_usd_jpy_default: float
    fx_refresh_seconds: int
    fx_cache_seconds: int


def _get_float(name: str, default: float) -> float:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _get_int(name: str, default: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def load_settings() -> Settings:
    db_default = ROOT_DIR / "data" / "ebayminer.db"
    db_path = Path((os.getenv("DB_PATH", "") or "").strip() or db_default)
    return Settings(
        db_path=db_path,
        fx_provider=(os.getenv("FX_PROVIDER", "open_er_api") or "open_er_api").strip(),
        fx_rate_provider_url=(os.getenv("FX_RATE_PROVIDER_URL", "") or "").strip(),
        fx_rate_url_template=(os.getenv("FX_RATE_URL_TEMPLATE", "") or "").strip(),
        fx_rate_json_path=(
            (os.getenv("FX_RATE_JSON_PATH", "") or "").strip() or "rates.{QUOTE}"
        ),
        fx_api_key=(os.getenv("FX_API_KEY", "") or "").strip(),
        fx_base_ccy=(os.getenv("FX_BASE_CCY", "USD") or "USD").strip().upper(),
        fx_quote_ccy=(os.getenv("FX_QUOTE_CCY", "JPY") or "JPY").strip().upper(),
        fx_usd_jpy_default=_get_float("FX_USD_JPY", 150.0),
        fx_refresh_seconds=max(60, _get_int("FX_REFRESH_SECONDS", 3600)),
        fx_cache_seconds=max(0, _get_int("FX_CACHE_SECONDS", 900)),
    )


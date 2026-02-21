#!/usr/bin/env python3
"""Fetch a single FX rate using env-based template configuration."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Tuple


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.local"
CACHE_PATH = ROOT_DIR / "docs" / "fx_rate_cache.json"


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and (
            (value.startswith('"') and value.endswith('"'))
            or (value.startswith("'") and value.endswith("'"))
        ):
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


def request_json(url: str, timeout: int) -> Tuple[int, Dict[str, str], Dict]:
    req = urllib.request.Request(url=url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            payload = json.loads(raw) if raw else {}
            return int(resp.status), dict(resp.headers.items()), payload
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(body) if body else {}
        except json.JSONDecodeError:
            payload = {"raw": body[:500]}
        return int(err.code), dict(err.headers.items()), payload
    except urllib.error.URLError as err:
        return 0, {}, {"error": str(err)}


def extract_json_path(payload: Dict, path: str) -> object:
    current: object = payload
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        if part not in current:
            return None
        current = current[part]
    return current


def read_cache(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_cache(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch FX rate from configured provider.")
    parser.add_argument("--base", default=os.getenv("FX_BASE_CCY", "USD"))
    parser.add_argument("--quote", default=os.getenv("FX_QUOTE_CCY", "JPY"))
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    fx_key = os.getenv("FX_API_KEY", "").strip()
    provider_url = os.getenv("FX_RATE_PROVIDER_URL", "").strip()
    template = os.getenv("FX_RATE_URL_TEMPLATE", "").strip()
    if not provider_url and not template:
        print("FX_RATE_PROVIDER_URL and FX_RATE_URL_TEMPLATE are both missing")
        return 1
    if not provider_url and (not fx_key or fx_key == "REPLACE_ME"):
        print("FX_API_KEY is missing or placeholder (template mode)")
        return 1

    base = args.base.strip().upper()
    quote = args.quote.strip().upper()
    default_path = "rates.{QUOTE}" if provider_url else "conversion_rate"
    json_path = (os.getenv("FX_RATE_JSON_PATH", default_path).strip() or default_path)
    cache_seconds = int(os.getenv("FX_CACHE_SECONDS", "900") or "900")
    provider = os.getenv("FX_PROVIDER", "open_er_api").strip() or "open_er_api"
    resolved_path = json_path.replace("{BASE}", base).replace("{QUOTE}", quote)
    if provider_url:
        url = (
            provider_url.replace("{FX_API_KEY}", urllib.parse.quote_plus(fx_key))
            .replace("{BASE}", urllib.parse.quote_plus(base))
            .replace("{QUOTE}", urllib.parse.quote_plus(quote))
        )
    else:
        url = (
            template.replace("{FX_API_KEY}", urllib.parse.quote_plus(fx_key))
            .replace("{BASE}", urllib.parse.quote_plus(base))
            .replace("{QUOTE}", urllib.parse.quote_plus(quote))
        )

    if not args.no_cache and cache_seconds > 0:
        cache = read_cache(CACHE_PATH)
        now = int(time.time())
        cached = cache.get("latest", {})
        if (
            isinstance(cached, dict)
            and cached.get("provider") == provider
            and cached.get("base") == base
            and cached.get("quote") == quote
            and isinstance(cached.get("rate"), (int, float))
            and isinstance(cached.get("fetched_at"), int)
            and now - cached["fetched_at"] <= cache_seconds
        ):
            print(
                json.dumps(
                    {
                        "status": 200,
                        "base": base,
                        "quote": quote,
                        "rate": cached["rate"],
                        "source": "cache",
                        "fetched_at": cached["fetched_at"],
                    },
                    ensure_ascii=False,
                )
            )
            return 0

    status, _, payload = request_json(url, timeout=args.timeout)
    rate = extract_json_path(payload, resolved_path)
    if status != 200 or not isinstance(rate, (int, float)) or rate <= 0:
        fallback_raw = os.getenv("FX_USD_JPY", "").strip()
        fallback_rate = None
        try:
            fallback_rate = float(fallback_raw)
        except ValueError:
            fallback_rate = None
        if fallback_rate and fallback_rate > 0:
            print(
                json.dumps(
                    {
                        "status": status,
                        "base": base,
                        "quote": quote,
                        "rate": fallback_rate,
                        "source": "fallback_env",
                        "path": resolved_path,
                        "payload": payload,
                    },
                    ensure_ascii=False,
                )
            )
            return 0
        print(
            json.dumps(
                {"status": status, "path": resolved_path, "rate": rate, "payload": payload},
                ensure_ascii=False,
            )
        )
        return 1

    fetched_at = int(time.time())
    write_cache(
        CACHE_PATH,
        {
            "latest": {
                "provider": provider,
                "base": base,
                "quote": quote,
                "rate": rate,
                "fetched_at": fetched_at,
            }
        },
    )
    print(
        json.dumps(
            {
                "status": status,
                "base": base,
                "quote": quote,
                "rate": rate,
                "source": "api",
                "fetched_at": fetched_at,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

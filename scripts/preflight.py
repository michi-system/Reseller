#!/usr/bin/env python3
"""Pre-validation checks for eBay, Yahoo Shopping, and Rakuten APIs."""

from __future__ import annotations

import argparse
import base64
import os
import sys
import urllib.parse
from pathlib import Path
from typing import Dict, Tuple


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.local"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from reselling.env import load_dotenv
from reselling.http_json import request_json
from reselling.json_utils import extract_json_path


def mask(value: str) -> str:
    if not value:
        return "<empty>"
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def first_env(*keys: str) -> str:
    for key in keys:
        val = os.getenv(key, "").strip()
        if val:
            return val
    return ""


def print_result(name: str, ok: bool, detail: str) -> None:
    status = "OK" if ok else "NG"
    print(f"[{status}] {name}: {detail}")


def require_env() -> Tuple[bool, Dict[str, str]]:
    env_map = {
        "EBAY_CLIENT_ID": os.getenv("EBAY_CLIENT_ID", "").strip(),
        "EBAY_CLIENT_SECRET": os.getenv("EBAY_CLIENT_SECRET", "").strip(),
        "YAHOO_APP_ID": first_env("YAHOO_APP_ID", "YAHOO_CLIENT_ID"),
        "RAKUTEN_APPLICATION_ID": os.getenv("RAKUTEN_APPLICATION_ID", "").strip(),
    }
    all_ok = True
    for key, value in env_map.items():
        ok = bool(value)
        all_ok = all_ok and ok
        print_result(f"env:{key}", ok, mask(value) if ok else "missing")
    fx_key = os.getenv("FX_API_KEY", "").strip()
    fx_ok = bool(fx_key and fx_key != "REPLACE_ME")
    print_result("env:FX_API_KEY", fx_ok, "set" if fx_ok else "placeholder/missing")
    fx_tpl = os.getenv("FX_RATE_URL_TEMPLATE", "").strip()
    fx_tpl_ok = bool(fx_tpl)
    print_result("env:FX_RATE_URL_TEMPLATE", fx_tpl_ok, "set" if fx_tpl_ok else "missing")
    fx_provider_url = os.getenv("FX_RATE_PROVIDER_URL", "").strip()
    fx_provider_ok = bool(fx_provider_url)
    print_result("env:FX_RATE_PROVIDER_URL", fx_provider_ok, "set" if fx_provider_ok else "missing")
    fx_config_ok = fx_provider_ok or (fx_ok and fx_tpl_ok)
    print_result("env:FX_CONFIG", fx_config_ok, "ready" if fx_config_ok else "missing usable config")
    return all_ok, env_map


def check_ebay(query: str, timeout: int) -> bool:
    client_id = os.getenv("EBAY_CLIENT_ID", "").strip()
    client_secret = os.getenv("EBAY_CLIENT_SECRET", "").strip()
    marketplace = os.getenv("TARGET_MARKETPLACE", "EBAY_US").strip() or "EBAY_US"
    if not client_id or not client_secret:
        print_result("eBay", False, "missing credentials")
        return False

    token_url = "https://api.ebay.com/identity/v1/oauth2/token"
    scope = "https://api.ebay.com/oauth/api_scope"
    token_body = urllib.parse.urlencode(
        {"grant_type": "client_credentials", "scope": scope}
    ).encode("utf-8")
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
    status, _, payload = request_json(
        token_url,
        method="POST",
        data=token_body,
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=timeout,
    )
    token = payload.get("access_token", "")
    if status != 200 or not token:
        print_result("eBay:token", False, f"http={status} payload={payload}")
        return False
    print_result("eBay:token", True, "issued")

    search_params = urllib.parse.urlencode({"q": query, "limit": "1"})
    search_url = f"https://api.ebay.com/buy/browse/v1/item_summary/search?{search_params}"
    status, _, payload = request_json(
        search_url,
        headers={
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": marketplace,
        },
        timeout=timeout,
    )
    total = payload.get("total")
    ok = status == 200 and total is not None
    detail = f"http={status} total={total}"
    if not ok:
        detail = f"{detail} payload={payload}"
    print_result("eBay:search", ok, detail)
    return ok


def check_yahoo(query: str, timeout: int) -> bool:
    app_id = first_env("YAHOO_APP_ID", "YAHOO_CLIENT_ID")
    if not app_id:
        print_result("Yahoo", False, "missing app id")
        return False

    params = urllib.parse.urlencode(
        {
            "appid": app_id,
            "query": query,
            "results": "1",
            "sort": "-score",
        }
    )
    url = f"https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch?{params}"
    status, headers, payload = request_json(url, timeout=timeout)
    hits = payload.get("totalResultsAvailable")
    ok = status == 200 and hits is not None
    detail = f"http={status} totalResultsAvailable={hits}"
    if not ok:
        auth_error = headers.get("x-yahooj-autherror")
        if auth_error:
            detail = f"{detail} authError={auth_error}"
        detail = f"{detail} payload={payload}"
    print_result("Yahoo:itemSearch", ok, detail)
    return ok


def check_rakuten(query: str, timeout: int) -> bool:
    app_id = os.getenv("RAKUTEN_APPLICATION_ID", "").strip()
    if not app_id:
        print_result("Rakuten", False, "missing application id")
        return False

    params = {
        "applicationId": app_id,
        "keyword": query,
        "hits": "1",
        "page": "1",
        "format": "json",
    }
    access_key = os.getenv("RAKUTEN_PUBLIC_KEY", "").strip()
    if access_key:
        params["accessKey"] = access_key
    affiliate_id = os.getenv("RAKUTEN_AFFILIATE_ID", "").strip()
    if affiliate_id:
        params["affiliateId"] = affiliate_id
    base_url = os.getenv(
        "RAKUTEN_API_BASE_URL",
        "https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20220601",
    ).strip()
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    status, _, payload = request_json(url, timeout=timeout)
    count = payload.get("count")
    ok = status == 200 and count is not None
    detail = f"http={status} count={count}"
    if not ok:
        detail = f"{detail} payload={payload}"
    print_result("Rakuten:IchibaItemSearch", ok, detail)
    return ok


def check_fx(timeout: int) -> bool:
    fx_key = os.getenv("FX_API_KEY", "").strip()
    provider_url = os.getenv("FX_RATE_PROVIDER_URL", "").strip()
    template = os.getenv("FX_RATE_URL_TEMPLATE", "").strip()
    if not provider_url and not template:
        print_result("FX", False, "missing FX_RATE_PROVIDER_URL and FX_RATE_URL_TEMPLATE")
        return False
    if not provider_url and (not fx_key or fx_key == "REPLACE_ME"):
        print_result("FX", False, "missing FX_API_KEY for template mode")
        return False

    base = os.getenv("FX_BASE_CCY", "USD").strip() or "USD"
    quote = os.getenv("FX_QUOTE_CCY", "JPY").strip() or "JPY"
    default_path = "rates.{QUOTE}" if provider_url else "conversion_rate"
    json_path = os.getenv("FX_RATE_JSON_PATH", default_path).strip() or default_path
    resolved_path = json_path.replace("{BASE}", base).replace("{QUOTE}", quote)
    if provider_url:
        url = (
            provider_url.replace("{FX_API_KEY}", urllib.parse.quote_plus(fx_key))
            .replace("{BASE}", urllib.parse.quote_plus(base))
            .replace("{QUOTE}", urllib.parse.quote_plus(quote))
        )
        mode = "provider_url"
    else:
        url = (
            template.replace("{FX_API_KEY}", urllib.parse.quote_plus(fx_key))
            .replace("{BASE}", urllib.parse.quote_plus(base))
            .replace("{QUOTE}", urllib.parse.quote_plus(quote))
        )
        mode = "template"
    status, _, payload = request_json(url, timeout=timeout)
    raw_rate = extract_json_path(payload, resolved_path)
    ok = status == 200 and isinstance(raw_rate, (int, float)) and raw_rate > 0
    detail = (
        f"http={status} mode={mode} base={base} quote={quote} "
        f"path={resolved_path} rate={raw_rate}"
    )
    if not ok:
        detail = f"{detail} payload={payload}"
    print_result("FX:rate", ok, detail)
    return ok


def check_liquidity_config() -> bool:
    gate_enabled = (os.getenv("LIQUIDITY_GATE_ENABLED", "1") or "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not gate_enabled:
        print_result("LIQUIDITY:gate", True, "disabled")
        return True

    require_signal = (os.getenv("LIQUIDITY_REQUIRE_SIGNAL", "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    fallback_mode = (os.getenv("LIQUIDITY_FALLBACK_MODE", "none") or "none").strip().lower()
    mode = (os.getenv("LIQUIDITY_PROVIDER_MODE", "none") or "none").strip().lower()
    if mode in {"none", "off", "disabled"}:
        ok = not require_signal
        detail = "provider disabled"
        if require_signal:
            detail += " (LIQUIDITY_REQUIRE_SIGNAL=1 のためNG)"
        print_result("LIQUIDITY:provider", ok, detail)
        return ok
    if mode == "mock":
        sold = (os.getenv("LIQUIDITY_MOCK_SOLD_90D", "") or "").strip()
        ok = bool(sold)
        print_result("LIQUIDITY:provider", ok, f"mode=mock sold_90d={sold or '<unset>'}")
        return ok
    if mode in {"http", "http_json"}:
        tpl = (os.getenv("LIQUIDITY_PROVIDER_URL_TEMPLATE", "") or "").strip()
        ok = bool(tpl)
        print_result("LIQUIDITY:provider", ok, "mode=http_json template set" if ok else "template missing")
        return ok
    if mode in {"rpa", "rpa_json"}:
        raw_path = (os.getenv("LIQUIDITY_RPA_JSON_PATH", "") or "").strip() or "data/liquidity_rpa_signals.jsonl"
        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            path = (ROOT_DIR / path).resolve()
        ok = path.exists()
        print_result("LIQUIDITY:provider", ok, f"mode=rpa_json path={path} {'exists' if ok else 'missing'}")
        return ok
    if mode in {"ebay_marketplace_insights", "insights"}:
        urls = (os.getenv("LIQUIDITY_INSIGHTS_URLS", "") or "").strip()
        endpoint = (
            os.getenv(
                "LIQUIDITY_INSIGHTS_URL",
                "https://api.ebay.com/buy/marketplace_insights/v1_beta/item_sales/search",
            )
            or ""
        ).strip()
        has_endpoint = bool(endpoint) or bool(urls)
        has_id = bool((os.getenv("EBAY_CLIENT_ID", "") or "").strip())
        has_secret = bool((os.getenv("EBAY_CLIENT_SECRET", "") or "").strip())
        ok = bool(has_endpoint and has_id and has_secret)
        detail = (
            f"mode=insights endpoint={'set' if has_endpoint else 'missing'} "
            f"creds={'ok' if has_id and has_secret else 'missing'}"
        )
        print_result("LIQUIDITY:provider", ok, detail)
        fallback_ok = True
        fallback_detail = f"fallback={fallback_mode}"
        if fallback_mode in {"none", "off", "disabled"}:
            fallback_ok = True
        elif fallback_mode in {"http", "http_json"}:
            tpl = (os.getenv("LIQUIDITY_PROVIDER_URL_TEMPLATE", "") or "").strip()
            fallback_ok = bool(tpl)
            fallback_detail += " template set" if fallback_ok else " template missing"
        elif fallback_mode in {"rpa", "rpa_json"}:
            raw_path = (os.getenv("LIQUIDITY_RPA_JSON_PATH", "") or "").strip() or "data/liquidity_rpa_signals.jsonl"
            path = Path(raw_path).expanduser()
            if not path.is_absolute():
                path = (ROOT_DIR / path).resolve()
            fallback_ok = path.exists()
            fallback_detail += f" rpa_json={'ok' if fallback_ok else 'missing'}"
        elif fallback_mode == "mock":
            fallback_ok = True
        else:
            fallback_ok = False
            fallback_detail += " unsupported"
        print_result("LIQUIDITY:fallback", fallback_ok, fallback_detail)
        return ok and fallback_ok
    print_result("LIQUIDITY:provider", False, f"unsupported mode={mode}")
    return False


def print_runtime_summary() -> None:
    print("\nRuntime settings")
    print(f"- TARGET_MARKETPLACE={os.getenv('TARGET_MARKETPLACE', 'EBAY_US')}")
    print(f"- TARGET_CATEGORY={os.getenv('TARGET_CATEGORY', 'watch_new')}")
    print(f"- ITEM_CONDITION={os.getenv('ITEM_CONDITION', 'new')}")
    print(f"- DEFAULT_QUERY={os.getenv('DEFAULT_QUERY', 'seiko watch')}")
    print(f"- EBAY_RATE_LIMIT_RPS={os.getenv('EBAY_RATE_LIMIT_RPS', '2')}")
    print(f"- YAHOO_RATE_LIMIT_RPS={os.getenv('YAHOO_RATE_LIMIT_RPS', '1')}")
    print(f"- RAKUTEN_RATE_LIMIT_RPS={os.getenv('RAKUTEN_RATE_LIMIT_RPS', '1')}")
    print(f"- FX_BASE_CCY={os.getenv('FX_BASE_CCY', 'USD')}")
    print(f"- FX_QUOTE_CCY={os.getenv('FX_QUOTE_CCY', 'JPY')}")
    print(f"- FX_PROVIDER={os.getenv('FX_PROVIDER', 'open_er_api')}")
    print(f"- FX_CACHE_SECONDS={os.getenv('FX_CACHE_SECONDS', '900')}")
    print(f"- FX_RATE_PROVIDER_URL={os.getenv('FX_RATE_PROVIDER_URL', '') or '<unset>'}")
    print(f"- LIQUIDITY_GATE_ENABLED={os.getenv('LIQUIDITY_GATE_ENABLED', '1')}")
    print(f"- LIQUIDITY_REQUIRE_SIGNAL={os.getenv('LIQUIDITY_REQUIRE_SIGNAL', '0')}")
    print(f"- LIQUIDITY_PROVIDER_MODE={os.getenv('LIQUIDITY_PROVIDER_MODE', 'none')}")
    print(f"- LIQUIDITY_FALLBACK_MODE={os.getenv('LIQUIDITY_FALLBACK_MODE', 'none')}")
    print(f"- EV90_MIN_USD={os.getenv('EV90_MIN_USD', '0')}")
    print(f"- EV90_ENFORCE_WITHOUT_LIQUIDITY={os.getenv('EV90_ENFORCE_WITHOUT_LIQUIDITY', '0')}")


def check_operation_policy(policy_path: Path) -> bool:
    if not policy_path.exists():
        print_result("POLICY:file", False, f"missing: {policy_path}")
        return False
    try:
        payload = json.loads(policy_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print_result("POLICY:file", False, f"invalid JSON: {exc}")
        return False
    if not isinstance(payload, dict):
        print_result("POLICY:file", False, "root must be object")
        return False
    print_result("POLICY:file", True, f"loaded: {policy_path}")
    env_req = payload.get("env_requirements", {})
    if not isinstance(env_req, dict):
        env_req = {}
    ok = True
    expected_condition = str(env_req.get("ITEM_CONDITION", "new") or "new").strip().lower()
    actual_condition = (os.getenv("ITEM_CONDITION", "new") or "new").strip().lower()
    cond_ok = actual_condition == expected_condition
    ok = ok and cond_ok
    print_result("POLICY:ITEM_CONDITION", cond_ok, f"{actual_condition} (expected {expected_condition})")

    expected_liq = str(env_req.get("LIQUIDITY_REQUIRE_SIGNAL", "1") or "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    actual_liq = (os.getenv("LIQUIDITY_REQUIRE_SIGNAL", "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    liq_ok = (actual_liq == expected_liq) if isinstance(expected_liq, bool) else True
    ok = ok and liq_ok
    print_result("POLICY:LIQUIDITY_REQUIRE_SIGNAL", liq_ok, f"{actual_liq} (expected {expected_liq})")

    expected_auto_liq = str(env_req.get("AUTO_MINER_REQUIRE_LIQUIDITY_SIGNAL", "1") or "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    actual_auto_liq = (os.getenv("AUTO_MINER_REQUIRE_LIQUIDITY_SIGNAL", "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    auto_liq_ok = (actual_auto_liq == expected_auto_liq) if isinstance(expected_auto_liq, bool) else True
    ok = ok and auto_liq_ok
    print_result(
        "POLICY:AUTO_MINER_REQUIRE_LIQUIDITY_SIGNAL",
        auto_liq_ok,
        f"{actual_auto_liq} (expected {expected_auto_liq})",
    )

    allowed_modes = env_req.get("LIQUIDITY_PROVIDER_MODE_allowed", [])
    if not isinstance(allowed_modes, list):
        allowed_modes = []
    allowed_set = {str(v or "").strip().lower() for v in allowed_modes if str(v or "").strip()}
    actual_mode = (os.getenv("LIQUIDITY_PROVIDER_MODE", "none") or "none").strip().lower()
    mode_ok = True if not allowed_set else actual_mode in allowed_set
    ok = ok and mode_ok
    print_result(
        "POLICY:LIQUIDITY_PROVIDER_MODE",
        mode_ok,
        f"{actual_mode} (allowed={sorted(allowed_set) if allowed_set else ['*']})",
    )
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description="Pre-validation checks for API readiness.")
    parser.add_argument(
        "--online",
        action="store_true",
        help="Run one lightweight API request per site after env checks.",
    )
    parser.add_argument(
        "--query",
        default=os.getenv("DEFAULT_QUERY", "seiko watch"),
        help="Query used for online smoke tests.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=15,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--require-fx",
        action="store_true",
        help="Fail the run unless FX rate check succeeds.",
    )
    parser.add_argument(
        "--policy-file",
        default=str(ROOT_DIR / "docs" / "OPERATION_POLICY.json"),
        help="Operation policy JSON to validate fixed env requirements.",
    )
    parser.add_argument(
        "--skip-policy-check",
        action="store_true",
        help="Skip operation policy check.",
    )
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    print("Preflight: env checks")
    env_ok, _ = require_env()
    policy_ok = True
    if not bool(args.skip_policy_check):
        policy_ok = check_operation_policy(Path(str(args.policy_file or "").strip()))
    print_runtime_summary()

    if not args.online:
        print("\nDry run complete (no network). Use --online for live checks.")
        return 0 if (env_ok and policy_ok) else 1

    if not env_ok or not policy_ok:
        print("\nOnline checks skipped because env/policy checks failed.")
        return 1

    print(f"\nOnline checks with query={args.query!r}")
    ebay_ok = check_ebay(args.query, args.timeout)
    yahoo_ok = check_yahoo(args.query, args.timeout)
    rakuten_ok = check_rakuten(args.query, args.timeout)
    fx_key = os.getenv("FX_API_KEY", "").strip()
    has_fx_key = bool(fx_key and fx_key != "REPLACE_ME")
    has_fx_provider = bool(os.getenv("FX_RATE_PROVIDER_URL", "").strip())
    has_fx_template = bool(os.getenv("FX_RATE_URL_TEMPLATE", "").strip())
    has_fx_config = has_fx_provider or (has_fx_template and has_fx_key)
    should_check_fx = args.require_fx or has_fx_config
    fx_ok = True
    if should_check_fx:
        fx_ok = check_fx(args.timeout)
    liquidity_ok = check_liquidity_config()
    all_ok = ebay_ok and yahoo_ok and rakuten_ok and fx_ok and liquidity_ok
    print(f"\nPreflight online result: {'PASS' if all_ok else 'FAIL'}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())

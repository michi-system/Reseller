#!/usr/bin/env python3
"""Pilot runner to find query breadth sweet spots per marketplace API."""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.local"
EBAY_TOKEN_CACHE: Optional[str] = None


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


def request_json(
    url: str,
    *,
    method: str = "GET",
    data: bytes | None = None,
    headers: Dict[str, str] | None = None,
    timeout: int = 20,
) -> Tuple[int, Dict[str, str], Dict]:
    req = urllib.request.Request(url=url, data=data, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            payload = json.loads(raw) if raw else {}
            return int(resp.status), dict(resp.headers.items()), payload
    except Exception as err:  # urllib HTTPError/URLError
        code = getattr(err, "code", 0)
        hdrs = dict(getattr(err, "headers", {}).items()) if hasattr(err, "headers") else {}
        body = ""
        if hasattr(err, "read"):
            try:
                body = err.read().decode("utf-8", errors="replace")
            except Exception:
                body = str(err)
        payload = {}
        if body:
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                payload = {"raw": body[:400]}
        else:
            payload = {"error": str(err)}
        return int(code), hdrs, payload


def request_with_retry(
    url: str,
    *,
    method: str = "GET",
    data: bytes | None = None,
    headers: Dict[str, str] | None = None,
    timeout: int = 20,
    retries: int = 1,
) -> Tuple[int, Dict[str, str], Dict]:
    status, resp_headers, payload = request_json(
        url, method=method, data=data, headers=headers, timeout=timeout
    )
    if status != 429 or retries <= 0:
        return status, resp_headers, payload

    retry_after_raw = (resp_headers.get("Retry-After") or "").strip()
    wait_seconds = 2.0
    try:
        wait_seconds = max(0.5, float(retry_after_raw))
    except ValueError:
        wait_seconds = 2.0
    time.sleep(wait_seconds)
    return request_with_retry(
        url,
        method=method,
        data=data,
        headers=headers,
        timeout=timeout,
        retries=retries - 1,
    )


def as_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def ebay_token(timeout: int) -> Optional[str]:
    client_id = os.getenv("EBAY_CLIENT_ID", "").strip()
    client_secret = os.getenv("EBAY_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        return None
    url = "https://api.ebay.com/identity/v1/oauth2/token"
    scope = "https://api.ebay.com/oauth/api_scope"
    body = urllib.parse.urlencode({"grant_type": "client_credentials", "scope": scope}).encode(
        "utf-8"
    )
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
    status, _, payload = request_with_retry(
        url,
        method="POST",
        data=body,
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=timeout,
    )
    if status != 200:
        return None
    return payload.get("access_token")


def run_ebay_stage(query: str, timeout: int) -> Dict:
    global EBAY_TOKEN_CACHE
    token = EBAY_TOKEN_CACHE
    if token is None:
        token = ebay_token(timeout)
        EBAY_TOKEN_CACHE = token
    if not token:
        return {"ok": False, "http": 0, "count": None, "error": "token_issue"}

    marketplace = os.getenv("TARGET_MARKETPLACE", "EBAY_US").strip() or "EBAY_US"
    params = urllib.parse.urlencode({"q": query, "limit": "1"})
    url = f"https://api.ebay.com/buy/browse/v1/item_summary/search?{params}"
    status, _, payload = request_with_retry(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": marketplace,
        },
        timeout=timeout,
    )
    count = payload.get("total")
    ok = status == 200 and count is not None
    return {"ok": ok, "http": status, "count": as_int(count, -1), "error": None if ok else payload}


def run_yahoo_stage(query: str, timeout: int) -> Dict:
    appid = (os.getenv("YAHOO_APP_ID") or os.getenv("YAHOO_CLIENT_ID") or "").strip()
    if not appid:
        return {"ok": False, "http": 0, "count": None, "error": "missing_app_id"}
    params = urllib.parse.urlencode({"appid": appid, "query": query, "results": "1", "sort": "-score"})
    url = f"https://shopping.yahooapis.jp/ShoppingWebService/V3/itemSearch?{params}"
    status, headers, payload = request_with_retry(url, timeout=timeout)
    count = payload.get("totalResultsAvailable")
    ok = status == 200 and count is not None
    error = None
    if not ok:
        error = {
            "authError": headers.get("x-yahooj-autherror"),
            "payload": payload,
        }
    return {"ok": ok, "http": status, "count": as_int(count, -1), "error": error}


def run_rakuten_stage(query: str, timeout: int) -> Dict:
    appid = os.getenv("RAKUTEN_APPLICATION_ID", "").strip()
    if not appid:
        return {"ok": False, "http": 0, "count": None, "error": "missing_application_id"}
    query_params = {
        "applicationId": appid,
        "keyword": query,
        "hits": "1",
        "page": "1",
        "format": "json",
    }
    access_key = os.getenv("RAKUTEN_PUBLIC_KEY", "").strip()
    if access_key:
        query_params["accessKey"] = access_key
    params = urllib.parse.urlencode(query_params)
    base_url = os.getenv(
        "RAKUTEN_API_BASE_URL",
        "https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20220601",
    ).strip()
    url = f"{base_url}?{params}"
    status, _, payload = request_with_retry(url, timeout=timeout)
    count = payload.get("count")
    ok = status == 200 and count is not None
    return {"ok": ok, "http": status, "count": as_int(count, -1), "error": None if ok else payload}


def default_stages(brand: str, model: str, noun: str) -> List[Tuple[str, str]]:
    brand = brand.strip()
    model = model.strip()
    noun = noun.strip()
    return [
        ("L1_precise_new", f"{brand} {model} {noun} new".strip()),
        ("L2_precise", f"{brand} {model} {noun}".strip()),
        ("L3_mid", f"{brand} {model}".strip()),
        ("L4_broad", f"{brand} {noun}".strip()),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run staged query-width pilot checks per site.")
    parser.add_argument("--brand", default="seiko")
    parser.add_argument("--model", default="sbga211")
    parser.add_argument("--noun", default="watch")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument(
        "--sites",
        default="ebay,yahoo,rakuten",
        help="Comma-separated: ebay,yahoo,rakuten",
    )
    parser.add_argument(
        "--output",
        default=str(ROOT_DIR / "docs" / "query_width_report.json"),
        help="Path for JSON report output.",
    )
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    sites = [s.strip().lower() for s in args.sites.split(",") if s.strip()]
    stages = default_stages(args.brand, args.model, args.noun)
    rps_map = {
        "ebay": float(os.getenv("EBAY_RATE_LIMIT_RPS", "2") or "2"),
        "yahoo": float(os.getenv("YAHOO_RATE_LIMIT_RPS", "1") or "1"),
        "rakuten": float(os.getenv("RAKUTEN_RATE_LIMIT_RPS", "1") or "1"),
    }
    min_map = {
        "ebay": int(os.getenv("EBAY_MIN_CANDIDATES", "20") or "20"),
        "yahoo": int(os.getenv("YAHOO_MIN_CANDIDATES", "20") or "20"),
        "rakuten": int(os.getenv("RAKUTEN_MIN_CANDIDATES", "10") or "10"),
    }
    max_map = {
        "ebay": int(os.getenv("EBAY_MAX_CANDIDATES", "2000") or "2000"),
        "yahoo": int(os.getenv("YAHOO_MAX_CANDIDATES", "1000") or "1000"),
        "rakuten": int(os.getenv("RAKUTEN_MAX_CANDIDATES", "1000") or "1000"),
    }
    runners = {
        "ebay": run_ebay_stage,
        "yahoo": run_yahoo_stage,
        "rakuten": run_rakuten_stage,
    }

    report: Dict[str, object] = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "inputs": {
            "brand": args.brand,
            "model": args.model,
            "noun": args.noun,
            "sites": sites,
        },
        "sites": {},
    }

    for site in sites:
        if site not in runners:
            continue
        runner = runners[site]
        interval = 1.0 / rps_map[site] if rps_map[site] > 0 else 1.0
        print(f"\n[{site}] staged queries (interval={interval:.2f}s)")
        site_rows: List[Dict[str, object]] = []
        prev_count: Optional[int] = None
        for label, query in stages:
            started = time.time()
            result = runner(query, args.timeout)
            count = result.get("count")
            delta = None
            if isinstance(count, int) and count >= 0 and prev_count is not None and prev_count >= 0:
                delta = count - prev_count
            if isinstance(count, int) and count >= 0:
                prev_count = count
            row = {
                "stage": label,
                "query": query,
                "http": result.get("http"),
                "ok": result.get("ok"),
                "count": count,
                "delta_from_prev": delta,
                "error": result.get("error"),
            }
            site_rows.append(row)
            print(
                f"- {label}: http={row['http']} ok={row['ok']} count={row['count']} delta={row['delta_from_prev']}"
            )
            elapsed = time.time() - started
            if elapsed < interval:
                time.sleep(interval - elapsed)

        # Recommend by bounded candidate window first, then fallbacks.
        recommended_stage = None
        min_candidates = min_map[site]
        max_candidates = max_map[site]
        for row in site_rows:
            count = row["count"]
            if (
                row["ok"]
                and isinstance(count, int)
                and min_candidates <= count <= max_candidates
            ):
                recommended_stage = row["stage"]
                break
        if recommended_stage is None:
            for row in site_rows:
                count = row["count"]
                if row["ok"] and isinstance(count, int) and count >= min_candidates:
                    recommended_stage = row["stage"]
                    break
        if recommended_stage is None:
            for row in site_rows:
                if row["ok"]:
                    recommended_stage = row["stage"]
                    break

        report["sites"][site] = {
            "rate_limit_rps": rps_map[site],
            "min_candidates": min_candidates,
            "max_candidates": max_candidates,
            "rows": site_rows,
            "recommended_stage": recommended_stage,
        }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSaved report: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Shared HTTP JSON request helper."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple


def _normalize_payload(raw: str, *, wrap_non_dict: bool) -> Dict[str, Any]:
    if not raw:
        return {}
    payload = json.loads(raw)
    if isinstance(payload, dict):
        return payload
    if wrap_non_dict:
        return {"data": payload}
    return {}


def _headers_to_dict(headers: Any) -> Dict[str, str]:
    try:
        items = headers.items() if headers is not None else []
        return {str(k): str(v) for k, v in items}
    except Exception:
        return {}


def request_json(
    url: str,
    *,
    method: str = "GET",
    data: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 20,
    raw_limit: int = 500,
    wrap_non_dict: bool = False,
    catch_all: bool = False,
) -> Tuple[int, Dict[str, str], Dict[str, Any]]:
    req = urllib.request.Request(url=url, data=data, method=method)
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            payload = _normalize_payload(raw, wrap_non_dict=wrap_non_dict)
            return int(resp.status), _headers_to_dict(resp.headers), payload
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        try:
            payload = _normalize_payload(body, wrap_non_dict=wrap_non_dict)
        except json.JSONDecodeError:
            payload = {"raw": body[: max(0, int(raw_limit))]}
        return int(err.code), _headers_to_dict(err.headers), payload
    except urllib.error.URLError as err:
        return 0, {}, {"error": str(err)}
    except Exception as err:
        if not catch_all:
            raise
        code = int(getattr(err, "code", 0) or 0)
        body = ""
        if hasattr(err, "read"):
            try:
                body = err.read().decode("utf-8", errors="replace")
            except Exception:
                body = str(err)
        if body:
            try:
                payload = _normalize_payload(body, wrap_non_dict=wrap_non_dict)
            except json.JSONDecodeError:
                payload = {"raw": body[: max(0, int(raw_limit))]}
        else:
            payload = {"error": str(err)}
        return code, _headers_to_dict(getattr(err, "headers", None)), payload

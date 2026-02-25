"""Liquidity signal collection and gate helpers."""

from __future__ import annotations

import base64
import json
import os
import re
import statistics
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple

from .coerce import env_bool as _env_bool
from .coerce import env_float as _env_float
from .coerce import env_int as _env_int
from .coerce import to_float as _to_float
from .coerce import to_int as _to_int
from .config import ROOT_DIR, Settings, load_settings
from .http_json import request_json as _http_request_json
from .json_utils import extract_json_path as _extract_json_path
from .models import connect, init_db
from .time_utils import iso_to_epoch as _iso_to_epoch
from .time_utils import utc_iso as _utc_iso

_IDENTIFIER_KEYS = ("jan", "upc", "ean", "gtin", "mpn")
_CODE_RE = re.compile(r"[A-Z0-9][A-Z0-9-]{3,}")


def _extract_codes(text: str) -> Sequence[str]:
    normalized = re.sub(r"[^A-Z0-9-]+", " ", str(text or "").upper())
    out = []
    seen = set()
    for token in _CODE_RE.findall(normalized):
        code = str(token or "").strip("-")
        if len(code) < 4:
            continue
        if code.isdigit():
            continue
        alpha = sum(1 for ch in code if "A" <= ch <= "Z")
        digit = sum(1 for ch in code if ch.isdigit())
        if alpha < 1 or digit < 1:
            continue
        if code in seen:
            continue
        seen.add(code)
        out.append(code)
    return out


def _is_specific_code(code: str) -> bool:
    token = str(code or "").strip().upper()
    if not token:
        return False
    return len(token) >= 8 or token.count("-") >= 2


def _normalize_identifiers(value: Any) -> Dict[str, str]:
    if not isinstance(value, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in value.items():
        key = str(k or "").strip().lower()
        val = str(v or "").strip()
        if not key or not val:
            continue
        out[key] = val
    return out


def _compact_query(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def resolve_liquidity_key(
    *,
    query: str,
    source_title: str,
    market_title: str,
    source_identifiers: Optional[Dict[str, str]] = None,
    market_identifiers: Optional[Dict[str, str]] = None,
) -> str:
    source_ids = _normalize_identifiers(source_identifiers)
    market_ids = _normalize_identifiers(market_identifiers)

    for key in _IDENTIFIER_KEYS:
        source_val = str(source_ids.get(key, "") or "").strip()
        market_val = str(market_ids.get(key, "") or "").strip()
        if source_val and market_val and source_val == market_val:
            return f"{key}:{source_val}"
    for key in _IDENTIFIER_KEYS:
        market_val = str(market_ids.get(key, "") or "").strip()
        if market_val:
            return f"{key}:{market_val}"
    for key in _IDENTIFIER_KEYS:
        source_val = str(source_ids.get(key, "") or "").strip()
        if source_val:
            return f"{key}:{source_val}"

    source_codes = set(_extract_codes(source_title))
    market_codes = set(_extract_codes(market_title))
    common = sorted(source_codes & market_codes, key=lambda v: (-len(v), v))
    for code in common:
        if _is_specific_code(code):
            return f"model:{code}"
    if common:
        return f"code:{common[0]}"
    merged = sorted((source_codes | market_codes), key=lambda v: (-len(v), v))
    if merged:
        return f"code:{merged[0]}"

    fallback = _compact_query(query).upper()
    fallback_key = re.sub(r"[^A-Z0-9]+", "-", fallback).strip("-")
    if not fallback_key:
        fallback_key = "unknown"
    return f"query:{fallback_key[:80]}"


def _request_json(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 15,
) -> Tuple[int, Dict[str, str], Dict[str, Any]]:
    return _http_request_json(
        url,
        headers=headers,
        timeout=timeout,
        raw_limit=800,
        wrap_non_dict=True,
    )


def _ebay_access_token(timeout: int) -> str:
    client_id = (os.getenv("EBAY_CLIENT_ID", "") or "").strip()
    client_secret = (os.getenv("EBAY_CLIENT_SECRET", "") or "").strip()
    if not client_id or not client_secret:
        raise ValueError("EBAY_CLIENT_ID/EBAY_CLIENT_SECRET missing")

    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "scope": "https://api.ebay.com/oauth/api_scope",
        }
    ).encode("utf-8")
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
    req = urllib.request.Request(
        url="https://api.ebay.com/identity/v1/oauth2/token",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            payload = json.loads(raw) if raw else {}
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        raise ValueError(f"ebay_token_http_{int(err.code)}:{body[:240]}") from err
    token = str((payload or {}).get("access_token", "") or "")
    if not token:
        raise ValueError("ebay_token_empty")
    return token


def _derive_median_price_from_rows(rows: Sequence[Any]) -> float:
    prices: list[float] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ("soldPrice", "price", "value", "amount"):
            value = row.get(key)
            if isinstance(value, dict):
                value = value.get("value")
            num = _to_float(value, -1.0)
            if num > 0:
                prices.append(num)
                break
    if not prices:
        return -1.0
    return float(statistics.median(prices))


def _normalize_sold_price_min(
    *,
    sold_price_min: float,
    sold_price_median: float,
) -> tuple[float, bool, float]:
    """Normalize sold min price by dropping extreme outliers.

    When sold_price_min is far below sold_price_median, it is often caused by
    accessory/parts contamination in sold data. In that case, we ignore min.
    """

    min_price = _to_float(sold_price_min, -1.0)
    median_price = _to_float(sold_price_median, -1.0)
    ratio_threshold = max(
        0.0,
        min(0.99, _env_float("LIQUIDITY_SOLD_PRICE_MIN_OUTLIER_RATIO", 0.35)),
    )
    if min_price <= 0:
        return -1.0, False, -1.0
    if median_price > 0 and ratio_threshold > 0:
        ratio = min_price / median_price
        if ratio < ratio_threshold:
            return -1.0, True, ratio
        return min_price, False, ratio
    return min_price, False, -1.0


def _to_signal_dict(
    *,
    signal_key: str,
    sold_90d_count: int,
    active_count: int,
    sold_price_median: float,
    sold_price_currency: str,
    source: str,
    confidence: float,
    unavailable_reason: str,
    metadata: Optional[Dict[str, Any]] = None,
    fetched_at_ts: Optional[float] = None,
    next_refresh_ts: Optional[float] = None,
    from_cache: bool = False,
) -> Dict[str, Any]:
    sold_count = int(sold_90d_count)
    active = int(active_count)
    if sold_count >= 0 and active >= 0 and (sold_count + active) > 0:
        sell_through = float(sold_count) / float(sold_count + active)
    else:
        sell_through = -1.0

    now_ts = float(fetched_at_ts) if fetched_at_ts is not None else time.time()
    if next_refresh_ts is None:
        ttl_ok = max(300, _env_int("LIQUIDITY_CACHE_SECONDS", 43200))
        ttl_unavailable = max(120, _env_int("LIQUIDITY_CACHE_SECONDS_UNAVAILABLE", 1800))
        ttl = ttl_ok if sold_count >= 0 else ttl_unavailable
        next_refresh_ts = now_ts + ttl

    return {
        "signal_key": signal_key,
        "sold_90d_count": sold_count,
        "active_count": active,
        "sell_through_90d": round(sell_through, 6) if sell_through >= 0 else -1.0,
        "sold_price_median": round(_to_float(sold_price_median, -1.0), 4),
        "sold_price_currency": str(sold_price_currency or "USD"),
        "source": str(source or "unknown"),
        "confidence": round(max(0.0, min(1.0, float(confidence))), 4),
        "unavailable_reason": str(unavailable_reason or ""),
        "fetched_at": _utc_iso(now_ts),
        "next_refresh_at": _utc_iso(float(next_refresh_ts)),
        "from_cache": bool(from_cache),
        "metadata": metadata if isinstance(metadata, dict) else {},
    }


def _load_cached_signal(settings: Settings, signal_key: str) -> Optional[Dict[str, Any]]:
    now_ts = int(time.time())
    with connect(settings.db_path) as conn:
        init_db(conn)
        row = conn.execute(
            """
            SELECT signal_key, sold_90d_count, active_count, sell_through_90d,
                   sold_price_median, sold_price_currency, source, confidence,
                   unavailable_reason, fetched_at, next_refresh_at, metadata_json
            FROM liquidity_signals
            WHERE signal_key = ?
            """,
            (signal_key,),
        ).fetchone()
    if row is None:
        return None
    next_refresh_ts = _iso_to_epoch(str(row["next_refresh_at"] or ""))
    if next_refresh_ts > 0 and now_ts >= next_refresh_ts:
        return None
    metadata_raw = str(row["metadata_json"] or "{}")
    try:
        metadata = json.loads(metadata_raw)
        if not isinstance(metadata, dict):
            metadata = {}
    except json.JSONDecodeError:
        metadata = {"_raw": metadata_raw}
    return _to_signal_dict(
        signal_key=str(row["signal_key"] or signal_key),
        sold_90d_count=_to_int(row["sold_90d_count"], -1),
        active_count=_to_int(row["active_count"], -1),
        sold_price_median=_to_float(row["sold_price_median"], -1.0),
        sold_price_currency=str(row["sold_price_currency"] or "USD"),
        source=str(row["source"] or ""),
        confidence=_to_float(row["confidence"], 0.0),
        unavailable_reason=str(row["unavailable_reason"] or ""),
        metadata=metadata,
        fetched_at_ts=float(_iso_to_epoch(str(row["fetched_at"] or "")) or now_ts),
        next_refresh_ts=float(next_refresh_ts or now_ts),
        from_cache=True,
    )


def _save_signal(settings: Settings, signal: Dict[str, Any]) -> None:
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    with connect(settings.db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            INSERT INTO liquidity_signals (
                signal_key, sold_90d_count, active_count, sell_through_90d,
                sold_price_median, sold_price_currency, source, confidence,
                unavailable_reason, fetched_at, next_refresh_at, metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(signal_key) DO UPDATE SET
                sold_90d_count = excluded.sold_90d_count,
                active_count = excluded.active_count,
                sell_through_90d = excluded.sell_through_90d,
                sold_price_median = excluded.sold_price_median,
                sold_price_currency = excluded.sold_price_currency,
                source = excluded.source,
                confidence = excluded.confidence,
                unavailable_reason = excluded.unavailable_reason,
                fetched_at = excluded.fetched_at,
                next_refresh_at = excluded.next_refresh_at,
                metadata_json = excluded.metadata_json
            """,
            (
                str(signal.get("signal_key", "") or "").strip(),
                _to_int(signal.get("sold_90d_count"), -1),
                _to_int(signal.get("active_count"), -1),
                _to_float(signal.get("sell_through_90d"), -1.0),
                _to_float(signal.get("sold_price_median"), -1.0),
                str(signal.get("sold_price_currency", "USD") or "USD"),
                str(signal.get("source", "") or ""),
                _to_float(signal.get("confidence"), 0.0),
                str(signal.get("unavailable_reason", "") or ""),
                str(signal.get("fetched_at", "") or _utc_iso()),
                str(signal.get("next_refresh_at", "") or _utc_iso()),
                json.dumps(metadata, ensure_ascii=False),
            ),
        )
        conn.commit()


def _provider_http_json(
    *,
    query: str,
    signal_key: str,
    timeout: int,
    active_count_hint: int,
) -> Tuple[Optional[Dict[str, Any]], str]:
    template = (os.getenv("LIQUIDITY_PROVIDER_URL_TEMPLATE", "") or "").strip()
    if not template:
        return None, "provider_url_template_missing"

    url = (
        template.replace("{QUERY}", urllib.parse.quote_plus(query))
        .replace("{KEY}", urllib.parse.quote_plus(signal_key))
    )
    headers: Dict[str, str] = {}
    bearer = (os.getenv("LIQUIDITY_PROVIDER_BEARER_TOKEN", "") or "").strip()
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"

    status, _, payload = _request_json(url, headers=headers, timeout=timeout)
    if status != 200:
        return None, f"provider_http_{status}"

    sold_path = (os.getenv("LIQUIDITY_JSON_SOLD_PATH", "sold_90d_count") or "sold_90d_count").strip()
    active_path = (os.getenv("LIQUIDITY_JSON_ACTIVE_PATH", "active_count") or "active_count").strip()
    median_path = (
        os.getenv("LIQUIDITY_JSON_MEDIAN_PATH", "sold_price_median") or "sold_price_median"
    ).strip()
    currency_path = (
        os.getenv("LIQUIDITY_JSON_CURRENCY_PATH", "sold_price_currency") or "sold_price_currency"
    ).strip()

    sold_90d_count = _to_int(_extract_json_path(payload, sold_path), -1)
    active_count = _to_int(_extract_json_path(payload, active_path), -1)
    sold_price_median = _to_float(_extract_json_path(payload, median_path), -1.0)
    sold_price_currency = str(_extract_json_path(payload, currency_path) or "USD")

    rows = payload.get("rows")
    if not isinstance(rows, list):
        rows = payload.get("items")
    if not isinstance(rows, list):
        rows = payload.get("results")
    if not isinstance(rows, list):
        rows = []

    if sold_90d_count < 0 and rows:
        sold_90d_count = len(rows)
    if sold_price_median <= 0 and rows:
        sold_price_median = _derive_median_price_from_rows(rows)

    if active_count < 0 and active_count_hint >= 0:
        active_count = active_count_hint

    if sold_90d_count < 0:
        return None, "provider_missing_sold_90d"

    signal = _to_signal_dict(
        signal_key=signal_key,
        sold_90d_count=sold_90d_count,
        active_count=active_count,
        sold_price_median=sold_price_median,
        sold_price_currency=sold_price_currency,
        source="http_json",
        confidence=0.86,
        unavailable_reason="",
        metadata={"provider_url": url},
    )
    return signal, ""


def _load_rpa_json_entries(path: Path) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    def _row_rank(row: Dict[str, Any]) -> tuple[int, int, int, float, int]:
        sold_90d = _to_int(row.get("sold_90d_count"), -1)
        sold_ok = 1 if sold_90d >= 0 else 0
        meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        pass_label = str(meta.get("pass_label", "") or "").strip().lower()
        if pass_label.startswith("primary_new"):
            pass_rank = 2
        elif pass_label.startswith("fallback_any"):
            pass_rank = 1
        else:
            pass_rank = 0
        confidence = _to_float(row.get("confidence"), 0.0)
        fetched_at = str(row.get("fetched_at", "") or row.get("updated_at", "") or "")
        fetched_epoch = _iso_to_epoch(fetched_at)
        return (sold_ok, pass_rank, sold_90d, confidence, fetched_epoch)

    def _upsert_best(bucket: Dict[str, Dict[str, Any]], key: str, row: Dict[str, Any]) -> None:
        existing = bucket.get(key)
        if not isinstance(existing, dict):
            bucket[key] = row
            return
        if _row_rank(row) >= _row_rank(existing):
            bucket[key] = row

    by_key: Dict[str, Dict[str, Any]] = {}
    by_query: Dict[str, Dict[str, Any]] = {}
    if not path.exists():
        return by_key, by_query
    suffix = path.suffix.lower()
    if suffix == ".json":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return by_key, by_query
        rows: list[dict[str, Any]] = []
        if isinstance(payload, list):
            rows = [row for row in payload if isinstance(row, dict)]
        elif isinstance(payload, dict):
            bucket = payload.get("signals")
            if isinstance(bucket, list):
                rows = [row for row in bucket if isinstance(row, dict)]
            else:
                rows = [payload]
        for row in rows:
            signal_key = str(row.get("signal_key", "") or "").strip()
            query = _compact_query(str(row.get("query", "") or "")).lower()
            if signal_key:
                _upsert_best(by_key, signal_key, row)
            if query:
                _upsert_best(by_query, query, row)
        return by_key, by_query

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        signal_key = str(row.get("signal_key", "") or "").strip()
        query = _compact_query(str(row.get("query", "") or "")).lower()
        if signal_key:
            _upsert_best(by_key, signal_key, row)
        if query:
            _upsert_best(by_query, query, row)
    return by_key, by_query


def _resolve_rpa_row(
    *,
    entries_by_key: Dict[str, Dict[str, Any]],
    entries_by_query: Dict[str, Dict[str, Any]],
    signal_key: str,
    query: str,
) -> Optional[Dict[str, Any]]:
    def _norm(text: str) -> str:
        return re.sub(r"[^A-Z0-9]+", "", str(text or "").upper())

    def _rank_row(row: Dict[str, Any]) -> tuple[int, float, int]:
        sold_ok = 1 if _to_int(row.get("sold_90d_count"), -1) >= 0 else 0
        conf = _to_float(row.get("confidence"), 0.0)
        fetched = _iso_to_epoch(str(row.get("fetched_at", "") or row.get("updated_at", "") or ""))
        return sold_ok, conf, fetched

    candidate_rows: list[Dict[str, Any]] = []
    seen_ids: set[int] = set()

    def _push_row(row: Any) -> None:
        if not isinstance(row, dict):
            return
        marker = id(row)
        if marker in seen_ids:
            return
        seen_ids.add(marker)
        candidate_rows.append(row)

    # まずは厳密一致（key/query）だけで解決する。
    _push_row(entries_by_key.get(signal_key))
    normalized_query = _compact_query(query).lower()
    if normalized_query:
        _push_row(entries_by_query.get(normalized_query))

    head = str(signal_key or "").split(":", 1)
    if len(head) == 2:
        prefix = str(head[0] or "").strip().lower()
        token = str(head[1] or "").strip().upper()
        if token and prefix in {"model", "code", "mpn", "gtin", "ean", "upc", "jan"}:
            token_norm = _norm(token)
            prefix_candidates: list[str] = [prefix, "model", "code"]
            seen_prefix: set[str] = set()
            ordered_prefixes: list[str] = []
            for candidate in prefix_candidates:
                name = str(candidate or "").strip().lower()
                if not name or name in seen_prefix:
                    continue
                seen_prefix.add(name)
                ordered_prefixes.append(name)
            for alt_prefix in ordered_prefixes:
                _push_row(entries_by_key.get(f"{alt_prefix}:{token}"))
                if token_norm and token_norm != token:
                    _push_row(entries_by_key.get(f"{alt_prefix}:{token_norm}"))

    if candidate_rows:
        candidate_rows.sort(key=_rank_row, reverse=True)
        return candidate_rows[0]

    # 互換維持のため、必要時のみ曖昧フォールバックを許可。
    if not _env_bool("LIQUIDITY_RPA_ALLOW_FUZZY_KEY_FALLBACK", False):
        return None

    row_by_query = entries_by_query.get(normalized_query) if normalized_query else None
    if isinstance(row_by_query, dict):
        return row_by_query
    head = str(signal_key or "").split(":", 1)
    if len(head) == 2:
        token = str(head[1] or "").strip().upper()
        if token:
            token_variants: list[str] = []
            token_variants.append(token)
            token_variants.append(_norm(token))
            if "-" in token:
                token_variants.append(token.split("-", 1)[0])
                token_variants.append(_norm(token.split("-", 1)[0]))
            if "_" in token:
                token_variants.append(token.split("_", 1)[0])
                token_variants.append(_norm(token.split("_", 1)[0]))
            token_variants = [x for x in token_variants if x]
            candidates: list[tuple[int, float, int, Dict[str, Any]]] = []
            for row in entries_by_key.values():
                hay = " ".join(
                    (
                        str(row.get("signal_key", "") or ""),
                        str(row.get("query", "") or ""),
                        json.dumps(row.get("metadata", {}), ensure_ascii=False),
                    )
                )
                hay_upper = hay.upper()
                hay_norm = _norm(hay_upper)
                matched = False
                best_len = 0
                for variant in token_variants:
                    variant_upper = str(variant or "").upper()
                    if not variant_upper:
                        continue
                    variant_norm = _norm(variant_upper)
                    if variant_upper in hay_upper or (variant_norm and variant_norm in hay_norm):
                        matched = True
                        best_len = max(best_len, len(variant_norm or variant_upper))
                if not matched:
                    continue
                sold_ok = 1 if _to_int(row.get("sold_90d_count"), -1) >= 0 else 0
                conf = _to_float(row.get("confidence"), 0.0)
                fetched = _iso_to_epoch(str(row.get("fetched_at", "") or row.get("updated_at", "") or ""))
                candidates.append((sold_ok * 1000 + best_len, conf, fetched, row))
            if candidates:
                candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
                return candidates[0][3]
    return None


def _specific_query_codes(text: str) -> set[str]:
    out: set[str] = set()
    for token in _extract_codes(text):
        canon = re.sub(r"[^A-Z0-9]+", "", str(token or "").upper())
        if not canon:
            continue
        alpha = sum(1 for ch in canon if "A" <= ch <= "Z")
        digit = sum(1 for ch in canon if ch.isdigit())
        if alpha >= 2 and digit >= 2 and len(canon) >= 6:
            out.add(canon)
    return out


def _rpa_row_has_strict_sold_filters(row: Dict[str, Any]) -> bool:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    filter_state = metadata.get("filter_state") if isinstance(metadata.get("filter_state"), dict) else {}
    early_no_sold = bool(filter_state.get("early_no_sold_detected")) or bool(metadata.get("no_sales_in_window_inferred"))
    if early_no_sold:
        return True
    sold_tab_selected = bool(filter_state.get("sold_tab_selected"))
    lookback_selected = str(filter_state.get("lookback_selected", "") or "").strip().lower()
    sold_90d_count = _to_int(row.get("sold_90d_count"), -1)
    # 売却0件のケースは sold tab のUI状態が欠損する場合がある。
    # lookback=90days が明示され sold_90d_count=0 なら strict 扱いにする。
    if (not sold_tab_selected) and sold_90d_count == 0 and lookback_selected == "last 90 days":
        return True
    if lookback_selected == "last 90 days":
        url_raw = str(metadata.get("url", "") or "").strip()
        if url_raw:
            try:
                parsed = urllib.parse.urlparse(url_raw)
                params = urllib.parse.parse_qs(parsed.query or "")
                tab_values = [str(v or "").strip().lower() for v in params.get("tabName", [])]
                if any(v == "sold" for v in tab_values):
                    return True
            except Exception:
                pass
    return sold_tab_selected and lookback_selected == "last 90 days"


def _has_signal_sold_sample_reference(metadata: Dict[str, Any]) -> bool:
    if not isinstance(metadata, dict):
        return False
    sold_sample = metadata.get("sold_sample") if isinstance(metadata.get("sold_sample"), dict) else {}
    item_url = str(sold_sample.get("item_url", "") or "").strip()
    sold_price = _to_float(sold_sample.get("sold_price"), _to_float(sold_sample.get("sold_price_usd"), -1.0))
    return bool(item_url and sold_price > 0)


def _sanitize_unreliable_rpa_signal(signal: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(signal, dict) or not signal:
        return signal
    sold_90d_count = _to_int(signal.get("sold_90d_count"), -1)
    if sold_90d_count <= 0:
        return signal
    source = str(signal.get("source", "") or "")
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    looks_rpa = (
        ("rpa" in source.lower())
        or bool(metadata.get("rpa_query"))
        or bool(metadata.get("filter_state"))
        or bool(metadata.get("rpa_json_path"))
    )
    if not looks_rpa:
        return signal

    filter_state = metadata.get("filter_state") if isinstance(metadata.get("filter_state"), dict) else {}
    sold_tab_selected = bool(filter_state.get("sold_tab_selected"))
    lookback_selected = str(filter_state.get("lookback_selected", "") or "").strip().lower()
    strict_ok = sold_tab_selected and lookback_selected == "last 90 days"
    if (not strict_ok) and lookback_selected == "last 90 days":
        url_raw = str(metadata.get("url", "") or "").strip()
        if url_raw:
            try:
                parsed = urllib.parse.urlparse(url_raw)
                params = urllib.parse.parse_qs(parsed.query or "")
                tab_values = [str(v or "").strip().lower() for v in params.get("tabName", [])]
                strict_ok = any(v == "sold" for v in tab_values)
            except Exception:
                strict_ok = False
    filtered_row_count = _to_int(metadata.get("filtered_row_count"), -1)
    has_sample = _has_signal_sold_sample_reference(metadata)
    accepted_without_filtered_rows = bool(metadata.get("accepted_without_filtered_rows"))

    if strict_ok and (filtered_row_count > 0 or has_sample or accepted_without_filtered_rows):
        return signal

    reason = "rpa_signal_not_strict_sold_last90"
    if strict_ok and filtered_row_count <= 0 and not has_sample:
        reason = "rpa_signal_positive_without_filtered_rows_or_sample"
    metadata_next = dict(metadata)
    metadata_next["invalidated_reason"] = reason
    metadata_next["invalidated_at"] = _utc_iso()
    return _to_signal_dict(
        signal_key=str(signal.get("signal_key", "") or ""),
        sold_90d_count=-1,
        active_count=_to_int(signal.get("active_count"), -1),
        sold_price_median=-1.0,
        sold_price_currency=str(signal.get("sold_price_currency", "USD") or "USD"),
        source=f"{source}:invalidated",
        confidence=0.0,
        unavailable_reason=reason,
        metadata=metadata_next,
        fetched_at_ts=float(_iso_to_epoch(str(signal.get("fetched_at", "") or "")) or time.time()),
        next_refresh_ts=float(_iso_to_epoch(str(signal.get("next_refresh_at", "") or "")) or time.time()),
        from_cache=bool(signal.get("from_cache")),
    )


def _provider_rpa_json(
    *,
    query: str,
    signal_key: str,
    active_count_hint: int,
) -> Tuple[Optional[Dict[str, Any]], str]:
    data_path = (os.getenv("LIQUIDITY_RPA_JSON_PATH", "") or "").strip()
    if not data_path:
        data_path = "data/liquidity_rpa_signals.jsonl"
    path = Path(data_path).expanduser()
    if not path.is_absolute():
        path = (ROOT_DIR / path).resolve()
    entries_by_key, entries_by_query = _load_rpa_json_entries(path)
    if not entries_by_key and not entries_by_query:
        return None, "rpa_json_empty"

    row = _resolve_rpa_row(
        entries_by_key=entries_by_key,
        entries_by_query=entries_by_query,
        signal_key=signal_key,
        query=query,
    )
    if not isinstance(row, dict):
        return None, "rpa_json_no_match"

    require_strict_filters = _env_bool("LIQUIDITY_RPA_REQUIRE_STRICT_FILTERS", True)
    if require_strict_filters and (not _rpa_row_has_strict_sold_filters(row)):
        return None, "rpa_json_not_strict_sold_filters"

    require_filtered_rows_for_positive = _env_bool("LIQUIDITY_RPA_REQUIRE_FILTERED_ROWS_FOR_POSITIVE_SOLD", True)
    accepted_without_filtered_rows = False
    if require_filtered_rows_for_positive:
        sold_probe = _to_int(row.get("sold_90d_count"), -1)
        if sold_probe > 0:
            metadata_probe = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            filtered_row_count = _to_int(metadata_probe.get("filtered_row_count"), -1)
            sold_sample = metadata_probe.get("sold_sample") if isinstance(metadata_probe.get("sold_sample"), dict) else {}
            has_sample = bool(
                str(sold_sample.get("item_url", "") or "").strip()
                and _to_float(sold_sample.get("sold_price"), -1.0) > 0
            )
            if filtered_row_count <= 0 and not has_sample:
                url_raw = str(metadata_probe.get("url", "") or "").strip()
                has_url_sold = False
                if url_raw:
                    try:
                        parsed = urllib.parse.urlparse(url_raw)
                        params = urllib.parse.parse_qs(parsed.query or "")
                        tab_values = [str(v or "").strip().lower() for v in params.get("tabName", [])]
                        lookback_selected = str(
                            (metadata_probe.get("filter_state") or {}).get("lookback_selected", "") if isinstance(metadata_probe.get("filter_state"), dict) else ""
                        ).strip().lower()
                        has_url_sold = (lookback_selected == "last 90 days") and any(v == "sold" for v in tab_values)
                    except Exception:
                        has_url_sold = False
                if not has_url_sold:
                    return None, "rpa_json_positive_sold_without_filtered_rows"
                accepted_without_filtered_rows = True

    # 型番クエリ時は、取得行のクエリ型番と整合している場合のみ採用する。
    requested_codes = _specific_query_codes(query)
    row_query = str(row.get("query", "") or "").strip()
    row_codes = _specific_query_codes(row_query)
    if requested_codes and row_codes:
        if requested_codes.isdisjoint(row_codes):
            return None, "rpa_json_query_code_mismatch"

    max_age = max(60, _env_int("LIQUIDITY_RPA_MAX_AGE_SECONDS", 259200))
    fetched_at = str(row.get("fetched_at", "") or row.get("updated_at", "") or "").strip()
    fetched_epoch = _iso_to_epoch(fetched_at)
    if fetched_epoch > 0 and (int(time.time()) - fetched_epoch) > max_age:
        return None, "rpa_json_stale"

    sold_90d_count = _to_int(row.get("sold_90d_count"), -1)
    if sold_90d_count < 0:
        return None, "rpa_json_missing_sold_90d"
    active_count = _to_int(row.get("active_count"), active_count_hint)
    sold_price_min_raw = _to_float(row.get("sold_price_min"), -1.0)
    sold_price_median = _to_float(row.get("sold_price_median"), -1.0)
    if sold_price_median <= 0 and sold_price_min_raw > 0:
        sold_price_median = sold_price_min_raw
    sold_price_min, sold_price_min_outlier, sold_price_min_ratio = _normalize_sold_price_min(
        sold_price_min=sold_price_min_raw,
        sold_price_median=sold_price_median,
    )
    sold_price_currency = str(row.get("sold_price_currency", "USD") or "USD")
    source = str(row.get("source", "") or "rpa_json")
    confidence = _to_float(row.get("confidence"), 0.68)
    meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    metadata = {
        **meta,
        "rpa_json_path": str(path),
        "rpa_query": str(row.get("query", "") or ""),
    }
    if sold_price_min_raw > 0:
        metadata["sold_price_min_raw"] = round(sold_price_min_raw, 4)
    if sold_price_min_ratio > 0:
        metadata["sold_price_min_ratio_vs_median"] = round(sold_price_min_ratio, 6)
    if sold_price_min_outlier:
        metadata["sold_price_min_outlier"] = True
        metadata["sold_price_min_outlier_ratio_threshold"] = max(
            0.0,
            min(0.99, _env_float("LIQUIDITY_SOLD_PRICE_MIN_OUTLIER_RATIO", 0.35)),
        )
    if sold_price_min > 0:
        metadata["sold_price_min"] = round(sold_price_min, 4)
    active_price_min = _to_float(
        meta.get("active_price_min"),
        _to_float(row.get("active_price_min"), -1.0),
    )
    if active_price_min > 0:
        metadata["active_price_min"] = round(active_price_min, 4)
    active_price_median = _to_float(
        meta.get("active_price_median"),
        _to_float(row.get("active_price_median"), -1.0),
    )
    if active_price_median > 0:
        metadata["active_price_median"] = round(active_price_median, 4)
    active_sample = meta.get("active_sample") if isinstance(meta.get("active_sample"), dict) else {}
    if isinstance(active_sample, dict) and active_sample:
        sample_url = str(active_sample.get("item_url", "") or "").strip()
        sample_price = _to_float(active_sample.get("active_price"), -1.0)
        if sample_url or sample_price > 0:
            metadata["active_sample"] = active_sample
    if accepted_without_filtered_rows:
        metadata["accepted_without_filtered_rows"] = True
        metadata["accepted_without_filtered_rows_reason"] = "url_tabName_sold"
        confidence = min(confidence, 0.7)
    signal = _to_signal_dict(
        signal_key=signal_key,
        sold_90d_count=sold_90d_count,
        active_count=active_count,
        sold_price_median=sold_price_median,
        sold_price_currency=sold_price_currency,
        source=source,
        confidence=max(0.0, min(1.0, confidence)),
        unavailable_reason="",
        metadata=metadata,
    )
    if sold_price_min > 0:
        signal["sold_price_min"] = round(sold_price_min, 4)
    if active_price_min > 0:
        signal["active_price_min"] = round(active_price_min, 4)
    return signal, ""


def _provider_ebay_marketplace_insights(
    *,
    query: str,
    signal_key: str,
    timeout: int,
    active_count_hint: int,
) -> Tuple[Optional[Dict[str, Any]], str]:
    # Endpoint and accepted query params vary by account rollout; try configured variants.
    raw_urls = (os.getenv("LIQUIDITY_INSIGHTS_URLS", "") or "").strip()
    urls: list[str] = []
    if raw_urls:
        for part in raw_urls.split(","):
            item = str(part or "").strip()
            if item:
                urls.append(item)
    default_url = (
        os.getenv("LIQUIDITY_INSIGHTS_URL", "")
        or "https://api.ebay.com/buy/marketplace_insights/v1_beta/item_sales/search"
    ).strip()
    if default_url:
        urls.append(default_url)
    if not urls:
        return None, "insights_url_missing"

    token = _ebay_access_token(timeout)
    marketplace = (os.getenv("TARGET_MARKETPLACE", "EBAY_US") or "EBAY_US").strip() or "EBAY_US"
    limit = str(max(1, min(200, _env_int("LIQUIDITY_INSIGHTS_LIMIT", 100))))
    horizon_days = str(max(30, min(365, _env_int("LIQUIDITY_INSIGHTS_DAYS", 90))))
    param_sets = [
        {"q": query, "limit": limit},
        {"query": query, "limit": limit},
        {"q": query, "days": horizon_days, "limit": limit},
        {"query": query, "days": horizon_days, "limit": limit},
    ]
    last_reason = "insights_unknown"
    tried: list[dict[str, Any]] = []
    for base_url in urls:
        for params in param_sets:
            if "{QUERY}" in base_url:
                url = (
                    base_url.replace("{QUERY}", urllib.parse.quote_plus(query))
                    .replace("{LIMIT}", urllib.parse.quote_plus(limit))
                    .replace("{DAYS}", urllib.parse.quote_plus(horizon_days))
                )
            else:
                separator = "&" if "?" in base_url else "?"
                url = f"{base_url}{separator}{urllib.parse.urlencode(params)}"
            status, _, payload = _request_json(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-EBAY-C-MARKETPLACE-ID": marketplace,
                },
                timeout=timeout,
            )
            tried.append({"url": url, "status": int(status)})
            if status != 200:
                if status in {401, 403}:
                    last_reason = f"insights_http_{status}_access_denied"
                elif status == 404:
                    last_reason = "insights_http_404_not_found"
                else:
                    last_reason = f"insights_http_{status}"
                continue

            rows = payload.get("itemSales")
            if not isinstance(rows, list):
                rows = payload.get("items")
            if not isinstance(rows, list):
                rows = payload.get("results")
            if not isinstance(rows, list):
                rows = payload.get("itemSalesResults")
            if not isinstance(rows, list):
                rows = []

            sold_90d_count = _to_int(payload.get("total"), -1)
            if sold_90d_count < 0:
                sold_90d_count = _to_int(payload.get("itemSalesCount"), -1)
            if sold_90d_count < 0:
                sold_90d_count = _to_int(payload.get("totalResults"), -1)
            if sold_90d_count < 0 and rows:
                sold_90d_count = len(rows)

            sold_price_median = _derive_median_price_from_rows(rows)
            active_count = active_count_hint
            if active_count < 0:
                active_count = _to_int(payload.get("activeCount"), -1)

            if sold_90d_count < 0:
                last_reason = "insights_missing_sold_90d"
                continue

            signal = _to_signal_dict(
                signal_key=signal_key,
                sold_90d_count=sold_90d_count,
                active_count=active_count,
                sold_price_median=sold_price_median,
                sold_price_currency="USD",
                source="ebay_marketplace_insights",
                confidence=0.92,
                unavailable_reason="",
                metadata={"insights_url": base_url, "resolved_url": url, "tried": tried[-4:]},
            )
            return signal, ""
    return None, last_reason


def _apply_liquidity_fallback(
    *,
    query: str,
    signal_key: str,
    timeout: int,
    active_count_hint: int,
    previous_reason: str,
) -> Tuple[Optional[Dict[str, Any]], str]:
    mode = (os.getenv("LIQUIDITY_FALLBACK_MODE", "none") or "none").strip().lower()
    if mode in {"none", "off", "disabled"}:
        return None, previous_reason
    if mode in {"http", "http_json"}:
        signal, reason = _provider_http_json(
            query=query,
            signal_key=signal_key,
            timeout=timeout,
            active_count_hint=active_count_hint,
        )
        if signal is not None:
            metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
            metadata["fallback_from"] = previous_reason
            signal["metadata"] = metadata
            signal["source"] = f"{signal.get('source', 'http_json')}:fallback"
            return signal, ""
        return None, f"{previous_reason}|fallback_http_json:{reason}"
    if mode in {"rpa_json", "rpa"}:
        signal, reason = _provider_rpa_json(
            query=query,
            signal_key=signal_key,
            active_count_hint=active_count_hint,
        )
        if signal is not None:
            metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
            metadata["fallback_from"] = previous_reason
            signal["metadata"] = metadata
            signal["source"] = f"{signal.get('source', 'rpa_json')}:fallback"
            return signal, ""
        return None, f"{previous_reason}|fallback_rpa_json:{reason}"
    if mode == "mock":
        sold_90d_count = _env_int("LIQUIDITY_MOCK_SOLD_90D", -1)
        active_count = _env_int("LIQUIDITY_MOCK_ACTIVE", active_count_hint)
        median_usd = _env_float("LIQUIDITY_MOCK_SOLD_PRICE_MEDIAN_USD", -1.0)
        signal = _to_signal_dict(
            signal_key=signal_key,
            sold_90d_count=sold_90d_count,
            active_count=active_count,
            sold_price_median=median_usd,
            sold_price_currency="USD",
            source="mock:fallback",
            confidence=0.45,
            unavailable_reason="" if sold_90d_count >= 0 else "mock_missing_sold_90d",
            metadata={"fallback_from": previous_reason},
        )
        return signal, ""
    return None, f"{previous_reason}|unsupported_fallback_mode:{mode}"


def get_liquidity_signal(
    *,
    query: str,
    source_title: str,
    market_title: str,
    source_identifiers: Optional[Dict[str, str]] = None,
    market_identifiers: Optional[Dict[str, str]] = None,
    active_count_hint: int = -1,
    timeout: int = 15,
    settings: Optional[Settings] = None,
) -> Dict[str, Any]:
    settings = settings or load_settings()
    mode = (os.getenv("LIQUIDITY_PROVIDER_MODE", "none") or "none").strip().lower()
    signal_key = resolve_liquidity_key(
        query=query,
        source_title=source_title,
        market_title=market_title,
        source_identifiers=source_identifiers,
        market_identifiers=market_identifiers,
    )

    # rpa_json は収集ファイル更新を優先し、DBキャッシュによる古い判定を避ける。
    if _env_bool("LIQUIDITY_CACHE_ENABLED", True) and mode not in {"rpa_json", "rpa"}:
        cached = _load_cached_signal(settings, signal_key)
        if cached is not None:
            return _sanitize_unreliable_rpa_signal(cached)

    signal: Optional[Dict[str, Any]] = None
    unavailable_reason = "provider_disabled"

    try:
        if mode in {"none", "off", "disabled"}:
            unavailable_reason = "provider_disabled"
        elif mode == "mock":
            sold_90d_count = _env_int("LIQUIDITY_MOCK_SOLD_90D", -1)
            active_count = _env_int("LIQUIDITY_MOCK_ACTIVE", active_count_hint)
            median_usd = _env_float("LIQUIDITY_MOCK_SOLD_PRICE_MEDIAN_USD", -1.0)
            signal = _to_signal_dict(
                signal_key=signal_key,
                sold_90d_count=sold_90d_count,
                active_count=active_count,
                sold_price_median=median_usd,
                sold_price_currency="USD",
                source="mock",
                confidence=0.5,
                unavailable_reason="" if sold_90d_count >= 0 else "mock_missing_sold_90d",
                metadata={"mode": "mock"},
            )
        elif mode in {"http", "http_json"}:
            signal, unavailable_reason = _provider_http_json(
                query=query,
                signal_key=signal_key,
                timeout=timeout,
                active_count_hint=active_count_hint,
            )
        elif mode in {"rpa_json", "rpa"}:
            signal, unavailable_reason = _provider_rpa_json(
                query=query,
                signal_key=signal_key,
                active_count_hint=active_count_hint,
            )
        elif mode in {"ebay_marketplace_insights", "insights"}:
            signal, unavailable_reason = _provider_ebay_marketplace_insights(
                query=query,
                signal_key=signal_key,
                timeout=timeout,
                active_count_hint=active_count_hint,
            )
            if signal is None:
                signal, unavailable_reason = _apply_liquidity_fallback(
                    query=query,
                    signal_key=signal_key,
                    timeout=timeout,
                    active_count_hint=active_count_hint,
                    previous_reason=unavailable_reason,
                )
        else:
            unavailable_reason = f"unsupported_provider_mode:{mode}"
    except Exception as err:  # pragma: no cover - network/provider dependent
        unavailable_reason = f"provider_exception:{err}"
        signal = None

    if signal is None:
        signal = _to_signal_dict(
            signal_key=signal_key,
            sold_90d_count=-1,
            active_count=active_count_hint,
            sold_price_median=-1.0,
            sold_price_currency="USD",
            source=mode or "none",
            confidence=0.0,
            unavailable_reason=unavailable_reason,
            metadata={"mode": mode or "none"},
        )

    signal = _sanitize_unreliable_rpa_signal(signal)
    _save_signal(settings, signal)
    return signal


def estimate_ev90(
    *,
    profit_usd: float,
    purchase_total_usd: float,
    liquidity_signal: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    signal = liquidity_signal if isinstance(liquidity_signal, dict) else {}
    sold_90d = _to_int(signal.get("sold_90d_count"), -1)
    active = _to_int(signal.get("active_count"), -1)
    str90 = _to_float(signal.get("sell_through_90d"), -1.0)
    confidence = _to_float(signal.get("confidence"), 0.0)
    unavailable_reason = str(signal.get("unavailable_reason", "") or "")
    has_liquidity_signal = sold_90d >= 0 and str90 >= 0

    default_p90 = max(0.01, min(0.99, _env_float("EV90_DEFAULT_P90", 0.28)))
    if str90 >= 0.0:
        volume_bonus = 0.0
        if sold_90d > 0:
            volume_bonus = min(0.22, sold_90d / 50.0)
        p90 = min(0.98, max(0.02, (str90 * 0.78) + volume_bonus))
    elif sold_90d >= 0:
        p90 = min(0.95, max(0.03, 0.20 + (sold_90d / 80.0)))
    else:
        p90 = default_p90

    holding_rate_90d = max(0.0, min(1.0, _env_float("EV90_HOLDING_COST_RATE_90D", 0.06)))
    holding_cost_usd = max(0.0, purchase_total_usd) * holding_rate_90d

    base_risk_penalty = max(0.0, _env_float("EV90_BASE_RISK_PENALTY_USD", 0.0))
    confidence_penalty = max(0.0, (1.0 - confidence)) * max(
        0.0, _env_float("EV90_LOW_CONFIDENCE_PENALTY_USD", 4.0)
    )
    unavailable_penalty = (
        max(0.0, _env_float("EV90_LIQUIDITY_UNAVAILABLE_PENALTY_USD", 6.0))
        if sold_90d < 0 or str90 < 0
        else 0.0
    )
    risk_penalty_usd = base_risk_penalty + confidence_penalty + unavailable_penalty

    profit = float(profit_usd)
    ev90_usd = (p90 * profit) - ((1.0 - p90) * holding_cost_usd) - risk_penalty_usd
    min_ev90 = _env_float("EV90_MIN_USD", 0.0)
    enforce_without_liquidity = _env_bool("EV90_ENFORCE_WITHOUT_LIQUIDITY", False)
    pass_gate = ev90_usd >= float(min_ev90)
    if (not has_liquidity_signal) and (not enforce_without_liquidity):
        pass_gate = True

    return {
        "score_usd": round(ev90_usd, 4),
        "min_required_usd": round(float(min_ev90), 4),
        "pass": pass_gate,
        "has_liquidity_signal": has_liquidity_signal,
        "enforce_without_liquidity": bool(enforce_without_liquidity),
        "prob_sell_90d": round(p90, 6),
        "holding_cost_usd": round(holding_cost_usd, 4),
        "risk_penalty_usd": round(risk_penalty_usd, 4),
        "inputs": {
            "profit_usd": round(profit, 4),
            "purchase_total_usd": round(max(0.0, purchase_total_usd), 4),
            "sold_90d_count": sold_90d,
            "active_count": active,
            "sell_through_90d": round(str90, 6) if str90 >= 0 else -1.0,
            "confidence": round(confidence, 4),
            "unavailable_reason": unavailable_reason,
        },
    }


def evaluate_liquidity_gate(
    signal: Dict[str, Any],
    *,
    min_sold_90d_count: int,
    min_sell_through_90d: float,
    require_signal: bool,
) -> Dict[str, Any]:
    sold = _to_int(signal.get("sold_90d_count"), -1)
    sell_through = _to_float(signal.get("sell_through_90d"), -1.0)
    source = str(signal.get("source", "") or "")

    if sold < 0:
        if require_signal:
            return {
                "pass": False,
                "reason": "liquidity_unavailable_required",
                "source": source,
                "sold_90d_count": sold,
                "sell_through_90d": sell_through,
            }
        return {
            "pass": True,
            "reason": "liquidity_unavailable_soft_pass",
            "source": source,
            "sold_90d_count": sold,
            "sell_through_90d": sell_through,
        }

    # RPA由来では active_count 欠損により sell_through が -1 になり得る。
    # sold_90d が取れている場合は sold閾値で判定を継続する。
    if sell_through < 0:
        if sold < int(min_sold_90d_count):
            return {
                "pass": False,
                "reason": "liquidity_sold_90d_below_threshold",
                "source": source,
                "sold_90d_count": sold,
                "sell_through_90d": sell_through,
            }
        return {
            "pass": True,
            "reason": "liquidity_sold_based_pass",
            "source": source,
            "sold_90d_count": sold,
            "sell_through_90d": sell_through,
        }

    if sold < int(min_sold_90d_count):
        return {
            "pass": False,
            "reason": "liquidity_sold_90d_below_threshold",
            "source": source,
            "sold_90d_count": sold,
            "sell_through_90d": sell_through,
        }

    if sell_through < float(min_sell_through_90d):
        return {
            "pass": False,
            "reason": "liquidity_sell_through_below_threshold",
            "source": source,
            "sold_90d_count": sold,
            "sell_through_90d": sell_through,
        }

    return {
        "pass": True,
        "reason": "liquidity_gate_pass",
        "source": source,
        "sold_90d_count": sold,
        "sell_through_90d": sell_through,
    }

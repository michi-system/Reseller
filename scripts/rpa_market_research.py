#!/usr/bin/env python3
"""Collect sold-90d signals from eBay Product Research (Terapeak) via browser RPA."""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import sys
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from reselling.coerce import to_float as _to_float
from reselling.coerce import to_int as _to_int
from reselling.time_utils import utc_iso as _utc_iso_now

_RE_SOLD_KEY = re.compile(r"(sold|sale).*(count|total|qty|quantity|items?)")
_RE_ACTIVE_KEY = re.compile(r"(active|listed|listing).*(count|total|qty|quantity|items?)")
_RE_MIN_PRICE_KEY = re.compile(r"(min|lowest|low).*(price|sold)")
_RE_MEDIAN_PRICE_KEY = re.compile(r"(median|med).*(price|sold)")
_RE_PRICE_KEY = re.compile(r"(sold|sale|price|amount|value)")
_RE_CURRENCY_KEY = re.compile(r"(currency|currencycode)")
_RE_DOM_SOLD = re.compile(r"([0-9][0-9,]{0,9})\s*(sold|sales)")
_RE_DOM_PRICE = re.compile(r"(?:US\$|\$|USD\s*)([0-9][0-9,]{0,9}(?:\.[0-9]{1,2})?)")
_RE_DOM_AVG_SOLD = re.compile(
    r"\$([0-9][0-9,]{0,9}(?:\.[0-9]{1,2})?)\s*Avg sold price", re.IGNORECASE | re.DOTALL
)
_RE_DOM_SOLD_RANGE = re.compile(
    r"\$([0-9][0-9,]{0,9}(?:\.[0-9]{1,2})?)\s*-\s*\$([0-9][0-9,]{0,9}(?:\.[0-9]{1,2})?)\s*Sold price range",
    re.IGNORECASE | re.DOTALL,
)
_RE_DOM_DATE_SOLD = re.compile(
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+[0-9]{1,2},\s+20[0-9]{2}\b"
)
_RE_HTML_ROW = re.compile(
    r'<(?:tr|div)[^>]*class="[^"]*(?<![A-Z0-9_-])research-table-row(?![A-Z0-9_-])[^"]*"[^>]*>',
    re.IGNORECASE,
)
_RE_HTML_ROW_PRICE = re.compile(
    r"research-table-row__(?:avgSoldPrice|listingPrice).*?<div[^>]*>\$?([0-9][0-9,]{0,9}(?:\.[0-9]{1,2})?)</div>",
    re.IGNORECASE | re.DOTALL,
)
_RE_HTML_ROW_DATE = re.compile(
    r"research-table-row__dateLastSold.*?<div[^>]*>((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+[0-9]{1,2},\s+20[0-9]{2})</div>",
    re.IGNORECASE | re.DOTALL,
)
_RE_HTML_ROW_LINK = re.compile(
    r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
_RE_HTML_IMG_SRC = re.compile(
    r'<img[^>]+(?:data-src|data-zoom-src|data-image|src)="([^"]+)"',
    re.IGNORECASE,
)
_RE_HTML_IMG_SRCSET = re.compile(
    r'<img[^>]+srcset="([^"]+)"',
    re.IGNORECASE,
)
_RE_HTML_ROW_START = re.compile(
    r'<(?:tr|div)[^>]*class="[^"]*(?<![A-Z0-9_-])research-table-row(?![A-Z0-9_-])[^"]*"[^>]*>',
    re.IGNORECASE,
)
_RE_HTML_TAG = re.compile(r"<[^>]+>")
_RE_DAILY_LIMIT_PHRASES = (
    re.compile(r"exceeded\s+the\s+number\s+of\s+requests\s+allowed\s+in\s+one\s+day", re.IGNORECASE),
    re.compile(r"please\s+try\s+again\s+tomorrow", re.IGNORECASE),
    re.compile(r"number\s+of\s+requests\s+allowed\s+in\s+one\s+day", re.IGNORECASE),
)
_RE_BOT_CHALLENGE_PHRASES = (
    re.compile(r"pardon\s+our\s+interruption", re.IGNORECASE),
    re.compile(r"think\s+you\s+were\s+a\s+bot", re.IGNORECASE),
    re.compile(r"browser\s+made\s+us\s+think\s+you\s+were\s+a\s+bot", re.IGNORECASE),
    re.compile(r"super-?human\s+speed", re.IGNORECASE),
    re.compile(r"disabled\s+javascript", re.IGNORECASE),
    re.compile(r"third-?party\s+browser\s+plugin", re.IGNORECASE),
)
_RE_NO_SOLD_PHRASES = (
    re.compile(r"\bno\s+sold\s+(?:items?|results?|found)\b", re.IGNORECASE),
    re.compile(r"\bno\s+sales?\s+(?:found|data|history)\b", re.IGNORECASE),
    re.compile(r"\b0\s+sold\b", re.IGNORECASE),
    re.compile(r"売れた商品はありません"),
    re.compile(r"販売実績がありません"),
)

_QUERY_STOPWORDS = {
    "NEW",
    "BRAND",
    "WITH",
    "WITHOUT",
    "FOR",
    "THE",
    "AND",
    "JAPAN",
    "EBAY",
    "WATCH",
    "MODEL",
    "SERIES",
    "ITEM",
    "USED",
    "ANY",
    "新品",
    "未使用",
}

_ACCESSORY_TERMS = {
    "BAND",
    "STRAP",
    "BRACELET",
    "BUCKLE",
    "LINK",
    "REPLACEMENT",
    "COVER",
    "EARPAD",
    "EAR PAD",
    "EARTIP",
    "EAR TIP",
    "EAR TIPS",
    "CABLE",
    "CHARGER",
    "ADAPTER",
    "PROTECTOR",
    "FILM",
    "ベルト",
    "バンド",
    "バックル",
    "コマ",
    "部品",
    "パーツ",
    "カバー",
    "イヤーパッド",
    "イヤーチップ",
    "保護フィルム",
}

_USED_ROW_TERMS = {
    "USED",
    "PRE-OWNED",
    "REFURB",
    "REFURBISHED",
    "FOR PARTS",
    "PARTS ONLY",
    "JUNK",
    "中古",
    "ジャンク",
    "動作未確認",
    "訳あり",
}

_NON_MAIN_ITEM_PATTERNS = (
    re.compile(r"\b(?:BOX|EMPTY BOX|MANUAL|INSTRUCTION|BOOKLET|PAPERS?|WARRANTY CARD|CARD)\s+ONLY\b", re.IGNORECASE),
    re.compile(r"\bONLY\s+(?:BOX|MANUAL|INSTRUCTION|BOOKLET|PAPERS?|WARRANTY CARD|CARD)\b", re.IGNORECASE),
    re.compile(r"\b(?:FOR|FITS?|COMPATIBLE WITH)\b.+\b(?:CASE|COVER|BAND|STRAP|BRACELET|BUCKLE|LINK|ADAPTER|CHARGER)\b", re.IGNORECASE),
    re.compile(r"\b(?:REPLACEMENT|SPARE)\b.+\b(?:BAND|STRAP|BRACELET|BUCKLE|LINK|CASE|COVER|PART)\b", re.IGNORECASE),
    re.compile(r"(箱|外箱|説明書|取扱説明書|保証書|カード|付属品|ケース)\s*のみ"),
)

_UI_NOISE_TITLE_TERMS = {
    "CAN'T FIND THE WORDS? SEARCH WITH AN IMAGE",
    "CANT FIND THE WORDS? SEARCH WITH AN IMAGE",
    "VISUAL_SEARCH_HANDLER",
    "RTM_TRACKING",
    "DEVICE_FINGER_PRINT",
    "USER_SHIP_LOCATION",
}

_PRICE_PATH_EXCLUDES = {
    "shipping",
    "postage",
    "tax",
    "vat",
    "fee",
    "discount",
    "coupon",
    "promotion",
    "promo",
    "ratio",
    "percent",
    "count",
    "quantity",
    "qty",
    "rank",
    "score",
    "watch",
    "bid",
    "offer",
    "id",
    "timestamp",
    "created",
    "updated",
    "year",
    "month",
    "day",
}

_PRICE_PATH_SIGNALS = {
    "sold",
    "sale",
    "avg",
    "average",
    "median",
    "min",
    "max",
    "range",
    "trend",
}


def _normalize_code(text: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(text or "").upper())


def _extract_query_codes(query: str) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw in re.findall(r"[A-Z0-9][A-Z0-9-]{3,}", str(query or "").upper()):
        token = _normalize_code(raw)
        if len(token) < 4:
            continue
        if token.isdigit():
            continue
        if sum(1 for ch in token if ch.isalpha()) < 1:
            continue
        if sum(1 for ch in token if ch.isdigit()) < 1:
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _extract_query_tokens(query: str, query_codes: List[str]) -> List[str]:
    code_set = set(query_codes)
    out: List[str] = []
    seen: set[str] = set()
    for raw in re.findall(r"[A-Z0-9]{3,}", str(query or "").upper()):
        token = raw.strip()
        if len(token) < 3:
            continue
        if token.isdigit():
            continue
        if token in _QUERY_STOPWORDS:
            continue
        norm = _normalize_code(token)
        if not norm or norm in code_set:
            continue
        if norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out[:8]


def _strip_html_text(html: str) -> str:
    text = _RE_HTML_TAG.sub(" ", str(html or ""))
    return re.sub(r"\s+", " ", text).strip()


def _contains_row_term(text: str, terms: set[str]) -> bool:
    upper = str(text or "").upper()
    jp = str(text or "")
    for term in terms:
        if term.isascii():
            if term in upper:
                return True
        else:
            if term in jp:
                return True
    return False


def _is_non_main_item_row(text: str) -> bool:
    if _contains_row_term(text, _USED_ROW_TERMS):
        return True
    normalized = str(text or "")
    for pattern in _NON_MAIN_ITEM_PATTERNS:
        if pattern.search(normalized):
            return True
    return False


def _is_accessory_row(text: str) -> bool:
    return _contains_row_term(text, _ACCESSORY_TERMS) or _is_non_main_item_row(text)


def _is_ui_noise_title(text: str) -> bool:
    normalized = re.sub(r"\s+", " ", str(text or "").strip().upper())
    if not normalized:
        return False
    if normalized in _UI_NOISE_TITLE_TERMS:
        return True
    if any(token in normalized for token in ("VISUAL_SEARCH_HANDLER", "DEVICE_FINGER_PRINT", "RTM_TRACKING")):
        return True
    return False


def _is_candidate_price_path(path: str) -> bool:
    key = str(path or "").lower()
    if not key:
        return False
    if not any(token in key for token in ("price", "amount", "value", "cost")):
        return False
    if any(term in key for term in _PRICE_PATH_EXCLUDES):
        return False
    if any(signal in key for signal in _PRICE_PATH_SIGNALS):
        return True
    # Fallback for nested row/item payloads that expose only "...price".
    return key.endswith(".price") and any(hint in key for hint in ("research", "row", "item"))


def _text_from_any(value: Any) -> str:
    if isinstance(value, str):
        return re.sub(r"\s+", " ", value).strip()
    if isinstance(value, dict):
        for key in ("title", "name", "text", "label", "value", "raw"):
            raw = value.get(key)
            if isinstance(raw, str) and raw.strip():
                return re.sub(r"\s+", " ", raw).strip()
    return ""


def _ebay_item_id_from_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    match = re.search(r"/itm/(?:[^/?#]+/)?([0-9]{9,15})", raw)
    if not match:
        return ""
    numeric_id = str(match.group(1) or "").strip()
    if not numeric_id:
        return ""
    return f"v1|{numeric_id}|0"


def _price_from_any(value: Any) -> float:
    if isinstance(value, (int, float)):
        return _to_float(value, -1.0)
    if isinstance(value, str):
        raw = value.replace(",", "").strip()
        return _to_float(raw, -1.0)
    if isinstance(value, dict):
        for key in ("value", "amount", "price", "convertedFromValue", "displayValue"):
            probe = _price_from_any(value.get(key))
            if probe > 0:
                return probe
    return -1.0


def _extract_filtered_rows_from_payload(
    payload: Any,
    *,
    query_codes: List[str],
    query_tokens: List[str],
) -> Tuple[List[float], int, Dict[str, Any], List[Dict[str, Any]]]:
    prices: List[float] = []
    sold_sample: Dict[str, Any] = {}
    rows: List[Dict[str, Any]] = []
    sold_count_candidates: List[int] = []

    max_nodes = max(200, _to_int(os.getenv("LIQUIDITY_RPA_PAYLOAD_NODE_LIMIT", "4000"), 4000))
    stack: List[Any] = [payload]
    seen_rows: set[str] = set()
    visited = 0

    while stack and visited < max_nodes:
        visited += 1
        node = stack.pop()
        if isinstance(node, list):
            for child in node[:120]:
                stack.append(child)
            continue
        if not isinstance(node, dict):
            continue

        for value in node.values():
            if isinstance(value, (dict, list)):
                stack.append(value)

        title = ""
        item_url = ""
        image_url = ""
        best_price_score = -1
        sold_price = -1.0
        sold_count_local = -1

        for raw_key, raw_val in node.items():
            key = str(raw_key or "").strip().lower()
            if not key:
                continue
            if any(token in key for token in ("sold", "sale")) and any(
                token in key for token in ("count", "total", "qty", "quantity", "items")
            ):
                count_value = _to_int(raw_val, -1)
                if count_value >= 0:
                    sold_count_local = max(sold_count_local, count_value)

            if (not title) and any(token in key for token in ("title", "itemtitle", "name", "productname")):
                text = _text_from_any(raw_val)
                if text:
                    title = text

            if (not item_url) and "url" in key:
                maybe_url = _text_from_any(raw_val)
                if maybe_url.startswith("http"):
                    item_url = maybe_url

            if (not image_url) and any(token in key for token in ("image", "thumbnail", "picture", "photo")):
                maybe_image = _text_from_any(raw_val)
                if maybe_image.startswith("http"):
                    image_url = maybe_image

            if any(token in key for token in ("price", "amount", "value")):
                numeric = _price_from_any(raw_val)
                if numeric > 0:
                    score = 1
                    if "sold" in key:
                        score += 4
                    if "avg" in key or "median" in key:
                        score += 2
                    if score > best_price_score or (score == best_price_score and (sold_price <= 0 or numeric < sold_price)):
                        best_price_score = score
                        sold_price = numeric

        if sold_count_local >= 0:
            sold_count_candidates.append(sold_count_local)

        if not title:
            continue
        if _is_ui_noise_title(title):
            continue
        if _is_accessory_row(title):
            continue
        if not _row_matches_query_text(title, query_codes=query_codes, query_tokens=query_tokens):
            continue

        dedup_key = _normalize_code(title) + "|" + _normalize_code(item_url)
        if dedup_key in seen_rows:
            continue
        seen_rows.add(dedup_key)

        row_entry: Dict[str, Any] = {"title": title[:220], "rank": len(rows) + 1}
        if item_url:
            row_entry["item_url"] = item_url
            item_id = _ebay_item_id_from_url(item_url)
            if item_id:
                row_entry["item_id"] = item_id
        if image_url:
            row_entry["image_url"] = image_url
        if sold_price > 0:
            row_entry["sold_price"] = round(sold_price, 4)
            prices.append(sold_price)
            current_sample_price = _to_float(sold_sample.get("sold_price"), -1.0)
            if not sold_sample or current_sample_price <= 0 or sold_price < current_sample_price:
                sold_sample = {
                    "title": row_entry.get("title", title[:220]),
                    "sold_price": round(sold_price, 4),
                }
                if item_url:
                    sold_sample["item_url"] = item_url
                    item_id = _ebay_item_id_from_url(item_url)
                    if item_id:
                        sold_sample["item_id"] = item_id
                if image_url:
                    sold_sample["image_url"] = image_url
        rows.append(row_entry)
        if len(rows) >= 240:
            break

    sold_count = max(sold_count_candidates) if sold_count_candidates else -1
    if sold_count < 0:
        sold_count = len(prices) if prices else len(rows)
    return prices, sold_count, sold_sample, rows


def _extract_raw_rows_from_payload(payload: Any) -> Tuple[List[float], List[Dict[str, Any]]]:
    prices: List[float] = []
    rows: List[Dict[str, Any]] = []
    max_nodes = max(200, _to_int(os.getenv("LIQUIDITY_RPA_PAYLOAD_NODE_LIMIT", "4000"), 4000))
    stack: List[Any] = [payload]
    seen_rows: set[str] = set()
    visited = 0

    while stack and visited < max_nodes:
        visited += 1
        node = stack.pop()
        if isinstance(node, list):
            for child in node[:120]:
                stack.append(child)
            continue
        if not isinstance(node, dict):
            continue

        for value in node.values():
            if isinstance(value, (dict, list)):
                stack.append(value)

        title = ""
        item_url = ""
        image_url = ""
        best_price_score = -1
        sold_price = -1.0

        for raw_key, raw_val in node.items():
            key = str(raw_key or "").strip().lower()
            if not key:
                continue

            if (not title) and any(token in key for token in ("title", "itemtitle", "name", "productname")):
                text = _text_from_any(raw_val)
                if text:
                    title = text

            if (not item_url) and "url" in key:
                maybe_url = _text_from_any(raw_val)
                if maybe_url.startswith("http"):
                    item_url = maybe_url

            if (not image_url) and any(token in key for token in ("image", "thumbnail", "picture", "photo")):
                maybe_image = _text_from_any(raw_val)
                if maybe_image.startswith("http"):
                    image_url = maybe_image

            if any(token in key for token in ("price", "amount", "value")):
                numeric = _price_from_any(raw_val)
                if numeric > 0:
                    score = 1
                    if "sold" in key:
                        score += 4
                    if "avg" in key or "median" in key:
                        score += 2
                    if score > best_price_score or (score == best_price_score and (sold_price <= 0 or numeric < sold_price)):
                        best_price_score = score
                        sold_price = numeric

        if not title:
            continue
        if _is_ui_noise_title(title):
            continue
        dedup_key = _normalize_code(title) + "|" + _normalize_code(item_url)
        if dedup_key in seen_rows:
            continue
        seen_rows.add(dedup_key)

        row_entry: Dict[str, Any] = {"title": title[:220], "rank": len(rows) + 1}
        if item_url:
            row_entry["item_url"] = item_url
            item_id = _ebay_item_id_from_url(item_url)
            if item_id:
                row_entry["item_id"] = item_id
        if image_url:
            row_entry["image_url"] = image_url
        if sold_price > 0:
            row_entry["sold_price"] = round(sold_price, 4)
            prices.append(sold_price)
        rows.append(row_entry)
        if len(rows) >= 240:
            break
    return prices, rows


def _contains_daily_limit_message(text: str) -> bool:
    haystack = str(text or "")
    if not haystack:
        return False
    matched = 0
    for pattern in _RE_DAILY_LIMIT_PHRASES:
        if pattern.search(haystack):
            matched += 1
    # "try again tomorrow" 単体の誤検出を避ける。
    return matched >= 2


def _page_has_daily_limit_message(page: Any) -> bool:
    try:
        body_text = page.inner_text("body")
        if _contains_daily_limit_message(body_text):
            return True
    except Exception:
        pass
    try:
        html_text = page.content()
        if _contains_daily_limit_message(html_text):
            return True
    except Exception:
        pass
    return False


def _contains_bot_challenge_message(text: str) -> bool:
    haystack = str(text or "")
    if not haystack:
        return False
    matched = 0
    for pattern in _RE_BOT_CHALLENGE_PHRASES:
        if pattern.search(haystack):
            matched += 1
    # 固有文言 or 複合一致で判定し、誤検出を抑える。
    if matched >= 2:
        return True
    return bool(re.search(r"pardon\s+our\s+interruption", haystack, re.IGNORECASE))


def _page_has_bot_challenge_message(page: Any) -> bool:
    try:
        body_text = page.inner_text("body")
        if _contains_bot_challenge_message(body_text):
            return True
    except Exception:
        pass
    try:
        html_text = page.content()
        if _contains_bot_challenge_message(html_text):
            return True
    except Exception:
        pass
    return False


def _contains_no_sold_message(text: str) -> bool:
    haystack = str(text or "")
    if not haystack:
        return False
    for pattern in _RE_NO_SOLD_PHRASES:
        if pattern.search(haystack):
            return True
    return False


def _page_has_no_sold_message(page: Any) -> bool:
    try:
        body_text = page.inner_text("body")
        if _contains_no_sold_message(body_text):
            return True
    except Exception:
        pass
    try:
        html_text = page.content()
        if _contains_no_sold_message(html_text):
            return True
    except Exception:
        pass
    return False


def _should_short_circuit_no_sold(
    *,
    query: str,
    lookback_days: int,
    no_sold_detected: bool,
    lookback_selected: str,
) -> bool:
    if not bool(no_sold_detected):
        return False
    if int(lookback_days) != 90:
        return False
    if not _extract_query_codes(query):
        return False
    lb = str(lookback_selected or "").strip().lower()
    if lb and lb != "last 90 days":
        return False
    return True


def _is_transient_navigation_error(err: Exception) -> bool:
    text = str(err or "").lower()
    return (
        "err_aborted" in text
        or ("navigation to" in text and "interrupted" in text)
        or "execution context was destroyed" in text
    )


def _trim_low_price_outliers(prices: List[float]) -> List[float]:
    if not prices:
        return []
    vals = [v for v in prices if 0 < v < 100000]
    if len(vals) < 4:
        return vals
    med = _to_float(statistics.median(vals), -1.0)
    if med <= 0:
        return vals
    ratio_floor = max(0.01, min(0.5, _to_float(os.getenv("LIQUIDITY_RPA_MIN_PRICE_RATIO_TO_MEDIAN", "0.08"), 0.08)))
    abs_floor = max(0.01, _to_float(os.getenv("LIQUIDITY_RPA_MIN_PRICE_ABS_USD", "1.0"), 1.0))
    floor = max(abs_floor, med * ratio_floor)
    trimmed = [v for v in vals if v >= floor]
    return trimmed if trimmed else vals


def _row_matches_query_text(
    row_text: str,
    *,
    query_codes: List[str],
    query_tokens: List[str],
) -> bool:
    upper = str(row_text or "").upper()
    row_norm = _normalize_code(upper)
    if not row_norm:
        return False

    if query_codes:
        if not any(code in row_norm for code in query_codes):
            return False
        return True

    if not query_tokens:
        return True
    matched = sum(1 for token in query_tokens if token in row_norm)
    min_needed = 1 if len(query_tokens) <= 2 else 2
    return matched >= min_needed


def _extract_filtered_rows_from_html(
    html: str,
    *,
    query_codes: List[str],
    query_tokens: List[str],
) -> Tuple[List[float], int, Dict[str, Any]]:
    prices, sold_count, sold_sample, _ = _extract_filtered_rows_with_rows(
        html,
        query_codes=query_codes,
        query_tokens=query_tokens,
    )
    return prices, sold_count, sold_sample


def _extract_filtered_rows_with_rows(
    html: str,
    *,
    query_codes: List[str],
    query_tokens: List[str],
) -> Tuple[List[float], int, Dict[str, Any], List[Dict[str, Any]]]:
    prices: List[float] = []
    sold_dates: set[str] = set()
    sold_sample: Dict[str, Any] = {}
    rows: List[Dict[str, Any]] = []
    text = str(html or "")
    starts = [m.start() for m in _RE_HTML_ROW_START.finditer(text)]
    if not starts:
        return prices, 0, sold_sample, rows
    for idx, start in enumerate(starts):
        end = starts[idx + 1] if (idx + 1) < len(starts) else len(text)
        block = text[start:end]
        row_text = _strip_html_text(block)
        if not row_text:
            continue
        if _is_ui_noise_title(row_text):
            continue
        if _is_accessory_row(row_text):
            continue
        if not _row_matches_query_text(row_text, query_codes=query_codes, query_tokens=query_tokens):
            continue
        row_entry: Dict[str, Any] = {"title": row_text[:220], "rank": idx + 1}
        link_match = _RE_HTML_ROW_LINK.search(block)
        if link_match:
            href = str(link_match.group(1) or "").strip()
            if href:
                row_entry["item_url"] = urllib.parse.urljoin("https://www.ebay.com", href)
                item_id = _ebay_item_id_from_url(str(row_entry.get("item_url", "")))
                if item_id:
                    row_entry["item_id"] = item_id
            title_raw = _strip_html_text(str(link_match.group(2) or ""))
            if title_raw:
                row_entry["title"] = title_raw[:220]
        img_src = ""
        img_match = _RE_HTML_IMG_SRC.search(block)
        if img_match:
            img_src = str(img_match.group(1) or "").strip()
        if not img_src:
            srcset_match = _RE_HTML_IMG_SRCSET.search(block)
            if srcset_match:
                srcset_text = str(srcset_match.group(1) or "").strip()
                for chunk in srcset_text.split(","):
                    url = chunk.strip().split(" ", 1)[0].strip()
                    if url:
                        img_src = url
                        break
        if img_src:
            row_entry["image_url"] = urllib.parse.urljoin("https://www.ebay.com", img_src)
        price_match = _RE_HTML_ROW_PRICE.search(block)
        if price_match:
            raw = str(price_match.group(1) or "").replace(",", "")
            price = _to_float(raw, -1.0)
            if price > 0:
                prices.append(price)
                row_entry["sold_price"] = round(price, 4)
                current_sample_price = _to_float(sold_sample.get("sold_price"), -1.0)
                if not sold_sample or current_sample_price <= 0 or price < current_sample_price:
                    sample: Dict[str, Any] = {
                        "title": row_text[:220],
                        "sold_price": round(price, 4),
                    }
                    if "item_url" in row_entry:
                        sample["item_url"] = row_entry["item_url"]
                    if "item_id" in row_entry:
                        sample["item_id"] = row_entry["item_id"]
                    if "image_url" in row_entry:
                        sample["image_url"] = row_entry["image_url"]
                    if row_entry.get("title"):
                        sample["title"] = row_entry["title"]
                    sold_sample = sample
        rows.append(row_entry)
        date_match = _RE_HTML_ROW_DATE.search(block)
        if date_match:
            sold_dates.add(str(date_match.group(1) or "").strip())
    sold_count = max(len(prices), len(sold_dates))
    if sold_dates and sold_sample:
        sold_sample["sold_date_count_detected"] = len(sold_dates)
    return prices, sold_count, sold_sample, rows


def _extract_raw_rows_with_rows(
    html: str,
) -> Tuple[List[float], List[Dict[str, Any]]]:
    prices: List[float] = []
    rows: List[Dict[str, Any]] = []
    text = str(html or "")
    starts = [m.start() for m in _RE_HTML_ROW_START.finditer(text)]
    if not starts:
        return prices, rows
    seen_rows: set[str] = set()
    for idx, start in enumerate(starts):
        end = starts[idx + 1] if (idx + 1) < len(starts) else len(text)
        block = text[start:end]
        row_text = _strip_html_text(block)
        if not row_text:
            continue
        if _is_ui_noise_title(row_text):
            continue
        row_entry: Dict[str, Any] = {"title": row_text[:220], "rank": idx + 1}
        link_match = _RE_HTML_ROW_LINK.search(block)
        if link_match:
            href = str(link_match.group(1) or "").strip()
            if href:
                row_entry["item_url"] = urllib.parse.urljoin("https://www.ebay.com", href)
                item_id = _ebay_item_id_from_url(str(row_entry.get("item_url", "")))
                if item_id:
                    row_entry["item_id"] = item_id
            title_raw = _strip_html_text(str(link_match.group(2) or ""))
            if title_raw:
                row_entry["title"] = title_raw[:220]
        img_src = ""
        img_match = _RE_HTML_IMG_SRC.search(block)
        if img_match:
            img_src = str(img_match.group(1) or "").strip()
        if not img_src:
            srcset_match = _RE_HTML_IMG_SRCSET.search(block)
            if srcset_match:
                srcset_text = str(srcset_match.group(1) or "").strip()
                for chunk in srcset_text.split(","):
                    url = chunk.strip().split(" ", 1)[0].strip()
                    if url:
                        img_src = url
                        break
        if img_src:
            row_entry["image_url"] = urllib.parse.urljoin("https://www.ebay.com", img_src)
        price_match = _RE_HTML_ROW_PRICE.search(block)
        if price_match:
            raw = str(price_match.group(1) or "").replace(",", "")
            price = _to_float(raw, -1.0)
            if price > 0:
                prices.append(price)
                row_entry["sold_price"] = round(price, 4)
        dedup_key = _normalize_code(str(row_entry.get("title", ""))) + "|" + _normalize_code(
            str(row_entry.get("item_url", ""))
        )
        if dedup_key in seen_rows:
            continue
        seen_rows.add(dedup_key)
        rows.append(row_entry)
        if len(rows) >= 240:
            break
    return prices, rows


def _norm_query(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _default_signal_key(query: str) -> str:
    tokens = re.findall(r"[A-Z0-9-]{5,}", _norm_query(query).upper())
    best = ""
    for token in tokens:
        t = token.strip("-")
        if len(t) < 5:
            continue
        if sum(1 for ch in t if ch.isalpha()) < 1:
            continue
        if sum(1 for ch in t if ch.isdigit()) < 1:
            continue
        if len(t) > len(best):
            best = t
    if best:
        return f"model:{best}"
    compact = re.sub(r"[^A-Z0-9]+", "-", _norm_query(query).upper()).strip("-")
    return f"query:{compact[:80] or 'UNKNOWN'}"


def _flatten(payload: Any, path: str = "") -> Iterable[Tuple[str, Any]]:
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_text = str(key or "")
            child = f"{path}.{key_text}" if path else key_text
            yield from _flatten(value, child)
        return
    if isinstance(payload, list):
        for idx, value in enumerate(payload[:300]):
            child = f"{path}[{idx}]"
            yield from _flatten(value, child)
        return
    yield path, payload


@dataclass
class MetricAccumulator:
    query: str
    query_codes: List[str]
    query_tokens: List[str]
    sold_counts: List[int]
    active_counts: List[int]
    min_prices: List[float]
    median_prices: List[float]
    avg_sold_prices: List[float]
    sold_range_mins: List[float]
    row_prices: List[float]
    filtered_row_prices: List[float]
    filtered_sold_counts: List[int]
    filtered_sold_samples: List[Dict[str, Any]]
    filtered_result_rows: List[Dict[str, Any]]
    raw_result_rows: List[Dict[str, Any]]
    currencies: List[str]

    @classmethod
    def create(cls, query: str = "") -> "MetricAccumulator":
        norm_query = _norm_query(query)
        query_codes = _extract_query_codes(norm_query)
        query_tokens = _extract_query_tokens(norm_query, query_codes)
        return cls(
            query=norm_query,
            query_codes=query_codes,
            query_tokens=query_tokens,
            sold_counts=[],
            active_counts=[],
            min_prices=[],
            median_prices=[],
            avg_sold_prices=[],
            sold_range_mins=[],
            row_prices=[],
            filtered_row_prices=[],
            filtered_sold_counts=[],
            filtered_sold_samples=[],
            filtered_result_rows=[],
            raw_result_rows=[],
            currencies=[],
        )

    def ingest_payload(self, payload: Any) -> None:
        if not isinstance(payload, (dict, list)):
            return
        local_prices: List[float] = []
        for path, value in _flatten(payload):
            key = str(path or "").lower()
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                number = float(value)
                if _RE_SOLD_KEY.search(key):
                    maybe = _to_int(number, -1)
                    if maybe >= 0:
                        self.sold_counts.append(maybe)
                if _RE_ACTIVE_KEY.search(key):
                    maybe = _to_int(number, -1)
                    if maybe >= 0:
                        self.active_counts.append(maybe)
                if _RE_MIN_PRICE_KEY.search(key):
                    if number > 0:
                        self.min_prices.append(number)
                if _RE_MEDIAN_PRICE_KEY.search(key):
                    if number > 0:
                        self.median_prices.append(number)
                if _is_candidate_price_path(key) and number > 0:
                    local_prices.append(number)
            elif isinstance(value, str):
                if _RE_CURRENCY_KEY.search(key):
                    code = value.strip().upper()
                    if re.fullmatch(r"[A-Z]{3}", code):
                        self.currencies.append(code)
        if local_prices:
            # Keep only realistic sold prices to avoid IDs and timestamps.
            filtered = _trim_low_price_outliers(local_prices)
            self.row_prices.extend(filtered)
        payload_prices, payload_sold, payload_sample, payload_rows = _extract_filtered_rows_from_payload(
            payload,
            query_codes=self.query_codes,
            query_tokens=self.query_tokens,
        )
        if payload_prices:
            self.filtered_row_prices.extend(payload_prices)
        if payload_sold >= 0:
            self.filtered_sold_counts.append(payload_sold)
        if isinstance(payload_sample, dict) and payload_sample:
            self.filtered_sold_samples.append(payload_sample)
        if payload_rows:
            self.filtered_result_rows.extend(payload_rows)
        _raw_prices, raw_rows = _extract_raw_rows_from_payload(payload)
        if raw_rows:
            self.raw_result_rows.extend(raw_rows)

    def ingest_html(self, html: str) -> None:
        row_count = len(_RE_HTML_ROW.findall(html or ""))
        if row_count > 0:
            self.sold_counts.append(row_count)
        date_count = len(set(_RE_HTML_ROW_DATE.findall(html or "")))
        if date_count > 0:
            self.sold_counts.append(date_count)
        filtered_prices, filtered_sold, sold_sample, filtered_rows = _extract_filtered_rows_with_rows(
            html,
            query_codes=self.query_codes,
            query_tokens=self.query_tokens,
        )
        if filtered_prices:
            self.filtered_row_prices.extend(filtered_prices)
        if row_count > 0 and filtered_sold >= 0:
            self.filtered_sold_counts.append(filtered_sold)
        if isinstance(sold_sample, dict) and sold_sample:
            self.filtered_sold_samples.append(sold_sample)
        if filtered_rows:
            self.filtered_result_rows.extend(filtered_rows)
        _raw_prices, raw_rows = _extract_raw_rows_with_rows(html)
        if raw_rows:
            self.raw_result_rows.extend(raw_rows)
        for match in _RE_HTML_ROW_PRICE.finditer(html or ""):
            raw = str(match.group(1) or "").replace(",", "")
            value = _to_float(raw, -1.0)
            if value > 0:
                self.row_prices.append(value)

    def ingest_dom_text(self, text: str) -> None:
        for match in _RE_DOM_AVG_SOLD.finditer(text or ""):
            raw = str(match.group(1) or "").replace(",", "")
            value = _to_float(raw, -1.0)
            if value > 0:
                self.avg_sold_prices.append(value)
        for match in _RE_DOM_SOLD_RANGE.finditer(text or ""):
            left = _to_float(str(match.group(1) or "").replace(",", ""), -1.0)
            right = _to_float(str(match.group(2) or "").replace(",", ""), -1.0)
            if left > 0 and right > 0:
                self.sold_range_mins.append(min(left, right))
                self.min_prices.append(min(left, right))
        sold_dates = set(_RE_DOM_DATE_SOLD.findall(text or ""))
        if sold_dates:
            self.sold_counts.append(len(sold_dates))

        for match in _RE_DOM_SOLD.finditer(text or ""):
            raw = str(match.group(1) or "").replace(",", "")
            value = _to_int(raw, -1)
            if value >= 0:
                self.sold_counts.append(value)
        for match in _RE_DOM_PRICE.finditer(text or ""):
            raw = str(match.group(1) or "").replace(",", "")
            value = _to_float(raw, -1.0)
            if value > 0:
                self.row_prices.append(value)

    def finalize(self) -> Dict[str, Any]:
        sold = max(self.sold_counts) if self.sold_counts else -1
        filtered_sold = max(self.filtered_sold_counts) if self.filtered_sold_counts else -1
        # filtered側が0件かつ価格抽出も空のときは抽出失敗の可能性が高いので、
        # DOM由来の件数を優先して偽陰性(売却0)を避ける。
        if filtered_sold > 0:
            sold = filtered_sold
        elif filtered_sold == 0 and self.filtered_row_prices:
            sold = 0
        active = max(self.active_counts) if self.active_counts else -1
        min_price = min(self.min_prices) if self.min_prices else -1.0
        if self.sold_range_mins:
            min_price = min(self.sold_range_mins)
        filtered_rows_trimmed = _trim_low_price_outliers(self.filtered_row_prices)
        fallback_rows_trimmed = _trim_low_price_outliers(self.row_prices)
        if filtered_rows_trimmed:
            min_price = min(filtered_rows_trimmed)
        elif min_price <= 0 and fallback_rows_trimmed:
            min_price = min(fallback_rows_trimmed)
        median_price = -1.0
        if filtered_rows_trimmed:
            median_price = statistics.median(filtered_rows_trimmed)
        elif self.median_prices:
            median_price = statistics.median(self.median_prices)
        elif self.avg_sold_prices:
            median_price = statistics.median(self.avg_sold_prices)
        elif fallback_rows_trimmed:
            median_price = statistics.median(fallback_rows_trimmed)
        if median_price <= 0 and min_price > 0:
            median_price = min_price
        currency = self.currencies[-1] if self.currencies else "USD"
        confidence = 0.30
        if sold >= 0:
            confidence += 0.25
        if self.filtered_row_prices:
            confidence += 0.08
        if min_price > 0:
            confidence += 0.20
        if median_price > 0:
            confidence += 0.20
        if active >= 0:
            confidence += 0.05
        best_sold_sample: Dict[str, Any] = {}
        best_sold_sample_price = -1.0
        for sample in self.filtered_sold_samples:
            if not isinstance(sample, dict) or not sample:
                continue
            sample_price = _to_float(sample.get("sold_price"), -1.0)
            if sample_price <= 0:
                if not best_sold_sample:
                    best_sold_sample = sample
                continue
            if best_sold_sample_price <= 0 or sample_price < best_sold_sample_price:
                best_sold_sample = sample
                best_sold_sample_price = sample_price
        if not best_sold_sample and self.filtered_sold_samples:
            first = self.filtered_sold_samples[0]
            if isinstance(first, dict):
                best_sold_sample = first
        compact_rows: List[Dict[str, Any]] = []
        for row in self.filtered_result_rows:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title", "") or "").strip()
            if not title:
                continue
            out_row: Dict[str, Any] = {"title": title[:220]}
            item_url = str(row.get("item_url", "") or "").strip()
            if item_url:
                out_row["item_url"] = item_url
            item_id = str(row.get("item_id", "") or "").strip()
            if item_id:
                out_row["item_id"] = item_id
            image_url = str(row.get("image_url", "") or "").strip()
            if image_url:
                out_row["image_url"] = image_url
            rank = _to_int(row.get("rank"), 0)
            if rank > 0:
                out_row["rank"] = rank
            sold_price = _to_float(row.get("sold_price"), -1.0)
            if sold_price > 0:
                out_row["sold_price"] = round(sold_price, 4)
            compact_rows.append(out_row)
            if len(compact_rows) >= 120:
                break
        compact_raw_rows: List[Dict[str, Any]] = []
        seen_raw: set[str] = set()
        for row in self.raw_result_rows:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title", "") or "").strip()
            if not title:
                continue
            item_url = str(row.get("item_url", "") or "").strip()
            dedup_key = _normalize_code(title) + "|" + _normalize_code(item_url)
            if dedup_key in seen_raw:
                continue
            seen_raw.add(dedup_key)
            out_row: Dict[str, Any] = {"title": title[:220]}
            if item_url:
                out_row["item_url"] = item_url
            item_id = str(row.get("item_id", "") or "").strip()
            if item_id:
                out_row["item_id"] = item_id
            image_url = str(row.get("image_url", "") or "").strip()
            if image_url:
                out_row["image_url"] = image_url
            rank = _to_int(row.get("rank"), 0)
            if rank > 0:
                out_row["rank"] = rank
            sold_price = _to_float(row.get("sold_price"), -1.0)
            if sold_price > 0:
                out_row["sold_price"] = round(sold_price, 4)
            compact_raw_rows.append(out_row)
            if len(compact_raw_rows) >= 120:
                break

        # filtered抽出でsampleが作れない場合でも、raw rowsにURL/価格が残っていれば
        # 最低限のsold sample参照を作る（C段階の根拠参照を確保）。
        if (not best_sold_sample) and compact_raw_rows:
            for row in compact_raw_rows:
                if not isinstance(row, dict):
                    continue
                item_url = str(row.get("item_url", "") or "").strip()
                if not item_url:
                    continue
                sample_price = _to_float(row.get("sold_price"), -1.0)
                if sample_price <= 0 and min_price > 0:
                    sample_price = float(min_price)
                if sample_price <= 0:
                    continue
                sample_title = str(row.get("title", "") or "").strip()
                sample: Dict[str, Any] = {
                    "sold_price": round(sample_price, 4),
                    "item_url": item_url,
                    "source": "raw_rows_fallback",
                }
                sample_item_id = str(row.get("item_id", "") or "").strip()
                if sample_item_id:
                    sample["item_id"] = sample_item_id
                else:
                    sample_item_id = _ebay_item_id_from_url(item_url)
                    if sample_item_id:
                        sample["item_id"] = sample_item_id
                if sample_title:
                    sample["title"] = sample_title
                image_url = str(row.get("image_url", "") or "").strip()
                if image_url:
                    sample["image_url"] = image_url
                best_sold_sample = sample
                break

        return {
            "sold_90d_count": sold,
            "active_count": active,
            "sold_price_min": round(min_price, 4) if min_price > 0 else -1.0,
            "sold_price_median": round(median_price, 4) if median_price > 0 else -1.0,
            "sold_price_currency": currency,
            "confidence": round(min(0.95, max(0.0, confidence)), 4),
            "raw_row_count": max(len(self.row_prices), len(self.raw_result_rows)),
            "filtered_row_count": len(self.filtered_row_prices),
            "sold_sample": best_sold_sample,
            "filtered_result_rows": compact_rows,
            "raw_result_rows": compact_raw_rows,
        }


def _rows_to_active_rows(rows: Any) -> List[Dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        converted = dict(row)
        price = _to_float(converted.get("sold_price"), -1.0)
        if price > 0:
            converted["active_price"] = round(price, 4)
        out.append(converted)
    return out


def _sold_sample_to_active_sample(sample: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(sample, dict):
        return {}
    out: Dict[str, Any] = {}
    title = str(sample.get("title", "") or "").strip()
    if title:
        out["title"] = title[:220]
    item_url = str(sample.get("item_url", "") or "").strip()
    if item_url:
        out["item_url"] = item_url
    item_id = str(sample.get("item_id", "") or "").strip()
    if item_id:
        out["item_id"] = item_id
    image_url = str(sample.get("image_url", "") or "").strip()
    if image_url:
        out["image_url"] = image_url
    price = _to_float(sample.get("sold_price"), _to_float(sample.get("sold_price_usd"), -1.0))
    if price > 0:
        out["active_price"] = round(price, 4)
    return out


def _collect_active_tab_metrics(
    page: Any,
    *,
    query: str,
    wait_seconds: int,
    screenshot_template: str = "",
    html_template: str = "",
    query_index: int = 1,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "active_tab_state": {},
        "active_count": -1,
        "active_price_min": -1.0,
        "active_price_median": -1.0,
        "active_sample": {},
        "active_result_rows": [],
        "raw_active_result_rows": [],
        "daily_limit_reached": False,
        "screenshot_path": "",
        "html_path": "",
    }
    state = _switch_to_active_tab(page, wait_seconds=max(1, int(wait_seconds)))
    result["active_tab_state"] = state
    if not bool(state.get("active_tab_selected")):
        return result

    acc = MetricAccumulator.create(query=query)

    def _on_response(resp: Any) -> None:
        try:
            req = resp.request
            if req.resource_type not in {"fetch", "xhr"}:
                return
            if not _is_research_response(resp):
                return
            body = resp.json()
            acc.ingest_payload(body)
        except Exception:
            return

    page.on("response", _on_response)
    try:
        list_state = _wait_for_research_list_visible(page, max(1, int(wait_seconds)))
        state["active_tab_rows"] = max(0, _to_int(list_state.get("rows"), 0))
        state["active_tab_no_sold_message"] = bool(list_state.get("no_sold"))
        state["active_tab_busy"] = bool(list_state.get("busy"))
        state["active_tab_count_label"] = _detect_active_count_from_tab_label(page)
        response_wait_timeout_ms = max(
            800,
            _to_int(
                os.getenv(
                    "LIQUIDITY_RPA_RESPONSE_WAIT_TIMEOUT_MS",
                    str(min(2200, max(1000, int(max(1, wait_seconds) * 300)))),
                ),
                min(2200, max(1000, int(max(1, wait_seconds) * 300))),
            ),
        )
        try:
            page.wait_for_response(
                lambda resp: bool(_is_research_response(resp)),
                timeout=response_wait_timeout_ms,
            )
        except Exception:
            pass
        _wait_for_research_ready(page, max(1, min(3, int(wait_seconds))))
        try:
            html = page.content()
            acc.ingest_html(html)
            if _contains_daily_limit_message(html):
                result["daily_limit_reached"] = True
        except Exception:
            html = ""
        if not acc.filtered_row_prices and not acc.row_prices:
            try:
                text = page.inner_text("body")
                acc.ingest_dom_text(text)
                if _contains_daily_limit_message(text):
                    result["daily_limit_reached"] = True
            except Exception:
                pass

        screenshot_template = str(screenshot_template or "").strip()
        html_template = str(html_template or "").strip()
        if screenshot_template:
            shot_path = _resolve_filters_screenshot_path(screenshot_template, query, query_index)
            shot_path.parent.mkdir(parents=True, exist_ok=True)
            shot_timeout_ms = max(3000, _to_int(os.getenv("LIQUIDITY_RPA_SCREENSHOT_TIMEOUT_MS", "15000"), 15000))
            page.screenshot(path=str(shot_path), full_page=False, timeout=shot_timeout_ms)
            result["screenshot_path"] = str(shot_path)
        if html_template:
            active_html_path = _resolve_filters_html_path(html_template, query, query_index)
            active_html_path.parent.mkdir(parents=True, exist_ok=True)
            active_html_path.write_text(str(page.content() or ""), encoding="utf-8")
            result["html_path"] = str(active_html_path)
    finally:
        page.remove_listener("response", _on_response)

    active_metrics = acc.finalize()
    active_count = _to_int(active_metrics.get("active_count"), -1)
    if active_count < 0:
        active_count = _to_int(state.get("active_tab_count_label"), -1)
    if active_count < 0:
        active_count = max(0, _to_int(state.get("active_tab_rows"), 0)) if _to_int(state.get("active_tab_rows"), 0) > 0 else -1
    result["active_count"] = active_count
    result["active_price_min"] = _to_float(active_metrics.get("sold_price_min"), -1.0)
    result["active_price_median"] = _to_float(active_metrics.get("sold_price_median"), -1.0)
    sample = active_metrics.get("sold_sample") if isinstance(active_metrics.get("sold_sample"), dict) else {}
    result["active_sample"] = _sold_sample_to_active_sample(sample)
    result["active_result_rows"] = _rows_to_active_rows(active_metrics.get("filtered_result_rows"))
    result["raw_active_result_rows"] = _rows_to_active_rows(active_metrics.get("raw_result_rows"))
    return result


def _load_queries(args: argparse.Namespace) -> List[str]:
    out: List[str] = []
    for item in args.query:
        text = _norm_query(item)
        if text:
            out.append(text)
    if args.queries_file:
        path = Path(args.queries_file).expanduser()
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                text = _norm_query(line)
                if text and not text.startswith("#"):
                    out.append(text)
    dedup: List[str] = []
    seen = set()
    for q in out:
        key = q.lower()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(q)
    return dedup


def _safe_name_token(text: str, max_len: int = 72) -> str:
    token = re.sub(r"[^A-Za-z0-9._-]+", "_", str(text or "").strip())
    token = re.sub(r"_+", "_", token).strip("._-")
    if not token:
        token = "query"
    return token[: max(8, int(max_len))]


def _resolve_filters_screenshot_path(template: str, query: str, query_index: int) -> Path:
    ts = time.strftime("%Y%m%dT%H%M%S")
    safe_query = _safe_name_token(query, 64)
    raw = str(template or "").strip()
    has_placeholders = any(mark in raw for mark in ("{query}", "{index}", "{ts}"))
    expanded = (
        raw.replace("{query}", safe_query)
        .replace("{index}", str(max(1, int(query_index))))
        .replace("{ts}", ts)
    )
    path = Path(expanded).expanduser()
    if not path.is_absolute():
        path = (ROOT_DIR / path).resolve()
    if not path.suffix:
        path = path.with_suffix(".png")
    if (not has_placeholders) and int(query_index) > 1:
        path = path.with_name(f"{path.stem}_{int(query_index)}{path.suffix}")
    return path


def _resolve_filters_html_path(template: str, query: str, query_index: int) -> Path:
    ts = time.strftime("%Y%m%dT%H%M%S")
    safe_query = _safe_name_token(query, 64)
    raw = str(template or "").strip()
    has_placeholders = any(mark in raw for mark in ("{query}", "{index}", "{ts}"))
    expanded = (
        raw.replace("{query}", safe_query)
        .replace("{index}", str(max(1, int(query_index))))
        .replace("{ts}", ts)
    )
    path = Path(expanded).expanduser()
    if not path.is_absolute():
        path = (ROOT_DIR / path).resolve()
    if not path.suffix:
        path = path.with_suffix(".html")
    if (not has_placeholders) and int(query_index) > 1:
        path = path.with_name(f"{path.stem}_{int(query_index)}{path.suffix}")
    return path


def _normalize_sold_sort(raw_sort: str) -> str:
    text = str(raw_sort or "").strip().lower().replace("-", "_").replace(" ", "_")
    compact = re.sub(r"[^a-z0-9]+", "", str(raw_sort or "").strip().lower())
    if text in {"", "default", "top_rated", "toprated", "best_match"}:
        return "default"
    if text in {"price_plus_shipping_asc", "price_plus_shipping_desc"}:
        return "price_asc" if text.endswith("_asc") else "price_desc"
    if compact in {"priceplusshippingasc", "priceplusshippingdesc"}:
        return "price_asc" if compact.endswith("asc") else "price_desc"
    if compact in {"datelastsold", "datelastsolddesc"}:
        return "recently_sold"
    if text in {"recently_sold", "recent", "most_recent", "newest", "newest_first", "latest"}:
        return "recently_sold"
    if text in {"price_desc", "price_high", "highest_price"}:
        return "price_desc"
    if text in {"price_asc", "price_low", "lowest_price"}:
        return "price_asc"
    return "default"


def _sold_sort_url_params(sold_sort: str) -> Dict[str, str]:
    mode = _normalize_sold_sort(sold_sort)
    if mode == "recently_sold":
        # eBay Product Research exposes "Date last sold" sort as `sorting=datelastsold`.
        return {"sorting": "datelastsold"}
    if mode == "price_desc":
        return {"sort": "PRICE_PLUS_SHIPPING_DESC"}
    if mode == "price_asc":
        return {"sort": "PRICE_PLUS_SHIPPING_ASC"}
    return {}


def _detect_listing_price_metric_selected(page: Any) -> bool:
    try:
        return bool(
            page.evaluate(
                """() => {
                  const visible = (el) => {
                    if (!el) return false;
                    const st = window.getComputedStyle(el);
                    if (st.display === "none" || st.visibility === "hidden") return false;
                    const r = el.getBoundingClientRect();
                    return r.width > 1 && r.height > 1;
                  };
                  const rows = [...document.querySelectorAll("tr.research-table-row, div.research-table-row")];
                  if (rows.length <= 0) return false;
                  for (const row of rows.slice(0, 8)) {
                    if (!visible(row)) continue;
                    if (row.querySelector("[class*='listingPrice']")) return true;
                  }
                  return false;
                }"""
            )
        )
    except Exception:
        return False


def _is_listing_price_metric_available(page: Any) -> bool:
    try:
        return bool(
            page.evaluate(
                """() => {
                  const visible = (el) => {
                    if (!el) return false;
                    const st = window.getComputedStyle(el);
                    if (st.display === "none" || st.visibility === "hidden") return false;
                    const r = el.getBoundingClientRect();
                    return r.width > 1 && r.height > 1;
                  };
                  const selectors = [
                    "th.research-table-header__listing-price",
                    "th.active-listing-table-header__listingPrice",
                    "th[class*='listing-price']",
                    "th[class*='listingPrice']",
                    "[role='columnheader'][class*='listing-price']",
                    "[role='columnheader'][class*='listingPrice']",
                  ];
                  for (const sel of selectors) {
                    const node = document.querySelector(sel);
                    if (visible(node)) return true;
                  }
                  const labels = [...document.querySelectorAll("button, [role='button'], th, [role='columnheader'], span, div")];
                  for (const el of labels) {
                    if (!visible(el)) continue;
                    const txt = (el.textContent || "").replace(/\\s+/g, " ").trim().toLowerCase();
                    if (!txt || txt.length > 120) continue;
                    if (txt.includes("listing price")) return true;
                  }
                  return false;
                }"""
            )
        )
    except Exception:
        return False


def _set_listing_price_metric(page: Any) -> Dict[str, Any]:
    state: Dict[str, Any] = {
        "price_metric_target": "listing_price",
        "price_metric_selected": False,
        "price_metric_selection_source": "",
        "price_metric_available": False,
    }
    state["price_metric_available"] = _is_listing_price_metric_available(page)
    if not bool(state.get("price_metric_available")):
        state["price_metric_selection_source"] = "not_available"
        return state
    if _detect_listing_price_metric_selected(page):
        state["price_metric_selected"] = True
        state["price_metric_selection_source"] = "detected_before"
        return state
    clicked = _click_first(
        page,
        [
            "th.active-listing-table-header__listingPrice:visible",
            "th.active-listing-table-header__listingPrice .active-listing-table-header__inner-item:visible",
            "th.research-table-header__listing-price:visible",
            "th.research-table-header__listing-price .research-table-header__inner-item:visible",
            "th[class*='listingPrice']:visible",
            "th[class*='listing-price']:visible",
            "[role='columnheader'][class*='listingPrice']:visible",
            "[role='columnheader'][class*='listing-price']:visible",
            "button:visible:has-text('Listing price')",
            "[role='button']:visible:has-text('Listing price')",
            "th:visible:has-text('Listing price')",
            "[role='columnheader']:visible:has-text('Listing price')",
            "button:visible:has-text('Listing')",
            "[role='button']:visible:has-text('Listing')",
        ],
    )
    if not clicked:
        clicked = _click_button_by_text_tokens(page, ["listing price", "listing"])
    if not clicked:
        return state
    _wait_for_research_ready(page, 2)
    page.wait_for_timeout(max(40, _to_int(os.getenv("LIQUIDITY_RPA_FILTER_SETTLE_MS", "45"), 45)))
    if _detect_listing_price_metric_selected(page):
        state["price_metric_selected"] = True
        state["price_metric_selection_source"] = "ui"
    return state


def _search_and_wait(
    page: Any,
    query: str,
    wait_seconds: int,
    *,
    result_offset: int = 0,
    result_limit: int = 50,
    category_id: int = 0,
    category_slug: str = "",
    fixed_price_only: bool = False,
    condition: str = "",
    min_price_usd: float = 0.0,
    sold_sort: str = "default",
) -> None:
    wait_until = str(os.getenv("LIQUIDITY_RPA_GOTO_WAIT_UNTIL", "commit") or "commit").strip().lower()
    if wait_until not in {"commit", "domcontentloaded", "load"}:
        wait_until = "commit"
    settle_ms = max(0, _to_int(os.getenv("LIQUIDITY_RPA_POST_GOTO_SETTLE_MS", "70"), 70))
    nav_retry = max(0, _to_int(os.getenv("LIQUIDITY_RPA_GOTO_RETRY_COUNT", "2"), 2))
    nav_retry_wait_ms = max(80, _to_int(os.getenv("LIQUIDITY_RPA_GOTO_RETRY_WAIT_MS", "220"), 220))
    params = {
        "marketplace": "EBAY-US",
        "keywords": str(query or ""),
        "tabName": "SOLD",
    }
    safe_category_id = max(0, int(category_id))
    safe_category_slug = re.sub(r"[^a-z0-9_-]+", "", str(category_slug or "").strip().lower())
    if safe_category_id > 0:
        params["categoryId"] = str(safe_category_id)
    if safe_category_slug:
        params["category"] = safe_category_slug
    safe_offset = max(0, int(result_offset))
    safe_limit = max(10, min(200, int(result_limit)))
    safe_min_price = max(0.0, _to_float(min_price_usd, 0.0))
    safe_sold_sort = _normalize_sold_sort(sold_sort)
    if str(condition or "").strip().lower() == "new":
        params["conditionId"] = "1000"
    if safe_min_price > 0:
        min_price_text = f"{safe_min_price:.2f}".rstrip("0").rstrip(".")
        if min_price_text:
            params["minPrice"] = min_price_text
    for param_key, param_value in _sold_sort_url_params(safe_sold_sort).items():
        if str(param_key or "").strip() and str(param_value or "").strip():
            params[str(param_key).strip()] = str(param_value).strip()
    if safe_offset > 0:
        params["offset"] = str(safe_offset)
    params["limit"] = str(safe_limit)
    url = "https://www.ebay.com/sh/research?" + urllib.parse.urlencode(params)

    def _goto_with_retry(target_url: str) -> bool:
        last_error: Optional[Exception] = None
        for attempt in range(nav_retry + 1):
            try:
                page.goto(target_url, wait_until=wait_until)
                return True
            except Exception as err:
                last_error = err
                if _is_transient_navigation_error(err) and attempt < nav_retry:
                    page.wait_for_timeout(nav_retry_wait_ms)
                    continue
                if _is_transient_navigation_error(err):
                    return False
                raise
        if last_error is not None and not _is_transient_navigation_error(last_error):
            raise last_error
        return False

    _goto_with_retry(url)
    if settle_ms > 0:
        page.wait_for_timeout(settle_ms)
    if "/sh/research" not in str(page.url or ""):
        _goto_with_retry(url)
        if settle_ms > 0:
            page.wait_for_timeout(settle_ms)
    pre_filter_wait = max(
        2,
        _to_int(
            os.getenv("LIQUIDITY_RPA_PRE_FILTER_WAIT_SECONDS", str(max(2, min(4, int(wait_seconds))))),
            max(2, min(4, int(wait_seconds))),
        ),
    )
    _wait_for_research_interactive(page, pre_filter_wait)


def _wait_for_research_ready(page: Any, wait_seconds: int) -> bool:
    timeout_ms = int(max(1000, wait_seconds * 1000))
    deadline = time.time() + (timeout_ms / 1000.0)
    poll_ms = max(80, _to_int(os.getenv("LIQUIDITY_RPA_READY_POLL_MS", "120"), 120))
    while time.time() < deadline:
        try:
            ready = page.evaluate(
                """() => {
                  const rows = document.querySelectorAll("tr.research-table-row, div.research-table-row").length;
                  const text = (document.body && document.body.innerText) ? document.body.innerText : "";
                  const hasSummary = /Avg sold price|Sold price range|\\b[0-9,]+\\s*(sold|sales)\\b/i.test(text);
                  const busy = !!document.querySelector('[aria-busy="true"], .loading, .spinner, .skeleton');
                  return (rows > 0 || hasSummary) && !busy;
                }"""
            )
            if bool(ready):
                page.wait_for_timeout(min(140, poll_ms))
                return True
        except Exception:
            pass
        page.wait_for_timeout(poll_ms)
    return False


def _get_research_list_state(page: Any) -> Dict[str, Any]:
    state: Dict[str, Any] = {"rows": 0, "no_sold": False, "busy": False}
    try:
        result = page.evaluate(
            """() => {
              const rows = document.querySelectorAll("tr.research-table-row, div.research-table-row").length;
              const text = (document.body && document.body.innerText) ? document.body.innerText : "";
              const noSold = /no\\s+sold\\s+(items?|results?|found)/i.test(text);
              const busy = !!document.querySelector('[aria-busy="true"], .loading, .spinner, .skeleton');
              return { rows, noSold, busy };
            }"""
        )
        if isinstance(result, dict):
            state["rows"] = max(0, _to_int(result.get("rows"), 0))
            state["no_sold"] = bool(result.get("noSold"))
            state["busy"] = bool(result.get("busy"))
    except Exception:
        return state
    return state


def _wait_for_research_list_visible(page: Any, wait_seconds: int) -> Dict[str, Any]:
    timeout_ms = int(max(1000, wait_seconds * 1000))
    deadline = time.time() + (timeout_ms / 1000.0)
    poll_ms = max(80, _to_int(os.getenv("LIQUIDITY_RPA_READY_POLL_MS", "120"), 120))
    last_state: Dict[str, Any] = {"rows": 0, "no_sold": False, "busy": False}
    while time.time() < deadline:
        last_state = _get_research_list_state(page)
        rows_now = _to_int(last_state.get("rows"), 0)
        no_sold_now = bool(last_state.get("no_sold"))
        busy_now = bool(last_state.get("busy"))
        if (rows_now > 0 or no_sold_now) and not busy_now:
            page.wait_for_timeout(min(140, poll_ms))
            return last_state
        page.wait_for_timeout(poll_ms)
    return _get_research_list_state(page)


def _wait_for_research_interactive(page: Any, wait_seconds: int) -> bool:
    timeout_ms = int(max(900, wait_seconds * 1000))
    deadline = time.time() + (timeout_ms / 1000.0)
    poll_ms = max(60, _to_int(os.getenv("LIQUIDITY_RPA_READY_POLL_MS", "120"), 120))
    while time.time() < deadline:
        try:
            interactive = page.evaluate(
                """() => {
                  const visible = (el) => {
                    if (!el) return false;
                    const st = window.getComputedStyle(el);
                    if (st.display === "none" || st.visibility === "hidden") return false;
                    const r = el.getBoundingClientRect();
                    return r.width > 0 && r.height > 0;
                  };
                  const soldTab = [...document.querySelectorAll("[role='tab'], button, div")].some((el) => {
                    const t = (el.textContent || "").trim().toLowerCase();
                    return visible(el) && (
                      t === "sold" ||
                      t.startsWith("sold ") ||
                      t.includes(" sold") ||
                      t.includes("売れた")
                    );
                  });
                  const hasFilterButton = [...document.querySelectorAll("button, [role='button']")].some((el) => {
                    const t = (el.textContent || "").trim().toLowerCase();
                    return visible(el) && (
                      t.includes("condition filter") ||
                      t.includes("format filter") ||
                      t.includes("price filter") ||
                      t.includes("more filters") ||
                      t.includes("lock selected filters") ||
                      t.startsWith("last ") ||
                      t.includes("コンディション") ||
                      t.includes("販売形式") ||
                      t.includes("価格") ||
                      t.includes("絞り込み")
                    );
                  });
                  const hasResearchRow = document.querySelectorAll("tr.research-table-row, div.research-table-row").length > 0;
                  const hasSummary = !!document.querySelector("[class*='avgSoldPrice'], [class*='soldPriceRange'], [class*='research-table']");
                  return soldTab || hasFilterButton || hasResearchRow || hasSummary;
                }"""
            )
            if bool(interactive):
                return True
        except Exception:
            pass
        page.wait_for_timeout(poll_ms)
    return False


def _is_research_response(resp: Any) -> bool:
    url = str(getattr(resp, "url", "") or "").lower()
    if not url:
        return False
    if not any(token in url for token in ("/sh/research", "terapeak", "research", "sales")):
        return False
    try:
        headers = resp.headers or {}
    except Exception:
        headers = {}
    ctype = str(headers.get("content-type", "") or "").lower()
    if ctype and ("json" not in ctype) and ("javascript" not in ctype):
        return False
    return True


def _click_first(page: Any, selectors: List[str], timeout_ms: int = 2500) -> bool:
    for selector in selectors:
        try:
            loc = page.locator(selector).first
            if loc.count() <= 0:
                continue
            loc.click(timeout=timeout_ms)
            return True
        except Exception:
            continue
    return False


def _click_button_by_text_tokens(page: Any, tokens: List[str]) -> bool:
    try:
        clicked = page.evaluate(
            """(tokens) => {
              const norm = (s) => (s || "").toLowerCase().replace(/\\s+/g, " ").trim();
              const visible = (el) => {
                if (!el) return false;
                const st = window.getComputedStyle(el);
                if (st.display === "none" || st.visibility === "hidden") return false;
                const r = el.getBoundingClientRect();
                return r.width > 1 && r.height > 1;
              };
              const needles = (tokens || []).map((v) => norm(v)).filter(Boolean);
              const nodes = [...document.querySelectorAll("button, [role='button'], [role='tab']")];
              for (const el of nodes) {
                if (!visible(el)) continue;
                const txt = norm(el.textContent || "");
                if (!txt || txt.length > 96) continue;
                if (!needles.some((t) => txt.includes(t))) continue;
                try { el.click(); return true; } catch (e) {}
              }
              return false;
            }""",
            tokens,
        )
        return bool(clicked)
    except Exception:
        return False


def _detect_sold_tab_selected(page: Any) -> bool:
    try:
        result = page.evaluate(
            """() => {
              const visible = (el) => {
                if (!el) return false;
                const st = window.getComputedStyle(el);
                if (st.display === "none" || st.visibility === "hidden") return false;
                const r = el.getBoundingClientRect();
                return r.width > 1 && r.height > 1;
              };
              const nodes = [...document.querySelectorAll("[role='tab'], button, [role='button'], a, div")];
              for (const el of nodes) {
                if (!visible(el)) continue;
                const text = (el.textContent || "").trim().toLowerCase();
                if (!text) continue;
                if (!(text === "sold" || text.startsWith("sold ") || text.includes(" sold"))) continue;
                const ariaSelected = (el.getAttribute("aria-selected") || "").toLowerCase();
                const ariaCurrent = (el.getAttribute("aria-current") || "").toLowerCase();
                const ariaPressed = (el.getAttribute("aria-pressed") || "").toLowerCase();
                const className = ((el.className || "") + "").toLowerCase();
                if (ariaSelected === "true" || ariaCurrent === "true" || ariaPressed === "true") return true;
                if (className.includes("active") || className.includes("selected")) return true;
              }
              return false;
            }"""
        )
        return bool(result)
    except Exception:
        return False


def _detect_active_tab_selected(page: Any) -> bool:
    try:
        result = page.evaluate(
            """() => {
              const visible = (el) => {
                if (!el) return false;
                const st = window.getComputedStyle(el);
                if (st.display === "none" || st.visibility === "hidden") return false;
                const r = el.getBoundingClientRect();
                return r.width > 1 && r.height > 1;
              };
              const nodes = [...document.querySelectorAll("[role='tab'], button, [role='button'], a, div")];
              for (const el of nodes) {
                if (!visible(el)) continue;
                const text = (el.textContent || "").trim().toLowerCase();
                if (!text) continue;
                if (!(text === "active" || text.startsWith("active ") || text.includes(" active"))) continue;
                const ariaSelected = (el.getAttribute("aria-selected") || "").toLowerCase();
                const ariaCurrent = (el.getAttribute("aria-current") || "").toLowerCase();
                const ariaPressed = (el.getAttribute("aria-pressed") || "").toLowerCase();
                const className = ((el.className || "") + "").toLowerCase();
                if (ariaSelected === "true" || ariaCurrent === "true" || ariaPressed === "true") return true;
                if (className.includes("active") || className.includes("selected")) return true;
              }
              return false;
            }"""
        )
        return bool(result)
    except Exception:
        return False


def _detect_fixed_price_selected(page: Any) -> bool:
    try:
        result = page.evaluate(
            """() => {
              const visible = (el) => {
                if (!el) return false;
                const st = window.getComputedStyle(el);
                if (st.display === "none" || st.visibility === "hidden") return false;
                const r = el.getBoundingClientRect();
                return r.width > 1 && r.height > 1;
              };
              const norm = (s) => (s || "").toLowerCase().replace(/\\s+/g, " ").trim();
              const isChecked = (el) => {
                if (!el) return false;
                const ariaChecked = norm(el.getAttribute("aria-checked"));
                const ariaPressed = norm(el.getAttribute("aria-pressed"));
                if (ariaChecked === "true" || ariaPressed === "true") return true;
                const input = el.matches?.("input[type='checkbox'], input[type='radio']")
                  ? el
                  : el.querySelector?.("input[type='checkbox'], input[type='radio']");
                return !!(input && input.checked);
              };

              // 1) Selected badge on the Format filter pill.
              const pills = [...document.querySelectorAll("button, [role='button'], [role='tab']")];
              for (const el of pills) {
                if (!visible(el)) continue;
                const txt = norm(el.textContent || "");
                if (!txt || txt.length > 80) continue;
                if (!(txt.includes("format filter") || txt.includes("selling format") || txt.includes("販売形式"))) continue;
                if (txt.includes("selected") || txt.includes("選択")) return true;
              }

              // 2) Checked option in currently open popup.
              const roots = [...document.querySelectorAll("[role='menu'], [role='dialog'], [role='listbox'], [aria-modal='true']")];
              for (const root of roots) {
                if (!visible(root)) continue;
                const options = [
                  ...root.querySelectorAll(
                    "[role='menuitemcheckbox'], [role='menuitemradio'], [role='checkbox'], [role='radio'], label, li, div, span"
                  ),
                ];
                for (const el of options) {
                  if (!visible(el)) continue;
                  const txt = norm(el.textContent || "");
                  if (!(txt.includes("fixed price") || txt.includes("buy it now") || txt.includes("固定価格") || txt.includes("即決"))) continue;
                  if (isChecked(el)) return true;
                }
              }
              return false;
            }"""
        )
        return bool(result)
    except Exception:
        return False


def _detect_sold_filters_from_url(page: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "tab_sold": False,
        "tab_active": False,
        "tab_name": "",
        "fixed_price": False,
        "condition_new": False,
        "min_price": 0.0,
        "sold_sort": "default",
        "sold_sort_raw": "",
    }
    try:
        current = str(getattr(page, "url", "") or "").strip()
    except Exception:
        current = ""
    if not current:
        return out
    try:
        parsed = urllib.parse.urlparse(current)
        params = urllib.parse.parse_qs(parsed.query or "")
        tab_values = [str(v or "").strip().lower() for v in params.get("tabName", [])]
        format_values = [str(v or "").strip().lower() for v in params.get("format", [])]
        condition_values = [str(v or "").strip().lower() for v in params.get("conditionId", [])]
        min_price_values = [str(v or "").strip() for v in params.get("minPrice", [])]
        sort_values = [
            str(v or "").strip()
            for key in ("sort", "sortBy", "sortOrder", "sorting")
            for v in params.get(key, [])
            if str(v or "").strip()
        ]
        out["tab_sold"] = any(value == "sold" for value in tab_values)
        out["tab_active"] = any(value == "active" for value in tab_values)
        if tab_values:
            out["tab_name"] = str(tab_values[0] or "").strip().upper()
        out["fixed_price"] = any(value in {"fixed_price", "buy_it_now", "bin"} for value in format_values)
        out["condition_new"] = any(value in {"1000", "new"} for value in condition_values)
        raw_sort = str(sort_values[0] if sort_values else "").strip()
        out["sold_sort_raw"] = raw_sort
        normalized_sort = _normalize_sold_sort(raw_sort)
        if normalized_sort == "default" and "datelastsold" in raw_sort.lower().replace("_", "").replace("-", ""):
            normalized_sort = "recently_sold"
        out["sold_sort"] = normalized_sort
        parsed_min_price = 0.0
        for raw_min in min_price_values:
            value = _to_float(raw_min, 0.0)
            if value > parsed_min_price:
                parsed_min_price = value
        out["min_price"] = round(max(0.0, parsed_min_price), 2)
    except Exception:
        return out
    return out


def _rewrite_research_tab_url(current_url: str, tab_name: str) -> str:
    raw_url = str(current_url or "").strip()
    if not raw_url:
        return ""
    safe_tab = str(tab_name or "").strip().upper()
    if safe_tab not in {"SOLD", "ACTIVE"}:
        safe_tab = "SOLD"
    try:
        parsed = urllib.parse.urlparse(raw_url)
    except Exception:
        return ""
    if "/sh/research" not in str(parsed.path or ""):
        return ""
    params = urllib.parse.parse_qs(parsed.query or "")
    params["tabName"] = [safe_tab]
    rebuilt = urllib.parse.urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urllib.parse.urlencode(params, doseq=True),
            parsed.fragment,
        )
    )
    return str(rebuilt or "").strip()


def _switch_to_active_tab(page: Any, *, wait_seconds: int) -> Dict[str, Any]:
    state: Dict[str, Any] = {
        "active_tab_attempted": True,
        "active_tab_selected": False,
        "active_tab_selection_source": "",
        "active_tab_url_attempted": False,
        "active_tab_url_target": "",
        "active_tab_ui_attempted": False,
        "active_tab_ui_clicked": False,
    }
    wait_sec = max(1, int(wait_seconds))
    try:
        current_url = str(getattr(page, "url", "") or "").strip()
    except Exception:
        current_url = ""
    state["active_tab_url_before"] = current_url

    target_url = _rewrite_research_tab_url(current_url, "ACTIVE")
    if target_url:
        state["active_tab_url_attempted"] = True
        state["active_tab_url_target"] = target_url
        try:
            page.goto(target_url, wait_until="commit")
            _wait_for_research_interactive(page, wait_sec)
            _wait_for_research_ready(page, wait_sec)
            url_state = _detect_sold_filters_from_url(page)
            if _detect_active_tab_selected(page) or bool(url_state.get("tab_active")):
                state["active_tab_selected"] = True
                state["active_tab_selection_source"] = "url"
        except Exception as err:
            state["active_tab_url_error"] = f"{type(err).__name__}:{err}"[:200]

    if not bool(state.get("active_tab_selected")):
        state["active_tab_ui_attempted"] = True
        clicked = _click_button_by_text_tokens(page, ["active", "出品中"])
        state["active_tab_ui_clicked"] = bool(clicked)
        if clicked:
            try:
                page.wait_for_timeout(max(30, _to_int(os.getenv("LIQUIDITY_RPA_FILTER_SETTLE_MS", "45"), 45)))
                _wait_for_research_ready(page, wait_sec)
                url_state = _detect_sold_filters_from_url(page)
                if _detect_active_tab_selected(page) or bool(url_state.get("tab_active")):
                    state["active_tab_selected"] = True
                    state["active_tab_selection_source"] = "ui"
            except Exception as err:
                state["active_tab_ui_error"] = f"{type(err).__name__}:{err}"[:200]

    try:
        state["active_tab_url_after"] = str(getattr(page, "url", "") or "").strip()
    except Exception:
        state["active_tab_url_after"] = ""
    return state


def _detect_active_count_from_tab_label(page: Any) -> int:
    try:
        value = page.evaluate(
            """() => {
              const visible = (el) => {
                if (!el) return false;
                const st = window.getComputedStyle(el);
                if (st.display === "none" || st.visibility === "hidden") return false;
                const r = el.getBoundingClientRect();
                return r.width > 1 && r.height > 1;
              };
              const nodes = [...document.querySelectorAll("[role='tab'], button, [role='button'], a, div")];
              for (const el of nodes) {
                if (!visible(el)) continue;
                const text = (el.textContent || "").trim();
                if (!/active/i.test(text)) continue;
                const m = text.match(/active\\s*\\(([0-9,]+)\\)/i);
                if (m && m[1]) {
                  return parseInt(String(m[1]).replace(/,/g, ""), 10) || -1;
                }
              }
              return -1;
            }"""
        )
        return max(-1, _to_int(value, -1))
    except Exception:
        return -1


def _get_result_offset_from_url(url: str) -> int:
    raw = str(url or "").strip()
    if not raw:
        return 0
    try:
        parsed = urllib.parse.urlparse(raw)
        params = urllib.parse.parse_qs(parsed.query or "")
        return max(0, _to_int((params.get("offset", ["0"]) or ["0"])[0], 0))
    except Exception:
        return 0


def _ensure_result_offset(page: Any, result_offset: int, wait_seconds: int = 4) -> Dict[str, Any]:
    target = max(0, int(result_offset))
    state: Dict[str, Any] = {
        "offset_target": target,
        "offset_before": 0,
        "offset_after": 0,
        "offset_reapplied": False,
        "offset_confirmed": target == 0,
    }
    if target <= 0:
        return state
    current_url = str(getattr(page, "url", "") or "").strip()
    before = _get_result_offset_from_url(current_url)
    state["offset_before"] = before
    if before == target:
        state["offset_after"] = before
        state["offset_confirmed"] = True
        return state
    try:
        parsed = urllib.parse.urlparse(current_url)
        params = urllib.parse.parse_qs(parsed.query or "")
        params["offset"] = [str(target)]
        rebuilt = urllib.parse.urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                urllib.parse.urlencode(params, doseq=True),
                parsed.fragment,
            )
        )
        page.goto(rebuilt, wait_until="commit")
        _wait_for_research_ready(page, max(2, int(wait_seconds)))
        state["offset_reapplied"] = True
        after = _get_result_offset_from_url(str(getattr(page, "url", "") or ""))
        state["offset_after"] = after
        state["offset_confirmed"] = after == target
    except Exception as err:
        state["offset_error"] = f"{type(err).__name__}:{err}"[:160]
    return state


def _apply_button_if_visible(page: Any) -> bool:
    return _click_first(
        page,
        [
            "button:visible:has-text('Apply')",
            "[role='button']:visible:has-text('Apply')",
            "span:visible:has-text('Apply')",
        ],
        timeout_ms=1200,
    )


def _toggle_visible_checkbox_by_tokens(page: Any, tokens: List[str]) -> Optional[str]:
    result = page.evaluate(
        """(tokens) => {
          const normalize = (s) => (s || "").toLowerCase();
          const hasControlAttrs = (el) => {
            if (!el) return false;
            const role = normalize(el.getAttribute("role"));
            if (["menuitemcheckbox", "menuitemradio", "checkbox", "radio"].includes(role)) return true;
            const ariaChecked = normalize(el.getAttribute("aria-checked"));
            const ariaPressed = normalize(el.getAttribute("aria-pressed"));
            return ariaChecked === "true" || ariaChecked === "false" || ariaPressed === "true" || ariaPressed === "false";
          };
          const isChecked = (el) => {
            if (!el) return false;
            const ariaChecked = normalize(el.getAttribute("aria-checked"));
            const ariaPressed = normalize(el.getAttribute("aria-pressed"));
            if (ariaChecked === "true" || ariaPressed === "true") return true;
            const input = el.querySelector("input[type='checkbox'], input[type='radio']");
            return !!(input && input.checked);
          };
          const isVisible = (el) => {
            if (!el) return false;
            const style = window.getComputedStyle(el);
            if (style.display === "none" || style.visibility === "hidden") return false;
            const r = el.getBoundingClientRect();
            return r.width >= 1 && r.height >= 1;
          };
          const items = [
            ...document.querySelectorAll(
              "[role='menuitemcheckbox'], [role='menuitemradio'], [role='checkbox'], [role='radio'], label, li, button, [role='button'], span, div"
            ),
          ];
          const tokenMatched = (txt) => tokens.some((t) => normalize(txt).includes(normalize(t)));
          for (const el of items) {
            if (!isVisible(el)) continue;
            const txt = normalize(el.textContent);
            if (!tokenMatched(txt)) continue;
            const text = (el.textContent || "").trim();
            if (!text) continue;
            if (text.length > 120) continue;
            const inPopup = !!el.closest("[role='menu'], [role='listbox'], [role='dialog'], [aria-modal='true']");
            const hasControlRole = ["menuitemcheckbox", "menuitemradio", "checkbox", "radio"].includes(normalize(el.getAttribute("role")));
            const hasInput = !!el.querySelector("input[type='checkbox'], input[type='radio']");
            const controlLike = hasControlRole || hasInput || hasControlAttrs(el);
            if (!inPopup && !controlLike) {
              // Avoid false positives from static page text when popup is not open.
              continue;
            }
            if (isChecked(el)) {
              return { text, checked: true };
            }
            el.click();
            if (isChecked(el)) return { text, checked: true };
            const nearest = el.closest("label, li, div, button");
            if (nearest && nearest !== el) {
              if (isChecked(nearest)) return { text, checked: true };
              try { nearest.click(); } catch (e) {}
              if (isChecked(nearest)) return { text, checked: true };
            }
            const input = el.querySelector("input[type='checkbox'], input[type='radio']");
            if (input) {
              if (!input.checked) {
                try { input.click(); } catch (e) {}
              }
              if (input.checked) return { text, checked: true };
            }
            return { text, checked: false };
          }
          return { text: "", checked: false };
        }""",
        tokens,
    )
    if isinstance(result, dict):
        text = str(result.get("text", "") or "").strip()
        if bool(result.get("checked")) and text:
            return text
        return None
    text = str(result or "").strip()
    return text or None


def _set_lookback_days(page: Any, days: int) -> Optional[str]:
    label_map = {
        7: "Last 7 days",
        30: "Last 30 days",
        90: "Last 90 days",
        180: "Last 6 months",
        365: "Last year",
        730: "Last 2 years",
        1095: "Last 3 years",
    }
    target = label_map.get(int(days), "Last 90 days")
    if int(days) == 90:
        current_label = page.evaluate(
            """() => {
              const candidates = [
                ...document.querySelectorAll("button, [role='button'], [role='tab'], span, div"),
              ];
              for (const el of candidates) {
                const style = window.getComputedStyle(el);
                if (style.display === "none" || style.visibility === "hidden") continue;
                const txt = (el.textContent || "").trim();
                if (!txt) continue;
                if (/^last\\s+90\\s+days$/i.test(txt)) return txt;
              }
              return "";
            }"""
        )
        selected = str(current_label or "").strip()
        if selected:
            return selected
    opened = _click_first(
        page,
        [
            "button:visible:has-text('Last')",
            "[role='button']:visible:has-text('Last')",
        ],
    )
    if not opened:
        return None
    page.wait_for_timeout(max(40, _to_int(os.getenv("LIQUIDITY_RPA_LOOKBACK_MENU_SETTLE_MS", "120"), 120)))
    picked = page.evaluate(
        """(target) => {
          const items = [...document.querySelectorAll("div[role='menuitemradio']")];
          for (const el of items) {
            const style = window.getComputedStyle(el);
            if (style.display === "none" || style.visibility === "hidden") continue;
            const txt = (el.textContent || "").trim();
            if (!txt || txt.toLowerCase() !== target.toLowerCase()) continue;
            el.click();
            return txt;
          }
          return "";
        }""",
        target,
    )
    text = str(picked or "").strip()
    return text or None


def _set_lock_selected_filters(page: Any) -> str:
    status = page.evaluate(
        """() => {
          const norm = (s) => (s || "").toLowerCase().replace(/\\s+/g, " ").trim();
          const visible = (el) => {
            if (!el) return false;
            const st = window.getComputedStyle(el);
            if (st.display === "none" || st.visibility === "hidden") return false;
            const r = el.getBoundingClientRect();
            return r.width > 1 && r.height > 1;
          };
          const isOn = (el) => {
            if (!el) return false;
            const ariaChecked = norm(el.getAttribute("aria-checked"));
            const ariaPressed = norm(el.getAttribute("aria-pressed"));
            if (ariaChecked === "true" || ariaPressed === "true") return true;
            const input = (el.matches && el.matches("input[type='checkbox']")) ? el : el.querySelector?.("input[type='checkbox']");
            return !!(input && input.checked);
          };
          const lockLabels = [...document.querySelectorAll("label, div, span")].filter(
            (el) => visible(el) && norm(el.textContent).includes("lock selected filters")
          );
          const controlsNear = (label) => {
            if (!label) return [];
            const scope = label.closest("section, div, form") || label.parentElement || document.body;
            const nodes = [
              ...scope.querySelectorAll("[role='switch'], [role='checkbox'], input[type='checkbox']"),
            ].filter((el) => visible(el));
            return nodes;
          };
          for (const label of lockLabels) {
            const candidates = controlsNear(label);
            for (const ctl of candidates) {
              if (isOn(ctl)) return "already_on";
              try { ctl.click(); } catch (e) {}
              if (isOn(ctl)) return "enabled";
            }
          }
          // Fallback: aria-label/name based direct switch lookup.
          const fallback = [...document.querySelectorAll("[role='switch'], [role='checkbox'], input[type='checkbox']")].filter(
            (el) => {
              if (!visible(el)) return false;
              const label = norm(el.getAttribute("aria-label") || el.getAttribute("name") || "");
              return label.includes("lock selected filters");
            }
          );
          for (const ctl of fallback) {
            if (isOn(ctl)) return "already_on";
            try { ctl.click(); } catch (e) {}
            if (isOn(ctl)) return "enabled";
          }
          return lockLabels.length > 0 ? "failed" : "not_found";
        }"""
    )
    text = str(status or "").strip().lower()
    if text in {"enabled", "already_on", "not_found", "failed"}:
        return text
    return "not_found"


def _detect_lock_selected_filters_enabled(page: Any) -> bool:
    try:
        result = page.evaluate(
            """() => {
              const norm = (s) => (s || "").toLowerCase().replace(/\\s+/g, " ").trim();
              const visible = (el) => {
                if (!el) return false;
                const st = window.getComputedStyle(el);
                if (st.display === "none" || st.visibility === "hidden") return false;
                const r = el.getBoundingClientRect();
                return r.width > 1 && r.height > 1;
              };
              const isOn = (el) => {
                if (!el) return false;
                const ariaChecked = norm(el.getAttribute("aria-checked"));
                const ariaPressed = norm(el.getAttribute("aria-pressed"));
                if (ariaChecked === "true" || ariaPressed === "true") return true;
                const input = el.matches?.("input[type='checkbox']") ? el : el.querySelector?.("input[type='checkbox']");
                return !!(input && input.checked);
              };
              const labels = [...document.querySelectorAll("label, div, span")].filter(
                (el) => visible(el) && norm(el.textContent).includes("lock selected filters")
              );
              for (const label of labels) {
                const scope = label.closest("section, div, form") || label.parentElement || document.body;
                const nodes = [
                  ...scope.querySelectorAll("[role='switch'], [role='checkbox'], input[type='checkbox']"),
                ].filter((el) => visible(el));
                for (const node of nodes) {
                  if (isOn(node)) return true;
                }
              }
              return false;
            }"""
        )
        return bool(result)
    except Exception:
        return False


def _sold_sort_option_tokens(sold_sort: str) -> List[str]:
    mode = _normalize_sold_sort(sold_sort)
    if mode == "recently_sold":
        return ["recently sold", "most recent", "newest sold", "最新", "新しい順"]
    if mode == "price_desc":
        return ["price + shipping: highest first", "highest first", "高い順"]
    if mode == "price_asc":
        return ["price + shipping: lowest first", "lowest first", "安い順"]
    return []


def _get_date_last_sold_header_state(page: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "has_header": False,
        "is_desc": False,
        "is_asc": False,
        "header_class": "",
        "icon_class": "",
        "icon_use_href": "",
        "aria_sort": "",
    }
    try:
        result = page.evaluate(
            """() => {
              const norm = (s) => (s || "").toLowerCase().trim();
              const header =
                document.querySelector("th.research-table-header__date-last-sold") ||
                document.querySelector("th[class*='date-last-sold']") ||
                document.querySelector("[role='columnheader'][class*='date-last-sold']");
              if (!header) {
                return {
                  hasHeader: false,
                  isDesc: false,
                  isAsc: false,
                  headerClass: "",
                  iconClass: "",
                  iconUseHref: "",
                  ariaSort: "",
                };
              }
              const icon = header.querySelector(".sort-icon, .icon-group .icon, svg");
              const iconUse = icon ? icon.querySelector("use") : null;
              const headerClass = String(header.className || "");
              const iconClass = String(icon?.className?.baseVal || icon?.className || "");
              const iconUseHref = String(
                (iconUse && (iconUse.getAttribute("href") || iconUse.getAttribute("xlink:href"))) || ""
              );
              const ariaSort = String(header.getAttribute("aria-sort") || "");
              const flat = `${headerClass} ${iconClass} ${iconUseHref} ${ariaSort}`.toLowerCase();
              const isDesc =
                flat.includes("sort-icon-single down") ||
                flat.includes("icon-chevron-down") ||
                norm(ariaSort) === "descending";
              const isAsc =
                flat.includes("sort-icon-single up") ||
                flat.includes("icon-chevron-up") ||
                norm(ariaSort) === "ascending";
              return {
                hasHeader: true,
                isDesc,
                isAsc,
                headerClass,
                iconClass,
                iconUseHref,
                ariaSort,
              };
            }"""
        )
        if isinstance(result, dict):
            out["has_header"] = bool(result.get("hasHeader"))
            out["is_desc"] = bool(result.get("isDesc"))
            out["is_asc"] = bool(result.get("isAsc"))
            out["header_class"] = str(result.get("headerClass", "") or "")
            out["icon_class"] = str(result.get("iconClass", "") or "")
            out["icon_use_href"] = str(result.get("iconUseHref", "") or "")
            out["aria_sort"] = str(result.get("ariaSort", "") or "")
    except Exception:
        return out
    return out


def _parse_ui_date_to_epoch(text: str) -> int:
    raw = str(text or "").strip()
    if not raw:
        return 0
    for fmt in ("%b %d, %Y", "%b %d %Y"):
        try:
            parsed = time.strptime(raw, fmt)
            return int(time.mktime(parsed))
        except Exception:
            continue
    return 0


def _get_sold_date_order_state(page: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "row_dates": [],
        "window_start": "",
        "window_end": "",
        "first_date": "",
        "last_date": "",
        "is_desc": False,
        "is_asc": False,
        "is_newest_first": False,
    }
    try:
        result = page.evaluate(
            r"""() => {
              const visible = (el) => {
                if (!el) return false;
                const st = window.getComputedStyle(el);
                if (st.display === "none" || st.visibility === "hidden") return false;
                const r = el.getBoundingClientRect();
                return r.width > 1 && r.height > 1;
              };
              const monthRe = /(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}/g;
              const rowDates = [];
              const rows = document.querySelectorAll("tr.research-table-row, div.research-table-row");
              for (const row of rows) {
                const node = row.querySelector(".research-table-row__dateLastSold div, .research-table-row__dateLastSold, [class*='dateLastSold']");
                const txt = (node?.textContent || "").replace(/\s+/g, " ").trim();
                if (txt && monthRe.test(txt)) rowDates.push(txt.match(monthRe)[0]);
                monthRe.lastIndex = 0;
                if (rowDates.length >= 12) break;
              }
              let windowStart = "";
              let windowEnd = "";
              const candidates = [...document.querySelectorAll("div, span, p, h2, h3, h4")];
              for (const el of candidates) {
                if (!visible(el)) continue;
                const txt = (el.textContent || "").replace(/\s+/g, " ").trim();
                if (!txt || txt.length > 80) continue;
                const hits = txt.match(monthRe) || [];
                monthRe.lastIndex = 0;
                if (hits.length >= 2 && txt.includes("-")) {
                  windowStart = hits[0];
                  windowEnd = hits[1];
                  break;
                }
              }
              return { rowDates, windowStart, windowEnd };
            }"""
        )
        if isinstance(result, dict):
            row_dates = [str(v or "").strip() for v in (result.get("rowDates") or []) if str(v or "").strip()]
            out["row_dates"] = row_dates[:12]
            out["window_start"] = str(result.get("windowStart", "") or "").strip()
            out["window_end"] = str(result.get("windowEnd", "") or "").strip()
    except Exception:
        return out

    epochs = [_parse_ui_date_to_epoch(v) for v in out["row_dates"]]
    epochs = [v for v in epochs if v > 0]
    if not epochs:
        return out
    first_epoch = epochs[0]
    last_epoch = epochs[-1]
    out["first_date"] = str(out["row_dates"][0] if out["row_dates"] else "")
    out["last_date"] = str(out["row_dates"][len(epochs) - 1] if len(out["row_dates"]) >= len(epochs) else "")
    out["is_desc"] = all(epochs[i] >= epochs[i + 1] for i in range(len(epochs) - 1))
    out["is_asc"] = all(epochs[i] <= epochs[i + 1] for i in range(len(epochs) - 1))
    if not str(out.get("window_end", "") or "").strip():
        try:
            body_text = str(page.inner_text("body") or "")
        except Exception:
            body_text = ""
        if body_text:
            m = re.search(
                r"\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})\b\s*-\s*\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})\b",
                body_text,
            )
            if m:
                out["window_start"] = str(m.group(1) or "").strip()
                out["window_end"] = str(m.group(2) or "").strip()
    window_end_epoch = _parse_ui_date_to_epoch(str(out.get("window_end", "") or ""))
    if window_end_epoch > 0:
        # 90日窓の最新日付に近ければ newest-first とみなす。
        out["is_newest_first"] = bool(first_epoch >= (window_end_epoch - (2 * 86400)))
    else:
        out["is_newest_first"] = bool(out["is_desc"] and (not out["is_asc"]) and (first_epoch >= last_epoch))
    return out


def _get_sold_price_order_state(page: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "row_prices": [],
        "first_price": -1.0,
        "last_price": -1.0,
        "is_asc": False,
        "is_desc": False,
    }
    try:
        result = page.evaluate(
            r"""() => {
              const visible = (el) => {
                if (!el) return false;
                const st = window.getComputedStyle(el);
                if (st.display === "none" || st.visibility === "hidden") return false;
                const r = el.getBoundingClientRect();
                return r.width > 1 && r.height > 1;
              };
              const parsePrice = (txt) => {
                const raw = String(txt || "").replace(/\s+/g, " ").trim();
                if (!raw) return -1;
                const m = raw.match(/([0-9][0-9,]*(?:\.[0-9]{1,2})?)/);
                if (!m) return -1;
                const n = Number(String(m[1] || "").replace(/,/g, ""));
                return Number.isFinite(n) ? n : -1;
              };
              const out = [];
              const rows = document.querySelectorAll("tr.research-table-row, div.research-table-row");
              for (const row of rows) {
                if (!visible(row)) continue;
                const node =
                  row.querySelector(".research-table-row__avgSoldPrice div") ||
                  row.querySelector(".research-table-row__avgSoldPrice") ||
                  row.querySelector(".research-table-row__listingPrice div") ||
                  row.querySelector(".research-table-row__listingPrice") ||
                  row.querySelector("[class*='avgSoldPrice']") ||
                  row.querySelector("[class*='listingPrice']");
                const value = parsePrice(node?.textContent || "");
                if (value > 0) out.push(value);
                if (out.length >= 20) break;
              }
              return { rowPrices: out };
            }"""
        )
        if isinstance(result, dict):
            values = []
            for raw in (result.get("rowPrices") or []):
                value = _to_float(raw, -1.0)
                if value > 0:
                    values.append(round(value, 4))
            out["row_prices"] = values[:20]
    except Exception:
        return out

    prices = [float(v) for v in out["row_prices"] if _to_float(v, -1.0) > 0]
    if len(prices) >= 2:
        out["first_price"] = float(prices[0])
        out["last_price"] = float(prices[-1])
        eps = 0.001
        out["is_asc"] = all(prices[i] <= (prices[i + 1] + eps) for i in range(len(prices) - 1))
        out["is_desc"] = all(prices[i] >= (prices[i + 1] - eps) for i in range(len(prices) - 1))
    return out


def _click_date_last_sold_header(page: Any) -> bool:
    clicked = _click_first(
        page,
        [
            "th.research-table-header__date-last-sold:visible",
            "th.research-table-header__date-last-sold .research-table-header__inner-item:visible",
            "th.research-table-header__date-last-sold .sort-group:visible",
            "th[class*='date-last-sold']:visible",
            "[role='columnheader'][class*='date-last-sold']:visible",
            "th:visible:has-text('Date last sold')",
            "[role='columnheader']:visible:has-text('Date last sold')",
            "button:visible:has-text('Date last sold')",
            "[role='button']:visible:has-text('Date last sold')",
            "div:visible:has-text('Date last sold')",
            "span:visible:has-text('Date last sold')",
            "button:visible:has-text('売れた日')",
            "[role='button']:visible:has-text('売れた日')",
        ],
    )
    if not clicked:
        clicked = _click_button_by_text_tokens(page, ["date last sold", "last sold", "売れた日"])
    return bool(clicked)


def _click_avg_sold_price_header(page: Any) -> bool:
    clicked = _click_first(
        page,
        [
            "th.research-table-header__avg-sold-price:visible",
            "th.research-table-header__avg-sold-price .research-table-header__inner-item:visible",
            "th[class*='avg-sold-price']:visible",
            "[role='columnheader'][class*='avg-sold-price']:visible",
            "th:visible:has-text('Avg sold price')",
            "[role='columnheader']:visible:has-text('Avg sold price')",
            "button:visible:has-text('Avg sold price')",
            "[role='button']:visible:has-text('Avg sold price')",
            "th:visible:has-text('平均売却価格')",
            "[role='columnheader']:visible:has-text('平均売却価格')",
        ],
    )
    if not clicked:
        clicked = _click_button_by_text_tokens(page, ["avg sold price", "sold price", "平均売却価格"])
    return bool(clicked)


def _detect_sold_sort_selected(page: Any, sold_sort: str) -> Tuple[bool, str]:
    mode = _normalize_sold_sort(sold_sort)
    if mode == "default":
        return True, "default"
    if mode in {"price_asc", "price_desc"}:
        price_state = _get_sold_price_order_state(page)
        is_target = bool(price_state.get("is_asc")) if mode == "price_asc" else bool(price_state.get("is_desc"))
        if is_target:
            first_price = _to_float(price_state.get("first_price"), -1.0)
            label = f"avg_sold_price_{mode}"
            if first_price > 0:
                label = f"{label}:{first_price:.2f}"
            return True, label
        try:
            url_state = _detect_sold_filters_from_url(page)
            if str(url_state.get("sold_sort", "default") or "default") == mode:
                raw = str(url_state.get("sold_sort_raw", "") or "").strip()
                return True, raw or "url_sort"
        except Exception:
            pass
        return False, ""
    if mode == "recently_sold":
        order_state = _get_sold_date_order_state(page)
        if bool(order_state.get("is_newest_first")):
            return True, str(order_state.get("first_date", "") or "date_last_sold_desc")
    try:
        url_state = _detect_sold_filters_from_url(page)
        if str(url_state.get("sold_sort", "default") or "default") == mode:
            raw = str(url_state.get("sold_sort_raw", "") or "").strip()
            return True, raw or "url_sort"
    except Exception:
        pass
    tokens = _sold_sort_option_tokens(mode)
    if not tokens:
        return False, ""
    try:
        label = page.evaluate(
            """(tokens) => {
              const norm = (s) => (s || "").toLowerCase().replace(/\\s+/g, " ").trim();
              const visible = (el) => {
                if (!el) return false;
                const st = window.getComputedStyle(el);
                if (st.display === "none" || st.visibility === "hidden") return false;
                const r = el.getBoundingClientRect();
                return r.width > 1 && r.height > 1;
              };
              const needles = (tokens || []).map((v) => norm(v)).filter(Boolean);
              const nodes = [
                ...document.querySelectorAll("button, [role='button'], [role='tab'], span, div, li, label"),
              ];
              for (const el of nodes) {
                if (!visible(el)) continue;
                const txt = norm(el.textContent || "");
                if (!txt || txt.length > 96) continue;
                if (!needles.some((t) => txt.includes(t))) continue;
                return (el.textContent || "").replace(/\\s+/g, " ").trim();
              }
              return "";
            }""",
            tokens,
        )
        text = str(label or "").strip()
        if text:
            return True, text
    except Exception:
        return False, ""
    return False, ""


def _set_sold_sort(page: Any, sold_sort: str) -> Dict[str, Any]:
    mode = _normalize_sold_sort(sold_sort)
    state: Dict[str, Any] = {
        "sort_target": mode,
        "sort_filter_panel_opened": False,
        "sort_selected": mode == "default",
        "sort_option_label": "",
        "sort_selection_source": "default" if mode == "default" else "",
        "sort_order_state": {},
        "sort_visible_options": [],
    }
    if mode == "default":
        return state
    if mode in {"price_asc", "price_desc"}:
        before_price_state = _get_sold_price_order_state(page)
        state["sort_order_state"] = before_price_state
        if (mode == "price_asc" and bool(before_price_state.get("is_asc"))) or (
            mode == "price_desc" and bool(before_price_state.get("is_desc"))
        ):
            state["sort_selected"] = True
            state["sort_option_label"] = f"avg_sold_price_{mode}_before"
            state["sort_selection_source"] = "avg_sold_price_detect_before"
            return state
        for attempt in range(4):
            clicked = _click_avg_sold_price_header(page)
            if not clicked:
                continue
            state["sort_filter_panel_opened"] = True
            page.wait_for_timeout(max(40, _to_int(os.getenv("LIQUIDITY_RPA_FILTER_SETTLE_MS", "45"), 45)))
            _wait_for_research_ready(page, 2)
            after_price_state = _get_sold_price_order_state(page)
            state["sort_order_state"] = after_price_state
            if (mode == "price_asc" and bool(after_price_state.get("is_asc"))) or (
                mode == "price_desc" and bool(after_price_state.get("is_desc"))
            ):
                state["sort_selected"] = True
                state["sort_option_label"] = f"avg_sold_price_{mode}"
                state["sort_selection_source"] = f"avg_sold_price_click_{attempt + 1}"
                return state
        return state
    if mode == "recently_sold":
        before_header_state = _get_date_last_sold_header_state(page)
        if bool(before_header_state.get("has_header")) and bool(before_header_state.get("is_desc")):
            state["sort_selected"] = True
            state["sort_option_label"] = "date_last_sold_header_desc"
            state["sort_selection_source"] = "date_last_sold_header_before"
            state["sort_order_state"] = _get_sold_date_order_state(page)
            return state
        before_state = _get_sold_date_order_state(page)
        state["sort_order_state"] = before_state
        if bool(before_state.get("is_newest_first")):
            state["sort_selected"] = True
            state["sort_option_label"] = str(before_state.get("first_date", "") or "").strip()[:120]
            state["sort_selection_source"] = "date_order_detect_before"
            return state
        for attempt in range(4):
            clicked = _click_date_last_sold_header(page)
            if not clicked:
                continue
            state["sort_filter_panel_opened"] = True
            page.wait_for_timeout(max(40, _to_int(os.getenv("LIQUIDITY_RPA_FILTER_SETTLE_MS", "45"), 45)))
            _wait_for_research_ready(page, 2)
            after_header_state = _get_date_last_sold_header_state(page)
            if bool(after_header_state.get("has_header")) and bool(after_header_state.get("is_desc")):
                state["sort_selected"] = True
                state["sort_option_label"] = "date_last_sold_header_desc"
                state["sort_selection_source"] = f"date_last_sold_click_{attempt + 1}"
                state["sort_order_state"] = _get_sold_date_order_state(page)
                return state
            after_state = _get_sold_date_order_state(page)
            state["sort_order_state"] = after_state
            if bool(after_state.get("is_newest_first")):
                state["sort_selected"] = True
                state["sort_option_label"] = str(after_state.get("first_date", "") or "").strip()[:120]
                state["sort_selection_source"] = f"date_last_sold_click_{attempt + 1}"
                return state
        return state

    selected_before, selected_label = _detect_sold_sort_selected(page, mode)
    if selected_before:
        state["sort_selected"] = True
        state["sort_option_label"] = str(selected_label or "").strip()[:120]
        state["sort_selection_source"] = "url_prefill"
        return state
    opened = _click_first(
        page,
        [
            "button:visible:has-text('Top rated')",
            "[role='button']:visible:has-text('Top rated')",
            "button:visible:has-text('Sort')",
            "[role='button']:visible:has-text('Sort')",
            "button:visible:has-text('並び')",
            "[role='button']:visible:has-text('並び')",
        ],
    )
    if not opened:
        opened = _click_button_by_text_tokens(page, ["top rated", "sort by", "sort", "並び"])
    if not opened:
        return state
    state["sort_filter_panel_opened"] = True
    page.wait_for_timeout(max(40, _to_int(os.getenv("LIQUIDITY_RPA_FILTER_SETTLE_MS", "45"), 45)))
    tokens = _sold_sort_option_tokens(mode)
    selected = _toggle_visible_checkbox_by_tokens(page, tokens)
    if selected:
        state["sort_selected"] = True
        state["sort_option_label"] = str(selected or "").strip()[:120]
        state["sort_selection_source"] = "ui"
    else:
        forced = _click_visible_popup_option_by_tokens(page, tokens)
        if forced:
            state["sort_selected"] = True
            state["sort_option_label"] = str(forced or "").strip()[:120]
            state["sort_selection_source"] = "ui_click"
        else:
            state["sort_visible_options"] = _collect_open_popup_option_texts(page, limit=24)
    _apply_button_if_visible(page)
    _wait_for_research_ready(page, 2)
    selected_after, selected_label_after = _detect_sold_sort_selected(page, mode)
    if selected_after:
        state["sort_selected"] = True
        if not str(state.get("sort_option_label", "") or "").strip():
            state["sort_option_label"] = str(selected_label_after or "").strip()[:120]
        if not str(state.get("sort_selection_source", "") or "").strip():
            state["sort_selection_source"] = "detect"
    return state


def _detect_min_price_filter_selected(page: Any, min_price_usd: float) -> bool:
    target = max(0.0, _to_float(min_price_usd, 0.0))
    if target <= 0:
        return False
    try:
        url_state = _detect_sold_filters_from_url(page)
        url_min_price = max(0.0, _to_float(url_state.get("min_price"), 0.0))
        if url_min_price >= (target - 0.01):
            return True
    except Exception:
        pass
    try:
        raw_texts = page.evaluate(
            """() => {
              const visible = (el) => {
                if (!el) return false;
                const st = window.getComputedStyle(el);
                if (st.display === "none" || st.visibility === "hidden") return false;
                const r = el.getBoundingClientRect();
                return r.width > 1 && r.height > 1;
              };
              const out = [];
              const nodes = [...document.querySelectorAll("button, [role='button'], span, div, li, label")];
              for (const el of nodes) {
                if (!visible(el)) continue;
                const txt = (el.textContent || "").replace(/\\s+/g, " ").trim().toLowerCase();
                if (!txt || txt.length > 120) continue;
                out.push(txt);
                if (out.length >= 1200) break;
              }
              return out;
            }"""
        )
        texts = raw_texts if isinstance(raw_texts, list) else []
        amount_re = re.compile(r"([0-9][0-9,]*(?:\.[0-9]{1,2})?)")
        min_chip_re = re.compile(r"\$?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*-\s*\*")
        for raw in texts:
            txt = str(raw or "").strip().lower()
            if not txt:
                continue
            # 価格チップ "$100 - *" を最優先で判定する（summaryの sold range との誤判定を避ける）。
            chip_match = min_chip_re.search(txt)
            if chip_match:
                chip_min = _to_float(str(chip_match.group(1) or "").replace(",", ""), 0.0)
                if chip_min >= (target - 0.01):
                    return True
            if not (("price" in txt) or ("$" in txt) or ("usd" in txt) or ("価格" in txt)):
                continue
            if not any(token in txt for token in ("and up", "+", "以上", "min", "minimum", "from")):
                continue
            for match in amount_re.findall(txt):
                value = _to_float(str(match).replace(",", ""), 0.0)
                if value >= (target - 0.01):
                    return True
        return False
    except Exception:
        return False


def _set_min_price_filter(page: Any, min_price_usd: float) -> Dict[str, Any]:
    target = max(0.0, _to_float(min_price_usd, 0.0))
    state: Dict[str, Any] = {
        "price_filter_panel_opened": False,
        "min_price_input_applied": False,
        "min_price_option_label": "",
        "min_price_selected": False,
    }
    if target <= 0:
        return state
    opened = _click_first(
        page,
        [
            "button:visible:has-text('Price filter')",
            "[role='button']:visible:has-text('Price filter')",
            "button:visible:has-text('Price')",
            "[role='button']:visible:has-text('Price')",
            "button:visible:has-text('価格')",
            "[role='button']:visible:has-text('価格')",
        ],
    )
    if not opened:
        opened = _click_button_by_text_tokens(page, ["price filter", "price", "価格"])
    if not opened:
        return state
    state["price_filter_panel_opened"] = True
    page.wait_for_timeout(max(40, _to_int(os.getenv("LIQUIDITY_RPA_FILTER_SETTLE_MS", "45"), 45)))
    input_value = str(int(round(target)))
    input_result = page.evaluate(
        """(value) => {
          const visible = (el) => {
            if (!el) return false;
            const st = window.getComputedStyle(el);
            if (st.display === "none" || st.visibility === "hidden") return false;
            const r = el.getBoundingClientRect();
            return r.width > 1 && r.height > 1;
          };
          const scoreInput = (el) => {
            if (!el) return -1;
            const attrs = [
              el.getAttribute("placeholder"),
              el.getAttribute("aria-label"),
              el.getAttribute("name"),
              el.id,
            ]
              .map((v) => (v || "").toLowerCase())
              .join(" ");
            let score = 1;
            if (attrs.includes("min") || attrs.includes("minimum") || attrs.includes("from")) score += 4;
            if (attrs.includes("price") || attrs.includes("amount") || attrs.includes("usd")) score += 2;
            if (attrs.includes("max") || attrs.includes("to")) score -= 2;
            return score;
          };
          const roots = [
            ...document.querySelectorAll("[role='dialog'], [role='menu'], [role='listbox'], [aria-modal='true']"),
          ].filter((el) => visible(el));
          const candidates = [];
          const scanRoots = roots.length > 0 ? roots : [document.body];
          for (const root of scanRoots) {
            const inputs = [
              ...root.querySelectorAll("input[type='number'], input[type='text'], input:not([type])"),
            ].filter((el) => visible(el));
            for (const input of inputs) {
              candidates.push(input);
            }
          }
          candidates.sort((a, b) => scoreInput(b) - scoreInput(a));
          const input = candidates[0];
          if (!input) return { applied: false, label: "" };
          const setValue = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value")?.set;
          if (setValue) setValue.call(input, value);
          else input.value = value;
          input.dispatchEvent(new Event("input", { bubbles: true }));
          input.dispatchEvent(new Event("change", { bubbles: true }));
          try { input.blur(); } catch (e) {}
          const label = [
            input.getAttribute("aria-label"),
            input.getAttribute("placeholder"),
            input.getAttribute("name"),
            input.id,
          ]
            .filter(Boolean)
            .join(" ")
            .trim();
          return { applied: true, label };
        }""",
        input_value,
    )
    if isinstance(input_result, dict) and bool(input_result.get("applied")):
        state["min_price_input_applied"] = True
        state["min_price_option_label"] = str(input_result.get("label", "") or "").strip()[:120]
    _apply_button_if_visible(page)
    _wait_for_research_ready(page, 2)
    state["min_price_selected"] = bool(_detect_min_price_filter_selected(page, target))
    return state


def _collect_open_popup_option_texts(page: Any, limit: int = 24) -> List[str]:
    try:
        result = page.evaluate(
            """(limit) => {
              const visible = (el) => {
                if (!el) return false;
                const st = window.getComputedStyle(el);
                if (st.display === "none" || st.visibility === "hidden") return false;
                const r = el.getBoundingClientRect();
                return r.width > 1 && r.height > 1;
              };
              const roots = [...document.querySelectorAll("[role='menu'], [role='dialog'], [role='listbox'], [aria-modal='true']")].filter(visible);
              const out = [];
              for (const root of roots) {
                const nodes = [...root.querySelectorAll("div, span, li, label, button, [role='menuitemcheckbox'], [role='menuitemradio'], [role='checkbox'], [role='radio']")];
                for (const el of nodes) {
                  if (!visible(el)) continue;
                  const txt = (el.textContent || "").replace(/\\s+/g, " ").trim();
                  if (!txt || txt.length > 120) continue;
                  if (out.includes(txt)) continue;
                  out.push(txt);
                  if (out.length >= Number(limit || 24)) return out;
                }
              }
              return out;
            }""",
            int(max(1, limit)),
        )
        if isinstance(result, list):
            return [str(v or "").strip() for v in result if str(v or "").strip()][: max(1, int(limit))]
    except Exception:
        return []
    return []


def _click_visible_popup_option_by_tokens(page: Any, tokens: List[str]) -> Optional[str]:
    try:
        result = page.evaluate(
            """(tokens) => {
              const norm = (s) => (s || "").toLowerCase().replace(/\\s+/g, " ").trim();
              const visible = (el) => {
                if (!el) return false;
                const st = window.getComputedStyle(el);
                if (st.display === "none" || st.visibility === "hidden") return false;
                const r = el.getBoundingClientRect();
                return r.width > 1 && r.height > 1;
              };
              const roots = [...document.querySelectorAll("[role='menu'], [role='dialog'], [role='listbox'], [aria-modal='true']")].filter(visible);
              const needles = (tokens || []).map((v) => norm(v)).filter(Boolean);
              for (const root of roots) {
                const nodes = [
                  ...root.querySelectorAll(
                    "[role='menuitemcheckbox'], [role='menuitemradio'], [role='checkbox'], [role='radio'], div, span, li, label, button"
                  ),
                ];
                for (const el of nodes) {
                  if (!visible(el)) continue;
                  const txt = norm(el.textContent || "");
                  if (!txt || txt.length > 120) continue;
                  if (/^\\([0-9,]+\\)$/.test(txt)) continue;
                  if (!needles.some((t) => txt.includes(t))) continue;
                  try { el.click(); } catch (e) {}
                  return (el.textContent || "").trim();
                }
              }
              return "";
            }""",
            tokens,
        )
        text = str(result or "").strip()
        return text or None
    except Exception:
        return None


def _apply_ui_filters(
    page: Any,
    *,
    lookback_days: int,
    condition: str,
    strict_condition: bool,
    fixed_price_only: bool,
    min_price_usd: float = 0.0,
    sold_sort: str = "default",
) -> Dict[str, Any]:
    filter_settle_ms = max(25, _to_int(os.getenv("LIQUIDITY_RPA_FILTER_SETTLE_MS", "45"), 45))
    sort_target = _normalize_sold_sort(sold_sort)
    state: Dict[str, Any] = {
        "sold_tab_selected": False,
        "lookback_selected": "",
        "lookback_default_kept": False,
        "condition_target": condition,
        "condition_selected": [],
        "condition_missing": False,
        "format_fixed_price_before": False,
        "format_fixed_price_selected": False,
        "format_filter_panel_opened": False,
        "format_option_label": "",
        "format_selection_source": "",
        "format_visible_options": [],
        "sort_target": sort_target,
        "sort_filter_panel_opened": False,
        "sort_selected": sort_target == "default",
        "sort_option_label": "",
        "sort_selection_source": "default" if sort_target == "default" else "",
        "sort_order_state": {},
        "sort_visible_options": [],
        "price_metric_target": "listing_price",
        "price_metric_selected": False,
        "price_metric_selection_source": "",
        "price_metric_available": False,
        "price_metric_ui_enabled": bool(_to_int(os.getenv("LIQUIDITY_RPA_ENABLE_LISTING_PRICE_METRIC_UI", "1"), 1)),
        "min_price_target_usd": round(max(0.0, _to_float(min_price_usd, 0.0)), 2),
        "min_price_ui_enabled": bool(_to_int(os.getenv("LIQUIDITY_RPA_ENABLE_MIN_PRICE_FILTER_UI", "1"), 1)),
        "min_price_input_applied": False,
        "min_price_selected": False,
        "price_filter_panel_opened": False,
        "min_price_option_label": "",
        "min_price_selection_source": "",
        "lock_selected_filters": "",
        "strict_blocked": False,
        "strict_reason": "",
    }

    state["sold_tab_selected"] = _detect_sold_tab_selected(page)
    if (not state["sold_tab_selected"]) and _click_first(
        page, ["[role='tab']:visible:has-text('Sold')", "div[role='tab']:visible:has-text('Sold')"]
    ):
        state["sold_tab_selected"] = True
        page.wait_for_timeout(filter_settle_ms)
    if not state["sold_tab_selected"]:
        state["sold_tab_selected"] = _detect_sold_tab_selected(page)
    if not state["sold_tab_selected"]:
        url_state = _detect_sold_filters_from_url(page)
        if url_state.get("tab_sold"):
            state["sold_tab_selected"] = True

    picked_lookback = _set_lookback_days(page, lookback_days)
    if picked_lookback:
        state["lookback_selected"] = picked_lookback
        state["lookback_default_kept"] = str(picked_lookback).strip().lower() == "last 90 days"
        page.wait_for_timeout(filter_settle_ms)

    if bool(_to_int(os.getenv("LIQUIDITY_RPA_ENABLE_LOCK_SELECTED_FILTERS", "1"), 1)):
        state["lock_selected_filters"] = _set_lock_selected_filters(page)
        if state["lock_selected_filters"] in {"enabled", "already_on"}:
            page.wait_for_timeout(filter_settle_ms)
        elif bool(_to_int(os.getenv("LIQUIDITY_RPA_REQUIRE_LOCK_SELECTED_FILTERS", "0"), 0)):
            state["strict_blocked"] = True
            if not str(state.get("strict_reason", "") or "").strip():
                state["strict_reason"] = "lock_selected_filters_not_confirmed"

    if sort_target != "default":
        url_state = _detect_sold_filters_from_url(page)
        if sort_target != "recently_sold" and str(url_state.get("sold_sort", "default") or "default") == sort_target:
            state["sort_selected"] = True
            state["sort_selection_source"] = "url_prefill"
            state["sort_option_label"] = str(url_state.get("sold_sort_raw", "") or "").strip()[:120]
        else:
            sort_state = _set_sold_sort(page, sort_target)
            for key in (
                "sort_target",
                "sort_filter_panel_opened",
                "sort_selected",
                "sort_option_label",
                "sort_selection_source",
                "sort_order_state",
                "sort_visible_options",
            ):
                if key in sort_state:
                    state[key] = sort_state.get(key)

    if sort_target in {"price_asc", "price_desc"}:
        state["price_metric_available"] = bool(_is_listing_price_metric_available(page))
    if (
        bool(state.get("price_metric_ui_enabled"))
        and sort_target in {"price_asc", "price_desc"}
        and bool(state.get("price_metric_available"))
    ):
        metric_state = _set_listing_price_metric(page)
        for key in ("price_metric_target", "price_metric_selected", "price_metric_selection_source", "price_metric_available"):
            if key in metric_state:
                state[key] = metric_state.get(key)
        if bool(state.get("price_metric_selected")):
            page.wait_for_timeout(filter_settle_ms)
    elif sort_target in {"price_asc", "price_desc"} and not bool(state.get("price_metric_available")):
        state["price_metric_selection_source"] = "not_available"

    if fixed_price_only:
        url_state = _detect_sold_filters_from_url(page)
        state["format_fixed_price_before"] = bool(_detect_fixed_price_selected(page) or url_state.get("fixed_price"))
        if state["format_fixed_price_before"]:
            state["format_selection_source"] = "before"

    condition_mode = str(condition or "").strip().lower()
    if condition_mode not in {"", "any", "all"}:
        url_state = _detect_sold_filters_from_url(page)
        if condition_mode == "new" and bool(url_state.get("condition_new")):
            state["condition_selected"].append("New(url_prefill)")
        else:
            opened = _click_first(
                page,
                [
                    "button:visible:has-text('Condition filter')",
                    "[role='button']:visible:has-text('Condition filter')",
                ],
            )
            if not opened:
                opened = _click_button_by_text_tokens(page, ["condition filter", "condition", "コンディション", "状態"])
            if not opened:
                state["condition_missing"] = True
                if strict_condition:
                    state["strict_blocked"] = True
                    state["strict_reason"] = "condition_filter_unavailable"
            if opened:
                page.wait_for_timeout(filter_settle_ms)
                token_map = {
                    "new": ["new", "brand new", "new with", "新品"],
                    "used": ["used", "中古"],
                    "refurbished": ["refurbished"],
                }
                tokens = token_map.get(condition_mode, [condition_mode])
                selected = _toggle_visible_checkbox_by_tokens(page, tokens)
                if selected:
                    state["condition_selected"].append(selected)
                else:
                    url_state = _detect_sold_filters_from_url(page)
                    if condition_mode == "new" and bool(url_state.get("condition_new")):
                        state["condition_selected"].append("New(url)")
                    else:
                        state["condition_missing"] = True
                        if strict_condition:
                            state["strict_blocked"] = True
                            state["strict_reason"] = "condition_filter_no_match"
                _apply_button_if_visible(page)
                _wait_for_research_ready(page, 2)

    if fixed_price_only and not bool(state.get("format_fixed_price_before")):
        format_opened = _click_first(
            page,
            [
                "button:visible:has-text('Format filter')",
                "[role='button']:visible:has-text('Format filter')",
                "button:visible:has-text('Format')",
                "[role='button']:visible:has-text('Format')",
                "button:visible:has-text('Selling format')",
                "[role='button']:visible:has-text('Selling format')",
                "button:visible:has-text('販売形式')",
                "[role='button']:visible:has-text('販売形式')",
                "button:visible:has-text('フォーマット')",
                "[role='button']:visible:has-text('フォーマット')",
            ],
        )
        if not format_opened:
            format_opened = _click_button_by_text_tokens(page, ["format filter", "selling format", "format", "販売形式"])
        if format_opened:
            state["format_filter_panel_opened"] = True
            page.wait_for_timeout(filter_settle_ms)
            picked = _toggle_visible_checkbox_by_tokens(page, ["fixed price", "buy it now", "固定価格", "即決"])
            if picked:
                state["format_fixed_price_selected"] = True
                state["format_option_label"] = str(picked or "").strip()[:120]
                state["format_selection_source"] = "ui"
            elif _detect_fixed_price_selected(page):
                state["format_fixed_price_selected"] = True
                state["format_selection_source"] = "detect_after_ui"
            else:
                forced = _click_visible_popup_option_by_tokens(page, ["fixed price", "buy it now", "固定価格", "即決"])
                if forced:
                    state["format_option_label"] = str(forced or "").strip()[:120]
                    state["format_selection_source"] = "ui_click"
                state["format_visible_options"] = _collect_open_popup_option_texts(page, limit=24)
            _apply_button_if_visible(page)
            _wait_for_research_ready(page, 2)
    if fixed_price_only and not state["format_fixed_price_selected"]:
        state["format_fixed_price_selected"] = bool(_detect_fixed_price_selected(page))
        if state["format_fixed_price_selected"] and (not str(state.get("format_selection_source", "") or "").strip()):
            state["format_selection_source"] = "detect"
    if fixed_price_only and not state["format_fixed_price_selected"]:
        url_state = _detect_sold_filters_from_url(page)
        if url_state.get("fixed_price"):
            state["format_fixed_price_selected"] = True
            if not str(state.get("format_selection_source", "") or "").strip():
                state["format_selection_source"] = "url"
    if fixed_price_only and not state["format_fixed_price_selected"]:
        state["strict_blocked"] = True
        if not str(state.get("strict_reason", "") or "").strip():
            state["strict_reason"] = "fixed_price_filter_not_confirmed"

    target_min_price = max(0.0, _to_float(min_price_usd, 0.0))
    enable_min_price_ui = bool(_to_int(os.getenv("LIQUIDITY_RPA_ENABLE_MIN_PRICE_FILTER_UI", "1"), 1))
    if target_min_price > 0 and enable_min_price_ui:
        if bool(_detect_min_price_filter_selected(page, target_min_price)):
            state["min_price_selected"] = True
            state["min_price_selection_source"] = "url_prefill"
        else:
            min_state = _set_min_price_filter(page, target_min_price)
            for key in (
                "price_filter_panel_opened",
                "min_price_input_applied",
                "min_price_option_label",
                "min_price_selected",
            ):
                if key in min_state:
                    state[key] = min_state.get(key)
            if bool(state.get("min_price_selected")):
                state["min_price_selection_source"] = "ui"
            elif bool(_detect_min_price_filter_selected(page, target_min_price)):
                state["min_price_selected"] = True
                state["min_price_selection_source"] = "detect"
        if (not bool(state.get("min_price_selected"))) and bool(
            _to_int(os.getenv("LIQUIDITY_RPA_REQUIRE_MIN_PRICE_FILTER", "0"), 0)
        ):
            state["strict_blocked"] = True
            if not str(state.get("strict_reason", "") or "").strip():
                state["strict_reason"] = "min_price_filter_not_confirmed"

    return state


def _finalize_filter_state_two_stage(
    page: Any,
    filter_state: Dict[str, Any],
    *,
    condition: str,
    strict_condition: bool,
    fixed_price_only: bool,
    min_price_usd: float,
    sold_sort: str,
) -> Dict[str, Any]:
    state = dict(filter_state or {})
    url_state = _detect_sold_filters_from_url(page)
    condition_mode = str(condition or "").strip().lower()
    sort_target = _normalize_sold_sort(sold_sort)
    target_min_price = max(0.0, _to_float(min_price_usd, 0.0))
    require_min_price = bool(_to_int(os.getenv("LIQUIDITY_RPA_REQUIRE_MIN_PRICE_FILTER", "0"), 0))
    require_sort = bool(_to_int(os.getenv("LIQUIDITY_RPA_REQUIRE_SOLD_SORT", "0"), 0))
    require_listing_price_metric = bool(_to_int(os.getenv("LIQUIDITY_RPA_REQUIRE_LISTING_PRICE_METRIC", "0"), 0))
    require_lock = bool(_to_int(os.getenv("LIQUIDITY_RPA_REQUIRE_LOCK_SELECTED_FILTERS", "0"), 0))

    confirm: Dict[str, Any] = {}

    sold_tab_ui = bool(_detect_sold_tab_selected(page))
    sold_tab_url = bool(url_state.get("tab_sold"))
    confirm["sold_tab"] = {
        "ui": sold_tab_ui,
        "url": sold_tab_url,
        "confirmed": bool(sold_tab_ui or sold_tab_url),
    }

    fixed_price_ui = bool(_detect_fixed_price_selected(page)) if fixed_price_only else False
    fixed_price_url = bool(url_state.get("fixed_price")) if fixed_price_only else False
    confirm["format_fixed_price"] = {
        "ui": fixed_price_ui,
        "url": fixed_price_url,
        "confirmed": bool((not fixed_price_only) or fixed_price_ui or fixed_price_url),
    }

    min_price_ui = bool(_detect_min_price_filter_selected(page, target_min_price)) if target_min_price > 0 else True
    min_price_url = (
        bool(max(0.0, _to_float(url_state.get("min_price"), 0.0)) >= (target_min_price - 0.01))
        if target_min_price > 0
        else True
    )
    confirm["min_price"] = {
        "target": round(target_min_price, 2),
        "ui": min_price_ui,
        "url": min_price_url,
        "confirmed": bool((target_min_price <= 0) or min_price_ui or min_price_url),
    }

    condition_url = bool(url_state.get("condition_new")) if condition_mode == "new" else True
    condition_selected = [str(v or "") for v in (state.get("condition_selected") or []) if str(v or "").strip()]
    condition_ui = bool(
        condition_mode != "new"
        or any("url" not in item.lower() for item in condition_selected)
    )
    confirm["condition"] = {
        "target": condition_mode,
        "ui": condition_ui,
        "url": condition_url,
        "confirmed": bool(condition_mode != "new" or condition_ui or condition_url),
    }

    if sort_target == "default":
        sort_ui = True
    elif sort_target == "recently_sold":
        header_state = _get_date_last_sold_header_state(page)
        order_state = _get_sold_date_order_state(page)
        sort_ui = bool(header_state.get("is_desc") or order_state.get("is_newest_first") or order_state.get("is_desc"))
    else:
        sort_ui = bool(_detect_sold_sort_selected(page, sort_target)[0])
    sort_url = bool(str(url_state.get("sold_sort", "default") or "default") == sort_target)
    confirm["sold_sort"] = {
        "target": sort_target,
        "ui": sort_ui,
        "url": sort_url,
        "confirmed": bool(sort_target == "default" or sort_ui or sort_url),
    }

    listing_price_required_mode = sort_target in {"price_asc", "price_desc"}
    listing_price_available = bool(_is_listing_price_metric_available(page)) if listing_price_required_mode else False
    listing_price_ui = (
        bool(_detect_listing_price_metric_selected(page))
        if (listing_price_required_mode and listing_price_available)
        else False
    )
    confirm["listing_price_metric"] = {
        "target": "listing_price",
        "available": listing_price_available,
        "ui": listing_price_ui,
        "confirmed": bool((not listing_price_required_mode) or (not listing_price_available) or listing_price_ui),
    }

    lock_ui = bool(_detect_lock_selected_filters_enabled(page))
    confirm["lock_selected_filters"] = {
        "ui": lock_ui,
        "confirmed": bool(lock_ui),
    }

    state["confirmations"] = confirm
    state["sold_tab_selected"] = bool(confirm["sold_tab"]["confirmed"])
    if fixed_price_only:
        state["format_fixed_price_selected"] = bool(confirm["format_fixed_price"]["confirmed"])
    if target_min_price > 0:
        state["min_price_selected"] = bool(confirm["min_price"]["confirmed"])
    state["price_metric_available"] = bool(confirm["listing_price_metric"].get("available"))
    state["price_metric_selected"] = bool(confirm["listing_price_metric"]["ui"])
    if condition_mode == "new":
        if not condition_selected and bool(confirm["condition"]["url"]):
            state["condition_selected"] = ["New(url_prefill)"]
        state["condition_missing"] = not bool(confirm["condition"]["confirmed"])
    if bool(confirm["lock_selected_filters"]["confirmed"]):
        state["lock_selected_filters"] = "already_on"

    failure_reason = ""
    if strict_condition and condition_mode == "new" and not bool(confirm["condition"]["confirmed"]):
        failure_reason = "condition_filter_not_confirmed"
    elif fixed_price_only and not bool(confirm["format_fixed_price"]["confirmed"]):
        failure_reason = "fixed_price_filter_not_confirmed"
    elif target_min_price > 0 and require_min_price and not bool(confirm["min_price"]["confirmed"]):
        failure_reason = "min_price_filter_not_confirmed"
    elif sort_target != "default" and require_sort and not bool(confirm["sold_sort"]["confirmed"]):
        failure_reason = "sold_sort_not_confirmed"
    elif (
        listing_price_required_mode
        and require_listing_price_metric
        and bool(confirm["listing_price_metric"].get("available"))
        and not bool(confirm["listing_price_metric"]["confirmed"])
    ):
        failure_reason = "listing_price_metric_not_confirmed"
    elif require_lock and not bool(confirm["lock_selected_filters"]["confirmed"]):
        failure_reason = "lock_selected_filters_not_confirmed"

    auto_reasons = {
        "condition_filter_unavailable",
        "condition_filter_no_match",
        "condition_filter_not_confirmed",
        "fixed_price_filter_not_confirmed",
        "min_price_filter_not_confirmed",
        "sold_sort_not_confirmed",
        "listing_price_metric_not_confirmed",
        "lock_selected_filters_not_confirmed",
    }
    current_reason = str(state.get("strict_reason", "") or "").strip()
    if failure_reason:
        state["strict_blocked"] = True
        if (not current_reason) or (current_reason in auto_reasons):
            state["strict_reason"] = failure_reason
    elif current_reason in auto_reasons:
        state["strict_blocked"] = False
        state["strict_reason"] = ""

    return state


def _should_retry_filter_application_once(state: Dict[str, Any]) -> bool:
    if not isinstance(state, dict) or not state:
        return True
    auto_reasons = {
        "condition_filter_unavailable",
        "condition_filter_no_match",
        "condition_filter_not_confirmed",
        "fixed_price_filter_not_confirmed",
        "min_price_filter_not_confirmed",
        "sold_sort_not_confirmed",
        "lock_selected_filters_not_confirmed",
    }
    reason = str(state.get("strict_reason", "") or "").strip()
    if reason in auto_reasons:
        return True
    sold_ok = bool(state.get("sold_tab_selected"))
    condition_ok = bool(state.get("condition_selected"))
    format_ok = bool(state.get("format_fixed_price_selected"))
    min_price_ok = bool(state.get("min_price_selected")) or (_to_float(state.get("min_price_target_usd"), 0.0) <= 0)
    sort_target = _normalize_sold_sort(str(state.get("sort_target", "default") or "default"))
    sort_ok = bool(state.get("sort_selected")) or sort_target == "default"
    return not (sold_ok and condition_ok and format_ok and min_price_ok and sort_ok)


def _load_existing_rows(path: Path) -> Dict[str, Dict[str, Any]]:
    rows: Dict[str, Dict[str, Any]] = {}
    if not path.exists():
        return rows
    if path.suffix.lower() == ".json":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return rows
        if isinstance(payload, list):
            for row in payload:
                if isinstance(row, dict):
                    key = str(row.get("signal_key", "") or "")
                    if key:
                        rows[key] = row
        elif isinstance(payload, dict):
            key = str(payload.get("signal_key", "") or "")
            if key:
                rows[key] = payload
        return rows
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = str(raw or "").strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        key = str(row.get("signal_key", "") or "")
        if key:
            rows[key] = row
    return rows


def _save_rows(path: Path, rows: Dict[str, Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    items = sorted(rows.values(), key=lambda row: str(row.get("signal_key", "")))
    if path.suffix.lower() == ".json":
        path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
        return
    lines = [json.dumps(row, ensure_ascii=False) for row in items]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _row_quality_rank(row: Dict[str, Any]) -> Tuple[int, int, int, int, float]:
    sold = _to_int(row.get("sold_90d_count"), -1)
    confidence = _to_float(row.get("confidence"), 0.0)
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    filter_state = metadata.get("filter_state") if isinstance(metadata.get("filter_state"), dict) else {}
    strict_blocked = bool(filter_state.get("strict_blocked"))
    filtered_row_count = _to_int(metadata.get("filtered_row_count"), 0)
    sold_sample = metadata.get("sold_sample") if isinstance(metadata.get("sold_sample"), dict) else {}
    has_sample = bool(
        str(sold_sample.get("item_url", "") or "").strip()
        and _to_float(sold_sample.get("sold_price"), -1.0) > 0
    )
    sold_ok = 1 if sold >= 0 else 0
    strict_ok = 0 if strict_blocked else 1
    filtered_ok = 1 if filtered_row_count > 0 else 0
    sample_ok = 1 if has_sample else 0
    return (sold_ok, strict_ok, filtered_ok, sample_ok, confidence)


def _should_replace_existing_row(existing: Dict[str, Any], incoming: Dict[str, Any]) -> bool:
    return _row_quality_rank(incoming) >= _row_quality_rank(existing)


def _resolve_signal_key(query: str, mapping: Dict[str, str]) -> str:
    mapped = mapping.get(query.lower(), "").strip()
    if mapped:
        return mapped
    return _default_signal_key(query)


def _parse_signal_key_map(items: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in items:
        raw = str(item or "").strip()
        if not raw or "=" not in raw:
            continue
        left, right = raw.split("=", 1)
        query = _norm_query(left).lower()
        key = str(right or "").strip()
        if query and key:
            out[query] = key
    return out


def _effective_wait_seconds_for_query(query: str, default_wait: int) -> int:
    base = max(2, int(default_wait))
    has_code = bool(_extract_query_codes(query))
    if has_code:
        code_wait = max(2, _to_int(os.getenv("LIQUIDITY_RPA_WAIT_SECONDS_CODE", "5"), 5))
        return min(base, code_wait)
    is_broad = (not re.search(r"\d", str(query or ""))) and len(str(query or "").strip().split()) <= 3
    if is_broad:
        broad_wait = max(2, _to_int(os.getenv("LIQUIDITY_RPA_WAIT_SECONDS_BROAD", "7"), 7))
        return min(base, broad_wait)
    return base


def _emit_progress(
    *,
    phase: str,
    message: str,
    progress_percent: float,
    query: str = "",
    query_index: int = 0,
    total_queries: int = 0,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    payload: Dict[str, Any] = {
        "phase": str(phase or "").strip() or "unknown",
        "message": str(message or "").strip() or "-",
        "progress_percent": round(max(0.0, min(100.0, float(progress_percent))), 2),
        "query": str(query or "").strip(),
        "query_index": max(0, int(query_index)),
        "total_queries": max(0, int(total_queries)),
        "at": _utc_iso_now(),
    }
    if isinstance(extra, dict) and extra:
        payload["extra"] = extra
    print(f"[progress] {json.dumps(payload, ensure_ascii=False)}", flush=True)


def _query_progress_percent(query_index: int, total_queries: int, phase_ratio: float) -> float:
    total = max(1, int(total_queries))
    idx = max(1, min(total, int(query_index)))
    ratio = max(0.0, min(1.0, float(phase_ratio)))
    return ((idx - 1) + ratio) * (100.0 / total)


def run(args: argparse.Namespace) -> int:
    queries = _load_queries(args)
    if not queries:
        print("No query provided. Use --query or --queries-file.", file=sys.stderr)
        return 2

    signal_map = _parse_signal_key_map(args.signal_key_map)
    output = Path(args.output).expanduser()
    if not output.is_absolute():
        output = (ROOT_DIR / output).resolve()
    profile_dir = Path(args.profile_dir).expanduser()
    if not profile_dir.is_absolute():
        profile_dir = (ROOT_DIR / profile_dir).resolve()

    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        print(
            "Playwright is required. Install with: pip install playwright && playwright install chromium",
            file=sys.stderr,
        )
        return 3

    existing = _load_existing_rows(output)
    records: Dict[str, Dict[str, Any]] = dict(existing)
    _emit_progress(
        phase="startup",
        message="Product Research 取得を開始します",
        progress_percent=0.5,
        total_queries=len(queries),
        extra={"headless": bool(args.headless)},
    )
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=bool(args.headless),
            viewport={"width": 1440, "height": 960},
            service_workers="block" if bool(_to_int(os.getenv("LIQUIDITY_RPA_BLOCK_SERVICE_WORKERS", "1"), 1)) else "allow",
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.set_default_timeout(max(1500, int(_to_int(os.getenv("LIQUIDITY_RPA_ACTION_TIMEOUT_MS", "4500"), 4500))))
        page.set_default_navigation_timeout(
            max(3000, int(_to_int(os.getenv("LIQUIDITY_RPA_NAV_TIMEOUT_MS", "9000"), 9000)))
        )

        if args.login_url:
            page.goto(args.login_url, wait_until="domcontentloaded")
            page.wait_for_timeout(1000)
            _emit_progress(
                phase="login_url_loaded",
                message="Product Research画面へ遷移しました",
                progress_percent=1.5,
                total_queries=len(queries),
            )
        if args.pause_for_login > 0:
            print(
                f"[login] waiting {args.pause_for_login}s. Complete sign-in + 2FA if needed.",
                flush=True,
            )
            page.wait_for_timeout(int(args.pause_for_login * 1000))
            _emit_progress(
                phase="login_pause_completed",
                message="ログイン待機が完了しました",
                progress_percent=2.0,
                total_queries=len(queries),
            )

        daily_limit_reached = False
        bot_challenge_detected = False
        bot_challenge_stage = ""

        def _mark_bot_challenge(stage: str, *, query: str = "", query_index: int = 0) -> None:
            nonlocal bot_challenge_detected, bot_challenge_stage
            bot_challenge_detected = True
            bot_challenge_stage = str(stage or "").strip() or "unknown"
            print(
                f"[bot_challenge] stage={bot_challenge_stage} query={str(query or '').strip()} url={str(page.url or '')}",
                flush=True,
            )
            _emit_progress(
                phase="bot_challenge_detected",
                message="eBay bot challenge を検知したため停止します",
                progress_percent=_query_progress_percent(max(1, int(query_index or 1)), len(queries), 1.0),
                query=str(query or "").strip(),
                query_index=max(0, int(query_index or 0)),
                total_queries=len(queries),
                extra={"stage": bot_challenge_stage, "url": str(page.url or "")},
            )

        if _page_has_daily_limit_message(page):
            print("[daily_limit] Product Research daily request limit reached before query loop.", flush=True)
            _emit_progress(
                phase="daily_limit_reached",
                message="Product Researchの1日上限に到達しています",
                progress_percent=100.0,
                total_queries=len(queries),
            )
            daily_limit_reached = True
        if _page_has_bot_challenge_message(page):
            _mark_bot_challenge("before_query_loop")

        for index, query in enumerate(queries, start=1):
            if daily_limit_reached or bot_challenge_detected:
                break
            print(f"[{index}/{len(queries)}] query={query}", flush=True)
            _emit_progress(
                phase="query_start",
                message=f"検索語を処理中: {query}",
                progress_percent=_query_progress_percent(index, len(queries), 0.05),
                query=query,
                query_index=index,
                total_queries=len(queries),
            )
            acc = MetricAccumulator.create(query=query)
            captured = {"responses": 0, "json_responses": 0}
            query_started = time.perf_counter()
            timings: Dict[str, float] = {}
            effective_wait = _effective_wait_seconds_for_query(query, int(args.wait_seconds))
            query_halted = False

            def on_response(resp: Any) -> None:
                try:
                    req = resp.request
                    if req.resource_type not in {"fetch", "xhr"}:
                        return
                    if not _is_research_response(resp):
                        return
                    captured["responses"] += 1
                    body = resp.json()
                    captured["json_responses"] += 1
                    acc.ingest_payload(body)
                except Exception:
                    return

            page.on("response", on_response)
            filter_state: Dict[str, Any] = {}
            filters_screenshot_path = ""
            filters_html_path = ""
            early_no_sold_stage = ""
            short_circuit_no_sold = False
            try:
                t_search = time.perf_counter()
                _search_and_wait(
                    page,
                    query,
                    effective_wait,
                    result_offset=max(0, int(args.result_offset)),
                    result_limit=max(10, min(200, int(args.result_limit))),
                    category_id=max(0, int(args.category_id)),
                    category_slug=str(args.category_slug or "").strip(),
                    fixed_price_only=bool(args.fixed_price_only),
                    condition=str(args.condition or ""),
                    min_price_usd=max(0.0, _to_float(getattr(args, "min_price_usd", 0.0), 0.0)),
                    sold_sort=str(getattr(args, "sold_sort", "default") or "default"),
                )
                timings["search_wait_sec"] = round(max(0.0, time.perf_counter() - t_search), 4)
                _emit_progress(
                    phase="search_done",
                    message="検索結果を読み込みました",
                    progress_percent=_query_progress_percent(index, len(queries), 0.34),
                    query=query,
                    query_index=index,
                    total_queries=len(queries),
                    extra={"search_wait_sec": timings["search_wait_sec"]},
                )
                if _page_has_daily_limit_message(page):
                    print(f"[daily_limit] query={query} after search", flush=True)
                    _emit_progress(
                        phase="daily_limit_reached",
                        message="検索中にProduct Research上限へ到達しました",
                        progress_percent=_query_progress_percent(index, len(queries), 1.0),
                        query=query,
                        query_index=index,
                        total_queries=len(queries),
                    )
                    daily_limit_reached = True
                    query_halted = True
                    continue
                if _page_has_bot_challenge_message(page):
                    _mark_bot_challenge("after_search", query=query, query_index=index)
                    query_halted = True
                    continue
                current_lookback = ""
                if int(args.lookback_days) == 90:
                    try:
                        current_lookback = str(
                            page.evaluate(
                                """() => {
                                  const nodes = [...document.querySelectorAll("button, [role='button'], [role='tab'], span, div")];
                                  for (const el of nodes) {
                                    const style = window.getComputedStyle(el);
                                    if (style.display === "none" || style.visibility === "hidden") continue;
                                    const txt = (el.textContent || "").trim();
                                    if (/^last\\s+90\\s+days$/i.test(txt)) return txt;
                                  }
                                  return "";
                                }"""
                            )
                            or ""
                        ).strip()
                    except Exception:
                        current_lookback = ""
                if _should_short_circuit_no_sold(
                    query=query,
                    lookback_days=int(args.lookback_days),
                    no_sold_detected=_page_has_no_sold_message(page),
                    lookback_selected=current_lookback,
                ):
                    short_circuit_no_sold = True
                    early_no_sold_stage = "after_search"
                    filter_state["sold_tab_selected"] = True
                    filter_state["lookback_selected"] = current_lookback or "Last 90 days"
                    filter_state["early_no_sold_detected"] = True
                t_filters = time.perf_counter()
                if not short_circuit_no_sold:
                    _emit_progress(
                        phase="filters_applying",
                        message="フィルタを設定しています",
                        progress_percent=_query_progress_percent(index, len(queries), 0.56),
                        query=query,
                        query_index=index,
                        total_queries=len(queries),
                    )
                    filter_state = _apply_ui_filters(
                        page,
                        lookback_days=int(args.lookback_days),
                        condition=str(args.condition or "new"),
                        strict_condition=bool(args.strict_condition),
                        fixed_price_only=bool(args.fixed_price_only),
                        min_price_usd=max(0.0, _to_float(getattr(args, "min_price_usd", 0.0), 0.0)),
                        sold_sort=str(getattr(args, "sold_sort", "default") or "default"),
                    )
                    if _should_retry_filter_application_once(filter_state):
                        retry_wait_sec = max(
                            2,
                            _to_int(
                                os.getenv(
                                    "LIQUIDITY_RPA_FILTER_RETRY_WAIT_SECONDS",
                                    str(max(2, min(8, int(effective_wait)))),
                                ),
                                max(2, min(8, int(effective_wait))),
                            ),
                        )
                        _wait_for_research_interactive(page, retry_wait_sec)
                        page.wait_for_timeout(max(40, _to_int(os.getenv("LIQUIDITY_RPA_FILTER_SETTLE_MS", "45"), 45)))
                        retry_state = _apply_ui_filters(
                            page,
                            lookback_days=int(args.lookback_days),
                            condition=str(args.condition or "new"),
                            strict_condition=bool(args.strict_condition),
                            fixed_price_only=bool(args.fixed_price_only),
                            min_price_usd=max(0.0, _to_float(getattr(args, "min_price_usd", 0.0), 0.0)),
                            sold_sort=str(getattr(args, "sold_sort", "default") or "default"),
                        )
                        retry_state["filter_apply_retry_attempted"] = True
                        retry_state["filter_apply_retry_wait_sec"] = retry_wait_sec
                        retry_state["filter_apply_retry_from_reason"] = str(
                            filter_state.get("strict_reason", "insufficient_confirmations")
                        )
                        filter_state = retry_state
                timings["filter_apply_sec"] = round(max(0.0, time.perf_counter() - t_filters), 4)
                if _page_has_bot_challenge_message(page):
                    _mark_bot_challenge("after_filters", query=query, query_index=index)
                    query_halted = True
                    continue
                desired_offset = max(0, int(args.result_offset))
                if (not short_circuit_no_sold) and desired_offset > 0:
                    t_offset = time.perf_counter()
                    offset_state = _ensure_result_offset(
                        page,
                        desired_offset,
                        wait_seconds=max(2, min(6, int(effective_wait))),
                    )
                    filter_state.update(offset_state)
                    if bool(_to_int(os.getenv("LIQUIDITY_RPA_ENABLE_LOCK_SELECTED_FILTERS", "1"), 1)):
                        lock_after_offset = _set_lock_selected_filters(page)
                        if lock_after_offset:
                            filter_state["lock_selected_filters_after_offset"] = lock_after_offset
                            if lock_after_offset in {"enabled", "already_on"}:
                                filter_state["lock_selected_filters"] = lock_after_offset
                                page.wait_for_timeout(max(20, _to_int(os.getenv("LIQUIDITY_RPA_FILTER_SETTLE_MS", "45"), 45)))
                    timings["offset_apply_sec"] = round(max(0.0, time.perf_counter() - t_offset), 4)
                _emit_progress(
                    phase="filters_done",
                    message="フィルタ設定が完了しました",
                    progress_percent=_query_progress_percent(index, len(queries), 0.72),
                    query=query,
                    query_index=index,
                    total_queries=len(queries),
                    extra={"filter_apply_sec": timings["filter_apply_sec"]},
                )
                if _page_has_daily_limit_message(page):
                    print(f"[daily_limit] query={query} after filters", flush=True)
                    _emit_progress(
                        phase="daily_limit_reached",
                        message="フィルタ適用中にProduct Research上限へ到達しました",
                        progress_percent=_query_progress_percent(index, len(queries), 1.0),
                        query=query,
                        query_index=index,
                        total_queries=len(queries),
                    )
                    daily_limit_reached = True
                    query_halted = True
                    continue
                if not short_circuit_no_sold and _should_short_circuit_no_sold(
                    query=query,
                    lookback_days=int(args.lookback_days),
                    no_sold_detected=_page_has_no_sold_message(page),
                    lookback_selected=str(filter_state.get("lookback_selected", "") or ""),
                ):
                    short_circuit_no_sold = True
                    early_no_sold_stage = "after_filters"
                    filter_state["early_no_sold_detected"] = True
                if short_circuit_no_sold:
                    timings["collection_short_circuit"] = 1.0
                else:
                    quick_list_state = _get_research_list_state(page)
                    quick_rows = max(0, _to_int(quick_list_state.get("rows"), 0))
                    quick_no_sold = bool(quick_list_state.get("no_sold"))
                    quick_busy = bool(quick_list_state.get("busy"))
                    list_ready_now = (quick_rows > 0 or quick_no_sold) and (not quick_busy)
                    if not list_ready_now:
                        response_wait_timeout_ms = max(
                            800,
                            _to_int(
                                os.getenv(
                                    "LIQUIDITY_RPA_RESPONSE_WAIT_TIMEOUT_MS",
                                    str(min(2200, max(1000, int(effective_wait * 300)))),
                                ),
                                min(2200, max(1000, int(effective_wait * 300))),
                            ),
                        )
                        try:
                            page.wait_for_response(
                                lambda resp: bool(_is_research_response(resp)),
                                timeout=response_wait_timeout_ms,
                            )
                        except Exception:
                            pass
                        _wait_for_research_ready(page, max(1, min(2, int(effective_wait // 3) or 1)))
                    sort_target = _normalize_sold_sort(str(getattr(args, "sold_sort", "default") or "default"))
                    require_sort = bool(_to_int(os.getenv("LIQUIDITY_RPA_REQUIRE_SOLD_SORT", "0"), 0))
                    if sort_target != "default" and not bool(filter_state.get("sort_selected")):
                        sort_retry_state = _set_sold_sort(page, sort_target)
                        for key in (
                            "sort_target",
                            "sort_filter_panel_opened",
                            "sort_selected",
                            "sort_option_label",
                            "sort_selection_source",
                            "sort_order_state",
                            "sort_visible_options",
                        ):
                            if key in sort_retry_state:
                                filter_state[key] = sort_retry_state.get(key)
                    if sort_target != "default" and require_sort and not bool(filter_state.get("sort_selected")):
                        filter_state["strict_blocked"] = True
                        if not str(filter_state.get("strict_reason", "") or "").strip():
                            filter_state["strict_reason"] = "sold_sort_not_confirmed"
                    elif str(filter_state.get("strict_reason", "") or "").strip() == "sold_sort_not_confirmed" and bool(
                        filter_state.get("sort_selected")
                    ):
                        filter_state["strict_blocked"] = False
                        filter_state["strict_reason"] = ""
                    if desired_offset > 0:
                        offset_after_sort = _ensure_result_offset(
                            page,
                            desired_offset,
                            wait_seconds=max(2, min(6, int(effective_wait))),
                        )
                        for key, value in offset_after_sort.items():
                            filter_state[f"{key}_after_sort"] = value
                        if bool(_to_int(os.getenv("LIQUIDITY_RPA_ENABLE_LOCK_SELECTED_FILTERS", "1"), 1)):
                            lock_after_sort = _set_lock_selected_filters(page)
                            if lock_after_sort:
                                filter_state["lock_selected_filters_after_sort"] = lock_after_sort
                                if lock_after_sort in {"enabled", "already_on"}:
                                    filter_state["lock_selected_filters"] = lock_after_sort
                                elif bool(_to_int(os.getenv("LIQUIDITY_RPA_REQUIRE_LOCK_SELECTED_FILTERS", "0"), 0)):
                                    filter_state["strict_blocked"] = True
                                    if not str(filter_state.get("strict_reason", "") or "").strip():
                                        filter_state["strict_reason"] = "lock_selected_filters_not_confirmed"
                    filter_state = _finalize_filter_state_two_stage(
                        page,
                        filter_state,
                        condition=str(args.condition or "new"),
                        strict_condition=bool(args.strict_condition),
                        fixed_price_only=bool(args.fixed_price_only),
                        min_price_usd=max(0.0, _to_float(getattr(args, "min_price_usd", 0.0), 0.0)),
                        sold_sort=str(getattr(args, "sold_sort", "default") or "default"),
                    )
                    try:
                        html_text = page.content()
                        acc.ingest_html(html_text)
                        if _contains_daily_limit_message(html_text):
                            print(f"[daily_limit] query={query} from html content", flush=True)
                            daily_limit_reached = True
                            query_halted = True
                            continue
                        if _contains_bot_challenge_message(html_text):
                            _mark_bot_challenge("html_content", query=query, query_index=index)
                            query_halted = True
                            continue
                    except Exception:
                        pass
                    # DOM全文の走査は重いので、HTML解析で十分な場合はスキップして高速化する。
                    if not acc.filtered_row_prices and not acc.row_prices:
                        try:
                            body_text = page.inner_text("body")
                            acc.ingest_dom_text(body_text)
                        except Exception:
                            pass
                screenshot_template = str(getattr(args, "screenshot_after_filters", "") or "").strip()
                html_template = str(getattr(args, "html_after_filters", "") or "").strip()
                if screenshot_template or html_template:
                    try:
                        # スクリーンショットは、フィルタ反映後に結果一覧（または no sold 表示）が
                        # 描画されるのを待ってから取得する。
                        list_wait_sec = max(
                            1,
                            _to_int(
                                os.getenv(
                                    "LIQUIDITY_RPA_POST_FILTER_LIST_WAIT_SECONDS",
                                    str(max(1, int(max(1, effective_wait // 4)))),
                                ),
                                max(1, int(max(1, effective_wait // 4))),
                            ),
                        )
                        list_state = _wait_for_research_list_visible(page, list_wait_sec)
                        filter_state["rows_before_screenshot"] = max(0, _to_int(list_state.get("rows"), 0))
                        filter_state["no_sold_before_screenshot"] = bool(list_state.get("no_sold"))
                        filter_state["busy_before_screenshot"] = bool(list_state.get("busy"))
                        if screenshot_template:
                            shot_path = _resolve_filters_screenshot_path(screenshot_template, query, index)
                            shot_path.parent.mkdir(parents=True, exist_ok=True)
                            shot_timeout_ms = max(
                                3000, _to_int(os.getenv("LIQUIDITY_RPA_SCREENSHOT_TIMEOUT_MS", "15000"), 15000)
                            )
                            page.screenshot(path=str(shot_path), full_page=False, timeout=shot_timeout_ms)
                            filters_screenshot_path = str(shot_path)
                            _emit_progress(
                                phase="filters_screenshot_saved",
                                message="商品一覧の表示後にスクリーンショットを保存しました",
                                progress_percent=_query_progress_percent(index, len(queries), 0.74),
                                query=query,
                                query_index=index,
                                total_queries=len(queries),
                                extra={
                                    "screenshot_path": filters_screenshot_path,
                                    "rows_before_screenshot": int(filter_state.get("rows_before_screenshot", 0)),
                                },
                            )
                        if html_template:
                            html_path = _resolve_filters_html_path(html_template, query, index)
                            html_path.parent.mkdir(parents=True, exist_ok=True)
                            html_text = page.content()
                            html_path.write_text(str(html_text or ""), encoding="utf-8")
                            filters_html_path = str(html_path)
                            _emit_progress(
                                phase="filters_html_saved",
                                message="商品一覧の表示後HTMLを保存しました",
                                progress_percent=_query_progress_percent(index, len(queries), 0.745),
                                query=query,
                                query_index=index,
                                total_queries=len(queries),
                                extra={
                                    "html_path": filters_html_path,
                                    "rows_before_screenshot": int(filter_state.get("rows_before_screenshot", 0)),
                                },
                            )
                    except Exception as shot_err:
                        filter_state["filters_screenshot_error"] = str(shot_err or "")[:220]
            finally:
                page.remove_listener("response", on_response)
            if query_halted:
                continue
            if _page_has_bot_challenge_message(page):
                _mark_bot_challenge("before_metrics_finalize", query=query, query_index=index)
                continue

            metrics = acc.finalize()
            active_result: Dict[str, Any] = {}
            if bool(getattr(args, "collect_active_tab", False)):
                _emit_progress(
                    phase="active_tab_collecting",
                    message="Activeタブ情報を取得しています",
                    progress_percent=_query_progress_percent(index, len(queries), 0.86),
                    query=query,
                    query_index=index,
                    total_queries=len(queries),
                )
                active_result = _collect_active_tab_metrics(
                    page,
                    query=query,
                    wait_seconds=max(2, min(6, int(effective_wait))),
                    screenshot_template=str(getattr(args, "screenshot_active", "") or "").strip(),
                    html_template=str(getattr(args, "html_active", "") or "").strip(),
                    query_index=index,
                )
                if bool(active_result.get("daily_limit_reached")):
                    daily_limit_reached = True
                _emit_progress(
                    phase="active_tab_done",
                    message="Activeタブ情報の取得が完了しました",
                    progress_percent=_query_progress_percent(index, len(queries), 0.9),
                    query=query,
                    query_index=index,
                    total_queries=len(queries),
                    extra={
                        "active_count": _to_int(active_result.get("active_count"), -1),
                        "active_price_min": _to_float(active_result.get("active_price_min"), -1.0),
                    },
                )
            timings["total_query_sec"] = round(max(0.0, time.perf_counter() - query_started), 4)
            confidence = _to_float(metrics.get("confidence"), 0.3)
            condition_mode = str(args.condition or "").strip().lower()
            if condition_mode in {"", "any", "all"}:
                confidence = max(0.05, confidence - 0.12)
            if not bool(args.strict_condition):
                confidence = max(0.05, confidence - 0.06)
            active_count_value = _to_int(metrics.get("active_count"), -1)
            active_count_from_tab = _to_int(active_result.get("active_count"), -1)
            if active_count_from_tab >= 0:
                active_count_value = active_count_from_tab

            signal_key = _resolve_signal_key(query, signal_map)
            row = {
                "signal_key": signal_key,
                "query": query,
                "sold_90d_count": metrics["sold_90d_count"],
                "active_count": active_count_value,
                "sold_price_min": metrics["sold_price_min"],
                "sold_price_median": metrics["sold_price_median"],
                "sold_price_currency": metrics["sold_price_currency"],
                "confidence": round(min(0.95, max(0.0, confidence)), 4),
                "source": "ebay_product_research_rpa",
                "fetched_at": _utc_iso_now(),
                "metadata": {
                    "url": page.url,
                    "response_count": captured["responses"],
                    "json_response_count": captured["json_responses"],
                    "wait_seconds": int(effective_wait),
                    "headless": bool(args.headless),
                    "pass_label": str(args.pass_label or "primary_new"),
                    "collect_active_tab": bool(getattr(args, "collect_active_tab", False)),
                    "filter_state": filter_state,
                    "filtered_row_count": int(metrics.get("filtered_row_count", 0)),
                    "raw_row_count": int(metrics.get("raw_row_count", 0)),
                    "result_offset": max(0, int(args.result_offset)),
                    "result_limit": max(10, min(200, int(args.result_limit))),
                    "category_id": max(0, int(args.category_id)),
                    "category_slug": str(args.category_slug or "").strip().lower(),
                    "min_price_usd_target": max(0.0, _to_float(getattr(args, "min_price_usd", 0.0), 0.0)),
                    "sold_sort_target": _normalize_sold_sort(str(getattr(args, "sold_sort", "default") or "default")),
                    "timings": timings,
                },
            }
            if filters_screenshot_path:
                row["metadata"]["filters_screenshot_path"] = filters_screenshot_path
            if filters_html_path:
                row["metadata"]["filters_html_path"] = filters_html_path
            if active_result:
                active_tab_state = active_result.get("active_tab_state") if isinstance(active_result.get("active_tab_state"), dict) else {}
                if active_tab_state:
                    row["metadata"]["active_tab_state"] = active_tab_state
                active_price_min = _to_float(active_result.get("active_price_min"), -1.0)
                if active_price_min > 0:
                    row["metadata"]["active_price_min"] = round(active_price_min, 4)
                active_price_median = _to_float(active_result.get("active_price_median"), -1.0)
                if active_price_median > 0:
                    row["metadata"]["active_price_median"] = round(active_price_median, 4)
                active_sample = active_result.get("active_sample") if isinstance(active_result.get("active_sample"), dict) else {}
                if active_sample:
                    row["metadata"]["active_sample"] = active_sample
                active_rows = active_result.get("active_result_rows") if isinstance(active_result.get("active_result_rows"), list) else []
                if active_rows:
                    row["metadata"]["active_result_rows"] = active_rows
                raw_active_rows = (
                    active_result.get("raw_active_result_rows")
                    if isinstance(active_result.get("raw_active_result_rows"), list)
                    else []
                )
                if raw_active_rows:
                    row["metadata"]["raw_active_result_rows"] = raw_active_rows
                active_shot = str(active_result.get("screenshot_path", "") or "").strip()
                if active_shot:
                    row["metadata"]["active_screenshot_path"] = active_shot
                active_html = str(active_result.get("html_path", "") or "").strip()
                if active_html:
                    row["metadata"]["active_html_path"] = active_html
            sold_sample = metrics.get("sold_sample") if isinstance(metrics.get("sold_sample"), dict) else {}
            if sold_sample:
                row["metadata"]["sold_sample"] = sold_sample
            filtered_rows = metrics.get("filtered_result_rows") if isinstance(metrics.get("filtered_result_rows"), list) else []
            if filtered_rows:
                row["metadata"]["filtered_result_rows"] = filtered_rows
            raw_rows = metrics.get("raw_result_rows") if isinstance(metrics.get("raw_result_rows"), list) else []
            if raw_rows:
                row["metadata"]["raw_result_rows"] = raw_rows
            if bool(filter_state.get("early_no_sold_detected")):
                row["sold_90d_count"] = 0
                row["active_count"] = active_count_from_tab if active_count_from_tab >= 0 else -1
                row["sold_price_min"] = -1.0
                row["sold_price_median"] = -1.0
                row["confidence"] = max(0.6, _to_float(row.get("confidence"), 0.3))
                row["metadata"]["no_sales_in_window_inferred"] = True
                row["metadata"]["early_no_sold_stage"] = str(early_no_sold_stage or "unknown")
            elif bool(filter_state.get("strict_blocked")):
                row["sold_90d_count"] = -1
                row["active_count"] = -1
                row["sold_price_min"] = -1.0
                row["sold_price_median"] = -1.0
                row["confidence"] = 0.1
                row["metadata"]["strict_filter_reason"] = str(filter_state.get("strict_reason", "filter_blocked"))
            else:
                strict_sold_tab_required = bool(
                    _to_int(os.getenv("LIQUIDITY_RPA_REQUIRE_SOLD_TAB_FOR_POSITIVE", "1"), 1)
                )
                sold_count_now = int(row.get("sold_90d_count", -1))
                sold_tab_selected = bool(filter_state.get("sold_tab_selected"))
                lookback_selected = str(filter_state.get("lookback_selected", "")).lower().strip()
                if strict_sold_tab_required and sold_count_now > 0 and (
                    (not sold_tab_selected) or lookback_selected != "last 90 days"
                ):
                    row["sold_90d_count"] = -1
                    row["active_count"] = -1
                    row["sold_price_min"] = -1.0
                    row["sold_price_median"] = -1.0
                    row["confidence"] = min(_to_float(row.get("confidence"), 0.3), 0.2)
                    row["metadata"]["strict_filter_reason"] = "sold_tab_or_lookback_not_confirmed"
                    row["metadata"]["strict_filter_expected"] = "sold_tab_selected_last_90_days"
                # Distinguish unknown (-1) from confirmed zero sales in the selected window.
                if (
                    int(row.get("sold_90d_count", -1)) < 0
                    and bool(filter_state.get("sold_tab_selected"))
                    and str(filter_state.get("lookback_selected", "")).lower().strip() == "last 90 days"
                    and "/sh/research" in str(page.url or "")
                ):
                    row["sold_90d_count"] = 0
                    row["confidence"] = max(0.55, _to_float(row.get("confidence"), 0.3))
                    row["metadata"]["no_sales_in_window_inferred"] = True
            existing_row = records.get(signal_key)
            if isinstance(existing_row, dict):
                if _should_replace_existing_row(existing_row, row):
                    records[signal_key] = row
            else:
                records[signal_key] = row
            print(
                "  -> sold_90d={sold} min={minp} active={active} active_min={active_min} median={med} ccy={ccy} conf={conf}".format(
                    sold=row["sold_90d_count"],
                    minp=row["sold_price_min"],
                    active=row["active_count"],
                    active_min=row["metadata"].get("active_price_min", -1.0),
                    med=row["sold_price_median"],
                    ccy=row["sold_price_currency"],
                    conf=row["confidence"],
                ),
                flush=True,
            )
            _emit_progress(
                phase="query_done",
                message=f"検索語の処理が完了しました: {query}",
                progress_percent=_query_progress_percent(index, len(queries), 1.0),
                query=query,
                query_index=index,
                total_queries=len(queries),
                extra={
                    "sold_90d_count": int(row["sold_90d_count"]),
                    "sold_price_min": float(row["sold_price_min"]),
                    "active_count": int(_to_int(row.get("active_count"), -1)),
                    "active_price_min": float(_to_float((row.get("metadata") or {}).get("active_price_min"), -1.0)),
                    "sold_price_median": float(row["sold_price_median"]),
                },
            )
            if args.inter_query_sleep > 0 and index < len(queries):
                time.sleep(args.inter_query_sleep)
            if _page_has_bot_challenge_message(page):
                _mark_bot_challenge("after_query_done", query=query, query_index=index)
                break

        pause_before_close_sec = max(0, int(getattr(args, "pause_before_close", 0)))
        if (not bool(args.headless)) and pause_before_close_sec > 0:
            _emit_progress(
                phase="final_visual_pause",
                message=f"最終確認のため {pause_before_close_sec} 秒待機します",
                progress_percent=99.5,
                total_queries=len(queries),
            )
            page.wait_for_timeout(pause_before_close_sec * 1000)

        context.close()

    _save_rows(output, records)
    print(f"saved {len(records)} rows -> {output}")
    _emit_progress(
        phase="completed",
        message="Product Research取得が完了しました",
        progress_percent=100.0,
        total_queries=len(queries),
        extra={"saved_rows": len(records)},
    )
    if daily_limit_reached:
        return 75
    if bot_challenge_detected:
        print(f"[bot_challenge] halted at stage={bot_challenge_stage}", flush=True)
        return 76
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="RPA collector for eBay Product Research (Terapeak) sold metrics."
    )
    parser.add_argument("--query", action="append", default=[], help="Search query (repeatable)")
    parser.add_argument("--queries-file", default="", help="Text file with one query per line")
    parser.add_argument(
        "--signal-key-map",
        action="append",
        default=[],
        help="Mapping in 'query=signal_key' form (repeatable)",
    )
    parser.add_argument(
        "--output",
        default="data/liquidity_rpa_signals.jsonl",
        help="Output path (.json or .jsonl)",
    )
    parser.add_argument(
        "--profile-dir",
        default="data/rpa/ebay_profile",
        help="Persistent browser profile directory",
    )
    parser.add_argument(
        "--login-url",
        default="https://www.ebay.com/sh/research",
        help="Login/start URL before collection",
    )
    parser.add_argument(
        "--pause-for-login",
        type=int,
        default=0,
        help="Seconds to allow manual sign-in before starting",
    )
    parser.add_argument(
        "--wait-seconds",
        type=int,
        default=8,
        help="Max wait time after query load (returns early when table is ready).",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=90,
        help="Lookback range for Product Research (7/30/90/180/365/730/1095).",
    )
    parser.add_argument(
        "--condition",
        default="new",
        help="Condition filter target (new/used/refurbished/any).",
    )
    parser.add_argument(
        "--pass-label",
        default="primary_new",
        help="Label to identify collection pass in metadata.",
    )
    parser.add_argument(
        "--strict-condition",
        action="store_true",
        help="Mark query as unavailable when condition option is not present.",
    )
    parser.add_argument(
        "--fixed-price-only",
        action="store_true",
        help="Also apply Fixed price format filter.",
    )
    parser.add_argument(
        "--inter-query-sleep",
        type=float,
        default=0.25,
        help="Sleep seconds between queries",
    )
    parser.add_argument(
        "--pause-before-close",
        type=int,
        default=0,
        help="Final visual pause seconds before closing browser (non-headless only).",
    )
    parser.add_argument(
        "--screenshot-after-filters",
        default="",
        help="Save screenshot after filter setup once results list is visible (supports {query}/{index}/{ts}).",
    )
    parser.add_argument(
        "--html-after-filters",
        default="",
        help="Save HTML after filter setup once results list is visible (supports {query}/{index}/{ts}).",
    )
    parser.add_argument(
        "--collect-active-tab",
        action="store_true",
        default=bool(_to_int(os.getenv("LIQUIDITY_RPA_COLLECT_ACTIVE_TAB", "0"), 0)),
        help="After sold metrics capture, switch to Active tab and collect active count/min sample.",
    )
    parser.add_argument(
        "--screenshot-active",
        default="",
        help="Save screenshot after Active tab capture (supports {query}/{index}/{ts}).",
    )
    parser.add_argument(
        "--html-active",
        default="",
        help="Save HTML after Active tab capture (supports {query}/{index}/{ts}).",
    )
    parser.add_argument(
        "--min-price-usd",
        type=float,
        default=max(0.0, _to_float(os.getenv("LIQUIDITY_RPA_MIN_PRICE_USD", "0"), 0.0)),
        help="Minimum sold price filter target in USD (0 disables).",
    )
    parser.add_argument(
        "--sold-sort",
        default=str(os.getenv("LIQUIDITY_RPA_SOLD_SORT", "default") or "default"),
        help="Sold listing sort target (default/recently_sold/price_desc/price_asc).",
    )
    parser.add_argument(
        "--result-offset",
        type=int,
        default=0,
        help="Result offset for Product Research listing rows (0-based).",
    )
    parser.add_argument(
        "--result-limit",
        type=int,
        default=50,
        help="Result page size hint for Product Research listing rows.",
    )
    parser.add_argument(
        "--category-id",
        type=int,
        default=0,
        help="eBay categoryId for Product Research filter (0 disables category filter).",
    )
    parser.add_argument(
        "--category-slug",
        default="",
        help="Optional category slug hint (for URL/debug visibility).",
    )
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())

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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT_DIR = Path(__file__).resolve().parents[1]

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
_RE_HTML_ROW = re.compile(r'class="research-table-row"')
_RE_HTML_ROW_PRICE = re.compile(
    r"research-table-row__avgSoldPrice.*?<div[^>]*>\$?([0-9][0-9,]{0,9}(?:\.[0-9]{1,2})?)</div>",
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
    r'<div[^>]*class="[^"]*(?<![A-Z0-9_-])research-table-row(?![A-Z0-9_-])[^"]*"[^>]*>',
    re.IGNORECASE,
)
_RE_HTML_TAG = re.compile(r"<[^>]+>")
_RE_DAILY_LIMIT_PHRASES = (
    re.compile(r"exceeded\s+the\s+number\s+of\s+requests\s+allowed\s+in\s+one\s+day", re.IGNORECASE),
    re.compile(r"please\s+try\s+again\s+tomorrow", re.IGNORECASE),
    re.compile(r"number\s+of\s+requests\s+allowed\s+in\s+one\s+day", re.IGNORECASE),
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
    prices: List[float] = []
    sold_dates: set[str] = set()
    sold_sample: Dict[str, Any] = {}
    text = str(html or "")
    starts = [m.start() for m in _RE_HTML_ROW_START.finditer(text)]
    if not starts:
        return prices, 0, sold_sample
    for idx, start in enumerate(starts):
        end = starts[idx + 1] if (idx + 1) < len(starts) else len(text)
        block = text[start:end]
        row_text = _strip_html_text(block)
        if not row_text:
            continue
        if _is_accessory_row(row_text):
            continue
        if not _row_matches_query_text(row_text, query_codes=query_codes, query_tokens=query_tokens):
            continue
        price_match = _RE_HTML_ROW_PRICE.search(block)
        if price_match:
            raw = str(price_match.group(1) or "").replace(",", "")
            price = _to_float(raw, -1.0)
            if price > 0:
                prices.append(price)
                current_sample_price = _to_float(sold_sample.get("sold_price"), -1.0)
                if not sold_sample or current_sample_price <= 0 or price < current_sample_price:
                    sample: Dict[str, Any] = {
                        "title": row_text[:220],
                        "sold_price": round(price, 4),
                    }
                    link_match = _RE_HTML_ROW_LINK.search(block)
                    if link_match:
                        href = str(link_match.group(1) or "").strip()
                        if href:
                            sample["item_url"] = urllib.parse.urljoin("https://www.ebay.com", href)
                        title_raw = _strip_html_text(str(link_match.group(2) or ""))
                        if title_raw:
                            sample["title"] = title_raw[:220]
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
                        sample["image_url"] = urllib.parse.urljoin("https://www.ebay.com", img_src)
                    sold_sample = sample
        date_match = _RE_HTML_ROW_DATE.search(block)
        if date_match:
            sold_dates.add(str(date_match.group(1) or "").strip())
    sold_count = len(sold_dates) if sold_dates else len(prices)
    if sold_dates and sold_sample:
        sold_sample["sold_date_count_detected"] = len(sold_dates)
    return prices, sold_count, sold_sample


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_int(value: Any, default: int = -1) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float = -1.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


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

    def ingest_html(self, html: str) -> None:
        row_count = len(_RE_HTML_ROW.findall(html or ""))
        if row_count > 0:
            self.sold_counts.append(row_count)
        date_count = len(set(_RE_HTML_ROW_DATE.findall(html or "")))
        if date_count > 0:
            self.sold_counts.append(date_count)
        filtered_prices, filtered_sold, sold_sample = _extract_filtered_rows_from_html(
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

        return {
            "sold_90d_count": sold,
            "active_count": active,
            "sold_price_min": round(min_price, 4) if min_price > 0 else -1.0,
            "sold_price_median": round(median_price, 4) if median_price > 0 else -1.0,
            "sold_price_currency": currency,
            "confidence": round(min(0.95, max(0.0, confidence)), 4),
            "raw_row_count": len(self.row_prices),
            "filtered_row_count": len(self.filtered_row_prices),
            "sold_sample": best_sold_sample,
        }


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


def _search_and_wait(page: Any, query: str, wait_seconds: int) -> None:
    wait_until = str(os.getenv("LIQUIDITY_RPA_GOTO_WAIT_UNTIL", "commit") or "commit").strip().lower()
    if wait_until not in {"commit", "domcontentloaded", "load"}:
        wait_until = "commit"
    settle_ms = max(0, _to_int(os.getenv("LIQUIDITY_RPA_POST_GOTO_SETTLE_MS", "70"), 70))
    nav_retry = max(0, _to_int(os.getenv("LIQUIDITY_RPA_GOTO_RETRY_COUNT", "2"), 2))
    nav_retry_wait_ms = max(80, _to_int(os.getenv("LIQUIDITY_RPA_GOTO_RETRY_WAIT_MS", "220"), 220))
    encoded = urllib.parse.quote_plus(query)
    url = f"https://www.ebay.com/sh/research?marketplace=EBAY-US&keywords={encoded}"

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
        1,
        _to_int(
            os.getenv("LIQUIDITY_RPA_PRE_FILTER_WAIT_SECONDS", str(max(1, min(2, int(wait_seconds))))),
            max(1, min(2, int(wait_seconds))),
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
                  const rows = document.querySelectorAll("div.research-table-row").length;
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
                    return visible(el) && t === "sold";
                  });
                  const hasFilterButton = [...document.querySelectorAll("button, [role='button']")].some((el) => {
                    const t = (el.textContent || "").trim().toLowerCase();
                    return visible(el) && (t.includes("condition filter") || t.includes("format filter") || t.startsWith("last "));
                  });
                  return soldTab || hasFilterButton;
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
          const items = [...document.querySelectorAll("div[role='menuitemcheckbox']")];
          const normalized = (s) => (s || "").toLowerCase();
          for (const el of items) {
            const style = window.getComputedStyle(el);
            if (style.display === "none" || style.visibility === "hidden") continue;
            const r = el.getBoundingClientRect();
            if (r.width < 1 || r.height < 1) continue;
            const txt = normalized(el.textContent);
            if (!tokens.some((t) => txt.includes(normalized(t)))) continue;
            el.click();
            return (el.textContent || "").trim();
          }
          return "";
        }""",
        tokens,
    )
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
    skip_when_default = bool(_to_int(os.getenv("LIQUIDITY_RPA_SKIP_LOOKBACK_WHEN_DEFAULT", "1"), 1))
    if skip_when_default and int(days) == 90:
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
          const nodes = [
            ...document.querySelectorAll(
              "div[role='menuitemcheckbox'], [role='checkbox'], label, button, [role='button'], span"
            ),
          ];
          for (const el of nodes) {
            const style = window.getComputedStyle(el);
            if (style.display === "none" || style.visibility === "hidden") continue;
            const txt = norm(el.textContent);
            if (!txt.includes("lock selected filters")) continue;
            const ariaChecked = norm(el.getAttribute("aria-checked"));
            const pressed = norm(el.getAttribute("aria-pressed"));
            let checked = (ariaChecked === "true") || (pressed === "true");
            const input = el.querySelector("input[type='checkbox']");
            if (input && (input.checked || norm(input.getAttribute("aria-checked")) === "true")) checked = true;
            if (checked) return "already_on";
            el.click();
            return "enabled";
          }
          return "not_found";
        }"""
    )
    text = str(status or "").strip().lower()
    if text in {"enabled", "already_on", "not_found"}:
        return text
    return "not_found"


def _apply_ui_filters(
    page: Any,
    *,
    lookback_days: int,
    condition: str,
    strict_condition: bool,
    fixed_price_only: bool,
) -> Dict[str, Any]:
    filter_settle_ms = max(25, _to_int(os.getenv("LIQUIDITY_RPA_FILTER_SETTLE_MS", "45"), 45))
    state: Dict[str, Any] = {
        "sold_tab_selected": False,
        "lookback_selected": "",
        "lookback_default_kept": False,
        "condition_target": condition,
        "condition_selected": [],
        "condition_missing": False,
        "format_fixed_price_selected": False,
        "lock_selected_filters": "",
        "strict_blocked": False,
        "strict_reason": "",
    }

    if _click_first(page, ["[role='tab']:visible:has-text('Sold')", "div[role='tab']:visible:has-text('Sold')"]):
        state["sold_tab_selected"] = True
        page.wait_for_timeout(filter_settle_ms)

    picked_lookback = _set_lookback_days(page, lookback_days)
    if picked_lookback:
        state["lookback_selected"] = picked_lookback
        state["lookback_default_kept"] = str(picked_lookback).strip().lower() == "last 90 days"
        page.wait_for_timeout(filter_settle_ms)

    if bool(_to_int(os.getenv("LIQUIDITY_RPA_ENABLE_LOCK_SELECTED_FILTERS", "1"), 1)):
        state["lock_selected_filters"] = _set_lock_selected_filters(page)
        if state["lock_selected_filters"] in {"enabled", "already_on"}:
            page.wait_for_timeout(filter_settle_ms)

    opened = _click_first(
        page,
        [
            "button:visible:has-text('Condition filter')",
            "[role='button']:visible:has-text('Condition filter')",
        ],
    )
    if opened:
        page.wait_for_timeout(filter_settle_ms)
        if str(condition or "").strip().lower() not in {"", "any", "all"}:
            token_map = {
                "new": ["new(", "brand new", "new with", "新品"],
                "used": ["used("],
                "refurbished": ["refurbished"],
            }
            tokens = token_map.get(str(condition).strip().lower(), [str(condition).strip().lower()])
            selected = _toggle_visible_checkbox_by_tokens(page, tokens)
            if selected:
                state["condition_selected"].append(selected)
            else:
                state["condition_missing"] = True
                if strict_condition:
                    state["strict_blocked"] = True
                    state["strict_reason"] = "condition_filter_no_match"
        if fixed_price_only:
            picked = _toggle_visible_checkbox_by_tokens(page, ["fixed price"])
            if picked:
                state["format_fixed_price_selected"] = True
        _apply_button_if_visible(page)
        _wait_for_research_ready(page, 2)

    if fixed_price_only and not state["format_fixed_price_selected"]:
        if _click_first(
            page,
            [
                "button:visible:has-text('Format filter')",
                "[role='button']:visible:has-text('Format filter')",
            ],
        ):
            page.wait_for_timeout(filter_settle_ms)
            picked = _toggle_visible_checkbox_by_tokens(page, ["fixed price"])
            if picked:
                state["format_fixed_price_selected"] = True
            _apply_button_if_visible(page)
            _wait_for_research_ready(page, 2)

    return state


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
        if _page_has_daily_limit_message(page):
            print("[daily_limit] Product Research daily request limit reached before query loop.", flush=True)
            _emit_progress(
                phase="daily_limit_reached",
                message="Product Researchの1日上限に到達しています",
                progress_percent=100.0,
                total_queries=len(queries),
            )
            daily_limit_reached = True

        for index, query in enumerate(queries, start=1):
            if daily_limit_reached:
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
            early_no_sold_stage = ""
            short_circuit_no_sold = False
            try:
                t_search = time.perf_counter()
                _search_and_wait(page, query, effective_wait)
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
                    )
                timings["filter_apply_sec"] = round(max(0.0, time.perf_counter() - t_filters), 4)
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
                    try:
                        page.wait_for_response(
                            lambda resp: bool(_is_research_response(resp)),
                            timeout=max(1200, int(effective_wait * 1000 * 0.7)),
                        )
                    except Exception:
                        pass
                    _wait_for_research_ready(page, max(1, int(effective_wait // 2) or 1))
                    try:
                        html_text = page.content()
                        acc.ingest_html(html_text)
                        if _contains_daily_limit_message(html_text):
                            print(f"[daily_limit] query={query} from html content", flush=True)
                            daily_limit_reached = True
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
            finally:
                page.remove_listener("response", on_response)
            if query_halted:
                continue

            metrics = acc.finalize()
            timings["total_query_sec"] = round(max(0.0, time.perf_counter() - query_started), 4)
            confidence = _to_float(metrics.get("confidence"), 0.3)
            condition_mode = str(args.condition or "").strip().lower()
            if condition_mode in {"", "any", "all"}:
                confidence = max(0.05, confidence - 0.12)
            if not bool(args.strict_condition):
                confidence = max(0.05, confidence - 0.06)

            signal_key = _resolve_signal_key(query, signal_map)
            row = {
                "signal_key": signal_key,
                "query": query,
                "sold_90d_count": metrics["sold_90d_count"],
                "active_count": metrics["active_count"],
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
                    "filter_state": filter_state,
                    "filtered_row_count": int(metrics.get("filtered_row_count", 0)),
                    "raw_row_count": int(metrics.get("raw_row_count", 0)),
                    "timings": timings,
                },
            }
            sold_sample = metrics.get("sold_sample") if isinstance(metrics.get("sold_sample"), dict) else {}
            if sold_sample:
                row["metadata"]["sold_sample"] = sold_sample
            if bool(filter_state.get("strict_blocked")):
                row["sold_90d_count"] = -1
                row["active_count"] = -1
                row["sold_price_min"] = -1.0
                row["sold_price_median"] = -1.0
                row["confidence"] = 0.1
                row["metadata"]["strict_filter_reason"] = str(filter_state.get("strict_reason", "filter_blocked"))
            elif bool(filter_state.get("early_no_sold_detected")):
                row["sold_90d_count"] = 0
                row["active_count"] = -1
                row["sold_price_min"] = -1.0
                row["sold_price_median"] = -1.0
                row["confidence"] = max(0.6, _to_float(row.get("confidence"), 0.3))
                row["metadata"]["no_sales_in_window_inferred"] = True
                row["metadata"]["early_no_sold_stage"] = str(early_no_sold_stage or "unknown")
            else:
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
            records[signal_key] = row
            print(
                "  -> sold_90d={sold} min={minp} median={med} ccy={ccy} conf={conf}".format(
                    sold=row["sold_90d_count"],
                    minp=row["sold_price_min"],
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
                    "sold_price_median": float(row["sold_price_median"]),
                },
            )
            if args.inter_query_sleep > 0 and index < len(queries):
                time.sleep(args.inter_query_sleep)

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
        default=40,
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
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())

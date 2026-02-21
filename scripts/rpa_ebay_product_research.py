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
    r'<img[^>]+src="([^"]+)"',
    re.IGNORECASE,
)
_RE_HTML_ROW_START = re.compile(
    r'<div[^>]*class="[^"]*(?<![A-Z0-9_-])research-table-row(?![A-Z0-9_-])[^"]*"[^>]*>',
    re.IGNORECASE,
)
_RE_HTML_TAG = re.compile(r"<[^>]+>")

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
    "COMPATIBLE",
    "PART",
    "PARTS",
    "CASE",
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
    "ケース",
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
                    img_match = _RE_HTML_IMG_SRC.search(block)
                    if img_match:
                        src = str(img_match.group(1) or "").strip()
                        if src:
                            sample["image_url"] = urllib.parse.urljoin("https://www.ebay.com", src)
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
        if filtered_sold >= 0:
            sold = filtered_sold
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
        return {
            "sold_90d_count": sold,
            "active_count": active,
            "sold_price_min": round(min_price, 4) if min_price > 0 else -1.0,
            "sold_price_median": round(median_price, 4) if median_price > 0 else -1.0,
            "sold_price_currency": currency,
            "confidence": round(min(0.95, max(0.0, confidence)), 4),
            "raw_row_count": len(self.row_prices),
            "filtered_row_count": len(self.filtered_row_prices),
            "sold_sample": self.filtered_sold_samples[0] if self.filtered_sold_samples else {},
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
    encoded = urllib.parse.quote_plus(query)
    url = f"https://www.ebay.com/sh/research?marketplace=EBAY-US&keywords={encoded}"
    page.goto(url, wait_until="domcontentloaded")
    page.wait_for_timeout(400)
    if "/sh/research" not in str(page.url or ""):
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(300)
    _wait_for_research_ready(page, wait_seconds)


def _wait_for_research_ready(page: Any, wait_seconds: int) -> bool:
    timeout_ms = int(max(1000, wait_seconds * 1000))
    deadline = time.time() + (timeout_ms / 1000.0)
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
                page.wait_for_timeout(120)
                return True
        except Exception:
            pass
        page.wait_for_timeout(180)
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
    opened = _click_first(
        page,
        [
            "button:visible:has-text('Last')",
            "[role='button']:visible:has-text('Last')",
        ],
    )
    if not opened:
        return None
    page.wait_for_timeout(250)
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


def _apply_ui_filters(
    page: Any,
    *,
    lookback_days: int,
    condition: str,
    strict_condition: bool,
    fixed_price_only: bool,
) -> Dict[str, Any]:
    state: Dict[str, Any] = {
        "sold_tab_selected": False,
        "lookback_selected": "",
        "condition_target": condition,
        "condition_selected": [],
        "condition_missing": False,
        "format_fixed_price_selected": False,
        "strict_blocked": False,
        "strict_reason": "",
    }

    if _click_first(page, ["[role='tab']:visible:has-text('Sold')", "div[role='tab']:visible:has-text('Sold')"]):
        state["sold_tab_selected"] = True
        page.wait_for_timeout(120)

    picked_lookback = _set_lookback_days(page, lookback_days)
    if picked_lookback:
        state["lookback_selected"] = picked_lookback
        page.wait_for_timeout(120)

    opened = _click_first(
        page,
        [
            "button:visible:has-text('Condition filter')",
            "[role='button']:visible:has-text('Condition filter')",
        ],
    )
    if opened:
        page.wait_for_timeout(100)
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
            page.wait_for_timeout(100)
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
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=bool(args.headless),
            viewport={"width": 1440, "height": 960},
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = context.pages[0] if context.pages else context.new_page()

        if args.login_url:
            page.goto(args.login_url, wait_until="domcontentloaded")
            page.wait_for_timeout(1000)
        if args.pause_for_login > 0:
            print(
                f"[login] waiting {args.pause_for_login}s. Complete sign-in + 2FA if needed.",
                flush=True,
            )
            page.wait_for_timeout(int(args.pause_for_login * 1000))

        for index, query in enumerate(queries, start=1):
            print(f"[{index}/{len(queries)}] query={query}", flush=True)
            acc = MetricAccumulator.create(query=query)
            captured = {"responses": 0, "json_responses": 0}
            query_started = time.perf_counter()
            timings: Dict[str, float] = {}

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
            t_search = time.perf_counter()
            _search_and_wait(page, query, args.wait_seconds)
            timings["search_wait_sec"] = round(max(0.0, time.perf_counter() - t_search), 4)
            t_filters = time.perf_counter()
            filter_state = _apply_ui_filters(
                page,
                lookback_days=int(args.lookback_days),
                condition=str(args.condition or "new"),
                strict_condition=bool(args.strict_condition),
                fixed_price_only=bool(args.fixed_price_only),
            )
            timings["filter_apply_sec"] = round(max(0.0, time.perf_counter() - t_filters), 4)
            _wait_for_research_ready(page, max(1, int(args.wait_seconds // 2) or 1))
            try:
                html_text = page.content()
                acc.ingest_html(html_text)
            except Exception:
                pass
            try:
                body_text = page.inner_text("body")
                acc.ingest_dom_text(body_text)
            except Exception:
                pass
            page.remove_listener("response", on_response)

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
                    "wait_seconds": args.wait_seconds,
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
            if args.inter_query_sleep > 0 and index < len(queries):
                time.sleep(args.inter_query_sleep)

        context.close()

    _save_rows(output, records)
    print(f"saved {len(records)} rows -> {output}")
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
        default=1.0,
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

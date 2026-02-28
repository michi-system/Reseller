"""seed-pool orchestration for Miner production fetch flow."""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
import unicodedata
import html
import urllib.parse
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from .coerce import env_bool, env_float, env_int, to_float, to_int
from .config import ROOT_DIR, Settings, load_settings
from .liquidity import get_liquidity_signal
from .live_miner_fetch import (
    MarketItem,
    _maybe_refresh_rpa_for_fetch,
    _rpa_daily_limit_reached,
    _build_site_queries,
    _build_category_seed_queries,
    _canonical_code_set,
    _load_category_knowledge,
    _ebay_access_token,
    _extract_codes,
    _is_accessory_title,
    _match_category_row,
    _request_with_retry,
    _resolve_rakuten_variant_price_jpy,
    _search_rakuten,
    _search_yahoo,
    _specific_model_codes_in_title,
)
from .miner import create_miner_candidate
from .models import connect, init_db
from .profit import ProfitInput, calculate_profit
from .rpa_runtime import resolve_rpa_output_path as _resolve_rpa_output_path
from .time_utils import iso_to_epoch, utc_iso


_COUNT_KEYS: Tuple[str, ...] = (
    "created_count",
    "skipped_duplicates",
    "skipped_low_match",
    "skipped_invalid_price",
    "skipped_unprofitable",
    "skipped_low_margin",
    "skipped_low_ev90",
    "skipped_low_liquidity",
    "skipped_liquidity_unavailable",
    "skipped_missing_sold_min",
    "skipped_missing_sold_sample",
    "skipped_below_sold_min",
    "skipped_implausible_sold_min",
    "skipped_source_variant_unresolved",
    "skipped_stage1_api_budget",
    "skipped_blocked",
    "skipped_group_cap",
    "skipped_ambiguous_model_title",
)

_SEED_STOPWORDS: Set[str] = {
    "NEW",
    "NIB",
    "JAPAN",
    "AUTHENTIC",
    "WATCH",
    "WATCHES",
    "MENS",
    "WOMENS",
    "LADIES",
    "UNUSED",
    "FREE",
    "SHIPPING",
    "PRICE",
    "LISTING",
    "SOLD",
    "SALE",
    "MODEL",
    "SERIES",
    "SPECIAL",
    "LIMITED",
    "LTD",
    "EDITION",
    "ANNIVERSARY",
}

_SEED_FALLBACK_BROAD_SERIES_KEYS: Set[str] = {
    "GSHOCK",
    "PROSPEX",
    "PROMASTER",
    "SPORTS",
}


def _is_way_noise_token(text: str) -> bool:
    return bool(re.fullmatch(r"[0-9]+WAY", str(text or "").strip().upper()))

_SEED_UI_NOISE_KEYS: Set[str] = {
    "CANTFINDTHEWORDSSEARCHWITHANIMAGE",
    "VISUALSEARCHHANDLER",
    "RTMTRACKING",
    "DEVICEFINGERPRINT",
    "USERSHIPLOCATION",
}

_SEED_LEADING_CONDITION_WORDS: Set[str] = {
    "NIB",
    "UNUSED",
    "USED",
    "PREOWNED",
    "PRE-OWNED",
    "OPENBOX",
    "OPEN-BOX",
    "JUNK",
    "REFURBISHED",
    "RENEWED",
    "新品",
    "未使用",
    "中古",
    "ジャンク",
    "訳あり",
}

_SEED_TRAILING_CONDITION_WORDS: Set[str] = {
    "NEW",
    "NIB",
    "UNUSED",
    "USED",
    "PREOWNED",
    "PRE-OWNED",
    "OPENBOX",
    "OPEN-BOX",
    "JUNK",
    "REFURBISHED",
    "RENEWED",
    "新品",
    "未使用",
    "中古",
    "ジャンク",
    "訳あり",
}

_SEED_TRAILING_CONDITION_PHRASES: Tuple[Tuple[str, ...], ...] = (
    ("FOR", "PARTS"),
    ("PARTS", "ONLY"),
    ("OPEN", "BOX"),
    ("LIKE", "NEW"),
    ("PRE", "OWNED"),
)

_SEED_MIDDLE_DROP_WORDS: Set[str] = {
    "USED",
    "UNUSED",
    "NIB",
    "JUNK",
    "REFURBISHED",
    "RENEWED",
    "新品",
    "未使用",
    "中古",
    "ジャンク",
    "訳あり",
}

_TARGET_LABELS: Dict[str, str] = {
    "watch": "腕時計",
    "sneakers": "スニーカー",
    "streetwear": "ストリートウェア",
    "trading_cards": "トレーディングカード",
    "toys_collectibles": "ホビー",
    "video_game_consoles": "ゲーム機",
    "camera_lenses": "レンズ",
}

_CATEGORY_SEED_MIN_SOLD_PRICE_USD_DEFAULTS: Dict[str, float] = {
    "watch": 100.0,
    "sneakers": 20.0,
    "streetwear": 15.0,
    "trading_cards": 10.0,
    "toys_collectibles": 12.0,
    "video_game_consoles": 60.0,
    "camera_lenses": 25.0,
}

_CATEGORY_STAGE_C_MIN_SOLD_90D_DEFAULTS: Dict[str, int] = {
    "watch": 3,
}

_EBAY_PR_CATEGORY_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "watch": {"id": 31387, "slug": "wristwatches"},
    "sneakers": {"id": 15709, "slug": "mens-sneakers"},
    "streetwear": {"id": 1059, "slug": "streetwear"},
    "trading_cards": {"id": 183454, "slug": "individual-collectible-card-game-cards"},
    "toys_collectibles": {"id": 220, "slug": "toys-hobbies"},
    "video_game_consoles": {"id": 139971, "slug": "video-game-consoles"},
    "camera_lenses": {"id": 3323, "slug": "camera-lenses"},
}

_SEED_RUN_JOURNAL_PATH = ROOT_DIR / "data" / "miner_seed_run_journal.jsonl"
_SEED_API_USAGE_PATH = ROOT_DIR / "data" / "miner_seed_api_usage.json"
_SEED_REFILL_TRACE_DIR = ROOT_DIR / "docs" / "cycle_diagnostics"
_LOW_LIQUIDITY_COOLDOWN_ENABLED_ENV = "MINER_SEED_LOW_LIQUIDITY_COOLDOWN_ENABLED"
_LOW_LIQUIDITY_COOLDOWN_DAYS_ZERO_ENV = "MINER_SEED_LOW_LIQUIDITY_COOLDOWN_DAYS_ZERO"
_LOW_LIQUIDITY_COOLDOWN_DAYS_LOW_ENV = "MINER_SEED_LOW_LIQUIDITY_COOLDOWN_DAYS_LOW"


@contextmanager
def _temporary_env(overrides: Dict[str, str]):
    if not overrides:
        yield
        return
    prev: Dict[str, Optional[str]] = {}
    try:
        for key, value in overrides.items():
            prev[key] = os.getenv(key)
            os.environ[key] = str(value)
        yield
    finally:
        for key, old in prev.items():
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old


def _seed_key(seed_query: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(seed_query or "").upper())


def _seed_pool_key(seed_query: str) -> str:
    normalized = _normalize_seed_query(seed_query)
    if not normalized:
        return ""
    gtins = _extract_gtin_candidates(normalized)
    if len(gtins) == 1:
        return _seed_key(gtins[0])
    codes: List[str] = []
    seen_codes: Set[str] = set()
    for raw in _extract_codes(normalized):
        text = str(raw or "").strip()
        canon = _seed_key(text)
        if len(canon) < 4 or canon in seen_codes:
            continue
        seen_codes.add(canon)
        codes.append(text)
    if len(codes) == 1 and _looks_specific_seed(codes[0]):
        return _seed_key(codes[0])
    return _seed_key(normalized)


def _seed_token_norm(token: str) -> str:
    return re.sub(
        r"[^A-Z0-9\u3040-\u30FF\u3400-\u9FFF]+",
        "",
        unicodedata.normalize("NFKC", str(token or "")).upper(),
    )


def _normalize_seed_query(seed_query: str) -> str:
    compact = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(seed_query or ""))).strip()
    if not compact:
        return ""
    tokens_raw = [v for v in compact.split(" ") if str(v or "").strip()]
    tokens: List[str] = []
    token_norms: List[str] = []
    for raw in tokens_raw:
        cleaned = re.sub(r"^[^0-9A-Z\u3040-\u30FF\u3400-\u9FFF-]+", "", str(raw or "").strip(), flags=re.IGNORECASE)
        cleaned = re.sub(r"[^0-9A-Z\u3040-\u30FF\u3400-\u9FFF-]+$", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        if not cleaned:
            continue
        norm = _seed_token_norm(cleaned)
        if not norm:
            continue
        tokens.append(cleaned)
        token_norms.append(norm)
    if not tokens:
        return ""

    # 末尾の状態語を削る（例: "G-SHOCK New"）。
    while tokens:
        removed = False
        for phrase in _SEED_TRAILING_CONDITION_PHRASES:
            n = len(phrase)
            if len(token_norms) >= n and tuple(token_norms[-n:]) == phrase:
                del tokens[-n:]
                del token_norms[-n:]
                removed = True
                break
        if removed:
            continue
        if token_norms and token_norms[-1] in _SEED_TRAILING_CONDITION_WORDS:
            tokens.pop()
            token_norms.pop()
            continue
        break

    # 先頭の状態語（ただし New Balance は維持）を削る。
    while token_norms and token_norms[0] in _SEED_LEADING_CONDITION_WORDS:
        tokens.pop(0)
        token_norms.pop(0)
    if len(token_norms) >= 2 and token_norms[0] == "NEW" and token_norms[1] != "BALANCE":
        tokens.pop(0)
        token_norms.pop(0)

    if not tokens:
        return ""

    # 中間に混ざった明確な状態語を除去（New Balance は維持）。
    pruned_tokens: List[str] = []
    pruned_norms: List[str] = []
    for idx, token in enumerate(tokens):
        norm = token_norms[idx]
        if norm in _SEED_MIDDLE_DROP_WORDS:
            continue
        if norm == "NEW":
            prev_norm = pruned_norms[-1] if pruned_norms else ""
            next_norm = token_norms[idx + 1] if idx + 1 < len(token_norms) else ""
            if prev_norm != "BALANCE" and next_norm != "BALANCE":
                continue
        pruned_tokens.append(token)
        pruned_norms.append(norm)

    if not pruned_tokens:
        return ""
    normalized = re.sub(r"\s+", " ", " ".join(pruned_tokens)).strip()
    if len(_seed_key(normalized)) < 4:
        return ""
    return normalized


def _normalize_category_key(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(text or "")).strip().lower()
    normalized = normalized.replace("-", "_").replace(" ", "_")
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


_BIG_WORD_CONDITION_WORDS: Set[str] = {
    "NEW",
    "NIB",
    "UNUSED",
    "USED",
    "PREOWNED",
    "PRE-OWNED",
    "OPENBOX",
    "OPEN-BOX",
    "JUNK",
    "REFURBISHED",
    "RENEWED",
    "新品",
    "未使用",
    "中古",
    "ジャンク",
    "訳あり",
}

_BIG_WORD_DROP_WORDS: Set[str] = {
    "NEW",
    "NIB",
    "UNUSED",
    "新品",
    "未使用",
}

_BIG_WORD_TRAILING_CONDITION_PHRASES: Tuple[Tuple[str, ...], ...] = (
    ("FOR", "PARTS"),
    ("PARTS", "ONLY"),
    ("OPEN", "BOX"),
    ("LIKE", "NEW"),
)


def _big_word_token_norm(token: str) -> str:
    return re.sub(
        r"[^A-Z0-9\u3040-\u30FF\u3400-\u9FFF]+",
        "",
        unicodedata.normalize("NFKC", str(token or "")).upper(),
    )


def _normalize_big_word(text: str) -> str:
    compact = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(text or ""))).strip()
    if not compact:
        return ""
    raw_tokens = [v for v in compact.split(" ") if str(v or "").strip()]
    tokens: List[str] = []
    token_norms: List[str] = []
    for raw in raw_tokens:
        cleaned = re.sub(r"^[^0-9A-Z\u3040-\u30FF\u3400-\u9FFF-]+", "", str(raw or "").strip(), flags=re.IGNORECASE)
        cleaned = re.sub(r"[^0-9A-Z\u3040-\u30FF\u3400-\u9FFF-]+$", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        if not cleaned:
            continue
        norm = _big_word_token_norm(cleaned)
        if not norm:
            continue
        tokens.append(cleaned)
        token_norms.append(norm)

    if not tokens:
        return ""

    # 末尾の状態語・状態フレーズは除去。
    while tokens:
        removed = False
        for phrase in _BIG_WORD_TRAILING_CONDITION_PHRASES:
            n = len(phrase)
            if len(token_norms) >= n and tuple(token_norms[-n:]) == phrase:
                del tokens[-n:]
                del token_norms[-n:]
                removed = True
                break
        if removed:
            continue
        if token_norms and token_norms[-1] in _BIG_WORD_CONDITION_WORDS:
            tokens.pop()
            token_norms.pop()
            continue
        break

    # 先頭の状態語を除去（New Balanceだけは温存）。
    while token_norms and token_norms[0] in _BIG_WORD_CONDITION_WORDS:
        if token_norms[0] == "NEW" and len(token_norms) >= 2 and token_norms[1] == "BALANCE":
            break
        tokens.pop(0)
        token_norms.pop(0)

    # 中間の "NEW" 等は基本ノイズなので落とす（New Balanceのみ温存）。
    pruned_tokens: List[str] = []
    pruned_norms: List[str] = []
    for idx, token in enumerate(tokens):
        norm = token_norms[idx]
        if norm in _BIG_WORD_DROP_WORDS:
            prev_norm = pruned_norms[-1] if pruned_norms else ""
            next_norm = token_norms[idx + 1] if idx + 1 < len(token_norms) else ""
            if not (norm == "NEW" and (prev_norm == "BALANCE" or next_norm == "BALANCE")):
                continue
        pruned_tokens.append(token)
        pruned_norms.append(norm)

    if not pruned_tokens:
        return ""
    normalized = re.sub(r"\s+", " ", " ".join(pruned_tokens)).strip()
    return normalized


def _big_word_dedupe_key(text: str) -> str:
    return re.sub(
        r"[^A-Z0-9\u3040-\u30FF\u3400-\u9FFF]+",
        "",
        unicodedata.normalize("NFKC", str(text or "")).upper(),
    )


def _resolve_category(category_input: str) -> Tuple[str, str, Dict[str, Any]]:
    row = _match_category_row(str(category_input or ""))
    if isinstance(row, dict):
        key = _normalize_category_key(str(row.get("category_key", "") or ""))
        label = str(row.get("display_name_ja", "") or "").strip() or key
        return key or _normalize_category_key(category_input), label, row
    key = _normalize_category_key(category_input)
    label = _TARGET_LABELS.get(key, key or "カテゴリ")
    return key, label, {}


def _category_seed_min_sold_price_usd(category_key: str, category_row: Dict[str, Any]) -> float:
    key = _normalize_category_key(category_key)
    env_suffix = re.sub(r"[^A-Z0-9_]+", "_", key.upper())
    env_value = to_float(os.getenv(f"MINER_SEED_POOL_MIN_SOLD_PRICE_USD_{env_suffix}", ""), -1.0)
    if env_value > 0:
        return float(env_value)

    if isinstance(category_row, dict):
        for value_key in (
            "seed_min_sold_price_usd",
            "min_sold_price_usd",
            "seed_min_price_usd",
            "min_price_usd",
        ):
            value = to_float(category_row.get(value_key), -1.0)
            if value > 0:
                return float(value)

    default_value = to_float(_CATEGORY_SEED_MIN_SOLD_PRICE_USD_DEFAULTS.get(key), -1.0)
    if default_value > 0:
        return float(default_value)

    return max(0.0, to_float(os.getenv("MINER_SEED_POOL_MIN_SOLD_PRICE_USD_DEFAULT", ""), 0.0))


def _category_stage_c_min_sold_90d(category_key: str, category_row: Dict[str, Any]) -> int:
    key = _normalize_category_key(category_key)
    env_suffix = re.sub(r"[^A-Z0-9_]+", "_", key.upper())
    env_value = env_int(f"MINER_STAGE_C_MIN_SOLD_90D_{env_suffix}", -1)
    if env_value >= 0:
        return int(env_value)

    if isinstance(category_row, dict):
        for value_key in (
            "stage_c_min_sold_90d",
            "liquidity_min_sold_90d",
            "min_sold_90d_count",
        ):
            value = to_int(category_row.get(value_key), -1)
            if value >= 0:
                return int(value)

    default_value = to_int(_CATEGORY_STAGE_C_MIN_SOLD_90D_DEFAULTS.get(key), -1)
    if default_value >= 0:
        return int(default_value)

    return max(0, env_int("LIQUIDITY_MIN_SOLD_90D", 10))


def _resolve_ebay_pr_category_filter(category_key: str, category_row: Dict[str, Any]) -> Dict[str, Any]:
    key = _normalize_category_key(category_key)
    defaults = _EBAY_PR_CATEGORY_DEFAULTS.get(key, {})
    default_id = max(0, to_int(defaults.get("id"), 0))
    default_slug = str(defaults.get("slug", "") or "").strip().lower()

    row_id = 0
    row_slug = ""
    if isinstance(category_row, dict) and category_row:
        row_id = max(0, to_int(category_row.get("ebay_pr_category_id"), 0))
        row_slug = str(category_row.get("ebay_pr_category_slug", "") or "").strip().lower()
        if (not row_id) or (not row_slug):
            extra = category_row.get("ebay_pr") if isinstance(category_row.get("ebay_pr"), dict) else {}
            if isinstance(extra, dict):
                row_id = row_id or max(0, to_int(extra.get("category_id"), 0))
                row_slug = row_slug or str(extra.get("category_slug", "") or "").strip().lower()

    env_suffix = re.sub(r"[^A-Z0-9_]+", "_", key.upper())
    env_id = max(0, env_int(f"MINER_EBAY_PR_CATEGORY_ID_{env_suffix}", 0))
    env_slug = str((os.getenv(f"MINER_EBAY_PR_CATEGORY_SLUG_{env_suffix}", "") or "")).strip().lower()

    category_id = env_id or row_id or default_id
    category_slug = env_slug or row_slug or default_slug
    category_slug = re.sub(r"[^a-z0-9_-]+", "", category_slug)
    enabled = bool(category_id > 0)
    return {
        "enabled": enabled,
        "category_id": int(category_id) if enabled else 0,
        "category_slug": category_slug if enabled else "",
    }


def _category_big_words(category_key: str, category_row: Dict[str, Any]) -> List[str]:
    """
    Load ordered "big word" used in seed補充A.
    This function does not perform web search; it only reads locally prepared
    category knowledge.
    Priority:
      1) phase_a_big_words
      2) seed_series
      3) aliases
      4) category key fallback
    """
    candidates: List[str] = []
    if isinstance(category_row, dict) and category_row:
        phase_a_values = category_row.get("phase_a_big_words", [])
        if isinstance(phase_a_values, list) and phase_a_values:
            for raw in phase_a_values:
                text = str(raw or "").strip()
                if text:
                    candidates.append(text)
        else:
            for key in ("seed_series", "aliases"):
                values = category_row.get(key, [])
                if not isinstance(values, list):
                    continue
                for raw in values:
                    text = str(raw or "").strip()
                    if text:
                        candidates.append(text)
    if category_key and not candidates:
        candidates.append(category_key.replace("_", " "))
    if not candidates:
        candidates.append("watch")

    out: List[str] = []
    seen: Set[str] = set()
    for raw in candidates:
        text = _normalize_big_word(str(raw or ""))
        if not text:
            text = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(raw or ""))).strip()
        if not text:
            continue
        key = _big_word_dedupe_key(text)
        if not key:
            key = text.upper()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out or ["watch"]


def _category_search_query(category_key: str, category_row: Dict[str, Any]) -> str:
    # Backward-compatible helper for legacy places that expect a single query.
    return _category_big_words(category_key, category_row)[0]


def _brand_hints(category_row: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    seeds = category_row.get("seed_brands", []) if isinstance(category_row, dict) else []
    if not isinstance(seeds, list):
        return out
    for raw in seeds:
        text = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(raw or ""))).strip()
        if text:
            out.append(text)
    return out


def _pick_brand(title: str, brand_hints: Sequence[str]) -> str:
    upper = unicodedata.normalize("NFKC", str(title or "")).upper()
    for raw in brand_hints:
        text = str(raw or "").strip()
        if not text:
            continue
        if text.upper() in upper:
            return text
    return ""


def _fallback_seed_phrases(title: str) -> List[str]:
    upper = unicodedata.normalize("NFKC", str(title or "")).upper()
    tokens = re.findall(r"[A-Z0-9][A-Z0-9-]{2,}", upper)
    cleaned: List[str] = []
    for token in tokens:
        normalized = token.strip("-")
        if len(normalized) < 3:
            continue
        if normalized in _SEED_STOPWORDS:
            continue
        if _is_way_noise_token(normalized):
            continue
        if normalized.isdigit():
            continue
        cleaned.append(normalized)
        if len(cleaned) >= 6:
            break
    out: List[str] = []
    if len(cleaned) >= 2:
        second = str(cleaned[1] or "").strip().upper()
        second_key = re.sub(r"[^A-Z0-9]+", "", second)
        if (
            second
            and second not in _SEED_STOPWORDS
            and not _is_way_noise_token(second)
            and second_key not in _SEED_FALLBACK_BROAD_SERIES_KEYS
        ):
            out.append(f"{cleaned[0]} {cleaned[1]}")
    if cleaned:
        first = str(cleaned[0] or "").strip().upper()
        if not _is_way_noise_token(first) and (len(cleaned) == 1 or any(ch.isdigit() for ch in first)):
            out.append(cleaned[0])
    return out


def _is_valid_gtin(text: str) -> bool:
    digits = re.sub(r"\D+", "", str(text or ""))
    if len(digits) not in {8, 12, 13, 14}:
        return False
    if not digits.isdigit():
        return False
    nums = [int(ch) for ch in digits]
    check = nums[-1]
    body = nums[:-1]
    total = 0
    rev = list(reversed(body))
    for idx, num in enumerate(rev):
        total += num * (3 if idx % 2 == 0 else 1)
    calc = (10 - (total % 10)) % 10
    return calc == check


def _extract_gtin_candidates(text: str) -> List[str]:
    upper = unicodedata.normalize("NFKC", str(text or "")).upper()
    out: List[str] = []
    seen: Set[str] = set()
    for token in re.findall(r"(?<!\d)\d{8,14}(?!\d)", upper):
        digits = str(token or "").strip()
        if not digits:
            continue
        if not _is_valid_gtin(digits):
            continue
        if digits in seen:
            continue
        seen.add(digits)
        out.append(digits)
    return out


def _extract_seed_queries_from_title(
    title: str,
    brand_hints: Sequence[str],
    *,
    prefer_strict_model_seed: bool = False,
) -> List[str]:
    compact = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(title or ""))).strip()
    if not compact:
        return []
    if _is_seed_ui_noise_title(compact):
        return []
    brand = _pick_brand(compact, brand_hints)
    out: List[str] = []
    gtins = _extract_gtin_candidates(compact)
    for gtin in gtins[:4]:
        if brand:
            out.append(f"{brand} {gtin}")
        out.append(gtin)
    for code in _extract_codes(compact)[:8]:
        text = str(code or "").strip().upper()
        if len(text) < 4:
            continue
        if text in _SEED_STOPWORDS:
            continue
        # 型番seedは桁情報を含むものを優先する（語だけの汎用語を抑制）。
        if not any(ch.isdigit() for ch in text):
            continue
        if brand:
            out.append(f"{brand} {text}")
        out.append(text)
    if not out:
        out.extend(_fallback_seed_phrases(compact))
    dedup: List[str] = []
    seen: Set[str] = set()
    for raw in out:
        text = _normalize_seed_query(raw)
        key = _seed_key(text)
        if len(key) < 4:
            continue
        if key in seen:
            continue
        seen.add(key)
        dedup.append(text)
    # 具体的な型番seedがある場合は、広すぎるseedを落として精度を上げる。
    has_specific = any(_looks_specific_seed(v) for v in dedup)
    if has_specific:
        narrowed = [
            v
            for v in dedup
            if _looks_specific_seed(v) or bool(_extract_gtin_candidates(v))
        ]
        if narrowed:
            dedup = narrowed
    if bool(prefer_strict_model_seed):
        dedup = [v for v in dedup if _seed_query_is_model_or_gtin(v)]
    return dedup


def _looks_specific_seed(seed_query: str) -> bool:
    text = str(seed_query or "").strip().upper()
    if not text:
        return False
    codes = _extract_codes(text)
    if not codes:
        return False
    for code in codes:
        token = str(code or "").strip().upper()
        alpha = sum(1 for ch in token if "A" <= ch <= "Z")
        digit = sum(1 for ch in token if ch.isdigit())
        if alpha >= 2 and digit >= 2 and len(token) >= 6:
            return True
    return False


def _seed_query_is_model_or_gtin(seed_query: str) -> bool:
    text = str(seed_query or "").strip()
    if not text:
        return False
    return bool(_looks_specific_seed(text) or _extract_gtin_candidates(text))


def _seed_candidates_have_model_or_gtin(seed_candidates: Sequence[str]) -> bool:
    return any(_seed_query_is_model_or_gtin(v) for v in seed_candidates)


def _seed_stage1_zero_hit_count(metadata: Dict[str, Any]) -> int:
    if not isinstance(metadata, dict):
        return 0
    return max(0, to_int(metadata.get("stage1_zero_hit_count"), 0))


def _to_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "off", "disabled"}:
        return False
    return None


def _category_requires_strict_model_seed(category_key: str, category_row: Dict[str, Any]) -> bool:
    row_bool = _to_bool((category_row or {}).get("seed_strict_model_only"))
    if row_bool is not None:
        return bool(row_bool)
    if _normalize_category_key(category_key) == "watch":
        return env_bool("MINER_SEED_STRICT_MODEL_ONLY_WATCH", True)
    return env_bool("MINER_SEED_STRICT_MODEL_ONLY", False)


def _ebay_item_id_from_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    m = re.search(r"/itm/(?:[^/?#]+/)?([0-9]{9,15})", raw)
    if not m:
        return ""
    item_num = str(m.group(1) or "").strip()
    if not item_num:
        return ""
    return f"v1|{item_num}|0"


def _is_seed_ui_noise_title(title: str) -> bool:
    text = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(title or ""))).strip()
    if not text:
        return False
    upper = text.upper()
    key = re.sub(r"[^A-Z0-9]+", "", upper)
    if not key:
        return False
    if key in _SEED_UI_NOISE_KEYS:
        return True
    # eBay UI文言混入の既知パターン
    if "CANTFIND" in key and "SEARCHWITHANIMAGE" in key:
        return True
    # 計測/ハンドラ名のような疑似タイトル
    if not re.search(r"\d", upper):
        if "_" in upper and any(token in upper for token in ("TRACKING", "HANDLER", "FINGER", "SHIP", "LOCATION")):
            return True
        if re.fullmatch(r"[A-Z _-]{8,}", upper) and any(
            token in upper for token in ("TRACKING", "HANDLER", "VISUAL", "FINGER", "LOCATION")
        ):
            return True
    return False


def _load_seed_api_usage() -> Dict[str, Any]:
    if not _SEED_API_USAGE_PATH.exists():
        return {}
    try:
        return json.loads(_SEED_API_USAGE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_seed_api_usage(payload: Dict[str, Any]) -> None:
    try:
        _SEED_API_USAGE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _SEED_API_USAGE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _consume_seed_api_budget(*, now_ts: int, state: Dict[str, Any]) -> Tuple[bool, str]:
    if not env_bool("MINER_SEED_API_SUPPLEMENT_ENABLED", True):
        return False, "disabled"
    if not (os.getenv("EBAY_CLIENT_ID", "") or "").strip():
        return False, "missing_client_id"
    if not (os.getenv("EBAY_CLIENT_SECRET", "") or "").strip():
        return False, "missing_client_secret"

    per_run_budget = max(0, env_int("MINER_SEED_API_SUPPLEMENT_PER_RUN_BUDGET", 40))
    if per_run_budget <= 0:
        return False, "run_budget_zero"
    used_run = max(0, to_int(state.get("run_api_calls"), 0))
    if used_run >= per_run_budget:
        return False, "run_budget_exhausted"

    daily_budget = max(0, env_int("MINER_SEED_API_SUPPLEMENT_DAILY_BUDGET", 300))
    hourly_budget = max(0, env_int("MINER_SEED_API_SUPPLEMENT_HOURLY_BUDGET", 80))
    usage = state.get("usage_cache")
    if not isinstance(usage, dict):
        usage = _load_seed_api_usage()
    days = usage.get("days", {})
    if not isinstance(days, dict):
        days = {}
    day_key = time.strftime("%Y-%m-%d", time.gmtime(now_ts))
    day_row = days.get(day_key, {})
    if not isinstance(day_row, dict):
        day_row = {}
    day_count = max(0, to_int(day_row.get("count"), 0))
    events = day_row.get("events", [])
    if not isinstance(events, list):
        events = []
    cutoff = now_ts - 3600
    events = [max(0, to_int(ts, 0)) for ts in events if max(0, to_int(ts, 0)) >= cutoff]

    if daily_budget > 0 and day_count >= daily_budget:
        state["usage_cache"] = usage
        return False, "daily_budget_exhausted"
    if hourly_budget > 0 and len(events) >= hourly_budget:
        state["usage_cache"] = usage
        return False, "hourly_budget_exhausted"

    day_row["count"] = day_count + 1
    events.append(now_ts)
    day_row["events"] = events[-2000:]
    days[day_key] = day_row
    valid_keys = sorted([str(k) for k in days.keys() if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(k))])
    keep = set(valid_keys[-45:])
    for key in list(days.keys()):
        if str(key) not in keep:
            days.pop(key, None)
    usage["days"] = days
    usage["updated_at"] = int(now_ts)
    _save_seed_api_usage(usage)
    state["usage_cache"] = usage
    state["run_api_calls"] = used_run + 1
    return True, "ok"


def _extract_text_values(node: Any) -> List[str]:
    if isinstance(node, str):
        text = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", node)).strip()
        return [text] if text else []
    if isinstance(node, list):
        out: List[str] = []
        for row in node[:20]:
            out.extend(_extract_text_values(row))
        return out
    if isinstance(node, dict):
        out: List[str] = []
        for key in ("value", "values", "localizedValue", "localizedValues", "name", "localizedName"):
            if key in node:
                out.extend(_extract_text_values(node.get(key)))
        return out
    return []


def _api_seed_candidates_from_payload(
    *,
    payload: Dict[str, Any],
    brand_hints: Sequence[str],
) -> List[str]:
    title = re.sub(r"\s+", " ", str(payload.get("title", "") or "").strip())
    if not title:
        title = re.sub(r"\s+", " ", str(payload.get("shortDescription", "") or "").strip())
    if _is_accessory_title(title):
        return []

    out: List[str] = []
    brand = str(payload.get("brand", "") or "").strip()
    if not brand:
        brand = _pick_brand(title, brand_hints)
    title_codes = _extract_codes(title)
    for code in title_codes[:6]:
        token = str(code or "").strip().upper()
        if not token:
            continue
        if brand:
            out.append(f"{brand} {token}")
        out.append(token)

    aspects = payload.get("localizedAspects")
    if isinstance(aspects, list):
        for row in aspects[:25]:
            if not isinstance(row, dict):
                continue
            name = " ".join(_extract_text_values(row.get("name")) or _extract_text_values(row.get("localizedName")))
            values = _extract_text_values(row.get("value")) + _extract_text_values(row.get("values"))
            values += _extract_text_values(row.get("localizedValue")) + _extract_text_values(row.get("localizedValues"))
            name_norm = str(name or "").lower()
            if not values:
                continue
            is_model_aspect = any(tag in name_norm for tag in ("model", "mpn", "part", "型番", "品番"))
            is_gtin_aspect = any(tag in name_norm for tag in ("gtin", "upc", "ean", "jan"))
            for value in values:
                if is_gtin_aspect:
                    for gtin in _extract_gtin_candidates(value)[:4]:
                        if brand:
                            out.append(f"{brand} {gtin}")
                        out.append(gtin)
                    continue
                if is_model_aspect:
                    for code in _extract_codes(value)[:4]:
                        token = str(code or "").strip().upper()
                        if not token:
                            continue
                        if brand:
                            out.append(f"{brand} {token}")
                        out.append(token)

    dedup: List[str] = []
    seen: Set[str] = set()
    for raw in out:
        text = _normalize_seed_query(raw)
        if not text:
            continue
        key = _seed_key(text)
        if len(key) < 4:
            continue
        if key in seen:
            continue
        seen.add(key)
        dedup.append(text)
    has_specific = any(_looks_specific_seed(v) for v in dedup)
    if has_specific:
        narrowed = [
            v
            for v in dedup
            if _looks_specific_seed(v) or bool(_extract_gtin_candidates(v))
        ]
        if narrowed:
            dedup = narrowed
    return dedup


def _api_seed_candidates_from_item_url(
    *,
    item_url: str,
    brand_hints: Sequence[str],
    timeout: int,
    state: Dict[str, Any],
) -> Tuple[List[str], str]:
    now_ts = int(time.time())
    allowed, reason = _consume_seed_api_budget(now_ts=now_ts, state=state)
    if not allowed:
        return [], reason

    item_id = _ebay_item_id_from_url(item_url)
    if not item_id:
        return [], "missing_item_id"

    token = _ebay_access_token(max(5, int(timeout)))
    marketplace = (os.getenv("TARGET_MARKETPLACE", "EBAY_US") or "EBAY_US").strip() or "EBAY_US"
    status, _headers, payload = _request_with_retry(
        f"https://api.ebay.com/buy/browse/v1/item/{urllib.parse.quote(item_id, safe='')}",
        headers={
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": marketplace,
        },
        timeout=max(5, int(timeout)),
        site="ebay",
    )
    if status != 200 or not isinstance(payload, dict):
        return [], f"http_{status if status else 'error'}"
    return _api_seed_candidates_from_payload(payload=payload, brand_hints=brand_hints), "ok"

def _build_bootstrap_seed_rows(
    *,
    category_key: str,
    category_label: str,
    category_row: Dict[str, Any],
    existing_keys: Set[str],
    max_rows: int,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if max_rows <= 0:
        return out
    candidates: List[str] = []
    if isinstance(category_row, dict):
        try:
            q_ebay, _ = _build_category_seed_queries(category_row=category_row, site="ebay")
            candidates.extend([str(v or "").strip() for v in q_ebay])
        except Exception:
            pass
        for key in ("model_examples", "seed_series", "seed_brands", "aliases"):
            rows = category_row.get(key, [])
            if not isinstance(rows, list):
                continue
            for raw in rows:
                text = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(raw or ""))).strip()
                if text:
                    candidates.append(text)
    if category_key:
        candidates.append(category_key.replace("_", " "))
    if category_label:
        candidates.append(category_label)

    scored_candidates: List[Tuple[int, int, str]] = []
    for idx, raw in enumerate(candidates):
        text = re.sub(r"\s+", " ", str(raw or "").strip())
        if not text:
            continue
        score = 0
        if _extract_codes(text):
            score += 220
        if re.search(r"\d", text):
            score += 90
        token_count = len([v for v in text.split(" ") if v.strip()])
        if token_count >= 2:
            score += 25
        if token_count <= 4:
            score += 10
        scored_candidates.append((score, idx, text))

    seen: Set[str] = set()
    rank = 1
    for _score, _idx, text in sorted(scored_candidates, key=lambda t: (-t[0], t[1])):
        seed_query = _normalize_seed_query(text)
        if not seed_query:
            continue
        skey = _seed_key(seed_query)
        if len(skey) < 4:
            continue
        if skey in seen or skey in existing_keys:
            continue
        seen.add(skey)
        existing_keys.add(skey)
        out.append(
            {
                "seed_query": seed_query,
                "source_title": f"bootstrap:{category_label or category_key}",
                "source_item_url": "",
                "source_page": 0,
                "source_offset": 0,
                "source_rank": rank,
                "metadata": {
                    "source": "category_knowledge",
                    "category_key": category_key,
                    "category_label": category_label,
                },
            }
        )
        rank += 1
        if len(out) >= max_rows:
            break
    return out


def _append_seed_run_journal(row: Dict[str, Any]) -> None:
    if not isinstance(row, dict):
        return
    try:
        _SEED_RUN_JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _SEED_RUN_JOURNAL_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False))
            f.write("\n")
    except Exception:
        pass


def _seed_refill_trace_path() -> Optional[Path]:
    if not env_bool("MINER_SEED_POOL_TRACE_ENABLED", False):
        return None
    raw = str((os.getenv("MINER_SEED_POOL_TRACE_PATH", "") or "")).strip()
    if raw:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (ROOT_DIR / path).resolve()
        return path
    return _SEED_REFILL_TRACE_DIR / f"seed_refill_trace_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.jsonl"


def _append_seed_refill_trace(path: Optional[Path], row: Dict[str, Any]) -> None:
    if path is None or not isinstance(row, dict):
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False))
            f.write("\n")
    except Exception:
        pass


def _prune_seed_run_journal_by_category(*, category_key: str) -> int:
    if not _SEED_RUN_JOURNAL_PATH.exists():
        return 0
    removed = 0
    kept_lines: List[str] = []
    try:
        for raw in _SEED_RUN_JOURNAL_PATH.read_text(encoding="utf-8").splitlines():
            line = str(raw or "").strip()
            if not line:
                continue
            row: Dict[str, Any] = {}
            try:
                parsed = json.loads(line)
                if isinstance(parsed, dict):
                    row = parsed
            except Exception:
                row = {}
            row_category_key = _normalize_category_key(str(row.get("category_key", "") or ""))
            if row_category_key and row_category_key == category_key:
                removed += 1
                continue
            kept_lines.append(line)
        if kept_lines:
            _SEED_RUN_JOURNAL_PATH.write_text("\n".join(kept_lines) + "\n", encoding="utf-8")
        else:
            _SEED_RUN_JOURNAL_PATH.unlink(missing_ok=True)
    except Exception:
        return 0
    return int(removed)


def _run_rpa_page(
    *,
    query: str,
    offset: int,
    limit: int,
    category_id: int = 0,
    category_slug: str = "",
    min_price_usd: float = 0.0,
) -> Dict[str, Any]:
    started_perf = time.perf_counter()
    started_at = utc_iso()
    script_path = ROOT_DIR / "scripts" / "rpa_market_research.py"
    if not script_path.exists():
        return {"ok": False, "reason": "rpa_script_missing", "rows": [], "returncode": -1}
    data_dir = ROOT_DIR / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    temp_file = tempfile.NamedTemporaryFile(
        prefix="miner_seed_page_",
        suffix=".jsonl",
        dir=str(data_dir),
        delete=False,
    )
    output_path = Path(temp_file.name)
    temp_file.close()
    python_exec = str(ROOT_DIR / ".venv" / "bin" / "python")
    if not Path(python_exec).exists():
        python_exec = os.getenv("PYTHON", "") or "python3"
    wait_seconds = max(2, env_int("MINER_SEED_POOL_RPA_WAIT_SECONDS", env_int("LIQUIDITY_RPA_WAIT_SECONDS", 8)))
    lookback_days = max(7, env_int("LIQUIDITY_RPA_LOOKBACK_DAYS", 90))
    condition_mode = str((os.getenv("LIQUIDITY_RPA_PRIMARY_CONDITION", "new") or "new").strip() or "new")
    sold_sort_mode = str((os.getenv("LIQUIDITY_RPA_PRIMARY_SOLD_SORT", "recently_sold") or "recently_sold").strip() or "recently_sold")
    strict_condition = env_bool("LIQUIDITY_RPA_PRIMARY_STRICT_CONDITION", True)
    fixed_price_only = env_bool("LIQUIDITY_RPA_PRIMARY_FIXED_PRICE_ONLY", True)
    force_headless = env_bool("LIQUIDITY_RPA_FORCE_HEADLESS", True)
    profile_dir_raw = str((os.getenv("LIQUIDITY_RPA_PROFILE_DIR", "") or "").strip())
    profile_dir_path = Path(profile_dir_raw).expanduser() if profile_dir_raw else (ROOT_DIR / "data" / "rpa" / "ebay_profile")
    if not profile_dir_path.is_absolute():
        profile_dir_path = (ROOT_DIR / profile_dir_path).resolve()
    login_marker_path = profile_dir_path / ".login_ready"
    visual_login_default_pause = max(30, env_int("LIQUIDITY_RPA_VISUAL_LOGIN_PAUSE_SECONDS", 180))
    pause_for_login_raw = str((os.getenv("LIQUIDITY_RPA_PAUSE_FOR_LOGIN_SECONDS", "") or "").strip())
    if force_headless:
        pause_for_login_sec = 0
    elif pause_for_login_raw:
        parsed_pause = max(0, env_int("LIQUIDITY_RPA_PAUSE_FOR_LOGIN_SECONDS", 0))
        if parsed_pause > 0:
            pause_for_login_sec = parsed_pause
        elif login_marker_path.exists():
            pause_for_login_sec = 0
        else:
            # 0指定でも初回可視実行は最低限の手動ログイン時間を確保する。
            pause_for_login_sec = visual_login_default_pause
    elif login_marker_path.exists():
        pause_for_login_sec = 0
    else:
        pause_for_login_sec = visual_login_default_pause

    cmd: List[str] = [
        python_exec,
        str(script_path),
        "--query",
        str(query),
        "--output",
        str(output_path),
        "--pause-for-login",
        str(max(0, int(pause_for_login_sec))),
        "--wait-seconds",
        str(wait_seconds),
        "--lookback-days",
        str(lookback_days),
        "--condition",
        condition_mode,
        "--sold-sort",
        sold_sort_mode,
        "--pass-label",
        "seed_pool_refill",
        "--result-offset",
        str(max(0, int(offset))),
        "--result-limit",
        str(max(10, min(200, int(limit)))),
    ]
    screenshot_template_raw = str(
        (
            os.getenv("MINER_SEED_POOL_RPA_SCREENSHOT_TEMPLATE", "")
            or os.getenv("LIQUIDITY_RPA_SCREENSHOT_AFTER_FILTERS", "")
            or ""
        ).strip()
    )
    screenshot_template = ""
    if screenshot_template_raw:
        screenshot_template = screenshot_template_raw.replace("{offset}", str(max(0, int(offset))))
        cmd.extend(["--screenshot-after-filters", screenshot_template])
    safe_category_id = max(0, int(category_id))
    safe_category_slug = re.sub(r"[^a-z0-9_-]+", "", str(category_slug or "").strip().lower())
    if safe_category_id > 0:
        cmd.extend(["--category-id", str(safe_category_id)])
    if safe_category_slug:
        cmd.extend(["--category-slug", safe_category_slug])
    safe_min_price = max(0.0, to_float(min_price_usd, 0.0))
    if safe_min_price > 0:
        cmd.extend(["--min-price-usd", str(round(safe_min_price, 2))])
    if strict_condition:
        cmd.append("--strict-condition")
    if fixed_price_only:
        cmd.append("--fixed-price-only")
    if force_headless:
        cmd.append("--headless")
    cmd.extend(["--profile-dir", str(profile_dir_path)])
    login_url = str((os.getenv("LIQUIDITY_RPA_LOGIN_URL", "") or "").strip())
    if login_url:
        cmd.extend(["--login-url", login_url])
    timeout_sec = max(
        15,
        env_int(
            "MINER_SEED_POOL_RPA_FETCH_TIMEOUT_SECONDS",
            min(env_int("LIQUIDITY_RPA_FETCH_TIMEOUT_SECONDS", 45), max(18, (wait_seconds * 4) + 8)),
        ),
    )
    if (not force_headless) and pause_for_login_sec > 0:
        timeout_sec = max(timeout_sec, int(pause_for_login_sec) + max(90, (wait_seconds * 6)))
    child_env = os.environ.copy()
    # seed補充Aは大量に回るので、RPAの待機/遷移設定を軽量化して全体時間を短縮する。
    child_env["LIQUIDITY_RPA_ACTION_TIMEOUT_MS"] = str(
        max(1800, env_int("MINER_SEED_POOL_RPA_ACTION_TIMEOUT_MS", env_int("LIQUIDITY_RPA_ACTION_TIMEOUT_MS", 3200)))
    )
    child_env["LIQUIDITY_RPA_NAV_TIMEOUT_MS"] = str(
        max(2600, env_int("MINER_SEED_POOL_RPA_NAV_TIMEOUT_MS", env_int("LIQUIDITY_RPA_NAV_TIMEOUT_MS", 7000)))
    )
    child_env["LIQUIDITY_RPA_FILTER_SETTLE_MS"] = str(
        max(15, env_int("MINER_SEED_POOL_RPA_FILTER_SETTLE_MS", env_int("LIQUIDITY_RPA_FILTER_SETTLE_MS", 30)))
    )
    child_env["LIQUIDITY_RPA_READY_POLL_MS"] = str(
        max(50, env_int("MINER_SEED_POOL_RPA_READY_POLL_MS", env_int("LIQUIDITY_RPA_READY_POLL_MS", 90)))
    )
    child_env["LIQUIDITY_RPA_PRE_FILTER_WAIT_SECONDS"] = str(
        max(
            2,
            env_int(
                "MINER_SEED_POOL_RPA_PRE_FILTER_WAIT_SECONDS",
                env_int("LIQUIDITY_RPA_PRE_FILTER_WAIT_SECONDS", 2),
            ),
        )
    )
    child_env["LIQUIDITY_RPA_POST_GOTO_SETTLE_MS"] = str(
        max(
            10,
            env_int(
                "MINER_SEED_POOL_RPA_POST_GOTO_SETTLE_MS",
                env_int("LIQUIDITY_RPA_POST_GOTO_SETTLE_MS", 40),
            ),
        )
    )
    child_env["LIQUIDITY_RPA_GOTO_RETRY_COUNT"] = str(
        max(0, env_int("MINER_SEED_POOL_RPA_GOTO_RETRY_COUNT", env_int("LIQUIDITY_RPA_GOTO_RETRY_COUNT", 1)))
    )
    child_env["LIQUIDITY_RPA_GOTO_RETRY_WAIT_MS"] = str(
        max(
            80,
            env_int(
                "MINER_SEED_POOL_RPA_GOTO_RETRY_WAIT_MS",
                env_int("LIQUIDITY_RPA_GOTO_RETRY_WAIT_MS", 160),
            ),
        )
    )
    # Phase A（seed補充）は条件の取り違えコストが高いので、条件確認をデフォルトで厳格化する。
    child_env["LIQUIDITY_RPA_ENABLE_LOCK_SELECTED_FILTERS"] = str(
        max(0, min(1, env_int("LIQUIDITY_RPA_ENABLE_LOCK_SELECTED_FILTERS", 1)))
    )
    child_env["LIQUIDITY_RPA_REQUIRE_LOCK_SELECTED_FILTERS"] = str(
        max(0, min(1, env_int("LIQUIDITY_RPA_REQUIRE_LOCK_SELECTED_FILTERS", 1)))
    )
    child_env["LIQUIDITY_RPA_REQUIRE_SOLD_SORT"] = str(
        max(0, min(1, env_int("LIQUIDITY_RPA_REQUIRE_SOLD_SORT", 1)))
    )
    if safe_min_price > 0:
        child_env["LIQUIDITY_RPA_ENABLE_MIN_PRICE_FILTER_UI"] = str(
            max(0, min(1, env_int("LIQUIDITY_RPA_ENABLE_MIN_PRICE_FILTER_UI", 1)))
        )
        child_env["LIQUIDITY_RPA_REQUIRE_MIN_PRICE_FILTER"] = str(
            max(0, min(1, env_int("LIQUIDITY_RPA_REQUIRE_MIN_PRICE_FILTER", 1)))
        )
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
            env=child_env,
        )
        return_code = int(proc.returncode)
        stdout = str(proc.stdout or "")
        stderr = str(proc.stderr or "")
    except subprocess.TimeoutExpired:
        return_code = -9
        stdout = ""
        stderr = "timeout"
    if (not force_headless) and pause_for_login_sec > 0 and return_code in {0, 75}:
        try:
            profile_dir_path.mkdir(parents=True, exist_ok=True)
            login_marker_path.write_text(utc_iso(), encoding="utf-8")
        except Exception:
            pass
    rows: List[Dict[str, Any]] = []
    try:
        if output_path.exists():
            for raw in output_path.read_text(encoding="utf-8").splitlines():
                line = str(raw or "").strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
    except Exception:
        rows = []
    finally:
        try:
            output_path.unlink(missing_ok=True)
        except Exception:
            pass
    elapsed_sec = round(max(0.0, time.perf_counter() - started_perf), 4)
    stdout_lower = stdout.lower()
    stderr_lower = stderr.lower()
    daily_limit = bool(
        return_code == 75
        or "requests allowed in one day" in stdout_lower
        or "requests allowed in one day" in stderr_lower
    )
    bot_challenge = bool(
        return_code == 76
        or "pardon our interruption" in stdout_lower
        or "think you were a bot" in stdout_lower
        or "pardon our interruption" in stderr_lower
        or "think you were a bot" in stderr_lower
    )
    if daily_limit:
        reason_text = "daily_limit_reached"
    elif bot_challenge:
        reason_text = "bot_challenge_detected"
    elif return_code == 0:
        reason_text = "ok"
    else:
        reason_text = "rpa_failed"
    return {
        "ok": return_code == 0,
        "daily_limit_reached": daily_limit,
        "bot_challenge_detected": bot_challenge,
        "reason": reason_text,
        "rows": rows,
        "returncode": return_code,
        "category_id": safe_category_id,
        "category_slug": safe_category_slug,
        "pause_for_login_sec": int(pause_for_login_sec),
        "timeout_sec": int(timeout_sec),
        "started_at": started_at,
        "elapsed_sec": float(elapsed_sec),
        "rpa_search_params": {
            "query": str(query),
            "offset": max(0, int(offset)),
            "limit": max(10, min(200, int(limit))),
            "wait_seconds": int(wait_seconds),
            "lookback_days": int(lookback_days),
            "condition": condition_mode,
            "sold_sort": sold_sort_mode,
            "strict_condition": bool(strict_condition),
            "fixed_price_only": bool(fixed_price_only),
            "category_id": int(safe_category_id),
            "category_slug": str(safe_category_slug),
            "min_price_usd": float(round(safe_min_price, 2)),
            "screenshot_after_filters": str(screenshot_template or ""),
            "require_lock_selected_filters": bool(
                to_int(child_env.get("LIQUIDITY_RPA_REQUIRE_LOCK_SELECTED_FILTERS", "0"), 0)
            ),
            "require_sold_sort": bool(to_int(child_env.get("LIQUIDITY_RPA_REQUIRE_SOLD_SORT", "0"), 0)),
            "require_min_price_filter": bool(
                to_int(child_env.get("LIQUIDITY_RPA_REQUIRE_MIN_PRICE_FILTER", "0"), 0)
            ),
        },
        "stdout_tail": stdout.splitlines()[-8:],
        "stderr_tail": stderr.splitlines()[-8:],
    }


def _collect_row_entries(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    entries = meta.get("filtered_result_rows") if isinstance(meta.get("filtered_result_rows"), list) else []
    raw_entries = meta.get("raw_result_rows") if isinstance(meta.get("raw_result_rows"), list) else []
    raw_row_count = max(0, to_int(meta.get("raw_row_count"), 0))
    use_raw_fallback = False
    if not entries and raw_entries and raw_row_count > 0:
        entries = raw_entries
        use_raw_fallback = True
    if not entries and raw_row_count > 0:
        fallback_title = str(row.get("query", "") or "").strip()
        if fallback_title:
            entries = [{"title": fallback_title, "rank": 1, "seed_entry_source": "query_fallback"}]
            use_raw_fallback = True
    sold_90d_count = to_int(row.get("sold_90d_count"), -1)
    sold_price_min = to_float(row.get("sold_price_min"), -1.0)
    out: List[Dict[str, Any]] = []
    for idx, raw in enumerate(entries, start=1):
        if not isinstance(raw, dict):
            continue
        title = str(raw.get("title", "") or "").strip()
        if not title:
            continue
        if _is_seed_ui_noise_title(title):
            continue
        out.append(
            {
                "title": title,
                "item_id": str(raw.get("item_id", "") or "").strip(),
                "item_url": str(raw.get("item_url", "") or "").strip(),
                "image_url": str(raw.get("image_url", "") or "").strip(),
                "sold_price": round(max(0.0, to_float(raw.get("sold_price"), 0.0)), 4),
                "rank": max(1, to_int(raw.get("rank"), idx)),
                "sold_90d_count": sold_90d_count,
                "sold_price_min_90d": sold_price_min,
                "seed_entry_source": str(raw.get("seed_entry_source", "") or ("raw_fallback" if use_raw_fallback else "filtered")),
            }
        )
    if out:
        return out
    sold_sample = meta.get("sold_sample") if isinstance(meta.get("sold_sample"), dict) else {}
    title = str(sold_sample.get("title", "") or "").strip()
    if not title:
        return []
    return [
        {
            "title": title,
            "item_id": str(sold_sample.get("item_id", "") or "").strip(),
            "item_url": str(sold_sample.get("item_url", "") or "").strip(),
            "image_url": str(sold_sample.get("image_url", "") or "").strip(),
            "sold_price": round(max(0.0, to_float(sold_sample.get("sold_price"), 0.0)), 4),
            "rank": 1,
            "sold_90d_count": sold_90d_count,
            "sold_price_min_90d": sold_price_min,
        }
    ]


def _seed_low_liquidity_cooldown_enabled() -> bool:
    return env_bool(_LOW_LIQUIDITY_COOLDOWN_ENABLED_ENV, True)


def _seed_usage_cooldown_enabled() -> bool:
    return env_bool("MINER_SEED_USAGE_COOLDOWN_ENABLED", True)


def _seed_usage_cooldown_days() -> int:
    return max(1, env_int("MINER_SEED_USAGE_COOLDOWN_DAYS", 7))


def _seed_low_liquidity_cooldown_days(*, sold_90d_count: int) -> int:
    if sold_90d_count <= 0:
        return max(1, env_int(_LOW_LIQUIDITY_COOLDOWN_DAYS_ZERO_ENV, 7))
    return max(1, env_int(_LOW_LIQUIDITY_COOLDOWN_DAYS_LOW_ENV, 3))


def _low_liquidity_reason_code(*, sold_90d_count: int, min_required: int) -> str:
    if sold_90d_count <= 0:
        return "sold_zero"
    if sold_90d_count < max(0, int(min_required)):
        return "sold_below_threshold"
    return "low_liquidity"


def _load_active_low_liquidity_seed_keys(conn: Any, *, category_key: str, now_ts: int) -> Set[str]:
    rows = conn.execute(
        """
        SELECT seed_key, blocked_until
        FROM miner_seed_liquidity_cooldowns
        WHERE category_key = ?
        """,
        (category_key,),
    ).fetchall()
    out: Set[str] = set()
    for row in rows:
        blocked_until_ts = iso_to_epoch(str(row["blocked_until"] or ""))
        if blocked_until_ts <= now_ts:
            continue
        seed_key = str(row["seed_key"] or "").strip().upper()
        if seed_key:
            out.add(seed_key)
    return out


def _upsert_low_liquidity_cooldowns(
    conn: Any,
    *,
    category_key: str,
    rows: Sequence[Dict[str, Any]],
    now_ts: Optional[int] = None,
) -> int:
    if not rows:
        return 0
    if not _seed_low_liquidity_cooldown_enabled():
        return 0
    now_ts = int(now_ts) if now_ts is not None else int(time.time())
    now_iso = utc_iso(now_ts)
    upserted_keys: Set[str] = set()
    for raw in rows:
        seed_query = _normalize_seed_query(str(raw.get("seed_query", "") or ""))
        seed_key = str(raw.get("seed_key", "") or "").strip().upper()
        if not seed_key and seed_query:
            seed_key = _seed_key(seed_query)
        if not seed_query and seed_key:
            seed_query = str(raw.get("seed_query", "") or "").strip()
        if len(seed_key) < 4:
            continue
        sold_90d_count = max(0, to_int(raw.get("sold_90d_count"), 0))
        min_required = max(0, to_int(raw.get("min_required"), 0))
        cooldown_days = _seed_low_liquidity_cooldown_days(sold_90d_count=sold_90d_count)
        blocked_until = utc_iso(now_ts + cooldown_days * 86400)
        metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
        reason_code = str(raw.get("reason_code", "") or "").strip() or _low_liquidity_reason_code(
            sold_90d_count=sold_90d_count,
            min_required=min_required,
        )
        conn.execute(
            """
            INSERT INTO miner_seed_liquidity_cooldowns (
                category_key,
                seed_key,
                seed_query,
                reason_code,
                sold_90d_count,
                min_required,
                blocked_until,
                last_rejected_at,
                reject_count,
                metadata_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(category_key, seed_key) DO UPDATE SET
                seed_query = excluded.seed_query,
                reason_code = excluded.reason_code,
                sold_90d_count = excluded.sold_90d_count,
                min_required = excluded.min_required,
                blocked_until = CASE
                    WHEN miner_seed_liquidity_cooldowns.blocked_until > excluded.blocked_until
                        THEN miner_seed_liquidity_cooldowns.blocked_until
                    ELSE excluded.blocked_until
                END,
                last_rejected_at = excluded.last_rejected_at,
                reject_count = COALESCE(miner_seed_liquidity_cooldowns.reject_count, 0) + 1,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                category_key,
                seed_key,
                seed_query,
                reason_code,
                sold_90d_count,
                min_required,
                blocked_until,
                now_iso,
                1,
                json.dumps(metadata, ensure_ascii=False),
                now_iso,
                now_iso,
            ),
        )
        upserted_keys.add(seed_key)
    return len(upserted_keys)


def _load_active_seed_usage_cooldown_keys(conn: Any, *, category_key: str, now_ts: int) -> Set[str]:
    rows = conn.execute(
        """
        SELECT seed_key, blocked_until
        FROM miner_seed_usage_cooldowns
        WHERE category_key = ?
        """,
        (category_key,),
    ).fetchall()
    out: Set[str] = set()
    for row in rows:
        blocked_until_ts = iso_to_epoch(str(row["blocked_until"] or ""))
        if blocked_until_ts <= now_ts:
            continue
        seed_key = str(row["seed_key"] or "").strip().upper()
        if seed_key:
            out.add(seed_key)
    return out


def _upsert_seed_usage_cooldowns(
    conn: Any,
    *,
    category_key: str,
    rows: Sequence[Dict[str, Any]],
    now_ts: Optional[int] = None,
) -> int:
    if not rows or not _seed_usage_cooldown_enabled():
        return 0
    now_ts = int(now_ts) if now_ts is not None else int(time.time())
    now_iso = utc_iso(now_ts)
    blocked_until = utc_iso(now_ts + (_seed_usage_cooldown_days() * 86400))
    upserted_keys: Set[str] = set()
    for raw in rows:
        seed_query = _normalize_seed_query(str(raw.get("seed_query", "") or ""))
        seed_key = str(raw.get("seed_key", "") or "").strip().upper()
        if not seed_key and seed_query:
            seed_key = _seed_key(seed_query)
        if len(seed_key) < 4:
            continue
        metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
        if not seed_query:
            seed_query = str(raw.get("seed_query", "") or "").strip()
        conn.execute(
            """
            INSERT INTO miner_seed_usage_cooldowns (
                category_key,
                seed_key,
                seed_query,
                blocked_until,
                last_consumed_at,
                consume_count,
                metadata_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(category_key, seed_key) DO UPDATE SET
                seed_query = excluded.seed_query,
                blocked_until = CASE
                    WHEN miner_seed_usage_cooldowns.blocked_until > excluded.blocked_until
                        THEN miner_seed_usage_cooldowns.blocked_until
                    ELSE excluded.blocked_until
                END,
                last_consumed_at = excluded.last_consumed_at,
                consume_count = COALESCE(miner_seed_usage_cooldowns.consume_count, 0) + 1,
                metadata_json = excluded.metadata_json,
                updated_at = excluded.updated_at
            """,
            (
                category_key,
                seed_key,
                seed_query,
                blocked_until,
                now_iso,
                1,
                json.dumps(metadata, ensure_ascii=False),
                now_iso,
                now_iso,
            ),
        )
        upserted_keys.add(seed_key)
    return len(upserted_keys)


def _load_active_seed_keys(conn: Any, *, category_key: str, now_ts: int) -> Set[str]:
    rows = conn.execute(
        """
        SELECT seed_key, expires_at
        FROM miner_seed_pool
        WHERE category_key = ?
        """,
        (category_key,),
    ).fetchall()
    out: Set[str] = set()
    for row in rows:
        expires_at = iso_to_epoch(str(row["expires_at"] or ""))
        if expires_at <= now_ts:
            continue
        key = str(row["seed_key"] or "").strip().upper()
        if key:
            out.add(key)
    return out


def _cleanup_expired(conn: Any, *, category_key: str, now_ts: int) -> int:
    rows = conn.execute(
        """
        SELECT id, expires_at
        FROM miner_seed_pool
        WHERE category_key = ?
        """,
        (category_key,),
    ).fetchall()
    stale_ids: List[int] = []
    for row in rows:
        expires_at = iso_to_epoch(str(row["expires_at"] or ""))
        if expires_at > 0 and expires_at <= now_ts:
            stale_ids.append(to_int(row["id"], 0))
    for sid in stale_ids:
        conn.execute("DELETE FROM miner_seed_pool WHERE id = ?", (sid,))
    return len(stale_ids)


def _normalize_existing_seed_rows(conn: Any, *, category_key: str) -> Dict[str, int]:
    rows = conn.execute(
        """
        SELECT id, seed_query, seed_key, source_rank, created_at
        FROM miner_seed_pool
        WHERE category_key = ?
        """,
        (category_key,),
    ).fetchall()
    if not rows:
        return {"normalized_count": 0, "deduped_count": 0, "deleted_invalid_count": 0}

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    deleted_invalid = 0
    for row in rows:
        sid = max(0, to_int(row["id"], 0))
        if sid <= 0:
            continue
        seed_query_raw = str(row["seed_query"] or "").strip()
        normalized_query = _normalize_seed_query(seed_query_raw)
        normalized_key = _seed_pool_key(normalized_query)
        if not normalized_query or len(normalized_key) < 4:
            conn.execute("DELETE FROM miner_seed_pool WHERE id = ?", (sid,))
            deleted_invalid += 1
            continue
        grouped.setdefault(normalized_key, []).append(
            {
                "id": sid,
                "seed_query": seed_query_raw,
                "seed_key": str(row["seed_key"] or "").strip(),
                "normalized_query": normalized_query,
                "normalized_key": normalized_key,
                "source_rank": max(0, to_int(row["source_rank"], 0)),
                "created_ts": max(0, iso_to_epoch(str(row["created_at"] or ""))),
            }
        )

    normalized_count = 0
    deduped_count = 0
    for normalized_key, candidates in grouped.items():
        if not candidates:
            continue
        candidates_sorted = sorted(
            candidates,
            key=lambda row: (
                row.get("source_rank", 0) if int(row.get("source_rank", 0)) > 0 else (10**12),
                row.get("created_ts", 0) if int(row.get("created_ts", 0)) > 0 else (10**12),
                max(0, to_int(row.get("id"), 0)),
            ),
        )
        keeper = candidates_sorted[0]
        keeper_id = max(0, to_int(keeper.get("id"), 0))
        for duplicate in candidates_sorted[1:]:
            dup_id = max(0, to_int(duplicate.get("id"), 0))
            if dup_id <= 0:
                continue
            conn.execute("DELETE FROM miner_seed_pool WHERE id = ?", (dup_id,))
            deduped_count += 1
        if keeper_id > 0 and (
            str(keeper.get("seed_query", "") or "").strip() != str(keeper.get("normalized_query", "") or "").strip()
            or str(keeper.get("seed_key", "") or "").strip().upper() != normalized_key
        ):
            conn.execute(
                """
                UPDATE miner_seed_pool
                SET seed_query = ?, seed_key = ?
                WHERE id = ?
                """,
                (
                    str(keeper.get("normalized_query", "") or "").strip(),
                    normalized_key,
                    keeper_id,
                ),
            )
            normalized_count += 1

    return {
        "normalized_count": int(normalized_count),
        "deduped_count": int(deduped_count),
        "deleted_invalid_count": int(deleted_invalid),
    }


def _count_available(conn: Any, *, category_key: str, now_ts: int) -> int:
    rows = conn.execute(
        """
        SELECT expires_at
        FROM miner_seed_pool
        WHERE category_key = ?
        """,
        (category_key,),
    ).fetchall()
    count = 0
    for row in rows:
        expires_at = iso_to_epoch(str(row["expires_at"] or ""))
        if expires_at <= now_ts:
            continue
        count += 1
    return count


def _load_refill_state(conn: Any, *, category_key: str) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT category_key, last_refill_at, last_refill_status, last_refill_message, last_rank_checked, cooldown_until
        FROM miner_seed_refill_state
        WHERE category_key = ?
        """,
        (category_key,),
    ).fetchone()
    return dict(row) if row is not None else {}


def _upsert_refill_state(
    conn: Any,
    *,
    category_key: str,
    last_refill_status: str,
    last_refill_message: str,
    last_rank_checked: int,
    cooldown_until: str,
) -> None:
    now_iso = utc_iso()
    conn.execute(
        """
        INSERT INTO miner_seed_refill_state (
            category_key, last_refill_at, last_refill_status, last_refill_message, last_rank_checked, cooldown_until, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(category_key) DO UPDATE SET
            last_refill_at = excluded.last_refill_at,
            last_refill_status = excluded.last_refill_status,
            last_refill_message = excluded.last_refill_message,
            last_rank_checked = excluded.last_rank_checked,
            cooldown_until = excluded.cooldown_until,
            updated_at = excluded.updated_at
        """,
        (
            category_key,
            now_iso,
            str(last_refill_status or "").strip(),
            str(last_refill_message or "").strip(),
            max(0, int(last_rank_checked)),
            str(cooldown_until or "").strip(),
            now_iso,
        ),
    )


def _query_window_key(query: str) -> str:
    return re.sub(r"\s+", " ", str(query or "").strip()).lower()


def _load_query_page_unlock_hours_overrides(raw: str) -> Dict[str, float]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    out: Dict[str, float] = {}
    for raw_key, raw_value in parsed.items():
        key = _query_window_key(str(raw_key or ""))
        if not key:
            continue
        hours = max(0.0, to_float(raw_value, 0.0))
        if hours <= 0:
            continue
        out[key] = float(hours)
    return out


def _load_query_page_unlock_hours_from_category_row(category_row: Dict[str, Any]) -> Dict[str, float]:
    if not isinstance(category_row, dict):
        return {}
    raw = category_row.get("phase_a_page_unlock_hours", {})
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, float] = {}
    for raw_key, raw_value in raw.items():
        key = _query_window_key(str(raw_key or ""))
        if not key:
            continue
        hours = max(0.0, to_float(raw_value, 0.0))
        if hours <= 0:
            continue
        out[key] = float(hours)
    return out


def _resolve_query_page_unlock_hours(
    *,
    category_key: str,
    query_key: str,
    default_hours: float,
    category_overrides: Dict[str, float],
    overrides: Dict[str, float],
) -> Tuple[float, str]:
    category = _normalize_category_key(category_key)
    key_pair = f"{category}:{query_key}" if category and query_key else ""
    if key_pair and key_pair in overrides:
        return max(0.25, float(overrides[key_pair])), "override_category_query"
    if query_key in overrides:
        return max(0.25, float(overrides[query_key])), "override_query"
    if key_pair and key_pair in category_overrides:
        return max(0.25, float(category_overrides[key_pair])), "category_row_category_query"
    if query_key in category_overrides:
        return max(0.25, float(category_overrides[query_key])), "category_row_query"
    env_suffix = re.sub(r"[^A-Z0-9_]+", "_", query_key.upper())
    if env_suffix:
        env_hours = max(0.0, to_float(os.getenv(f"MINER_STAGEA_QUERY_PAGE_UNLOCK_HOURS_{env_suffix}", ""), 0.0))
        if env_hours > 0:
            return max(0.25, float(env_hours)), "override_query_env"
    return max(0.25, float(default_hours)), "default"


def _compute_query_unlocked_pages(
    *,
    now_ts: int,
    latest_fetched_ts: int,
    max_pages: int,
    hours_per_page: float,
    min_pages: int,
    initial_pages: int,
) -> Tuple[int, int]:
    safe_max_pages = max(1, int(max_pages))
    if latest_fetched_ts <= 0:
        return max(1, min(safe_max_pages, int(initial_pages))), 0
    elapsed_sec = max(0, int(now_ts) - int(latest_fetched_ts))
    seconds_per_page = max(1, int(round(max(0.25, float(hours_per_page)) * 3600)))
    unlocked_pages = int(elapsed_sec // seconds_per_page)
    unlocked_pages = max(max(0, int(min_pages)), unlocked_pages)
    unlocked_pages = min(safe_max_pages, unlocked_pages)
    return int(unlocked_pages), int(elapsed_sec)


def _compute_next_query_page_unlock_ts(
    *,
    latest_fetched_ts: int,
    unlocked_pages: int,
    max_pages: int,
    hours_per_page: float,
    now_ts: int,
) -> int:
    if latest_fetched_ts <= 0:
        return 0
    safe_max_pages = max(1, int(max_pages))
    safe_unlocked_pages = max(0, int(unlocked_pages))
    if safe_unlocked_pages >= safe_max_pages:
        return 0
    next_threshold_pages = safe_unlocked_pages + 1
    seconds_per_page = max(1, int(round(max(0.25, float(hours_per_page)) * 3600)))
    next_ts = int(latest_fetched_ts) + (next_threshold_pages * seconds_per_page)
    if next_ts <= int(now_ts):
        return 0
    return int(next_ts)


def _load_page_window_entries(
    conn: Any,
    *,
    category_key: str,
    query_key: str,
) -> Dict[int, Dict[str, int]]:
    rows = conn.execute(
        """
        SELECT page_offset, fetched_at, result_count, new_seed_count
        FROM miner_seed_refill_pages
        WHERE category_key = ? AND query_key = ?
        """,
        (category_key, query_key),
    ).fetchall()
    out: Dict[int, Dict[str, int]] = {}
    for row in rows:
        offset = max(0, to_int(row["page_offset"], 0))
        fetched_ts = iso_to_epoch(str(row["fetched_at"] or ""))
        if fetched_ts > 0:
            out[offset] = {
                "fetched_ts": fetched_ts,
                "result_count": max(0, to_int(row["result_count"], 0)),
                "new_seed_count": max(0, to_int(row["new_seed_count"], 0)),
            }
    return out


def _upsert_page_window_entry(
    conn: Any,
    *,
    category_key: str,
    query_key: str,
    page_offset: int,
    page_size: int,
    result_count: int,
    new_seed_count: int,
) -> None:
    now_iso = utc_iso()
    conn.execute(
        """
        INSERT INTO miner_seed_refill_pages (
            category_key, query_key, page_offset, page_size, fetched_at, result_count, new_seed_count, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(category_key, query_key, page_offset) DO UPDATE SET
            page_size = excluded.page_size,
            fetched_at = excluded.fetched_at,
            result_count = excluded.result_count,
            new_seed_count = excluded.new_seed_count,
            updated_at = excluded.updated_at
        """,
        (
            category_key,
            query_key,
            max(0, int(page_offset)),
            max(1, int(page_size)),
            now_iso,
            max(0, int(result_count)),
            max(0, int(new_seed_count)),
            now_iso,
        ),
    )


def _insert_seed_rows(
    conn: Any,
    *,
    category_key: str,
    rows: Sequence[Dict[str, Any]],
    ttl_days: int,
    strict_model_only: bool = False,
) -> int:
    now_ts = int(time.time())
    created_at = utc_iso(now_ts)
    expires_at = utc_iso(now_ts + max(1, int(ttl_days)) * 86400)
    inserted = 0
    for row in rows:
        seed_query = _normalize_seed_query(row.get("seed_query", ""))
        seed_key = _seed_pool_key(seed_query)
        if not seed_query or len(seed_key) < 4:
            continue
        if bool(strict_model_only) and not _seed_query_is_model_or_gtin(seed_query):
            continue
        cur = conn.execute(
            """
            INSERT INTO miner_seed_pool (
                category_key,
                seed_query,
                seed_key,
                source_title,
                source_item_url,
                source_page,
                source_offset,
                source_rank,
                created_at,
                expires_at,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(category_key, seed_key) DO NOTHING
            """,
            (
                category_key,
                seed_query,
                seed_key,
                str(row.get("source_title", "") or ""),
                str(row.get("source_item_url", "") or ""),
                max(1, int(row.get("source_page", 1) or 1)),
                max(0, int(row.get("source_offset", 0) or 0)),
                max(0, int(row.get("source_rank", 0) or 0)),
                created_at,
                expires_at,
                json.dumps(row.get("metadata", {}), ensure_ascii=False),
            ),
        )
        rowcount = int(getattr(cur, "rowcount", 1))
        if rowcount != 0:
            inserted += 1
    return inserted


def _prune_non_model_seed_rows(
    conn: Any,
    *,
    category_key: str,
) -> int:
    rows = conn.execute(
        """
        SELECT id, seed_query
        FROM miner_seed_pool
        WHERE category_key = ?
        """,
        (category_key,),
    ).fetchall()
    delete_ids: List[int] = []
    for row in rows:
        sid = max(0, to_int(row["id"], 0))
        if sid <= 0:
            continue
        seed_query = _normalize_seed_query(str(row["seed_query"] or ""))
        if not seed_query:
            delete_ids.append(sid)
            continue
        if not _seed_query_is_model_or_gtin(seed_query):
            delete_ids.append(sid)
    if not delete_ids:
        return 0
    chunk_size = 500
    deleted = 0
    for start in range(0, len(delete_ids), chunk_size):
        chunk = delete_ids[start : start + chunk_size]
        placeholders = ", ".join(["?"] * len(chunk))
        conn.execute(
            f"DELETE FROM miner_seed_pool WHERE id IN ({placeholders})",
            tuple(chunk),
        )
        deleted += len(chunk)
    return int(deleted)


def _take_seeds_for_run(
    conn: Any,
    *,
    category_key: str,
    take_count: int,
    now_ts: int,
) -> Tuple[List[Dict[str, Any]], int]:
    _normalize_existing_seed_rows(conn, category_key=category_key)
    active_low_liquidity_cooldown_keys = _load_active_low_liquidity_seed_keys(
        conn,
        category_key=category_key,
        now_ts=now_ts,
    )
    active_used_cooldown_keys = _load_active_seed_usage_cooldown_keys(
        conn,
        category_key=category_key,
        now_ts=now_ts,
    )
    active_cooldown_keys = set(active_low_liquidity_cooldown_keys)
    active_cooldown_keys.update(active_used_cooldown_keys)
    rows = conn.execute(
        """
        SELECT id, seed_query, seed_key, source_rank, source_title, created_at, expires_at, last_used_at, use_count, metadata_json
        FROM miner_seed_pool
        WHERE category_key = ?
        """,
        (category_key,),
    ).fetchall()
    def _sort_key(row: Dict[str, Any]) -> Tuple[int, int, int, int, int]:
        created_ts = iso_to_epoch(str(row.get("created_at", "") or ""))
        source_rank = max(0, to_int(row.get("source_rank"), 0))
        sid = max(0, to_int(row.get("id"), 0))
        use_count = max(0, to_int(row.get("use_count"), 0))
        metadata_raw = str(row.get("metadata_json", "") or "").strip()
        metadata: Dict[str, Any] = {}
        if metadata_raw:
            try:
                parsed = json.loads(metadata_raw)
                if isinstance(parsed, dict):
                    metadata = parsed
            except Exception:
                metadata = {}
        stage1_zero_hit_count = _seed_stage1_zero_hit_count(metadata)
        # 同じseed群に張り付くのを防ぐため、未使用seedを優先した上で古い順で処理する。
        return (use_count, stage1_zero_hit_count, created_ts if created_ts > 0 else (10**12), source_rank, sid)

    ordered_rows = sorted([dict(v) for v in rows], key=_sort_key)
    out: List[Dict[str, Any]] = []
    skipped_cooldown_count = 0
    now_iso = utc_iso(now_ts)
    for row in ordered_rows:
        if len(out) >= max(1, int(take_count)):
            break
        expires_at = iso_to_epoch(str(row["expires_at"] or ""))
        if expires_at <= now_ts:
            continue
        seed_key_text = str(row.get("seed_key", "") or "").strip().upper()
        if seed_key_text and seed_key_text in active_cooldown_keys:
            skipped_cooldown_count += 1
            continue
        seed_query = str(row["seed_query"] or "").strip()
        metadata_raw = str(row.get("metadata_json", "") or "").strip()
        metadata: Dict[str, Any] = {}
        if metadata_raw:
            try:
                parsed = json.loads(metadata_raw)
                if isinstance(parsed, dict):
                    metadata = parsed
            except Exception:
                metadata = {}
        sid = to_int(row["id"], 0)
        if sid <= 0:
            continue
        conn.execute(
            """
            UPDATE miner_seed_pool
            SET consumed_at = NULL, last_used_at = ?, use_count = COALESCE(use_count, 0) + 1
            WHERE id = ?
            """,
            (now_iso, sid),
        )
        out.append(
            {
                "id": sid,
                "seed_query": seed_query,
                "seed_key": seed_key_text,
                "source_rank": to_int(row["source_rank"], 0),
                "source_title": str(row["source_title"] or "").strip(),
                "seed_quality_score": to_int(metadata.get("seed_quality_score"), 0),
                "stage1_zero_hit_count": _seed_stage1_zero_hit_count(metadata),
                "seed_collected_sold_price_min_usd": to_float(metadata.get("seed_collected_sold_price_min_usd"), -1.0),
                "seed_collected_sold_90d_count": to_int(metadata.get("seed_collected_sold_90d_count"), -1),
            }
        )
    if out and _seed_usage_cooldown_enabled():
        _upsert_seed_usage_cooldowns(
            conn,
            category_key=category_key,
            rows=[
                {
                    "seed_query": str(row.get("seed_query", "") or ""),
                    "seed_key": str(row.get("seed_key", "") or "").strip().upper(),
                    "metadata": {
                        "source": "take_seeds_for_run",
                        "seed_id": max(0, to_int(row.get("id"), 0)),
                    },
                }
                for row in out
            ],
            now_ts=now_ts,
        )
    return out, int(skipped_cooldown_count)


def _preview_seeds_for_run(
    conn: Any,
    *,
    category_key: str,
    take_count: int,
    now_ts: int,
) -> Tuple[int, int]:
    _normalize_existing_seed_rows(conn, category_key=category_key)
    active_cooldown_keys = _load_active_low_liquidity_seed_keys(conn, category_key=category_key, now_ts=now_ts)
    active_cooldown_keys.update(
        _load_active_seed_usage_cooldown_keys(conn, category_key=category_key, now_ts=now_ts)
    )
    rows = conn.execute(
        """
        SELECT id, seed_query, seed_key, source_rank, created_at, expires_at, metadata_json, use_count
        FROM miner_seed_pool
        WHERE category_key = ?
        """,
        (category_key,),
    ).fetchall()

    def _sort_key(row: Dict[str, Any]) -> Tuple[int, int, int, int, int]:
        created_ts = iso_to_epoch(str(row.get("created_at", "") or ""))
        source_rank = max(0, to_int(row.get("source_rank"), 0))
        sid = max(0, to_int(row.get("id"), 0))
        use_count = max(0, to_int(row.get("use_count"), 0))
        metadata_raw = str(row.get("metadata_json", "") or "").strip()
        metadata: Dict[str, Any] = {}
        if metadata_raw:
            try:
                parsed = json.loads(metadata_raw)
                if isinstance(parsed, dict):
                    metadata = parsed
            except Exception:
                metadata = {}
        stage1_zero_hit_count = _seed_stage1_zero_hit_count(metadata)
        return (use_count, stage1_zero_hit_count, created_ts if created_ts > 0 else (10**12), source_rank, sid)

    ordered_rows = sorted([dict(v) for v in rows], key=_sort_key)
    selected_count = 0
    skipped_cooldown_count = 0
    for row in ordered_rows:
        if selected_count >= max(1, int(take_count)):
            break
        expires_at = iso_to_epoch(str(row.get("expires_at", "") or ""))
        if expires_at <= now_ts:
            continue
        seed_key_text = str(row.get("seed_key", "") or "").strip().upper()
        if seed_key_text and seed_key_text in active_cooldown_keys:
            skipped_cooldown_count += 1
            continue
        selected_count += 1
    return int(selected_count), int(skipped_cooldown_count)


def _apply_stage1_seed_feedback(
    conn: Any,
    *,
    rows: Sequence[Dict[str, Any]],
) -> int:
    updated = 0
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        seed_id = max(0, to_int(raw.get("seed_id"), 0))
        if seed_id <= 0:
            continue
        row = conn.execute(
            "SELECT metadata_json FROM miner_seed_pool WHERE id = ?",
            (seed_id,),
        ).fetchone()
        metadata: Dict[str, Any] = {}
        if row:
            metadata_raw = str(row["metadata_json"] or "").strip()
            if metadata_raw:
                try:
                    parsed = json.loads(metadata_raw)
                    if isinstance(parsed, dict):
                        metadata = parsed
                except Exception:
                    metadata = {}
        prev_zero_hit_count = _seed_stage1_zero_hit_count(metadata)
        had_raw_results = bool(raw.get("had_raw_results"))
        had_stage1_candidates = bool(raw.get("had_stage1_candidates"))
        next_zero_hit_count = 0 if (had_raw_results or had_stage1_candidates) else prev_zero_hit_count + 1
        if next_zero_hit_count == prev_zero_hit_count:
            continue
        metadata["stage1_zero_hit_count"] = int(next_zero_hit_count)
        conn.execute(
            "UPDATE miner_seed_pool SET metadata_json = ? WHERE id = ?",
            (json.dumps(metadata, ensure_ascii=False), seed_id),
        )
        updated += 1
    return int(updated)


def get_seed_pool_status(
    *,
    category_query: str,
    settings: Optional[Settings] = None,
) -> Dict[str, Any]:
    settings = settings or load_settings()
    category_key, category_label, category_row = _resolve_category(category_query)
    if not category_key:
        raise ValueError("category query is required")
    pr_category_filter = _resolve_ebay_pr_category_filter(category_key, category_row)

    now_ts = int(time.time())
    run_batch_size = 1
    freshness_days = max(1, env_int("MINER_SEED_POOL_PAGE_FRESH_DAYS", 7))
    freshness_sec = freshness_days * 86400
    search_query = _category_search_query(category_key, category_row)
    query_key = _query_window_key(search_query)

    with connect(settings.db_path) as conn:
        init_db(conn)
        normalize_stats = _normalize_existing_seed_rows(conn, category_key=category_key)
        cleaned = _cleanup_expired(conn, category_key=category_key, now_ts=now_ts)
        available = _count_available(conn, category_key=category_key, now_ts=now_ts)
        run_batch_size = max(1, int(available))
        selected_count, skipped_cooldown_count = _preview_seeds_for_run(
            conn,
            category_key=category_key,
            take_count=run_batch_size,
            now_ts=now_ts,
        )
        low_liquidity_cooldown_active_count = len(
            _load_active_low_liquidity_seed_keys(conn, category_key=category_key, now_ts=now_ts)
        )
        used_cooldown_active_count = len(
            _load_active_seed_usage_cooldown_keys(conn, category_key=category_key, now_ts=now_ts)
        )
        cooldown_active_count = low_liquidity_cooldown_active_count + used_cooldown_active_count
        refill_state = _load_refill_state(conn, category_key=category_key)
        page_window_entries = _load_page_window_entries(conn, category_key=category_key, query_key=query_key)

    history_page_count = len(page_window_entries)
    history_fresh_page_count = sum(
        1
        for meta in page_window_entries.values()
        if isinstance(meta, dict) and (now_ts - to_int(meta.get("fetched_ts"), 0)) < freshness_sec
    )
    reason = str(refill_state.get("last_refill_status", "") or "").strip().lower() or "snapshot"

    seed_pool_summary = {
        "category_key": category_key,
        "category_label": category_label,
        "seed_count": int(selected_count),
        # backward-compat keys (deprecated)
        "selected_seed_count": int(selected_count),
        "available_after_refill": int(available),
        "skipped_low_quality_count": 0,
        "skipped_cooldown_count": int(skipped_cooldown_count),
        "cooldown_active_count": int(cooldown_active_count),
        "low_liquidity_cooldown_active_count": int(low_liquidity_cooldown_active_count),
        "used_cooldown_active_count": int(used_cooldown_active_count),
        "select_min_seed_score": 0,
            "cleaned_expired_count": int(cleaned),
            "normalized_seed_count": int(normalize_stats.get("normalized_count", 0)),
            "deduped_seed_count": int(normalize_stats.get("deduped_count", 0)),
            "deleted_invalid_seed_count": int(normalize_stats.get("deleted_invalid_count", 0)),
            "refill": {
            "ran": False,
            "available_before": int(available),
            "available_after": int(available),
            "added_count": 0,
            "bootstrap_added_count": 0,
            "skipped_fresh_pages": int(history_fresh_page_count),
            "page_runs": [],
            "reason": reason,
            "daily_limit_reached": False,
            "cooldown_until": str(refill_state.get("cooldown_until", "") or "").strip(),
            "query": search_query,
            "history_page_count": int(history_page_count),
            "history_fresh_page_count": int(history_fresh_page_count),
            "last_refill_at": str(refill_state.get("last_refill_at", "") or "").strip(),
            "last_refill_message": str(refill_state.get("last_refill_message", "") or "").strip(),
            "last_rank_checked": max(0, to_int(refill_state.get("last_rank_checked"), 0)),
        },
    }
    return {
        "query": category_key,
        "market_site": "ebay",
        "source_sites": ["rakuten", "yahoo"],
        "created_count": 0,
        "created_ids": [],
        "created": [],
        "errors": [],
        "hints": [],
        "search_scope_done": False,
        "applied_filters": {},
        "query_cache_skip": False,
        "query_cache_ttl_sec": 0,
        "rpa_daily_limit_reached": False,
        "seed_pool": seed_pool_summary,
        "stage1_skip_counts": {},
        "timed_fetch": {
            "enabled": True,
            "min_target_candidates": 0,
            "timebox_sec": 0,
            "max_passes": 0,
            "continue_after_target": True,
            "passes_run": 0,
            "stop_reason": "seed_pool_status",
            "elapsed_sec": 0.0,
            "reached_min_target": False,
            "stage1_pass_total": 0,
            "stage2_runs": 0,
            "stage1_seed_baseline_reject_total": 0,
            "passes": [],
        },
    }


def reset_seed_pool_category_state(
    *,
    category_query: str,
    settings: Optional[Settings] = None,
    clear_pool: bool = False,
    clear_history: bool = False,
) -> Dict[str, Any]:
    """Reset category state.

    clear_pool=True ならseed pool系を削除。
    clear_history=True ならカテゴリseed由来の候補/否認履歴とrun journalも削除。
    """
    settings = settings or load_settings()
    category_key, category_label, _category_row = _resolve_category(category_query)
    if not category_key:
        raise ValueError("category query is required")

    now_ts = int(time.time())
    with connect(settings.db_path) as conn:
        init_db(conn)
        cleaned_expired = _cleanup_expired(conn, category_key=category_key, now_ts=now_ts)
        available_before = _count_available(conn, category_key=category_key, now_ts=now_ts)
        clear_pool_effective = bool(clear_pool or clear_history)

        refill_row = conn.execute(
            """
            SELECT category_key
            FROM miner_seed_refill_state
            WHERE category_key = ?
            """,
            (category_key,),
        ).fetchone()
        had_refill_state = refill_row is not None

        page_row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM miner_seed_refill_pages
            WHERE category_key = ?
            """,
            (category_key,),
        ).fetchone()
        cleared_page_windows = max(0, to_int(page_row["c"] if page_row else 0, 0))

        seed_rows = conn.execute(
            """
            SELECT id, seed_key
            FROM miner_seed_pool
            WHERE category_key = ?
            """,
            (category_key,),
        ).fetchall()
        category_seed_ids: Set[int] = set()
        category_seed_keys: Set[str] = set()
        for row in seed_rows:
            sid = max(0, to_int(row["id"], 0))
            if sid > 0:
                category_seed_ids.add(sid)
            skey = str(row["seed_key"] or "").strip().upper()
            if skey:
                category_seed_keys.add(skey)

        conn.execute(
            """
            DELETE FROM miner_seed_refill_pages
            WHERE category_key = ?
            """,
            (category_key,),
        )
        conn.execute(
            """
            DELETE FROM miner_seed_refill_state
            WHERE category_key = ?
            """,
            (category_key,),
        )
        cooldown_row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM miner_seed_liquidity_cooldowns
            WHERE category_key = ?
            """,
            (category_key,),
        ).fetchone()
        cleared_liquidity_cooldowns = max(0, to_int(cooldown_row["c"] if cooldown_row else 0, 0))
        conn.execute(
            """
            DELETE FROM miner_seed_liquidity_cooldowns
            WHERE category_key = ?
            """,
            (category_key,),
        )
        used_cooldown_row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM miner_seed_usage_cooldowns
            WHERE category_key = ?
            """,
            (category_key,),
        ).fetchone()
        cleared_usage_cooldowns = max(0, to_int(used_cooldown_row["c"] if used_cooldown_row else 0, 0))
        conn.execute(
            """
            DELETE FROM miner_seed_usage_cooldowns
            WHERE category_key = ?
            """,
            (category_key,),
        )

        cleared_seed_rows = 0
        if clear_pool_effective:
            cleared_seed_rows = len(category_seed_ids)
            conn.execute(
                """
                DELETE FROM miner_seed_pool
                WHERE category_key = ?
                """,
                (category_key,),
            )

        cleared_rejection_rows = 0
        cleared_candidate_rows = 0
        if bool(clear_history):
            candidate_rows = conn.execute(
                """
                SELECT id, metadata_json
                FROM miner_candidates
                """
            ).fetchall()
            target_candidate_ids: List[int] = []
            for row in candidate_rows:
                cid = max(0, to_int(row["id"], 0))
                if cid <= 0:
                    continue
                metadata_raw = str(row.get("metadata_json", "") if isinstance(row, dict) else row["metadata_json"] or "").strip()
                metadata: Dict[str, Any] = {}
                if metadata_raw:
                    try:
                        parsed = json.loads(metadata_raw)
                        if isinstance(parsed, dict):
                            metadata = parsed
                    except Exception:
                        metadata = {}
                seed_pool_meta = metadata.get("seed_pool") if isinstance(metadata.get("seed_pool"), dict) else {}
                row_category_key = _normalize_category_key(str(metadata.get("category_key", "") or ""))
                row_query_key = _normalize_category_key(str(metadata.get("query", "") or ""))
                seed_pool_category_key = _normalize_category_key(str(seed_pool_meta.get("category_key", "") or ""))
                seed_pool_id = max(0, to_int(seed_pool_meta.get("id"), 0))
                seed_pool_key = str(seed_pool_meta.get("seed_key", "") or "").strip().upper()
                belongs = False
                if seed_pool_category_key and seed_pool_category_key == category_key:
                    belongs = True
                elif row_category_key and row_category_key == category_key:
                    belongs = True
                elif row_query_key and row_query_key == category_key:
                    belongs = True
                elif seed_pool_id > 0 and seed_pool_id in category_seed_ids:
                    belongs = True
                elif seed_pool_key and seed_pool_key in category_seed_keys:
                    belongs = True
                if belongs:
                    target_candidate_ids.append(cid)

            if target_candidate_ids:
                chunk_size = 500
                for start in range(0, len(target_candidate_ids), chunk_size):
                    chunk = target_candidate_ids[start : start + chunk_size]
                    placeholders = ", ".join(["?"] * len(chunk))
                    count_row = conn.execute(
                        f"SELECT COUNT(*) AS c FROM miner_rejections WHERE candidate_id IN ({placeholders})",
                        tuple(chunk),
                    ).fetchone()
                    cleared_rejection_rows += max(0, to_int(count_row["c"] if count_row else 0, 0))
                    conn.execute(
                        f"DELETE FROM miner_rejections WHERE candidate_id IN ({placeholders})",
                        tuple(chunk),
                    )
                    conn.execute(
                        f"DELETE FROM miner_candidates WHERE id IN ({placeholders})",
                        tuple(chunk),
                    )
                cleared_candidate_rows = len(target_candidate_ids)

        conn.commit()
        available_after = _count_available(conn, category_key=category_key, now_ts=now_ts)
    cleared_seed_journal_rows = _prune_seed_run_journal_by_category(category_key=category_key) if bool(clear_history) else 0

    return {
        "category_key": category_key,
        "category_label": category_label,
        "reset_at": utc_iso(now_ts),
        "had_refill_state": bool(had_refill_state),
        "cleared_page_windows": int(cleared_page_windows),
        "cleared_liquidity_cooldowns": int(cleared_liquidity_cooldowns),
        "cleared_usage_cooldowns": int(cleared_usage_cooldowns),
        "cleared_seed_rows": int(cleared_seed_rows),
        "cleared_candidate_rows": int(cleared_candidate_rows),
        "cleared_rejection_rows": int(cleared_rejection_rows),
        "cleared_seed_journal_rows": int(cleared_seed_journal_rows),
        "cleaned_expired_count": int(cleaned_expired),
        "available_before": int(available_before),
        "available_after": int(available_after),
        "clear_pool": bool(clear_pool_effective),
        "clear_history": bool(clear_history),
    }


def _build_stage_a_tuning_recommendations(refill_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(refill_summary, dict):
        return []
    reason = str(refill_summary.get("reason", "") or "").strip().lower()
    added_count = max(0, to_int(refill_summary.get("added_count"), 0))
    fetched_pages = max(0, to_int(refill_summary.get("fetched_pages"), 0))
    skipped_fresh_pages = max(0, to_int(refill_summary.get("skipped_fresh_pages"), 0))
    minimize_transitions = bool(refill_summary.get("minimize_transitions"))
    big_word_limit = max(0, to_int(refill_summary.get("big_word_limit"), 0))
    big_word_count = max(0, to_int(refill_summary.get("big_word_count"), 0))
    big_word_total_count = max(0, to_int(refill_summary.get("big_word_total_count"), 0))
    timebox_base_sec = max(0, to_int(refill_summary.get("timebox_base_sec"), 0))
    timebox_sec = max(0, to_int(refill_summary.get("timebox_sec"), 0))
    diagnostics = refill_summary.get("diagnostics", {}) if isinstance(refill_summary.get("diagnostics"), dict) else {}
    rpa_failed_pages = max(0, to_int(diagnostics.get("rpa_failed_pages"), 0))
    bot_challenge_pages = max(0, to_int(diagnostics.get("bot_challenge_pages"), 0))
    empty_result_pages = max(0, to_int(diagnostics.get("empty_result_pages"), 0))
    non_empty_result_pages = max(0, to_int(diagnostics.get("non_empty_result_pages"), 0))
    strict_filter_blocked_pages = max(0, to_int(diagnostics.get("strict_filter_blocked_pages"), 0))
    sold_tab_unconfirmed_pages = max(0, to_int(diagnostics.get("sold_tab_unconfirmed_pages"), 0))
    lookback_unconfirmed_pages = max(0, to_int(diagnostics.get("lookback_unconfirmed_pages"), 0))
    recommendations: List[Dict[str, Any]] = []

    def _append(
        *,
        code: str,
        message: str,
        priority: str,
        env_overrides: Optional[Dict[str, Any]] = None,
        fetch_payload_overrides: Optional[Dict[str, Any]] = None,
    ) -> None:
        row: Dict[str, Any] = {
            "code": str(code or "").strip(),
            "message": str(message or "").strip(),
            "priority": str(priority or "").strip() or "medium",
        }
        if isinstance(env_overrides, dict) and env_overrides:
            row["env_overrides"] = dict(env_overrides)
        if isinstance(fetch_payload_overrides, dict) and fetch_payload_overrides:
            row["fetch_payload_overrides"] = dict(fetch_payload_overrides)
        recommendations.append(row)

    if rpa_failed_pages > 0:
        _append(
            code="stabilize_rpa_fetch",
            message=(
                f"A段階でProduct Research取得失敗が {rpa_failed_pages} ページ発生。"
                "先にRPA待機/タイムアウトを緩めて取得安定性を確認してください。"
            ),
            priority="high",
            env_overrides={
                "MINER_SEED_POOL_RPA_FETCH_TIMEOUT_SECONDS": max(
                    95,
                    min(240, env_int("MINER_SEED_POOL_RPA_FETCH_TIMEOUT_SECONDS", 95) + 25),
                ),
                "MINER_SEED_POOL_RPA_WAIT_SECONDS": max(
                    8,
                    min(20, env_int("MINER_SEED_POOL_RPA_WAIT_SECONDS", 8) + 2),
                ),
            },
        )

    if bot_challenge_pages > 0:
        _append(
            code="resolve_ebay_bot_challenge",
            message=(
                f"A段階でeBay bot challengeが {bot_challenge_pages} ページ発生。"
                "手動ログイン済みの同一profileでchallenge解除後に再実行してください。"
            ),
            priority="high",
            env_overrides={
                "LIQUIDITY_RPA_FORCE_HEADLESS": 0,
                "LIQUIDITY_RPA_PAUSE_FOR_LOGIN_SECONDS": max(
                    60,
                    env_int("LIQUIDITY_RPA_PAUSE_FOR_LOGIN_SECONDS", 180),
                ),
            },
        )

    if strict_filter_blocked_pages > 0 or sold_tab_unconfirmed_pages > 0 or lookback_unconfirmed_pages > 0:
        _append(
            code="verify_pr_filter_lock",
            message=(
                "A段階で90日売却フィルタ確認に失敗したページがあります。"
                "ログイン状態とフィルタロック動作を確認し、traceを有効化して再実行してください。"
            ),
            priority="high",
            env_overrides={
                "MINER_SEED_POOL_TRACE_ENABLED": 1,
                "LIQUIDITY_RPA_ENABLE_LOCK_SELECTED_FILTERS": 1,
            },
        )

    if reason == "refill_timebox_reached":
        suggested_timebox = max(
            300,
            min(1800, int(round(max(300, timebox_base_sec, timebox_sec) * 1.5))),
        )
        _append(
            code="increase_refill_timebox",
            message=(
                "A段階がtimeboxで停止しています。"
                f"補充timeboxを {suggested_timebox}s まで引き上げて連続空振りを防いでください。"
            ),
            priority="medium",
            env_overrides={"MINER_SEED_POOL_REFILL_TIMEBOX_SEC": suggested_timebox},
        )

    if reason == "empty_result_page" and added_count <= 0 and fetched_pages > 0:
        if minimize_transitions:
            _append(
                code="disable_transition_minimize",
                message=(
                    "A段階が空振りのため、まず遷移最小化を無効にして通常ページ走査へ戻すことを推奨します。"
                ),
                priority="high",
                fetch_payload_overrides={"stage_a_minimize_transitions": False},
            )
        else:
            _append(
                code="widen_refill_scan",
                message=(
                    "A段階が空振りのため、補充ページ走査幅を拡げて再探索することを推奨します。"
                ),
                priority="medium",
                env_overrides={
                    "MINER_SEED_POOL_MAX_PAGES": max(4, min(40, env_int("MINER_SEED_POOL_MAX_PAGES", 40))),
                    "MINER_SEED_POOL_LOW_YIELD_CONSECUTIVE_PAGES": max(
                        3,
                        min(8, env_int("MINER_SEED_POOL_LOW_YIELD_CONSECUTIVE_PAGES", 3) + 1),
                    ),
                },
            )

    if reason == "fresh_window_skip" or (skipped_fresh_pages > 0 and fetched_pages <= 0):
        _append(
            code="reduce_page_fresh_window",
            message=(
                "A段階がfresh windowで再取得を回避しています。短期検証ではfresh日数を縮めるか履歴クリアを推奨します。"
            ),
            priority="medium",
            env_overrides={
                "MINER_SEED_POOL_PAGE_FRESH_DAYS": max(
                    1,
                    min(7, env_int("MINER_SEED_POOL_PAGE_FRESH_DAYS", 7) // 2),
                ),
            },
        )

    if big_word_limit > 0 and big_word_total_count > big_word_count:
        _append(
            code="expand_big_word_coverage",
            message=(
                f"A段階はbig wordを {big_word_count}/{big_word_total_count} に制限中です。"
                "腕時計の初期安定化では 0（全件）を推奨します。"
            ),
            priority="medium",
            fetch_payload_overrides={"stage_a_big_word_limit": 0},
        )

    if added_count <= 0 and empty_result_pages > 0 and non_empty_result_pages <= 0:
        _append(
            code="temporary_bootstrap_seed",
            message=(
                "A段階で有効seedが増えていません。暫定でカテゴリ知識bootstrapを有効化し、B段階の消化を維持してください。"
            ),
            priority="low",
            env_overrides={
                "MINER_SEED_POOL_BOOTSTRAP_ENABLED": 1,
                "MINER_SEED_POOL_BOOTSTRAP_TARGET": max(20, env_int("MINER_SEED_POOL_BOOTSTRAP_TARGET", 60)),
            },
        )

    return recommendations


def _phase_a_fallback_categories(primary_category_key: str) -> List[Tuple[str, str, Dict[str, Any]]]:
    payload = _load_category_knowledge()
    categories = payload.get("categories", []) if isinstance(payload, dict) else []
    if not isinstance(categories, list):
        return []
    primary_key = _normalize_category_key(primary_category_key)
    ranked: List[Tuple[int, int, str, str, Dict[str, Any]]] = []
    for idx, raw_row in enumerate(categories):
        if not isinstance(raw_row, dict):
            continue
        key = _normalize_category_key(str(raw_row.get("category_key", "") or ""))
        if not key or key == primary_key:
            continue
        priority_text = str(raw_row.get("priority", "") or "").strip().lower()
        if priority_text == "high":
            priority_rank = 0
        elif priority_text == "low":
            priority_rank = 2
        else:
            priority_rank = 1
        label = str(raw_row.get("display_name_ja", "") or "").strip() or _TARGET_LABELS.get(key, key)
        ranked.append((priority_rank, idx, key, label, dict(raw_row)))
    ranked.sort(key=lambda row: (row[0], row[1]))
    return [(key, label, row) for _rank, _idx, key, label, row in ranked]


def _refill_seed_pool_with_page_unlock_fallback(
    conn: Any,
    *,
    category_key: str,
    category_label: str,
    category_row: Dict[str, Any],
    stage_a_big_word_limit: int = 0,
    stage_a_minimize_transitions: bool = False,
    refill_timebox_override_sec: Optional[int] = None,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    primary = _refill_seed_pool(
        conn,
        category_key=category_key,
        category_label=category_label,
        category_row=category_row,
        stage_a_big_word_limit=max(0, int(stage_a_big_word_limit)),
        stage_a_minimize_transitions=bool(stage_a_minimize_transitions),
        refill_timebox_override_sec=refill_timebox_override_sec,
        progress_callback=progress_callback,
    )
    primary_reason = str(primary.get("reason", "") or "").strip().lower()
    target_new_seeds = max(20, env_int("MINER_SEED_POOL_TARGET_COUNT", 100))
    total_added = max(0, to_int(primary.get("added_count"), 0))
    fallback_enabled = env_bool("MINER_STAGEA_FALLBACK_ON_PAGE_UNLOCK_WAIT", True)
    fallback_max_categories = max(0, env_int("MINER_STAGEA_FALLBACK_MAX_CATEGORIES", 8))
    fallback_runs: List[Dict[str, Any]] = []
    daily_limit_hit = bool(primary.get("daily_limit_reached"))

    if fallback_enabled and primary_reason == "page_unlock_wait" and total_added < target_new_seeds and not daily_limit_hit:
        fallback_candidates = _phase_a_fallback_categories(category_key)
        for fb_idx, (fb_key, fb_label, fb_row) in enumerate(fallback_candidates, start=1):
            if fallback_max_categories > 0 and fb_idx > fallback_max_categories:
                break
            if total_added >= target_new_seeds:
                break
            if callable(progress_callback):
                progress_callback(
                    {
                        "phase": "seed_refill_fallback_category",
                        "message": f"{category_label}待機中のため、{fb_label}のseed補充を実行しています",
                        "flow_stage": "A",
                        "flow_stage_label": "A: seed補充",
                        "flow_stage_index": 1,
                        "flow_stage_total": 3,
                        "current_seed_query": str(fb_key),
                    }
                )
            fb_summary = _refill_seed_pool(
                conn,
                category_key=fb_key,
                category_label=fb_label,
                category_row=fb_row if isinstance(fb_row, dict) else {},
                stage_a_big_word_limit=max(0, int(stage_a_big_word_limit)),
                stage_a_minimize_transitions=bool(stage_a_minimize_transitions),
                refill_timebox_override_sec=refill_timebox_override_sec,
                progress_callback=progress_callback,
            )
            fb_added = max(0, to_int(fb_summary.get("added_count"), 0))
            total_added += fb_added
            fallback_runs.append(
                {
                    "category_key": fb_key,
                    "category_label": fb_label,
                    "reason": str(fb_summary.get("reason", "") or ""),
                    "added_count": int(fb_added),
                    "daily_limit_reached": bool(fb_summary.get("daily_limit_reached")),
                    "cooldown_until": str(fb_summary.get("cooldown_until", "") or "").strip(),
                }
            )
            if bool(fb_summary.get("daily_limit_reached")):
                daily_limit_hit = True
                break

    primary["fallback_on_page_unlock_wait_enabled"] = bool(fallback_enabled)
    primary["fallback_target_new_seeds"] = int(target_new_seeds)
    primary["fallback_total_added_count"] = int(total_added)
    primary["fallback_refill_runs"] = list(fallback_runs)
    primary["phase_a_completed_with_fallback"] = bool(total_added >= target_new_seeds)
    if primary_reason == "page_unlock_wait" and bool(primary["phase_a_completed_with_fallback"]):
        primary["reason"] = "target_reached_with_fallback"
        primary["cooldown_until"] = ""
    return primary


def _refill_seed_pool(
    conn: Any,
    *,
    category_key: str,
    category_label: str,
    category_row: Dict[str, Any],
    stage_a_big_word_limit: int = 0,
    stage_a_minimize_transitions: bool = False,
    refill_timebox_override_sec: Optional[int] = None,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    now_ts = int(time.time())
    refill_started = time.monotonic()
    refill_trigger_available_le = max(0, env_int("MINER_SEED_POOL_REFILL_THRESHOLD", 0))
    run_batch_size = max(1, env_int("MINER_SEED_POOL_RUN_BATCH_SIZE", 20))
    refill_timebox_sec = max(30, env_int("MINER_SEED_POOL_REFILL_TIMEBOX_SEC", 300))
    if refill_timebox_override_sec is not None:
        refill_timebox_sec = max(30, min(refill_timebox_sec, int(max(1, refill_timebox_override_sec))))
    refill_timebox_base_sec = int(refill_timebox_sec)
    max_timeout_pages_per_run = max(1, env_int("MINER_SEED_POOL_MAX_TIMEOUT_PAGES_PER_RUN", 2))
    query_page_unlock_enabled = env_bool("MINER_STAGEA_QUERY_PAGE_UNLOCK_ENABLED", True)
    query_page_unlock_default_hours = max(0.25, env_float("MINER_STAGEA_QUERY_PAGE_UNLOCK_HOURS_DEFAULT", 24.0))
    query_page_unlock_min_pages = max(0, env_int("MINER_STAGEA_QUERY_PAGE_UNLOCK_MIN_PAGES", 1))
    query_page_unlock_category_overrides = _load_query_page_unlock_hours_from_category_row(category_row)
    query_page_unlock_overrides = _load_query_page_unlock_hours_overrides(
        os.getenv("MINER_STAGEA_QUERY_PAGE_UNLOCK_HOURS_JSON", "")
    )
    pr_category_filter = _resolve_ebay_pr_category_filter(category_key, category_row)
    min_seed_sold_price_usd = _category_seed_min_sold_price_usd(category_key, category_row)
    _normalize_existing_seed_rows(conn, category_key=category_key)
    available = _count_available(conn, category_key=category_key, now_ts=now_ts)
    summary: Dict[str, Any] = {
        "ran": False,
        "available_before": available,
        "available_after": available,
        "pre_refill_pruned_non_model_seed_count": 0,
        "added_count": 0,
        "bootstrap_added_count": 0,
        "skipped_fresh_pages": 0,
        "page_runs": [],
        "reason": "threshold_not_reached",
        "daily_limit_reached": False,
        "cooldown_until": "",
        "cooldown_active_count": 0,
        "low_liquidity_cooldown_active_count": 0,
        "used_cooldown_active_count": 0,
        "cooldown_blocked_count": 0,
        "used_cooldown_blocked_count": 0,
        "query": "",
        "queries": [],
        "big_word_limit": max(0, int(stage_a_big_word_limit)),
        "big_word_count": 0,
        "big_word_total_count": 0,
        "big_word_scale_ratio": 1.0,
        "target_count": 0,
        "target_count_base": 0,
        "timebox_sec": int(refill_timebox_sec),
        "timebox_base_sec": int(refill_timebox_base_sec),
        "trace_path": "",
        "trace_session": "",
        "minimize_transitions": bool(stage_a_minimize_transitions),
        "transition_page_size": 0,
        "transition_max_pages_per_query": 0,
        "query_page_unlock_enabled": bool(query_page_unlock_enabled),
        "query_page_unlock_default_hours": round(float(query_page_unlock_default_hours), 4),
        "query_page_unlock_min_pages": int(query_page_unlock_min_pages),
        "query_page_unlock_category_overrides_count": int(len(query_page_unlock_category_overrides)),
        "query_page_unlock_overrides_count": int(len(query_page_unlock_overrides)),
        "pr_category_id": int(pr_category_filter.get("category_id", 0) or 0),
        "pr_category_slug": str(pr_category_filter.get("category_slug", "") or ""),
        "pr_category_filter_enabled": bool(pr_category_filter.get("enabled", False)),
        "min_seed_sold_price_usd": float(min_seed_sold_price_usd),
        "min_price_filtered_count": 0,
    }
    trace_path = _seed_refill_trace_path()
    trace_session = f"seed-refill-{int(time.time() * 1000)}"
    trace_seq = 0
    if trace_path is not None:
        summary["trace_path"] = str(trace_path)
        summary["trace_session"] = trace_session

    def _trace(event: str, **payload: Any) -> None:
        nonlocal trace_seq
        if trace_path is None:
            return
        trace_seq += 1
        row: Dict[str, Any] = {
            "ts": utc_iso(),
            "event": str(event or "").strip() or "unknown",
            "seq": int(trace_seq),
            "trace_session": trace_session,
            "category_key": category_key,
            "category_label": category_label,
        }
        row.update(payload)
        _append_seed_refill_trace(trace_path, row)

    _trace(
        "refill_start",
        refill_trigger_available_le=int(refill_trigger_available_le),
        available_before=int(available),
        timebox_sec=int(refill_timebox_sec),
        page_size=int(max(10, min(100, env_int("MINER_SEED_POOL_PAGE_SIZE", 50)))),
        max_pages=int(max(1, min(40, env_int("MINER_SEED_POOL_MAX_PAGES", 40)))),
        target_count_base=int(max(20, env_int("MINER_SEED_POOL_TARGET_COUNT", 100))),
        stage_a_big_word_limit=max(0, int(stage_a_big_word_limit)),
        pr_category_filter=dict(pr_category_filter),
    )
    active_low_liquidity_cooldown_keys = _load_active_low_liquidity_seed_keys(
        conn,
        category_key=category_key,
        now_ts=now_ts,
    )
    active_used_cooldown_keys = _load_active_seed_usage_cooldown_keys(
        conn,
        category_key=category_key,
        now_ts=now_ts,
    )
    summary["low_liquidity_cooldown_active_count"] = int(len(active_low_liquidity_cooldown_keys))
    summary["used_cooldown_active_count"] = int(len(active_used_cooldown_keys))
    summary["cooldown_active_count"] = int(len(active_low_liquidity_cooldown_keys) + len(active_used_cooldown_keys))
    strict_model_seed = _category_requires_strict_model_seed(category_key, category_row)
    summary["strict_model_seed"] = bool(strict_model_seed)
    if bool(strict_model_seed):
        pruned_count = _prune_non_model_seed_rows(conn, category_key=category_key)
        summary["pre_refill_pruned_non_model_seed_count"] = int(pruned_count)
        if pruned_count > 0:
            available = _count_available(conn, category_key=category_key, now_ts=now_ts)
            summary["available_before"] = int(available)
            _trace(
                "prune_non_model_seed_rows",
                pruned_count=int(pruned_count),
                available_before=int(available),
            )

    if available > refill_trigger_available_le:
        _trace(
            "refill_skipped_threshold",
            available_before=int(available),
            threshold=int(refill_trigger_available_le),
            cooldown_active_count=int(len(active_low_liquidity_cooldown_keys)),
        )
        return summary

    active_pool_seed_keys = _load_active_seed_keys(conn, category_key=category_key, now_ts=now_ts)
    existing_keys = set(active_pool_seed_keys)
    existing_keys.update(active_low_liquidity_cooldown_keys)
    existing_keys.update(active_used_cooldown_keys)
    run_added_seed_keys: Set[str] = set()
    ttl_days = max(1, env_int("MINER_SEED_POOL_TTL_DAYS", 7))
    bootstrap_enabled = env_bool("MINER_SEED_POOL_BOOTSTRAP_ENABLED", False)
    if bootstrap_enabled:
        bootstrap_target = max(run_batch_size, env_int("MINER_SEED_POOL_BOOTSTRAP_TARGET", 60))
        bootstrap_rows = _build_bootstrap_seed_rows(
            category_key=category_key,
            category_label=category_label,
            category_row=category_row,
            existing_keys=existing_keys,
            max_rows=bootstrap_target,
        )
        bootstrap_added = _insert_seed_rows(
            conn,
            category_key=category_key,
            rows=bootstrap_rows,
            ttl_days=ttl_days,
            strict_model_only=bool(strict_model_seed),
        )
        summary["bootstrap_added_count"] = max(0, int(bootstrap_added))
        if summary["bootstrap_added_count"] > 0:
            summary["ran"] = True
            summary["reason"] = "bootstrap_refilled"

        available = _count_available(conn, category_key=category_key, now_ts=now_ts)
        summary["available_after"] = available
        if available > refill_trigger_available_le:
            _trace(
                "refill_bootstrap_only",
                bootstrap_added_count=int(summary["bootstrap_added_count"]),
                available_after=int(available),
                threshold=int(refill_trigger_available_le),
            )
            _upsert_refill_state(
                conn,
                category_key=category_key,
                last_refill_status=str(summary.get("reason", "") or "bootstrap_refilled"),
                last_refill_message=f"{category_label}: bootstrap +{summary['bootstrap_added_count']} seeds",
                last_rank_checked=0,
                cooldown_until="",
            )
            return summary

    state = _load_refill_state(conn, category_key=category_key)
    cooldown_until = str(state.get("cooldown_until", "") or "").strip()
    cooldown_ts = iso_to_epoch(cooldown_until)
    if cooldown_ts > now_ts:
        summary["reason"] = "category_cooldown"
        summary["cooldown_until"] = cooldown_until
        _trace(
            "refill_skipped_category_cooldown",
            cooldown_until=str(cooldown_until),
            cooldown_active_count=int(len(active_low_liquidity_cooldown_keys)),
        )
        _upsert_refill_state(
            conn,
            category_key=category_key,
            last_refill_status="category_cooldown",
            last_refill_message=f"{category_label}: cooldown active",
            last_rank_checked=max(0, to_int(state.get("last_rank_checked"), 0)),
            cooldown_until=cooldown_until,
        )
        return summary

    summary["ran"] = True
    summary["reason"] = "refilled"
    page_size = max(10, min(100, env_int("MINER_SEED_POOL_PAGE_SIZE", 50)))
    # 1ページ50件 x 最大40ページ = 調査上限2000件
    max_pages = max(1, min(40, env_int("MINER_SEED_POOL_MAX_PAGES", 40)))
    query_page_unlock_initial_pages = max(
        1,
        min(
            max_pages,
            env_int("MINER_STAGEA_QUERY_PAGE_UNLOCK_INITIAL_PAGES", max_pages),
        ),
    )
    summary["query_page_unlock_initial_pages"] = int(query_page_unlock_initial_pages)
    if bool(stage_a_minimize_transitions):
        transition_page_size = max(50, min(200, env_int("MINER_STAGEA_TRANSITION_PAGE_SIZE", 200)))
        transition_max_pages_per_query = max(1, min(5, env_int("MINER_STAGEA_TRANSITION_MAX_PAGES_PER_QUERY", 1)))
        page_size = max(page_size, transition_page_size)
        max_pages = min(max_pages, transition_max_pages_per_query)
        summary["transition_page_size"] = int(transition_page_size)
        summary["transition_max_pages_per_query"] = int(transition_max_pages_per_query)
    # 新規seed目標は100件（達成後も現在ページは完走する）
    target_count_base = max(20, env_int("MINER_SEED_POOL_TARGET_COUNT", 100))
    target_count = int(target_count_base)
    min_pages_before_low_yield_stop = max(1, env_int("MINER_SEED_POOL_MIN_PAGES_BEFORE_LOW_YIELD_STOP", 2))
    low_yield_consecutive_limit = max(2, env_int("MINER_SEED_POOL_LOW_YIELD_CONSECUTIVE_PAGES", 3))
    cooldown_days = max(1, env_int("MINER_SEED_POOL_COOLDOWN_DAYS", 7))
    freshness_days = max(0, env_int("MINER_SEED_POOL_PAGE_FRESH_DAYS", 7))
    freshness_sec = freshness_days * 86400
    brand_hints = _brand_hints(category_row)
    all_big_words = _category_big_words(category_key, category_row)
    big_words = list(all_big_words)
    if int(stage_a_big_word_limit) > 0:
        big_words = big_words[: max(1, int(stage_a_big_word_limit))]
    selected_count = max(0, len(big_words))
    total_count = max(0, len(all_big_words))
    scale_ratio = 1.0
    if total_count > 0 and selected_count > 0:
        scale_ratio = max(0.05, min(1.0, float(selected_count) / float(total_count)))
    if total_count > 0 and selected_count < total_count:
        target_count = max(20, int(round(float(target_count_base) * scale_ratio)))
        refill_timebox_sec = max(30, int(round(float(refill_timebox_base_sec) * scale_ratio)))
    summary["query"] = big_words[0] if big_words else category_key
    summary["queries"] = list(big_words)
    summary["big_word_count"] = int(selected_count)
    summary["big_word_total_count"] = int(total_count)
    summary["big_word_scale_ratio"] = round(float(scale_ratio), 4)
    summary["target_count"] = int(target_count)
    summary["target_count_base"] = int(target_count_base)
    summary["timebox_sec"] = int(refill_timebox_sec)
    _trace(
        "refill_plan",
        queries=list(big_words),
        big_word_total_count=int(total_count),
        big_word_selected_count=int(selected_count),
        big_word_scale_ratio=float(scale_ratio),
        target_count_base=int(target_count_base),
        target_count=int(target_count),
        timebox_base_sec=int(refill_timebox_base_sec),
        timebox_sec=int(refill_timebox_sec),
        page_size=int(page_size),
        max_pages=int(max_pages),
    )

    added_total = 0
    last_rank = 0
    fetched_pages = 0
    skipped_fresh_pages = 0
    next_fresh_available_ts = 0
    rank_ceiling = max_pages * page_size
    query_runs: List[Dict[str, Any]] = []
    seed_api_state: Dict[str, Any] = {"usage_cache": None, "run_api_calls": 0}
    seed_api_attempts = 0
    seed_api_hits = 0
    seed_api_budget_skips = 0
    accessory_filtered_count = 0
    min_price_filtered_count = 0
    rpa_timeout_pages = 0
    rpa_failed_pages = 0
    bot_challenge_pages = 0
    daily_limit_pages = 0
    empty_result_pages = 0
    non_empty_result_pages = 0
    strict_filter_blocked_pages = 0
    sold_tab_unconfirmed_pages = 0
    lookback_unconfirmed_pages = 0
    page_unlock_limited_queries = 0
    page_unlock_blocked_pages = 0
    page_unlock_next_ts = 0
    page_reason_counts: Dict[str, int] = {}
    failure_samples: List[Dict[str, Any]] = []
    diagnostic_max_failure_pages = max(1, min(12, env_int("MINER_SEED_POOL_DIAGNOSTIC_MAX_FAILURE_PAGES", 4)))

    total_page_budget = max(1, len(big_words) * max_pages)
    for query_index, query in enumerate(big_words, start=1):
        if added_total >= target_count:
            summary["reason"] = "target_reached"
            break
        query_key = _query_window_key(query)
        page_window_entries = _load_page_window_entries(conn, category_key=category_key, query_key=query_key)
        latest_fetched_ts = 0
        if page_window_entries:
            latest_fetched_ts = max(
                max(0, to_int(meta.get("fetched_ts"), 0))
                for meta in page_window_entries.values()
                if isinstance(meta, dict)
            )
        unlock_hours, unlock_hours_source = _resolve_query_page_unlock_hours(
            category_key=category_key,
            query_key=query_key,
            default_hours=query_page_unlock_default_hours,
            category_overrides=query_page_unlock_category_overrides,
            overrides=query_page_unlock_overrides,
        )
        unlocked_pages, query_elapsed_sec = _compute_query_unlocked_pages(
            now_ts=now_ts,
            latest_fetched_ts=latest_fetched_ts,
            max_pages=max_pages,
            hours_per_page=unlock_hours,
            min_pages=query_page_unlock_min_pages,
            initial_pages=query_page_unlock_initial_pages,
        )
        if not bool(query_page_unlock_enabled):
            unlocked_pages = int(max_pages)
        if unlocked_pages < max_pages:
            page_unlock_limited_queries += 1
        query_fetch_quota = max(0, int(unlocked_pages))
        query_fetch_quota_remaining = int(query_fetch_quota)
        query_unlock_blocked_pages = 0
        query_next_unlock_ts = _compute_next_query_page_unlock_ts(
            latest_fetched_ts=latest_fetched_ts,
            unlocked_pages=unlocked_pages,
            max_pages=max_pages,
            hours_per_page=unlock_hours,
            now_ts=now_ts,
        )
        if query_next_unlock_ts > 0 and (page_unlock_next_ts <= 0 or query_next_unlock_ts < page_unlock_next_ts):
            page_unlock_next_ts = query_next_unlock_ts
        word_added_before = added_total
        word_fetched_pages = 0
        word_skipped_pages = 0
        word_last_rank = 0
        word_stop_reason = "query_scanned"
        zero_gain_pages = 0

        for page_index in range(max_pages):
            refill_elapsed = time.monotonic() - refill_started
            if refill_elapsed >= float(refill_timebox_sec):
                summary["reason"] = "refill_timebox_reached"
                word_stop_reason = "refill_timebox_reached"
                break
            if added_total >= target_count:
                summary["reason"] = "target_reached"
                word_stop_reason = "target_reached"
                break
            if bool(query_page_unlock_enabled) and query_fetch_quota_remaining <= 0:
                query_unlock_blocked_pages = max(0, max_pages - page_index)
                page_unlock_blocked_pages += query_unlock_blocked_pages
                word_stop_reason = "page_unlock_quota_reached"
                break
            offset = page_index * page_size
            if callable(progress_callback):
                page_step = ((query_index - 1) * max_pages) + page_index + 1
                refill_progress = min(52.0, 6.0 + (46.0 * page_step / max(1, total_page_budget)))
                progress_callback(
                    {
                        "phase": "seed_refill_scanning",
                        "message": (
                            f"seed補充 {query_index}/{len(big_words)}語 "
                            f"(page {page_index + 1}/{max_pages}, +{added_total}/{target_count})"
                        ),
                        "progress_percent": round(refill_progress, 2),
                        "flow_stage": "A",
                        "flow_stage_label": "A: seed補充",
                        "flow_stage_index": 1,
                        "flow_stage_total": 3,
                        "current_seed_query": str(query),
                        "elapsed_sec": round(refill_elapsed, 3),
                    }
                )
            window = page_window_entries.get(offset, {}) if isinstance(page_window_entries.get(offset), dict) else {}
            fetched_at = to_int(window.get("fetched_ts"), 0)
            if fetched_at > 0 and (now_ts - fetched_at) < freshness_sec:
                skipped_fresh_pages += 1
                word_skipped_pages += 1
                ready_ts = fetched_at + freshness_sec
                if ready_ts > now_ts and (next_fresh_available_ts <= 0 or ready_ts < next_fresh_available_ts):
                    next_fresh_available_ts = ready_ts
                _trace(
                    "page_skipped_fresh",
                    query=str(query),
                    query_index=int(query_index),
                    page=int(page_index + 1),
                    offset=int(offset),
                    fetched_at=str(utc_iso(fetched_at)),
                    freshness_days=int(freshness_days),
                    ready_at=str(utc_iso(ready_ts)) if ready_ts > 0 else "",
                )
                continue

            result = _run_rpa_page(
                query=query,
                offset=offset,
                limit=page_size,
                category_id=int(pr_category_filter.get("category_id", 0) or 0),
                category_slug=str(pr_category_filter.get("category_slug", "") or ""),
                min_price_usd=float(min_seed_sold_price_usd),
            )
            if bool(query_page_unlock_enabled) and query_fetch_quota_remaining > 0:
                query_fetch_quota_remaining -= 1
            fetched_pages += 1
            word_fetched_pages += 1
            rows = result.get("rows", []) if isinstance(result.get("rows"), list) else []
            row = rows[0] if rows and isinstance(rows[0], dict) else {}
            row_meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            entries = _collect_row_entries(row) if row else []
            page_reason = str(result.get("reason", "") or "").strip().lower() or "unknown"
            page_reason_counts[page_reason] = int(page_reason_counts.get(page_reason, 0)) + 1
            if page_reason == "rpa_failed":
                rpa_failed_pages += 1
            if page_reason == "bot_challenge_detected":
                bot_challenge_pages += 1
            if bool(result.get("daily_limit_reached")):
                daily_limit_pages += 1
            if len(entries) <= 0:
                empty_result_pages += 1
            else:
                non_empty_result_pages += 1
            filter_state = row_meta.get("filter_state") if isinstance(row_meta.get("filter_state"), dict) else {}
            sold_tab_selected = bool(filter_state.get("sold_tab_selected")) if isinstance(filter_state, dict) else False
            lookback_selected = str(filter_state.get("lookback_selected", "") or "").strip().lower()
            strict_blocked = bool(filter_state.get("strict_blocked")) if isinstance(filter_state, dict) else False
            if strict_blocked:
                strict_filter_blocked_pages += 1
            if isinstance(filter_state, dict):
                if not sold_tab_selected:
                    sold_tab_unconfirmed_pages += 1
                if lookback_selected and ("90" not in lookback_selected):
                    lookback_unconfirmed_pages += 1
            failure_detected = page_reason != "ok" or len(entries) <= 0 or strict_blocked
            if failure_detected and len(failure_samples) < diagnostic_max_failure_pages:
                failure_samples.append(
                    {
                        "query": str(query),
                        "page": int(page_index + 1),
                        "offset": int(offset),
                        "reason": page_reason,
                        "returncode": int(to_int(result.get("returncode"), 0)),
                        "daily_limit_reached": bool(result.get("daily_limit_reached")),
                        "raw_result_count": int(max(0, to_int(row_meta.get("raw_row_count"), 0))),
                        "filtered_result_count": int(max(0, to_int(row_meta.get("filtered_row_count"), len(entries)))),
                        "sold_tab_selected": bool(sold_tab_selected),
                        "lookback_selected": lookback_selected,
                        "strict_blocked": bool(strict_blocked),
                        "strict_filter_reason": str(filter_state.get("strict_reason", "") or "")
                        if isinstance(filter_state, dict)
                        else "",
                        "stdout_tail": list(result.get("stdout_tail", []))[-3:]
                        if isinstance(result.get("stdout_tail"), list)
                        else [],
                        "stderr_tail": list(result.get("stderr_tail", []))[-3:]
                        if isinstance(result.get("stderr_tail"), list)
                        else [],
                    }
                )
            new_rows: List[Dict[str, Any]] = []
            title_traces: List[Dict[str, Any]] = []
            page_duplicate_pool_count = 0
            page_duplicate_run_count = 0
            page_cooldown_blocked_count = 0
            page_min_price_filtered_count = 0
            page_accessory_filtered_count = 0
            page_empty_seed_count = 0
            page_generated_seed_count = 0
            for idx, entry in enumerate(entries, start=1):
                title = str(entry.get("title", "") or "").strip()
                if not title:
                    continue
                title_trace: Dict[str, Any] = {
                    "rank": max(1, to_int(entry.get("rank"), idx)),
                    "title": title,
                    "entry_source": str(entry.get("seed_entry_source", "") or "filtered").strip().lower() or "filtered",
                    "item_url": str(entry.get("item_url", "") or "").strip(),
                    "sold_90d_count": to_int(entry.get("sold_90d_count"), -1),
                    "sold_price_min_90d": to_float(entry.get("sold_price_min_90d"), -1.0),
                    "title_seed_candidates": [],
                    "final_seed_candidates": [],
                    "decisions": [],
                    "status": "processing",
                }
                if _is_accessory_title(title):
                    accessory_filtered_count += 1
                    page_accessory_filtered_count += 1
                    title_trace["status"] = "accessory_filtered"
                    title_traces.append(title_trace)
                    continue
                title_candidates = _extract_seed_queries_from_title(
                    title,
                    brand_hints,
                    prefer_strict_model_seed=bool(strict_model_seed),
                )
                seed_candidates = list(title_candidates)
                entry_source = str(entry.get("seed_entry_source", "") or "filtered").strip().lower()
                extraction_mode = "title_raw_fallback" if entry_source in {"raw_fallback", "query_fallback"} else "title"
                api_backfill_reason = ""
                if bool(strict_model_seed):
                    needs_api_backfill = (not seed_candidates) or (not _seed_candidates_have_model_or_gtin(seed_candidates))
                else:
                    needs_api_backfill = (not seed_candidates) or (not any(_looks_specific_seed(v) for v in seed_candidates))
                title_trace["title_seed_candidates"] = list(title_candidates)
                if needs_api_backfill:
                    seed_api_attempts += 1
                    item_url = str(entry.get("item_url", "") or "").strip()
                    if item_url:
                        try:
                            api_candidates, api_reason = _api_seed_candidates_from_item_url(
                                item_url=item_url,
                                brand_hints=brand_hints,
                                timeout=max(6, env_int("MINER_SEED_API_SUPPLEMENT_TIMEOUT_SECONDS", 10)),
                                state=seed_api_state,
                            )
                        except Exception as err:
                            api_candidates, api_reason = [], f"error:{type(err).__name__}"
                        api_backfill_reason = str(api_reason or "")
                        if api_backfill_reason in {
                            "run_budget_exhausted",
                            "daily_budget_exhausted",
                            "hourly_budget_exhausted",
                            "run_budget_zero",
                        }:
                            seed_api_budget_skips += 1
                        if api_candidates:
                            seed_api_hits += 1
                            extraction_mode = "title+api" if title_candidates else "api"
                            merged: List[str] = []
                            seen_keys: Set[str] = set()
                            for raw_seed in [*api_candidates, *title_candidates]:
                                text = _normalize_seed_query(raw_seed)
                                skey = _seed_key(text)
                                if not text or len(skey) < 4 or skey in seen_keys:
                                    continue
                                if bool(strict_model_seed) and not _seed_query_is_model_or_gtin(text):
                                    continue
                                seen_keys.add(skey)
                                merged.append(text)
                            seed_candidates = merged
                    else:
                        api_backfill_reason = "missing_item_url"
                if not seed_candidates:
                    page_empty_seed_count += 1
                    title_trace["status"] = "no_seed_extracted"
                    title_trace["seed_api_backfill_reason"] = api_backfill_reason
                    title_traces.append(title_trace)
                    continue
                page_generated_seed_count += int(len(seed_candidates))
                title_trace["final_seed_candidates"] = list(seed_candidates)
                rank = max(1, to_int(entry.get("rank"), idx))
                global_rank = offset + rank
                word_last_rank = max(word_last_rank, global_rank)
                last_rank = max(last_rank, global_rank)
                accepted_count = 0
                for raw_seed_query in seed_candidates:
                    seed_query = _normalize_seed_query(raw_seed_query)
                    decision: Dict[str, Any] = {
                        "raw_seed_query": str(raw_seed_query or ""),
                        "normalized_seed_query": seed_query,
                    }
                    if not seed_query:
                        decision["outcome"] = "invalid_normalized"
                        title_trace["decisions"].append(decision)
                        continue
                    sold_price_min_90d = to_float(entry.get("sold_price_min_90d"), -1.0)
                    if (
                        min_seed_sold_price_usd > 0
                        and sold_price_min_90d > 0
                        and sold_price_min_90d < min_seed_sold_price_usd
                    ):
                        min_price_filtered_count += 1
                        page_min_price_filtered_count += 1
                        decision["outcome"] = "filtered_min_price"
                        decision["seed_key"] = _seed_key(seed_query)
                        title_trace["decisions"].append(decision)
                        continue
                    skey = _seed_key(seed_query)
                    decision["seed_key"] = skey
                    if skey in active_low_liquidity_cooldown_keys or skey in active_used_cooldown_keys:
                        summary["cooldown_blocked_count"] = int(summary.get("cooldown_blocked_count", 0)) + 1
                        if skey in active_used_cooldown_keys:
                            summary["used_cooldown_blocked_count"] = int(
                                summary.get("used_cooldown_blocked_count", 0)
                            ) + 1
                        page_cooldown_blocked_count += 1
                        decision["outcome"] = "blocked_cooldown"
                        title_trace["decisions"].append(decision)
                        continue
                    if skey in existing_keys:
                        if skey in run_added_seed_keys:
                            page_duplicate_run_count += 1
                            decision["outcome"] = "duplicate_run"
                        elif skey in active_pool_seed_keys:
                            page_duplicate_pool_count += 1
                            decision["outcome"] = "duplicate_pool"
                        else:
                            page_duplicate_pool_count += 1
                            decision["outcome"] = "duplicate_existing"
                        title_trace["decisions"].append(decision)
                        continue
                    existing_keys.add(skey)
                    run_added_seed_keys.add(skey)
                    accepted_count += 1
                    decision["outcome"] = "accepted_new_seed"
                    title_trace["decisions"].append(decision)
                    new_rows.append(
                        {
                            "seed_query": seed_query,
                            "source_title": title,
                            "source_item_url": str(entry.get("item_url", "") or "").strip(),
                            "source_page": page_index + 1,
                            "source_offset": offset,
                            "source_rank": global_rank,
                            "metadata": {
                                "query": query,
                                "category_key": category_key,
                                "category_label": category_label,
                                "seed_collected_at": utc_iso(now_ts),
                                "seed_collected_sold_90d_count": to_int(entry.get("sold_90d_count"), -1),
                                "seed_collected_sold_price_min_usd": sold_price_min_90d,
                                "seed_quality_score": 0,
                                "seed_extraction_mode": extraction_mode,
                                "seed_api_backfill_reason": api_backfill_reason,
                            },
                        }
                    )
                title_trace["seed_extraction_mode"] = extraction_mode
                title_trace["seed_api_backfill_reason"] = api_backfill_reason
                title_trace["accepted_seed_count"] = int(accepted_count)
                title_trace["status"] = "accepted" if accepted_count > 0 else "all_filtered_or_duplicate"
                title_traces.append(title_trace)

            inserted = _insert_seed_rows(
                conn,
                category_key=category_key,
                rows=new_rows,
                ttl_days=ttl_days,
                strict_model_only=bool(strict_model_seed),
            )
            if max(0, int(inserted)) > 0:
                zero_gain_pages = 0
            else:
                zero_gain_pages += 1
            _upsert_page_window_entry(
                conn,
                category_key=category_key,
                query_key=query_key,
                page_offset=offset,
                page_size=page_size,
                result_count=len(entries),
                new_seed_count=max(0, int(inserted)),
            )
            page_window_entries[offset] = {
                "fetched_ts": now_ts,
                "result_count": len(entries),
                "new_seed_count": max(0, int(inserted)),
            }
            added_total += max(0, int(inserted))
            summary["page_runs"].append(
                {
                    "query": query,
                    "page": page_index + 1,
                    "offset": offset,
                    "new_seed_count": max(0, int(inserted)),
                    "raw_result_count": len(entries),
                    "daily_limit_reached": bool(result.get("daily_limit_reached")),
                    "reason": str(result.get("reason", "") or ""),
                }
            )
            _trace(
                "page_processed",
                query=str(query),
                query_index=int(query_index),
                page=int(page_index + 1),
                offset=int(offset),
                limit=int(page_size),
                rpa_result={
                    "ok": bool(result.get("ok")),
                    "reason": str(result.get("reason", "") or ""),
                    "returncode": to_int(result.get("returncode"), 0),
                    "daily_limit_reached": bool(result.get("daily_limit_reached")),
                    "elapsed_sec": round(to_float(result.get("elapsed_sec"), 0.0), 4),
                    "timeout_sec": max(0, to_int(result.get("timeout_sec"), 0)),
                    "pause_for_login_sec": max(0, to_int(result.get("pause_for_login_sec"), 0)),
                    "search_params": dict(result.get("rpa_search_params", {}))
                    if isinstance(result.get("rpa_search_params"), dict)
                    else {},
                    "stdout_tail": list(result.get("stdout_tail", []))
                    if isinstance(result.get("stdout_tail"), list)
                    else [],
                    "stderr_tail": list(result.get("stderr_tail", []))
                    if isinstance(result.get("stderr_tail"), list)
                    else [],
                },
                pr_row_metadata={
                    "timings": dict(row_meta.get("timings", {})) if isinstance(row_meta.get("timings"), dict) else {},
                    "filter_state": dict(row_meta.get("filter_state", {}))
                    if isinstance(row_meta.get("filter_state"), dict)
                    else {},
                    "raw_row_count": max(0, to_int(row_meta.get("raw_row_count"), 0)),
                    "filtered_row_count": max(0, to_int(row_meta.get("filtered_row_count"), 0)),
                    "sold_90d_count": to_int(row.get("sold_90d_count"), -1),
                    "sold_price_min": to_float(row.get("sold_price_min"), -1.0),
                    "sold_price_median": to_float(row.get("sold_price_median"), -1.0),
                },
                page_stats={
                    "entries_count": int(len(entries)),
                    "titles_processed": int(len(title_traces)),
                    "generated_seed_candidates": int(page_generated_seed_count),
                    "inserted_seed_count": int(max(0, int(inserted))),
                    "duplicate_pool_count": int(page_duplicate_pool_count),
                    "duplicate_run_count": int(page_duplicate_run_count),
                    "cooldown_blocked_count": int(page_cooldown_blocked_count),
                    "min_price_filtered_count": int(page_min_price_filtered_count),
                    "accessory_filtered_count": int(page_accessory_filtered_count),
                    "no_seed_extracted_count": int(page_empty_seed_count),
                },
                titles=title_traces,
            )
            try:
                conn.commit()
            except Exception:
                pass
            if bool(result.get("daily_limit_reached")):
                summary["daily_limit_reached"] = True
                summary["reason"] = "daily_limit_reached"
                word_stop_reason = "daily_limit_reached"
                break
            if page_reason == "bot_challenge_detected":
                summary["reason"] = "bot_challenge_detected"
                word_stop_reason = "bot_challenge_detected"
                break
            if to_int(result.get("returncode"), 0) == -9:
                rpa_timeout_pages += 1
                word_stop_reason = "rpa_timeout"
                if rpa_timeout_pages >= max_timeout_pages_per_run:
                    summary["reason"] = "rpa_timeout_guard"
                break
            if len(entries) <= 0:
                word_stop_reason = "empty_result_page"
                break
            if (
                zero_gain_pages >= low_yield_consecutive_limit
                and (page_index + 1) >= min_pages_before_low_yield_stop
                and added_total < target_count
            ):
                word_stop_reason = "low_yield_stop"
                break

        query_runs.append(
            {
                "query": query,
                "added_count": max(0, added_total - word_added_before),
                "fetched_pages": word_fetched_pages,
                "skipped_fresh_pages": word_skipped_pages,
                "last_rank_checked": word_last_rank,
                "stop_reason": word_stop_reason,
                "page_unlock": {
                    "enabled": bool(query_page_unlock_enabled),
                    "hours_per_page": round(float(unlock_hours), 4),
                    "hours_source": str(unlock_hours_source),
                    "elapsed_sec": int(query_elapsed_sec),
                    "latest_fetched_at": str(utc_iso(latest_fetched_ts)) if latest_fetched_ts > 0 else "",
                    "fetch_quota_pages": int(query_fetch_quota),
                    "remaining_quota_pages": int(max(0, query_fetch_quota_remaining)),
                    "blocked_pages": int(query_unlock_blocked_pages),
                    "next_unlock_at": str(utc_iso(query_next_unlock_ts)) if query_next_unlock_ts > 0 else "",
                },
            }
        )
        _trace(
            "query_completed",
            query=str(query),
            query_index=int(query_index),
            added_count=max(0, int(added_total - word_added_before)),
            fetched_pages=int(word_fetched_pages),
            skipped_fresh_pages=int(word_skipped_pages),
            last_rank_checked=int(word_last_rank),
            stop_reason=str(word_stop_reason or ""),
        )
        if bool(summary.get("daily_limit_reached")):
            break
        if str(summary.get("reason", "") or "") in {"refill_timebox_reached", "rpa_timeout_guard", "bot_challenge_detected"}:
            break

    available_after = _count_available(conn, category_key=category_key, now_ts=now_ts)
    summary["added_count"] = added_total
    summary["skipped_fresh_pages"] = skipped_fresh_pages
    summary["available_after"] = available_after
    summary["last_rank_checked"] = last_rank
    summary["query_runs"] = query_runs
    summary["low_yield_stop_query_count"] = int(
        sum(1 for row in query_runs if str((row or {}).get("stop_reason", "") or "") == "low_yield_stop")
    )
    summary["accessory_filtered_count"] = int(accessory_filtered_count)
    summary["min_price_filtered_count"] = int(min_price_filtered_count)
    summary["fetched_pages"] = int(fetched_pages)
    summary["rpa_timeout_pages"] = int(rpa_timeout_pages)
    summary["diagnostics"] = {
        "rpa_failed_pages": int(rpa_failed_pages),
        "bot_challenge_pages": int(bot_challenge_pages),
        "rpa_timeout_pages": int(rpa_timeout_pages),
        "daily_limit_pages": int(daily_limit_pages),
        "empty_result_pages": int(empty_result_pages),
        "non_empty_result_pages": int(non_empty_result_pages),
        "strict_filter_blocked_pages": int(strict_filter_blocked_pages),
        "sold_tab_unconfirmed_pages": int(sold_tab_unconfirmed_pages),
        "lookback_unconfirmed_pages": int(lookback_unconfirmed_pages),
        "page_unlock_enabled": bool(query_page_unlock_enabled),
        "page_unlock_limited_queries": int(page_unlock_limited_queries),
        "page_unlock_blocked_pages": int(page_unlock_blocked_pages),
        "page_reason_counts": dict(sorted(page_reason_counts.items(), key=lambda kv: kv[0])),
        "failure_samples": list(failure_samples),
    }
    summary["refill_elapsed_sec"] = round(max(0.0, float(time.monotonic() - refill_started)), 3)
    summary["seed_api_backfill"] = {
        "attempts": int(seed_api_attempts),
        "api_calls": int(to_int(seed_api_state.get("run_api_calls"), 0)),
        "hits": int(seed_api_hits),
        "budget_skips": int(seed_api_budget_skips),
    }
    reason_now = str(summary.get("reason", "") or "")
    cooldown_text = ""
    if bool(summary.get("daily_limit_reached")):
        summary["reason"] = "daily_limit_reached"
    elif reason_now == "bot_challenge_detected":
        bot_cooldown_minutes = max(5, env_int("MINER_SEED_POOL_BOT_CHALLENGE_COOLDOWN_MINUTES", 30))
        cooldown_text = utc_iso(now_ts + (bot_cooldown_minutes * 60))
        summary["cooldown_until"] = cooldown_text
    elif reason_now == "rpa_timeout_guard":
        timeout_cooldown_hours = max(1, env_int("MINER_SEED_POOL_TIMEOUT_COOLDOWN_HOURS", 1))
        cooldown_text = utc_iso(now_ts + timeout_cooldown_hours * 3600)
        summary["cooldown_until"] = cooldown_text
    elif added_total >= target_count:
        summary["reason"] = "target_reached"
    elif reason_now == "refill_timebox_reached":
        pass
    elif fetched_pages <= 0 and skipped_fresh_pages > 0:
        summary["reason"] = "fresh_window_skip"
        if next_fresh_available_ts > now_ts:
            cooldown_text = utc_iso(next_fresh_available_ts)
            summary["cooldown_until"] = cooldown_text
    elif (
        bool(query_page_unlock_enabled)
        and fetched_pages <= 0
        and page_unlock_limited_queries > 0
        and page_unlock_next_ts > now_ts
    ):
        summary["reason"] = "page_unlock_wait"
        cooldown_text = utc_iso(page_unlock_next_ts)
        summary["cooldown_until"] = cooldown_text
    elif fetched_pages >= max(1, len(big_words)) * max_pages and added_total < target_count:
        summary["reason"] = "rank_limit_cooldown"
        cooldown_text = utc_iso(now_ts + cooldown_days * 86400)
        summary["cooldown_until"] = cooldown_text
    elif added_total <= 0:
        summary["reason"] = "all_big_words_exhausted"
        cooldown_text = utc_iso(now_ts + cooldown_days * 86400)
        summary["cooldown_until"] = cooldown_text
    elif str(summary.get("reason", "") or "") == "refilled":
        summary["reason"] = "partial_refill"

    if str(summary.get("reason", "") or "") == "empty_result_page" and available_after <= 0:
        cooldown_text = utc_iso(now_ts + cooldown_days * 86400)
        summary["cooldown_until"] = cooldown_text
        summary["reason"] = "empty_result_cooldown"
    summary["rank_ceiling"] = int(rank_ceiling)
    summary["tuning_recommendations"] = _build_stage_a_tuning_recommendations(summary)

    refill_note = f"{category_label}: +{added_total} seeds"
    if summary["bootstrap_added_count"] > 0:
        refill_note += f" / bootstrap +{summary['bootstrap_added_count']} seeds"
    _upsert_refill_state(
        conn,
        category_key=category_key,
        last_refill_status=str(summary.get("reason", "") or "refilled"),
        last_refill_message=refill_note,
        last_rank_checked=max(0, int(summary.get("last_rank_checked", 0))),
        cooldown_until=cooldown_text,
    )
    _trace(
        "refill_completed",
        reason=str(summary.get("reason", "") or ""),
        added_count=max(0, to_int(summary.get("added_count"), 0)),
        available_before=max(0, to_int(summary.get("available_before"), 0)),
        available_after=max(0, to_int(summary.get("available_after"), 0)),
        fetched_pages=int(fetched_pages),
        skipped_fresh_pages=max(0, to_int(summary.get("skipped_fresh_pages"), 0)),
        low_yield_stop_query_count=max(0, to_int(summary.get("low_yield_stop_query_count"), 0)),
        rpa_timeout_pages=max(0, to_int(summary.get("rpa_timeout_pages"), 0)),
        daily_limit_reached=bool(summary.get("daily_limit_reached")),
        refill_elapsed_sec=round(to_float(summary.get("refill_elapsed_sec"), 0.0), 4),
        trace_path=str(trace_path) if trace_path is not None else "",
    )
    if trace_path is not None:
        summary["trace_path"] = str(trace_path)
        summary["trace_session"] = trace_session
    return summary


def _canonical_model_code(text: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(text or "").upper())


def _model_code_set(text: str) -> Set[str]:
    out: Set[str] = set()
    for code in _extract_codes(str(text or "")):
        normalized = _canonical_model_code(code)
        if normalized:
            out.add(normalized)
    return out


def _model_codes_equivalent(seed_code: str, candidate_code: str) -> bool:
    seed = str(seed_code or "").strip().upper()
    cand = str(candidate_code or "").strip().upper()
    if not seed or not cand:
        return False
    if seed == cand:
        return True
    shorter, longer = (seed, cand) if len(seed) <= len(cand) else (cand, seed)
    if len(shorter) < 6:
        return False
    if longer.startswith(shorter):
        suffix = longer[len(shorter) :]
        if suffix and len(suffix) <= 3 and bool(re.fullmatch(r"[A-Z0-9]{1,3}", suffix)):
            return True

    shorter, longer = (seed, cand) if len(seed) <= len(cand) else (cand, seed)
    if len(shorter) >= 6 and longer.endswith(shorter):
        prefix = longer[: len(longer) - len(shorter)]
        if 1 <= len(prefix) <= 4 and re.fullmatch(r"[A-Z0-9]{1,4}", prefix) and any(ch.isdigit() for ch in prefix):
            return True
    return False


def _query_tokens(text: str) -> List[str]:
    normalized = unicodedata.normalize("NFKC", str(text or "")).upper()
    normalized = re.sub(r"[^A-Z0-9\u3040-\u30FF\u3400-\u9FFF]+", " ", normalized)
    tokens: List[str] = []
    for raw in normalized.split():
        token = str(raw or "").strip()
        if len(token) < 2:
            continue
        if token in _SEED_STOPWORDS:
            continue
        tokens.append(token)
    return tokens


def _build_seed_match_context(*, seed_query: str, seed_source_title: str) -> Dict[str, Any]:
    return {
        "seed_codes": _model_code_set(seed_query) | _model_code_set(seed_source_title),
        "seed_tokens": set(_query_tokens(seed_query)),
        "broad_seed": not _looks_specific_seed(seed_query),
    }


def _seed_title_match_score(
    *,
    seed_query: str,
    seed_source_title: str,
    candidate_title: str,
    seed_match_context: Optional[Dict[str, Any]] = None,
) -> Tuple[float, str]:
    if _is_accessory_title(candidate_title):
        return 0.0, "accessory_title"

    context = seed_match_context if isinstance(seed_match_context, dict) else {}
    seed_codes = context.get("seed_codes")
    if not isinstance(seed_codes, set):
        seed_codes = _model_code_set(seed_query) | _model_code_set(seed_source_title)
    candidate_codes = _model_code_set(candidate_title)
    if seed_codes:
        if not candidate_codes:
            allow_token_rescue = env_bool("MINER_STAGE1_ALLOW_TOKEN_RESCUE_WHEN_CANDIDATE_MODEL_MISSING", True)
            if allow_token_rescue:
                seed_token_source = str(seed_source_title or seed_query or "")
                seed_tokens_raw = set(_query_tokens(seed_token_source))
                candidate_tokens = set(_query_tokens(candidate_title))
                seed_tokens = {
                    token
                    for token in seed_tokens_raw
                    if token not in _SEED_STOPWORDS
                    and not (any(ch.isdigit() for ch in token) and any("A" <= ch <= "Z" for ch in token))
                }
                common = seed_tokens.intersection(candidate_tokens)
                long_common = sum(1 for token in common if len(token) >= 4)
                if len(common) >= 2 and long_common >= 1:
                    # 型番欠損タイトルのみを保守的に救済する。
                    return 0.66, "candidate_model_missing_token_overlap"
            return 0.0, "candidate_model_missing"
        matched_seed_codes = {
            seed_code
            for seed_code in seed_codes
            if any(_model_codes_equivalent(seed_code, cand_code) for cand_code in candidate_codes)
        }
        if not matched_seed_codes:
            return 0.0, "model_code_mismatch"
        overlap = len(matched_seed_codes) / max(1, len(seed_codes))
        score = min(0.98, 0.82 + (0.12 * overlap))
        return float(score), "model_code_match"

    seed_tokens = context.get("seed_tokens")
    if not isinstance(seed_tokens, set):
        seed_tokens = set(_query_tokens(seed_query))
    candidate_tokens = set(_query_tokens(candidate_title))
    broad_seed_raw = context.get("broad_seed")
    broad_seed = bool(broad_seed_raw) if isinstance(broad_seed_raw, bool) else not _looks_specific_seed(seed_query)
    if not seed_tokens or not candidate_tokens:
        if broad_seed and candidate_codes:
            # 例: "PROSPEX" vs "セイコー プロスペックス SBDC101"。
            # token化では一致ゼロでも、型番がある候補は次段で再判定できるため通す。
            return 0.64, "token_overlap_relaxed_with_candidate_code"
        return 0.0, "token_missing"
    common = seed_tokens.intersection(candidate_tokens)
    if not common:
        if broad_seed and candidate_codes:
            return 0.64, "token_overlap_relaxed_with_candidate_code"
        return 0.0, "token_overlap_zero"
    jaccard = len(common) / max(1, len(seed_tokens.union(candidate_tokens)))
    score = min(0.86, 0.48 + (0.46 * jaccard))
    if broad_seed:
        # broad seedはtoken被りが薄くても、実運用での取りこぼしを減らすため最低スコアを設ける。
        score = max(score, 0.64)
    return float(score), "token_overlap"


def _stage1_candidate_match_text(item: MarketItem) -> str:
    parts: List[str] = [str(item.title or "").strip()]
    identifiers = item.identifiers if isinstance(item.identifiers, dict) else {}
    for key in ("model", "mpn", "manufacturerPartNumber", "sku", "jan", "ean", "upc"):
        raw = identifiers.get(key) if isinstance(identifiers, dict) else ""
        text = str(raw or "").strip()
        if text:
            parts.append(text)
    raw = item.raw if isinstance(item.raw, dict) else {}
    if isinstance(raw, dict):
        for key in ("itemCode", "model", "modelNumber", "mpn", "manufacturerPartNumber", "sku"):
            raw_val = raw.get(key)
            text = str(raw_val or "").strip()
            if text:
                parts.append(text)
    merged: List[str] = []
    seen: Set[str] = set()
    for raw_text in parts:
        text = re.sub(r"\s+", " ", str(raw_text or "").strip())
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        merged.append(text)
    return " ".join(merged).strip()


def _pick_jp_seed_query(
    *,
    seed_query: str,
    source_title: str,
    brand_hints: Sequence[str],
) -> str:
    candidates = _extract_seed_queries_from_title(source_title, brand_hints)
    if not candidates:
        return str(seed_query or "").strip()
    for row in candidates:
        if _looks_specific_seed(row):
            return str(row).strip()
    return str(candidates[0]).strip()


def _prefer_stage1_query_for_seed_only(*, seed_query: str, stage1_query: str) -> str:
    seed_text = _normalize_query_text(seed_query)
    stage1_text = _normalize_query_text(stage1_query)
    if not seed_text:
        return stage1_text
    if not stage1_text:
        return seed_text

    seed_codes = _model_code_set(seed_text)
    stage1_codes = _model_code_set(stage1_text)
    if seed_codes and stage1_codes:
        for seed_code in seed_codes:
            for stage1_code in stage1_codes:
                if stage1_code == seed_code:
                    continue
                if not stage1_code.startswith(seed_code):
                    continue
                suffix_len = len(stage1_code) - len(seed_code)
                if 1 <= suffix_len <= 3:
                    return stage1_text
    return seed_text


def _stage1_seed_only_strict_queries(
    *,
    seed_query: str,
    stage1_query: str,
    seed_source_title: str,
) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()

    def _push(raw: str) -> None:
        text = _normalize_query_text(raw)
        if not text:
            return
        key = text.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(text)

    def _push_domestic_suffix_trim_variant(raw_query: str) -> bool:
        brand = "Seiko" if "SEIKO" in f"{seed_query} {stage1_query} {seed_source_title}".upper() else ""
        for raw_code in _extract_codes(raw_query):
            code_text = str(raw_code or "").strip().upper()
            trimmed = re.sub(r"(J1|P1|K1)$", "", code_text)
            if trimmed == code_text or len(trimmed) < 5:
                continue
            if brand:
                _push(f"{brand} {trimmed}")
            else:
                _push(trimmed)
            return True
        return False

    preferred = _prefer_stage1_query_for_seed_only(seed_query=seed_query, stage1_query=stage1_query)
    _push(preferred)
    if len(out) < 2 and "SEIKO" in f"{seed_query} {stage1_query} {seed_source_title}".upper():
        if _push_domestic_suffix_trim_variant(preferred):
            return out[:2]
    for text in (preferred, stage1_query, seed_query, seed_source_title):
        for raw_code in _extract_codes(text):
            _push(raw_code)
            if len(out) >= 2:
                return out
    return out[:2]


def _phase_c_best_model_code(
    *,
    seed_query: str,
    jp_seed_query: str,
    seed_source_title: str = "",
    source_title: str = "",
    source_identifiers: Optional[Dict[str, Any]] = None,
) -> str:
    # A/B双方から候補型番を集め、共通出現(+重み)が高いものを優先する。
    score_map: Dict[str, float] = {}
    display_map: Dict[str, str] = {}
    order: List[str] = []

    def _add(raw_code: str, score: float) -> None:
        text = str(raw_code or "").strip()
        if not text:
            return
        canon = _canonical_model_code(text)
        if len(canon) < 4:
            return
        alpha = sum(1 for ch in canon if "A" <= ch <= "Z")
        digit = sum(1 for ch in canon if ch.isdigit())
        if alpha < 1 or digit < 1:
            return
        if canon not in score_map:
            order.append(canon)
            score_map[canon] = 0.0
            display_map[canon] = text
        score_map[canon] += float(score)
        current_disp = str(display_map.get(canon, "") or "")
        if len(text) > len(current_disp):
            display_map[canon] = text

    for code in _extract_codes(seed_query):
        _add(code, 3.0)
    for code in _extract_codes(jp_seed_query):
        _add(code, 4.0)
    for code in _extract_codes(seed_source_title):
        _add(code, 2.0)
    for code in _extract_codes(source_title):
        _add(code, 4.0)
    identifiers = source_identifiers if isinstance(source_identifiers, dict) else {}
    for raw_value in identifiers.values():
        for code in _extract_codes(str(raw_value or "")):
            _add(code, 5.0)

    if not score_map:
        return ""
    ranked = sorted(
        score_map.items(),
        key=lambda kv: (-float(kv[1]), -len(str(display_map.get(kv[0], "") or kv[0])), order.index(kv[0])),
    )
    best_canon = str(ranked[0][0] or "").strip()
    return str(display_map.get(best_canon, best_canon) or "").strip()


def _pick_liquidity_query(
    *,
    seed_query: str,
    jp_seed_query: str,
    seed_source_title: str = "",
    source_title: str = "",
    source_identifiers: Optional[Dict[str, Any]] = None,
) -> str:
    preferred_code = _phase_c_best_model_code(
        seed_query=seed_query,
        jp_seed_query=jp_seed_query,
        seed_source_title=seed_source_title,
        source_title=source_title,
        source_identifiers=source_identifiers if isinstance(source_identifiers, dict) else {},
    )
    if preferred_code:
        return preferred_code
    jp_query = re.sub(r"\s+", " ", str(jp_seed_query or "").strip())
    if not jp_query:
        jp_query = re.sub(r"\s+", " ", str(seed_query or "").strip())
    digits = re.sub(r"\D+", "", jp_query)
    is_digit_like = bool(digits and len(digits) == len(re.sub(r"\s+", "", jp_query)) and len(digits) >= 8)
    if is_digit_like:
        # GTIN/JANのみはヒットが弱いことがあるため、元seed側の型番を優先する。
        for code in _extract_codes(seed_query):
            token = str(code or "").strip()
            if _looks_specific_seed(token):
                return token
    return jp_query


def _normalize_query_text(raw: str) -> str:
    return re.sub(r"\s+", " ", str(raw or "").strip())


def _resolve_stage1_baseline_usd(
    *,
    seed_collected_sold_price_min_usd: float,
    category_min_seed_price_usd: float,
) -> Tuple[float, str]:
    seed_value = to_float(seed_collected_sold_price_min_usd, -1.0)
    if seed_value > 0:
        return float(seed_value), "seed_collected"
    category_value = max(0.0, to_float(category_min_seed_price_usd, 0.0))
    if category_value > 0:
        return float(category_value), "category_min"
    return -1.0, "unavailable"


def _seed_baseline_metric_pair(seed: Dict[str, Any]) -> Optional[Tuple[float, int]]:
    if not isinstance(seed, dict):
        return None
    sold_min = round(float(to_float(seed.get("seed_collected_sold_price_min_usd"), -1.0)), 2)
    sold_count = int(to_int(seed.get("seed_collected_sold_90d_count"), -1))
    if sold_min <= 0 or sold_count < 0:
        return None
    return sold_min, sold_count


def _build_seed_baseline_pair_counts(seeds: Sequence[Dict[str, Any]]) -> Dict[Tuple[float, int], int]:
    counts: Dict[Tuple[float, int], int] = {}
    for seed in seeds:
        pair = _seed_baseline_metric_pair(seed)
        if pair is None:
            continue
        counts[pair] = int(counts.get(pair, 0)) + 1
    return counts


def _seed_baseline_is_suspicious(
    seed: Dict[str, Any],
    *,
    pair_counts: Dict[Tuple[float, int], int],
) -> bool:
    pair = _seed_baseline_metric_pair(seed)
    if pair is None:
        return False
    threshold = max(2, env_int("MINER_STAGE1_BASELINE_PAIR_DUPLICATE_THRESHOLD", 6))
    return int(pair_counts.get(pair, 0)) >= threshold


def _stage1_site_queries(
    *,
    seed_query: str,
    stage1_query: str,
    seed_source_title: str,
    site: str,
    max_queries: int,
) -> List[str]:
    limit = max(1, int(max_queries))
    out: List[str] = []
    seen: Set[str] = set()

    def _push(raw: str) -> None:
        text = re.sub(r"\s+", " ", str(raw or "").strip())
        if not text:
            return
        key = text.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(text)

    code_queries: List[str] = []
    code_seen: Set[str] = set()
    for text in (stage1_query, seed_query, seed_source_title):
        for raw_code in _extract_codes(text):
            code_text = str(raw_code or "").strip().upper()
            code_key = _seed_key(code_text)
            if len(code_key) < 4 or code_key in code_seen:
                continue
            code_seen.add(code_key)
            code_queries.append(code_text)

    _push(stage1_query)
    for code in code_queries:
        _push(code)
        if len(out) >= limit:
            return out[:limit]
    if str(seed_query or "").strip():
        _push(seed_query)
    if len(out) >= limit:
        return out[:limit]

    for q in _build_site_queries(stage1_query, site):
        _push(q)
        if len(out) >= limit:
            break
    if len(out) < limit:
        for q in _build_site_queries(seed_query, site):
            _push(q)
            if len(out) >= limit:
                break
    if len(out) < limit:
        for q in _extract_seed_queries_from_title(seed_source_title, []):
            _push(q)
            if len(out) >= limit:
                break
    return out[:limit]


def _stage1_seed_model_code_set(*, seed_query: str, seed_source_title: str) -> Set[str]:
    seed_codes: List[str] = []
    seed_codes.extend(_specific_model_codes_in_title(seed_query))
    seed_codes.extend(_specific_model_codes_in_title(seed_source_title))
    return _canonical_code_set(seed_codes)


def _stage1_item_model_codes(item: MarketItem) -> Tuple[List[str], Set[str]]:
    raw_codes: List[str] = []
    raw_codes.extend(_specific_model_codes_in_title(str(item.title or "")))
    identifiers = item.identifiers if isinstance(item.identifiers, dict) else {}
    for key in ("model", "mpn", "manufacturerPartNumber", "sku", "jan", "ean", "upc"):
        text = str(identifiers.get(key) or "").strip()
        if not text:
            continue
        raw_codes.extend(_specific_model_codes_in_title(text))
    raw = item.raw if isinstance(item.raw, dict) else {}
    if isinstance(raw, dict):
        for key in ("itemCode", "model", "modelNumber", "mpn", "manufacturerPartNumber", "sku"):
            text = str(raw.get(key) or "").strip()
            if not text:
                continue
            raw_codes.extend(_specific_model_codes_in_title(text))
    return raw_codes, _canonical_code_set(raw_codes)


def _source_stock_alert(item: MarketItem) -> Dict[str, Any]:
    raw = item.raw if isinstance(item.raw, dict) else {}
    status = {
        "is_low_stock": False,
        "remaining": -1,
        "source_key": "",
    }
    if not isinstance(raw, dict):
        return status
    for key in ("stock", "stockCount", "inventory", "quantity", "remaining"):
        val = raw.get(key)
        qty = to_int(val, -1)
        if qty < 0:
            continue
        status["remaining"] = int(qty)
        status["source_key"] = str(key)
        if 0 <= qty <= 3:
            status["is_low_stock"] = True
        return status
    return status


def _resolve_stage1_source_pricing(
    *,
    item: MarketItem,
    seed_query: str,
    seed_source_title: str,
    timeout: int,
    strict_multi_sku: bool,
    allow_non_rakuten_fallback: Optional[bool] = None,
    allow_timeout_fallback: Optional[bool] = None,
) -> Dict[str, Any]:
    base_price = max(0.0, to_float(item.price, 0.0))
    base_shipping = max(0.0, to_float(item.shipping, 0.0))
    item_codes_raw, item_codes = _stage1_item_model_codes(item)
    seed_codes = _stage1_seed_model_code_set(seed_query=seed_query, seed_source_title=seed_source_title)
    stock_alert = _source_stock_alert(item)
    resolution: Dict[str, Any] = {
        "site": str(item.site or ""),
        "applied": False,
        "ambiguous_source_model_codes": sorted(item_codes),
        "target_model_code": "",
        "reason": "",
        "is_low_stock": bool(stock_alert.get("is_low_stock")),
        "remaining": int(to_int(stock_alert.get("remaining"), -1)),
        "stock_source_key": str(stock_alert.get("source_key", "") or ""),
    }
    if len(item_codes) < 2:
        return {
            "ok": True,
            "price_jpy": float(base_price),
            "shipping_jpy": float(base_shipping),
            "price_basis_type": "listing_price",
            "resolution": resolution,
        }

    target_canon = ""
    if seed_codes:
        shared = sorted(seed_codes & item_codes)
        if shared:
            target_canon = shared[0]
    resolution["target_model_code"] = target_canon

    if str(item.site or "").strip().lower() == "rakuten" and target_canon:
        target_raw = ""
        for raw in item_codes_raw:
            if re.sub(r"[^A-Z0-9]+", "", str(raw or "").upper()) == target_canon:
                target_raw = str(raw)
                break
        target_code = target_raw or target_canon
        try:
            resolved_price, resolved_info = _resolve_rakuten_variant_price_jpy(
                item_url=str(item.item_url or ""),
                target_code=target_code,
                timeout=timeout,
            )
        except TimeoutError:
            resolved_price, resolved_info = -1.0, {"ok": False, "reason": "variant_price_timeout"}
        except Exception as err:
            resolved_price, resolved_info = -1.0, {"ok": False, "reason": f"variant_price_error_{type(err).__name__}"}
        if isinstance(resolved_info, dict):
            resolution.update(resolved_info)
        if resolved_price > 0:
            resolution["applied"] = True
            resolution["reason"] = str(resolution.get("reason", "") or "resolved")
            return {
                "ok": True,
                "price_jpy": float(resolved_price),
                "shipping_jpy": float(base_shipping),
                "price_basis_type": "rakuten_variant_model_price",
                "resolution": resolution,
            }
        resolution["reason"] = str(resolution.get("reason", "") or "price_not_found")
    elif not target_canon:
        resolution["reason"] = "target_model_code_missing"
    else:
        resolution["reason"] = "multi_sku_site_not_supported"

    def _fallback_allowed_for_strict_listing_price(reason_text: str) -> bool:
        reason_key = str(reason_text or "").strip().lower()
        if not target_canon:
            return False
        site_key = str(item.site or "").strip().lower()
        allow_non_rakuten = (
            bool(env_bool("MINER_STAGE1_MULTI_SKU_FALLBACK_NON_RAKUTEN", True))
            if allow_non_rakuten_fallback is None
            else bool(allow_non_rakuten_fallback)
        )
        allow_timeout = (
            bool(env_bool("MINER_STAGE1_MULTI_SKU_FALLBACK_ON_TIMEOUT", True))
            if allow_timeout_fallback is None
            else bool(allow_timeout_fallback)
        )
        if site_key in {"yahoo", "yahoo_shopping"} and allow_non_rakuten:
            return reason_key in {"multi_sku_site_not_supported"}
        if site_key == "rakuten" and allow_timeout:
            return reason_key in {"variant_price_timeout", "timeout"}
        return False

    if strict_multi_sku:
        strict_reason = str(resolution.get("reason", "") or "source_variant_unresolved")
        if _fallback_allowed_for_strict_listing_price(strict_reason):
            resolution["reason"] = f"{strict_reason}_fallback_listing_price"
            return {
                "ok": True,
                "price_jpy": float(base_price),
                "shipping_jpy": float(base_shipping),
                "price_basis_type": "listing_price_multi_sku_fallback",
                "resolution": resolution,
            }
        return {
            "ok": False,
            "price_jpy": float(base_price),
            "shipping_jpy": float(base_shipping),
            "price_basis_type": "listing_price",
            "resolution": resolution,
            "skip_reason": str(resolution.get("reason", "") or "source_variant_unresolved"),
        }

    return {
        "ok": True,
        "price_jpy": float(base_price),
        "shipping_jpy": float(base_shipping),
        "price_basis_type": "listing_price",
        "resolution": resolution,
    }


def _liquidity_sold_sample(signal: Dict[str, Any]) -> Dict[str, Any]:
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    sample = metadata.get("sold_sample") if isinstance(metadata.get("sold_sample"), dict) else {}
    if not isinstance(sample, dict):
        sample = {}
    item_url = str(sample.get("item_url", "") or "").strip()
    title = str(sample.get("title", "") or "").strip()
    image_url = str(sample.get("image_url", "") or "").strip()
    sold_price = to_float(sample.get("sold_price"), to_float(sample.get("sold_price_usd"), -1.0))
    out: Dict[str, Any] = {}
    if item_url:
        out["item_url"] = item_url
    if title:
        out["title"] = title
    if image_url:
        out["image_url"] = image_url
    if sold_price > 0:
        out["sold_price_usd"] = float(sold_price)

    # RPAの行抽出が崩れて sold_sample が空でも、strict sold URL と最低価格がある場合は
    # 参照用sampleを補完する（C段階の根拠URL必須を満たすための最小フォールバック）。
    if not out:
        fallback_url = str(metadata.get("url", "") or "").strip()
        fallback_price = to_float(
            metadata.get("sold_price_min"),
            to_float(signal.get("sold_price_min"), -1.0),
        )
        filter_state = metadata.get("filter_state") if isinstance(metadata.get("filter_state"), dict) else {}
        lookback = str(filter_state.get("lookback_selected", "") or "").strip().lower()
        tab_is_sold = False
        if fallback_url:
            try:
                parsed = urllib.parse.urlparse(fallback_url)
                params = urllib.parse.parse_qs(parsed.query or "")
                tab_values = [str(v or "").strip().lower() for v in params.get("tabName", [])]
                tab_is_sold = any(v == "sold" for v in tab_values)
            except Exception:
                tab_is_sold = False
        if fallback_url and fallback_price > 0 and lookback == "last 90 days" and tab_is_sold:
            out = {
                "item_url": fallback_url,
                "title": str(signal.get("query", "") or ""),
                "sold_price_usd": float(fallback_price),
                "reference_type": "search_url_fallback",
            }
    return out


def _liquidity_sold_min_usd(signal: Dict[str, Any]) -> float:
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    candidates = [
        metadata.get("sold_price_min"),
        metadata.get("sold_price_min_usd"),
        signal.get("sold_price_min"),
        signal.get("sold_price_min_usd"),
        signal.get("sold_price_median"),
    ]
    sold_sample = _liquidity_sold_sample(signal)
    if sold_sample:
        candidates.append(sold_sample.get("sold_price_usd"))
    for raw in candidates:
        sold_min = to_float(raw, -1.0)
        if sold_min > 0:
            return sold_min
    return -1.0


def _liquidity_active_sample(signal: Dict[str, Any]) -> Dict[str, Any]:
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    sample = metadata.get("active_sample") if isinstance(metadata.get("active_sample"), dict) else {}
    if not isinstance(sample, dict):
        sample = {}
    item_url = str(sample.get("item_url", "") or "").strip()
    title = str(sample.get("title", "") or "").strip()
    image_url = str(sample.get("image_url", "") or "").strip()
    active_price = to_float(sample.get("active_price"), to_float(sample.get("sold_price"), -1.0))
    out: Dict[str, Any] = {}
    if item_url:
        out["item_url"] = item_url
    if title:
        out["title"] = title
    if image_url:
        out["image_url"] = image_url
    if active_price > 0:
        out["active_price_usd"] = float(active_price)
    return out


def _liquidity_active_min_usd(signal: Dict[str, Any]) -> float:
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    active_sample = _liquidity_active_sample(signal)
    candidates = [
        metadata.get("active_price_min"),
        metadata.get("active_price_median"),
        signal.get("active_price_min"),
        active_sample.get("active_price_usd"),
    ]
    for raw in candidates:
        active_min = to_float(raw, -1.0)
        if active_min > 0:
            return active_min
    return -1.0


def _iter_json_ld_dicts(html_text: str) -> List[Dict[str, Any]]:
    text = str(html_text or "")
    out: List[Dict[str, Any]] = []
    if not text:
        return out
    scripts = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for script in scripts:
        raw = html.unescape(str(script or "").strip())
        if not raw:
            continue
        parsed: Any = None
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        stack: List[Any] = [parsed]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                out.append(node)
                for value in node.values():
                    if isinstance(value, (dict, list)):
                        stack.append(value)
            elif isinstance(node, list):
                stack.extend(node[:80])
    return out


def _parse_ebay_item_detail_html(html_text: str, *, item_url: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    nodes = _iter_json_ld_dicts(html_text)
    product_node: Dict[str, Any] = {}
    offer_node: Dict[str, Any] = {}

    for node in nodes:
        node_type = str(node.get("@type", "") or "").strip().lower()
        if (not product_node) and ("product" in node_type):
            product_node = node
        if (not offer_node) and ("offer" in node_type):
            offer_node = node
        offers = node.get("offers")
        if (not offer_node) and isinstance(offers, dict):
            offer_node = offers
        if (not offer_node) and isinstance(offers, list):
            for offer in offers:
                if isinstance(offer, dict):
                    offer_node = offer
                    break
        if product_node and offer_node:
            break

    title = ""
    for probe in (product_node.get("name"), offer_node.get("name")):
        text = re.sub(r"\s+", " ", str(probe or "").strip())
        if text:
            title = text
            break
    if not title:
        og = re.search(
            r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
            str(html_text or ""),
            flags=re.IGNORECASE,
        )
        if og:
            title = re.sub(r"\s+", " ", html.unescape(str(og.group(1) or "").strip()))
    if title:
        out["title"] = title[:220]

    brand_val = ""
    brand_raw = product_node.get("brand")
    if isinstance(brand_raw, dict):
        brand_val = str(brand_raw.get("name", "") or "").strip()
    elif isinstance(brand_raw, str):
        brand_val = str(brand_raw or "").strip()
    if brand_val:
        out["brand"] = brand_val[:120]

    price_usd = to_float(
        offer_node.get("price"),
        to_float(offer_node.get("lowPrice"), -1.0),
    )
    if price_usd > 0:
        out["price_usd"] = float(price_usd)

    currency = str(
        offer_node.get("priceCurrency")
        or product_node.get("priceCurrency")
        or ""
    ).strip()
    if currency:
        out["currency"] = currency[:16]

    shipping_usd = -1.0
    shipping_details = offer_node.get("shippingDetails") if isinstance(offer_node.get("shippingDetails"), dict) else {}
    shipping_rate = (
        shipping_details.get("shippingRate")
        if isinstance(shipping_details.get("shippingRate"), dict)
        else {}
    )
    if isinstance(shipping_rate, dict):
        shipping_usd = to_float(shipping_rate.get("value"), -1.0)
    if shipping_usd <= 0:
        m_ship = re.search(
            r"(?:US\s*\$|\$)\s*([0-9][0-9,]{0,8}(?:\.[0-9]{1,2})?)\s*(?:shipping|postage)",
            str(html_text or ""),
            flags=re.IGNORECASE,
        )
        if m_ship:
            shipping_usd = to_float(str(m_ship.group(1) or "").replace(",", ""), -1.0)
    if shipping_usd <= 0 and re.search(r"free\s+shipping", str(html_text or ""), flags=re.IGNORECASE):
        shipping_usd = 0.0
    if shipping_usd >= 0:
        out["shipping_usd"] = float(shipping_usd)

    image_url = ""
    image_raw = product_node.get("image")
    if isinstance(image_raw, str):
        image_url = str(image_raw or "").strip()
    elif isinstance(image_raw, list):
        for candidate in image_raw:
            text = str(candidate or "").strip()
            if text:
                image_url = text
                break
    if not image_url:
        og_img = re.search(
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            str(html_text or ""),
            flags=re.IGNORECASE,
        )
        if og_img:
            image_url = str(og_img.group(1) or "").strip()
    if image_url:
        out["image_url"] = image_url

    item_url_text = str(item_url or "").strip()
    if item_url_text:
        out["item_url"] = item_url_text
        item_id = _ebay_item_id_from_url(item_url_text)
        if item_id:
            out["item_id"] = item_id

    model_code = ""
    for code in _extract_codes(
        " ".join(
            [
                str(out.get("title", "") or ""),
                str(product_node.get("sku", "") or ""),
                str(product_node.get("mpn", "") or ""),
            ]
        )
    ):
        model_code = str(code or "").strip()
        if model_code:
            break
    if model_code:
        out["model"] = model_code

    return out


def _fetch_ebay_item_detail_from_url(item_url: str, *, timeout: int) -> Dict[str, Any]:
    url = str(item_url or "").strip()
    if not url:
        return {}
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    html_text = ""
    try:
        with urllib.request.urlopen(req, timeout=max(3, int(timeout))) as res:
            body = res.read(1_500_000)
        html_text = body.decode("utf-8", errors="ignore")
    except Exception:
        return {}
    parsed = _parse_ebay_item_detail_html(html_text, item_url=url)
    return parsed if isinstance(parsed, dict) else {}


def _is_rpa_daily_limit_signal(signal: Dict[str, Any]) -> bool:
    parts: List[str] = []
    parts.append(str(signal.get("unavailable_reason", "") or ""))
    parts.append(str(signal.get("source", "") or ""))
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    parts.append(str(metadata.get("invalidated_reason", "") or ""))
    parts.append(str(metadata.get("provider_reason", "") or ""))
    text = " ".join(parts).lower()
    if not text:
        return False
    return any(
        token in text
        for token in (
            "daily_limit",
            "one day",
            "try again tomorrow",
            "requests allowed in one day",
        )
    )


def _liquidity_refresh_queries_for_seed(
    seed_query: str,
    *,
    max_count: int,
    source_title: str = "",
    brand_hints: Optional[Sequence[str]] = None,
) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()

    def _push(raw: str) -> None:
        text = re.sub(r"\s+", " ", str(raw or "").strip())
        if not text:
            return
        key = text.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(text)

    normalized_brand_hints: List[str] = []
    if isinstance(brand_hints, Sequence):
        for raw_brand in brand_hints:
            text = re.sub(r"\s+", " ", str(raw_brand or "").strip())
            if text:
                normalized_brand_hints.append(text)

    title_hint = re.sub(r"\s+", " ", str(source_title or "").strip())
    brand = _pick_brand(f"{seed_query} {title_hint}".strip(), normalized_brand_hints)

    code_candidates: List[str] = []
    code_seen: Set[str] = set()
    for text in (seed_query, source_title):
        for raw_code in _extract_codes(text):
            code_text = str(raw_code or "").strip().upper()
            code_key = _seed_key(code_text)
            if len(code_key) < 4 or code_key in code_seen:
                continue
            code_seen.add(code_key)
            code_candidates.append(code_text)

    for code in code_candidates:
        _push(code)
        if brand:
            _push(f"{brand} {code}")

    if title_hint:
        for candidate in _extract_seed_queries_from_title(title_hint, normalized_brand_hints):
            _push(candidate)
    _push(seed_query)
    if len(out) > max_count:
        return out[:max_count]
    return out


def _refresh_liquidity_rpa(
    queries: Sequence[str],
    *,
    max_queries: int,
    force: bool = False,
    category_id: int = 0,
    category_slug: str = "",
) -> Dict[str, Any]:
    normalized: List[str] = []
    seen: Set[str] = set()
    for raw in queries:
        text = re.sub(r"\s+", " ", str(raw or "").strip())
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text)
        if len(normalized) >= max(1, int(max_queries)):
            break
    if not normalized:
        return {"enabled": False, "ran": False, "reason": "empty_queries", "queries": []}
    # seedフローでは型番クエリを多めに渡しつつ、C段階仕様に合わせて
    # new + fixed price + strict condition を強制して流動性判定の一貫性を保つ。
    safe_category_id = max(0, int(category_id))
    safe_category_slug = re.sub(r"[^a-z0-9_-]+", "", str(category_slug or "").strip().lower())
    seed_rpa_json_path = _resolve_rpa_output_path(
        ROOT_DIR,
        env_key="MINER_SEED_POOL_RPA_JSON_PATH",
        default_path="data/liquidity_rpa_signals.jsonl",
    )
    with _temporary_env(
        {
            "LIQUIDITY_PROVIDER_MODE": "rpa_json",
            "LIQUIDITY_RPA_FETCH_MAX_QUERIES": str(max(1, int(max_queries))),
            "LIQUIDITY_RPA_PRIMARY_CONDITION": "new",
            "LIQUIDITY_RPA_PRIMARY_STRICT_CONDITION": "1",
            "LIQUIDITY_RPA_PRIMARY_FIXED_PRICE_ONLY": "1",
            "LIQUIDITY_RPA_COLLECT_ACTIVE_TAB": "1",
            "LIQUIDITY_RPA_CATEGORY_ID": str(safe_category_id),
            "LIQUIDITY_RPA_CATEGORY_SLUG": safe_category_slug,
            "LIQUIDITY_RPA_JSON_PATH": str(seed_rpa_json_path),
        }
    ):
        summary = _maybe_refresh_rpa_for_fetch(normalized, force=bool(force))
    if not isinstance(summary, dict):
        return {"enabled": True, "ran": False, "reason": "invalid_refresh_summary", "queries": list(normalized)}
    return summary


def _match_level_from_score(score: float) -> str:
    if score >= 0.90:
        return "L1_identifier"
    if score >= 0.78:
        return "L2_precise"
    if score >= 0.58:
        return "L3_mid"
    return "L4_broad"


def run_seeded_fetch(
    *,
    category_query: str,
    source_sites: Sequence[str],
    market_site: str,
    limit_per_site: int,
    max_candidates: int,
    min_match_score: float,
    min_profit_usd: float,
    min_margin_rate: float,
    require_in_stock: bool,
    timeout: int,
    timed_mode: bool,
    min_target_candidates: int,
    timebox_sec: int,
    max_passes: int,
    continue_after_target: bool,
    stage_a_big_word_limit: int = 0,
    stage_a_minimize_transitions: bool = False,
    stage_b_query_mode: Optional[str] = None,
    stage_b_max_queries_per_site: Optional[int] = None,
    stage_b_top_matches_per_seed: Optional[int] = None,
    stage_b_api_max_calls_per_run: Optional[int] = None,
    stage_c_min_sold_90d: Optional[int] = None,
    stage_c_liquidity_refresh_on_miss_enabled: Optional[bool] = None,
    stage_c_liquidity_refresh_on_miss_budget: Optional[int] = None,
    stage_c_allow_missing_sold_sample: Optional[bool] = None,
    stage_c_ebay_item_detail_enabled: Optional[bool] = None,
    stage_c_ebay_item_detail_max_fetch_per_run: Optional[int] = None,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    settings: Optional[Settings] = None,
) -> Dict[str, Any]:
    settings = settings or load_settings()
    category_key, category_label, category_row = _resolve_category(category_query)
    if not category_key:
        raise ValueError("category query is required")
    pr_category_filter = _resolve_ebay_pr_category_filter(category_key, category_row)
    started = time.monotonic()
    now_ts = int(time.time())
    run_batch_size = max(1, env_int("MINER_SEED_POOL_RUN_BATCH_SIZE", 20))
    aggregate_counts = {key: 0 for key in _COUNT_KEYS}
    stage1_skip_counts: Dict[str, int] = {}
    stage2_skip_counts: Dict[str, int] = {}
    stage1_low_match_reasons: Dict[str, int] = {}
    stage2_low_match_reasons: Dict[str, int] = {}
    stage1_low_match_samples: List[Dict[str, Any]] = []
    stage1_rejected_examples: List[Dict[str, Any]] = []
    stage1_no_hit_examples: List[Dict[str, Any]] = []
    stage2_low_liquidity_examples: List[Dict[str, Any]] = []
    stage2_liquidity_unavailable_examples: List[Dict[str, Any]] = []
    stage2_rejected_examples: List[Dict[str, Any]] = []
    fetched_aggregate: Dict[str, Dict[str, Any]] = {}
    created_ids: List[int] = []
    created_seen: Set[int] = set()
    created_items: List[Dict[str, Any]] = []
    errors: List[Any] = []
    hints: List[str] = []
    passes: List[Dict[str, Any]] = []
    applied_filters: Dict[str, Any] = {}
    stop_reason = "seed_batch_completed"
    search_scope_done = True
    rpa_daily_limit_reached = False
    stage1_low_match_sample_limit = max(0, env_int("MINER_STAGE1_LOW_MATCH_SAMPLE_LIMIT", 0))
    stage1_rejected_example_limit = max(0, env_int("MINER_STAGE1_REJECTED_EXAMPLE_LIMIT", 0))
    stage2_low_liquidity_example_limit = max(0, env_int("MINER_STAGE2_LOW_LIQUIDITY_EXAMPLE_LIMIT", 0))
    stage2_liquidity_unavailable_example_limit = max(0, env_int("MINER_STAGE2_LIQUIDITY_UNAVAILABLE_EXAMPLE_LIMIT", 0))
    stage2_rejected_example_limit = max(0, env_int("MINER_STAGE2_REJECTED_EXAMPLE_LIMIT", 0))

    def _inc_skip(dst: Dict[str, int], key: str, amount: int = 1) -> None:
        label = str(key or "").strip()
        if not label or amount <= 0:
            return
        dst[label] = int(dst.get(label, 0)) + int(amount)
        if label in aggregate_counts:
            aggregate_counts[label] = int(aggregate_counts.get(label, 0)) + int(amount)

    def _append_stage1_rejected(row: Dict[str, Any]) -> None:
        if stage1_rejected_example_limit > 0 and len(stage1_rejected_examples) >= stage1_rejected_example_limit:
            return
        stage1_rejected_examples.append(row)

    def _append_stage1_no_hit(row: Dict[str, Any]) -> None:
        if stage1_rejected_example_limit > 0 and len(stage1_no_hit_examples) >= stage1_rejected_example_limit:
            return
        stage1_no_hit_examples.append(row)

    def _append_stage2_rejected(row: Dict[str, Any]) -> None:
        if stage2_rejected_example_limit > 0 and len(stage2_rejected_examples) >= stage2_rejected_example_limit:
            return
        stage2_rejected_examples.append(row)

    if callable(progress_callback):
        progress_callback(
            {
                "phase": "timed_fetch_start",
                "message": f"{category_label}のseed poolを確認しています",
                "progress_percent": 5.0,
                "pass_index": 0,
                "max_passes": run_batch_size,
                "created_count": 0,
                "stage1_pass_total": 0,
                "stage2_runs": 0,
                "flow_stage": "A",
                "flow_stage_label": "A: seed補充",
                "flow_stage_index": 1,
                "flow_stage_total": 3,
            }
        )

    refill_timebox_override_sec: Optional[int] = None
    if bool(timed_mode):
        # 既定ではA段階timeboxの短縮は行わない（仕様書の補充timebox設定を優先）。
        # 必要な場合のみ明示ENVで割合短縮する。
        refill_ratio_raw = str((os.getenv("MINER_SEED_POOL_REFILL_TIMEBOX_RATIO", "") or "")).strip()
        if refill_ratio_raw:
            refill_ratio = max(0.2, min(1.0, to_float(refill_ratio_raw, 1.0)))
            refill_timebox_override_sec = max(30, int(max(10, int(timebox_sec)) * refill_ratio))

    with connect(settings.db_path) as conn:
        init_db(conn)
        normalize_stats = _normalize_existing_seed_rows(conn, category_key=category_key)
        normalized_seed_count = max(0, to_int(normalize_stats.get("normalized_count"), 0))
        deduped_seed_count = max(0, to_int(normalize_stats.get("deduped_count"), 0))
        deleted_invalid_seed_count = max(0, to_int(normalize_stats.get("deleted_invalid_count"), 0))
        if normalized_seed_count > 0:
            hints.append(f"既存seedの表記を正規化: {normalized_seed_count}件")
        if deduped_seed_count > 0:
            hints.append(f"既存seedの重複を整理: {deduped_seed_count}件")
        if deleted_invalid_seed_count > 0:
            hints.append(f"無効seedを削除: {deleted_invalid_seed_count}件")
        cleaned = _cleanup_expired(conn, category_key=category_key, now_ts=now_ts)
        if cleaned > 0:
            hints.append(f"期限切れseedを {cleaned} 件削除しました。")
        refill_summary = _refill_seed_pool_with_page_unlock_fallback(
            conn,
            category_key=category_key,
            category_label=category_label,
            category_row=category_row,
            stage_a_big_word_limit=max(0, int(stage_a_big_word_limit)),
            stage_a_minimize_transitions=bool(stage_a_minimize_transitions),
            refill_timebox_override_sec=refill_timebox_override_sec,
            progress_callback=progress_callback,
        )
        available_after = _count_available(conn, category_key=category_key, now_ts=int(time.time()))
        # 1探索は「古い順20件」を上限に実行する。
        run_batch_size = max(1, min(int(available_after), env_int("MINER_SEED_POOL_RUN_BATCH_SIZE", 20)))
        selected_seeds, skipped_cooldown_count = _take_seeds_for_run(
            conn,
            category_key=category_key,
            take_count=run_batch_size,
            now_ts=int(time.time()),
        )
        low_liquidity_cooldown_active_count = len(
            _load_active_low_liquidity_seed_keys(conn, category_key=category_key, now_ts=int(time.time()))
        )
        used_cooldown_active_count = len(
            _load_active_seed_usage_cooldown_keys(conn, category_key=category_key, now_ts=int(time.time()))
        )
        cooldown_active_count = low_liquidity_cooldown_active_count + used_cooldown_active_count
        seed_pool_summary = {
            "category_key": category_key,
            "category_label": category_label,
            "seed_count": len(selected_seeds),
            # backward-compat keys (deprecated)
            "selected_seed_count": len(selected_seeds),
            "available_after_refill": available_after,
            "skipped_low_quality_count": 0,
            "skipped_cooldown_count": int(skipped_cooldown_count),
            "cooldown_active_count": int(cooldown_active_count),
            "low_liquidity_cooldown_active_count": int(low_liquidity_cooldown_active_count),
            "used_cooldown_active_count": int(used_cooldown_active_count),
            "strict_model_seed": bool(refill_summary.get("strict_model_seed", False)),
            "cleaned_expired_count": int(cleaned),
            "normalized_seed_count": int(normalized_seed_count),
            "deduped_seed_count": int(deduped_seed_count),
            "deleted_invalid_seed_count": int(deleted_invalid_seed_count),
            "select_min_seed_score": 0,
            "refill": refill_summary,
        }

    refill_reason = str(seed_pool_summary["refill"].get("reason", "") or "")
    bootstrap_added = max(0, to_int(seed_pool_summary["refill"].get("bootstrap_added_count"), 0))
    if bootstrap_added > 0:
        hints.append(f"カテゴリ知識からseedを {bootstrap_added} 件補充しました。")
    if refill_reason == "category_cooldown":
        cooldown_until = str(seed_pool_summary["refill"].get("cooldown_until", "") or "").strip()
        hints.append(f"{category_label}は深掘り上限に到達したため、{cooldown_until} まで補充を停止しています。")
    if refill_reason == "rank_limit_cooldown":
        cooldown_until = str(seed_pool_summary["refill"].get("cooldown_until", "") or "").strip()
        rank_ceiling = max(0, to_int(seed_pool_summary["refill"].get("rank_ceiling"), 0))
        hints.append(f"{category_label}の上位{rank_ceiling}件まで確認しました。{cooldown_until} 以降に再実行してください。")
    if refill_reason == "fresh_window_skip":
        hints.append(f"{category_label}の補充対象ページは7日以内に取得済みのため、今回は再取得を行いませんでした。")
    if refill_reason == "empty_result_cooldown":
        cooldown_until = str(seed_pool_summary["refill"].get("cooldown_until", "") or "").strip()
        hints.append(f"{category_label}は検索結果が空だったため、{cooldown_until} までProduct Research補充を停止しています。")
    if refill_reason == "all_big_words_exhausted":
        cooldown_until = str(seed_pool_summary["refill"].get("cooldown_until", "") or "").strip()
        hints.append(f"{category_label}の全big wordを確認しました。{cooldown_until} 以降に再実行してください。")
    if refill_reason == "refill_timebox_reached":
        elapsed_sec = max(0.0, to_float(seed_pool_summary["refill"].get("refill_elapsed_sec"), 0.0))
        hints.append(f"seed補充の実行時間上限に達したため、{elapsed_sec:.1f}秒で中断しました。")
    if refill_reason == "rpa_timeout_guard":
        timeout_pages = max(0, to_int(seed_pool_summary["refill"].get("rpa_timeout_pages"), 0))
        hints.append(f"Product Research応答タイムアウトが連続したため、補充を停止しました（{timeout_pages}ページ）。")
    if refill_reason == "partial_refill":
        hints.append(f"{category_label}の補充は目標100件に未達ですが、追加seedを確保して探索を継続します。")
    low_yield_stop_query_count = max(0, to_int(seed_pool_summary["refill"].get("low_yield_stop_query_count"), 0))
    if low_yield_stop_query_count > 0:
        hints.append(f"seed補充で低収穫ページが連続したため、{low_yield_stop_query_count}個のbig wordを早期切替しました。")
    if bool(seed_pool_summary["refill"].get("daily_limit_reached")):
        rpa_daily_limit_reached = True
        hints.append("Product Researchの日次上限に到達したため、seed補充を停止しました。")
    skipped_cooldown_count = max(0, to_int(seed_pool_summary.get("skipped_cooldown_count"), 0))
    if skipped_cooldown_count > 0:
        hints.append(f"cooldown中のseedを {skipped_cooldown_count} 件スキップしました。")
    cooldown_active_count = max(0, to_int(seed_pool_summary.get("cooldown_active_count"), 0))
    if cooldown_active_count > 0:
        hints.append(f"cooldownで待機中のseed: {cooldown_active_count}件")
    used_cooldown_active_count = max(0, to_int(seed_pool_summary.get("used_cooldown_active_count"), 0))
    if used_cooldown_active_count > 0:
        hints.append(f"うち消化済みcooldown: {used_cooldown_active_count}件")
    refill_cooldown_blocked_count = max(
        0,
        to_int(seed_pool_summary.get("refill", {}).get("cooldown_blocked_count"), 0)
        if isinstance(seed_pool_summary.get("refill"), dict)
        else 0,
    )
    if refill_cooldown_blocked_count > 0:
        hints.append(f"A段階でcooldown中seedを再補充せずに除外: {refill_cooldown_blocked_count}件")
    refill_used_cooldown_blocked_count = max(
        0,
        to_int(seed_pool_summary.get("refill", {}).get("used_cooldown_blocked_count"), 0)
        if isinstance(seed_pool_summary.get("refill"), dict)
        else 0,
    )
    if refill_used_cooldown_blocked_count > 0:
        hints.append(f"A段階で消化済みcooldown中seedを再補充せずに除外: {refill_used_cooldown_blocked_count}件")
    accessory_filtered_count = max(0, to_int(seed_pool_summary["refill"].get("accessory_filtered_count"), 0))
    if accessory_filtered_count > 0:
        hints.append(f"付属品タイトルを {accessory_filtered_count} 件除外しました。")
    min_seed_price_usd = max(0.0, to_float(seed_pool_summary["refill"].get("min_seed_sold_price_usd"), 0.0))
    min_price_filtered_count = max(0, to_int(seed_pool_summary["refill"].get("min_price_filtered_count"), 0))
    if min_seed_price_usd > 0:
        if min_price_filtered_count > 0:
            hints.append(
                f"A段階でカテゴリ最低価格(${min_seed_price_usd:.2f})未満を {min_price_filtered_count} 件除外しました。"
            )
        else:
            hints.append(f"A段階のカテゴリ最低価格フィルタ: ${min_seed_price_usd:.2f} を適用しました。")
    api_backfill = seed_pool_summary["refill"].get("seed_api_backfill", {})
    if isinstance(api_backfill, dict):
        api_hits = max(0, to_int(api_backfill.get("hits"), 0))
        api_attempts = max(0, to_int(api_backfill.get("attempts"), 0))
        api_budget_skips = max(0, to_int(api_backfill.get("budget_skips"), 0))
        if api_attempts > 0:
            hints.append(f"seed API補完: 試行 {api_attempts} 件 / 補完成功 {api_hits} 件")
        if api_budget_skips > 0:
            hints.append(f"seed API補完は予算上限で {api_budget_skips} 件スキップしました。")
    tuning_recommendations = (
        seed_pool_summary["refill"].get("tuning_recommendations", [])
        if isinstance(seed_pool_summary.get("refill"), dict)
        else []
    )
    if isinstance(tuning_recommendations, list):
        for row in tuning_recommendations[:3]:
            if not isinstance(row, dict):
                continue
            msg = str(row.get("message", "") or "").strip()
            if msg:
                hints.append(f"A段階チューニング提案: {msg}")
    if refill_reason == "empty_result_page":
        hints.append("seed補充は0件でしたが、既存seedを使って探索を継続します。")

    refill_trigger_available_le = max(0, env_int("MINER_SEED_POOL_REFILL_THRESHOLD", 0))
    if callable(progress_callback):
        progress_callback(
            {
                "phase": "seed_pool_ready",
                "message": f"seed準備完了: 実行 {len(selected_seeds)} 件 / 残り {available_after} 件",
                "progress_percent": 10.0,
                "pass_index": 0,
                "max_passes": len(selected_seeds),
                "created_count": 0,
                "seed_count": len(selected_seeds),
                "selected_seed_count": len(selected_seeds),
                "pool_available": available_after,
                "refill_reason": refill_reason,
                "skipped_low_quality_count": 0,
                "skipped_cooldown_count": skipped_cooldown_count,
                "cooldown_active_count": cooldown_active_count,
                "select_min_seed_score": int(seed_pool_summary.get("select_min_seed_score", 0)),
                "stage1_pass_total": 0,
                "stage2_runs": 0,
                "flow_stage": "A",
                "flow_stage_label": "A: seed補充",
                "flow_stage_index": 1,
                "flow_stage_total": 3,
                "pool_threshold": refill_trigger_available_le,
                "pool_gate_passed": bool(available_after > refill_trigger_available_le),
            }
        )

    if not selected_seeds:
        stop_reason = "seed_pool_empty"
        search_scope_done = False
        empty_payload = {
            "query": category_key,
            "market_site": market_site,
            "source_sites": list(source_sites),
            "fetched": {},
            "created_count": 0,
            "created_ids": [],
            "created": [],
            "errors": errors,
            "hints": hints + ["有効なseedがありません。カテゴリを変更するか、後で再実行してください。"],
            "search_scope_done": search_scope_done,
            "applied_filters": {},
            "query_cache_skip": False,
            "query_cache_ttl_sec": 0,
            "rpa_daily_limit_reached": rpa_daily_limit_reached,
            "seed_pool": seed_pool_summary,
            "stage_b": {
                "rows_count": 0,
                "rows": [],
                "api_calls": 0,
                "api_max_calls_per_run": 0,
            },
            "timed_fetch": {
                "enabled": bool(timed_mode),
                "min_target_candidates": max(1, int(min_target_candidates)),
                "timebox_sec": max(10, int(timebox_sec)),
                "max_passes": max(1, len(selected_seeds)),
                "continue_after_target": bool(continue_after_target),
                "passes_run": 0,
                "stop_reason": stop_reason,
                "elapsed_sec": round(time.monotonic() - started, 3),
                "reached_min_target": False,
                "passes": [],
            },
        }
        _append_seed_run_journal(
            {
                "run_at": utc_iso(),
                "category_key": category_key,
                "category_label": category_label,
                "created_count": 0,
                "selected_seed_count": 0,
                "available_after_refill": max(0, to_int(seed_pool_summary.get("available_after_refill"), 0)),
                "refill_reason": refill_reason,
                "bootstrap_added_count": bootstrap_added,
                "stage_b_rows_count": 0,
                "stop_reason": stop_reason,
                "rpa_daily_limit_reached": bool(rpa_daily_limit_reached),
            }
        )
        return empty_payload

    source_fetchers: List[Tuple[str, Callable[[str, int, int, int, bool], Tuple[List[MarketItem], Dict[str, Any]]]]] = []
    for raw in source_sites:
        site_key = str(raw or "").strip().lower()
        if site_key == "rakuten":
            source_fetchers.append(("rakuten", _search_rakuten))
        elif site_key in {"yahoo", "yahoo_shopping"}:
            source_fetchers.append(("yahoo", _search_yahoo))
    if not source_fetchers:
        source_fetchers = [("rakuten", _search_rakuten), ("yahoo", _search_yahoo)]

    stage1_query_mode_raw = str(stage_b_query_mode if stage_b_query_mode is not None else os.getenv("MINER_STAGE1_QUERY_MODE", "") or "").strip().lower()
    stage1_query_mode = stage1_query_mode_raw if stage1_query_mode_raw in {"seed_only", "auto"} else ""
    if not stage1_query_mode:
        stage1_query_mode = "seed_only" if _category_requires_strict_model_seed(category_key, category_row) else "auto"
    stage1_seed_only_effective_queries_per_site = 2 if (
        stage1_query_mode == "seed_only" and _category_requires_strict_model_seed(category_key, category_row)
    ) else 1

    stage1_limit_per_site = max(5, min(100, env_int("MINER_STAGE1_LIMIT_PER_SITE", max(20, int(limit_per_site)))))
    stage1_max_queries_default = 1 if stage1_query_mode == "seed_only" else 2
    if stage_b_max_queries_per_site is None:
        stage1_max_queries_per_site = max(
            1, min(4, env_int("MINER_STAGE1_MAX_QUERIES_PER_SITE", stage1_max_queries_default))
        )
    else:
        stage1_max_queries_per_site = max(1, min(4, int(stage_b_max_queries_per_site)))
    stage1_take_per_seed = max(
        1,
        min(
            max(1, int(max_candidates)),
            5,
            int(stage_b_top_matches_per_seed)
            if stage_b_top_matches_per_seed is not None
            else env_int("MINER_STAGE1_TOP_MATCHES_PER_SEED", 3),
        ),
    )
    stage1_multi_sku_strict = env_bool("MINER_STAGE1_MULTI_SKU_STRICT", True)
    stage1_multi_sku_fallback_non_rakuten = env_bool("MINER_STAGE1_MULTI_SKU_FALLBACK_NON_RAKUTEN", True)
    stage1_multi_sku_fallback_on_timeout = env_bool("MINER_STAGE1_MULTI_SKU_FALLBACK_ON_TIMEOUT", True)
    stage1_include_diagnostics = env_bool("MINER_STAGE1_INCLUDE_DIAGNOSTICS", False)
    if stage_b_api_max_calls_per_run is None:
        stage1_api_max_calls = max(
            0,
            env_int(
                "MINER_STAGE1_API_MAX_CALLS_PER_RUN",
                max(
                    4,
                    len(selected_seeds)
                    * max(1, len(source_fetchers))
                    * (
                        stage1_seed_only_effective_queries_per_site
                        if stage1_query_mode == "seed_only"
                        else stage1_max_queries_per_site
                    ),
                ),
            ),
        )
    else:
        stage1_api_max_calls = max(0, int(stage_b_api_max_calls_per_run))
    stage1_api_calls = 0
    stage_b_rows: List[Dict[str, Any]] = []
    stage_b_seen_keys: Set[str] = set()
    liquidity_min_sold_90d = max(
        0,
        int(stage_c_min_sold_90d)
        if stage_c_min_sold_90d is not None
        else _category_stage_c_min_sold_90d(category_key, category_row),
    )
    fx_seed_rate = max(1.0, to_float(getattr(settings, "fx_usd_jpy_default", 150.0), 150.0))
    brand_hints = _brand_hints(category_row)
    marketplace_fee_rate = max(0.0, env_float("MARKETPLACE_FEE_RATE", 0.13))
    payment_fee_rate = max(0.0, env_float("PAYMENT_FEE_RATE", 0.03))
    international_shipping_usd = max(0.0, env_float("EST_INTL_SHIPPING_USD", 18.0))
    customs_usd = max(0.0, env_float("EST_CUSTOMS_USD", 0.0))
    packaging_usd = max(0.0, env_float("EST_PACKAGING_USD", 0.0))
    fixed_fee_usd = max(0.0, env_float("FIXED_FEE_USD", 0.0))
    runtime_pair_signatures: Set[str] = set()
    liquidity_mode_raw = (os.getenv("LIQUIDITY_PROVIDER_MODE", "none") or "none").strip().lower()
    if liquidity_mode_raw in {"rpa", "rpa_json"}:
        liquidity_mode = liquidity_mode_raw
    elif stage_c_liquidity_refresh_on_miss_enabled is True:
        # UIでC段階再取得が有効化されている場合は、未設定環境でもrpa_jsonで実行する。
        liquidity_mode = "rpa_json"
    else:
        liquidity_mode = "none"
    stage2_liquidity_refresh_enabled_default = bool(
        liquidity_mode in {"rpa", "rpa_json"}
        and env_bool("LIQUIDITY_RPA_AUTO_REFRESH", True)
        and env_bool("LIQUIDITY_RPA_RUN_ON_FETCH", True)
        and env_bool("MINER_STAGE2_LIQUIDITY_REFRESH_ON_MISS_ENABLED", True)
    )
    if stage_c_liquidity_refresh_on_miss_enabled is None:
        stage2_liquidity_refresh_enabled = stage2_liquidity_refresh_enabled_default
    else:
        stage2_liquidity_refresh_enabled = bool(
            stage_c_liquidity_refresh_on_miss_enabled
            and liquidity_mode in {"rpa", "rpa_json"}
            and env_bool("LIQUIDITY_RPA_AUTO_REFRESH", True)
            and env_bool("LIQUIDITY_RPA_RUN_ON_FETCH", True)
        )
    stage2_liquidity_prefetch_max_queries = max(0, env_int("MINER_STAGE2_LIQUIDITY_PREFETCH_MAX_QUERIES", 0))
    stage2_liquidity_retry_budget_total = max(
        0,
        int(stage_c_liquidity_refresh_on_miss_budget)
        if stage_c_liquidity_refresh_on_miss_budget is not None
        else env_int("MINER_STAGE2_LIQUIDITY_REFRESH_ON_MISS_BUDGET", 12),
    )
    stage2_liquidity_retry_budget_used = 0
    stage2_liquidity_retry_queries_seen: Set[str] = set()
    stage2_liquidity_refresh_runs: List[Dict[str, Any]] = []
    stage2_low_liquidity_cooldown_rows: List[Dict[str, Any]] = []
    stage1_seed_feedback_rows: List[Dict[str, Any]] = []
    stage2_liquidity_query_fallback_max = max(0, env_int("MINER_STAGE2_LIQUIDITY_QUERY_FALLBACK_MAX", 3))
    stage2_retry_missing_active_enabled = bool(
        stage2_liquidity_refresh_enabled and env_bool("MINER_STAGE2_RETRY_MISSING_ACTIVE_ENABLED", False)
    )
    stage2_allow_missing_sold_sample = (
        bool(stage_c_allow_missing_sold_sample)
        if stage_c_allow_missing_sold_sample is not None
        else env_bool("MINER_STAGE2_ALLOW_MISSING_SOLD_SAMPLE", False)
    )
    stage2_ebay_item_detail_enabled = (
        bool(stage_c_ebay_item_detail_enabled)
        if stage_c_ebay_item_detail_enabled is not None
        else env_bool("MINER_STAGE2_EBAY_ITEM_DETAIL_ENABLED", True)
    )
    stage2_ebay_item_detail_timeout_sec = max(3, env_int("MINER_STAGE2_EBAY_ITEM_DETAIL_TIMEOUT_SECONDS", 8))
    stage2_ebay_item_detail_max_fetch = max(
        0,
        int(stage_c_ebay_item_detail_max_fetch_per_run)
        if stage_c_ebay_item_detail_max_fetch_per_run is not None
        else env_int("MINER_STAGE2_EBAY_ITEM_DETAIL_MAX_FETCH_PER_RUN", 30),
    )
    stage2_allow_seed_signal_fallback = env_bool("MINER_STAGE2_ALLOW_SEED_SIGNAL_FALLBACK", False)
    stage2_ebay_item_detail_fetched = 0
    stage2_ebay_item_detail_cache: Dict[str, Dict[str, Any]] = {}
    stage_c_rpa_json_path = str(
        _resolve_rpa_output_path(
            ROOT_DIR,
            env_key="MINER_SEED_POOL_RPA_JSON_PATH",
            default_path="data/liquidity_rpa_signals.jsonl",
        )
    )

    applied_filters = {
        "seed_pool_mode": "A/B/C_spec",
        "stage_a_refill_trigger_available_le": max(0, env_int("MINER_SEED_POOL_REFILL_THRESHOLD", 0)),
        "stage_a_rpa_wait_seconds": max(
            2, env_int("MINER_SEED_POOL_RPA_WAIT_SECONDS", env_int("LIQUIDITY_RPA_WAIT_SECONDS", 8))
        ),
        "stage_a_low_yield_consecutive_pages": max(2, env_int("MINER_SEED_POOL_LOW_YIELD_CONSECUTIVE_PAGES", 3)),
        "stage_a_min_pages_before_low_yield_stop": max(
            1, env_int("MINER_SEED_POOL_MIN_PAGES_BEFORE_LOW_YIELD_STOP", 2)
        ),
        "stage_a_min_seed_sold_price_usd": float(
            max(0.0, to_float(seed_pool_summary.get("refill", {}).get("min_seed_sold_price_usd"), 0.0))
        ),
        "stage_a_pr_category_id": int(pr_category_filter.get("category_id", 0) or 0),
        "stage_a_pr_category_slug": str(pr_category_filter.get("category_slug", "") or ""),
        "stage_a_big_word_limit": max(0, to_int(seed_pool_summary.get("refill", {}).get("big_word_limit"), 0)),
        "stage_a_big_word_count": max(0, to_int(seed_pool_summary.get("refill", {}).get("big_word_count"), 0)),
        "stage_a_target_count_base": max(0, to_int(seed_pool_summary.get("refill", {}).get("target_count_base"), 0)),
        "stage_a_target_count": max(0, to_int(seed_pool_summary.get("refill", {}).get("target_count"), 0)),
        "stage_a_timebox_base_sec": max(0, to_int(seed_pool_summary.get("refill", {}).get("timebox_base_sec"), 0)),
        "stage_a_timebox_sec": max(0, to_int(seed_pool_summary.get("refill", {}).get("timebox_sec"), 0)),
        "stage_a_minimize_transitions": bool(seed_pool_summary.get("refill", {}).get("minimize_transitions")),
        "stage_a_transition_page_size": max(
            0, to_int(seed_pool_summary.get("refill", {}).get("transition_page_size"), 0)
        ),
        "stage_a_transition_max_pages_per_query": max(
            0, to_int(seed_pool_summary.get("refill", {}).get("transition_max_pages_per_query"), 0)
        ),
        "stage_b_limit_per_site": int(stage1_limit_per_site),
        "stage_b_pick_per_seed": int(stage1_take_per_seed),
        "stage_b_condition": "new",
        "stage_b_sort": "price_asc",
        "stage_b_price_cap": "seed_collected_sold_price_min_usd",
        "stage_b_baseline_floor_usd": float(min_seed_price_usd),
        "stage_b_baseline_policy": "seed_collected_or_category_min",
        "stage_b_multi_sku_strict": bool(stage1_multi_sku_strict),
        "stage_b_multi_sku_fallback_non_rakuten": bool(stage1_multi_sku_fallback_non_rakuten),
        "stage_b_multi_sku_fallback_on_timeout": bool(stage1_multi_sku_fallback_on_timeout),
        "stage_b_query_mode": str(stage1_query_mode),
        "stage_b_api_max_calls_per_run": int(stage1_api_max_calls),
        "stage_b_max_queries_per_site": int(stage1_max_queries_per_site),
        "stage_c_price_basis": "sold_price_min_90d",
        "stage_c_min_sold_90d": int(liquidity_min_sold_90d),
        "stage_c_pr_category_id": int(pr_category_filter.get("category_id", 0) or 0),
        "stage_c_pr_category_slug": str(pr_category_filter.get("category_slug", "") or ""),
        "stage_c_liquidity_mode": liquidity_mode,
        "stage_c_liquidity_refresh_on_miss_enabled": bool(stage2_liquidity_refresh_enabled),
        "stage_c_liquidity_refresh_on_miss_budget": int(stage2_liquidity_retry_budget_total),
        "stage_c_liquidity_query_fallback_max": int(stage2_liquidity_query_fallback_max),
        "stage_c_retry_missing_active_enabled": bool(stage2_retry_missing_active_enabled),
        "stage_c_liquidity_prefetch_max_queries": int(stage2_liquidity_prefetch_max_queries),
        "stage_c_allow_missing_sold_sample": bool(stage2_allow_missing_sold_sample),
        "stage_c_allow_seed_signal_fallback": bool(stage2_allow_seed_signal_fallback),
        "stage_c_collect_active_tab": bool(stage2_liquidity_refresh_enabled),
        "stage_c_rpa_json_path": stage_c_rpa_json_path,
        "stage_c_ebay_item_detail_enabled": bool(stage2_ebay_item_detail_enabled),
        "stage_c_ebay_item_detail_max_fetch_per_run": int(stage2_ebay_item_detail_max_fetch),
    }

    stage1_pass_total = 0
    stage2_runs = 0
    stage2_seen_item_keys: Set[str] = set()
    stage1_seed_baseline_reject_total = 0
    stage1_baseline_softened_seed_keys: Set[str] = set()
    min_stage1_attempts_before_timebox = min(
        len(selected_seeds),
        max(1, env_int("MINER_TIMED_FETCH_MIN_STAGE1_ATTEMPTS", 20)),
    )
    stage1_baseline_pair_counts = _build_seed_baseline_pair_counts(selected_seeds)
    if stage2_liquidity_refresh_enabled and selected_seeds and stage2_liquidity_prefetch_max_queries > 0:
        prefetch_queries: List[str] = []
        for seed in selected_seeds:
            seed_query_text = str(seed.get("seed_query", "") or "")
            prefetch_queries.extend(
                _liquidity_refresh_queries_for_seed(
                    seed_query_text,
                    max_count=2,
                    source_title=str(seed.get("source_title", "") or ""),
                    brand_hints=brand_hints,
                )
            )
            if len(prefetch_queries) >= stage2_liquidity_prefetch_max_queries:
                break
        prefetch_summary = _refresh_liquidity_rpa(
            prefetch_queries,
            max_queries=stage2_liquidity_prefetch_max_queries,
            force=False,
            category_id=int(pr_category_filter.get("category_id", 0) or 0),
            category_slug=str(pr_category_filter.get("category_slug", "") or ""),
        )
        stage2_liquidity_refresh_runs.append(
            {
                "phase": "prefetch",
                "summary": prefetch_summary,
            }
        )
        if _rpa_daily_limit_reached(prefetch_summary):
            rpa_daily_limit_reached = True
            stop_reason = "rpa_daily_limit_reached"
            hints.append("C段階事前更新でProduct Research上限に到達しました。")
            search_scope_done = False
            selected_seeds = []

    for idx, seed in enumerate(selected_seeds, start=1):
        elapsed = time.monotonic() - started
        seed_query = str(seed.get("seed_query", "") or "")
        seed_key_text = str(seed.get("seed_key", "") or "").strip().upper() or _seed_key(seed_query)
        seed_source_title = str(seed.get("source_title", "") or "")
        stage1_query = _pick_jp_seed_query(
            seed_query=seed_query,
            source_title=seed_source_title,
            brand_hints=brand_hints,
        )
        if not stage1_query:
            stage1_query = seed_query
        baseline_usd, baseline_source = _resolve_stage1_baseline_usd(
            seed_collected_sold_price_min_usd=to_float(seed.get("seed_collected_sold_price_min_usd"), -1.0),
            category_min_seed_price_usd=min_seed_price_usd,
        )
        baseline_suspicious = _seed_baseline_is_suspicious(
            seed,
            pair_counts=stage1_baseline_pair_counts,
        )
        if baseline_suspicious:
            stage1_baseline_softened_seed_keys.add(seed_key_text)
        seed_max_price_jpy = int(max(0.0, baseline_usd * fx_seed_rate)) if baseline_usd > 0 else 0
        if bool(timed_mode) and elapsed >= max(10, int(timebox_sec)) and idx > min_stage1_attempts_before_timebox:
            stop_reason = "timebox_reached"
            search_scope_done = False
            break
        if callable(progress_callback):
            progress_callback(
                {
                    "phase": "stage1_running",
                    "message": f"一次判定 {idx}/{len(selected_seeds)}: {stage1_query}",
                    "progress_percent": min(88.0, 12.0 + (72.0 * (idx - 1) / max(1, len(selected_seeds)))),
                "pass_index": idx,
                "max_passes": len(selected_seeds),
                "created_count": len(created_ids),
                "seed_count": len(selected_seeds),
                "selected_seed_count": len(selected_seeds),
                "pool_available": available_after,
                    "current_seed_query": seed_query,
                    "current_stage1_query": stage1_query,
                    "current_seed_quality_score": to_int(seed.get("seed_quality_score"), 0),
                    "stage1_pass_total": int(stage1_pass_total),
                    "stage2_runs": int(stage2_runs),
                    "elapsed_sec": round(elapsed, 3),
                    "flow_stage": "B",
                    "flow_stage_label": "B: 日本側探索",
                    "flow_stage_index": 2,
                    "flow_stage_total": 3,
                }
            )
        has_model_code = bool(_extract_codes(seed_query))
        effective_min_match_score = max(0.0, min(0.99, float(min_match_score)))
        if (not has_model_code) and effective_min_match_score > 0.62:
            effective_min_match_score = 0.62
        seed_match_context = _build_seed_match_context(
            seed_query=stage1_query,
            seed_source_title=seed_source_title,
        )
        stage_context_env: Dict[str, str] = {
            "MINER_ACTIVE_CATEGORY_KEY": category_key,
            "MINER_ACTIVE_CATEGORY_LABEL": category_label,
            "LIQUIDITY_RPA_JSON_PATH": stage_c_rpa_json_path,
        }
        if liquidity_mode in {"rpa_json", "rpa"}:
            stage_context_env["LIQUIDITY_PROVIDER_MODE"] = liquidity_mode
        if seed_max_price_jpy > 0:
            stage_context_env["MINER_ACTIVE_SEED_MAX_PRICE_JPY"] = str(seed_max_price_jpy)

        def _get_stage2_liquidity_signal(item_obj: MarketItem, query_text: str) -> Dict[str, Any]:
            market_identifiers: Dict[str, str] = {}
            query_codes = _extract_codes(query_text)
            if query_codes:
                primary_code = str(query_codes[0] or "").strip()
                if primary_code:
                    market_identifiers["model"] = primary_code
                    market_identifiers["mpn"] = primary_code
            with _temporary_env(stage_context_env):
                return get_liquidity_signal(
                    query=query_text,
                    source_title=str(item_obj.title or ""),
                    market_title=query_text,
                    source_identifiers=item_obj.identifiers if isinstance(item_obj.identifiers, dict) else {},
                    market_identifiers=market_identifiers,
                    active_count_hint=-1,
                    timeout=max(5, int(timeout)),
                    settings=settings,
                )

        stage1_candidates_by_key: Dict[str, Dict[str, Any]] = {}
        stage1_site_logs: List[Dict[str, Any]] = []
        stage1_baseline_reject = 0
        stage1_raw_item_count = 0
        for site_key, fetcher in source_fetchers:
            if stage1_api_max_calls > 0 and stage1_api_calls >= stage1_api_max_calls:
                _inc_skip(stage1_skip_counts, "skipped_stage1_api_budget", 1)
                if stage1_include_diagnostics:
                    stage1_site_logs.append(
                        {
                            "site": site_key,
                            "queries": [stage1_query],
                            "seed_query": seed_query,
                            "ok": False,
                            "error": "stage1_api_budget_reached",
                            "raw_count": 0,
                            "selected_count": 0,
                        }
                    )
                break
            selected_for_site = 0
            site_query_logs: List[Dict[str, Any]] = []
            if stage1_query_mode == "seed_only":
                if _category_requires_strict_model_seed(category_key, category_row):
                    site_queries = _stage1_seed_only_strict_queries(
                        seed_query=seed_query,
                        stage1_query=stage1_query,
                        seed_source_title=seed_source_title,
                    )
                else:
                    primary_query = _prefer_stage1_query_for_seed_only(
                        seed_query=seed_query,
                        stage1_query=stage1_query,
                    )
                    site_queries = [primary_query] if primary_query else []
            else:
                site_queries = _stage1_site_queries(
                    seed_query=seed_query,
                    stage1_query=stage1_query,
                    seed_source_title=seed_source_title,
                    site=site_key,
                    max_queries=stage1_max_queries_per_site,
                )
            for site_query in site_queries:
                if stage1_api_max_calls > 0 and stage1_api_calls >= stage1_api_max_calls:
                    _inc_skip(stage1_skip_counts, "skipped_stage1_api_budget", 1)
                    if stage1_include_diagnostics:
                        site_query_logs.append(
                            {
                                "query": site_query,
                                "ok": False,
                                "error": "stage1_api_budget_reached",
                                "raw_count": 0,
                                "matched_count": 0,
                            }
                        )
                    break
                with _temporary_env(stage_context_env):
                    try:
                        stage1_api_calls += 1
                        items, fetch_info = fetcher(site_query, stage1_limit_per_site, timeout, 1, require_in_stock)
                    except Exception as err:
                        _inc_skip(stage1_skip_counts, "skipped_fetch_error", 1)
                        errors.append(
                            {
                                "seed_query": seed_query,
                                "stage1_query": stage1_query,
                                "site_query": site_query,
                                "site": site_key,
                                "message": str(err),
                            }
                        )
                        if stage1_include_diagnostics:
                            site_query_logs.append(
                                {
                                    "query": site_query,
                                    "ok": False,
                                    "error": str(err),
                                    "raw_count": 0,
                                    "matched_count": 0,
                                }
                            )
                        continue

                slot = fetched_aggregate.setdefault(
                    site_key, {"calls_made": 0, "network_calls": 0, "cache_hits": 0, "count": 0}
                )
                slot["calls_made"] = int(slot.get("calls_made", 0)) + 1
                slot["network_calls"] = int(slot.get("network_calls", 0)) + 1
                slot["count"] = int(slot.get("count", 0)) + len(items)
                stage1_raw_item_count += len(items)
                if bool((fetch_info or {}).get("cache_hit")):
                    slot["cache_hits"] = int(slot.get("cache_hits", 0)) + 1

                matched_this_query = 0
                for item in items:
                    if not isinstance(item, MarketItem):
                        continue
                    source_pricing = _resolve_stage1_source_pricing(
                        item=item,
                        seed_query=stage1_query,
                        seed_source_title=seed_source_title,
                        timeout=timeout,
                        strict_multi_sku=stage1_multi_sku_strict,
                        allow_non_rakuten_fallback=stage1_multi_sku_fallback_non_rakuten,
                        allow_timeout_fallback=stage1_multi_sku_fallback_on_timeout,
                    )
                    if not bool(source_pricing.get("ok")):
                        _inc_skip(stage1_skip_counts, "skipped_source_variant_unresolved", 1)
                        _append_stage1_rejected(
                            {
                                "seed_query": str(seed_query),
                                "stage1_query": str(stage1_query),
                                "seed_source_title": str(seed_source_title),
                                "seed_source_item_url": str(seed.get("source_item_url", "") or ""),
                                "seed_baseline_usd": round(float(baseline_usd), 2),
                                "seed_baseline_source": str(baseline_source),
                                "seed_collected_sold_90d_count": int(
                                    max(0, to_int(seed.get("seed_collected_sold_90d_count"), -1))
                                ),
                                "site": str(site_key),
                                "site_query": str(site_query),
                                "candidate_title": str(item.title or ""),
                                "candidate_item_id": str(item.item_id or ""),
                                "candidate_item_url": str(item.item_url or ""),
                                "candidate_image_url": str(item.image_url or ""),
                                "candidate_condition": str(item.condition or "new"),
                                "source_price_jpy": round(float(to_float(item.price, 0.0)), 2),
                                "source_shipping_jpy": round(float(to_float(item.shipping, 0.0)), 2),
                                "source_total_jpy": round(float(to_float(item.price, 0.0) + to_float(item.shipping, 0.0)), 2),
                                "reason": str(source_pricing.get("skip_reason", "") or "source_variant_unresolved"),
                                "debug_drop_stage": "stage1",
                                "debug_drop_reason": str(source_pricing.get("skip_reason", "") or "source_variant_unresolved"),
                                "source_variant_price_resolution": source_pricing.get("resolution")
                                if isinstance(source_pricing.get("resolution"), dict)
                                else {},
                            }
                        )
                        continue
                    source_price_jpy = max(0.0, to_float(source_pricing.get("price_jpy"), to_float(item.price, 0.0)))
                    source_shipping_jpy = max(
                        0.0,
                        to_float(source_pricing.get("shipping_jpy"), to_float(item.shipping, 0.0)),
                    )
                    source_total_jpy = source_price_jpy + source_shipping_jpy
                    if source_total_jpy <= 0:
                        _inc_skip(stage1_skip_counts, "skipped_invalid_price", 1)
                        continue
                    score, reason = _seed_title_match_score(
                        seed_query=stage1_query,
                        seed_source_title=seed_source_title,
                        candidate_title=_stage1_candidate_match_text(item),
                        seed_match_context=seed_match_context,
                    )
                    if reason == "accessory_title":
                        _inc_skip(stage1_skip_counts, "skipped_accessory_title", 1)
                        _append_stage1_rejected(
                            {
                                "seed_query": str(seed_query),
                                "stage1_query": str(stage1_query),
                                "seed_source_title": str(seed_source_title),
                                "seed_source_item_url": str(seed.get("source_item_url", "") or ""),
                                "seed_baseline_usd": round(float(baseline_usd), 2),
                                "seed_baseline_source": str(baseline_source),
                                "seed_collected_sold_90d_count": int(
                                    max(0, to_int(seed.get("seed_collected_sold_90d_count"), -1))
                                ),
                                "site": str(site_key),
                                "site_query": str(site_query),
                                "candidate_title": str(item.title or ""),
                                "candidate_item_id": str(item.item_id or ""),
                                "candidate_item_url": str(item.item_url or ""),
                                "candidate_image_url": str(item.image_url or ""),
                                "candidate_condition": str(item.condition or "new"),
                                "source_price_jpy": round(float(source_price_jpy), 2),
                                "source_shipping_jpy": round(float(source_shipping_jpy), 2),
                                "source_total_jpy": round(float(source_total_jpy), 2),
                                "reason": "accessory_title",
                                "debug_drop_stage": "stage1",
                                "debug_drop_reason": "accessory_title",
                            }
                        )
                        continue
                    if score < effective_min_match_score:
                        _inc_skip(stage1_skip_counts, "skipped_low_match", 1)
                        stage1_low_match_reasons[reason] = int(stage1_low_match_reasons.get(reason, 0)) + 1
                        _append_stage1_rejected(
                            {
                                "seed_query": str(seed_query),
                                "stage1_query": str(stage1_query),
                                "seed_source_title": str(seed_source_title),
                                "seed_source_item_url": str(seed.get("source_item_url", "") or ""),
                                "seed_baseline_usd": round(float(baseline_usd), 2),
                                "seed_baseline_source": str(baseline_source),
                                "seed_collected_sold_90d_count": int(
                                    max(0, to_int(seed.get("seed_collected_sold_90d_count"), -1))
                                ),
                                "site": str(site_key),
                                "site_query": str(site_query),
                                "candidate_title": str(item.title or ""),
                                "candidate_item_id": str(item.item_id or ""),
                                "candidate_item_url": str(item.item_url or ""),
                                "candidate_image_url": str(item.image_url or ""),
                                "candidate_condition": str(item.condition or "new"),
                                "score": round(float(score), 4),
                                "min_required_score": round(float(effective_min_match_score), 4),
                                "reason": str(reason),
                                "source_price_jpy": round(float(source_price_jpy), 2),
                                "source_shipping_jpy": round(float(source_shipping_jpy), 2),
                                "source_total_jpy": round(float(source_total_jpy), 2),
                                "debug_drop_stage": "stage1",
                                "debug_drop_reason": str(reason),
                            }
                        )
                        if stage1_low_match_sample_limit <= 0 or len(stage1_low_match_samples) < stage1_low_match_sample_limit:
                            stage1_low_match_samples.append(
                                {
                                    "seed_query": str(seed_query),
                                    "stage1_query": str(stage1_query),
                                    "seed_source_title": str(seed_source_title),
                                    "seed_source_item_url": str(seed.get("source_item_url", "") or ""),
                                    "seed_baseline_usd": round(float(baseline_usd), 2),
                                    "seed_baseline_source": str(baseline_source),
                                    "seed_collected_sold_90d_count": int(
                                        max(0, to_int(seed.get("seed_collected_sold_90d_count"), -1))
                                    ),
                                    "site": str(site_key),
                                    "site_query": str(site_query),
                                    "candidate_title": str(item.title or ""),
                                    "candidate_item_id": str(item.item_id or ""),
                                    "candidate_item_url": str(item.item_url or ""),
                                    "candidate_image_url": str(item.image_url or ""),
                                    "candidate_condition": str(item.condition or "new"),
                                    "score": round(float(score), 4),
                                    "min_required_score": round(float(effective_min_match_score), 4),
                                    "reason": str(reason),
                                    "source_price_jpy": round(float(source_price_jpy), 2),
                                    "source_shipping_jpy": round(float(source_shipping_jpy), 2),
                                    "source_total_jpy": round(float(source_total_jpy), 2),
                                }
                            )
                        continue
                    if baseline_usd > 0 and not baseline_suspicious:
                        source_total_usd = source_total_jpy / fx_seed_rate if fx_seed_rate > 0 else -1.0
                        if source_total_usd <= 0 or source_total_usd >= baseline_usd:
                            stage1_baseline_reject += 1
                            _append_stage1_rejected(
                                {
                                    "seed_query": str(seed_query),
                                    "stage1_query": str(stage1_query),
                                    "seed_source_title": str(seed_source_title),
                                    "seed_source_item_url": str(seed.get("source_item_url", "") or ""),
                                    "seed_baseline_usd": round(float(baseline_usd), 2),
                                    "seed_baseline_source": str(baseline_source),
                                    "seed_collected_sold_90d_count": int(
                                        max(0, to_int(seed.get("seed_collected_sold_90d_count"), -1))
                                    ),
                                    "site": str(site_key),
                                    "site_query": str(site_query),
                                    "candidate_title": str(item.title or ""),
                                    "candidate_item_id": str(item.item_id or ""),
                                    "candidate_item_url": str(item.item_url or ""),
                                    "candidate_image_url": str(item.image_url or ""),
                                    "candidate_condition": str(item.condition or "new"),
                                    "source_price_jpy": round(float(source_price_jpy), 2),
                                    "source_shipping_jpy": round(float(source_shipping_jpy), 2),
                                    "source_total_jpy": round(float(source_total_jpy), 2),
                                    "reason": "seed_baseline_reject",
                                    "debug_drop_stage": "stage1",
                                    "debug_drop_reason": "seed_baseline_reject",
                                }
                            )
                            continue

                    item_key = "|".join(
                        [
                            str(item.site or ""),
                            str(item.item_id or ""),
                            str(item.item_url or ""),
                            _seed_key(str(item.title or "")),
                        ]
                    )
                    candidate_row = {
                        "item": item,
                        "site": site_key,
                        "score": float(score),
                        "reason": str(reason),
                        "source_price_jpy": float(source_price_jpy),
                        "source_shipping_jpy": float(source_shipping_jpy),
                        "source_total_jpy": float(source_total_jpy),
                        "source_price_basis_type": str(source_pricing.get("price_basis_type", "") or "listing_price"),
                        "source_variant_resolution": source_pricing.get("resolution")
                        if isinstance(source_pricing.get("resolution"), dict)
                        else {},
                    }
                    existing_candidate = stage1_candidates_by_key.get(item_key)
                    if existing_candidate is not None:
                        current_rank = (
                            to_float(candidate_row.get("source_total_jpy"), 10**12),
                            -to_float(candidate_row.get("score"), 0.0),
                            str(item.title or ""),
                        )
                        existing_item = existing_candidate.get("item")
                        existing_rank = (
                            to_float(existing_candidate.get("source_total_jpy"), 10**12),
                            -to_float(existing_candidate.get("score"), 0.0),
                            str(existing_item.title if isinstance(existing_item, MarketItem) else ""),
                        )
                        if current_rank >= existing_rank:
                            continue
                    stage1_candidates_by_key[item_key] = candidate_row
                    selected_for_site += 1
                    matched_this_query += 1

                if stage1_include_diagnostics:
                    site_query_logs.append(
                        {
                            "query": site_query,
                            "ok": True,
                            "raw_count": len(items),
                            "matched_count": int(matched_this_query),
                            "category_filter": (fetch_info or {}).get("category_filter")
                            if isinstance(fetch_info, dict)
                            else {},
                        }
                    )
                if selected_for_site >= stage1_take_per_seed:
                    break
            if stage1_include_diagnostics:
                stage1_site_logs.append(
                    {
                        "site": site_key,
                        "queries": list(site_queries),
                        "seed_query": seed_query,
                        "ok": True,
                        "selected_count": int(selected_for_site),
                        "query_logs": site_query_logs,
                    }
                )

        stage1_candidates = list(stage1_candidates_by_key.values())
        stage1_candidates.sort(
            key=lambda row: (
                to_float(row.get("source_total_jpy"), 10**12),
                -to_float(row.get("score"), 0.0),
                str(row.get("item").title if isinstance(row.get("item"), MarketItem) else ""),
            )
        )
        selected_stage1_raw = stage1_candidates[:stage1_take_per_seed]
        selected_stage1: List[Dict[str, Any]] = []
        stage1_selected_rows_preview: List[Dict[str, Any]] = []
        for row in selected_stage1_raw:
            item = row.get("item")
            if not isinstance(item, MarketItem):
                continue
            stage_b_key = "|".join(
                [
                    str(item.site or ""),
                    str(item.item_id or ""),
                    str(item.item_url or ""),
                    f"{to_float(row.get('source_total_jpy'), 0.0):.2f}",
                ]
            )
            if stage_b_key in stage_b_seen_keys:
                continue
            stage_b_seen_keys.add(stage_b_key)
            selected_stage1.append(row)
            stage1_rank = len(selected_stage1)
            if stage1_include_diagnostics:
                stage1_selected_rows_preview.append(
                    {
                        "stage1_rank": int(stage1_rank),
                        "source_site": str(item.site or row.get("site") or ""),
                        "source_item_id": str(item.item_id or ""),
                        "source_item_url": str(item.item_url or ""),
                        "source_title": str(item.title or ""),
                        "source_price_jpy": float(to_float(row.get("source_price_jpy"), to_float(item.price, 0.0))),
                        "source_shipping_jpy": float(
                            to_float(row.get("source_shipping_jpy"), to_float(item.shipping, 0.0))
                        ),
                        "source_total_jpy": float(to_float(row.get("source_total_jpy"), 0.0)),
                        "source_price_basis_type": str(row.get("source_price_basis_type", "") or "listing_price"),
                        "stage1_match_score": float(to_float(row.get("score"), 0.0)),
                        "stage1_match_reason": str(row.get("reason", "") or ""),
                    }
                )
            stage_b_rows.append(
                {
                    "seed_id": int(to_int(seed.get("id"), 0)),
                    "stage1_rank": int(stage1_rank),
                    "seed_key": str(seed_key_text),
                    "seed_query": str(seed_query),
                    "seed_source_title": str(seed_source_title),
                    "seed_baseline_source": str(baseline_source),
                    "seed_baseline_usd": float(baseline_usd),
                    "stage1_query": str(stage1_query),
                    "source_site": str(item.site or row.get("site") or ""),
                    "source_item_id": str(item.item_id or ""),
                    "source_item_url": str(item.item_url or ""),
                    "source_title": str(item.title or ""),
                    "source_image_url": str(item.image_url or ""),
                    "source_currency": str(item.currency or "JPY"),
                    "source_condition": str(item.condition or "new"),
                    "source_price_jpy": float(to_float(row.get("source_price_jpy"), to_float(item.price, 0.0))),
                    "source_shipping_jpy": float(
                        to_float(row.get("source_shipping_jpy"), to_float(item.shipping, 0.0))
                    ),
                    "source_total_jpy": float(to_float(row.get("source_total_jpy"), 0.0)),
                    "source_price_basis_type": str(row.get("source_price_basis_type", "") or "listing_price"),
                    "source_variant_price_resolution": row.get("source_variant_resolution")
                    if isinstance(row.get("source_variant_resolution"), dict)
                    else {},
                    "stage1_match_score": float(to_float(row.get("score"), 0.0)),
                    "stage1_match_reason": str(row.get("reason", "") or ""),
                }
            )
        stage1_count = len(selected_stage1)
        stage1_seed_feedback_rows.append(
            {
                "seed_id": int(to_int(seed.get("id"), 0)),
                "had_raw_results": bool(stage1_raw_item_count > 0),
                "had_stage1_candidates": bool(stage1_count > 0),
            }
        )
        stage1_seed_baseline_reject_total += int(stage1_baseline_reject)
        stage1_pass_total += stage1_count

        def _pass_row(*, stage2_created_count: int) -> Dict[str, Any]:
            row_payload: Dict[str, Any] = {
                "pass": idx,
                "seed_query": seed_query,
                "stage1_query": stage1_query,
                "stage1_candidate_count": int(stage1_count),
                "stage2_created_count": int(stage2_created_count),
                "stage1_seed_baseline_reject_count": int(stage1_baseline_reject),
                "stage1_seed_baseline_softened": bool(baseline_suspicious),
                "elapsed_sec": round(time.monotonic() - started, 3),
                "min_match_score": effective_min_match_score,
                "has_model_code": has_model_code,
            }
            if stage1_include_diagnostics:
                row_payload["stage1_site_logs"] = stage1_site_logs
                row_payload["stage1_selected_rows"] = stage1_selected_rows_preview
            return row_payload

        if stage1_count <= 0:
            if stage1_raw_item_count <= 0:
                _append_stage1_no_hit(
                    {
                        "seed_query": str(seed_query),
                        "stage1_query": str(stage1_query),
                        "seed_source_title": str(seed_source_title),
                        "seed_source_item_url": str(seed.get("source_item_url", "") or ""),
                        "seed_baseline_usd": round(float(baseline_usd), 2),
                        "seed_baseline_source": str(baseline_source),
                        "seed_collected_sold_90d_count": int(
                            max(0, to_int(seed.get("seed_collected_sold_90d_count"), -1))
                        ),
                        "site": "",
                        "site_query": "",
                        "candidate_title": "",
                        "candidate_item_id": "",
                        "candidate_item_url": "",
                        "candidate_image_url": "",
                        "candidate_condition": "new",
                        "score": 0.0,
                        "min_required_score": round(float(effective_min_match_score), 4),
                        "reason": "no_source_hit",
                        "source_price_jpy": 0.0,
                        "source_shipping_jpy": 0.0,
                        "source_total_jpy": 0.0,
                        "debug_drop_stage": "stage1",
                        "debug_drop_reason": "no_source_hit",
                    }
                )
            passes.append(_pass_row(stage2_created_count=0))
            continue

        if callable(progress_callback):
            progress_callback(
                {
                    "phase": "stage2_running",
                    "message": f"最終再判定 {idx}/{len(selected_seeds)}: {seed_query}",
                    "progress_percent": min(93.0, 16.0 + (72.0 * idx / max(1, len(selected_seeds)))),
                    "pass_index": idx,
                    "max_passes": len(selected_seeds),
                    "created_count": len(created_ids),
                    "seed_count": len(selected_seeds),
                    "selected_seed_count": len(selected_seeds),
                    "pool_available": available_after,
                    "current_seed_query": seed_query,
                    "current_stage1_query": stage1_query,
                    "current_seed_quality_score": to_int(seed.get("seed_quality_score"), 0),
                    "stage1_candidate_count": int(stage1_count),
                    "stage2_created_count": 0,
                    "stage1_pass_total": int(stage1_pass_total),
                    "stage2_runs": int(stage2_runs),
                    "elapsed_sec": round(time.monotonic() - started, 3),
                    "flow_stage": "C",
                    "flow_stage_label": "C: eBay最終再判定",
                    "flow_stage_index": 3,
                    "flow_stage_total": 3,
                }
            )

        stage2_created_count = 0
        for stage2_index, row in enumerate(selected_stage1, start=1):
            item = row.get("item")
            if not isinstance(item, MarketItem):
                _inc_skip(stage2_skip_counts, "skipped_invalid_price", 1)
                continue
            stage2_item_key = "|".join(
                [
                    str(item.site or ""),
                    str(item.item_id or ""),
                    str(item.item_url or ""),
                    f"{to_float(row.get('source_total_jpy'), 0.0):.2f}",
                ]
            )
            if stage2_item_key in stage2_seen_item_keys:
                _inc_skip(stage2_skip_counts, "skipped_duplicates", 1)
                continue
            stage2_seen_item_keys.add(stage2_item_key)
            stage2_runs += 1
            jp_seed_query = _pick_jp_seed_query(
                seed_query=stage1_query,
                source_title=str(item.title or ""),
                brand_hints=brand_hints,
            )
            liquidity_query = _pick_liquidity_query(
                seed_query=stage1_query,
                jp_seed_query=jp_seed_query,
                seed_source_title=seed_source_title,
                source_title=str(item.title or ""),
                source_identifiers=item.identifiers if isinstance(item.identifiers, dict) else {},
            )
            query_key = re.sub(r"\s+", " ", str(liquidity_query or "").strip()).lower()
            signal = _get_stage2_liquidity_signal(item, liquidity_query)
            sold_count_raw = to_int(signal.get("sold_90d_count"), -1)
            if sold_count_raw < 0 and stage2_liquidity_query_fallback_max > 0:
                fallback_queries = _liquidity_refresh_queries_for_seed(
                    liquidity_query,
                    max_count=max(2, stage2_liquidity_query_fallback_max + 2),
                    source_title=f"{seed_source_title} {str(item.title or '')} {stage1_query}".strip(),
                    brand_hints=brand_hints,
                )
                # まず seed query / stage1 query を優先候補として試す。
                preferred_queries: List[str] = []
                for candidate in (stage1_query, seed_query):
                    text = re.sub(r"\s+", " ", str(candidate or "").strip())
                    if text:
                        preferred_queries.append(text)
                for candidate in fallback_queries:
                    text = re.sub(r"\s+", " ", str(candidate or "").strip())
                    if text:
                        preferred_queries.append(text)
                seen_query_keys: Set[str] = set()
                tried = 0
                for candidate_query in preferred_queries:
                    ckey = candidate_query.lower()
                    if not ckey or ckey in seen_query_keys:
                        continue
                    seen_query_keys.add(ckey)
                    if ckey == query_key:
                        continue
                    cand_signal = _get_stage2_liquidity_signal(item, candidate_query)
                    if _is_rpa_daily_limit_signal(cand_signal):
                        rpa_daily_limit_reached = True
                        stop_reason = "rpa_daily_limit_reached"
                        break
                    cand_sold = to_int(cand_signal.get("sold_90d_count"), -1)
                    tried += 1
                    if cand_sold >= 0:
                        signal = cand_signal
                        sold_count_raw = cand_sold
                        liquidity_query = candidate_query
                        query_key = liquidity_query.lower()
                        break
                    if tried >= stage2_liquidity_query_fallback_max:
                        break
            if _is_rpa_daily_limit_signal(signal):
                rpa_daily_limit_reached = True
                stop_reason = "rpa_daily_limit_reached"
                break
            sold_count_raw = to_int(signal.get("sold_90d_count"), -1)
            if sold_count_raw < 0 and stage2_liquidity_refresh_enabled:
                can_retry = (
                    bool(query_key)
                    and query_key not in stage2_liquidity_retry_queries_seen
                    and stage2_liquidity_retry_budget_used < stage2_liquidity_retry_budget_total
                )
                if can_retry:
                    stage2_liquidity_retry_queries_seen.add(query_key)
                    stage2_liquidity_retry_budget_used += 1
                    retry_queries = _liquidity_refresh_queries_for_seed(
                        liquidity_query,
                        max_count=2,
                        source_title=f"{seed_source_title} {str(item.title or '')}".strip(),
                        brand_hints=brand_hints,
                    )
                    retry_summary = _refresh_liquidity_rpa(
                        retry_queries,
                        max_queries=2,
                        force=True,
                        category_id=int(pr_category_filter.get("category_id", 0) or 0),
                        category_slug=str(pr_category_filter.get("category_slug", "") or ""),
                    )
                    stage2_liquidity_refresh_runs.append(
                        {
                            "phase": "on_miss_retry",
                            "query": liquidity_query,
                            "summary": retry_summary,
                        }
                    )
                    if _rpa_daily_limit_reached(retry_summary):
                        rpa_daily_limit_reached = True
                        stop_reason = "rpa_daily_limit_reached"
                        break
                    signal = _get_stage2_liquidity_signal(item, liquidity_query)
                    if _is_rpa_daily_limit_signal(signal):
                        rpa_daily_limit_reached = True
                        stop_reason = "rpa_daily_limit_reached"
                        break
                    sold_count_raw = to_int(signal.get("sold_90d_count"), -1)
            if (
                sold_count_raw < 0
                and bool(stage2_allow_seed_signal_fallback)
                and bool(stage2_allow_missing_sold_sample)
            ):
                seed_fallback_sold_count = max(0, to_int(seed.get("seed_collected_sold_90d_count"), -1))
                seed_fallback_sold_min = max(0.0, to_float(seed.get("seed_collected_sold_price_min_usd"), -1.0))
                if seed_fallback_sold_count >= liquidity_min_sold_90d and seed_fallback_sold_min > 0:
                    signal = {
                        "sold_90d_count": int(seed_fallback_sold_count),
                        "active_count": -1,
                        "sold_price_min": float(seed_fallback_sold_min),
                        "sold_price_median": float(seed_fallback_sold_min),
                        "sold_price_currency": "USD",
                        "source": "seed_collected_fallback",
                        "confidence": 0.35,
                        "unavailable_reason": "",
                        "metadata": {
                            "fallback_from_seed_pool": True,
                            "fallback_reason": "liquidity_signal_unavailable",
                            "seed_query": str(seed_query),
                            "seed_key": str(seed_key_text),
                            "seed_collected_sold_90d_count": int(seed_fallback_sold_count),
                            "seed_collected_sold_price_min_usd": float(seed_fallback_sold_min),
                            "sold_sample": {
                                "title": str(liquidity_query or jp_seed_query or seed_query),
                                "sold_price_usd": float(seed_fallback_sold_min),
                                "sold_price": float(seed_fallback_sold_min),
                                "item_url": "",
                                "item_id": "",
                                "image_url": "",
                            },
                        },
                    }
                    sold_count_raw = int(seed_fallback_sold_count)

            def _stage2_rejected_common(*, gate_passed: bool, gate_reason: str = "") -> Dict[str, Any]:
                sold_sample_hint = _liquidity_sold_sample(signal)
                active_sample_hint = _liquidity_active_sample(signal)
                market_item_url_hint = str(
                    sold_sample_hint.get("item_url", "")
                    or active_sample_hint.get("item_url", "")
                    or ""
                ).strip()
                market_image_url_hint = str(
                    sold_sample_hint.get("image_url", "")
                    or active_sample_hint.get("image_url", "")
                    or ""
                ).strip()
                market_title_hint = str(
                    sold_sample_hint.get("title", "")
                    or active_sample_hint.get("title", "")
                    or seed_source_title
                    or liquidity_query
                    or seed_query
                ).strip()
                market_item_id_hint = _ebay_item_id_from_url(market_item_url_hint)
                return {
                    "seed_query": str(seed_query),
                    "stage1_query": str(stage1_query),
                    "seed_source_title": str(seed_source_title),
                    "seed_source_item_url": str(seed.get("source_item_url", "") or ""),
                    "source_site": str(item.site or row.get("site") or ""),
                    "source_title": str(item.title or ""),
                    "source_item_id": str(item.item_id or ""),
                    "source_item_url": str(item.item_url or ""),
                    "source_image_url": str(item.image_url or ""),
                    "source_condition": str(item.condition or "new"),
                    "source_price_jpy": round(
                        float(to_float(row.get("source_price_jpy"), to_float(item.price, 0.0))), 2
                    ),
                    "source_shipping_jpy": round(
                        float(to_float(row.get("source_shipping_jpy"), to_float(item.shipping, 0.0))), 2
                    ),
                    "source_total_jpy": round(float(to_float(row.get("source_total_jpy"), 0.0)), 2),
                    "liquidity_query": str(liquidity_query),
                    "market_title": market_title_hint,
                    "market_item_id": market_item_id_hint,
                    "market_item_url": market_item_url_hint,
                    "market_image_url": market_image_url_hint,
                    "ebay_sold_title": str(sold_sample_hint.get("title", "") or market_title_hint),
                    "ebay_sold_item_url": market_item_url_hint,
                    "ebay_sold_image_url": market_image_url_hint,
                    "ebay_active_item_url": str(active_sample_hint.get("item_url", "") or "").strip(),
                    "ebay_active_image_url": str(active_sample_hint.get("image_url", "") or "").strip(),
                    "sold_90d_count": int(max(0, to_int(signal.get("sold_90d_count"), -1))),
                    "sold_price_min_usd": round(float(_liquidity_sold_min_usd(signal)), 2),
                    "active_count": int(to_int(signal.get("active_count"), -1)),
                    "active_price_min_usd": round(float(_liquidity_active_min_usd(signal)), 2),
                    "liquidity_gate_passed": bool(gate_passed),
                    "liquidity_gate_reason": str(gate_reason or "").strip(),
                    "stage1_match_score": round(float(to_float(row.get("score"), 0.0)), 4),
                    "stage1_match_reason": str(row.get("reason", "") or ""),
                }
            if sold_count_raw < 0:
                _inc_skip(stage2_skip_counts, "skipped_liquidity_unavailable", 1)
                unavailable_reason = str(signal.get("unavailable_reason", "") or "").strip() or "unknown"
                stage2_low_match_reasons[unavailable_reason] = (
                    int(stage2_low_match_reasons.get(unavailable_reason, 0)) + 1
                )
                _append_stage2_rejected(
                    {
                        **_stage2_rejected_common(gate_passed=False, gate_reason=str(unavailable_reason)),
                        "seed_baseline_usd": round(float(baseline_usd), 2),
                        "seed_baseline_source": str(baseline_source),
                        "reason": str(unavailable_reason),
                        "debug_drop_stage": "stage2",
                        "debug_drop_reason": "liquidity_unavailable",
                    }
                )
                if (
                    stage2_liquidity_unavailable_example_limit <= 0
                    or len(stage2_liquidity_unavailable_examples) < stage2_liquidity_unavailable_example_limit
                ):
                    stage2_liquidity_unavailable_examples.append(
                        {
                            **_stage2_rejected_common(gate_passed=False, gate_reason=str(unavailable_reason)),
                            "seed_baseline_usd": round(float(baseline_usd), 2),
                            "seed_baseline_source": str(baseline_source),
                            "unavailable_reason": str(unavailable_reason),
                        }
                    )
                continue
            sold_count_90d = max(0, sold_count_raw)
            if sold_count_90d < liquidity_min_sold_90d:
                _inc_skip(stage2_skip_counts, "skipped_low_liquidity", 1)
                _append_stage2_rejected(
                    {
                        **_stage2_rejected_common(gate_passed=False, gate_reason="low_liquidity"),
                        "seed_baseline_usd": round(float(baseline_usd), 2),
                        "seed_baseline_source": str(baseline_source),
                        "min_required": int(liquidity_min_sold_90d),
                        "reason": "low_liquidity",
                        "debug_drop_stage": "stage2",
                        "debug_drop_reason": "low_liquidity",
                    }
                )
                if (
                    stage2_low_liquidity_example_limit <= 0
                    or len(stage2_low_liquidity_examples) < stage2_low_liquidity_example_limit
                ):
                    stage2_low_liquidity_examples.append(
                        {
                            **_stage2_rejected_common(gate_passed=False, gate_reason="low_liquidity"),
                            "seed_baseline_usd": round(float(baseline_usd), 2),
                            "seed_baseline_source": str(baseline_source),
                            "min_required": int(liquidity_min_sold_90d),
                        }
                    )
                if _seed_low_liquidity_cooldown_enabled():
                    stage2_low_liquidity_cooldown_rows.append(
                        {
                            "seed_query": seed_query,
                            "seed_key": seed_key_text,
                            "sold_90d_count": sold_count_90d,
                            "min_required": liquidity_min_sold_90d,
                            "metadata": {
                                "liquidity_query": liquidity_query,
                                "stage1_match_reason": str(row.get("reason", "") or ""),
                                "stage1_site": str(row.get("site", "") or ""),
                                "source_title": str(item.title or ""),
                            },
                        }
                    )
                continue
            sold_min_usd = _liquidity_sold_min_usd(signal)
            if sold_min_usd <= 0:
                _inc_skip(stage2_skip_counts, "skipped_missing_sold_min", 1)
                _append_stage2_rejected(
                    {
                        **_stage2_rejected_common(gate_passed=False, gate_reason="missing_sold_min"),
                        "reason": "missing_sold_min",
                        "debug_drop_stage": "stage2",
                        "debug_drop_reason": "missing_sold_min",
                    }
                )
                continue
            sold_sample = _liquidity_sold_sample(signal)
            sold_item_url = str(sold_sample.get("item_url", "") or "").strip()
            sold_sample_price = to_float(sold_sample.get("sold_price_usd"), -1.0)
            used_missing_sold_sample_fallback = False
            if not sold_item_url or sold_sample_price <= 0:
                if stage2_liquidity_refresh_enabled:
                    can_retry_missing_sample = (
                        bool(query_key)
                        and query_key not in stage2_liquidity_retry_queries_seen
                        and stage2_liquidity_retry_budget_used < stage2_liquidity_retry_budget_total
                    )
                    if can_retry_missing_sample:
                        stage2_liquidity_retry_queries_seen.add(query_key)
                        stage2_liquidity_retry_budget_used += 1
                        retry_queries = _liquidity_refresh_queries_for_seed(
                            liquidity_query,
                            max_count=2,
                            source_title=f"{seed_source_title} {str(item.title or '')}".strip(),
                            brand_hints=brand_hints,
                        )
                        retry_summary = _refresh_liquidity_rpa(
                            retry_queries,
                            max_queries=2,
                            force=True,
                            category_id=int(pr_category_filter.get("category_id", 0) or 0),
                            category_slug=str(pr_category_filter.get("category_slug", "") or ""),
                        )
                        stage2_liquidity_refresh_runs.append(
                            {
                                "phase": "on_missing_sample_retry",
                                "query": liquidity_query,
                                "summary": retry_summary,
                            }
                        )
                        if _rpa_daily_limit_reached(retry_summary):
                            rpa_daily_limit_reached = True
                            stop_reason = "rpa_daily_limit_reached"
                            break
                        signal = _get_stage2_liquidity_signal(item, liquidity_query)
                        if _is_rpa_daily_limit_signal(signal):
                            rpa_daily_limit_reached = True
                            stop_reason = "rpa_daily_limit_reached"
                            break
                        sold_min_usd = _liquidity_sold_min_usd(signal)
                        if sold_min_usd <= 0:
                            _inc_skip(stage2_skip_counts, "skipped_missing_sold_min", 1)
                            continue
                        sold_sample = _liquidity_sold_sample(signal)
                        sold_item_url = str(sold_sample.get("item_url", "") or "").strip()
                        sold_sample_price = to_float(sold_sample.get("sold_price_usd"), -1.0)

            if stop_reason == "rpa_daily_limit_reached":
                break

            if not sold_item_url or sold_sample_price <= 0:
                if not bool(stage2_allow_missing_sold_sample):
                    _inc_skip(stage2_skip_counts, "skipped_missing_sold_sample", 1)
                    _append_stage2_rejected(
                        {
                            **_stage2_rejected_common(gate_passed=False, gate_reason="missing_sold_sample"),
                            "reason": "missing_sold_sample",
                            "debug_drop_stage": "stage2",
                            "debug_drop_reason": "missing_sold_sample",
                        }
                    )
                    continue
                used_missing_sold_sample_fallback = True
                sold_sample_price = float(sold_min_usd)
                if not str(sold_sample.get("title", "") or "").strip():
                    sold_sample["title"] = str(liquidity_query or jp_seed_query or seed_query)
            active_count_signal = to_int(signal.get("active_count"), -1)
            active_min_usd = _liquidity_active_min_usd(signal)
            active_sample = _liquidity_active_sample(signal)

            source_price_jpy = max(
                0.0,
                to_float(row.get("source_price_jpy"), to_float(item.price, 0.0)),
            )
            source_shipping_jpy = max(
                0.0,
                to_float(row.get("source_shipping_jpy"), to_float(item.shipping, 0.0)),
            )
            source_total_jpy = source_price_jpy + source_shipping_jpy
            if source_total_jpy <= 0:
                _inc_skip(stage2_skip_counts, "skipped_invalid_price", 1)
                continue

            calc = calculate_profit(
                ProfitInput(
                    sale_price_usd=sold_min_usd,
                    purchase_price_jpy=source_price_jpy,
                    domestic_shipping_jpy=source_shipping_jpy,
                    international_shipping_usd=international_shipping_usd,
                    customs_usd=customs_usd,
                    packaging_usd=packaging_usd,
                    marketplace_fee_rate=marketplace_fee_rate,
                    payment_fee_rate=payment_fee_rate,
                    fixed_fee_usd=fixed_fee_usd,
                ),
                settings=settings,
            )
            breakdown = calc.get("breakdown", {}) if isinstance(calc.get("breakdown"), dict) else {}
            fx = calc.get("fx", {}) if isinstance(calc.get("fx"), dict) else {}
            profit_usd = to_float(breakdown.get("profit_usd"), -1.0)
            margin_rate = to_float(breakdown.get("margin_rate"), -1.0)
            if profit_usd < float(min_profit_usd):
                _inc_skip(stage2_skip_counts, "skipped_unprofitable", 1)
                _append_stage2_rejected(
                    {
                        **_stage2_rejected_common(gate_passed=True, gate_reason=""),
                        "expected_profit_usd": round(float(profit_usd), 4),
                        "expected_margin_rate": round(float(margin_rate), 6),
                        "reason": "unprofitable",
                        "debug_drop_stage": "stage2",
                        "debug_drop_reason": "unprofitable",
                    }
                )
                continue
            if margin_rate < float(min_margin_rate):
                _inc_skip(stage2_skip_counts, "skipped_low_margin", 1)
                _append_stage2_rejected(
                    {
                        **_stage2_rejected_common(gate_passed=True, gate_reason=""),
                        "expected_profit_usd": round(float(profit_usd), 4),
                        "expected_margin_rate": round(float(margin_rate), 6),
                        "reason": "low_margin",
                        "debug_drop_stage": "stage2",
                        "debug_drop_reason": "low_margin",
                    }
                )
                continue

            fx_rate = to_float(fx.get("rate"), 0.0)
            source_total_usd = (source_total_jpy / fx_rate) if fx_rate > 0 else -1.0
            if source_total_usd <= 0 or source_total_usd >= sold_min_usd:
                _inc_skip(stage2_skip_counts, "skipped_below_sold_min", 1)
                _append_stage2_rejected(
                    {
                        **_stage2_rejected_common(gate_passed=True, gate_reason=""),
                        "expected_profit_usd": round(float(profit_usd), 4),
                        "expected_margin_rate": round(float(margin_rate), 6),
                        "reason": "below_sold_min",
                        "debug_drop_stage": "stage2",
                        "debug_drop_reason": "below_sold_min",
                    }
                )
                continue

            pair_signature = "|".join(
                [
                    str(item.site or ""),
                    str(item.item_id or ""),
                    str(item.item_url or ""),
                    sold_item_url or str(liquidity_query or ""),
                ]
            )
            if pair_signature in runtime_pair_signatures:
                _inc_skip(stage2_skip_counts, "skipped_duplicates", 1)
                continue
            runtime_pair_signatures.add(pair_signature)

            # active 指標は候補化条件ではないため、候補化可能と判定できた行のみ再取得する。
            if stage2_retry_missing_active_enabled and active_count_signal < 0 and active_min_usd <= 0:
                can_retry_missing_active = (
                    bool(query_key)
                    and query_key not in stage2_liquidity_retry_queries_seen
                    and stage2_liquidity_retry_budget_used < stage2_liquidity_retry_budget_total
                )
                if can_retry_missing_active:
                    stage2_liquidity_retry_queries_seen.add(query_key)
                    stage2_liquidity_retry_budget_used += 1
                    retry_queries = _liquidity_refresh_queries_for_seed(
                        liquidity_query,
                        max_count=2,
                        source_title=f"{seed_source_title} {str(item.title or '')}".strip(),
                        brand_hints=brand_hints,
                    )
                    retry_summary = _refresh_liquidity_rpa(
                        retry_queries,
                        max_queries=2,
                        force=True,
                        category_id=int(pr_category_filter.get("category_id", 0) or 0),
                        category_slug=str(pr_category_filter.get("category_slug", "") or ""),
                    )
                    stage2_liquidity_refresh_runs.append(
                        {
                            "phase": "on_missing_active_retry",
                            "query": liquidity_query,
                            "summary": retry_summary,
                        }
                    )
                    if _rpa_daily_limit_reached(retry_summary):
                        rpa_daily_limit_reached = True
                        stop_reason = "rpa_daily_limit_reached"
                        break
                    signal = _get_stage2_liquidity_signal(item, liquidity_query)
                    if _is_rpa_daily_limit_signal(signal):
                        rpa_daily_limit_reached = True
                        stop_reason = "rpa_daily_limit_reached"
                        break
                    active_count_signal = to_int(signal.get("active_count"), -1)
                    active_min_usd = _liquidity_active_min_usd(signal)
                    active_sample = _liquidity_active_sample(signal)

            if stop_reason == "rpa_daily_limit_reached":
                break

            active_item_url = str(active_sample.get("item_url", "") or "").strip()
            active_item_title = str(active_sample.get("title", "") or "").strip()
            active_image_url = str(active_sample.get("image_url", "") or "").strip()
            active_sample_price = to_float(active_sample.get("active_price_usd"), -1.0)

            score = to_float(row.get("score"), 0.0)
            market_title = str(sold_sample.get("title", "") or "").strip() or jp_seed_query
            market_item_id = _ebay_item_id_from_url(sold_item_url)
            if not market_item_id and bool(used_missing_sold_sample_fallback):
                market_item_id = _seed_key(str(liquidity_query or jp_seed_query or seed_query))
            sold_item_detail: Dict[str, Any] = {}
            if bool(stage2_ebay_item_detail_enabled) and sold_item_url:
                cached_detail = stage2_ebay_item_detail_cache.get(sold_item_url)
                if isinstance(cached_detail, dict):
                    sold_item_detail = dict(cached_detail)
                elif stage2_ebay_item_detail_fetched < stage2_ebay_item_detail_max_fetch:
                    detail = _fetch_ebay_item_detail_from_url(
                        sold_item_url,
                        timeout=stage2_ebay_item_detail_timeout_sec,
                    )
                    sold_item_detail = detail if isinstance(detail, dict) else {}
                    stage2_ebay_item_detail_cache[sold_item_url] = dict(sold_item_detail)
                    stage2_ebay_item_detail_fetched += 1
            detail_title = str(sold_item_detail.get("title", "") or "").strip()
            if detail_title:
                market_title = detail_title
            market_identifiers: Dict[str, str] = {}
            detail_brand = str(sold_item_detail.get("brand", "") or "").strip()
            detail_model = str(sold_item_detail.get("model", "") or "").strip()
            if detail_brand:
                market_identifiers["brand"] = detail_brand
            if detail_model:
                market_identifiers["model"] = detail_model
                market_identifiers["mpn"] = detail_model
            if (not detail_model) and market_title:
                codes = _extract_codes(market_title)
                if codes:
                    first_code = str(codes[0] or "").strip()
                    if first_code:
                        market_identifiers["model"] = first_code
                        market_identifiers["mpn"] = first_code
            candidate_payload = {
                "source_site": str(item.site or row.get("site") or ""),
                "market_site": str(market_site or "ebay"),
                "source_item_id": str(item.item_id or ""),
                "market_item_id": market_item_id,
                "source_title": str(item.title or ""),
                "market_title": market_title,
                "condition": "new",
                "match_level": _match_level_from_score(score),
                "match_score": float(round(score, 4)),
                "expected_profit_usd": float(round(profit_usd, 4)),
                "expected_margin_rate": float(round(margin_rate, 6)),
                "fx_rate": float(to_float(fx.get("rate"), fx_seed_rate)),
                "fx_source": str(fx.get("source", "") or ""),
                "metadata": {
                    "source_item_url": str(item.item_url or ""),
                    "source_image_url": str(item.image_url or ""),
                    "source_price_jpy": float(source_price_jpy),
                    "source_shipping_jpy": float(source_shipping_jpy),
                    "source_total_jpy": float(source_total_jpy),
                    "source_currency": str(item.currency or "JPY"),
                    "source_identifiers": item.identifiers if isinstance(item.identifiers, dict) else {},
                    "market_identifiers": market_identifiers,
                    "source_price_basis_type": str(row.get("source_price_basis_type", "") or "listing_price"),
                    "source_variant_price_resolution": row.get("source_variant_resolution")
                    if isinstance(row.get("source_variant_resolution"), dict)
                    else {},
                    "market_item_url": sold_item_url,
                    "market_image_url": str(
                        sold_sample.get("image_url", "")
                        or sold_item_detail.get("image_url", "")
                        or ""
                    ),
                    "market_item_url_active": active_item_url,
                    "market_image_url_active": active_image_url,
                    "market_price_basis_usd": float(sold_min_usd),
                    "market_shipping_basis_usd": 0.0,
                    "market_revenue_basis_usd": float(sold_min_usd),
                    "market_price_basis_type": "sold_price_min_90d",
                    "ebay_sold_item_url": sold_item_url,
                    "ebay_sold_image_url": str(
                        sold_sample.get("image_url", "")
                        or sold_item_detail.get("image_url", "")
                        or ""
                    ),
                    "ebay_sold_title": str(sold_item_detail.get("title", "") or market_title),
                    "ebay_sold_price_usd": float(sold_sample_price),
                    "ebay_sold_sample_reference_ok": not bool(used_missing_sold_sample_fallback),
                    "ebay_sold_sample_fallback_used": bool(used_missing_sold_sample_fallback),
                    "ebay_active_count": int(active_count_signal),
                    "ebay_active_price_min_usd": float(active_min_usd) if active_min_usd > 0 else -1.0,
                    "ebay_active_item_url": active_item_url,
                    "ebay_active_image_url": active_image_url,
                    "ebay_active_title": active_item_title,
                    "ebay_active_sample_price_usd": float(active_sample_price) if active_sample_price > 0 else -1.0,
                    "ebay_sold_detail": sold_item_detail,
                    "ebay_sold_detail_brand": str(sold_item_detail.get("brand", "") or "").strip(),
                    "ebay_sold_detail_title": str(sold_item_detail.get("title", "") or "").strip(),
                    "ebay_sold_detail_price_usd": float(to_float(sold_item_detail.get("price_usd"), -1.0)),
                    "ebay_sold_detail_shipping_usd": float(to_float(sold_item_detail.get("shipping_usd"), -1.0)),
                    "liquidity_query": liquidity_query,
                    "liquidity": signal,
                    "seed_pool": {
                        "id": to_int(seed.get("id"), 0),
                        "seed_query": seed_query,
                        "seed_source_title": seed_source_title,
                        "seed_collected_sold_price_min_usd": baseline_usd,
                        "seed_collected_sold_price_min_usd_source": str(baseline_source),
                    },
                    "seed_jp": {
                        "query": jp_seed_query,
                        "liquidity_query": liquidity_query,
                        "stage2_index": int(stage2_index),
                        "stage1_site": str(row.get("site", "") or ""),
                        "stage1_match_reason": str(row.get("reason", "") or ""),
                        "stage1_match_score": float(round(score, 4)),
                        "source_price_basis_type": str(row.get("source_price_basis_type", "") or "listing_price"),
                    },
                    "calc_input": calc.get("input", {}),
                    "calc_breakdown": calc.get("breakdown", {}),
                    "calc_fx": calc.get("fx", {}),
                    "pair_signature": pair_signature,
                },
            }
            created = create_miner_candidate(candidate_payload, settings=settings)
            cid = to_int(created.get("id"), -1)
            if cid <= 0 or cid in created_seen:
                _inc_skip(stage2_skip_counts, "skipped_duplicates", 1)
                continue
            created_seen.add(cid)
            created_ids.append(cid)
            created_items.append(created)
            stage2_created_count += 1

            if len(created_ids) >= max(1, int(min_target_candidates)) and not bool(continue_after_target):
                stop_reason = "target_reached"
                break

        passes.append(_pass_row(stage2_created_count=stage2_created_count))
        if bool(rpa_daily_limit_reached):
            break
        if stop_reason == "target_reached":
            break

    else:
        stop_reason = "seed_batch_completed"

    if callable(progress_callback):
        stage1_top_key = ""
        stage1_top_count = 0
        if stage1_skip_counts:
            stage1_top = sorted(stage1_skip_counts.items(), key=lambda kv: kv[1], reverse=True)[0]
            stage1_top_key = str(stage1_top[0])
            stage1_top_count = int(stage1_top[1])
        progress_callback(
            {
                "phase": "pass_completed",
                "message": "seed探索を完了しました",
                "progress_percent": 90.0,
                "pass_index": len(passes),
                "max_passes": len(selected_seeds),
                "created_count": len(created_ids),
                "seed_count": len(selected_seeds),
                "stop_reason": stop_reason,
                "selected_seed_count": len(selected_seeds),
                "pool_available": available_after,
                "stage1_pass_total": int(stage1_pass_total),
                "stage2_runs": int(stage2_runs),
                "stage1_skip_top_reason": stage1_top_key,
                "stage1_skip_top_count": stage1_top_count,
                "stage1_seed_baseline_reject_total": int(stage1_seed_baseline_reject_total),
                "elapsed_sec": round(time.monotonic() - started, 3),
                "flow_stage": "C",
                "flow_stage_label": "C: eBay最終再判定",
                "flow_stage_index": 3,
                "flow_stage_total": 3,
            }
        )

    stage2_low_liquidity_cooldown_saved = 0
    if stage2_low_liquidity_cooldown_rows and _seed_low_liquidity_cooldown_enabled():
        try:
            with connect(settings.db_path) as cooldown_conn:
                init_db(cooldown_conn)
                stage2_low_liquidity_cooldown_saved = _upsert_low_liquidity_cooldowns(
                    cooldown_conn,
                    category_key=category_key,
                    rows=stage2_low_liquidity_cooldown_rows,
                )
        except Exception as err:
            hints.append(f"低流動性cooldown保存でエラー: {type(err).__name__}")
    if stage2_low_liquidity_cooldown_saved > 0:
        hints.append(f"C段階で低流動性seedをcooldown登録: {stage2_low_liquidity_cooldown_saved}件")
    stage1_seed_feedback_updated = 0
    if stage1_seed_feedback_rows:
        try:
            with connect(settings.db_path) as feedback_conn:
                init_db(feedback_conn)
                stage1_seed_feedback_updated = _apply_stage1_seed_feedback(
                    feedback_conn,
                    rows=stage1_seed_feedback_rows,
                )
        except Exception as err:
            hints.append(f"B段階seedフィードバック保存でエラー: {type(err).__name__}")
    if stage1_seed_feedback_updated > 0:
        hints.append(f"B段階 no-hit seed フィードバック更新: {stage1_seed_feedback_updated}件")

    if len(created_ids) <= 0 and stage1_skip_counts:
        top_stage1 = sorted(stage1_skip_counts.items(), key=lambda kv: kv[1], reverse=True)[0]
        hints.append(f"B段階の主な除外理由: {top_stage1[0]} ({top_stage1[1]}件)")
    if len(created_ids) <= 0 and stage1_low_match_reasons:
        top_reason = sorted(stage1_low_match_reasons.items(), key=lambda kv: kv[1], reverse=True)[0]
        hints.append(f"B段階の一致不足主因: {top_reason[0]} ({top_reason[1]}件)")
    if stage_b_rows:
        hints.append(f"B段階でseed B行を生成: {len(stage_b_rows)}件")
    if len(created_ids) <= 0 and stage2_skip_counts:
        top_stage2 = sorted(stage2_skip_counts.items(), key=lambda kv: kv[1], reverse=True)[0]
        hints.append(f"C段階の主な除外理由: {top_stage2[0]} ({top_stage2[1]}件)")
    if len(created_ids) <= 0 and stage2_low_match_reasons:
        top_stage2_reason = sorted(stage2_low_match_reasons.items(), key=lambda kv: kv[1], reverse=True)[0]
        hints.append(f"C段階の詳細理由: {top_stage2_reason[0]} ({top_stage2_reason[1]}件)")
    if stage2_liquidity_refresh_runs:
        miss_retry_runs = [
            row
            for row in stage2_liquidity_refresh_runs
            if isinstance(row, dict) and str(row.get("phase", "") or "") == "on_miss_retry"
        ]
        sample_retry_runs = [
            row
            for row in stage2_liquidity_refresh_runs
            if isinstance(row, dict) and str(row.get("phase", "") or "") == "on_missing_sample_retry"
        ]
        active_retry_runs = [
            row
            for row in stage2_liquidity_refresh_runs
            if isinstance(row, dict) and str(row.get("phase", "") or "") == "on_missing_active_retry"
        ]
        ran_count = sum(
            1
            for row in stage2_liquidity_refresh_runs
            if isinstance((row.get("summary") if isinstance(row, dict) else {}), dict)
            and bool((row.get("summary") if isinstance(row, dict) else {}).get("ran"))
        )
        hints.append(
            f"C段階の流動性RPA更新: 実行{ran_count}回 / miss再試行{len(miss_retry_runs)}回 / sample再試行{len(sample_retry_runs)}回 / active再試行{len(active_retry_runs)}回"
        )
    if stage2_ebay_item_detail_enabled and stage2_ebay_item_detail_fetched > 0:
        hints.append(
            f"C段階のeBay商品ページ補完: {stage2_ebay_item_detail_fetched}件取得"
        )
    if stage1_baseline_softened_seed_keys:
        hints.append(
            f"B段階でseed baseline疑義のためCへ継続: {len(stage1_baseline_softened_seed_keys)}件"
        )
    if len(created_ids) <= 0 and stage1_seed_baseline_reject_total > 0:
        hints.append(f"B段階でseed基準価格を満たさず除外: {stage1_seed_baseline_reject_total}件")
    if stop_reason == "timebox_reached":
        hints.append(
            "探索の時間上限に達したため、"
            f"{len(passes)}件で停止しました（最低保証{min_stage1_attempts_before_timebox}件は実行）。"
        )

    aggregate_counts["created_count"] = len(created_ids)

    payload: Dict[str, Any] = {
        "query": category_key,
        "market_site": market_site,
        "source_sites": list(source_sites),
        "fetched": fetched_aggregate,
        "created_count": len(created_ids),
        "created_ids": created_ids,
        "created": created_items,
        "errors": errors,
        "hints": hints,
        "search_scope_done": bool(search_scope_done),
        "applied_filters": applied_filters,
        "query_cache_skip": False,
        "query_cache_ttl_sec": 0,
        "rpa_daily_limit_reached": bool(rpa_daily_limit_reached),
        "seed_pool": seed_pool_summary,
        "stage_b": {
            "rows_count": int(len(stage_b_rows)),
            "rows": stage_b_rows,
            "api_calls": int(stage1_api_calls),
            "api_max_calls_per_run": int(stage1_api_max_calls),
        },
        "stage1_skip_counts": stage1_skip_counts,
        "stage2_skip_counts": stage2_skip_counts,
        "stage1_low_match_reason_counts": stage1_low_match_reasons,
        "stage2_low_match_reason_counts": stage2_low_match_reasons,
        "stage1_low_match_samples": stage1_low_match_samples,
        "stage1_rejected_examples": stage1_rejected_examples,
        "stage1_no_hit_examples": stage1_no_hit_examples,
        "stage2_low_liquidity_examples": stage2_low_liquidity_examples,
        "stage2_liquidity_unavailable_examples": stage2_liquidity_unavailable_examples,
        "stage2_rejected_examples": stage2_rejected_examples,
        "stage1_seed_baseline_reject_total": int(stage1_seed_baseline_reject_total),
        "stage1_baseline_softened_seed_count": int(len(stage1_baseline_softened_seed_keys)),
        "stage2_liquidity_refresh": {
            "enabled": bool(stage2_liquidity_refresh_enabled),
            "retry_budget_total": int(stage2_liquidity_retry_budget_total),
            "retry_budget_used": int(stage2_liquidity_retry_budget_used),
            "runs": stage2_liquidity_refresh_runs,
        },
        "stage2_ebay_item_detail": {
            "enabled": bool(stage2_ebay_item_detail_enabled),
            "fetched_count": int(stage2_ebay_item_detail_fetched),
            "max_fetch_per_run": int(stage2_ebay_item_detail_max_fetch),
            "timeout_sec": int(stage2_ebay_item_detail_timeout_sec),
        },
        "stage2_low_liquidity_cooldown_saved": int(stage2_low_liquidity_cooldown_saved),
        "timed_fetch": {
            "enabled": bool(timed_mode),
            "min_target_candidates": max(1, int(min_target_candidates)),
            "timebox_sec": max(10, int(timebox_sec)),
            "max_passes": max(1, len(selected_seeds)),
            "continue_after_target": bool(continue_after_target),
            "passes_run": len(passes),
            "stop_reason": str(stop_reason),
            "elapsed_sec": round(time.monotonic() - started, 3),
            "reached_min_target": len(created_ids) >= max(1, int(min_target_candidates)),
            "stage1_pass_total": int(stage1_pass_total),
            "stage2_runs": int(stage2_runs),
            "stage1_skip_counts": stage1_skip_counts,
            "stage2_skip_counts": stage2_skip_counts,
            "stage1_low_match_reason_counts": stage1_low_match_reasons,
            "stage2_low_match_reason_counts": stage2_low_match_reasons,
            "stage1_low_match_samples": stage1_low_match_samples,
            "stage1_rejected_examples": stage1_rejected_examples,
            "stage1_no_hit_examples": stage1_no_hit_examples,
            "stage2_low_liquidity_examples": stage2_low_liquidity_examples,
            "stage2_liquidity_unavailable_examples": stage2_liquidity_unavailable_examples,
            "stage2_rejected_examples": stage2_rejected_examples,
            "stage1_seed_baseline_reject_total": int(stage1_seed_baseline_reject_total),
            "stage1_baseline_softened_seed_count": int(len(stage1_baseline_softened_seed_keys)),
            "stage2_liquidity_refresh": {
                "enabled": bool(stage2_liquidity_refresh_enabled),
                "retry_budget_total": int(stage2_liquidity_retry_budget_total),
                "retry_budget_used": int(stage2_liquidity_retry_budget_used),
                "runs": stage2_liquidity_refresh_runs,
            },
            "stage2_ebay_item_detail": {
                "enabled": bool(stage2_ebay_item_detail_enabled),
                "fetched_count": int(stage2_ebay_item_detail_fetched),
                "max_fetch_per_run": int(stage2_ebay_item_detail_max_fetch),
                "timeout_sec": int(stage2_ebay_item_detail_timeout_sec),
            },
            "stage2_low_liquidity_cooldown_saved": int(stage2_low_liquidity_cooldown_saved),
            "stage_b": {
                "rows_count": int(len(stage_b_rows)),
                "api_calls": int(stage1_api_calls),
                "api_max_calls_per_run": int(stage1_api_max_calls),
            },
            "passes": passes,
        },
    }
    for key, value in aggregate_counts.items():
        payload[key] = value
    _append_seed_run_journal(
        {
            "run_at": utc_iso(),
            "category_key": category_key,
            "category_label": category_label,
            "created_count": len(created_ids),
            "selected_seed_count": len(selected_seeds),
            "available_after_refill": max(0, to_int(seed_pool_summary.get("available_after_refill"), 0)),
            "refill_reason": refill_reason,
            "bootstrap_added_count": bootstrap_added,
            "stop_reason": str(stop_reason),
            "stage_b_rows_count": int(len(stage_b_rows)),
            "stage_b_api_calls": int(stage1_api_calls),
            "stage_b_api_max_calls_per_run": int(stage1_api_max_calls),
            "passes_run": len(passes),
            "rpa_daily_limit_reached": bool(rpa_daily_limit_reached),
            "errors_count": len(errors),
            "stage1_pass_total": int(stage1_pass_total),
            "stage2_runs": int(stage2_runs),
            "stage1_seed_baseline_reject_total": int(stage1_seed_baseline_reject_total),
            "stage2_low_liquidity_cooldown_saved": int(stage2_low_liquidity_cooldown_saved),
            "stage2_ebay_item_detail_fetched": int(stage2_ebay_item_detail_fetched),
        }
    )
    return payload

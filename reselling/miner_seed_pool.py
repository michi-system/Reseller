"""seed-pool orchestration for Miner production fetch flow."""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
import unicodedata
import urllib.parse
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from .coerce import env_bool, env_float, env_int, to_float, to_int
from .config import ROOT_DIR, Settings, load_settings
from .liquidity import get_liquidity_signal
from .live_miner_fetch import (
    MarketItem,
    _build_category_seed_queries,
    _ebay_access_token,
    _extract_codes,
    _is_accessory_title,
    _match_category_row,
    _request_with_retry,
    _search_rakuten,
    _search_yahoo,
)
from .miner import create_miner_candidate
from .models import connect, init_db
from .profit import ProfitInput, calculate_profit
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

_SEED_RUN_JOURNAL_PATH = ROOT_DIR / "data" / "miner_seed_run_journal.jsonl"
_SEED_API_USAGE_PATH = ROOT_DIR / "data" / "miner_seed_api_usage.json"


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


def _resolve_category(category_input: str) -> Tuple[str, str, Dict[str, Any]]:
    row = _match_category_row(str(category_input or ""))
    if isinstance(row, dict):
        key = _normalize_category_key(str(row.get("category_key", "") or ""))
        label = str(row.get("display_name_ja", "") or "").strip() or key
        return key or _normalize_category_key(category_input), label, row
    key = _normalize_category_key(category_input)
    label = _TARGET_LABELS.get(key, key or "カテゴリ")
    return key, label, {}


def _category_big_words(category_key: str, category_row: Dict[str, Any]) -> List[str]:
    """
    Build ordered "big word" used in seed補充A.
    Priority: category knowledge queries -> aliases -> category key fallback.
    """
    candidates: List[str] = []
    if isinstance(category_row, dict) and category_row:
        try:
            knowledge_queries, _meta = _build_category_seed_queries(category_row=category_row, site="ebay")
            candidates.extend([str(v or "").strip() for v in knowledge_queries])
        except Exception:
            pass
        aliases = category_row.get("aliases", [])
        if isinstance(aliases, list):
            for raw in aliases:
                text = str(raw or "").strip()
                if text:
                    candidates.append(text)
    if category_key:
        candidates.append(category_key.replace("_", " "))
    if not candidates:
        candidates.append("watch")

    out: List[str] = []
    seen: Set[str] = set()
    for raw in candidates:
        text = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(raw or ""))).strip()
        if not text:
            continue
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
        if normalized.isdigit():
            continue
        cleaned.append(normalized)
        if len(cleaned) >= 6:
            break
    out: List[str] = []
    if len(cleaned) >= 2:
        out.append(f"{cleaned[0]} {cleaned[1]}")
    if cleaned:
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


def _extract_seed_queries_from_title(title: str, brand_hints: Sequence[str]) -> List[str]:
    compact = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", str(title or ""))).strip()
    if not compact:
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


def _run_rpa_page(*, query: str, offset: int, limit: int) -> Dict[str, Any]:
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
    cmd: List[str] = [
        python_exec,
        str(script_path),
        "--query",
        str(query),
        "--output",
        str(output_path),
        "--pause-for-login",
        str(max(0, env_int("LIQUIDITY_RPA_PAUSE_FOR_LOGIN_SECONDS", 0))),
        "--wait-seconds",
        str(max(3, env_int("LIQUIDITY_RPA_WAIT_SECONDS", 8))),
        "--lookback-days",
        str(max(7, env_int("LIQUIDITY_RPA_LOOKBACK_DAYS", 90))),
        "--condition",
        str((os.getenv("LIQUIDITY_RPA_PRIMARY_CONDITION", "new") or "new").strip() or "new"),
        "--pass-label",
        "seed_pool_refill",
        "--result-offset",
        str(max(0, int(offset))),
        "--result-limit",
        str(max(10, min(200, int(limit)))),
    ]
    if env_bool("LIQUIDITY_RPA_PRIMARY_STRICT_CONDITION", True):
        cmd.append("--strict-condition")
    if env_bool("LIQUIDITY_RPA_PRIMARY_FIXED_PRICE_ONLY", True):
        cmd.append("--fixed-price-only")
    if env_bool("LIQUIDITY_RPA_FORCE_HEADLESS", True):
        cmd.append("--headless")
    profile_dir = str((os.getenv("LIQUIDITY_RPA_PROFILE_DIR", "") or "").strip())
    if profile_dir:
        cmd.extend(["--profile-dir", profile_dir])
    login_url = str((os.getenv("LIQUIDITY_RPA_LOGIN_URL", "") or "").strip())
    if login_url:
        cmd.extend(["--login-url", login_url])
    timeout_sec = max(15, env_int("LIQUIDITY_RPA_FETCH_TIMEOUT_SECONDS", 45))
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
        return_code = int(proc.returncode)
        stdout = str(proc.stdout or "")
        stderr = str(proc.stderr or "")
    except subprocess.TimeoutExpired:
        return_code = -9
        stdout = ""
        stderr = "timeout"
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
    daily_limit = bool(return_code == 75 or "requests allowed in one day" in stdout.lower() or "requests allowed in one day" in stderr.lower())
    return {
        "ok": return_code == 0,
        "daily_limit_reached": daily_limit,
        "reason": "daily_limit_reached" if daily_limit else ("ok" if return_code == 0 else "rpa_failed"),
        "rows": rows,
        "returncode": return_code,
        "stdout_tail": stdout.splitlines()[-8:],
        "stderr_tail": stderr.splitlines()[-8:],
    }


def _collect_row_entries(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    entries = meta.get("filtered_result_rows") if isinstance(meta.get("filtered_result_rows"), list) else []
    sold_90d_count = to_int(row.get("sold_90d_count"), -1)
    sold_price_min = to_float(row.get("sold_price_min"), -1.0)
    out: List[Dict[str, Any]] = []
    for idx, raw in enumerate(entries, start=1):
        if not isinstance(raw, dict):
            continue
        title = str(raw.get("title", "") or "").strip()
        if not title:
            continue
        out.append(
            {
                "title": title,
                "item_url": str(raw.get("item_url", "") or "").strip(),
                "image_url": str(raw.get("image_url", "") or "").strip(),
                "rank": max(1, to_int(raw.get("rank"), idx)),
                "sold_90d_count": sold_90d_count,
                "sold_price_min_90d": sold_price_min,
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
            "item_url": str(sold_sample.get("item_url", "") or "").strip(),
            "image_url": str(sold_sample.get("image_url", "") or "").strip(),
            "rank": 1,
            "sold_90d_count": sold_90d_count,
            "sold_price_min_90d": sold_price_min,
        }
    ]


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
        normalized_key = _seed_key(normalized_query)
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
) -> int:
    now_ts = int(time.time())
    created_at = utc_iso(now_ts)
    expires_at = utc_iso(now_ts + max(1, int(ttl_days)) * 86400)
    inserted = 0
    for row in rows:
        seed_query = _normalize_seed_query(row.get("seed_query", ""))
        seed_key = _seed_key(seed_query)
        if not seed_query or len(seed_key) < 4:
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


def _take_seeds_for_run(
    conn: Any,
    *,
    category_key: str,
    take_count: int,
    now_ts: int,
) -> Tuple[List[Dict[str, Any]], int]:
    _normalize_existing_seed_rows(conn, category_key=category_key)
    rows = conn.execute(
        """
        SELECT id, seed_query, seed_key, source_rank, source_title, created_at, expires_at, last_used_at, use_count, metadata_json
        FROM miner_seed_pool
        WHERE category_key = ?
        """,
        (category_key,),
    ).fetchall()
    def _sort_key(row: Dict[str, Any]) -> Tuple[int, int, int, int]:
        created_ts = iso_to_epoch(str(row.get("created_at", "") or ""))
        source_rank = max(0, to_int(row.get("source_rank"), 0))
        sid = max(0, to_int(row.get("id"), 0))
        # 実行は「古い順20件」を優先する。
        return (created_ts if created_ts > 0 else (10**12), source_rank, sid, max(0, to_int(row.get("use_count"), 0)))

    ordered_rows = sorted([dict(v) for v in rows], key=_sort_key)
    out: List[Dict[str, Any]] = []
    now_iso = utc_iso(now_ts)
    for row in ordered_rows:
        if len(out) >= max(1, int(take_count)):
            break
        expires_at = iso_to_epoch(str(row["expires_at"] or ""))
        if expires_at <= now_ts:
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
                "seed_key": str(row["seed_key"] or "").strip(),
                "source_rank": to_int(row["source_rank"], 0),
                "source_title": str(row["source_title"] or "").strip(),
                "seed_quality_score": to_int(metadata.get("seed_quality_score"), 0),
                "seed_collected_sold_price_min_usd": to_float(metadata.get("seed_collected_sold_price_min_usd"), -1.0),
                "seed_collected_sold_90d_count": to_int(metadata.get("seed_collected_sold_90d_count"), -1),
            }
        )
    return out, 0


def _preview_seeds_for_run(
    conn: Any,
    *,
    category_key: str,
    take_count: int,
    now_ts: int,
) -> Tuple[int, int]:
    _normalize_existing_seed_rows(conn, category_key=category_key)
    rows = conn.execute(
        """
        SELECT id, seed_query, seed_key, source_rank, created_at, expires_at, metadata_json, use_count
        FROM miner_seed_pool
        WHERE category_key = ?
        """,
        (category_key,),
    ).fetchall()

    def _sort_key(row: Dict[str, Any]) -> Tuple[int, int, int, int]:
        created_ts = iso_to_epoch(str(row.get("created_at", "") or ""))
        source_rank = max(0, to_int(row.get("source_rank"), 0))
        sid = max(0, to_int(row.get("id"), 0))
        return (created_ts if created_ts > 0 else (10**12), source_rank, sid, max(0, to_int(row.get("use_count"), 0)))

    ordered_rows = sorted([dict(v) for v in rows], key=_sort_key)
    selected_count = 0
    for row in ordered_rows:
        if selected_count >= max(1, int(take_count)):
            break
        expires_at = iso_to_epoch(str(row.get("expires_at", "") or ""))
        if expires_at <= now_ts:
            continue
        selected_count += 1
    return int(selected_count), 0


def get_seed_pool_status(
    *,
    category_query: str,
    settings: Optional[Settings] = None,
) -> Dict[str, Any]:
    settings = settings or load_settings()
    category_key, category_label, category_row = _resolve_category(category_query)
    if not category_key:
        raise ValueError("category query is required")

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
        selected_count, skipped_low_quality_count = _preview_seeds_for_run(
            conn,
            category_key=category_key,
            take_count=run_batch_size,
            now_ts=now_ts,
        )
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
        "skipped_low_quality_count": int(skipped_low_quality_count),
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
) -> Dict[str, Any]:
    """Reset refill cooldown/page-window state for a category.

    This is intended for manual recovery when a category entered cooldown
    (for example rank-limit wait) and the operator wants to restart scanning
    from page 0 immediately.
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

        cleared_seed_rows = 0
        if bool(clear_pool):
            seed_row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM miner_seed_pool
                WHERE category_key = ?
                """,
                (category_key,),
            ).fetchone()
            cleared_seed_rows = max(0, to_int(seed_row["c"] if seed_row else 0, 0))
            conn.execute(
                """
                DELETE FROM miner_seed_pool
                WHERE category_key = ?
                """,
                (category_key,),
            )

        conn.commit()
        available_after = _count_available(conn, category_key=category_key, now_ts=now_ts)

    return {
        "category_key": category_key,
        "category_label": category_label,
        "reset_at": utc_iso(now_ts),
        "had_refill_state": bool(had_refill_state),
        "cleared_page_windows": int(cleared_page_windows),
        "cleared_seed_rows": int(cleared_seed_rows),
        "cleaned_expired_count": int(cleaned_expired),
        "available_before": int(available_before),
        "available_after": int(available_after),
        "clear_pool": bool(clear_pool),
    }


def _refill_seed_pool(
    conn: Any,
    *,
    category_key: str,
    category_label: str,
    category_row: Dict[str, Any],
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    now_ts = int(time.time())
    refill_started = time.monotonic()
    refill_trigger_available_le = max(0, env_int("MINER_SEED_POOL_REFILL_THRESHOLD", 0))
    run_batch_size = max(1, env_int("MINER_SEED_POOL_RUN_BATCH_SIZE", 20))
    refill_timebox_sec = max(30, env_int("MINER_SEED_POOL_REFILL_TIMEBOX_SEC", 300))
    max_timeout_pages_per_run = max(1, env_int("MINER_SEED_POOL_MAX_TIMEOUT_PAGES_PER_RUN", 2))
    _normalize_existing_seed_rows(conn, category_key=category_key)
    available = _count_available(conn, category_key=category_key, now_ts=now_ts)
    summary: Dict[str, Any] = {
        "ran": False,
        "available_before": available,
        "available_after": available,
        "added_count": 0,
        "bootstrap_added_count": 0,
        "skipped_fresh_pages": 0,
        "page_runs": [],
        "reason": "threshold_not_reached",
        "daily_limit_reached": False,
        "cooldown_until": "",
        "query": "",
    }
    if available > refill_trigger_available_le:
        return summary

    existing_keys = _load_active_seed_keys(conn, category_key=category_key, now_ts=now_ts)
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
        bootstrap_added = _insert_seed_rows(conn, category_key=category_key, rows=bootstrap_rows, ttl_days=ttl_days)
        summary["bootstrap_added_count"] = max(0, int(bootstrap_added))
        if summary["bootstrap_added_count"] > 0:
            summary["ran"] = True
            summary["reason"] = "bootstrap_refilled"

        available = _count_available(conn, category_key=category_key, now_ts=now_ts)
        summary["available_after"] = available
        if available > refill_trigger_available_le:
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
    # 新規seed目標は100件（達成後も現在ページは完走する）
    target_count = max(20, env_int("MINER_SEED_POOL_TARGET_COUNT", 100))
    cooldown_days = max(1, env_int("MINER_SEED_POOL_COOLDOWN_DAYS", 7))
    freshness_days = max(1, env_int("MINER_SEED_POOL_PAGE_FRESH_DAYS", 7))
    freshness_sec = freshness_days * 86400
    brand_hints = _brand_hints(category_row)
    big_words = _category_big_words(category_key, category_row)
    summary["query"] = big_words[0] if big_words else category_key
    summary["queries"] = list(big_words)

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
    rpa_timeout_pages = 0

    total_page_budget = max(1, len(big_words) * max_pages)
    for query_index, query in enumerate(big_words, start=1):
        if added_total >= target_count:
            summary["reason"] = "target_reached"
            break
        query_key = _query_window_key(query)
        page_window_entries = _load_page_window_entries(conn, category_key=category_key, query_key=query_key)
        word_added_before = added_total
        word_fetched_pages = 0
        word_skipped_pages = 0
        word_last_rank = 0
        word_stop_reason = "query_scanned"

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
                continue

            result = _run_rpa_page(query=query, offset=offset, limit=page_size)
            fetched_pages += 1
            word_fetched_pages += 1
            rows = result.get("rows", []) if isinstance(result.get("rows"), list) else []
            row = rows[0] if rows and isinstance(rows[0], dict) else {}
            entries = _collect_row_entries(row) if row else []
            new_rows: List[Dict[str, Any]] = []
            for idx, entry in enumerate(entries, start=1):
                title = str(entry.get("title", "") or "").strip()
                if not title:
                    continue
                if _is_accessory_title(title):
                    accessory_filtered_count += 1
                    continue
                title_candidates = _extract_seed_queries_from_title(title, brand_hints)
                seed_candidates = list(title_candidates)
                extraction_mode = "title"
                api_backfill_reason = ""
                needs_api_backfill = (not seed_candidates) or (not any(_looks_specific_seed(v) for v in seed_candidates))
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
                                seen_keys.add(skey)
                                merged.append(text)
                            seed_candidates = merged
                    else:
                        api_backfill_reason = "missing_item_url"
                if not seed_candidates:
                    continue
                rank = max(1, to_int(entry.get("rank"), idx))
                global_rank = offset + rank
                word_last_rank = max(word_last_rank, global_rank)
                last_rank = max(last_rank, global_rank)
                for raw_seed_query in seed_candidates:
                    seed_query = _normalize_seed_query(raw_seed_query)
                    if not seed_query:
                        continue
                    skey = _seed_key(seed_query)
                    if skey in existing_keys:
                        continue
                    existing_keys.add(skey)
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
                                "seed_collected_sold_price_min_usd": to_float(entry.get("sold_price_min_90d"), -1.0),
                                "seed_quality_score": 0,
                                "seed_extraction_mode": extraction_mode,
                                "seed_api_backfill_reason": api_backfill_reason,
                            },
                        }
                    )

            inserted = _insert_seed_rows(conn, category_key=category_key, rows=new_rows, ttl_days=ttl_days)
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
            try:
                conn.commit()
            except Exception:
                pass
            if bool(result.get("daily_limit_reached")):
                summary["daily_limit_reached"] = True
                summary["reason"] = "daily_limit_reached"
                word_stop_reason = "daily_limit_reached"
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

        query_runs.append(
            {
                "query": query,
                "added_count": max(0, added_total - word_added_before),
                "fetched_pages": word_fetched_pages,
                "skipped_fresh_pages": word_skipped_pages,
                "last_rank_checked": word_last_rank,
                "stop_reason": word_stop_reason,
            }
        )
        if bool(summary.get("daily_limit_reached")):
            break
        if str(summary.get("reason", "") or "") in {"refill_timebox_reached", "rpa_timeout_guard"}:
            break

    available_after = _count_available(conn, category_key=category_key, now_ts=now_ts)
    summary["added_count"] = added_total
    summary["skipped_fresh_pages"] = skipped_fresh_pages
    summary["available_after"] = available_after
    summary["last_rank_checked"] = last_rank
    summary["query_runs"] = query_runs
    summary["accessory_filtered_count"] = int(accessory_filtered_count)
    summary["rpa_timeout_pages"] = int(rpa_timeout_pages)
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


def _seed_title_match_score(
    *,
    seed_query: str,
    seed_source_title: str,
    candidate_title: str,
) -> Tuple[float, str]:
    if _is_accessory_title(candidate_title):
        return 0.0, "accessory_title"

    seed_codes = _model_code_set(seed_query) | _model_code_set(seed_source_title)
    candidate_codes = _model_code_set(candidate_title)
    if seed_codes:
        if not candidate_codes:
            return 0.0, "candidate_model_missing"
        shared = seed_codes.intersection(candidate_codes)
        if not shared:
            return 0.0, "model_code_mismatch"
        overlap = len(shared) / max(1, len(seed_codes))
        score = min(0.98, 0.82 + (0.12 * overlap))
        return float(score), "model_code_match"

    seed_tokens = set(_query_tokens(seed_query))
    candidate_tokens = set(_query_tokens(candidate_title))
    if not seed_tokens or not candidate_tokens:
        return 0.0, "token_missing"
    common = seed_tokens.intersection(candidate_tokens)
    if not common:
        return 0.0, "token_overlap_zero"
    jaccard = len(common) / max(1, len(seed_tokens.union(candidate_tokens)))
    score = min(0.86, 0.48 + (0.46 * jaccard))
    return float(score), "token_overlap"


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


def _liquidity_sold_sample(signal: Dict[str, Any]) -> Dict[str, Any]:
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    sample = metadata.get("sold_sample") if isinstance(metadata.get("sold_sample"), dict) else {}
    if not isinstance(sample, dict):
        return {}
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
    return out


def _liquidity_sold_min_usd(signal: Dict[str, Any]) -> float:
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    sold_min = to_float(metadata.get("sold_price_min"), -1.0)
    return sold_min if sold_min > 0 else -1.0


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
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    settings: Optional[Settings] = None,
) -> Dict[str, Any]:
    settings = settings or load_settings()
    category_key, category_label, category_row = _resolve_category(category_query)
    if not category_key:
        raise ValueError("category query is required")
    started = time.monotonic()
    now_ts = int(time.time())
    run_batch_size = max(1, env_int("MINER_SEED_POOL_RUN_BATCH_SIZE", 20))
    aggregate_counts = {key: 0 for key in _COUNT_KEYS}
    stage1_skip_counts: Dict[str, int] = {}
    stage2_skip_counts: Dict[str, int] = {}
    stage1_low_match_reasons: Dict[str, int] = {}
    stage2_low_match_reasons: Dict[str, int] = {}
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

    def _inc_skip(dst: Dict[str, int], key: str, amount: int = 1) -> None:
        label = str(key or "").strip()
        if not label or amount <= 0:
            return
        dst[label] = int(dst.get(label, 0)) + int(amount)
        if label in aggregate_counts:
            aggregate_counts[label] = int(aggregate_counts.get(label, 0)) + int(amount)

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
        refill_summary = _refill_seed_pool(
            conn,
            category_key=category_key,
            category_label=category_label,
            category_row=category_row,
            progress_callback=progress_callback,
        )
        available_after = _count_available(conn, category_key=category_key, now_ts=int(time.time()))
        # 1探索は「古い順20件」を上限に実行する。
        run_batch_size = max(1, min(int(available_after), env_int("MINER_SEED_POOL_RUN_BATCH_SIZE", 20)))
        selected_seeds, skipped_low_quality_count = _take_seeds_for_run(
            conn,
            category_key=category_key,
            take_count=run_batch_size,
            now_ts=int(time.time()),
        )
        seed_pool_summary = {
            "category_key": category_key,
            "category_label": category_label,
            "seed_count": len(selected_seeds),
            # backward-compat keys (deprecated)
            "selected_seed_count": len(selected_seeds),
            "available_after_refill": available_after,
            "skipped_low_quality_count": int(skipped_low_quality_count),
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
    if bool(seed_pool_summary["refill"].get("daily_limit_reached")):
        rpa_daily_limit_reached = True
        hints.append("Product Researchの日次上限に到達したため、seed補充を停止しました。")
    skipped_low_quality_count = max(0, to_int(seed_pool_summary.get("skipped_low_quality_count"), 0))
    if skipped_low_quality_count > 0:
        hints.append(f"低品質seedを {skipped_low_quality_count} 件スキップしました。")
    accessory_filtered_count = max(0, to_int(seed_pool_summary["refill"].get("accessory_filtered_count"), 0))
    if accessory_filtered_count > 0:
        hints.append(f"付属品タイトルを {accessory_filtered_count} 件除外しました。")
    api_backfill = seed_pool_summary["refill"].get("seed_api_backfill", {})
    if isinstance(api_backfill, dict):
        api_hits = max(0, to_int(api_backfill.get("hits"), 0))
        api_attempts = max(0, to_int(api_backfill.get("attempts"), 0))
        api_budget_skips = max(0, to_int(api_backfill.get("budget_skips"), 0))
        if api_attempts > 0:
            hints.append(f"seed API補完: 試行 {api_attempts} 件 / 補完成功 {api_hits} 件")
        if api_budget_skips > 0:
            hints.append(f"seed API補完は予算上限で {api_budget_skips} 件スキップしました。")
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
                "skipped_low_quality_count": skipped_low_quality_count,
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

    stage1_limit_per_site = max(5, min(100, env_int("MINER_STAGE1_LIMIT_PER_SITE", max(20, int(limit_per_site)))))
    stage1_take_per_seed = max(
        1,
        min(
            max(1, int(max_candidates)),
            5,
            env_int("MINER_STAGE1_TOP_MATCHES_PER_SEED", 2),
        ),
    )
    liquidity_min_sold_90d = max(0, env_int("LIQUIDITY_MIN_SOLD_90D", 3))
    fx_seed_rate = max(1.0, to_float(getattr(settings, "fx_usd_jpy_default", 150.0), 150.0))
    brand_hints = _brand_hints(category_row)
    marketplace_fee_rate = max(0.0, env_float("MARKETPLACE_FEE_RATE", 0.13))
    payment_fee_rate = max(0.0, env_float("PAYMENT_FEE_RATE", 0.03))
    international_shipping_usd = max(0.0, env_float("EST_INTL_SHIPPING_USD", 18.0))
    customs_usd = max(0.0, env_float("EST_CUSTOMS_USD", 0.0))
    packaging_usd = max(0.0, env_float("EST_PACKAGING_USD", 0.0))
    fixed_fee_usd = max(0.0, env_float("FIXED_FEE_USD", 0.0))
    runtime_pair_signatures: Set[str] = set()

    applied_filters = {
        "seed_pool_mode": "A/B/C_spec",
        "stage_a_refill_trigger_available_le": max(0, env_int("MINER_SEED_POOL_REFILL_THRESHOLD", 0)),
        "stage_b_limit_per_site": int(stage1_limit_per_site),
        "stage_b_pick_per_seed": int(stage1_take_per_seed),
        "stage_b_condition": "new",
        "stage_b_sort": "price_asc",
        "stage_b_price_cap": "seed_collected_sold_price_min_usd",
        "stage_c_price_basis": "sold_price_min_90d",
        "stage_c_min_sold_90d": int(liquidity_min_sold_90d),
    }

    stage1_pass_total = 0
    stage2_runs = 0
    stage1_seed_baseline_reject_total = 0
    min_stage1_attempts_before_timebox = min(
        len(selected_seeds),
        max(1, env_int("MINER_TIMED_FETCH_MIN_STAGE1_ATTEMPTS", 20)),
    )
    for idx, seed in enumerate(selected_seeds, start=1):
        elapsed = time.monotonic() - started
        seed_query = str(seed.get("seed_query", "") or "")
        seed_source_title = str(seed.get("source_title", "") or "")
        baseline_usd = to_float(seed.get("seed_collected_sold_price_min_usd"), -1.0)
        seed_max_price_jpy = int(max(0.0, baseline_usd * fx_seed_rate)) if baseline_usd > 0 else 0
        if bool(timed_mode) and elapsed >= max(10, int(timebox_sec)) and idx > min_stage1_attempts_before_timebox:
            stop_reason = "timebox_reached"
            search_scope_done = False
            break
        if callable(progress_callback):
            progress_callback(
                {
                    "phase": "stage1_running",
                    "message": f"一次判定 {idx}/{len(selected_seeds)}: {seed.get('seed_query', '')}",
                    "progress_percent": min(88.0, 12.0 + (72.0 * (idx - 1) / max(1, len(selected_seeds)))),
                "pass_index": idx,
                "max_passes": len(selected_seeds),
                "created_count": len(created_ids),
                "seed_count": len(selected_seeds),
                "selected_seed_count": len(selected_seeds),
                "pool_available": available_after,
                    "current_seed_query": seed_query,
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
        stage_context_env: Dict[str, str] = {
            "MINER_ACTIVE_CATEGORY_KEY": category_key,
            "MINER_ACTIVE_CATEGORY_LABEL": category_label,
        }
        if seed_max_price_jpy > 0:
            stage_context_env["MINER_ACTIVE_SEED_MAX_PRICE_JPY"] = str(seed_max_price_jpy)

        stage1_candidates: List[Dict[str, Any]] = []
        stage1_seen: Set[str] = set()
        stage1_site_logs: List[Dict[str, Any]] = []
        stage1_baseline_reject = 0
        for site_key, fetcher in source_fetchers:
            with _temporary_env(stage_context_env):
                try:
                    items, fetch_info = fetcher(seed_query, stage1_limit_per_site, timeout, 1, require_in_stock)
                except Exception as err:
                    _inc_skip(stage1_skip_counts, "skipped_fetch_error", 1)
                    errors.append({"seed_query": seed_query, "site": site_key, "message": str(err)})
                    stage1_site_logs.append(
                        {
                            "site": site_key,
                            "query": seed_query,
                            "ok": False,
                            "error": str(err),
                            "raw_count": 0,
                            "selected_count": 0,
                        }
                    )
                    continue

            slot = fetched_aggregate.setdefault(site_key, {"calls_made": 0, "network_calls": 0, "cache_hits": 0, "count": 0})
            slot["calls_made"] = int(slot.get("calls_made", 0)) + 1
            slot["network_calls"] = int(slot.get("network_calls", 0)) + 1
            slot["count"] = int(slot.get("count", 0)) + len(items)
            if bool((fetch_info or {}).get("cache_hit")):
                slot["cache_hits"] = int(slot.get("cache_hits", 0)) + 1

            selected_for_site = 0
            for item in items:
                if not isinstance(item, MarketItem):
                    continue
                source_total_jpy = max(0.0, to_float(item.price, 0.0)) + max(0.0, to_float(item.shipping, 0.0))
                if source_total_jpy <= 0:
                    _inc_skip(stage1_skip_counts, "skipped_invalid_price", 1)
                    continue
                score, reason = _seed_title_match_score(
                    seed_query=seed_query,
                    seed_source_title=seed_source_title,
                    candidate_title=str(item.title or ""),
                )
                if reason == "accessory_title":
                    _inc_skip(stage1_skip_counts, "skipped_accessory_title", 1)
                    continue
                if score < effective_min_match_score:
                    _inc_skip(stage1_skip_counts, "skipped_low_match", 1)
                    stage1_low_match_reasons[reason] = int(stage1_low_match_reasons.get(reason, 0)) + 1
                    continue
                if baseline_usd > 0:
                    source_total_usd = source_total_jpy / fx_seed_rate if fx_seed_rate > 0 else -1.0
                    if source_total_usd <= 0 or source_total_usd >= baseline_usd:
                        stage1_baseline_reject += 1
                        continue

                item_key = "|".join(
                    [
                        str(item.site or ""),
                        str(item.item_id or ""),
                        str(item.item_url or ""),
                        _seed_key(str(item.title or "")),
                    ]
                )
                if item_key in stage1_seen:
                    _inc_skip(stage1_skip_counts, "skipped_duplicates", 1)
                    continue
                stage1_seen.add(item_key)
                stage1_candidates.append(
                    {
                        "item": item,
                        "site": site_key,
                        "score": float(score),
                        "reason": str(reason),
                        "source_total_jpy": float(source_total_jpy),
                    }
                )
                selected_for_site += 1

            stage1_site_logs.append(
                {
                    "site": site_key,
                    "query": seed_query,
                    "ok": True,
                    "raw_count": len(items),
                    "selected_count": selected_for_site,
                    "category_filter": (fetch_info or {}).get("category_filter") if isinstance(fetch_info, dict) else {},
                }
            )

        stage1_candidates.sort(
            key=lambda row: (
                to_float(row.get("source_total_jpy"), 10**12),
                -to_float(row.get("score"), 0.0),
                str(row.get("item").title if isinstance(row.get("item"), MarketItem) else ""),
            )
        )
        selected_stage1 = stage1_candidates[:stage1_take_per_seed]
        stage1_count = len(selected_stage1)
        stage1_seed_baseline_reject_total += int(stage1_baseline_reject)
        stage1_pass_total += stage1_count
        if stage1_count <= 0:
            passes.append(
                {
                    "pass": idx,
                    "seed_query": seed_query,
                    "stage1_candidate_count": 0,
                    "stage2_created_count": 0,
                    "stage1_seed_baseline_reject_count": int(stage1_baseline_reject),
                    "elapsed_sec": round(time.monotonic() - started, 3),
                    "min_match_score": effective_min_match_score,
                    "has_model_code": has_model_code,
                    "stage1_site_logs": stage1_site_logs,
                }
            )
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
            stage2_runs += 1
            item = row.get("item")
            if not isinstance(item, MarketItem):
                _inc_skip(stage2_skip_counts, "skipped_invalid_price", 1)
                continue
            jp_seed_query = _pick_jp_seed_query(
                seed_query=seed_query,
                source_title=str(item.title or ""),
                brand_hints=brand_hints,
            )
            signal = get_liquidity_signal(
                query=jp_seed_query,
                source_title=str(item.title or ""),
                market_title=jp_seed_query,
                source_identifiers=item.identifiers if isinstance(item.identifiers, dict) else {},
                market_identifiers={},
                active_count_hint=-1,
                timeout=max(5, int(timeout)),
                settings=settings,
            )
            if _is_rpa_daily_limit_signal(signal):
                rpa_daily_limit_reached = True
                stop_reason = "rpa_daily_limit_reached"
                break
            sold_count_raw = to_int(signal.get("sold_90d_count"), -1)
            if sold_count_raw < 0:
                _inc_skip(stage2_skip_counts, "skipped_liquidity_unavailable", 1)
                continue
            sold_count_90d = max(0, sold_count_raw)
            if sold_count_90d < liquidity_min_sold_90d:
                _inc_skip(stage2_skip_counts, "skipped_low_liquidity", 1)
                continue
            sold_min_usd = _liquidity_sold_min_usd(signal)
            if sold_min_usd <= 0:
                _inc_skip(stage2_skip_counts, "skipped_missing_sold_min", 1)
                continue
            sold_sample = _liquidity_sold_sample(signal)
            sold_item_url = str(sold_sample.get("item_url", "") or "").strip()
            sold_sample_price = to_float(sold_sample.get("sold_price_usd"), -1.0)
            if not sold_item_url or sold_sample_price <= 0:
                _inc_skip(stage2_skip_counts, "skipped_missing_sold_sample", 1)
                continue

            source_price_jpy = max(0.0, to_float(item.price, 0.0))
            source_shipping_jpy = max(0.0, to_float(item.shipping, 0.0))
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
                continue
            if margin_rate < float(min_margin_rate):
                _inc_skip(stage2_skip_counts, "skipped_low_margin", 1)
                continue

            fx_rate = to_float(fx.get("rate"), 0.0)
            source_total_usd = (source_total_jpy / fx_rate) if fx_rate > 0 else -1.0
            if source_total_usd <= 0 or source_total_usd >= sold_min_usd:
                _inc_skip(stage2_skip_counts, "skipped_below_sold_min", 1)
                continue

            pair_signature = "|".join(
                [
                    str(item.site or ""),
                    str(item.item_id or ""),
                    str(item.item_url or ""),
                    sold_item_url,
                ]
            )
            if pair_signature in runtime_pair_signatures:
                _inc_skip(stage2_skip_counts, "skipped_duplicates", 1)
                continue
            runtime_pair_signatures.add(pair_signature)

            score = to_float(row.get("score"), 0.0)
            market_title = str(sold_sample.get("title", "") or "").strip() or jp_seed_query
            market_item_id = _ebay_item_id_from_url(sold_item_url)
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
                    "market_item_url": sold_item_url,
                    "market_image_url": str(sold_sample.get("image_url", "") or ""),
                    "market_price_basis_usd": float(sold_min_usd),
                    "market_shipping_basis_usd": 0.0,
                    "market_revenue_basis_usd": float(sold_min_usd),
                    "market_price_basis_type": "sold_price_min_90d",
                    "ebay_sold_item_url": sold_item_url,
                    "ebay_sold_image_url": str(sold_sample.get("image_url", "") or ""),
                    "ebay_sold_title": market_title,
                    "ebay_sold_price_usd": float(sold_sample_price),
                    "ebay_sold_sample_reference_ok": True,
                    "liquidity_query": jp_seed_query,
                    "liquidity": signal,
                    "seed_pool": {
                        "id": to_int(seed.get("id"), 0),
                        "seed_query": seed_query,
                        "seed_source_title": seed_source_title,
                        "seed_collected_sold_price_min_usd": baseline_usd,
                    },
                    "seed_jp": {
                        "query": jp_seed_query,
                        "stage2_index": int(stage2_index),
                        "stage1_site": str(row.get("site", "") or ""),
                        "stage1_match_reason": str(row.get("reason", "") or ""),
                        "stage1_match_score": float(round(score, 4)),
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

        passes.append(
            {
                "pass": idx,
                "seed_query": seed_query,
                "stage1_candidate_count": stage1_count,
                "stage2_created_count": stage2_created_count,
                "stage1_seed_baseline_reject_count": int(stage1_baseline_reject),
                "elapsed_sec": round(time.monotonic() - started, 3),
                "min_match_score": effective_min_match_score,
                "has_model_code": has_model_code,
                "stage1_site_logs": stage1_site_logs,
            }
        )
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

    if len(created_ids) <= 0 and stage1_skip_counts:
        top_stage1 = sorted(stage1_skip_counts.items(), key=lambda kv: kv[1], reverse=True)[0]
        hints.append(f"B段階の主な除外理由: {top_stage1[0]} ({top_stage1[1]}件)")
    if len(created_ids) <= 0 and stage1_low_match_reasons:
        top_reason = sorted(stage1_low_match_reasons.items(), key=lambda kv: kv[1], reverse=True)[0]
        hints.append(f"B段階の一致不足主因: {top_reason[0]} ({top_reason[1]}件)")
    if len(created_ids) <= 0 and stage2_skip_counts:
        top_stage2 = sorted(stage2_skip_counts.items(), key=lambda kv: kv[1], reverse=True)[0]
        hints.append(f"C段階の主な除外理由: {top_stage2[0]} ({top_stage2[1]}件)")
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
        "stage1_skip_counts": stage1_skip_counts,
        "stage2_skip_counts": stage2_skip_counts,
        "stage1_low_match_reason_counts": stage1_low_match_reasons,
        "stage2_low_match_reason_counts": stage2_low_match_reasons,
        "stage1_seed_baseline_reject_total": int(stage1_seed_baseline_reject_total),
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
            "stage1_seed_baseline_reject_total": int(stage1_seed_baseline_reject_total),
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
            "passes_run": len(passes),
            "rpa_daily_limit_reached": bool(rpa_daily_limit_reached),
            "errors_count": len(errors),
            "stage1_pass_total": int(stage1_pass_total),
            "stage2_runs": int(stage2_runs),
            "stage1_seed_baseline_reject_total": int(stage1_seed_baseline_reject_total),
        }
    )
    return payload

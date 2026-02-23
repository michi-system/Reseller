"""Seed-pool orchestration for Miner production fetch flow."""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
import unicodedata
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from .coerce import env_bool, env_float, env_int, to_float, to_int
from .config import ROOT_DIR, Settings, load_settings
from .live_miner_fetch import (
    _build_category_seed_queries,
    _extract_codes,
    _match_category_row,
    fetch_live_miner_candidates,
)
from .models import connect, init_db
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


def _category_search_query(category_key: str, category_row: Dict[str, Any]) -> str:
    aliases = category_row.get("aliases", []) if isinstance(category_row, dict) else []
    if isinstance(aliases, list):
        for raw in aliases:
            text = str(raw or "").strip()
            if not text:
                continue
            if re.search(r"[A-Za-z]", text):
                return text
    if category_key:
        return category_key.replace("_", " ")
    return "watch"


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
        text = re.sub(r"\s+", " ", str(raw or "").strip())
        key = _seed_key(text)
        if len(key) < 4:
            continue
        if key in seen:
            continue
        seen.add(key)
        dedup.append(text)
    return dedup

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
        if not text:
            continue
        skey = _seed_key(text)
        if len(skey) < 4:
            continue
        if skey in seen or skey in existing_keys:
            continue
        seen.add(skey)
        existing_keys.add(skey)
        out.append(
            {
                "seed_query": text,
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


def _merge_fetched(aggregate: Dict[str, Dict[str, Any]], fetched: Any) -> None:
    if not isinstance(fetched, dict):
        return
    for site, info in fetched.items():
        if not isinstance(info, dict):
            continue
        slot = aggregate.setdefault(str(site), {})
        for key in ("calls_made", "network_calls", "cache_hits", "count"):
            slot[key] = int(slot.get(key, 0) or 0) + max(0, to_int(info.get(key), 0))
        budget_remaining = to_int(info.get("budget_remaining"), -1)
        if budget_remaining >= 0:
            slot["budget_remaining"] = budget_remaining
        stop_reason = str(info.get("stop_reason", "") or "").strip()
        if stop_reason:
            slot["stop_reason"] = stop_reason
        if isinstance(info.get("knowledge"), dict) and not isinstance(slot.get("knowledge"), dict):
            slot["knowledge"] = info.get("knowledge")
        if isinstance(info.get("model_backfill"), dict) and not isinstance(slot.get("model_backfill"), dict):
            slot["model_backfill"] = info.get("model_backfill")


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
    timeout_sec = max(20, env_int("LIQUIDITY_RPA_FETCH_TIMEOUT_SECONDS", 95))
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
        seed_query = re.sub(r"\s+", " ", str(row.get("seed_query", "") or "").strip())
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


def _refill_seed_pool(
    conn: Any,
    *,
    category_key: str,
    category_label: str,
    category_row: Dict[str, Any],
) -> Dict[str, Any]:
    now_ts = int(time.time())
    threshold = max(1, env_int("MINER_SEED_POOL_REFILL_THRESHOLD", 60))
    run_batch_size = max(1, env_int("MINER_SEED_POOL_RUN_BATCH_SIZE", 20))
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
    if available >= threshold:
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
        if available >= max(run_batch_size, threshold):
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
    max_pages = max(1, min(30, env_int("MINER_SEED_POOL_MAX_PAGES", 30)))
    target_count = max(20, env_int("MINER_SEED_POOL_TARGET_COUNT", 100))
    soft_ratio = max(0.5, min(1.0, env_float("MINER_SEED_POOL_SOFT_TARGET_RATIO", 0.8)))
    soft_target = max(1, int(target_count * soft_ratio))
    cooldown_days = max(1, env_int("MINER_SEED_POOL_COOLDOWN_DAYS", 7))
    empty_result_cooldown_hours = max(1, env_int("MINER_SEED_POOL_EMPTY_COOLDOWN_HOURS", 12))
    freshness_days = max(1, env_int("MINER_SEED_POOL_PAGE_FRESH_DAYS", 7))
    freshness_sec = freshness_days * 86400
    brand_hints = _brand_hints(category_row)
    search_query = _category_search_query(category_key, category_row)
    query_key = _query_window_key(search_query)
    page_window_entries = _load_page_window_entries(conn, category_key=category_key, query_key=query_key)
    zero_only_history = bool(page_window_entries) and all(
        to_int(meta.get("result_count"), 0) <= 0 and to_int(meta.get("new_seed_count"), 0) <= 0
        for meta in page_window_entries.values()
        if isinstance(meta, dict)
    )
    force_top_recheck = bool(zero_only_history)
    forced_top_recheck_ran = False
    summary["history_all_zero"] = bool(zero_only_history)
    summary["query"] = search_query

    added_total = 0
    last_rank = 0
    fetched_pages = 0
    skipped_fresh_pages = 0
    rank_ceiling = max_pages * page_size
    for page_index in range(max_pages):
        offset = page_index * page_size
        window = page_window_entries.get(offset, {}) if isinstance(page_window_entries.get(offset), dict) else {}
        fetched_at = to_int(window.get("fetched_ts"), 0)
        if fetched_at > 0 and (now_ts - fetched_at) < freshness_sec:
            if force_top_recheck and (offset == 0) and (not forced_top_recheck_ran):
                pass
            else:
                skipped_fresh_pages += 1
                continue
        if force_top_recheck and fetched_pages >= 1:
            summary["reason"] = "zero_history_top_rechecked"
            break
        if added_total >= target_count:
            summary["reason"] = "target_reached"
            break
        if added_total >= soft_target:
            summary["reason"] = "soft_target_reached"
            break
        if force_top_recheck and offset == 0:
            forced_top_recheck_ran = True
        result = _run_rpa_page(query=search_query, offset=offset, limit=page_size)
        fetched_pages += 1
        rows = result.get("rows", []) if isinstance(result.get("rows"), list) else []
        row = rows[0] if rows and isinstance(rows[0], dict) else {}
        entries = _collect_row_entries(row) if row else []
        new_rows: List[Dict[str, Any]] = []
        for idx, entry in enumerate(entries, start=1):
            title = str(entry.get("title", "") or "").strip()
            if not title:
                continue
            seed_candidates = _extract_seed_queries_from_title(title, brand_hints)
            if not seed_candidates:
                continue
            rank = max(1, to_int(entry.get("rank"), idx))
            global_rank = offset + rank
            last_rank = max(last_rank, global_rank)
            for raw_seed_query in seed_candidates:
                seed_query = re.sub(r"\s+", " ", str(raw_seed_query or "").strip())
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
                            "query": search_query,
                            "category_key": category_key,
                            "category_label": category_label,
                            "seed_collected_at": utc_iso(now_ts),
                            "seed_collected_sold_90d_count": to_int(entry.get("sold_90d_count"), -1),
                            "seed_collected_sold_price_min_usd": to_float(entry.get("sold_price_min_90d"), -1.0),
                            "seed_quality_score": 0,
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
                "page": page_index + 1,
                "offset": offset,
                "new_seed_count": max(0, int(inserted)),
                "raw_result_count": len(entries),
                "daily_limit_reached": bool(result.get("daily_limit_reached")),
                "reason": str(result.get("reason", "") or ""),
            }
        )
        if bool(result.get("daily_limit_reached")):
            summary["daily_limit_reached"] = True
            summary["reason"] = "daily_limit_reached"
            break
        if len(entries) <= 0:
            summary["reason"] = "empty_result_page"
            break

    available_after = _count_available(conn, category_key=category_key, now_ts=now_ts)
    summary["added_count"] = added_total
    summary["skipped_fresh_pages"] = skipped_fresh_pages
    summary["available_after"] = available_after
    summary["last_rank_checked"] = last_rank
    cooldown_text = ""
    if (
        not summary["daily_limit_reached"]
        and fetched_pages >= max_pages
        and added_total < soft_target
    ):
        cooldown_text = utc_iso(now_ts + cooldown_days * 86400)
        summary["cooldown_until"] = cooldown_text
        summary["reason"] = "rank_limit_cooldown"
    if (
        not summary["daily_limit_reached"]
        and fetched_pages <= 0
        and skipped_fresh_pages > 0
        and str(summary.get("reason", "") or "") == "refilled"
    ):
        summary["reason"] = "fresh_window_skip"
    if (
        not summary["daily_limit_reached"]
        and str(summary.get("reason", "") or "") == "empty_result_page"
        and available_after <= 0
    ):
        cooldown_text = utc_iso(now_ts + empty_result_cooldown_hours * 3600)
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

    def _accumulate_skip_counts(dst: Dict[str, int], result_row: Dict[str, Any]) -> None:
        for key, value in result_row.items():
            if not str(key).startswith("skipped_"):
                continue
            dst[str(key)] = int(dst.get(str(key), 0)) + max(0, to_int(value, 0))

    def _accumulate_reason_counts(dst: Dict[str, int], result_row: Dict[str, Any]) -> None:
        row = result_row.get("low_match_reason_counts")
        if not isinstance(row, dict):
            return
        for key, value in row.items():
            name = str(key or "").strip()
            if not name:
                continue
            dst[name] = int(dst.get(name, 0)) + max(0, to_int(value, 0))

    if callable(progress_callback):
        progress_callback(
            {
                "phase": "timed_fetch_start",
                "message": f"{category_label}のSeedプールを確認しています",
                "progress_percent": 5.0,
                "pass_index": 0,
                "max_passes": run_batch_size,
                "created_count": 0,
                "stage1_pass_total": 0,
                "stage2_runs": 0,
            }
        )

    with connect(settings.db_path) as conn:
        init_db(conn)
        cleaned = _cleanup_expired(conn, category_key=category_key, now_ts=now_ts)
        if cleaned > 0:
            hints.append(f"期限切れSeedを {cleaned} 件削除しました。")
        refill_summary = _refill_seed_pool(
            conn,
            category_key=category_key,
            category_label=category_label,
            category_row=category_row,
        )
        available_after = _count_available(conn, category_key=category_key, now_ts=int(time.time()))
        # 有効Seedはすべて実行候補として扱う（非実行候補を作らない）。
        run_batch_size = max(1, int(available_after))
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
        hints.append(f"カテゴリ知識からSeedを {bootstrap_added} 件補充しました。")
    if refill_reason == "category_cooldown":
        cooldown_until = str(seed_pool_summary["refill"].get("cooldown_until", "") or "").strip()
        hints.append(f"{category_label}は深掘り上限に到達したため、{cooldown_until} まで補充を停止しています。")
    if refill_reason == "rank_limit_cooldown":
        cooldown_until = str(seed_pool_summary["refill"].get("cooldown_until", "") or "").strip()
        rank_ceiling = max(0, to_int(seed_pool_summary["refill"].get("rank_ceiling"), 0))
        hints.append(f"{category_label}の上位{rank_ceiling}件まで確認しました。{cooldown_until} 以降に再実行してください。")
    if refill_reason == "fresh_window_skip":
        hints.append(f"{category_label}の補充対象ページは7日以内に取得済みのため、今回は再取得を行いませんでした。")
    if refill_reason == "zero_history_top_rechecked":
        hints.append(f"{category_label}の補充履歴が空ページのみだったため、先頭ページを再確認しました。")
    if refill_reason == "empty_result_cooldown":
        cooldown_until = str(seed_pool_summary["refill"].get("cooldown_until", "") or "").strip()
        hints.append(f"{category_label}は検索結果が空だったため、{cooldown_until} までProduct Research補充を停止しています。")
    if bool(seed_pool_summary["refill"].get("daily_limit_reached")):
        rpa_daily_limit_reached = True
        hints.append("Product Researchの日次上限に到達したため、Seed補充を停止しました。")
    skipped_low_quality_count = max(0, to_int(seed_pool_summary.get("skipped_low_quality_count"), 0))
    if skipped_low_quality_count > 0:
        hints.append(f"低品質Seedを {skipped_low_quality_count} 件スキップしました。")
    if refill_reason == "empty_result_page":
        hints.append("Seed補充は0件でしたが、既存Seedを使って探索を継続します。")

    if callable(progress_callback):
        progress_callback(
            {
                "phase": "seed_pool_ready",
                "message": f"Seed準備完了: 実行 {len(selected_seeds)} 件 / 残り {available_after} 件",
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
            "hints": hints + ["有効なSeedがありません。カテゴリを変更するか、後で再実行してください。"],
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
                "max_passes": max(1, int(max_passes)),
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

    stage1_pass_total = 0
    stage2_runs = 0
    stage1_seed_baseline_reject_total = 0
    for idx, seed in enumerate(selected_seeds, start=1):
        elapsed = time.monotonic() - started
        seed_query = str(seed.get("seed_query", "") or "")
        if elapsed >= max(10, int(timebox_sec)):
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
                }
            )
        has_model_code = bool(_extract_codes(seed_query))
        relaxed_floor = max(0.5, min(0.75, env_float("MINER_SEED_POOL_NON_MODEL_MIN_MATCH_SCORE", 0.62)))
        effective_min_match_score = float(min_match_score)
        if (not has_model_code) and effective_min_match_score > relaxed_floor:
            effective_min_match_score = relaxed_floor
        with _temporary_env(
            {
                "LIQUIDITY_REQUIRE_SIGNAL": "0",
                "LIQUIDITY_REQUIRE_SOLD_MIN_PRICE": "0",
                "LIQUIDITY_STRICT_SOLD_MIN_BASIS": "0",
            }
        ):
            stage1_result = fetch_live_miner_candidates(
                query=seed_query,
                source_sites=source_sites,
                market_site=market_site,
                limit_per_site=max(5, min(limit_per_site, env_int("MINER_STAGE1_LIMIT_PER_SITE", 10))),
                max_candidates=max(3, min(max_candidates, env_int("MINER_STAGE1_MAX_CANDIDATES", 8))),
                min_match_score=effective_min_match_score,
                min_profit_usd=min_profit_usd,
                min_margin_rate=min_margin_rate,
                require_in_stock=require_in_stock,
                timeout=timeout,
                settings=settings,
                run_rpa_refresh=False,
                persist_candidates=False,
            )
        _accumulate_skip_counts(stage1_skip_counts, stage1_result)
        _accumulate_reason_counts(stage1_low_match_reasons, stage1_result)
        if isinstance(stage1_result.get("applied_filters"), dict) and not applied_filters:
            applied_filters = dict(stage1_result.get("applied_filters"))
        if isinstance(stage1_result.get("errors"), list):
            errors.extend(stage1_result.get("errors"))
        if isinstance(stage1_result.get("hints"), list):
            for hint in stage1_result.get("hints"):
                text = str(hint or "").strip()
                if text:
                    hints.append(text)
        stage1_candidates = stage1_result.get("created") if isinstance(stage1_result.get("created"), list) else []
        baseline_usd = to_float(seed.get("seed_collected_sold_price_min_usd"), -1.0)
        stage1_baseline_reject = 0
        if baseline_usd > 0:
            filtered_stage1_candidates: List[Dict[str, Any]] = []
            for row in stage1_candidates:
                source_total_usd = to_float(row.get("source_total_usd"), -1.0) if isinstance(row, dict) else -1.0
                if source_total_usd > 0 and source_total_usd < baseline_usd:
                    filtered_stage1_candidates.append(row)
                else:
                    stage1_baseline_reject += 1
            stage1_candidates = filtered_stage1_candidates
        stage1_count = len(stage1_candidates)
        stage1_seed_baseline_reject_total += int(stage1_baseline_reject)
        stage1_pass_total += stage1_count
        if bool(stage1_result.get("rpa_daily_limit_reached")):
            rpa_daily_limit_reached = True
            stop_reason = "rpa_daily_limit_reached"
            passes.append(
                {
                    "pass": idx,
                    "seed_query": seed_query,
                    "stage1_candidate_count": stage1_count,
                    "stage2_created_count": 0,
                    "stop_reason": "rpa_daily_limit_reached",
                    "min_match_score": effective_min_match_score,
                    "has_model_code": has_model_code,
                }
            )
            break

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
                }
            )

        stage2_runs += 1
        result = fetch_live_miner_candidates(
            query=seed_query,
            source_sites=source_sites,
            market_site=market_site,
            limit_per_site=limit_per_site,
            max_candidates=max(1, min(max_candidates, stage1_count)),
            min_match_score=effective_min_match_score,
            min_profit_usd=min_profit_usd,
            min_margin_rate=min_margin_rate,
            require_in_stock=require_in_stock,
            timeout=timeout,
            settings=settings,
            run_rpa_refresh=True,
            persist_candidates=True,
        )
        _accumulate_skip_counts(stage2_skip_counts, result)
        _accumulate_reason_counts(stage2_low_match_reasons, result)
        _merge_fetched(fetched_aggregate, result.get("fetched"))
        for key in _COUNT_KEYS:
            aggregate_counts[key] += max(0, to_int(result.get(key), 0))
        for raw in result.get("created_ids", []) if isinstance(result.get("created_ids"), list) else []:
            cid = to_int(raw, -1)
            if cid <= 0 or cid in created_seen:
                continue
            created_seen.add(cid)
            created_ids.append(cid)
        rows = result.get("created", []) if isinstance(result.get("created"), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            cid = to_int(row.get("id"), -1)
            if cid <= 0 or cid not in created_seen:
                continue
            if any(to_int(existing.get("id"), -1) == cid for existing in created_items):
                continue
            created_items.append(row)
        if isinstance(result.get("errors"), list):
            errors.extend(result.get("errors"))
        if isinstance(result.get("hints"), list):
            for hint in result.get("hints"):
                text = str(hint or "").strip()
                if text:
                    hints.append(text)
        if bool(result.get("rpa_daily_limit_reached")):
            rpa_daily_limit_reached = True
            stop_reason = "rpa_daily_limit_reached"
        stage2_created_count = max(0, to_int(result.get("created_count"), 0))
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
            }
        )
        if bool(rpa_daily_limit_reached):
            break
        if len(created_ids) >= max(1, int(min_target_candidates)) and not bool(continue_after_target):
            stop_reason = "target_reached"
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
                "message": "Seed探索を完了しました",
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
            }
        )

    if len(created_ids) <= 0 and stage1_skip_counts:
        top_stage1 = sorted(stage1_skip_counts.items(), key=lambda kv: kv[1], reverse=True)[0]
        hints.append(f"一次判定の主な除外理由: {top_stage1[0]} ({top_stage1[1]}件)")
    if len(created_ids) <= 0 and stage1_low_match_reasons:
        top_reason = sorted(stage1_low_match_reasons.items(), key=lambda kv: kv[1], reverse=True)[0]
        hints.append(f"一次判定の一致不足主因: {top_reason[0]} ({top_reason[1]}件)")
    if len(created_ids) <= 0 and stage1_seed_baseline_reject_total > 0:
        hints.append(f"一次判定でSeed基準価格を満たさず除外: {stage1_seed_baseline_reject_total}件")

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
            "max_passes": max(1, int(max_passes)),
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

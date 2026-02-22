#!/usr/bin/env python3
"""Start one precision-first review cycle and snapshot a fixed review batch."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.local"
QUERY_STATS_PATH = ROOT_DIR / "data" / "query_efficiency_stats.json"
LIQUIDITY_BACKFILL_TARGETS_PATH = ROOT_DIR / "data" / "liquidity_backfill_targets.json"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from reselling.config import load_settings
from reselling.env import load_dotenv
from reselling.live_review_fetch import fetch_live_review_candidates
from reselling.models import connect, init_db


DEFAULT_QUERIES = [
    "casio gwm5610-1jf watch",
    "casio gw-5000u-1jf watch",
    "casio gmw-b5000 watch",
    "casio mtg-b2000 watch",
    "casio ga-b2100 watch",
    "g-shock gw9400 watch",
    "seiko sbga211 watch",
    "seiko sbdc101 watch",
    "seiko spb121 watch",
    "seiko spb143 watch",
    "seiko ssc813 watch",
    "seiko snxs79 watch",
    "citizen nb1050 watch",
    "citizen at8185 watch",
    "citizen cb0261 watch",
    "citizen bn0156 watch",
    "citizen tsuyosa nj015 watch",
    "orient ra-aa0810n watch",
    "orient fac00009n watch",
    "orient rn-aa0001b watch",
    "tissot prx t137407 watch",
    "hamilton h70455533 watch",
    "casio ocw-t200 watch",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _iso_to_epoch(text: str) -> int:
    raw = str(text or "").strip()
    if not raw:
        return 0
    try:
        norm = raw.replace("Z", "+00:00")
        return int(datetime.fromisoformat(norm).timestamp())
    except Exception:
        return 0


def _default_query_stat() -> Dict[str, Any]:
    return {
        "attempts": 0,
        "network_calls": 0,
        "cache_hits": 0,
        "created_total": 0,
        "queue_gain_total": 0,
        "zero_gain_streak": 0,
        "last_attempt_run_seq": 0,
        "last_attempt_at": "",
        "skip_count": 0,
    }


def _query_efficiency(stat: Dict[str, Any]) -> float:
    network_calls = int(stat.get("network_calls", 0) or 0)
    queue_gain = int(stat.get("queue_gain_total", 0) or 0)
    if network_calls <= 0:
        return 9999.0 if int(stat.get("attempts", 0) or 0) <= 0 else 0.0
    return queue_gain / max(1, network_calls)


def count_reviewable_pending(
    *,
    min_profit_usd: float,
    min_margin_rate: float,
    min_match_score: float,
    condition: str,
) -> int:
    settings = load_settings()
    with connect(settings.db_path) as conn:
        init_db(conn)
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM review_candidates
            WHERE status = 'pending'
              AND expected_profit_usd >= ?
              AND expected_margin_rate >= ?
              AND match_score >= ?
              AND LOWER(condition) = ?
            """,
            (
                float(min_profit_usd),
                float(min_margin_rate),
                float(min_match_score),
                str(condition).lower(),
            ),
        ).fetchone()
        return int(row["c"]) if row is not None else 0


def select_reviewable_pending_ids(
    *,
    min_profit_usd: float,
    min_margin_rate: float,
    min_match_score: float,
    condition: str,
    limit: int,
) -> List[int]:
    settings = load_settings()
    with connect(settings.db_path) as conn:
        init_db(conn)
        rows = conn.execute(
            """
            SELECT id
            FROM review_candidates
            WHERE status = 'pending'
              AND expected_profit_usd >= ?
              AND expected_margin_rate >= ?
              AND match_score >= ?
              AND LOWER(condition) = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (
                float(min_profit_usd),
                float(min_margin_rate),
                float(min_match_score),
                str(condition).lower(),
                int(limit),
            ),
        ).fetchall()
        return [int(r["id"]) for r in rows]


def make_cycle_id() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("cycle-%Y%m%d-%H%M%S")


def collect_api_efficiency(result: Dict[str, Any]) -> Dict[str, Any]:
    fetched = result.get("fetched", {})
    if not isinstance(fetched, dict):
        fetched = {}
    out_by_site: Dict[str, Dict[str, int]] = {}
    total_calls = 0
    cache_hits = 0
    network_calls = 0
    for site, info in fetched.items():
        if not isinstance(info, dict):
            continue
        calls = int(info.get("calls_made", 0) or 0)
        info_cache_hits = int(info.get("cache_hits", 0) or 0)
        info_network_calls = int(info.get("network_calls", 0) or 0)
        if info_cache_hits + info_network_calls == 0 and calls > 0:
            info_network_calls = calls
        total_calls += calls
        cache_hits += info_cache_hits
        network_calls += info_network_calls
        out_by_site[str(site)] = {
            "calls_made": calls,
            "cache_hits": info_cache_hits,
            "network_calls": info_network_calls,
            "budget_remaining": int(info.get("budget_remaining", -1) or -1),
        }
    if network_calls <= 0 and total_calls > 0:
        network_calls = max(0, total_calls - cache_hits)
    return {
        "total_calls": total_calls,
        "cache_hits": cache_hits,
        "network_calls": network_calls,
        "by_site": out_by_site,
    }


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _resolve_rpa_output_path() -> Path:
    raw = (os.getenv("LIQUIDITY_RPA_JSON_PATH", "") or "").strip() or "data/liquidity_rpa_signals.jsonl"
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (ROOT_DIR / path).resolve()
    return path


def _resolve_rpa_python() -> str:
    raw = (os.getenv("LIQUIDITY_RPA_PYTHON", "") or "").strip()
    if raw:
        return raw
    venv_python = ROOT_DIR / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable or "python3"


def _load_rpa_rows(path: Path) -> list[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[Dict[str, Any]] = []
    if path.suffix.lower() == ".json":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return rows
        if isinstance(payload, list):
            rows = [row for row in payload if isinstance(row, dict)]
        elif isinstance(payload, dict):
            rows = [payload]
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        text = str(line or "").strip()
        if not text:
            continue
        try:
            row = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _extract_model_codes(text: str) -> list[str]:
    raw = re.findall(r"[A-Z0-9][A-Z0-9-]{3,}", str(text or "").upper())
    out: list[str] = []
    seen: set[str] = set()
    for token in raw:
        cleaned = str(token or "").strip("-").upper()
        if len(cleaned) < 4:
            continue
        alpha = sum(1 for ch in cleaned if "A" <= ch <= "Z")
        digit = sum(1 for ch in cleaned if ch.isdigit())
        if alpha < 2 or digit < 1:
            continue
        compact = re.sub(r"[^A-Z0-9]+", "", cleaned)
        if compact in seen:
            continue
        seen.add(compact)
        out.append(compact)
    return out


def _build_model_code_backfill_queries(queries: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for query in queries:
        for code in _extract_model_codes(query):
            if code in seen:
                continue
            seen.add(code)
            out.append(code)
    return out


def _load_liquidity_backfill_targets() -> Dict[str, Any]:
    payload = _load_json(LIQUIDITY_BACKFILL_TARGETS_PATH)
    if not payload:
        return {"items": []}
    if not isinstance(payload.get("items"), list):
        payload["items"] = []
    return payload


def _save_liquidity_backfill_targets(payload: Dict[str, Any]) -> None:
    _save_json(LIQUIDITY_BACKFILL_TARGETS_PATH, payload)


def _target_reason_model_codes(queries: list[str], *, reason: str) -> list[str]:
    payload = _load_liquidity_backfill_targets()
    rows = payload.get("items", []) if isinstance(payload.get("items"), list) else []
    ttl = max(3600, int((os.getenv("LIQUIDITY_BACKFILL_TARGET_TTL_SECONDS", "604800") or "604800")))
    now_ts = int(time.time())
    query_set = {str(q or "").strip().lower() for q in queries if str(q or "").strip()}
    score_by_code: Dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_reason = str(row.get("reason", "") or "").strip()
        if row_reason != reason:
            continue
        code = str(row.get("model_code", "") or "").strip().upper()
        if not code:
            continue
        row_query = str(row.get("query", "") or "").strip().lower()
        if query_set and row_query and row_query not in query_set:
            continue
        seen_epoch = _iso_to_epoch(str(row.get("last_seen_at", "") or ""))
        if seen_epoch > 0 and (now_ts - seen_epoch) > ttl:
            continue
        count = max(1, int(row.get("count", 1) or 1))
        score_by_code[code] = score_by_code.get(code, 0) + count
    ordered = sorted(score_by_code.items(), key=lambda kv: kv[1], reverse=True)
    return [code for code, _ in ordered]


def _update_liquidity_backfill_targets(unavailable_codes_by_query: Dict[str, Dict[str, int]]) -> Dict[str, Any]:
    if not unavailable_codes_by_query:
        existing = _load_liquidity_backfill_targets()
        items = existing.get("items", []) if isinstance(existing.get("items"), list) else []
        return {
            "updated": False,
            "added_entries": 0,
            "touched_entries": 0,
            "total_entries": len(items),
        }
    payload = _load_liquidity_backfill_targets()
    rows = payload.get("items", []) if isinstance(payload.get("items"), list) else []
    now_iso = _now_iso()
    index: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    added_entries = 0
    touched_entries = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        reason = str(row.get("reason", "") or "").strip()
        query = str(row.get("query", "") or "").strip().lower()
        code = str(row.get("model_code", "") or "").strip().upper()
        if not reason or not query or not code:
            continue
        index[(reason, query, code)] = row

    for query, code_map in unavailable_codes_by_query.items():
        q = str(query or "").strip().lower()
        if not q or not isinstance(code_map, dict):
            continue
        for code_raw, add_count in code_map.items():
            code = str(code_raw or "").strip().upper()
            if not code:
                continue
            key = ("liquidity_unavailable_required", q, code)
            row = index.get(key)
            if not isinstance(row, dict):
                row = {
                    "reason": "liquidity_unavailable_required",
                    "query": q,
                    "model_code": code,
                    "count": 0,
                    "first_seen_at": now_iso,
                    "last_seen_at": now_iso,
                }
                rows.append(row)
                index[key] = row
                added_entries += 1
            touched_entries += 1
            row["count"] = int(row.get("count", 0) or 0) + max(1, int(add_count or 1))
            row["last_seen_at"] = now_iso

    ttl = max(3600, int((os.getenv("LIQUIDITY_BACKFILL_TARGET_TTL_SECONDS", "604800") or "604800")))
    now_ts = int(time.time())
    pruned: list[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        last_seen = _iso_to_epoch(str(row.get("last_seen_at", "") or ""))
        if last_seen > 0 and (now_ts - last_seen) > ttl:
            continue
        pruned.append(row)
    payload["updated_at"] = now_iso
    payload["items"] = sorted(
        pruned,
        key=lambda row: (
            -int(row.get("count", 0) or 0),
            str(row.get("query", "") or ""),
            str(row.get("model_code", "") or ""),
        ),
    )
    _save_liquidity_backfill_targets(payload)
    return {
        "updated": True,
        "added_entries": int(added_entries),
        "touched_entries": int(touched_entries),
        "total_entries": len(payload["items"]),
    }


def _queries_missing_sold_90d(rows: list[Dict[str, Any]], queries: list[str]) -> list[str]:
    def _safe_int(value: Any, default: int = -1) -> int:
        try:
            if value is None:
                return default
            return int(value)
        except (TypeError, ValueError):
            return default

    by_query: Dict[str, int] = {}
    for row in rows:
        query = str(row.get("query", "") or "").strip().lower()
        if not query:
            continue
        sold = _safe_int(row.get("sold_90d_count"), -1)
        prev = by_query.get(query, -999999)
        by_query[query] = max(prev, sold)
    missing: list[str] = []
    for query in queries:
        key = str(query or "").strip().lower()
        if not key:
            continue
        if by_query.get(key, -1) < 0:
            missing.append(query)
    return missing


def _run_rpa_collect(
    *,
    queries: list[str],
    output_path: Path,
    condition: str,
    strict_condition: bool,
    fixed_price_only: bool,
    pass_label: str,
) -> Dict[str, Any]:
    if not queries:
        return {
            "ran": False,
            "reason": "empty_queries",
            "query_count": 0,
            "returncode": 0,
            "stdout_tail": [],
            "stderr_tail": [],
        }
    runner = _resolve_rpa_python()
    script_path = ROOT_DIR / "scripts" / "rpa_market_research.py"
    cmd: list[str] = [
        runner,
        str(script_path),
        "--output",
        str(output_path),
        "--pause-for-login",
        str(max(0, int((os.getenv("LIQUIDITY_RPA_PAUSE_FOR_LOGIN_SECONDS", "0") or "0")))),
        "--wait-seconds",
        str(max(3, int((os.getenv("LIQUIDITY_RPA_WAIT_SECONDS", "10") or "10")))),
        "--lookback-days",
        str(max(7, int((os.getenv("LIQUIDITY_RPA_LOOKBACK_DAYS", "90") or "90")))),
        "--inter-query-sleep",
        str(max(0.0, float((os.getenv("LIQUIDITY_RPA_INTER_QUERY_SLEEP", "0.8") or "0.8")))),
        "--condition",
        str(condition or "new"),
        "--pass-label",
        str(pass_label or "primary_new"),
    ]
    profile_dir = (os.getenv("LIQUIDITY_RPA_PROFILE_DIR", "") or "").strip()
    if profile_dir:
        cmd.extend(["--profile-dir", profile_dir])
    login_url = (os.getenv("LIQUIDITY_RPA_LOGIN_URL", "") or "").strip()
    if login_url:
        cmd.extend(["--login-url", login_url])
    if _env_bool("LIQUIDITY_RPA_HEADLESS", False):
        cmd.append("--headless")
    if strict_condition:
        cmd.append("--strict-condition")
    if fixed_price_only:
        cmd.append("--fixed-price-only")
    for query in queries:
        cmd.extend(["--query", str(query)])

    started = _now_iso()
    proc = subprocess.run(cmd, cwd=str(ROOT_DIR), capture_output=True, text=True)
    ended = _now_iso()
    stdout_lines = [line for line in (proc.stdout or "").splitlines() if line.strip()]
    stderr_lines = [line for line in (proc.stderr or "").splitlines() if line.strip()]
    return {
        "ran": True,
        "reason": "ok" if int(proc.returncode) == 0 else "collector_failed",
        "query_count": len(queries),
        "queries": queries,
        "condition": condition,
        "strict_condition": bool(strict_condition),
        "fixed_price_only": bool(fixed_price_only),
        "pass_label": pass_label,
        "started_at": started,
        "ended_at": ended,
        "returncode": int(proc.returncode),
        "stdout_tail": stdout_lines[-24:],
        "stderr_tail": stderr_lines[-24:],
    }


def maybe_refresh_rpa_liquidity(
    queries: list[str],
    *,
    cache_only: bool,
    unavailable_reason_model_codes: list[str] | None = None,
) -> Dict[str, Any]:
    mode = (os.getenv("LIQUIDITY_PROVIDER_MODE", "none") or "none").strip().lower()
    enabled = mode in {"rpa", "rpa_json"} and _env_bool("LIQUIDITY_RPA_AUTO_REFRESH", True)
    summary: Dict[str, Any] = {
        "enabled": bool(enabled),
        "mode": mode,
        "cache_only": bool(cache_only),
        "ran": False,
        "output_path": str(_resolve_rpa_output_path()),
        "primary": {},
        "fallback": {},
        "model_backfill": {},
        "missing_before": 0,
        "missing_after_primary": 0,
        "missing_after_fallback": 0,
        "missing_after_model_backfill": 0,
    }
    if not enabled:
        summary["reason"] = "disabled_or_non_rpa_mode"
        return summary
    if cache_only:
        summary["reason"] = "cache_only_skip"
        return summary

    output_path = _resolve_rpa_output_path()
    primary_fixed = _env_bool("LIQUIDITY_RPA_PRIMARY_FIXED_PRICE_ONLY", True)
    primary = _run_rpa_collect(
        queries=queries,
        output_path=output_path,
        condition=str((os.getenv("LIQUIDITY_RPA_PRIMARY_CONDITION", "new") or "new").strip() or "new"),
        strict_condition=_env_bool("LIQUIDITY_RPA_PRIMARY_STRICT_CONDITION", True),
        fixed_price_only=primary_fixed,
        pass_label="primary_new",
    )
    summary["ran"] = bool(primary.get("ran"))
    summary["primary"] = primary
    rows_after_primary = _load_rpa_rows(output_path)
    missing_after_primary = _queries_missing_sold_90d(rows_after_primary, queries)
    summary["missing_after_primary"] = len(missing_after_primary)
    summary["missing_before"] = len(queries)

    fallback_enabled = _env_bool("LIQUIDITY_RPA_ENABLE_FALLBACK", True)
    if not fallback_enabled or not missing_after_primary:
        summary["missing_after_fallback"] = len(missing_after_primary)
        summary["missing_after_model_backfill"] = len(missing_after_primary)
        summary["reason"] = "primary_only" if not fallback_enabled else "no_missing_after_primary"
        return summary

    fallback_condition = str(
        (os.getenv("LIQUIDITY_RPA_FALLBACK_CONDITION", "any") or "any").strip() or "any"
    )
    fallback = _run_rpa_collect(
        queries=missing_after_primary,
        output_path=output_path,
        condition=fallback_condition,
        strict_condition=_env_bool("LIQUIDITY_RPA_FALLBACK_STRICT_CONDITION", False),
        fixed_price_only=_env_bool("LIQUIDITY_RPA_FALLBACK_FIXED_PRICE_ONLY", False),
        pass_label=f"fallback_{fallback_condition}",
    )
    summary["fallback"] = fallback
    rows_after_fallback = _load_rpa_rows(output_path)
    missing_after_fallback = _queries_missing_sold_90d(rows_after_fallback, queries)
    summary["missing_after_fallback"] = len(missing_after_fallback)
    summary["missing_after_model_backfill"] = len(missing_after_fallback)
    model_backfill_enabled = _env_bool("LIQUIDITY_RPA_ENABLE_MODEL_CODE_BACKFILL", True)
    if (not model_backfill_enabled) or (not missing_after_fallback):
        summary["reason"] = "fallback_completed"
        return summary

    backfill_source = str(
        (os.getenv("LIQUIDITY_RPA_MODEL_CODE_BACKFILL_SOURCE", "unavailable_reason_only") or "unavailable_reason_only")
        .strip()
        .lower()
    )
    reason_codes = [str(v or "").strip().upper() for v in (unavailable_reason_model_codes or []) if str(v or "").strip()]
    reason_codes = list(dict.fromkeys(reason_codes))
    model_queries: list[str] = []
    source_missing_queries: list[str] = list(missing_after_fallback)
    if backfill_source in {"unavailable_reason_only", "reason_only"}:
        model_queries = list(reason_codes)
    elif backfill_source in {"unavailable_then_query", "reason_then_query"}:
        model_queries = list(reason_codes)
        if not model_queries:
            model_queries = _build_model_code_backfill_queries(missing_after_fallback)
    else:
        model_queries = _build_model_code_backfill_queries(missing_after_fallback)

    max_model_queries = max(1, int((os.getenv("LIQUIDITY_RPA_MODEL_CODE_MAX_QUERIES", "8") or "8")))
    model_queries = model_queries[:max_model_queries]
    if not model_queries:
        summary["reason"] = "fallback_completed_no_model_codes"
        return summary

    model_condition = str((os.getenv("LIQUIDITY_RPA_MODEL_CODE_CONDITION", "any") or "any").strip() or "any")
    model_backfill = _run_rpa_collect(
        queries=model_queries,
        output_path=output_path,
        condition=model_condition,
        strict_condition=_env_bool("LIQUIDITY_RPA_MODEL_CODE_STRICT_CONDITION", False),
        fixed_price_only=_env_bool("LIQUIDITY_RPA_MODEL_CODE_FIXED_PRICE_ONLY", False),
        pass_label=f"model_code_{model_condition}",
    )
    summary["model_backfill"] = {
        **model_backfill,
        "source_missing_queries": source_missing_queries,
        "source_mode": backfill_source,
        "source_reason_model_codes": reason_codes[:max_model_queries],
    }
    rows_after_model_backfill = _load_rpa_rows(output_path)
    missing_after_model_backfill = _queries_missing_sold_90d(rows_after_model_backfill, queries)
    summary["missing_after_model_backfill"] = len(missing_after_model_backfill)
    summary["reason"] = "model_code_backfill_completed"
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Start review fetch cycle.")
    parser.add_argument("--target-count", type=int, default=24, help="Target pending reviewable candidates.")
    parser.add_argument("--hard-cap", type=int, default=30, help="Upper cap for one cycle batch size.")
    parser.add_argument("--limit-per-site", type=int, default=20)
    parser.add_argument("--max-candidates-per-query", type=int, default=20)
    parser.add_argument("--min-match-score", type=float, default=0.70)
    parser.add_argument("--min-profit-usd", type=float, default=0.01)
    parser.add_argument("--min-margin-rate", type=float, default=0.03)
    parser.add_argument("--timeout", type=int, default=18)
    parser.add_argument("--sleep-seconds", type=float, default=0.8)
    parser.add_argument("--max-rounds", type=int, default=4)
    parser.add_argument(
        "--max-zero-gain-strikes",
        type=int,
        default=2,
        help="Skip query when no queue gain repeats N times.",
    )
    parser.add_argument(
        "--historical-min-attempts",
        type=int,
        default=3,
        help="Minimum attempts before historical low-efficiency skip applies.",
    )
    parser.add_argument(
        "--historical-min-network-calls",
        type=int,
        default=6,
        help="Minimum network calls before historical low-efficiency skip applies.",
    )
    parser.add_argument(
        "--historical-min-gain-per-network-call",
        type=float,
        default=0.05,
        help="Skip query when queue_gain_total/network_calls falls below this threshold.",
    )
    parser.add_argument(
        "--historical-retry-every-runs",
        type=int,
        default=4,
        help="Even low-efficiency queries are retried every N cycles.",
    )
    parser.add_argument(
        "--duplicate-heavy-ratio-threshold",
        type=float,
        default=0.70,
        help="Apply cooldown when skipped_duplicates/evaluated_candidates exceeds this ratio.",
    )
    parser.add_argument(
        "--duplicate-heavy-min-evaluated",
        type=int,
        default=12,
        help="Minimum evaluated candidates before duplicate-heavy cooldown is considered.",
    )
    parser.add_argument(
        "--duplicate-heavy-min-duplicates",
        type=int,
        default=8,
        help="Minimum skipped_duplicates before duplicate-heavy cooldown is considered.",
    )
    parser.add_argument(
        "--disable-duplicate-heavy-cooldown",
        action="store_true",
        help="Disable duplicate-heavy cooldown optimization.",
    )
    parser.add_argument(
        "--disable-query-reorder",
        action="store_true",
        help="Keep query order as provided instead of efficiency-based reorder.",
    )
    parser.add_argument(
        "--cache-only",
        action="store_true",
        help="Use cached API responses only (no external API calls).",
    )
    parser.add_argument(
        "--cache-ttl-seconds",
        type=int,
        default=-1,
        help="Override REVIEW_FETCH_CACHE_TTL_SECONDS when >=0.",
    )
    parser.add_argument("--daily-budget-ebay", type=int, default=-1, help="Override eBay daily call budget.")
    parser.add_argument("--daily-budget-rakuten", type=int, default=-1, help="Override Rakuten daily call budget.")
    parser.add_argument("--daily-budget-yahoo", type=int, default=-1, help="Override Yahoo daily call budget.")
    parser.add_argument(
        "--output",
        default=str(ROOT_DIR / "docs" / "review_cycle_report_latest.json"),
        help="JSON report path",
    )
    parser.add_argument(
        "--active-manifest",
        default=str(ROOT_DIR / "docs" / "review_cycle_active.json"),
        help="Path to active cycle manifest JSON.",
    )
    parser.add_argument(
        "--queries",
        default="",
        help="Comma-separated queries (optional). If empty, internal defaults are used.",
    )
    parser.add_argument(
        "--require-full-batch",
        action="store_true",
        help="Return non-zero unless batch_size reaches target-count.",
    )
    args = parser.parse_args()

    load_dotenv(ENV_PATH)
    if args.cache_only:
        os.environ["REVIEW_FETCH_CACHE_ONLY"] = "1"
    if int(args.cache_ttl_seconds) >= 0:
        os.environ["REVIEW_FETCH_CACHE_TTL_SECONDS"] = str(int(args.cache_ttl_seconds))
    if int(args.daily_budget_ebay) >= 0:
        os.environ["REVIEW_FETCH_DAILY_CALL_BUDGET_EBAY"] = str(int(args.daily_budget_ebay))
    if int(args.daily_budget_rakuten) >= 0:
        os.environ["REVIEW_FETCH_DAILY_CALL_BUDGET_RAKUTEN"] = str(int(args.daily_budget_rakuten))
    if int(args.daily_budget_yahoo) >= 0:
        os.environ["REVIEW_FETCH_DAILY_CALL_BUDGET_YAHOO"] = str(int(args.daily_budget_yahoo))

    raw_queries = [q.strip() for q in args.queries.split(",") if q.strip()] if args.queries else list(DEFAULT_QUERIES)
    stats_payload = _load_json(QUERY_STATS_PATH)
    stats_meta = stats_payload.get("meta", {}) if isinstance(stats_payload.get("meta"), dict) else {}
    stats_rows = stats_payload.get("queries", {}) if isinstance(stats_payload.get("queries"), dict) else {}
    run_seq = int(stats_meta.get("run_seq", 0) or 0) + 1
    for query in raw_queries:
        if query not in stats_rows or not isinstance(stats_rows.get(query), dict):
            stats_rows[query] = _default_query_stat()
    if bool(args.disable_query_reorder):
        queries = list(raw_queries)
    else:
        queries = sorted(
            raw_queries,
            key=lambda q: (
                -_query_efficiency(stats_rows.get(q, {})),
                int(stats_rows.get(q, {}).get("attempts", 0) or 0),
            ),
        )
    target = max(1, int(args.target_count))
    hard_cap = max(target, int(args.hard_cap))
    min_match = float(args.min_match_score)
    min_profit = float(args.min_profit_usd)
    min_margin = float(args.min_margin_rate)
    max_rounds = max(1, int(args.max_rounds))
    sleep_seconds = max(0.0, float(args.sleep_seconds))
    max_zero_gain_strikes = max(1, int(args.max_zero_gain_strikes))
    historical_min_attempts = max(1, int(args.historical_min_attempts))
    historical_min_network_calls = max(1, int(args.historical_min_network_calls))
    historical_min_gain_per_network_call = max(0.0, float(args.historical_min_gain_per_network_call))
    historical_retry_every_runs = max(1, int(args.historical_retry_every_runs))
    duplicate_heavy_ratio_threshold = min(1.0, max(0.0, float(args.duplicate_heavy_ratio_threshold)))
    duplicate_heavy_min_evaluated = max(1, int(args.duplicate_heavy_min_evaluated))
    duplicate_heavy_min_duplicates = max(1, int(args.duplicate_heavy_min_duplicates))
    duplicate_heavy_cooldown_enabled = not bool(args.disable_duplicate_heavy_cooldown)
    unavailable_reason_model_codes = _target_reason_model_codes(
        queries,
        reason="liquidity_unavailable_required",
    )
    rpa_refresh_summary = maybe_refresh_rpa_liquidity(
        queries,
        cache_only=bool(args.cache_only),
        unavailable_reason_model_codes=unavailable_reason_model_codes,
    )
    if bool(rpa_refresh_summary.get("enabled")):
        print(
            "[rpa] mode={mode} ran={ran} missing primary={mp} fallback={mf} model_backfill={mm}".format(
                mode=rpa_refresh_summary.get("mode", ""),
                ran=bool(rpa_refresh_summary.get("ran")),
                mp=int(rpa_refresh_summary.get("missing_after_primary", 0) or 0),
                mf=int(rpa_refresh_summary.get("missing_after_fallback", 0) or 0),
                mm=int(rpa_refresh_summary.get("missing_after_model_backfill", 0) or 0),
            )
        )
        primary = rpa_refresh_summary.get("primary", {})
        if isinstance(primary, dict) and primary:
            print(
                f"[rpa] primary rc={int(primary.get('returncode', 0) or 0)} "
                f"queries={int(primary.get('query_count', 0) or 0)} condition={primary.get('condition', '')}"
            )
        fallback = rpa_refresh_summary.get("fallback", {})
        if isinstance(fallback, dict) and fallback:
            print(
                f"[rpa] fallback rc={int(fallback.get('returncode', 0) or 0)} "
                f"queries={int(fallback.get('query_count', 0) or 0)} condition={fallback.get('condition', '')}"
            )
        model_backfill = rpa_refresh_summary.get("model_backfill", {})
        if isinstance(model_backfill, dict) and model_backfill:
            print(
                f"[rpa] model_backfill rc={int(model_backfill.get('returncode', 0) or 0)} "
                f"queries={int(model_backfill.get('query_count', 0) or 0)} condition={model_backfill.get('condition', '')}"
            )

    start_ts = _now_iso()
    initial_count = count_reviewable_pending(
        min_profit_usd=min_profit,
        min_margin_rate=min_margin,
        min_match_score=min_match,
        condition="new",
    )
    current_count = initial_count

    runs: List[Dict[str, Any]] = []
    stopped_reason = "completed_rounds"
    query_zero_gain_streak: Dict[str, int] = {
        str(query): int((stats_rows.get(query) or {}).get("zero_gain_streak", 0) or 0)
        for query in queries
    }
    query_skipped_count = 0
    duplicate_heavy_cooldown_count = 0
    api_total_calls = 0
    api_cache_hits = 0
    api_network_calls = 0
    api_by_site_totals: Dict[str, Dict[str, int]] = {}
    budget_exhausted = False
    unavailable_codes_by_query: Dict[str, Dict[str, int]] = {}
    for round_idx in range(max_rounds):
        if current_count >= target:
            stopped_reason = "target_reached"
            break
        round_had_fetch = False
        for query in queries:
            if current_count >= target:
                stopped_reason = "target_reached"
                break
            stat_row = stats_rows.setdefault(query, _default_query_stat())
            attempts_hist = int(stat_row.get("attempts", 0) or 0)
            network_calls_hist = int(stat_row.get("network_calls", 0) or 0)
            last_attempt_run_seq = int(stat_row.get("last_attempt_run_seq", 0) or 0)
            runs_since_last_attempt = max(0, run_seq - last_attempt_run_seq)
            gain_per_network_call_hist = _query_efficiency(stat_row)
            zero_gain_hist = int(query_zero_gain_streak.get(query, 0) or 0)

            if (
                attempts_hist >= historical_min_attempts
                and network_calls_hist >= historical_min_network_calls
                and gain_per_network_call_hist < historical_min_gain_per_network_call
                and runs_since_last_attempt < historical_retry_every_runs
            ):
                query_skipped_count += 1
                stat_row["skip_count"] = int(stat_row.get("skip_count", 0) or 0) + 1
                runs.append(
                    {
                        "round": round_idx + 1,
                        "query": query,
                        "skipped_query": True,
                        "skip_reason": "historical_low_efficiency",
                        "queue_before": current_count,
                        "queue_after": current_count,
                        "historical_attempts": attempts_hist,
                        "historical_network_calls": network_calls_hist,
                        "historical_gain_per_network_call": round(gain_per_network_call_hist, 4),
                        "runs_since_last_attempt": runs_since_last_attempt,
                    }
                )
                continue

            if (
                zero_gain_hist >= max_zero_gain_strikes
                and runs_since_last_attempt < historical_retry_every_runs
            ):
                query_skipped_count += 1
                stat_row["skip_count"] = int(stat_row.get("skip_count", 0) or 0) + 1
                runs.append(
                    {
                        "round": round_idx + 1,
                        "query": query,
                        "skipped_query": True,
                        "skip_reason": "zero_gain_cooldown",
                        "zero_gain_streak": zero_gain_hist,
                        "runs_since_last_attempt": runs_since_last_attempt,
                        "queue_before": current_count,
                        "queue_after": current_count,
                    }
                )
                continue

            round_had_fetch = True
            before_count = current_count
            result = fetch_live_review_candidates(
                query=query,
                source_sites=["rakuten", "yahoo"],
                market_site="ebay",
                limit_per_site=int(args.limit_per_site),
                max_candidates=int(args.max_candidates_per_query),
                min_match_score=min_match,
                min_profit_usd=min_profit,
                min_margin_rate=min_margin,
                timeout=int(args.timeout),
            )
            api_eff = collect_api_efficiency(result)
            api_total_calls += int(api_eff["total_calls"])
            api_cache_hits += int(api_eff["cache_hits"])
            api_network_calls += int(api_eff["network_calls"])
            for site, metrics in api_eff["by_site"].items():
                slot = api_by_site_totals.setdefault(
                    str(site),
                    {"calls_made": 0, "cache_hits": 0, "network_calls": 0, "budget_remaining": -1},
                )
                slot["calls_made"] += int(metrics.get("calls_made", 0) or 0)
                slot["cache_hits"] += int(metrics.get("cache_hits", 0) or 0)
                slot["network_calls"] += int(metrics.get("network_calls", 0) or 0)
                br = int(metrics.get("budget_remaining", -1) or -1)
                if br >= 0:
                    slot["budget_remaining"] = br

            current_count = count_reviewable_pending(
                min_profit_usd=min_profit,
                min_margin_rate=min_margin,
                min_match_score=min_match,
                condition="new",
            )
            added_to_queue = max(0, current_count - before_count)
            run_row = {
                "round": round_idx + 1,
                "query": query,
                "created_count": int(result.get("created_count", 0)),
                "added_to_reviewable_queue": added_to_queue,
                "queue_before": before_count,
                "queue_after": current_count,
                "fetched": result.get("fetched", {}),
                "errors": result.get("errors", []),
                "skipped_duplicates": int(result.get("skipped_duplicates", 0)),
                "skipped_low_match": int(result.get("skipped_low_match", 0)),
                "skipped_invalid_price": int(result.get("skipped_invalid_price", 0)),
                "skipped_unprofitable": int(result.get("skipped_unprofitable", 0)),
                "skipped_low_margin": int(result.get("skipped_low_margin", 0)),
                "skipped_low_ev90": int(result.get("skipped_low_ev90", 0)),
                "skipped_low_liquidity": int(result.get("skipped_low_liquidity", 0)),
                "skipped_liquidity_unavailable": int(result.get("skipped_liquidity_unavailable", 0)),
                "skipped_ambiguous_model_title": int(result.get("skipped_ambiguous_model_title", 0)),
                "liquidity_unavailable_model_codes": result.get("liquidity_unavailable_model_codes", [])
                if isinstance(result.get("liquidity_unavailable_model_codes"), list)
                else [],
                "low_match_reason_counts": result.get("low_match_reason_counts", {})
                if isinstance(result.get("low_match_reason_counts"), dict)
                else {},
                "low_match_samples": result.get("low_match_samples", [])
                if isinstance(result.get("low_match_samples"), list)
                else [],
                "hints": result.get("hints", []) if isinstance(result.get("hints"), list) else [],
                "api_efficiency": api_eff,
            }
            evaluated_candidate_count = (
                int(run_row.get("created_count", 0) or 0)
                + int(run_row.get("skipped_duplicates", 0) or 0)
                + int(run_row.get("skipped_low_match", 0) or 0)
                + int(run_row.get("skipped_invalid_price", 0) or 0)
                + int(run_row.get("skipped_unprofitable", 0) or 0)
                + int(run_row.get("skipped_low_margin", 0) or 0)
                + int(run_row.get("skipped_low_ev90", 0) or 0)
                + int(run_row.get("skipped_low_liquidity", 0) or 0)
                + int(run_row.get("skipped_liquidity_unavailable", 0) or 0)
                + int(run_row.get("skipped_ambiguous_model_title", 0) or 0)
            )
            duplicate_ratio = (
                float(int(run_row.get("skipped_duplicates", 0) or 0)) / float(evaluated_candidate_count)
                if evaluated_candidate_count > 0
                else 0.0
            )
            run_row["evaluated_candidate_count"] = int(evaluated_candidate_count)
            run_row["duplicate_ratio"] = round(float(duplicate_ratio), 4)
            runs.append(run_row)
            if int(run_row.get("skipped_liquidity_unavailable", 0) or 0) > 0:
                bucket = unavailable_codes_by_query.setdefault(str(query or "").strip().lower(), {})
                unresolved_codes = [
                    str(code or "").strip().upper()
                    for code in run_row.get("liquidity_unavailable_model_codes", [])
                    if str(code or "").strip()
                ]
                if not unresolved_codes:
                    # 型番抽出が落ちたケースはクエリ文字列から補完して欠損を埋める。
                    unresolved_codes = [code.upper() for code in _extract_model_codes(str(query or ""))]
                for code in unresolved_codes:
                    key = str(code or "").strip().upper()
                    if not key:
                        continue
                    bucket[key] = int(bucket.get(key, 0) or 0) + 1
            created_count = int(result.get("created_count", 0))
            duplicate_heavy_cooldown_applied = bool(
                duplicate_heavy_cooldown_enabled
                and created_count <= 0
                and added_to_queue <= 0
                and int(api_eff.get("network_calls", 0) or 0) > 0
                and int(run_row.get("skipped_duplicates", 0) or 0) >= duplicate_heavy_min_duplicates
                and int(evaluated_candidate_count) >= duplicate_heavy_min_evaluated
                and float(duplicate_ratio) >= duplicate_heavy_ratio_threshold
            )
            if added_to_queue <= 0 and created_count <= 0 and int(api_eff.get("network_calls", 0) or 0) > 0:
                query_zero_gain_streak[query] = int(query_zero_gain_streak.get(query, 0)) + 1
            elif added_to_queue > 0 or created_count > 0:
                query_zero_gain_streak[query] = 0
            if duplicate_heavy_cooldown_applied:
                duplicate_heavy_cooldown_count += 1
                query_zero_gain_streak[query] = max(
                    int(query_zero_gain_streak.get(query, 0) or 0),
                    max_zero_gain_strikes,
                )
            run_row["duplicate_heavy_cooldown_applied"] = bool(duplicate_heavy_cooldown_applied)

            stat_row["attempts"] = int(stat_row.get("attempts", 0) or 0) + 1
            stat_row["network_calls"] = int(stat_row.get("network_calls", 0) or 0) + int(
                api_eff.get("network_calls", 0) or 0
            )
            stat_row["cache_hits"] = int(stat_row.get("cache_hits", 0) or 0) + int(api_eff.get("cache_hits", 0) or 0)
            stat_row["created_total"] = int(stat_row.get("created_total", 0) or 0) + created_count
            stat_row["queue_gain_total"] = int(stat_row.get("queue_gain_total", 0) or 0) + added_to_queue
            stat_row["zero_gain_streak"] = int(query_zero_gain_streak.get(query, 0) or 0)
            stat_row["last_attempt_run_seq"] = run_seq
            stat_row["last_attempt_at"] = _now_iso()

            print(
                f"[round {run_row['round']}] {query} -> created={run_row['created_count']} "
                f"queue+={added_to_queue} now={current_count} "
                f"api(network/cache)={api_eff['network_calls']}/{api_eff['cache_hits']}"
            )
            errors = result.get("errors", [])
            if isinstance(errors, list):
                for err in errors:
                    if not isinstance(err, dict):
                        continue
                    if "daily_budget_exhausted" in str(err.get("message", "")):
                        budget_exhausted = True
                        stopped_reason = "api_budget_exhausted"
                        break
            if budget_exhausted:
                break
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        if budget_exhausted:
            break
        if not round_had_fetch:
            stopped_reason = "low_yield_cooldown"
            break

    final_count = count_reviewable_pending(
        min_profit_usd=min_profit,
        min_margin_rate=min_margin,
        min_match_score=min_match,
        condition="new",
    )
    batch_size = min(final_count, target, hard_cap)
    selected_candidate_ids = select_reviewable_pending_ids(
        min_profit_usd=min_profit,
        min_margin_rate=min_margin,
        min_match_score=min_match,
        condition="new",
        limit=batch_size,
    )
    cycle_id = make_cycle_id()

    cycle_ready = batch_size >= target
    backfill_update_summary = _update_liquidity_backfill_targets(unavailable_codes_by_query)
    stats_payload["meta"] = {
        "run_seq": run_seq,
        "updated_at": _now_iso(),
    }
    stats_payload["queries"] = stats_rows
    _save_json(QUERY_STATS_PATH, stats_payload)

    ranked_queries = sorted(
        [
            {
                "query": q,
                "attempts": int((stats_rows.get(q) or {}).get("attempts", 0) or 0),
                "network_calls": int((stats_rows.get(q) or {}).get("network_calls", 0) or 0),
                "queue_gain_total": int((stats_rows.get(q) or {}).get("queue_gain_total", 0) or 0),
                "gain_per_network_call": round(_query_efficiency(stats_rows.get(q, {})), 4),
                "zero_gain_streak": int((stats_rows.get(q) or {}).get("zero_gain_streak", 0) or 0),
            }
            for q in queries
        ],
        key=lambda row: float(row["gain_per_network_call"]),
        reverse=True,
    )

    report = {
        "cycle_id": cycle_id,
        "started_at": start_ts,
        "ended_at": _now_iso(),
        "run_seq": run_seq,
        "target_count": target,
        "hard_cap": hard_cap,
        "batch_size": batch_size,
        "cycle_ready": cycle_ready,
        "initial_reviewable_pending": initial_count,
        "final_reviewable_pending": final_count,
        "delta_reviewable_pending": final_count - initial_count,
        "min_match_score": min_match,
        "min_profit_usd": min_profit,
        "min_margin_rate": min_margin,
        "condition": "new",
        "stopped_reason": stopped_reason,
        "selected_candidate_ids": selected_candidate_ids,
        "cache_only": bool(args.cache_only),
        "query_reordered": not bool(args.disable_query_reorder),
        "query_order": queries,
        "max_zero_gain_strikes": max_zero_gain_strikes,
        "historical_min_attempts": historical_min_attempts,
        "historical_min_network_calls": historical_min_network_calls,
        "historical_min_gain_per_network_call": historical_min_gain_per_network_call,
        "historical_retry_every_runs": historical_retry_every_runs,
        "duplicate_heavy_cooldown_enabled": bool(duplicate_heavy_cooldown_enabled),
        "duplicate_heavy_ratio_threshold": float(duplicate_heavy_ratio_threshold),
        "duplicate_heavy_min_evaluated": int(duplicate_heavy_min_evaluated),
        "duplicate_heavy_min_duplicates": int(duplicate_heavy_min_duplicates),
        "rpa_refresh": rpa_refresh_summary,
        "liquidity_backfill": backfill_update_summary,
        "query_skipped_count": query_skipped_count,
        "duplicate_heavy_cooldown_count": int(duplicate_heavy_cooldown_count),
        "query_efficiency_snapshot": ranked_queries,
        "api_efficiency_summary": {
            "total_calls": api_total_calls,
            "cache_hits": api_cache_hits,
            "network_calls": api_network_calls,
            "by_site": api_by_site_totals,
            "cache_hit_rate": round((api_cache_hits / api_total_calls), 4) if api_total_calls > 0 else 0.0,
        },
        "runs": runs,
    }

    manifest = {
        "cycle_id": cycle_id,
        "created_at": _now_iso(),
        "target_count": target,
        "hard_cap": hard_cap,
        "batch_size": batch_size,
        "filters": {
            "status": "pending",
            "min_profit_usd": min_profit,
            "min_margin_rate": min_margin,
            "min_match_score": min_match,
            "condition": "new",
        },
        "selected_candidate_ids": selected_candidate_ids,
        "notes": "Review all selected candidates. Approve only if same item and profit confidence is high.",
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    active_path = Path(args.active_manifest)
    active_path.parent.mkdir(parents=True, exist_ok=True)
    active_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nSaved report: {output_path}")
    print(f"Saved active cycle manifest: {active_path}")
    print(
        f"reviewable pending: initial={initial_count} final={final_count} "
        f"delta={final_count - initial_count} target={target}"
    )
    print(f"cycle_id={cycle_id} batch_size={batch_size} cycle_ready={cycle_ready}")
    print(
        "[backfill] updated={u} touched={t} added={a} total={n}".format(
            u=bool(backfill_update_summary.get("updated")),
            t=int(backfill_update_summary.get("touched_entries", 0) or 0),
            a=int(backfill_update_summary.get("added_entries", 0) or 0),
            n=int(backfill_update_summary.get("total_entries", 0) or 0),
        )
    )
    if args.require_full_batch and not cycle_ready:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

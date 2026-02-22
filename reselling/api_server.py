"""Minimal JSON API server (no external dependencies)."""

from __future__ import annotations

import json
from pathlib import Path
import threading
import time
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import re
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from .fx_rate import get_current_usd_jpy_snapshot, maybe_refresh_usd_jpy_rate
from .live_review_fetch import (
    backfill_candidate_market_images,
    fetch_live_review_candidates,
    get_rpa_progress_snapshot,
)
from .profit import ProfitInput, calculate_profit
from .review import (
    approve_review_candidate,
    create_review_candidate,
    get_review_candidate,
    list_review_queue,
    reject_review_candidate,
)

ROOT_DIR = Path(__file__).resolve().parents[1]
WEB_DIR = ROOT_DIR / "web"
ACTIVE_CYCLE_PATH = ROOT_DIR / "docs" / "review_cycle_active.json"
CATEGORY_KNOWLEDGE_PATH = ROOT_DIR / "data" / "category_knowledge_seeds_v1.json"
_FETCH_PROGRESS_LOCK = threading.Lock()


def _default_fetch_progress() -> Dict[str, Any]:
    now_ts = int(time.time())
    return {
        "status": "idle",
        "phase": "idle",
        "message": "待機中",
        "progress_percent": 0.0,
        "run_id": "",
        "query": "",
        "started_at_epoch": 0,
        "updated_at_epoch": now_ts,
        "ended_at_epoch": 0,
        "timed_mode": True,
        "pass_index": 0,
        "max_passes": 0,
        "created_count": 0,
        "stop_reason": "",
    }


_FETCH_PROGRESS_STATE: Dict[str, Any] = _default_fetch_progress()


def _json_error(message: str, *, code: str) -> Dict[str, Any]:
    return {"error": {"code": code, "message": message}}


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    if isinstance(value, str):
        norm = value.strip().lower()
        if not norm:
            return default
        if norm in {"1", "true", "yes", "on"}:
            return True
        if norm in {"0", "false", "no", "off"}:
            return False
    return default


def _to_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _set_fetch_progress(update: Dict[str, Any]) -> Dict[str, Any]:
    now_ts = int(time.time())
    with _FETCH_PROGRESS_LOCK:
        prev = dict(_FETCH_PROGRESS_STATE)
        _FETCH_PROGRESS_STATE.update(update if isinstance(update, dict) else {})
        current_status = str(_FETCH_PROGRESS_STATE.get("status", "")).lower().strip()
        prev_status = str(prev.get("status", "")).lower().strip()
        current_run_id = str(_FETCH_PROGRESS_STATE.get("run_id", "") or "").strip()
        prev_run_id = str(prev.get("run_id", "") or "").strip()
        prev_percent = _to_float(prev.get("progress_percent"), 0.0)
        current_percent = _to_float(_FETCH_PROGRESS_STATE.get("progress_percent"), 0.0)
        same_run = bool(current_run_id and prev_run_id and current_run_id == prev_run_id)
        same_implicit_run = (
            (not current_run_id)
            and (not prev_run_id)
            and prev_status == "running"
            and current_status == "running"
        )
        # 進捗バー逆戻り防止: 同一run中のrunning更新は単調増加に固定する。
        if current_status == "running" and current_percent < prev_percent and (same_run or same_implicit_run):
            _FETCH_PROGRESS_STATE["progress_percent"] = prev_percent
        _FETCH_PROGRESS_STATE["progress_percent"] = round(
            max(0.0, min(100.0, _to_float(_FETCH_PROGRESS_STATE.get("progress_percent"), 0.0))), 2
        )
        _FETCH_PROGRESS_STATE["updated_at_epoch"] = now_ts
        if _to_int(_FETCH_PROGRESS_STATE.get("started_at_epoch"), 0) <= 0 and str(
            _FETCH_PROGRESS_STATE.get("status", "")
        ).lower() == "running":
            _FETCH_PROGRESS_STATE["started_at_epoch"] = now_ts
        return dict(_FETCH_PROGRESS_STATE)


def _get_fetch_progress_snapshot() -> Dict[str, Any]:
    with _FETCH_PROGRESS_LOCK:
        snap = dict(_FETCH_PROGRESS_STATE)
    now_ts = int(time.time())
    updated_at = _to_int(snap.get("updated_at_epoch"), 0)
    snap["updated_ago_sec"] = max(0, now_ts - updated_at) if updated_at > 0 else -1
    rpa = get_rpa_progress_snapshot()
    snap["rpa"] = rpa
    if str(snap.get("status", "")).lower() == "running":
        rpa_status = str(rpa.get("status", "")).lower()
        rpa_percent = max(0.0, min(100.0, _to_float(rpa.get("progress_percent"), 0.0)))
        base_percent = max(0.0, min(100.0, _to_float(snap.get("progress_percent"), 0.0)))
        fetch_started = _to_int(snap.get("started_at_epoch"), 0)
        rpa_started = _to_int(rpa.get("started_at_epoch"), 0)
        same_window = (
            fetch_started <= 0
            or rpa_started <= 0
            or rpa_started >= max(0, fetch_started - 5)
        )
        # 古いRPA完了状態の混入を避けるため、running中のみブレンドする。
        if (
            rpa_status == "running"
            and _to_int(rpa.get("updated_ago_sec"), 99999) <= 120
            and same_window
        ):
            blended = max(base_percent, min(99.0, base_percent * 0.72 + rpa_percent * 0.28))
            blended_rounded = round(blended, 2)
            snap["progress_percent"] = blended_rounded
            # 合成進捗を下限として保持し、次ポーリングでの逆戻りを防ぐ。
            with _FETCH_PROGRESS_LOCK:
                current_state_percent = max(
                    0.0, min(100.0, _to_float(_FETCH_PROGRESS_STATE.get("progress_percent"), 0.0))
                )
                if blended_rounded > current_state_percent and str(
                    _FETCH_PROGRESS_STATE.get("status", "")
                ).lower() == "running":
                    _FETCH_PROGRESS_STATE["progress_percent"] = blended_rounded
            rpa_message = str(rpa.get("message", "") or "").strip()
            if rpa_message:
                snap["message"] = f"{str(snap.get('message', '') or '').strip()} / Product Research: {rpa_message}"
    return snap


def _fetch_live_review_candidates_timed(
    *,
    query: str,
    source_sites: List[str],
    market_site: str,
    limit_per_site: int,
    max_candidates: int,
    min_match_score: float,
    min_profit_usd: float,
    min_margin_rate: float,
    require_in_stock: bool,
    timeout: int,
    min_target_candidates: int,
    timebox_sec: int,
    max_passes: int,
    continue_after_target: bool,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    started = time.monotonic()
    pass_rows: List[Dict[str, Any]] = []
    created_ids: List[int] = []
    created_items: List[Dict[str, Any]] = []
    created_seen: set[int] = set()
    zero_gain_streak = 0
    stop_reason = "timebox"
    reached_min_target = False
    current_min_match_score = float(min_match_score)
    broad_query = bool((not re.search(r"\d", str(query or ""))) and len(str(query or "").strip().split()) <= 3)
    adaptive_floor = 0.54 if broad_query else 0.64
    adaptive_step = 0.05 if broad_query else 0.03

    aggregate_counts: Dict[str, int] = {
        "created_count": 0,
        "skipped_duplicates": 0,
        "skipped_low_match": 0,
        "skipped_invalid_price": 0,
        "skipped_unprofitable": 0,
        "skipped_low_margin": 0,
        "skipped_low_ev90": 0,
        "skipped_low_liquidity": 0,
        "skipped_liquidity_unavailable": 0,
        "skipped_missing_sold_min": 0,
        "skipped_missing_sold_sample": 0,
        "skipped_below_sold_min": 0,
        "skipped_implausible_sold_min": 0,
        "skipped_blocked": 0,
        "skipped_group_cap": 0,
        "skipped_ambiguous_model_title": 0,
    }

    latest_result: Dict[str, Any] = {}
    if callable(progress_callback):
        progress_callback(
            {
                "phase": "timed_fetch_start",
                "message": "探索パスを開始しました",
                "progress_percent": 6.0,
                "pass_index": 0,
                "max_passes": max(1, int(max_passes)),
                "created_count": 0,
            }
        )

    for pass_index in range(max(1, int(max_passes))):
        elapsed_before = time.monotonic() - started
        if elapsed_before >= float(max(1, int(timebox_sec))):
            stop_reason = "timebox_reached_before_next_pass"
            break
        if callable(progress_callback):
            # 前パス完了値より下がらないよう、running開始点を単調増加にする。
            phase_progress = 12.0 + (72.0 * (pass_index / max(1, int(max_passes))))
            progress_callback(
                {
                    "phase": "pass_running",
                    "message": f"{pass_index + 1}パス目を探索中",
                    "progress_percent": phase_progress,
                    "pass_index": pass_index + 1,
                    "max_passes": max(1, int(max_passes)),
                    "created_count": len(created_ids),
                }
            )

        result = fetch_live_review_candidates(
            query=query,
            source_sites=source_sites,
            market_site=market_site,
            limit_per_site=limit_per_site,
            max_candidates=max_candidates,
            min_match_score=current_min_match_score,
            min_profit_usd=min_profit_usd,
            min_margin_rate=min_margin_rate,
            require_in_stock=require_in_stock,
            timeout=timeout,
        )
        latest_result = result if isinstance(result, dict) else {}

        pass_created_ids = []
        for raw in latest_result.get("created_ids", []) if isinstance(latest_result.get("created_ids"), list) else []:
            cid = _to_int(raw, -1)
            if cid <= 0:
                continue
            pass_created_ids.append(cid)
            if cid in created_seen:
                continue
            created_seen.add(cid)
            created_ids.append(cid)

        pass_created_items = latest_result.get("created", [])
        if isinstance(pass_created_items, list):
            for row in pass_created_items:
                if not isinstance(row, dict):
                    continue
                cid = _to_int(row.get("id"), -1)
                if cid <= 0 or cid not in created_seen:
                    continue
                if any(_to_int(existing.get("id"), -1) == cid for existing in created_items):
                    continue
                created_items.append(row)

        pass_created_count = len(pass_created_ids)
        if pass_created_count > 0:
            zero_gain_streak = 0
        else:
            zero_gain_streak += 1

        for key in list(aggregate_counts.keys()):
            aggregate_counts[key] += max(0, _to_int(latest_result.get(key), 0))

        elapsed_after = time.monotonic() - started
        pass_rows.append(
            {
                "pass": pass_index + 1,
                "min_match_score": round(current_min_match_score, 4),
                "created_count": int(pass_created_count),
                "created_ids": pass_created_ids,
                "elapsed_sec": round(elapsed_after, 3),
                "query_cache_skip": bool(latest_result.get("query_cache_skip")),
                "search_scope_done": bool(latest_result.get("search_scope_done")),
            }
        )

        reached_min_target = len(created_ids) >= max(1, int(min_target_candidates))
        if callable(progress_callback):
            phase_progress = 12.0 + (72.0 * ((pass_index + 1) / max(1, int(max_passes))))
            progress_callback(
                {
                    "phase": "pass_completed",
                    "message": f"{pass_index + 1}パス目を完了",
                    "progress_percent": min(88.0, phase_progress),
                    "pass_index": pass_index + 1,
                    "max_passes": max(1, int(max_passes)),
                    "created_count": len(created_ids),
                }
            )
        if elapsed_after >= float(max(1, int(timebox_sec))):
            stop_reason = "timebox_reached"
            break
        if bool(latest_result.get("query_cache_skip")):
            stop_reason = "query_cache_skip"
            break
        if bool(latest_result.get("rpa_daily_limit_reached")):
            stop_reason = "rpa_daily_limit_reached"
            break
        if bool(latest_result.get("search_scope_done")) and pass_created_count <= 0:
            stop_reason = "search_scope_done_no_gain"
            break
        low_match = max(0, _to_int(latest_result.get("skipped_low_match"), 0))
        low_match_reason_counts = (
            latest_result.get("low_match_reason_counts")
            if isinstance(latest_result.get("low_match_reason_counts"), dict)
            else {}
        )
        dominant_reason = ""
        if low_match_reason_counts:
            dominant_reason = str(
                sorted(low_match_reason_counts.items(), key=lambda kv: _to_int(kv[1], 0), reverse=True)[0][0]
            ).strip()
        should_relax_match = (
            pass_created_count <= 0
            and low_match >= 12
            and current_min_match_score > adaptive_floor
            and (
                dominant_reason in {
                    "token_overlap",
                    "model_code_conflict",
                    "color_missing_market",
                    "variant_color_missing_market",
                    "variant_color_missing_source",
                }
                or not dominant_reason
            )
        )
        if dominant_reason == "token_overlap" and pass_index >= 1 and pass_created_count <= 0:
            adaptive_step = max(adaptive_step, 0.06 if broad_query else 0.04)
        if should_relax_match:
            current_min_match_score = max(adaptive_floor, round(current_min_match_score - adaptive_step, 4))
            continue
        if zero_gain_streak >= 2:
            stop_reason = "consecutive_no_gain"
            break
        if reached_min_target and (not continue_after_target):
            stop_reason = "min_target_reached"
            break
    else:
        stop_reason = "max_passes_reached"

    final_result = dict(latest_result) if isinstance(latest_result, dict) else {}
    final_result["created_ids"] = created_ids
    final_result["created"] = created_items
    final_result["created_count"] = len(created_ids)
    for key, value in aggregate_counts.items():
        final_result[key] = int(value)
    final_result["timed_fetch"] = {
        "enabled": True,
        "min_target_candidates": max(1, int(min_target_candidates)),
        "timebox_sec": max(1, int(timebox_sec)),
        "max_passes": max(1, int(max_passes)),
        "continue_after_target": bool(continue_after_target),
        "passes_run": len(pass_rows),
        "stop_reason": stop_reason,
        "elapsed_sec": round(time.monotonic() - started, 3),
        "reached_min_target": bool(reached_min_target),
        "adaptive_min_match_floor": adaptive_floor,
        "adaptive_min_match_step": adaptive_step,
        "passes": pass_rows,
    }
    if callable(progress_callback):
        progress_callback(
            {
                "phase": "timed_fetch_finalize",
                "message": "探索結果を集計しています",
                "progress_percent": 94.0,
                "pass_index": len(pass_rows),
                "max_passes": max(1, int(max_passes)),
                "created_count": len(created_ids),
                "stop_reason": stop_reason,
            }
        )
    return final_result


class ApiHandler(BaseHTTPRequestHandler):
    # Legacy identifier was "ebayminer-api/0.1".
    server_version = "reseller-api/0.1"

    def _send_bytes(
        self,
        status: int,
        payload: bytes,
        *,
        content_type: str = "application/octet-stream",
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(payload)

    def _send(self, status: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._send_bytes(status, body, content_type="application/json; charset=utf-8")

    def _send_file(self, path: Path, *, content_type: str) -> None:
        if not path.exists() or not path.is_file():
            self._send(
                HTTPStatus.NOT_FOUND,
                _json_error("file not found", code="file_not_found"),
            )
            return
        self._send_bytes(HTTPStatus.OK, path.read_bytes(), content_type=content_type)

    def _read_json(self) -> Dict[str, Any]:
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        if content_length <= 0:
            return {}
        raw = self.rfile.read(content_length).decode("utf-8", errors="replace")
        if not raw.strip():
            return {}
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("JSON body must be an object")
        return parsed

    def log_message(self, fmt: str, *args: Any) -> None:  # pragma: no cover
        # Keep logs concise for local usage.
        print(f"[api] {self.address_string()} - {fmt % args}")

    def do_OPTIONS(self) -> None:
        self._send(HTTPStatus.NO_CONTENT, {})

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        if parsed.path == "/" or parsed.path == "/review":
            self._send_file(WEB_DIR / "review.html", content_type="text/html; charset=utf-8")
            return
        if parsed.path == "/static/review.css":
            self._send_file(WEB_DIR / "review.css", content_type="text/css; charset=utf-8")
            return
        if parsed.path == "/static/review.js":
            self._send_file(
                WEB_DIR / "review.js",
                content_type="application/javascript; charset=utf-8",
            )
            return
        if parsed.path == "/healthz":
            self._send(HTTPStatus.OK, {"ok": True})
            return

        if parsed.path == "/v1/system/rpa-progress":
            self._send(HTTPStatus.OK, get_rpa_progress_snapshot())
            return

        if parsed.path == "/v1/system/fetch-progress":
            self._send(HTTPStatus.OK, _get_fetch_progress_snapshot())
            return

        if parsed.path == "/v1/system/fx-rate":
            snap = get_current_usd_jpy_snapshot()
            self._send(
                HTTPStatus.OK,
                {
                    "pair": snap.pair,
                    "rate": snap.rate,
                    "source": snap.source,
                    "fetched_at": snap.fetched_at,
                    "next_refresh_at": snap.next_refresh_at,
                    "provenance": snap.provenance,
                },
            )
            return

        if parsed.path == "/v1/review/queue":
            try:
                status = (query.get("status", ["pending"])[0] or "pending").strip()
                limit = int((query.get("limit", ["50"])[0] or "50").strip())
                offset = int((query.get("offset", ["0"])[0] or "0").strip())
                min_profit_raw = (query.get("min_profit_usd", [""])[0] or "").strip()
                min_margin_raw = (query.get("min_margin_rate", [""])[0] or "").strip()
                min_match_raw = (query.get("min_match_score", [""])[0] or "").strip()
                condition = (query.get("condition", [""])[0] or "").strip() or None
                min_profit_usd = float(min_profit_raw) if min_profit_raw else None
                min_margin_rate = float(min_margin_raw) if min_margin_raw else None
                min_match_score = float(min_match_raw) if min_match_raw else None
                candidate_ids_raw = (query.get("candidate_ids", [""])[0] or "").strip()
                candidate_ids = None
                if candidate_ids_raw:
                    candidate_ids = []
                    for part in candidate_ids_raw.split(","):
                        token = part.strip()
                        if not token:
                            continue
                        candidate_ids.append(int(token))
                payload = list_review_queue(
                    status=status,
                    limit=limit,
                    offset=offset,
                    min_profit_usd=min_profit_usd,
                    min_margin_rate=min_margin_rate,
                    min_match_score=min_match_score,
                    condition=condition,
                    candidate_ids=candidate_ids,
                )
                items = payload.get("items") if isinstance(payload, dict) else None
                if isinstance(items, list) and items:
                    backfill_candidate_market_images(items, timeout=8, max_calls=6)
                self._send(HTTPStatus.OK, payload)
            except ValueError as err:
                self._send(
                    HTTPStatus.BAD_REQUEST,
                    _json_error(str(err), code="bad_request"),
                )
            return

        if parsed.path == "/v1/review/category-options":
            items: list[Dict[str, str]] = []
            try:
                if CATEGORY_KNOWLEDGE_PATH.exists():
                    payload = json.loads(CATEGORY_KNOWLEDGE_PATH.read_text(encoding="utf-8"))
                    categories = payload.get("categories", [])
                    if isinstance(categories, list):
                        for row in categories:
                            if not isinstance(row, dict):
                                continue
                            value = str(row.get("category_key", "") or "").strip()
                            if not value:
                                continue
                            label = str(row.get("display_name_ja", "") or "").strip() or value
                            items.append({"value": value, "label": label})
            except Exception:
                items = []
            self._send(HTTPStatus.OK, {"items": items})
            return

        m = re.fullmatch(r"/v1/review/candidates/(\d+)", parsed.path)
        if m:
            candidate_id = int(m.group(1))
            candidate = get_review_candidate(candidate_id)
            if candidate is None:
                self._send(
                    HTTPStatus.NOT_FOUND,
                    _json_error("candidate not found", code="candidate_not_found"),
                )
                return
            backfill_candidate_market_images([candidate], timeout=8, max_calls=1)
            self._send(HTTPStatus.OK, candidate)
            return

        if parsed.path == "/v1/review/cycle/active":
            if not ACTIVE_CYCLE_PATH.exists():
                self._send(
                    HTTPStatus.NOT_FOUND,
                    _json_error("active cycle manifest not found", code="cycle_not_found"),
                )
                return
            try:
                payload = json.loads(ACTIVE_CYCLE_PATH.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("manifest is not an object")
            except Exception as err:
                self._send(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    _json_error(f"failed to load active cycle: {err}", code="cycle_manifest_error"),
                )
                return
            self._send(HTTPStatus.OK, payload)
            return

        self._send(
            HTTPStatus.NOT_FOUND,
            _json_error("route not found", code="not_found"),
        )

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)

        try:
            body = self._read_json()
        except Exception as err:
            self._send(
                HTTPStatus.BAD_REQUEST,
                _json_error(f"invalid JSON body: {err}", code="invalid_json"),
            )
            return

        try:
            if parsed.path == "/v1/system/fx-rate/refresh":
                force_q = (query.get("force", ["false"])[0] or "false").lower() in {
                    "1",
                    "true",
                    "yes",
                }
                force_body = bool(body.get("force", False))
                result = maybe_refresh_usd_jpy_rate(force=(force_q or force_body))
                self._send(HTTPStatus.OK, result)
                return

            if parsed.path == "/v1/review/candidates":
                candidate = create_review_candidate(body)
                self._send(HTTPStatus.CREATED, candidate)
                return

            if parsed.path == "/v1/review/fetch":
                query_text = str(body.get("query", "") or "")
                source_sites = body.get("source_sites", ["rakuten", "yahoo"])
                if not isinstance(source_sites, list):
                    raise ValueError("source_sites must be an array")
                timed_mode = _to_bool(body.get("timed_mode", True), True)
                min_target_candidates = max(1, _to_int(body.get("target_min_candidates", 3), 3))
                timebox_sec = max(10, _to_int(body.get("fetch_timebox_sec", 60), 60))
                max_passes = max(1, min(12, _to_int(body.get("fetch_max_passes", 4), 4)))
                continue_after_target = _to_bool(body.get("continue_after_target", True), True)
                fetch_kwargs = {
                    "query": query_text,
                    "source_sites": [str(v) for v in source_sites],
                    "market_site": str(body.get("market_site", "ebay") or "ebay"),
                    "limit_per_site": int(body.get("limit_per_site", 20)),
                    "max_candidates": int(body.get("max_candidates", 20)),
                    "min_match_score": float(body.get("min_match_score", 0.72)),
                    "min_profit_usd": float(body.get("min_profit_usd", 0.01)),
                    "min_margin_rate": float(body.get("min_margin_rate", 0.03)),
                    "require_in_stock": _to_bool(body.get("require_in_stock", True), True),
                    "timeout": int(body.get("timeout", 18)),
                }
                run_id = f"fetch-{int(time.time() * 1000)}"
                started_at = int(time.time())
                _set_fetch_progress(
                    {
                        "status": "running",
                        "phase": "starting",
                        "message": "探索リクエストを受け付けました",
                        "progress_percent": 1.0,
                        "run_id": run_id,
                        "query": query_text,
                        "started_at_epoch": started_at,
                        "ended_at_epoch": 0,
                        "timed_mode": bool(timed_mode),
                        "pass_index": 0,
                        "max_passes": max_passes if timed_mode else 1,
                        "created_count": 0,
                        "stop_reason": "",
                    }
                )

                def _progress_cb(update: Dict[str, Any]) -> None:
                    if not isinstance(update, dict):
                        return
                    row = {
                        "status": "running",
                        "phase": str(update.get("phase", "running") or "running"),
                        "message": str(update.get("message", "探索中") or "探索中"),
                        "progress_percent": _to_float(update.get("progress_percent"), 0.0),
                        "pass_index": max(0, _to_int(update.get("pass_index"), 0)),
                        "max_passes": max(1, _to_int(update.get("max_passes"), max_passes if timed_mode else 1)),
                        "created_count": max(0, _to_int(update.get("created_count"), 0)),
                        "stop_reason": str(update.get("stop_reason", "") or ""),
                    }
                    _set_fetch_progress(row)

                try:
                    if timed_mode:
                        payload = _fetch_live_review_candidates_timed(
                            **fetch_kwargs,
                            min_target_candidates=min_target_candidates,
                            timebox_sec=timebox_sec,
                            max_passes=max_passes,
                            continue_after_target=continue_after_target,
                            progress_callback=_progress_cb,
                        )
                    else:
                        _progress_cb(
                            {
                                "phase": "single_pass_running",
                                "message": "探索を実行中",
                                "progress_percent": 45.0,
                                "pass_index": 1,
                                "max_passes": 1,
                            }
                        )
                        payload = fetch_live_review_candidates(
                            **fetch_kwargs,
                        )
                        payload["timed_fetch"] = {
                            "enabled": False,
                            "min_target_candidates": min_target_candidates,
                            "timebox_sec": timebox_sec,
                            "max_passes": max_passes,
                            "continue_after_target": bool(continue_after_target),
                            "passes_run": 1,
                            "stop_reason": "timed_mode_disabled",
                            "elapsed_sec": 0.0,
                            "reached_min_target": int(payload.get("created_count", 0) or 0) >= min_target_candidates,
                            "passes": [],
                        }
                    created_count = max(0, _to_int(payload.get("created_count"), 0))
                    stop_reason = str((payload.get("timed_fetch", {}) or {}).get("stop_reason", "") or "")
                    _set_fetch_progress(
                        {
                            "status": "completed",
                            "phase": "completed",
                            "message": f"探索完了: 候補 {created_count} 件",
                            "progress_percent": 100.0,
                            "created_count": created_count,
                            "stop_reason": stop_reason,
                            "pass_index": max(1, _to_int((payload.get("timed_fetch", {}) or {}).get("passes_run"), 1)),
                            "max_passes": max(1, _to_int((payload.get("timed_fetch", {}) or {}).get("max_passes"), max_passes if timed_mode else 1)),
                            "ended_at_epoch": int(time.time()),
                        }
                    )
                except Exception as err:
                    _set_fetch_progress(
                        {
                            "status": "failed",
                            "phase": "failed",
                            "message": f"探索失敗: {err}",
                            "progress_percent": 100.0,
                            "ended_at_epoch": int(time.time()),
                        }
                    )
                    raise
                self._send(HTTPStatus.OK, payload)
                return

            m = re.fullmatch(r"/v1/review/candidates/(\d+)/approve", parsed.path)
            if m:
                candidate_id = int(m.group(1))
                candidate = approve_review_candidate(candidate_id)
                self._send(HTTPStatus.OK, candidate)
                return

            m = re.fullmatch(r"/v1/review/candidates/(\d+)/reject", parsed.path)
            if m:
                candidate_id = int(m.group(1))
                issue_targets = body.get("issue_targets", [])
                if not isinstance(issue_targets, list):
                    raise ValueError("issue_targets must be an array")
                reason_text = str(body.get("reason_text", "") or "")
                candidate = reject_review_candidate(
                    candidate_id,
                    issue_targets=[str(v) for v in issue_targets],
                    reason_text=reason_text,
                )
                self._send(HTTPStatus.OK, candidate)
                return

            if parsed.path == "/v1/profit/calc":
                refresh_fx = bool(body.get("refresh_fx", False))
                force_refresh_fx = bool(body.get("force_refresh_fx", False))
                refresh_info = None
                if refresh_fx or force_refresh_fx:
                    refresh_info = maybe_refresh_usd_jpy_rate(force=force_refresh_fx)

                required = ["sale_price_usd", "purchase_price_jpy"]
                missing = [name for name in required if name not in body]
                if missing:
                    self._send(
                        HTTPStatus.BAD_REQUEST,
                        _json_error(
                            f"missing required fields: {', '.join(missing)}",
                            code="missing_fields",
                        ),
                    )
                    return

                profit_input = ProfitInput(
                    sale_price_usd=float(body["sale_price_usd"]),
                    purchase_price_jpy=float(body["purchase_price_jpy"]),
                    domestic_shipping_jpy=float(body.get("domestic_shipping_jpy", 0.0)),
                    international_shipping_usd=float(
                        body.get("international_shipping_usd", 0.0)
                    ),
                    customs_usd=float(body.get("customs_usd", 0.0)),
                    packaging_usd=float(body.get("packaging_usd", 0.0)),
                    misc_cost_jpy=float(body.get("misc_cost_jpy", 0.0)),
                    misc_cost_usd=float(body.get("misc_cost_usd", 0.0)),
                    marketplace_fee_rate=float(body.get("marketplace_fee_rate", 0.13)),
                    payment_fee_rate=float(body.get("payment_fee_rate", 0.03)),
                    fixed_fee_usd=float(body.get("fixed_fee_usd", 0.0)),
                )
                payload = calculate_profit(profit_input)
                if refresh_info is not None:
                    payload["fx_refresh"] = refresh_info
                self._send(HTTPStatus.OK, payload)
                return

            self._send(
                HTTPStatus.NOT_FOUND,
                _json_error("route not found", code="not_found"),
            )
        except ValueError as err:
            self._send(
                HTTPStatus.BAD_REQUEST,
                _json_error(str(err), code="bad_request"),
            )
        except KeyError as err:
            self._send(
                HTTPStatus.NOT_FOUND,
                _json_error(str(err), code="not_found"),
            )
        except Exception as err:  # pragma: no cover
            self._send(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                _json_error(
                    f"internal error: {err}",
                    code="internal_error",
                ),
            )
            traceback.print_exc()


def run_server(host: str = "127.0.0.1", port: int = 8000) -> None:
    server = ThreadingHTTPServer((host, port), ApiHandler)
    print(f"API server listening on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

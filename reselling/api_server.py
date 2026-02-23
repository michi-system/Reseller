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

from .coerce import to_bool as _to_bool
from .coerce import to_float as _to_float
from .coerce import to_int as _to_int
from .fx_rate import get_current_usd_jpy_snapshot, maybe_refresh_usd_jpy_rate
from .live_miner_fetch import (
    backfill_candidate_market_images,
    get_rpa_progress_snapshot,
)
from .miner_seed_pool import get_seed_pool_status, run_seeded_fetch
from .profit import ProfitInput, calculate_profit
from .miner import (
    approve_miner_candidate,
    create_miner_candidate,
    get_miner_candidate,
    list_miner_queue,
    reject_miner_candidate,
)
from listing_ops.config import load_operator_settings
from listing_ops.config_versions import create_config_version, load_or_default
from listing_ops.ingest import ingest_approved_listing_jsonl
from listing_ops.listing_cycle import run_listing_cycle
from listing_ops.manual_actions import (
    manual_mark_alert_review,
    manual_mark_listed,
    manual_resume_to_ready,
    manual_stop_listing,
)
from listing_ops.monitor_cycle import run_monitor_cycle
from listing_ops.query import get_summary as get_operator_summary
from listing_ops.query import (
    get_operator_listing,
    list_operator_events,
    list_operator_listings,
    list_operator_snapshots,
)

ROOT_DIR = Path(__file__).resolve().parents[1]
WEB_DIR = ROOT_DIR / "web"
ACTIVE_CYCLE_PATH = ROOT_DIR / "docs" / "miner_cycle_active.json"
CATEGORY_KNOWLEDGE_PATH = ROOT_DIR / "data" / "category_knowledge_seeds_v1.json"
DEFAULT_APPROVED_JSONL_PATH = ROOT_DIR / "data" / "approved_listing_exports" / "latest.jsonl"
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
        "seed_count": 0,
        "selected_seed_count": 0,
        "pool_available": 0,
        "refill_reason": "",
        "current_seed_query": "",
        "current_seed_quality_score": 0,
        "stage1_candidate_count": 0,
        "stage2_created_count": 0,
        "stage1_pass_total": 0,
        "stage2_runs": 0,
        "stage1_skip_top_reason": "",
        "stage1_skip_top_count": 0,
        "stage1_seed_baseline_reject_total": 0,
        "skipped_low_quality_count": 0,
        "select_min_seed_score": 0,
        "elapsed_sec": 0.0,
    }


_FETCH_PROGRESS_STATE: Dict[str, Any] = _default_fetch_progress()


def _json_error(message: str, *, code: str) -> Dict[str, Any]:
    return {"error": {"code": code, "message": message}}


def _resolve_local_path(value: Any, *, default_path: Path) -> Path:
    text = str(value or "").strip()
    if not text:
        return default_path
    path = Path(text)
    if path.is_absolute():
        return path
    return ROOT_DIR / path


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
    started_at = _to_int(snap.get("started_at_epoch"), 0)
    snap["updated_ago_sec"] = max(0, now_ts - updated_at) if updated_at > 0 else -1
    if str(snap.get("status", "")).lower() == "running" and started_at > 0:
        snap["elapsed_sec"] = round(max(0.0, float(now_ts - started_at)), 3)
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

    @staticmethod
    def _q_raw(query: Dict[str, List[str]], key: str, default: str = "") -> str:
        values = query.get(key, [default])
        if not values:
            return default
        return str(values[0] or "").strip()

    @classmethod
    def _q_str(cls, query: Dict[str, List[str]], key: str, default: str = "") -> str:
        raw = cls._q_raw(query, key, default)
        return raw if raw else default

    @classmethod
    def _q_int(
        cls,
        query: Dict[str, List[str]],
        key: str,
        default: int,
        *,
        strict: bool = False,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
    ) -> int:
        raw = cls._q_raw(query, key, str(default))
        if strict:
            try:
                value = int(raw)
            except (TypeError, ValueError) as err:
                raise ValueError(f"invalid integer query param: {key}") from err
        else:
            value = _to_int(raw, default)
        if min_value is not None:
            value = max(min_value, value)
        if max_value is not None:
            value = min(max_value, value)
        return value

    @classmethod
    def _q_float(
        cls,
        query: Dict[str, List[str]],
        key: str,
        default: Optional[float] = None,
        *,
        strict: bool = False,
    ) -> Optional[float]:
        raw = cls._q_raw(query, key, "")
        if not raw:
            return default
        if strict:
            try:
                return float(raw)
            except (TypeError, ValueError) as err:
                raise ValueError(f"invalid float query param: {key}") from err
        return _to_float(raw, default if default is not None else 0.0)

    @classmethod
    def _q_bool(cls, query: Dict[str, List[str]], key: str, default: bool = False) -> bool:
        raw = cls._q_raw(query, key, "")
        if not raw:
            return default
        return _to_bool(raw, default)

    @classmethod
    def _q_optional_int(cls, query: Dict[str, List[str]], key: str) -> Optional[int]:
        raw = cls._q_raw(query, key, "")
        if not raw:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _csv_ints(text: str) -> List[int]:
        out: List[int] = []
        for part in str(text or "").split(","):
            token = part.strip()
            if not token:
                continue
            try:
                value = int(token)
            except (TypeError, ValueError) as err:
                raise ValueError(f"invalid integer token in CSV list: {token}") from err
            if value <= 0:
                raise ValueError(f"CSV ids must be positive integers: {token}")
            out.append(value)
        return out

    @staticmethod
    def _path_id(path: str, route_pattern: str) -> Optional[int]:
        match = re.fullmatch(route_pattern, path)
        if not match:
            return None
        return int(match.group(1))

    def _handle_manual_operator_action(
        self,
        *,
        path: str,
        body: Dict[str, Any],
        route_pattern: str,
        action: Callable[..., Dict[str, Any]],
        default_reason_code: str,
    ) -> bool:
        match = re.fullmatch(route_pattern, path)
        if not match:
            return False
        settings = load_operator_settings()
        listing_id = int(match.group(1))
        payload = action(
            db_path=settings.db_path,
            listing_id=listing_id,
            actor_id=str(body.get("actor_id", "") or "").strip(),
            reason_code=str(body.get("reason_code", default_reason_code) or default_reason_code).strip(),
            note=str(body.get("note", "") or "").strip(),
        )
        self._send(HTTPStatus.OK, payload)
        return True

    def _send_if_static_route(self, path: str) -> bool:
        static_files = {
            "/": (WEB_DIR / "miner.html", "text/html; charset=utf-8"),
            "/miner": (WEB_DIR / "miner.html", "text/html; charset=utf-8"),
            "/operator": (WEB_DIR / "operator.html", "text/html; charset=utf-8"),
            "/static/miner.css": (WEB_DIR / "miner.css", "text/css; charset=utf-8"),
            "/static/miner.js": (WEB_DIR / "miner.js", "application/javascript; charset=utf-8"),
            "/static/operator.css": (WEB_DIR / "operator.css", "text/css; charset=utf-8"),
            "/static/operator.js": (WEB_DIR / "operator.js", "application/javascript; charset=utf-8"),
        }
        route = static_files.get(path)
        if route is None:
            return False
        file_path, content_type = route
        self._send_file(file_path, content_type=content_type)
        return True

    def _send_if_system_get(self, path: str) -> bool:
        if path == "/healthz":
            self._send(HTTPStatus.OK, {"ok": True})
            return True
        if path == "/v1/system/rpa-progress":
            self._send(HTTPStatus.OK, get_rpa_progress_snapshot())
            return True
        if path == "/v1/system/fetch-progress":
            self._send(HTTPStatus.OK, _get_fetch_progress_snapshot())
            return True
        if path == "/v1/system/fx-rate":
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
            return True
        return False

    def _query_limit_offset(self, query: Dict[str, List[str]], default_limit: int) -> tuple[int, int]:
        limit = self._q_int(query, "limit", default_limit, min_value=1)
        offset = self._q_int(query, "offset", 0, min_value=0)
        return limit, offset

    def _send_operator_records(
        self,
        *,
        query: Dict[str, List[str]],
        query_fn: Callable[..., Dict[str, Any]],
        default_limit: int = 100,
    ) -> None:
        settings = load_operator_settings()
        listing_id = self._q_optional_int(query, "listing_id")
        limit, offset = self._query_limit_offset(query, default_limit)
        payload = query_fn(
            settings.db_path,
            listing_id=listing_id,
            limit=limit,
            offset=offset,
        )
        self._send(HTTPStatus.OK, payload)

    @staticmethod
    def _body_str(body: Dict[str, Any], key: str, default: str = "") -> str:
        return str(body.get(key, default) or default).strip()

    @staticmethod
    def _body_int(
        body: Dict[str, Any],
        key: str,
        default: int,
        *,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
    ) -> int:
        value = _to_int(body.get(key, default), default)
        if min_value is not None:
            value = max(min_value, value)
        if max_value is not None:
            value = min(max_value, value)
        return value

    @staticmethod
    def _body_bool(body: Dict[str, Any], key: str, default: bool = False) -> bool:
        return _to_bool(body.get(key, default), default)

    def _handle_operator_post(self, *, path: str, body: Dict[str, Any]) -> bool:
        if path == "/v1/operator/ingest":
            settings = load_operator_settings()
            input_path = _resolve_local_path(
                body.get("input_path", ""),
                default_path=DEFAULT_APPROVED_JSONL_PATH,
            )
            payload = ingest_approved_listing_jsonl(
                db_path=settings.db_path,
                input_path=input_path,
            )
            self._send(HTTPStatus.OK, payload)
            return True

        if path == "/v1/operator/listing-cycle":
            settings = load_operator_settings()
            payload = run_listing_cycle(
                db_path=settings.db_path,
                limit=self._body_int(body, "limit", 20, min_value=1),
                dry_run=self._body_bool(body, "dry_run", True),
                actor_id=self._body_str(body, "actor_id"),
            )
            self._send(HTTPStatus.OK, payload)
            return True

        if path == "/v1/operator/monitor-cycle":
            settings = load_operator_settings()
            raw_obs = body.get("observation_jsonl_path", "")
            obs_path = None
            if str(raw_obs or "").strip():
                obs_path = _resolve_local_path(
                    raw_obs,
                    default_path=ROOT_DIR / "data" / "operator_observations.jsonl",
                )
            payload = run_monitor_cycle(
                db_path=settings.db_path,
                check_type=self._body_str(body, "check_type", "light").lower() or "light",
                observation_jsonl_path=obs_path,
                limit=self._body_int(body, "limit", 300, min_value=1),
                actor_id=self._body_str(body, "actor_id"),
            )
            self._send(HTTPStatus.OK, payload)
            return True

        manual_routes = (
            (r"/v1/operator/listings/(\d+)/manual-stop", manual_stop_listing, "manual_stop"),
            (r"/v1/operator/listings/(\d+)/manual-alert", manual_mark_alert_review, "manual_alert_review"),
            (r"/v1/operator/listings/(\d+)/manual-resume-ready", manual_resume_to_ready, "manual_resume_ready"),
            (r"/v1/operator/listings/(\d+)/manual-keep-listed", manual_mark_listed, "manual_keep_listed"),
        )
        for route_pattern, action, default_reason_code in manual_routes:
            if self._handle_manual_operator_action(
                path=path,
                body=body,
                route_pattern=route_pattern,
                action=action,
                default_reason_code=default_reason_code,
            ):
                return True

        if path == "/v1/operator/config":
            settings = load_operator_settings()
            active = load_or_default(settings.db_path)
            payload = create_config_version(
                db_path=settings.db_path,
                config_version=self._body_str(body, "config_version"),
                created_by=self._body_str(body, "created_by", settings.default_actor_id) or settings.default_actor_id,
                min_profit_jpy=_to_float(body.get("min_profit_jpy"), float(active["min_profit_jpy"])),
                min_profit_rate=_to_float(body.get("min_profit_rate"), float(active["min_profit_rate"])),
                stop_consecutive_fail_count=_to_int(
                    body.get("stop_consecutive_fail_count"),
                    int(active["stop_consecutive_fail_count"]),
                ),
                light_interval_new_hours=_to_int(
                    body.get("light_interval_new_hours"),
                    int(active["light_interval_new_hours"]),
                ),
                light_interval_stable_hours=_to_int(
                    body.get("light_interval_stable_hours"),
                    int(active["light_interval_stable_hours"]),
                ),
                light_interval_stopped_hours=_to_int(
                    body.get("light_interval_stopped_hours"),
                    int(active["light_interval_stopped_hours"]),
                ),
                heavy_interval_days=_to_int(body.get("heavy_interval_days"), int(active["heavy_interval_days"])),
            )
            self._send(HTTPStatus.OK, {"db_path": str(settings.db_path), "active_config": payload})
            return True

        return False

    def _handle_operator_get(self, *, path: str, query: Dict[str, List[str]]) -> bool:
        if path == "/v1/operator/summary":
            settings = load_operator_settings()
            payload = get_operator_summary(settings.db_path)
            payload["db_path"] = str(settings.db_path)
            self._send(HTTPStatus.OK, payload)
            return True

        if path == "/v1/operator/config":
            settings = load_operator_settings()
            payload = load_or_default(settings.db_path)
            self._send(
                HTTPStatus.OK,
                {
                    "db_path": str(settings.db_path),
                    "active_config": payload,
                },
            )
            return True

        if path == "/v1/operator/listings":
            settings = load_operator_settings()
            state = self._q_str(query, "state", "").lower()
            limit, offset = self._query_limit_offset(query, 50)
            payload = list_operator_listings(
                settings.db_path,
                listing_state=state,
                limit=limit,
                offset=offset,
            )
            self._send(HTTPStatus.OK, payload)
            return True

        listing_id = self._path_id(path, r"/v1/operator/listings/(\d+)")
        if listing_id is not None:
            settings = load_operator_settings()
            row = get_operator_listing(settings.db_path, listing_id)
            if row is None:
                self._send(
                    HTTPStatus.NOT_FOUND,
                    _json_error("listing not found", code="listing_not_found"),
                )
                return True
            self._send(HTTPStatus.OK, row)
            return True

        if path == "/v1/operator/events":
            self._send_operator_records(query=query, query_fn=list_operator_events, default_limit=100)
            return True

        if path == "/v1/operator/snapshots":
            self._send_operator_records(query=query, query_fn=list_operator_snapshots, default_limit=100)
            return True

        return False

    def _handle_miner_fetch_post(self, body: Dict[str, Any]) -> None:
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
                "selected_seed_count": 0,
                "pool_available": 0,
                "refill_reason": "",
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
                "seed_count": max(
                    0,
                    _to_int(
                        update.get(
                            "seed_count",
                            _to_int(update.get("selected_seed_count"), _to_int(update.get("pool_available"), 0)),
                        ),
                        0,
                    ),
                ),
                "selected_seed_count": max(0, _to_int(update.get("selected_seed_count"), 0)),
                "pool_available": max(0, _to_int(update.get("pool_available"), 0)),
                "refill_reason": str(update.get("refill_reason", "") or ""),
                "current_seed_query": str(update.get("current_seed_query", "") or ""),
                "current_seed_quality_score": _to_int(update.get("current_seed_quality_score"), 0),
                "stage1_candidate_count": max(0, _to_int(update.get("stage1_candidate_count"), 0)),
                "stage2_created_count": max(0, _to_int(update.get("stage2_created_count"), 0)),
                "stage1_pass_total": max(0, _to_int(update.get("stage1_pass_total"), 0)),
                "stage2_runs": max(0, _to_int(update.get("stage2_runs"), 0)),
                "stage1_skip_top_reason": str(update.get("stage1_skip_top_reason", "") or ""),
                "stage1_skip_top_count": max(0, _to_int(update.get("stage1_skip_top_count"), 0)),
                "stage1_seed_baseline_reject_total": max(
                    0, _to_int(update.get("stage1_seed_baseline_reject_total"), 0)
                ),
                "skipped_low_quality_count": max(0, _to_int(update.get("skipped_low_quality_count"), 0)),
                "select_min_seed_score": max(0, _to_int(update.get("select_min_seed_score"), 0)),
                "elapsed_sec": round(max(0.0, _to_float(update.get("elapsed_sec"), 0.0)), 3),
            }
            _set_fetch_progress(row)

        try:
            payload = run_seeded_fetch(
                category_query=query_text,
                source_sites=[str(v) for v in source_sites],
                market_site=str(body.get("market_site", "ebay") or "ebay"),
                timed_mode=bool(timed_mode),
                min_target_candidates=min_target_candidates,
                timebox_sec=timebox_sec,
                max_passes=max_passes,
                continue_after_target=continue_after_target,
                progress_callback=_progress_cb,
                **fetch_kwargs,
            )
            created_count = max(0, _to_int(payload.get("created_count"), 0))
            stop_reason = str((payload.get("timed_fetch", {}) or {}).get("stop_reason", "") or "")
            seed_pool = (payload.get("seed_pool", {}) or {}) if isinstance(payload, dict) else {}
            refill = (seed_pool.get("refill", {}) or {}) if isinstance(seed_pool, dict) else {}
            seed_count = max(
                0,
                _to_int(
                    seed_pool.get(
                        "seed_count",
                        _to_int(seed_pool.get("selected_seed_count"), _to_int(seed_pool.get("available_after_refill"), 0)),
                    ),
                    0,
                ),
            )
            _set_fetch_progress(
                {
                    "status": "completed",
                    "phase": "completed",
                    "message": f"探索完了: 候補 {created_count} 件",
                    "progress_percent": 100.0,
                    "created_count": created_count,
                    "stop_reason": stop_reason,
                    "seed_count": seed_count,
                    "selected_seed_count": max(0, _to_int(seed_pool.get("selected_seed_count"), 0)),
                    "pool_available": max(0, _to_int(seed_pool.get("available_after_refill"), 0)),
                    "refill_reason": str(refill.get("reason", "") or ""),
                    "pass_index": max(1, _to_int((payload.get("timed_fetch", {}) or {}).get("passes_run"), 1)),
                    "max_passes": max(1, _to_int((payload.get("timed_fetch", {}) or {}).get("max_passes"), max_passes if timed_mode else 1)),
                    "stage1_pass_total": max(
                        0, _to_int((payload.get("timed_fetch", {}) or {}).get("stage1_pass_total"), 0)
                    ),
                    "stage2_runs": max(
                        0, _to_int((payload.get("timed_fetch", {}) or {}).get("stage2_runs"), 0)
                    ),
                    "stage1_skip_top_reason": str(
                        (
                            sorted(
                                ((payload.get("stage1_skip_counts") or {}).items()),
                                key=lambda kv: kv[1],
                                reverse=True,
                            )[0][0]
                            if isinstance(payload.get("stage1_skip_counts"), dict)
                            and len(payload.get("stage1_skip_counts")) > 0
                            else ""
                        )
                    ),
                    "stage1_skip_top_count": max(
                        0,
                        _to_int(
                            (
                                sorted(
                                    ((payload.get("stage1_skip_counts") or {}).items()),
                                    key=lambda kv: kv[1],
                                    reverse=True,
                                )[0][1]
                                if isinstance(payload.get("stage1_skip_counts"), dict)
                                and len(payload.get("stage1_skip_counts")) > 0
                                else 0
                            ),
                            0,
                        ),
                    ),
                    "stage1_seed_baseline_reject_total": max(
                        0, _to_int((payload.get("timed_fetch", {}) or {}).get("stage1_seed_baseline_reject_total"), 0)
                    ),
                    "skipped_low_quality_count": max(0, _to_int(seed_pool.get("skipped_low_quality_count"), 0)),
                    "select_min_seed_score": max(0, _to_int(seed_pool.get("select_min_seed_score"), 0)),
                    "elapsed_sec": round(
                        max(0.0, _to_float((payload.get("timed_fetch", {}) or {}).get("elapsed_sec"), 0.0)), 3
                    ),
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

    def _handle_profit_calc_post(self, body: Dict[str, Any]) -> None:
        refresh_fx = _to_bool(body.get("refresh_fx", False), False)
        force_refresh_fx = _to_bool(body.get("force_refresh_fx", False), False)
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
            international_shipping_usd=float(body.get("international_shipping_usd", 0.0)),
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

    def _handle_miner_candidate_action_post(self, *, path: str, body: Dict[str, Any]) -> bool:
        approve_id = self._path_id(path, r"/v1/miner/candidates/(\d+)/approve")
        if approve_id is not None:
            candidate = approve_miner_candidate(approve_id)
            self._send(HTTPStatus.OK, candidate)
            return True

        reject_id = self._path_id(path, r"/v1/miner/candidates/(\d+)/reject")
        if reject_id is not None:
            issue_targets = body.get("issue_targets", [])
            if not isinstance(issue_targets, list):
                raise ValueError("issue_targets must be an array")
            reason_text = str(body.get("reason_text", "") or "")
            candidate = reject_miner_candidate(
                reject_id,
                issue_targets=[str(v) for v in issue_targets],
                reason_text=reason_text,
            )
            self._send(HTTPStatus.OK, candidate)
            return True

        return False

    def log_message(self, fmt: str, *args: Any) -> None:  # pragma: no cover
        # Keep logs concise for local usage.
        print(f"[api] {self.address_string()} - {fmt % args}")

    def do_OPTIONS(self) -> None:
        self._send(HTTPStatus.NO_CONTENT, {})

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        path = parsed.path
        if self._send_if_static_route(path):
            return
        if self._send_if_system_get(path):
            return

        miner_api_path = path

        if miner_api_path == "/v1/miner/queue":
            try:
                status = self._q_str(query, "status", "pending")
                limit = self._q_int(query, "limit", 50, strict=True, min_value=1)
                offset = self._q_int(query, "offset", 0, strict=True, min_value=0)
                min_profit_usd = self._q_float(query, "min_profit_usd", strict=True)
                min_margin_rate = self._q_float(query, "min_margin_rate", strict=True)
                min_match_score = self._q_float(query, "min_match_score", strict=True)
                condition = self._q_str(query, "condition", "") or None
                candidate_ids = None
                candidate_ids_raw = self._q_str(query, "candidate_ids", "")
                if candidate_ids_raw:
                    candidate_ids = self._csv_ints(candidate_ids_raw)
                payload = list_miner_queue(
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

        if miner_api_path == "/v1/miner/category-options":
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

        if miner_api_path == "/v1/miner/seed-pool-status":
            try:
                category = self._q_str(query, "category", "")
                if not category:
                    category = self._q_str(query, "query", "")
                payload = get_seed_pool_status(category_query=category)
                self._send(HTTPStatus.OK, payload)
            except ValueError as err:
                self._send(
                    HTTPStatus.BAD_REQUEST,
                    _json_error(str(err), code="bad_request"),
                )
            return

        candidate_id = self._path_id(miner_api_path, r"/v1/miner/candidates/(\d+)")
        if candidate_id is not None:
            candidate = get_miner_candidate(candidate_id)
            if candidate is None:
                self._send(
                    HTTPStatus.NOT_FOUND,
                    _json_error("candidate not found", code="candidate_not_found"),
                )
                return
            backfill_candidate_market_images([candidate], timeout=8, max_calls=1)
            self._send(HTTPStatus.OK, candidate)
            return

        if miner_api_path == "/v1/miner/cycle/active":
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

        if self._handle_operator_get(path=path, query=query):
            return

        self._send(
            HTTPStatus.NOT_FOUND,
            _json_error("route not found", code="not_found"),
        )

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        path = parsed.path
        miner_api_path = path

        try:
            body = self._read_json()
        except Exception as err:
            self._send(
                HTTPStatus.BAD_REQUEST,
                _json_error(f"invalid JSON body: {err}", code="invalid_json"),
            )
            return

        try:
            if path == "/v1/system/fx-rate/refresh":
                force_q = self._q_bool(query, "force", False)
                force_body = _to_bool(body.get("force", False), False)
                result = maybe_refresh_usd_jpy_rate(force=(force_q or force_body))
                self._send(HTTPStatus.OK, result)
                return

            if miner_api_path == "/v1/miner/candidates":
                candidate = create_miner_candidate(body)
                self._send(HTTPStatus.CREATED, candidate)
                return

            if miner_api_path == "/v1/miner/fetch":
                self._handle_miner_fetch_post(body)
                return

            if self._handle_miner_candidate_action_post(path=miner_api_path, body=body):
                return

            if self._handle_operator_post(path=path, body=body):
                return

            if path == "/v1/profit/calc":
                self._handle_profit_calc_post(body)
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

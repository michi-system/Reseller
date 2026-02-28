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
from .config import load_settings
from .fx_rate import get_current_usd_jpy_snapshot, maybe_refresh_usd_jpy_rate
from .live_miner_fetch import (
    backfill_candidate_market_images,
    get_rpa_progress_snapshot,
)
from .miner_seed_pool import (
    _category_stage_c_min_sold_90d,
    get_seed_pool_status,
    reset_seed_pool_category_state,
    run_seeded_fetch,
)
from .profit import ProfitInput, calculate_profit
from .models import connect, init_db
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
        "flow_stage": "",
        "flow_stage_label": "",
        "flow_stage_index": 0,
        "flow_stage_total": 0,
        "pool_threshold": 0,
        "pool_gate_passed": False,
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


_MINER_UI_SETTINGS_KEY = "miner_fetch_settings_v1"
_MINER_UI_SETTINGS_DEFAULTS: Dict[str, Any] = {
    "requireInStock": True,
    "limitPerSite": 20,
    "maxCandidates": 20,
    "stageABigWordLimit": 0,
    "stageAMinimizeTransitions": True,
    "stageBQueryMode": "seed_only",
    "stageBMaxQueriesPerSite": 1,
    "stageBTopMatchesPerSeed": 3,
    "stageBApiMaxCallsPerRun": 0,
    "stageCMinSold90d": 10,
    "stageCLiquidityRefreshEnabled": True,
    "stageCLiquidityRefreshBudget": 12,
    "stageCAllowMissingSoldSample": False,
    "stageCEbayItemDetailEnabled": True,
    "stageCEbayItemDetailMaxFetch": 30,
    "minMatchScore": 0.72,
    "minProfitUsd": 0.01,
    "minMarginRate": 0.03,
}


def _sanitize_miner_ui_settings(raw: Any) -> Dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    query_mode_raw = str(
        payload.get("stageBQueryMode", _MINER_UI_SETTINGS_DEFAULTS["stageBQueryMode"])
        or _MINER_UI_SETTINGS_DEFAULTS["stageBQueryMode"]
    ).strip().lower()
    query_mode = query_mode_raw if query_mode_raw in {"seed_only", "auto"} else "seed_only"
    return {
        "requireInStock": _to_bool(
            payload.get("requireInStock", _MINER_UI_SETTINGS_DEFAULTS["requireInStock"]),
            bool(_MINER_UI_SETTINGS_DEFAULTS["requireInStock"]),
        ),
        "limitPerSite": max(
            1, min(30, _to_int(payload.get("limitPerSite", _MINER_UI_SETTINGS_DEFAULTS["limitPerSite"]), 20))
        ),
        "maxCandidates": max(
            1, min(50, _to_int(payload.get("maxCandidates", _MINER_UI_SETTINGS_DEFAULTS["maxCandidates"]), 20))
        ),
        "stageABigWordLimit": max(
            0,
            min(
                50,
                _to_int(payload.get("stageABigWordLimit", _MINER_UI_SETTINGS_DEFAULTS["stageABigWordLimit"]), 0),
            ),
        ),
        "stageAMinimizeTransitions": _to_bool(
            payload.get("stageAMinimizeTransitions", _MINER_UI_SETTINGS_DEFAULTS["stageAMinimizeTransitions"]),
            bool(_MINER_UI_SETTINGS_DEFAULTS["stageAMinimizeTransitions"]),
        ),
        "stageBQueryMode": query_mode,
        "stageBMaxQueriesPerSite": max(
            1,
            min(
                4,
                _to_int(
                    payload.get("stageBMaxQueriesPerSite", _MINER_UI_SETTINGS_DEFAULTS["stageBMaxQueriesPerSite"]),
                    1,
                ),
            ),
        ),
        "stageBTopMatchesPerSeed": max(
            1,
            min(
                5,
                _to_int(
                    payload.get("stageBTopMatchesPerSeed", _MINER_UI_SETTINGS_DEFAULTS["stageBTopMatchesPerSeed"]),
                    3,
                ),
            ),
        ),
        "stageBApiMaxCallsPerRun": max(
            0,
            min(
                2000,
                _to_int(
                    payload.get("stageBApiMaxCallsPerRun", _MINER_UI_SETTINGS_DEFAULTS["stageBApiMaxCallsPerRun"]),
                    0,
                ),
            ),
        ),
        "stageCMinSold90d": max(
            0,
            min(1000, _to_int(payload.get("stageCMinSold90d", _MINER_UI_SETTINGS_DEFAULTS["stageCMinSold90d"]), 10)),
        ),
        "stageCLiquidityRefreshEnabled": _to_bool(
            payload.get(
                "stageCLiquidityRefreshEnabled", _MINER_UI_SETTINGS_DEFAULTS["stageCLiquidityRefreshEnabled"]
            ),
            bool(_MINER_UI_SETTINGS_DEFAULTS["stageCLiquidityRefreshEnabled"]),
        ),
        "stageCLiquidityRefreshBudget": max(
            0,
            min(
                200,
                _to_int(
                    payload.get(
                        "stageCLiquidityRefreshBudget", _MINER_UI_SETTINGS_DEFAULTS["stageCLiquidityRefreshBudget"]
                    ),
                    12,
                ),
            ),
        ),
        "stageCAllowMissingSoldSample": _to_bool(
            payload.get("stageCAllowMissingSoldSample", _MINER_UI_SETTINGS_DEFAULTS["stageCAllowMissingSoldSample"]),
            bool(_MINER_UI_SETTINGS_DEFAULTS["stageCAllowMissingSoldSample"]),
        ),
        "stageCEbayItemDetailEnabled": _to_bool(
            payload.get("stageCEbayItemDetailEnabled", _MINER_UI_SETTINGS_DEFAULTS["stageCEbayItemDetailEnabled"]),
            bool(_MINER_UI_SETTINGS_DEFAULTS["stageCEbayItemDetailEnabled"]),
        ),
        "stageCEbayItemDetailMaxFetch": max(
            0,
            min(
                500,
                _to_int(
                    payload.get("stageCEbayItemDetailMaxFetch", _MINER_UI_SETTINGS_DEFAULTS["stageCEbayItemDetailMaxFetch"]),
                    30,
                ),
            ),
        ),
        "minMatchScore": max(
            0.5,
            min(
                0.99,
                _to_float(payload.get("minMatchScore", _MINER_UI_SETTINGS_DEFAULTS["minMatchScore"]), 0.72),
            ),
        ),
        "minProfitUsd": max(
            0.0,
            min(
                999999.0,
                _to_float(payload.get("minProfitUsd", _MINER_UI_SETTINGS_DEFAULTS["minProfitUsd"]), 0.01),
            ),
        ),
        "minMarginRate": max(
            0.0,
            min(
                1.0,
                _to_float(payload.get("minMarginRate", _MINER_UI_SETTINGS_DEFAULTS["minMarginRate"]), 0.03),
            ),
        ),
    }


def _resolve_requested_stage_c_min_sold_90d(category_query: str, body: Dict[str, Any]) -> int:
    category_default = max(0, _category_stage_c_min_sold_90d(category_query, {}))
    if not isinstance(body, dict) or "stage_c_min_sold_90d" not in body:
        return category_default
    return max(0, min(1000, _to_int(body.get("stage_c_min_sold_90d", category_default), category_default)))


def _load_miner_ui_settings(db_path: Path) -> Dict[str, Any]:
    with connect(db_path) as conn:
        init_db(conn)
        row = conn.execute(
            """
            SELECT settings_json
            FROM miner_ui_settings
            WHERE settings_key = ?
            """,
            (_MINER_UI_SETTINGS_KEY,),
        ).fetchone()
    if row is None:
        return dict(_MINER_UI_SETTINGS_DEFAULTS)
    try:
        raw = json.loads(str(row["settings_json"] or "{}"))
    except Exception:
        raw = {}
    return _sanitize_miner_ui_settings(raw)


def _save_miner_ui_settings(db_path: Path, raw: Any) -> Dict[str, Any]:
    settings = _sanitize_miner_ui_settings(raw)
    encoded = json.dumps(settings, ensure_ascii=False)
    now_epoch = str(int(time.time()))
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            INSERT INTO miner_ui_settings (settings_key, settings_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(settings_key) DO UPDATE SET
                settings_json = excluded.settings_json,
                updated_at = excluded.updated_at
            """,
            (_MINER_UI_SETTINGS_KEY, encoded, now_epoch),
        )
        conn.commit()
    return settings


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
        timebox_sec = max(10, _to_int(body.get("fetch_timebox_sec", 300), 300))
        max_passes = max(1, min(40, _to_int(body.get("fetch_max_passes", 20), 20)))
        continue_after_target = _to_bool(body.get("continue_after_target", True), True)
        stage_a_big_word_limit = max(0, min(50, _to_int(body.get("stage_a_big_word_limit", 0), 0)))
        stage_a_minimize_transitions = _to_bool(body.get("stage_a_minimize_transitions", True), True)
        stage_b_query_mode_raw = str(body.get("stage_b_query_mode", "seed_only") or "seed_only").strip().lower()
        stage_b_query_mode = stage_b_query_mode_raw if stage_b_query_mode_raw in {"seed_only", "auto"} else "seed_only"
        stage_b_max_queries_per_site = max(1, min(4, _to_int(body.get("stage_b_max_queries_per_site", 1), 1)))
        stage_b_top_matches_per_seed = max(1, min(5, _to_int(body.get("stage_b_top_matches_per_seed", 3), 3)))
        stage_b_api_max_calls_per_run = max(0, min(2000, _to_int(body.get("stage_b_api_max_calls_per_run", 0), 0)))
        stage_c_min_sold_90d = _resolve_requested_stage_c_min_sold_90d(query_text, body)
        stage_c_liquidity_refresh_on_miss_enabled = _to_bool(
            body.get("stage_c_liquidity_refresh_on_miss_enabled", True), True
        )
        stage_c_liquidity_refresh_on_miss_budget = max(
            0,
            min(200, _to_int(body.get("stage_c_liquidity_refresh_on_miss_budget", 12), 12)),
        )
        stage_c_allow_missing_sold_sample = _to_bool(body.get("stage_c_allow_missing_sold_sample", False), False)
        stage_c_ebay_item_detail_enabled = _to_bool(body.get("stage_c_ebay_item_detail_enabled", True), True)
        stage_c_ebay_item_detail_max_fetch_per_run = max(
            0,
            min(500, _to_int(body.get("stage_c_ebay_item_detail_max_fetch_per_run", 30), 30)),
        )
        fetch_kwargs = {
            "limit_per_site": int(body.get("limit_per_site", 20)),
            "max_candidates": int(body.get("max_candidates", 20)),
            "stage_a_big_word_limit": stage_a_big_word_limit,
            "stage_a_minimize_transitions": stage_a_minimize_transitions,
            "stage_b_query_mode": stage_b_query_mode,
            "stage_b_max_queries_per_site": stage_b_max_queries_per_site,
            "stage_b_top_matches_per_seed": stage_b_top_matches_per_seed,
            "stage_b_api_max_calls_per_run": stage_b_api_max_calls_per_run,
            "stage_c_min_sold_90d": stage_c_min_sold_90d,
            "stage_c_liquidity_refresh_on_miss_enabled": stage_c_liquidity_refresh_on_miss_enabled,
            "stage_c_liquidity_refresh_on_miss_budget": stage_c_liquidity_refresh_on_miss_budget,
            "stage_c_allow_missing_sold_sample": stage_c_allow_missing_sold_sample,
            "stage_c_ebay_item_detail_enabled": stage_c_ebay_item_detail_enabled,
            "stage_c_ebay_item_detail_max_fetch_per_run": stage_c_ebay_item_detail_max_fetch_per_run,
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
                "flow_stage": "A",
                "flow_stage_label": "A: seed補充",
                "flow_stage_index": 1,
                "flow_stage_total": 3,
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
                "flow_stage": str(update.get("flow_stage", "") or ""),
                "flow_stage_label": str(update.get("flow_stage_label", "") or ""),
                "flow_stage_index": max(0, _to_int(update.get("flow_stage_index"), 0)),
                "flow_stage_total": max(0, _to_int(update.get("flow_stage_total"), 0)),
                "pool_threshold": max(0, _to_int(update.get("pool_threshold"), 0)),
                "pool_gate_passed": bool(_to_bool(update.get("pool_gate_passed"), False)),
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
                    "flow_stage": "C",
                    "flow_stage_label": "C: eBay最終再判定",
                    "flow_stage_index": 3,
                    "flow_stage_total": 3,
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
                    "flow_stage": "",
                    "flow_stage_label": "",
                    "flow_stage_index": 0,
                    "flow_stage_total": 3,
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

        if miner_api_path == "/v1/miner/settings":
            settings = load_settings()
            payload = _load_miner_ui_settings(settings.db_path)
            self._send(
                HTTPStatus.OK,
                {
                    "settings": payload,
                    "db_path": str(settings.db_path),
                },
            )
            return

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

            if miner_api_path == "/v1/miner/settings":
                settings = load_settings()
                payload = _save_miner_ui_settings(settings.db_path, body)
                self._send(
                    HTTPStatus.OK,
                    {
                        "settings": payload,
                        "db_path": str(settings.db_path),
                    },
                )
                return

            if miner_api_path == "/v1/miner/candidates":
                candidate = create_miner_candidate(body)
                self._send(HTTPStatus.CREATED, candidate)
                return

            if miner_api_path == "/v1/miner/fetch":
                self._handle_miner_fetch_post(body)
                return

            if miner_api_path == "/v1/miner/seed-pool-reset":
                category = str(body.get("category", "") or "").strip()
                if not category:
                    category = str(body.get("query", "") or "").strip()
                clear_pool = _to_bool(body.get("clear_pool", False), False)
                clear_history = _to_bool(body.get("clear_history", False), False)
                payload = reset_seed_pool_category_state(
                    category_query=category,
                    clear_pool=clear_pool,
                    clear_history=clear_history,
                )
                self._send(HTTPStatus.OK, payload)
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

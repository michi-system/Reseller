"""Minimal JSON API server (no external dependencies)."""

from __future__ import annotations

import json
from pathlib import Path
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import re
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from .fx_rate import get_current_usd_jpy_snapshot, maybe_refresh_usd_jpy_rate
from .live_review_fetch import fetch_live_review_candidates
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


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "ebayminer-api/0.1"

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
                payload = fetch_live_review_candidates(
                    query=query_text,
                    source_sites=[str(v) for v in source_sites],
                    market_site=str(body.get("market_site", "ebay") or "ebay"),
                    limit_per_site=int(body.get("limit_per_site", 20)),
                    max_candidates=int(body.get("max_candidates", 20)),
                    min_match_score=float(body.get("min_match_score", 0.75)),
                    min_profit_usd=float(body.get("min_profit_usd", 0.01)),
                    min_margin_rate=float(body.get("min_margin_rate", 0.03)),
                    require_in_stock=_to_bool(body.get("require_in_stock", True), True),
                    timeout=int(body.get("timeout", 18)),
                )
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

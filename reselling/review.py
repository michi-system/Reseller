"""Human review queue and feedback persistence."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .config import Settings, load_settings
from .fx_rate import get_current_usd_jpy_snapshot
from .models import connect, init_db
from .db_runtime import is_postgres_connection


VALID_STATUSES = {"pending", "approved", "rejected", "listed"}
REVIEWED_STATUSES = ("approved", "rejected", "listed")


def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _env_bool(key: str, default: bool = False) -> bool:
    raw = (os.getenv(key, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _row_to_candidate(row: Any) -> Dict[str, Any]:
    payload = dict(row)
    metadata_raw = payload.get("metadata_json", "{}")
    try:
        payload["metadata"] = json.loads(metadata_raw) if metadata_raw else {}
    except json.JSONDecodeError:
        payload["metadata"] = {"_raw": metadata_raw}
    payload.pop("metadata_json", None)
    return payload


def _parse_metadata(raw: Any) -> Dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def create_review_candidate(
    data: Dict[str, Any],
    settings: Optional[Settings] = None,
) -> Dict[str, Any]:
    settings = settings or load_settings()
    required = ["source_site", "market_site", "source_title", "market_title"]
    missing = [name for name in required if not str(data.get(name, "")).strip()]
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")

    now = _utc_iso()
    fx = get_current_usd_jpy_snapshot(settings)
    metadata = data.get("metadata", {})
    if not isinstance(metadata, dict):
        raise ValueError("metadata must be an object")

    with connect(settings.db_path) as conn:
        init_db(conn)
        insert_params = (
            str(data["source_site"]).strip(),
            str(data["market_site"]).strip(),
            str(data.get("source_item_id", "")).strip() or None,
            str(data.get("market_item_id", "")).strip() or None,
            str(data["source_title"]).strip(),
            str(data["market_title"]).strip(),
            str(data.get("condition", "new")).strip() or "new",
            str(data.get("match_level", "L2_precise")).strip() or "L2_precise",
            float(data.get("match_score", 0.0)),
            float(data.get("expected_profit_usd", 0.0)),
            float(data.get("expected_margin_rate", 0.0)),
            float(data.get("fx_rate", fx.rate)),
            str(data.get("fx_source", fx.source)),
            json.dumps(metadata, ensure_ascii=False),
            now,
            now,
        )
        if is_postgres_connection(conn):
            row = conn.execute(
                """
                INSERT INTO review_candidates (
                    source_site, market_site, source_item_id, market_item_id,
                    source_title, market_title, condition, match_level, match_score,
                    expected_profit_usd, expected_margin_rate,
                    fx_rate, fx_source, status, listing_state, metadata_json,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 'dummy_pending', ?, ?, ?)
                RETURNING id
                """,
                insert_params,
            ).fetchone()
            if row is None:
                raise RuntimeError("failed to create candidate")
            candidate_id = int(row["id"])
        else:
            cur = conn.execute(
                """
                INSERT INTO review_candidates (
                    source_site, market_site, source_item_id, market_item_id,
                    source_title, market_title, condition, match_level, match_score,
                    expected_profit_usd, expected_margin_rate,
                    fx_rate, fx_source, status, listing_state, metadata_json,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 'dummy_pending', ?, ?, ?)
                """,
                insert_params,
            )
            candidate_id = int(cur.lastrowid)
        conn.commit()
        row = conn.execute(
            "SELECT * FROM review_candidates WHERE id = ?",
            (candidate_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError("failed to create candidate")
        return _row_to_candidate(row)


def get_review_candidate(candidate_id: int, settings: Optional[Settings] = None) -> Optional[Dict[str, Any]]:
    settings = settings or load_settings()
    with connect(settings.db_path) as conn:
        init_db(conn)
        row = conn.execute(
            "SELECT * FROM review_candidates WHERE id = ?",
            (candidate_id,),
        ).fetchone()
        if row is None:
            return None
        candidate = _row_to_candidate(row)
        rej_rows = conn.execute(
            """
            SELECT id, issue_targets_json, reason_text, created_at
            FROM review_rejections
            WHERE candidate_id = ?
            ORDER BY id DESC
            """,
            (candidate_id,),
        ).fetchall()
        rejections: List[Dict[str, Any]] = []
        for rr in rej_rows:
            item = dict(rr)
            try:
                item["issue_targets"] = json.loads(item.pop("issue_targets_json", "[]"))
            except json.JSONDecodeError:
                item["issue_targets"] = []
            rejections.append(item)
        candidate["rejections"] = rejections
        return candidate


def list_review_queue(
    *,
    status: str = "pending",
    limit: int = 50,
    offset: int = 0,
    min_profit_usd: Optional[float] = None,
    min_margin_rate: Optional[float] = None,
    min_match_score: Optional[float] = None,
    condition: Optional[str] = None,
    candidate_ids: Optional[List[int]] = None,
    settings: Optional[Settings] = None,
) -> Dict[str, Any]:
    settings = settings or load_settings()
    status = (status or "pending").strip().lower()
    if status not in VALID_STATUSES and status not in {"all", "reviewed"}:
        raise ValueError(f"invalid status: {status}")
    limit = max(1, min(200, int(limit)))
    offset = max(0, int(offset))

    with connect(settings.db_path) as conn:
        init_db(conn)
        where_clauses: List[str] = []
        where_params: List[Any] = []
        if status == "reviewed":
            placeholders = ",".join("?" for _ in REVIEWED_STATUSES)
            where_clauses.append(f"status IN ({placeholders})")
            where_params.extend(REVIEWED_STATUSES)
        elif status != "all":
            where_clauses.append("status = ?")
            where_params.append(status)
        if min_profit_usd is not None:
            where_clauses.append("expected_profit_usd >= ?")
            where_params.append(float(min_profit_usd))
        if min_margin_rate is not None:
            where_clauses.append("expected_margin_rate >= ?")
            where_params.append(float(min_margin_rate))
        if min_match_score is not None:
            where_clauses.append("match_score >= ?")
            where_params.append(float(min_match_score))
        if condition is not None and str(condition).strip():
            where_clauses.append("LOWER(condition) = ?")
            where_params.append(str(condition).strip().lower())
        # 安全性のため常時厳格化:
        # pending/approved で sold_price_min_90d を採用している候補は
        # 売却サンプルURL(ebay_sold_item_url)が存在するものだけ表示する。
        if is_postgres_connection(conn):
            where_clauses.append(
                "(status NOT IN ('pending','approved') OR (metadata_json::jsonb ->> 'market_price_basis_type') = 'sold_price_min_90d')"
            )
            where_clauses.append(
                "("
                "status NOT IN ('pending','approved') "
                "OR (metadata_json::jsonb ->> 'market_price_basis_type') <> 'sold_price_min_90d' "
                "OR LENGTH(TRIM(COALESCE(metadata_json::jsonb ->> 'ebay_sold_item_url', ''))) > 0"
                ")"
            )
        else:
            where_clauses.append(
                "(status NOT IN ('pending','approved') OR json_extract(metadata_json, '$.market_price_basis_type') = 'sold_price_min_90d')"
            )
            where_clauses.append(
                "("
                "status NOT IN ('pending','approved') "
                "OR json_extract(metadata_json, '$.market_price_basis_type') <> 'sold_price_min_90d' "
                "OR LENGTH(TRIM(COALESCE(json_extract(metadata_json, '$.ebay_sold_item_url'), ''))) > 0"
                ")"
            )
        if candidate_ids:
            normalized_ids = sorted({int(v) for v in candidate_ids})
            placeholders = ",".join("?" for _ in normalized_ids)
            where_clauses.append(f"id IN ({placeholders})")
            where_params.extend(normalized_ids)

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        total = int(
            conn.execute(
                f"SELECT COUNT(*) AS c FROM review_candidates {where_sql}",
                tuple(where_params),
            ).fetchone()["c"]
        )
        rows = conn.execute(
            f"""
            SELECT * FROM review_candidates
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            tuple(where_params + [limit, offset]),
        ).fetchall()

        return {
            "status": status,
            "limit": limit,
            "offset": offset,
            "total": total,
            "filters": {
                "min_profit_usd": min_profit_usd,
                "min_margin_rate": min_margin_rate,
                "min_match_score": min_match_score,
                "condition": condition,
                "candidate_ids_count": len(candidate_ids or []),
            },
            "items": [_row_to_candidate(r) for r in rows],
        }


def approve_review_candidate(
    candidate_id: int,
    settings: Optional[Settings] = None,
) -> Dict[str, Any]:
    settings = settings or load_settings()
    now = _utc_iso()
    listing_ref = f"dummy-listing-{candidate_id}-{int(time.time())}"
    with connect(settings.db_path) as conn:
        init_db(conn)
        row = conn.execute(
            "SELECT id, status FROM review_candidates WHERE id = ?",
            (candidate_id,),
        ).fetchone()
        if row is None:
            raise KeyError("candidate_not_found")
        status = str(row["status"])
        if status == "rejected":
            raise ValueError("candidate already rejected")

        conn.execute(
            """
            UPDATE review_candidates
            SET status = 'listed',
                updated_at = ?,
                approved_at = COALESCE(approved_at, ?),
                listed_at = ?,
                listing_state = 'dummy_submitted',
                listing_reference = ?
            WHERE id = ?
            """,
            (now, now, now, listing_ref, candidate_id),
        )
        conn.commit()
    candidate = get_review_candidate(candidate_id, settings)
    if candidate is None:
        raise RuntimeError("candidate disappeared")
    candidate["listing"] = {
        "mode": "dummy",
        "state": candidate["listing_state"],
        "reference": candidate.get("listing_reference"),
    }
    return candidate


def auto_approve_review_candidate(
    candidate_id: int,
    *,
    cycle_id: str = "",
    decision_reason: str = "",
    decision_metrics: Optional[Dict[str, Any]] = None,
    settings: Optional[Settings] = None,
) -> Dict[str, Any]:
    """Mark candidate as auto-approved and keep it waiting for final human check."""

    settings = settings or load_settings()
    now = _utc_iso()
    metrics = decision_metrics if isinstance(decision_metrics, dict) else {}
    with connect(settings.db_path) as conn:
        init_db(conn)
        row = conn.execute(
            "SELECT id, status, metadata_json FROM review_candidates WHERE id = ?",
            (candidate_id,),
        ).fetchone()
        if row is None:
            raise KeyError("candidate_not_found")
        status = str(row["status"])
        if status == "listed":
            raise ValueError("candidate already listed")
        if status == "rejected":
            raise ValueError("candidate already rejected")

        metadata = _parse_metadata(row["metadata_json"])
        metadata["auto_review"] = {
            "approved": True,
            "approved_at": now,
            "cycle_id": str(cycle_id or "").strip(),
            "reason": str(decision_reason or "").strip(),
            "metrics": metrics,
        }

        conn.execute(
            """
            UPDATE review_candidates
            SET status = 'approved',
                updated_at = ?,
                approved_at = COALESCE(approved_at, ?),
                listing_state = 'dummy_pending_final_review',
                metadata_json = ?
            WHERE id = ?
            """,
            (now, now, json.dumps(metadata, ensure_ascii=False), candidate_id),
        )
        conn.commit()

    candidate = get_review_candidate(candidate_id, settings)
    if candidate is None:
        raise RuntimeError("candidate disappeared")
    return candidate


def reject_review_candidate(
    candidate_id: int,
    *,
    issue_targets: List[str],
    reason_text: str,
    settings: Optional[Settings] = None,
) -> Dict[str, Any]:
    settings = settings or load_settings()
    cleaned_targets = [str(v).strip() for v in issue_targets if str(v).strip()]
    if not cleaned_targets:
        raise ValueError("issue_targets must include at least one value")
    cleaned_reason = str(reason_text or "").strip()

    now = _utc_iso()
    with connect(settings.db_path) as conn:
        init_db(conn)
        row = conn.execute(
            "SELECT id, status FROM review_candidates WHERE id = ?",
            (candidate_id,),
        ).fetchone()
        if row is None:
            raise KeyError("candidate_not_found")
        status = str(row["status"])
        if status == "listed":
            raise ValueError("candidate already listed")

        conn.execute(
            """
            INSERT INTO review_rejections (candidate_id, issue_targets_json, reason_text, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (candidate_id, json.dumps(cleaned_targets, ensure_ascii=False), cleaned_reason, now),
        )
        conn.execute(
            """
            UPDATE review_candidates
            SET status = 'rejected',
                rejected_at = COALESCE(rejected_at, ?),
                updated_at = ?
            WHERE id = ?
            """,
            (now, now, candidate_id),
        )
        conn.commit()

    candidate = get_review_candidate(candidate_id, settings)
    if candidate is None:
        raise RuntimeError("candidate disappeared")
    return candidate

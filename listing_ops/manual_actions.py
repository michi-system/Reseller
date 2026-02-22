"""Manual listing state actions for Operator."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict

from .config import load_operator_settings
from .models import connect, init_db
from .runtime_config import ensure_and_get_active_config
from .time_utils import add_days, add_hours, parse_iso, utcnow_iso


def _to_text(value: Any) -> str:
    return str(value or "").strip()


def _light_hours_for_state(*, listing_state: str, created_at: str, now_iso: str, config: Dict[str, Any]) -> int:
    state = _to_text(listing_state).lower()
    if state == "stopped":
        return int(config["light_interval_stopped_hours"])
    created = parse_iso(created_at)
    now = parse_iso(now_iso)
    if now - created <= timedelta(hours=72):
        return int(config["light_interval_new_hours"])
    return int(config["light_interval_stable_hours"])


def _load_listing(conn, listing_id: int):
    row = conn.execute("SELECT * FROM operator_listings WHERE id = ?", (int(listing_id),)).fetchone()
    if row is None:
        raise KeyError(f"listing not found: {listing_id}")
    return row


def _insert_event(
    conn,
    *,
    listing_id: int,
    event_type: str,
    actor_id: str,
    reason_code: str,
    note: str,
    now_iso: str,
    payload: Dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO listing_events (
            listing_id,
            event_type,
            actor_type,
            actor_id,
            reason_code,
            note,
            created_at,
            payload_json
        ) VALUES (?, ?, 'human', ?, ?, ?, ?, ?)
        """,
        (
            int(listing_id),
            _to_text(event_type),
            _to_text(actor_id) or "human",
            _to_text(reason_code),
            _to_text(note),
            now_iso,
            json.dumps(payload, ensure_ascii=False),
        ),
    )


def _manual_transition(
    *,
    db_path: Path,
    listing_id: int,
    actor_id: str,
    target_state: str,
    event_type: str,
    reason_code: str,
    note: str,
) -> Dict[str, Any]:
    settings = load_operator_settings()
    actor = _to_text(actor_id) or settings.default_actor_id
    now_iso = utcnow_iso()
    target = _to_text(target_state).lower()
    if target not in {"ready", "listed", "alert_review", "stopped"}:
        raise ValueError("target_state must be one of ready/listed/alert_review/stopped")

    conn = connect(db_path)
    init_db(conn)
    try:
        config = ensure_and_get_active_config(conn, settings)
        prev = _load_listing(conn, int(listing_id))
        prev_state = _to_text(prev["listing_state"]).lower()

        if target == "ready":
            conn.execute(
                """
                UPDATE operator_listings
                SET listing_state = 'ready',
                    needs_review = 0,
                    low_profit_streak = 0,
                    low_stock_streak = 0,
                    channel_listing_id = '',
                    next_light_check_at = NULL,
                    next_heavy_check_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (now_iso, int(listing_id)),
            )
        else:
            light_hours = _light_hours_for_state(
                listing_state=target,
                created_at=_to_text(prev["created_at"]),
                now_iso=now_iso,
                config=config,
            )
            next_light = add_hours(now_iso, light_hours)
            next_heavy = add_days(now_iso, int(config["heavy_interval_days"]))
            needs_review = 1 if target == "alert_review" else 0
            conn.execute(
                """
                UPDATE operator_listings
                SET listing_state = ?,
                    needs_review = ?,
                    next_light_check_at = ?,
                    next_heavy_check_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (target, needs_review, next_light, next_heavy, now_iso, int(listing_id)),
            )

        _insert_event(
            conn,
            listing_id=int(listing_id),
            event_type=event_type,
            actor_id=actor,
            reason_code=reason_code,
            note=note,
            now_iso=now_iso,
            payload={
                "previous_state": prev_state,
                "next_state": target,
            },
        )
        conn.commit()

        updated = _load_listing(conn, int(listing_id))
        return {
            "listing": dict(updated),
            "action": {
                "event_type": event_type,
                "actor_id": actor,
                "reason_code": reason_code,
                "note": note,
                "previous_state": prev_state,
                "next_state": target,
                "executed_at": now_iso,
            },
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def manual_stop_listing(
    *,
    db_path: Path,
    listing_id: int,
    actor_id: str,
    reason_code: str = "manual_stop",
    note: str = "",
) -> Dict[str, Any]:
    return _manual_transition(
        db_path=db_path,
        listing_id=listing_id,
        actor_id=actor_id,
        target_state="stopped",
        event_type="manual_stop",
        reason_code=reason_code or "manual_stop",
        note=note or "manual stop",
    )


def manual_mark_alert_review(
    *,
    db_path: Path,
    listing_id: int,
    actor_id: str,
    reason_code: str = "manual_alert_review",
    note: str = "",
) -> Dict[str, Any]:
    return _manual_transition(
        db_path=db_path,
        listing_id=listing_id,
        actor_id=actor_id,
        target_state="alert_review",
        event_type="manual_alert_review",
        reason_code=reason_code or "manual_alert_review",
        note=note or "manual set alert review",
    )


def manual_resume_to_ready(
    *,
    db_path: Path,
    listing_id: int,
    actor_id: str,
    reason_code: str = "manual_resume_ready",
    note: str = "",
) -> Dict[str, Any]:
    return _manual_transition(
        db_path=db_path,
        listing_id=listing_id,
        actor_id=actor_id,
        target_state="ready",
        event_type="manual_resume_ready",
        reason_code=reason_code or "manual_resume_ready",
        note=note or "manual move to ready",
    )


def manual_mark_listed(
    *,
    db_path: Path,
    listing_id: int,
    actor_id: str,
    reason_code: str = "manual_keep_listed",
    note: str = "",
) -> Dict[str, Any]:
    return _manual_transition(
        db_path=db_path,
        listing_id=listing_id,
        actor_id=actor_id,
        target_state="listed",
        event_type="manual_keep_listed",
        reason_code=reason_code or "manual_keep_listed",
        note=note or "manual set listed",
    )

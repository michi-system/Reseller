"""Monitoring cycle for Operator listings."""

from __future__ import annotations

import json
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict

from .config import load_operator_settings
from .judge import evaluate, judge_input_from_listing
from .models import connect, finish_job_run, init_db, insert_job_run
from .runtime_config import ensure_and_get_active_config
from .time_utils import add_days, add_hours, parse_iso, utcnow_iso


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_bool(value: Any, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "y", "on"}


def _load_observations(path: Path | None) -> Dict[str, Dict[str, Any]]:
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"observation file not found: {path}")
    by_listing_id: Dict[str, Dict[str, Any]] = {}
    by_approved_id: Dict[str, Dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            record = json.loads(raw)
            if not isinstance(record, dict):
                continue
            listing_id = str(record.get("listing_id") or "").strip()
            approved_id = str(record.get("approved_id") or "").strip()
            if listing_id:
                by_listing_id[listing_id] = record
            if approved_id:
                by_approved_id[approved_id] = record
    return {"by_listing_id": by_listing_id, "by_approved_id": by_approved_id}


def _next_light_hours(*, listing_state: str, created_at: str, now_iso: str, config: Dict[str, Any]) -> int:
    if listing_state == "stopped":
        return int(config["light_interval_stopped_hours"])
    created = parse_iso(created_at)
    now = parse_iso(now_iso)
    if now - created <= timedelta(hours=72):
        return int(config["light_interval_new_hours"])
    return int(config["light_interval_stable_hours"])


def run_monitor_cycle(
    *,
    db_path: Path,
    check_type: str = "light",
    observation_jsonl_path: Path | None = None,
    limit: int = 300,
    actor_id: str = "",
) -> Dict[str, Any]:
    check = str(check_type or "light").strip().lower()
    if check not in {"light", "heavy"}:
        raise ValueError("check_type must be 'light' or 'heavy'")

    settings = load_operator_settings()
    actor = actor_id.strip() or settings.default_actor_id
    run_id = f"monitor_{check}_{uuid.uuid4().hex[:12]}"
    started_at = utcnow_iso()
    observations = _load_observations(observation_jsonl_path)

    conn = connect(db_path)
    init_db(conn)
    insert_job_run(conn, run_id, f"monitor_cycle_{check}", started_at)

    processed = 0
    success = 0
    errors = 0
    keep_count = 0
    alert_count = 0
    stop_count = 0
    skip_count = 0
    error_messages: list[str] = []

    try:
        config = ensure_and_get_active_config(conn, settings)
        now_iso = utcnow_iso()
        due_field = "next_light_check_at" if check == "light" else "next_heavy_check_at"
        query = f"""
            SELECT *
            FROM operator_listings
            WHERE listing_state IN ('listed', 'alert_review', 'stopped')
              AND ({due_field} IS NULL OR {due_field} <= ?)
            ORDER BY updated_at ASC, id ASC
            LIMIT ?
        """
        rows = conn.execute(query, (now_iso, max(1, int(limit)))).fetchall()

        by_listing_id = observations.get("by_listing_id", {})
        by_approved_id = observations.get("by_approved_id", {})
        for row in rows:
            processed += 1
            listing_id = int(row["id"])
            try:
                obs = by_listing_id.get(str(listing_id)) or by_approved_id.get(str(row["approved_id"]))
                if not obs:
                    light_hours = _next_light_hours(
                        listing_state=str(row["listing_state"]),
                        created_at=str(row["created_at"]),
                        now_iso=now_iso,
                        config=config,
                    )
                    if check == "light":
                        conn.execute(
                            """
                            UPDATE operator_listings
                            SET next_light_check_at = ?,
                                last_light_checked_at = ?,
                                updated_at = ?
                            WHERE id = ?
                            """,
                            (add_hours(now_iso, light_hours), now_iso, now_iso, listing_id),
                        )
                    else:
                        conn.execute(
                            """
                            UPDATE operator_listings
                            SET next_heavy_check_at = ?,
                                last_heavy_checked_at = ?,
                                updated_at = ?
                            WHERE id = ?
                            """,
                            (add_days(now_iso, int(config["heavy_interval_days"])), now_iso, now_iso, listing_id),
                        )
                    skip_count += 1
                    continue

                source_price_jpy = _to_float(
                    obs.get("source_price_jpy"),
                    _to_float(row["current_source_price_jpy"], _to_float(row["source_price_jpy"], 0.0)),
                )
                target_price_usd = _to_float(
                    obs.get("target_price_usd"),
                    _to_float(row["current_target_price_usd"], _to_float(row["target_price_usd"], 0.0)),
                )
                fx_rate = _to_float(obs.get("fx_rate"), _to_float(row["current_fx_rate"], _to_float(row["fx_rate"], 0.0)))
                source_in_stock = _to_bool(obs.get("source_in_stock"), bool(row["source_in_stock"]))
                heavy_price_drop = _to_bool(obs.get("heavy_price_drop"), False)

                judge_input = judge_input_from_listing(
                    dict(row),
                    source_price_jpy=source_price_jpy,
                    target_price_usd=target_price_usd,
                    fx_rate=fx_rate,
                    source_in_stock=source_in_stock,
                    min_profit_jpy=_to_float(config["min_profit_jpy"], settings.min_profit_jpy),
                    min_profit_rate=_to_float(config["min_profit_rate"], settings.min_profit_rate),
                    stop_consecutive_fail_count=int(config["stop_consecutive_fail_count"]),
                    heavy_price_drop=heavy_price_drop if check == "heavy" else False,
                )
                result = evaluate(judge_input)

                prev_state = str(row["listing_state"])
                if prev_state == "stopped" and result.decision == "keep":
                    # Never auto-resume in MVP; instead raise manual review flag.
                    new_state = "stopped"
                    decision = "alert_review"
                    reason_code = "restart_candidate_detected"
                    needs_review = 1
                else:
                    decision = result.decision
                    reason_code = result.reason_code
                    if decision == "stop":
                        new_state = "stopped"
                        needs_review = 0
                    elif decision == "alert_review":
                        new_state = "alert_review"
                        needs_review = 1
                    else:
                        new_state = "listed"
                        needs_review = 0

                light_hours = _next_light_hours(
                    listing_state=new_state,
                    created_at=str(row["created_at"]),
                    now_iso=now_iso,
                    config=config,
                )
                next_light = add_hours(now_iso, light_hours)
                next_heavy = add_days(now_iso, int(config["heavy_interval_days"]))

                conn.execute(
                    """
                    UPDATE operator_listings
                    SET listing_state = ?,
                        current_source_price_jpy = ?,
                        current_target_price_usd = ?,
                        current_fx_rate = ?,
                        current_profit_jpy = ?,
                        current_profit_rate = ?,
                        source_in_stock = ?,
                        low_profit_streak = ?,
                        low_stock_streak = ?,
                        needs_review = ?,
                        next_light_check_at = ?,
                        next_heavy_check_at = ?,
                        last_light_checked_at = CASE WHEN ? = 'light' THEN ? ELSE last_light_checked_at END,
                        last_heavy_checked_at = CASE WHEN ? = 'heavy' THEN ? ELSE last_heavy_checked_at END,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        new_state,
                        source_price_jpy,
                        target_price_usd,
                        fx_rate,
                        result.profit_jpy,
                        result.profit_rate,
                        1 if source_in_stock else 0,
                        result.next_low_profit_streak,
                        result.next_low_stock_streak,
                        needs_review,
                        next_light,
                        next_heavy,
                        check,
                        now_iso,
                        check,
                        now_iso,
                        now_iso,
                        listing_id,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO monitor_snapshots (
                        listing_id,
                        check_type,
                        source_price_jpy,
                        target_price_usd,
                        fx_rate,
                        source_in_stock,
                        profit_jpy,
                        profit_rate,
                        decision,
                        reason_code,
                        captured_at,
                        payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        listing_id,
                        check,
                        source_price_jpy,
                        target_price_usd,
                        fx_rate,
                        1 if source_in_stock else 0,
                        result.profit_jpy,
                        result.profit_rate,
                        decision,
                        reason_code,
                        now_iso,
                        json.dumps(obs, ensure_ascii=False),
                    ),
                )

                event_type = ""
                if prev_state != new_state:
                    if new_state == "stopped":
                        event_type = "auto_stop"
                    elif new_state == "alert_review":
                        event_type = "alert_review"
                    elif new_state == "listed":
                        event_type = "back_to_listed"
                elif reason_code == "restart_candidate_detected":
                    event_type = "restart_candidate"
                if event_type:
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
                        ) VALUES (?, ?, 'system', ?, ?, ?, ?, ?)
                        """,
                        (
                            listing_id,
                            event_type,
                            actor,
                            reason_code,
                            f"{check} monitor decision={decision}",
                            now_iso,
                            json.dumps({"run_id": run_id, "previous_state": prev_state}, ensure_ascii=False),
                        ),
                    )

                if decision == "keep":
                    keep_count += 1
                elif decision == "stop":
                    stop_count += 1
                else:
                    alert_count += 1
                success += 1
            except Exception as exc:  # noqa: BLE001
                errors += 1
                if len(error_messages) < 20:
                    error_messages.append(f"id={listing_id}: {exc}")

        conn.commit()
        status = "success" if errors == 0 else "partial_success"
        finish_job_run(
            conn,
            run_id=run_id,
            finished_at=utcnow_iso(),
            status=status,
            processed_count=processed,
            success_count=success,
            error_count=errors,
            error_summary="; ".join(error_messages),
        )
        return {
            "run_id": run_id,
            "status": status,
            "check_type": check,
            "processed_count": processed,
            "success_count": success,
            "error_count": errors,
            "skip_count": skip_count,
            "keep_count": keep_count,
            "alert_count": alert_count,
            "stop_count": stop_count,
            "db_path": str(db_path),
            "observation_jsonl_path": str(observation_jsonl_path) if observation_jsonl_path else "",
        }
    except Exception:
        conn.rollback()
        finish_job_run(
            conn,
            run_id=run_id,
            finished_at=utcnow_iso(),
            status="failed",
            processed_count=processed,
            success_count=success,
            error_count=errors + 1,
            error_summary="runtime failure",
        )
        raise
    finally:
        conn.close()

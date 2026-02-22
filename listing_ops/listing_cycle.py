"""Listing execution cycle."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Dict

from .config import load_operator_settings
from .models import connect, finish_job_run, init_db, insert_job_run
from .runtime_config import ensure_and_get_active_config
from .time_utils import add_days, add_hours, utcnow_iso


def run_listing_cycle(
    *,
    db_path: Path,
    limit: int = 20,
    dry_run: bool = True,
    actor_id: str = "",
) -> Dict[str, Any]:
    settings = load_operator_settings()
    if actor_id.strip():
        actor = actor_id.strip()
    else:
        actor = settings.default_actor_id

    run_id = f"listing_{uuid.uuid4().hex[:12]}"
    started_at = utcnow_iso()
    conn = connect(db_path)
    init_db(conn)
    insert_job_run(conn, run_id, "listing_cycle", started_at)
    processed = 0
    success = 0
    errors = 0
    listed_ids: list[int] = []
    error_messages: list[str] = []

    try:
        config = ensure_and_get_active_config(conn, settings)
        rows = conn.execute(
            """
            SELECT *
            FROM operator_listings
            WHERE listing_state = 'ready'
              AND needs_review = 0
            ORDER BY id ASC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()

        for row in rows:
            processed += 1
            now_iso = utcnow_iso()
            try:
                listing_id = int(row["id"])
                external_listing_id = (
                    f"dry_{listing_id}_{uuid.uuid4().hex[:8]}"
                    if dry_run
                    else f"live_{listing_id}_{uuid.uuid4().hex[:8]}"
                )
                next_light = add_hours(now_iso, int(config["light_interval_new_hours"]))
                next_heavy = add_days(now_iso, int(config["heavy_interval_days"]))

                conn.execute(
                    """
                    UPDATE operator_listings
                    SET listing_state = 'listed',
                        channel = 'ebay',
                        channel_listing_id = ?,
                        next_light_check_at = ?,
                        next_heavy_check_at = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (external_listing_id, next_light, next_heavy, now_iso, listing_id),
                )
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
                    ) VALUES (?, ?, 'system', ?, '', ?, ?, ?)
                    """,
                    (
                        listing_id,
                        "listed_dry_run" if dry_run else "listed_live",
                        actor,
                        "Dry-run listing publication"
                        if dry_run
                        else "Live listing publication",
                        now_iso,
                        json.dumps(
                            {
                                "run_id": run_id,
                                "channel_listing_id": external_listing_id,
                                "dry_run": dry_run,
                            },
                            ensure_ascii=False,
                        ),
                    ),
                )
                listed_ids.append(listing_id)
                success += 1
            except Exception as exc:  # noqa: BLE001
                errors += 1
                if len(error_messages) < 10:
                    error_messages.append(f"id={row['id']}: {exc}")

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
            "processed_count": processed,
            "listed_count": success,
            "error_count": errors,
            "listed_ids": listed_ids,
            "dry_run": dry_run,
            "db_path": str(db_path),
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

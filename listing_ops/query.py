"""Read-model queries for Operator."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

from .models import connect, init_db


def get_summary(db_path: Path) -> Dict[str, Any]:
    conn = connect(db_path)
    init_db(conn)
    counts = dict(
        conn.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN listing_state = 'ready' THEN 1 ELSE 0 END), 0) AS ready_count,
                COALESCE(SUM(CASE WHEN listing_state = 'listed' THEN 1 ELSE 0 END), 0) AS listed_count,
                COALESCE(SUM(CASE WHEN listing_state = 'alert_review' THEN 1 ELSE 0 END), 0) AS alert_review_count,
                COALESCE(SUM(CASE WHEN listing_state = 'stopped' THEN 1 ELSE 0 END), 0) AS stopped_count,
                COUNT(*) AS total_count
            FROM operator_listings
            """
        ).fetchone()
    )
    latest_jobs = [
        dict(row)
        for row in conn.execute(
            """
            SELECT run_id, job_type, status, started_at, finished_at, processed_count, success_count, error_count
            FROM job_runs
            ORDER BY started_at DESC
            LIMIT 20
            """
        ).fetchall()
    ]
    conn.close()
    return {"listing_counts": counts, "latest_jobs": latest_jobs}


def list_operator_listings(
    db_path: Path,
    *,
    listing_state: str = "",
    limit: int = 50,
    offset: int = 0,
) -> Dict[str, Any]:
    state = str(listing_state or "").strip().lower()
    conn = connect(db_path)
    init_db(conn)

    where_sql = ""
    params: list[Any] = []
    if state:
        where_sql = "WHERE listing_state = ?"
        params.append(state)

    total = int(conn.execute(f"SELECT COUNT(*) AS c FROM operator_listings {where_sql}", params).fetchone()["c"])
    rows = conn.execute(
        f"""
        SELECT *
        FROM operator_listings
        {where_sql}
        ORDER BY updated_at DESC, id DESC
        LIMIT ? OFFSET ?
        """,
        [*params, max(1, int(limit)), max(0, int(offset))],
    ).fetchall()
    items = [dict(row) for row in rows]
    conn.close()
    return {"items": items, "total_count": total, "limit": max(1, int(limit)), "offset": max(0, int(offset))}


def list_operator_events(
    db_path: Path,
    *,
    listing_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
) -> Dict[str, Any]:
    conn = connect(db_path)
    init_db(conn)
    where_sql = ""
    params: list[Any] = []
    if listing_id is not None:
        where_sql = "WHERE listing_id = ?"
        params.append(int(listing_id))

    total = int(conn.execute(f"SELECT COUNT(*) AS c FROM listing_events {where_sql}", params).fetchone()["c"])
    rows = conn.execute(
        f"""
        SELECT *
        FROM listing_events
        {where_sql}
        ORDER BY created_at DESC, id DESC
        LIMIT ? OFFSET ?
        """,
        [*params, max(1, int(limit)), max(0, int(offset))],
    ).fetchall()
    items = [dict(row) for row in rows]
    conn.close()
    return {"items": items, "total_count": total, "limit": max(1, int(limit)), "offset": max(0, int(offset))}


def get_operator_listing(db_path: Path, listing_id: int) -> Optional[Dict[str, Any]]:
    conn = connect(db_path)
    init_db(conn)
    row = conn.execute("SELECT * FROM operator_listings WHERE id = ?", (int(listing_id),)).fetchone()
    conn.close()
    if row is None:
        return None
    return dict(row)


def list_operator_snapshots(
    db_path: Path,
    *,
    listing_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
) -> Dict[str, Any]:
    conn = connect(db_path)
    init_db(conn)
    where_sql = ""
    params: list[Any] = []
    if listing_id is not None:
        where_sql = "WHERE listing_id = ?"
        params.append(int(listing_id))

    total = int(conn.execute(f"SELECT COUNT(*) AS c FROM monitor_snapshots {where_sql}", params).fetchone()["c"])
    rows = conn.execute(
        f"""
        SELECT *
        FROM monitor_snapshots
        {where_sql}
        ORDER BY captured_at DESC, id DESC
        LIMIT ? OFFSET ?
        """,
        [*params, max(1, int(limit)), max(0, int(offset))],
    ).fetchall()
    items = [dict(row) for row in rows]
    conn.close()
    return {"items": items, "total_count": total, "limit": max(1, int(limit)), "offset": max(0, int(offset))}

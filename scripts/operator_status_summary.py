#!/usr/bin/env python3
"""Print Operator DB status summary."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from listing_ops.config import load_operator_settings
from listing_ops.models import connect, init_db


def main() -> int:
    settings = load_operator_settings()
    conn = connect(settings.db_path)
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
            LIMIT 10
            """
        ).fetchall()
    ]
    conn.close()
    print(
        json.dumps(
            {
                "db_path": str(settings.db_path),
                "listing_counts": counts,
                "latest_jobs": latest_jobs,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

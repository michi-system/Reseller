from __future__ import annotations

import json

import pytest

from listing_ops.ingest import ingest_approved_listing_jsonl
from listing_ops.listing_cycle import run_listing_cycle
from listing_ops.manual_actions import (
    manual_mark_alert_review,
    manual_mark_listed,
    manual_resume_to_ready,
    manual_stop_listing,
)
from listing_ops.models import connect, init_db


def _write_jsonl(path, records) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(r, ensure_ascii=False) for r in records]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _listing_row(db_path):
    conn = connect(db_path)
    init_db(conn)
    row = conn.execute("SELECT * FROM operator_listings ORDER BY id ASC LIMIT 1").fetchone()
    assert row is not None
    conn.close()
    return row


def _event_types(db_path):
    conn = connect(db_path)
    init_db(conn)
    rows = conn.execute("SELECT event_type FROM listing_events ORDER BY id ASC").fetchall()
    conn.close()
    return [str(r["event_type"]) for r in rows]


def test_manual_state_transitions_work_end_to_end(tmp_path) -> None:
    db_path = tmp_path / "operator.db"
    approved_path = tmp_path / "approved.jsonl"
    _write_jsonl(
        approved_path,
        [
            {
                "approved_id": "apr_manual_1",
                "approved_at": "2026-02-22T10:00:00+09:00",
                "approved_by": "tester",
                "sku_key": "CASIO_GWM5610",
                "title": "Casio GW-M5610",
                "brand": "Casio",
                "model": "GW-M5610",
                "source_market": "rakuten",
                "source_price_jpy": 21000,
                "target_market": "ebay",
                "target_price_usd": 165,
                "fx_rate": 150,
                "estimated_profit_jpy": 4000,
                "estimated_profit_rate": 0.19,
                "risk_flags": [],
                "listing_status": "ready",
            }
        ],
    )

    ingest_approved_listing_jsonl(db_path=db_path, input_path=approved_path)
    run_listing_cycle(db_path=db_path, limit=10, dry_run=True, actor_id="tester")

    listing_id = int(_listing_row(db_path)["id"])

    stopped = manual_stop_listing(db_path=db_path, listing_id=listing_id, actor_id="michi-system")
    assert stopped["listing"]["listing_state"] == "stopped"
    assert int(stopped["listing"]["needs_review"]) == 0

    ready = manual_resume_to_ready(db_path=db_path, listing_id=listing_id, actor_id="michi-system")
    assert ready["listing"]["listing_state"] == "ready"
    assert ready["listing"]["channel_listing_id"] == ""
    assert ready["listing"]["next_light_check_at"] is None
    assert ready["listing"]["next_heavy_check_at"] is None

    listed = manual_mark_listed(db_path=db_path, listing_id=listing_id, actor_id="michi-system")
    assert listed["listing"]["listing_state"] == "listed"
    assert int(listed["listing"]["needs_review"]) == 0
    assert listed["listing"]["next_light_check_at"]
    assert listed["listing"]["next_heavy_check_at"]

    alert = manual_mark_alert_review(db_path=db_path, listing_id=listing_id, actor_id="michi-system")
    assert alert["listing"]["listing_state"] == "alert_review"
    assert int(alert["listing"]["needs_review"]) == 1

    events = _event_types(db_path)
    assert "listed_dry_run" in events
    assert "manual_stop" in events
    assert "manual_resume_ready" in events
    assert "manual_keep_listed" in events
    assert "manual_alert_review" in events


def test_manual_action_raises_when_listing_not_found(tmp_path) -> None:
    db_path = tmp_path / "operator.db"
    conn = connect(db_path)
    init_db(conn)
    conn.close()

    with pytest.raises(KeyError):
        manual_stop_listing(db_path=db_path, listing_id=99999, actor_id="michi-system")

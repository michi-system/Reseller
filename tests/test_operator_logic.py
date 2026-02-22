from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

from listing_ops.ingest import ingest_approved_listing_jsonl
from listing_ops.listing_cycle import run_listing_cycle
from listing_ops.monitor_cycle import run_monitor_cycle
from listing_ops.models import connect, init_db


def _iso_past(hours: int = 24) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat(timespec="seconds")


def _write_jsonl(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(r, ensure_ascii=False) for r in records]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _get_listing(conn: sqlite3.Connection):
    row = conn.execute("SELECT * FROM operator_listings LIMIT 1").fetchone()
    assert row is not None
    return row


def test_operator_cycle_stops_on_consecutive_low_profit(tmp_path) -> None:
    db_path = tmp_path / "operator.db"
    approved_path = tmp_path / "approved.jsonl"
    _write_jsonl(
        approved_path,
        [
            {
                "approved_id": "apr_1",
                "approved_at": "2026-02-22T10:00:00+09:00",
                "approved_by": "tad",
                "sku_key": "CASIO_GWM5610",
                "title": "Casio GW-M5610",
                "brand": "Casio",
                "model": "GW-M5610",
                "source_market": "rakuten",
                "source_price_jpy": 20000,
                "target_market": "ebay",
                "target_price_usd": 160,
                "fx_rate": 150,
                "estimated_profit_jpy": 4000,
                "estimated_profit_rate": 0.2,
                "risk_flags": [],
                "listing_status": "ready",
            }
        ],
    )

    ingest = ingest_approved_listing_jsonl(db_path=db_path, input_path=approved_path)
    assert ingest["inserted_listing_count"] == 1

    listed = run_listing_cycle(db_path=db_path, limit=10, dry_run=True, actor_id="tester")
    assert listed["listed_count"] == 1

    conn = connect(db_path)
    init_db(conn)
    listing = _get_listing(conn)
    listing_id = int(listing["id"])
    conn.execute("UPDATE operator_listings SET next_light_check_at = ? WHERE id = ?", (_iso_past(), listing_id))
    conn.commit()
    conn.close()

    obs1 = tmp_path / "obs1.jsonl"
    _write_jsonl(
        obs1,
        [
            {
                "listing_id": listing_id,
                "source_price_jpy": 22000,
                "target_price_usd": 130,
                "fx_rate": 150,
                "source_in_stock": True,
            }
        ],
    )
    first = run_monitor_cycle(db_path=db_path, check_type="light", observation_jsonl_path=obs1, actor_id="tester")
    assert first["alert_count"] == 1

    conn = connect(db_path)
    listing = _get_listing(conn)
    assert listing["listing_state"] == "alert_review"
    conn.execute("UPDATE operator_listings SET next_light_check_at = ? WHERE id = ?", (_iso_past(), listing_id))
    conn.commit()
    conn.close()

    obs2 = tmp_path / "obs2.jsonl"
    _write_jsonl(
        obs2,
        [
            {
                "listing_id": listing_id,
                "source_price_jpy": 22500,
                "target_price_usd": 120,
                "fx_rate": 150,
                "source_in_stock": True,
            }
        ],
    )
    second = run_monitor_cycle(db_path=db_path, check_type="light", observation_jsonl_path=obs2, actor_id="tester")
    assert second["stop_count"] == 1

    conn = connect(db_path)
    listing = _get_listing(conn)
    assert listing["listing_state"] == "stopped"
    assert int(listing["needs_review"]) == 0
    conn.close()


def test_stopped_listing_becomes_restart_candidate_on_heavy_check(tmp_path) -> None:
    db_path = tmp_path / "operator.db"
    approved_path = tmp_path / "approved.jsonl"
    _write_jsonl(
        approved_path,
        [
            {
                "approved_id": "apr_2",
                "approved_at": "2026-02-22T10:00:00+09:00",
                "approved_by": "tad",
                "sku_key": "SEIKO_SBDC101",
                "title": "Seiko SBDC101",
                "brand": "Seiko",
                "model": "SBDC101",
                "source_market": "rakuten",
                "source_price_jpy": 90000,
                "target_market": "ebay",
                "target_price_usd": 900,
                "fx_rate": 150,
                "estimated_profit_jpy": 20000,
                "estimated_profit_rate": 0.2,
                "risk_flags": [],
                "listing_status": "ready",
            }
        ],
    )
    ingest_approved_listing_jsonl(db_path=db_path, input_path=approved_path)
    run_listing_cycle(db_path=db_path, limit=10, dry_run=True, actor_id="tester")

    conn = connect(db_path)
    listing = _get_listing(conn)
    listing_id = int(listing["id"])
    conn.execute(
        """
        UPDATE operator_listings
        SET listing_state = 'stopped',
            low_profit_streak = 2,
            next_heavy_check_at = ?
        WHERE id = ?
        """,
        (_iso_past(), listing_id),
    )
    conn.commit()
    conn.close()

    obs = tmp_path / "obs_heavy.jsonl"
    _write_jsonl(
        obs,
        [
            {
                "listing_id": listing_id,
                "source_price_jpy": 80000,
                "target_price_usd": 900,
                "fx_rate": 150,
                "source_in_stock": True,
                "heavy_price_drop": False,
            }
        ],
    )
    result = run_monitor_cycle(db_path=db_path, check_type="heavy", observation_jsonl_path=obs, actor_id="tester")
    assert result["alert_count"] == 1

    conn = connect(db_path)
    listing = _get_listing(conn)
    assert listing["listing_state"] == "stopped"
    assert int(listing["needs_review"]) == 1
    conn.close()

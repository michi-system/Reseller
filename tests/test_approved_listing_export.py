from __future__ import annotations

import json
import sqlite3

from reselling.approved_export import REQUIRED_FIELDS, export_approved_listing_jsonl
from reselling.models import init_db


def _insert_candidate(
    conn: sqlite3.Connection,
    *,
    status: str,
    approved_at: str,
    metadata: dict,
) -> None:
    conn.execute(
        """
        INSERT INTO miner_candidates (
            source_site,
            market_site,
            source_item_id,
            market_item_id,
            source_title,
            market_title,
            expected_profit_usd,
            expected_margin_rate,
            fx_rate,
            status,
            metadata_json,
            created_at,
            updated_at,
            approved_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "rakuten",
            "ebay",
            "src-1",
            "mkt-1",
            "Casio GW-M5610U-1JF",
            "Casio G-SHOCK GW-M5610U-1JF",
            34.0,
            0.257,
            150.0,
            status,
            json.dumps(metadata, ensure_ascii=False),
            "2026-02-22T10:00:00+09:00",
            "2026-02-22T10:00:00+09:00",
            approved_at,
        ),
    )
    conn.commit()


def test_export_approved_jsonl_with_required_contract_fields(tmp_path) -> None:
    db_path = tmp_path / "reseller.db"
    output_path = tmp_path / "data" / "approved_listing_exports" / "latest.jsonl"

    conn = sqlite3.connect(str(db_path))
    init_db(conn)
    _insert_candidate(
        conn,
        status="approved",
        approved_at="2026-02-22T10:35:00+09:00",
        metadata={
            "approved_by": "qa_user",
            "source_identifiers": {"brand": "Casio", "model": "GW-M5610U-1JF"},
            "source_price_basis_jpy": 14980,
            "market_price_basis_usd": 179.0,
            "calc_breakdown": {"profit_usd": 34.0, "variable_fee_usd": 18.5},
            "risk_flags": ["manual_check_required"],
            "category_hint": "watches",
            "market_image_url": "https://example.com/item.jpg",
            "notes": "箱あり",
            "source_shipping_basis_jpy": 900,
        },
    )
    conn.close()

    summary = export_approved_listing_jsonl(db_path=db_path, output_path=output_path)
    assert summary["exported_count"] == 1
    assert output_path.exists()

    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])

    for key in REQUIRED_FIELDS:
        assert key in record

    assert isinstance(record["approved_id"], str) and record["approved_id"]
    assert isinstance(record["approved_at"], str) and record["approved_at"]
    assert isinstance(record["approved_by"], str) and record["approved_by"]
    assert isinstance(record["sku_key"], str) and record["sku_key"]
    assert isinstance(record["title"], str) and record["title"]
    assert isinstance(record["brand"], str) and record["brand"]
    assert isinstance(record["model"], str) and record["model"]
    assert isinstance(record["source_market"], str) and record["source_market"]
    assert isinstance(record["source_price_jpy"], (int, float))
    assert isinstance(record["target_market"], str) and record["target_market"]
    assert isinstance(record["target_price_usd"], (int, float))
    assert isinstance(record["fx_rate"], (int, float))
    assert isinstance(record["estimated_profit_jpy"], (int, float))
    assert isinstance(record["estimated_profit_rate"], (int, float))
    assert isinstance(record["risk_flags"], list)
    assert isinstance(record["listing_status"], str) and record["listing_status"]

    assert record["approved_by"] == "qa_user"
    assert record["sku_key"] == "GW_M5610U_1JF"
    assert record["listing_status"] == "ready"
    assert record["estimated_profit_jpy"] == 5100.0


def test_export_includes_only_status_approved_or_listed_with_approved_at(tmp_path) -> None:
    db_path = tmp_path / "reseller.db"
    output_path = tmp_path / "data" / "approved_listing_exports" / "latest.jsonl"

    conn = sqlite3.connect(str(db_path))
    init_db(conn)

    _insert_candidate(
        conn,
        status="pending",
        approved_at="2026-02-22T10:35:00+09:00",
        metadata={"source_identifiers": {"brand": "Casio", "model": "GW-M5610U-1JF"}},
    )
    _insert_candidate(
        conn,
        status="listed",
        approved_at="2026-02-22T11:35:00+09:00",
        metadata={"source_identifiers": {"brand": "Seiko", "model": "SBDC101"}},
    )
    _insert_candidate(
        conn,
        status="approved",
        approved_at="",
        metadata={"source_identifiers": {"brand": "Citizen", "model": "NB1050"}},
    )
    conn.close()

    summary = export_approved_listing_jsonl(db_path=db_path, output_path=output_path)
    assert summary["exported_count"] == 1

    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["listing_status"] == "listed"

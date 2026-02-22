"""Ingest approved listings into Operator DB."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from pathlib import Path
from typing import Any, Dict, Tuple

from reselling.approved_export import REQUIRED_FIELDS

from .models import connect, finish_job_run, init_db, insert_job_run
from .time_utils import utcnow_iso


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_text(value: Any) -> str:
    return str(value or "").strip()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _validate_record(record: Dict[str, Any]) -> None:
    for key in REQUIRED_FIELDS:
        if key not in record:
            raise ValueError(f"missing required field: {key}")


def _upsert_inbox(
    conn: sqlite3.Connection,
    *,
    record: Dict[str, Any],
    ingest_run_id: str,
    ingested_at: str,
    source_file_path: str,
    source_file_hash: str,
) -> bool:
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO approved_listing_inbox (
            approved_id,
            approved_at,
            approved_by,
            sku_key,
            payload_json,
            ingest_run_id,
            ingested_at,
            source_file_path,
            source_file_hash,
            listing_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _to_text(record["approved_id"]),
            _to_text(record["approved_at"]),
            _to_text(record["approved_by"]),
            _to_text(record["sku_key"]),
            json.dumps(record, ensure_ascii=False),
            ingest_run_id,
            ingested_at,
            source_file_path,
            source_file_hash,
            _to_text(record.get("listing_status") or "ready"),
        ),
    )
    return cur.rowcount > 0


def _ensure_operator_listing(conn: sqlite3.Connection, record: Dict[str, Any], now_iso: str) -> bool:
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO operator_listings (
            approved_id,
            listing_state,
            title,
            sku_key,
            source_market,
            target_market,
            source_price_jpy,
            target_price_usd,
            fx_rate,
            estimated_profit_jpy,
            estimated_profit_rate,
            current_source_price_jpy,
            current_target_price_usd,
            current_fx_rate,
            current_profit_jpy,
            current_profit_rate,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _to_text(record["approved_id"]),
            _to_text(record.get("listing_status") or "ready"),
            _to_text(record.get("title")),
            _to_text(record.get("sku_key")),
            _to_text(record.get("source_market")),
            _to_text(record.get("target_market")),
            _to_float(record.get("source_price_jpy"), 0.0),
            _to_float(record.get("target_price_usd"), 0.0),
            _to_float(record.get("fx_rate"), 0.0),
            _to_float(record.get("estimated_profit_jpy"), 0.0),
            _to_float(record.get("estimated_profit_rate"), 0.0),
            _to_float(record.get("source_price_jpy"), 0.0),
            _to_float(record.get("target_price_usd"), 0.0),
            _to_float(record.get("fx_rate"), 0.0),
            _to_float(record.get("estimated_profit_jpy"), 0.0),
            _to_float(record.get("estimated_profit_rate"), 0.0),
            now_iso,
            now_iso,
        ),
    )
    return cur.rowcount > 0


def ingest_approved_listing_jsonl(
    *,
    db_path: Path,
    input_path: Path,
) -> Dict[str, Any]:
    if not input_path.exists():
        raise FileNotFoundError(f"input jsonl not found: {input_path}")

    run_id = f"ingest_{uuid.uuid4().hex[:12]}"
    started_at = utcnow_iso()
    source_file_hash = _file_sha256(input_path)

    conn = connect(db_path)
    init_db(conn)
    insert_job_run(conn, run_id, "ingest_approved_jsonl", started_at)

    processed = 0
    inserted_inbox = 0
    inserted_listing = 0
    skipped = 0
    errors = 0
    error_samples: list[str] = []
    try:
        with input_path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                raw = line.strip()
                if not raw:
                    continue
                processed += 1
                try:
                    record = json.loads(raw)
                    if not isinstance(record, dict):
                        raise ValueError("line is not a JSON object")
                    _validate_record(record)
                    now_iso = utcnow_iso()
                    inbox_added = _upsert_inbox(
                        conn,
                        record=record,
                        ingest_run_id=run_id,
                        ingested_at=now_iso,
                        source_file_path=str(input_path),
                        source_file_hash=source_file_hash,
                    )
                    if not inbox_added:
                        skipped += 1
                        continue
                    inserted_inbox += 1
                    if _ensure_operator_listing(conn, record, now_iso):
                        inserted_listing += 1
                except Exception as exc:  # noqa: BLE001
                    errors += 1
                    if len(error_samples) < 10:
                        error_samples.append(f"line {line_no}: {exc}")
        conn.commit()
        status = "success" if errors == 0 else "partial_success"
        finish_job_run(
            conn,
            run_id=run_id,
            finished_at=utcnow_iso(),
            status=status,
            processed_count=processed,
            success_count=processed - errors,
            error_count=errors,
            error_summary="; ".join(error_samples),
        )
        return {
            "run_id": run_id,
            "status": status,
            "processed_count": processed,
            "inserted_inbox_count": inserted_inbox,
            "inserted_listing_count": inserted_listing,
            "skipped_count": skipped,
            "error_count": errors,
            "error_samples": error_samples,
            "input_path": str(input_path),
            "db_path": str(db_path),
            "source_file_hash": source_file_hash,
        }
    except Exception:
        conn.rollback()
        finish_job_run(
            conn,
            run_id=run_id,
            finished_at=utcnow_iso(),
            status="failed",
            processed_count=processed,
            success_count=max(0, processed - errors),
            error_count=errors + 1,
            error_summary="runtime failure",
        )
        raise
    finally:
        conn.close()

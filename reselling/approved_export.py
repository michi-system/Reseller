"""Export approved/listed review candidates for Operator ingestion."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from .models import connect, init_db

REQUIRED_FIELDS = (
    "approved_id",
    "approved_at",
    "approved_by",
    "sku_key",
    "title",
    "brand",
    "model",
    "source_market",
    "source_price_jpy",
    "target_market",
    "target_price_usd",
    "fx_rate",
    "estimated_profit_jpy",
    "estimated_profit_rate",
    "risk_flags",
    "listing_status",
)


def _parse_json_object(raw: Any) -> Dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _text(value: Any, default: str = "") -> str:
    return str(value or default).strip()


def _first_non_empty(values: Iterable[Any]) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text
    return ""


def _sanitize_key(value: str) -> str:
    text = _text(value).upper()
    if not text:
        return ""
    return re.sub(r"[^A-Z0-9]+", "_", text).strip("_")


def _listing_status(status: str) -> str:
    normalized = _text(status).lower()
    if normalized == "listed":
        return "listed"
    if normalized == "approved":
        return "ready"
    return "ready"


def _brand_model(
    row: Any,
    metadata: Dict[str, Any],
) -> Tuple[str, str]:
    source_ids = metadata.get("source_identifiers") if isinstance(metadata.get("source_identifiers"), dict) else {}
    market_ids = metadata.get("market_identifiers") if isinstance(metadata.get("market_identifiers"), dict) else {}

    brand = _first_non_empty(
        [
            source_ids.get("brand"),
            market_ids.get("brand"),
            source_ids.get("maker"),
            market_ids.get("maker"),
        ]
    )
    model = _first_non_empty(
        [
            source_ids.get("model"),
            source_ids.get("mpn"),
            source_ids.get("jan"),
            market_ids.get("model"),
            market_ids.get("mpn"),
            market_ids.get("jan"),
        ]
    )

    title_hint = _first_non_empty([row["source_title"], row["market_title"]])
    tokens = [tok for tok in re.split(r"\s+", title_hint) if tok]
    if not brand and tokens:
        brand = tokens[0]
    if not model and len(tokens) >= 2:
        model = tokens[1]

    return brand, model


def _sku_key(
    row: Any,
    metadata: Dict[str, Any],
    model: str,
) -> str:
    source_ids = metadata.get("source_identifiers") if isinstance(metadata.get("source_identifiers"), dict) else {}
    market_ids = metadata.get("market_identifiers") if isinstance(metadata.get("market_identifiers"), dict) else {}
    seed = _first_non_empty(
        [
            model,
            source_ids.get("model"),
            source_ids.get("mpn"),
            source_ids.get("jan"),
            market_ids.get("model"),
            market_ids.get("mpn"),
            market_ids.get("jan"),
            row["source_item_id"],
            row["market_item_id"],
        ]
    )
    key = _sanitize_key(seed)
    if key:
        return key
    # Last-resort deterministic fallback
    return f"RC_{int(row['id'])}"


def _risk_flags(metadata: Dict[str, Any]) -> List[str]:
    raw = metadata.get("risk_flags")
    if isinstance(raw, list):
        out: List[str] = []
        for item in raw:
            text = _text(item)
            if text:
                out.append(text)
        return out

    out = []
    if metadata.get("source_stock_status") not in {None, "", "in_stock"}:
        out.append("source_stock_uncertain")
    liquidity = metadata.get("liquidity")
    if isinstance(liquidity, dict):
        gate_passed = bool(liquidity.get("gate_passed", True))
        gate_reason = _text(liquidity.get("gate_reason"))
        if not gate_passed:
            out.append(gate_reason or "liquidity_gate_failed")
    return out


def _approved_by(metadata: Dict[str, Any], default_value: str) -> str:
    auto_review = metadata.get("auto_review") if isinstance(metadata.get("auto_review"), dict) else {}
    return _first_non_empty(
        [
            metadata.get("approved_by"),
            auto_review.get("approved_by"),
            default_value,
        ]
    )


def _approved_record(row: Any, default_approved_by: str) -> Dict[str, Any]:
    metadata = _parse_json_object(row["metadata_json"])
    calc_breakdown = metadata.get("calc_breakdown") if isinstance(metadata.get("calc_breakdown"), dict) else {}

    brand, model = _brand_model(row, metadata)
    sku_key = _sku_key(row, metadata, model)
    fx_rate = _to_float(row["fx_rate"], 0.0)
    source_price_jpy = _to_float(
        _first_non_empty(
            [
                metadata.get("source_price_basis_jpy"),
                metadata.get("source_price_jpy"),
                (metadata.get("calc_input") or {}).get("purchase_price_jpy")
                if isinstance(metadata.get("calc_input"), dict)
                else None,
            ]
        ),
        0.0,
    )
    target_price_usd = _to_float(
        _first_non_empty(
            [
                metadata.get("market_price_basis_usd"),
                metadata.get("market_price_usd"),
                (metadata.get("calc_input") or {}).get("sale_price_usd")
                if isinstance(metadata.get("calc_input"), dict)
                else None,
            ]
        ),
        0.0,
    )

    estimated_profit_jpy = _to_float(calc_breakdown.get("profit_usd"), _to_float(row["expected_profit_usd"], 0.0))
    estimated_profit_jpy = estimated_profit_jpy * fx_rate if fx_rate > 0 else 0.0
    estimated_profit_rate = _to_float(row["expected_margin_rate"], 0.0)
    approved_at = _first_non_empty([row["approved_at"], row["updated_at"], row["created_at"]])
    approved_id = f"apr_{int(row['id'])}_{re.sub(r'[^0-9]', '', approved_at)[:14] or 'na'}"

    shipping_cost_jpy = _to_float(
        _first_non_empty([metadata.get("source_shipping_basis_jpy"), metadata.get("source_shipping_jpy")]),
        0.0,
    )
    fee_total_jpy = _to_float(calc_breakdown.get("variable_fee_usd"), 0.0) * fx_rate if fx_rate > 0 else 0.0

    return {
        "approved_id": approved_id,
        "approved_at": approved_at,
        "approved_by": _approved_by(metadata, default_approved_by),
        "sku_key": sku_key,
        "title": _first_non_empty([row["market_title"], row["source_title"]]),
        "brand": brand,
        "model": model,
        "source_market": _text(row["source_site"]),
        "source_price_jpy": round(source_price_jpy, 2),
        "target_market": _text(row["market_site"]),
        "target_price_usd": round(target_price_usd, 2),
        "fx_rate": round(fx_rate, 6),
        "estimated_profit_jpy": round(estimated_profit_jpy, 2),
        "estimated_profit_rate": round(estimated_profit_rate, 6),
        "risk_flags": _risk_flags(metadata),
        "listing_status": _listing_status(_text(row["status"])),
        "notes": _first_non_empty(
            [
                metadata.get("notes"),
                (metadata.get("auto_review") or {}).get("reason")
                if isinstance(metadata.get("auto_review"), dict)
                else None,
            ]
        ),
        "category_hint": _text(metadata.get("category_hint")),
        "image_url": _first_non_empty([metadata.get("market_image_url"), metadata.get("source_image_url")]),
        "shipping_cost_jpy": round(shipping_cost_jpy, 2),
        "fee_total_jpy": round(fee_total_jpy, 2),
    }


def _validate_required(record: Dict[str, Any]) -> None:
    for key in REQUIRED_FIELDS:
        if key not in record:
            raise ValueError(f"missing required field: {key}")


def export_approved_listing_jsonl(
    *,
    db_path: Path,
    output_path: Path,
    default_approved_by: str = "human_reviewer",
) -> Dict[str, Any]:
    with connect(db_path) as conn:
        init_db(conn)
        rows = conn.execute(
            """
            SELECT
                id,
                status,
                source_site,
                market_site,
                source_item_id,
                market_item_id,
                source_title,
                market_title,
                fx_rate,
                expected_profit_usd,
                expected_margin_rate,
                approved_at,
                created_at,
                updated_at,
                metadata_json
            FROM review_candidates
            WHERE status IN ('approved', 'listed')
              AND COALESCE(approved_at, '') <> ''
            ORDER BY approved_at ASC, id ASC
            """
        ).fetchall()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    exported = 0
    with output_path.open("w", encoding="utf-8") as f:
        for row in rows:
            record = _approved_record(row, default_approved_by=default_approved_by)
            _validate_required(record)
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            exported += 1

    return {
        "output_path": str(output_path),
        "exported_count": exported,
        "db_path": str(db_path),
    }

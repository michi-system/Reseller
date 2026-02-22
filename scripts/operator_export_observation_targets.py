#!/usr/bin/env python3
"""Export due listings as observation template JSONL."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from listing_ops.config import load_operator_settings
from listing_ops.models import connect, init_db
from listing_ops.time_utils import utcnow_iso


def build_parser() -> argparse.ArgumentParser:
    settings = load_operator_settings()
    parser = argparse.ArgumentParser(description="Export due monitor targets")
    parser.add_argument("--db-path", type=Path, default=settings.db_path)
    parser.add_argument("--check-type", choices=["light", "heavy"], default="light")
    parser.add_argument(
        "--output-path",
        type=Path,
        default=ROOT_DIR / "data" / "operator_observations_template.jsonl",
    )
    parser.add_argument("--limit", type=int, default=500)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    due_field = "next_light_check_at" if args.check_type == "light" else "next_heavy_check_at"
    now_iso = utcnow_iso()
    conn = connect(args.db_path.resolve())
    init_db(conn)
    rows = conn.execute(
        f"""
        SELECT id, approved_id, source_price_jpy, target_price_usd, fx_rate, source_in_stock, {due_field}
        FROM operator_listings
        WHERE listing_state IN ('listed', 'alert_review', 'stopped')
          AND ({due_field} IS NULL OR {due_field} <= ?)
        ORDER BY updated_at ASC, id ASC
        LIMIT ?
        """,
        (now_iso, max(1, int(args.limit))),
    ).fetchall()
    conn.close()

    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    with args.output_path.open("w", encoding="utf-8") as f:
        for row in rows:
            payload = {
                "listing_id": int(row["id"]),
                "approved_id": str(row["approved_id"]),
                "source_price_jpy": float(row["source_price_jpy"] or 0.0),
                "target_price_usd": float(row["target_price_usd"] or 0.0),
                "fx_rate": float(row["fx_rate"] or 0.0),
                "source_in_stock": bool(row["source_in_stock"]),
                "heavy_price_drop": False,
            }
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    print(
        json.dumps(
            {
                "check_type": args.check_type,
                "output_path": str(args.output_path.resolve()),
                "item_count": len(rows),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

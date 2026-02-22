#!/usr/bin/env python3
"""Run Operator monitor cycle with optional observations input."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from listing_ops.config import load_operator_settings
from listing_ops.monitor_cycle import run_monitor_cycle


def build_parser() -> argparse.ArgumentParser:
    settings = load_operator_settings()
    parser = argparse.ArgumentParser(description="Run Operator monitor cycle")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=settings.db_path,
        help=f"Operator DB path (default: {settings.db_path})",
    )
    parser.add_argument(
        "--check-type",
        choices=["light", "heavy"],
        default="light",
        help="Monitor type (default: light)",
    )
    parser.add_argument(
        "--observation-jsonl",
        type=Path,
        default=None,
        help=(
            "Observation JSONL path. Each line supports listing_id/approved_id, "
            "source_price_jpy, target_price_usd, fx_rate, source_in_stock, heavy_price_drop."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=300,
        help="Max number of listings to evaluate per run (default: 300)",
    )
    parser.add_argument(
        "--actor-id",
        default="",
        help="Actor id for event logs (default from config).",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = run_monitor_cycle(
        db_path=args.db_path.resolve(),
        check_type=str(args.check_type),
        observation_jsonl_path=args.observation_jsonl.resolve() if args.observation_jsonl else None,
        limit=max(1, int(args.limit)),
        actor_id=str(args.actor_id or "").strip(),
    )
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

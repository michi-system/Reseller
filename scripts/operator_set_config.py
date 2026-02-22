#!/usr/bin/env python3
"""Create a new Operator config version."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from listing_ops.config import load_operator_settings
from listing_ops.config_versions import create_config_version, load_or_default


def build_parser() -> argparse.ArgumentParser:
    settings = load_operator_settings()
    parser = argparse.ArgumentParser(description="Create Operator config version")
    parser.add_argument("--db-path", type=Path, default=settings.db_path)
    parser.add_argument("--config-version", default="")
    parser.add_argument("--created-by", default=settings.default_actor_id)
    parser.add_argument("--min-profit-jpy", type=float, default=None)
    parser.add_argument("--min-profit-rate", type=float, default=None)
    parser.add_argument("--stop-fail-count", type=int, default=None)
    parser.add_argument("--light-new-hours", type=int, default=None)
    parser.add_argument("--light-stable-hours", type=int, default=None)
    parser.add_argument("--light-stopped-hours", type=int, default=None)
    parser.add_argument("--heavy-days", type=int, default=None)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    db_path = args.db_path.resolve()
    active = load_or_default(db_path)
    created = create_config_version(
        db_path=db_path,
        config_version=str(args.config_version or "").strip(),
        created_by=str(args.created_by or "").strip() or "system",
        min_profit_jpy=args.min_profit_jpy if args.min_profit_jpy is not None else float(active["min_profit_jpy"]),
        min_profit_rate=args.min_profit_rate
        if args.min_profit_rate is not None
        else float(active["min_profit_rate"]),
        stop_consecutive_fail_count=args.stop_fail_count
        if args.stop_fail_count is not None
        else int(active["stop_consecutive_fail_count"]),
        light_interval_new_hours=args.light_new_hours
        if args.light_new_hours is not None
        else int(active["light_interval_new_hours"]),
        light_interval_stable_hours=args.light_stable_hours
        if args.light_stable_hours is not None
        else int(active["light_interval_stable_hours"]),
        light_interval_stopped_hours=args.light_stopped_hours
        if args.light_stopped_hours is not None
        else int(active["light_interval_stopped_hours"]),
        heavy_interval_days=args.heavy_days if args.heavy_days is not None else int(active["heavy_interval_days"]),
    )
    print(json.dumps({"db_path": str(db_path), "active_config": created}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

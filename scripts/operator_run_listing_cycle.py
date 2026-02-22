#!/usr/bin/env python3
"""Run Operator listing cycle."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from listing_ops.config import load_operator_settings
from listing_ops.listing_cycle import run_listing_cycle


def build_parser() -> argparse.ArgumentParser:
    settings = load_operator_settings()
    parser = argparse.ArgumentParser(description="Run listing cycle on Operator DB")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=settings.db_path,
        help=f"Operator DB path (default: {settings.db_path})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max number of ready listings to process (default: 20)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Use live mode (default is dry-run).",
    )
    parser.add_argument(
        "--actor-id",
        default="",
        help="Actor id for event logs (default from config).",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = run_listing_cycle(
        db_path=args.db_path.resolve(),
        limit=max(1, int(args.limit)),
        dry_run=not bool(args.live),
        actor_id=str(args.actor_id or "").strip(),
    )
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

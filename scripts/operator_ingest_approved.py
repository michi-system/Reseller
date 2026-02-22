#!/usr/bin/env python3
"""Ingest approved listing JSONL into Operator DB."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from listing_ops.config import load_operator_settings
from listing_ops.ingest import ingest_approved_listing_jsonl


DEFAULT_INPUT = ROOT_DIR / "data" / "approved_listing_exports" / "latest.jsonl"


def build_parser() -> argparse.ArgumentParser:
    settings = load_operator_settings()
    parser = argparse.ArgumentParser(description="Ingest approved JSONL to Operator DB")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=settings.db_path,
        help=f"Operator DB path (default: {settings.db_path})",
    )
    parser.add_argument(
        "--input-path",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Approved JSONL path (default: {DEFAULT_INPUT})",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary = ingest_approved_listing_jsonl(
        db_path=args.db_path.resolve(),
        input_path=args.input_path.resolve(),
    )
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

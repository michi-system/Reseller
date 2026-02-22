#!/usr/bin/env python3
"""Export approved/listed candidates to Operator JSONL contract."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from reselling.approved_export import export_approved_listing_jsonl
from reselling.config import load_settings


DEFAULT_OUTPUT_PATH = ROOT_DIR / "data" / "approved_listing_exports" / "latest.jsonl"


def build_parser() -> argparse.ArgumentParser:
    settings = load_settings()
    parser = argparse.ArgumentParser(
        description="Export approved/listed records for Operator ingestion."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=settings.db_path,
        help=f"SQLite DB path (default: {settings.db_path})",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output JSONL path (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--default-approved-by",
        default="human_reviewer",
        help="Fallback approver name when metadata has no approved_by.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary = export_approved_listing_jsonl(
        db_path=args.db_path.resolve(),
        output_path=args.output_path.resolve(),
        default_approved_by=str(args.default_approved_by or "human_reviewer").strip()
        or "human_reviewer",
    )
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

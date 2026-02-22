#!/usr/bin/env python3
"""Export reseller/operator SQLite tables into a timestamped CSV bundle."""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUT_ROOT = ROOT_DIR / "backups" / "sqlite_exports"


@dataclass(frozen=True)
class DbTarget:
    label: str
    db_path: Path
    tables: tuple[str, ...]


DB_TARGETS: tuple[DbTarget, ...] = (
    DbTarget(
        label="reseller",
        db_path=ROOT_DIR / "data" / "reseller.db",
        tables=(
            "fx_rate_states",
            "miner_candidates",
            "miner_rejections",
            "liquidity_signals",
        ),
    ),
    DbTarget(
        label="operator",
        db_path=ROOT_DIR / "data" / "operator.db",
        tables=(
            "approved_listing_inbox",
            "operator_listings",
            "monitor_snapshots",
            "listing_events",
            "job_runs",
            "operator_config_versions",
        ),
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export SQLite tables to CSV bundle.")
    parser.add_argument(
        "--tag",
        default="pre-supabase",
        help="Bundle tag prefix (default: pre-supabase)",
    )
    parser.add_argument(
        "--out-root",
        type=Path,
        default=DEFAULT_OUT_ROOT,
        help=f"Export root directory (default: {DEFAULT_OUT_ROOT})",
    )
    parser.add_argument(
        "--include-empty",
        action="store_true",
        help="Write CSV even when row count is 0.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be exported without writing files.",
    )
    return parser


def _bundle_dir(tag: str, out_root: Path) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_tag = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in tag.strip()) or "bundle"
    return out_root / f"{safe_tag}_{ts}"


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _dump_table(conn: sqlite3.Connection, table: str) -> tuple[list[str], list[sqlite3.Row]]:
    rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    if not rows:
        col_info = conn.execute(f"PRAGMA table_info({table})").fetchall()
        headers = [str(c[1]) for c in col_info]
        return headers, []
    headers = list(rows[0].keys())
    return headers, rows


def _write_csv(path: Path, headers: Iterable[str], rows: Iterable[sqlite3.Row]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(list(headers))
        for row in rows:
            writer.writerow([row[h] for h in row.keys()])
            row_count += 1
    return row_count


def main() -> int:
    args = build_parser().parse_args()
    out_root = args.out_root.resolve()
    bundle_dir = _bundle_dir(args.tag, out_root)
    dry_run = bool(args.dry_run)

    if dry_run:
        print(f"[dry-run] bundle dir: {bundle_dir}")
    else:
        bundle_dir.mkdir(parents=True, exist_ok=False)

    manifest: Dict[str, object] = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tag": args.tag,
        "bundle_dir": str(bundle_dir),
        "databases": [],
    }
    db_items: List[Dict[str, object]] = []

    for target in DB_TARGETS:
        db_info: Dict[str, object] = {
            "label": target.label,
            "db_path": str(target.db_path),
            "exists": target.db_path.exists(),
            "tables": [],
        }
        table_items: List[Dict[str, object]] = []
        if not target.db_path.exists():
            print(f"[skip] db not found: {target.db_path}")
            db_info["tables"] = table_items
            db_items.append(db_info)
            continue

        conn = sqlite3.connect(str(target.db_path))
        conn.row_factory = sqlite3.Row
        try:
            for table in target.tables:
                table_info: Dict[str, object] = {
                    "table": table,
                    "exists": _table_exists(conn, table),
                    "row_count": 0,
                    "csv_path": "",
                }
                if not table_info["exists"]:
                    print(f"[skip] table not found: {target.label}.{table}")
                    table_items.append(table_info)
                    continue

                headers, rows = _dump_table(conn, table)
                csv_rel = Path(target.label) / f"{table}.csv"
                csv_abs = bundle_dir / csv_rel
                table_info["csv_path"] = str(csv_rel)
                table_info["row_count"] = len(rows)

                if not rows and not args.include_empty:
                    print(f"[skip] table empty: {target.label}.{table}")
                    table_items.append(table_info)
                    continue

                if dry_run:
                    print(f"[dry-run] export {target.label}.{table} -> {csv_abs} (rows={len(rows)})")
                else:
                    written = _write_csv(csv_abs, headers, rows)
                    print(f"[ok] export {target.label}.{table} -> {csv_abs} (rows={written})")
                table_items.append(table_info)
        finally:
            conn.close()

        db_info["tables"] = table_items
        db_items.append(db_info)

    manifest["databases"] = db_items
    if dry_run:
        print("[dry-run] manifest:")
        print(json.dumps(manifest, ensure_ascii=False, indent=2))
        return 0

    manifest_path = bundle_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ok] manifest: {manifest_path}")
    print(f"[done] export bundle ready: {bundle_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

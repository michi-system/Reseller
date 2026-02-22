#!/usr/bin/env python3
"""Create timestamped backups for local SQLite DB and prune old snapshots."""

from __future__ import annotations

import argparse
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
CANONICAL_DB_PATH = ROOT_DIR / "data" / "reseller.db"
LEGACY_DB_PATHS = (
    ROOT_DIR / "data" / "ebayminer.db",
    ROOT_DIR / "reselling.db",
)
DEFAULT_BACKUP_DIR = ROOT_DIR / "backups"


def _default_db_path() -> Path:
    if CANONICAL_DB_PATH.exists():
        return CANONICAL_DB_PATH
    for path in LEGACY_DB_PATHS:
        if path.exists():
            return path
    return CANONICAL_DB_PATH


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backup local SQLite DB.")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=_default_db_path(),
        help=(
            "Target SQLite file "
            f"(default: auto-detected from {CANONICAL_DB_PATH} / legacy paths)"
        ),
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=DEFAULT_BACKUP_DIR,
        help=f"Backup directory (default: {DEFAULT_BACKUP_DIR})",
    )
    parser.add_argument(
        "--keep-days",
        type=int,
        default=7,
        help="Delete backups older than this many days (default: 7).",
    )
    parser.add_argument(
        "--skip-prune",
        action="store_true",
        help="Create backup without deleting older snapshots.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show actions without writing/deleting files.",
    )
    return parser


def ensure_db_exists(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    if not db_path.is_file():
        raise ValueError(f"DB path is not a file: {db_path}")


def create_backup(db_path: Path, backup_dir: Path, dry_run: bool) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{db_path.name}.auto_{ts}.bak"
    backup_path = backup_dir / backup_name
    if dry_run:
        print(f"[dry-run] create backup: {db_path} -> {backup_path}")
        return backup_path
    backup_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(db_path, backup_path)
    print(f"[ok] backup created: {backup_path}")
    return backup_path


def prune_old_backups(backup_dir: Path, keep_days: int, dry_run: bool) -> int:
    if keep_days < 1:
        raise ValueError("--keep-days must be 1 or greater")
    if not backup_dir.exists():
        return 0

    cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)
    removed = 0
    for path in sorted(backup_dir.glob("*.auto_*.bak")):
        modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        if modified >= cutoff:
            continue
        if dry_run:
            print(f"[dry-run] remove old backup: {path}")
        else:
            path.unlink(missing_ok=True)
            print(f"[ok] removed old backup: {path}")
        removed += 1
    return removed


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    db_path = args.db_path.resolve()
    backup_dir = args.backup_dir.resolve()
    ensure_db_exists(db_path)

    create_backup(db_path, backup_dir, args.dry_run)

    if args.skip_prune:
        print("[info] prune skipped")
        return 0

    removed = prune_old_backups(backup_dir, args.keep_days, args.dry_run)
    print(f"[info] prune done: removed={removed}, keep_days={args.keep_days}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

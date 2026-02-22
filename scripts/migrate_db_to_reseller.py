#!/usr/bin/env python3
"""Migrate runtime DB to canonical path: data/reseller.db."""

from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
CANONICAL_DB = ROOT_DIR / "data" / "reseller.db"
LEGACY_DBS = (
    ROOT_DIR / "data" / "ebayminer.db",
    ROOT_DIR / "reselling.db",
)
ENV_LOCAL = ROOT_DIR / ".env.local"


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _existing_nonempty(paths: tuple[Path, ...]) -> list[Path]:
    out: list[Path] = []
    for path in paths:
        if path.exists() and path.is_file() and path.stat().st_size > 0:
            out.append(path)
    return out


def _pick_source() -> Path:
    candidates = _existing_nonempty(LEGACY_DBS)
    if not candidates:
        raise FileNotFoundError("No non-empty legacy DB found.")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _backup(path: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    target = backup_dir / f"{path.name}.before_reseller_migration_{_timestamp()}.bak"
    shutil.copy2(path, target)
    return target


def _update_env_local(canonical: Path) -> str:
    if not ENV_LOCAL.exists():
        return "skip (.env.local missing)"

    lines = ENV_LOCAL.read_text(encoding="utf-8").splitlines()
    replaced = False
    out: list[str] = []
    for line in lines:
        if line.startswith("DB_PATH="):
            current = line.split("=", 1)[1].strip()
            if not current or current in {str(p) for p in LEGACY_DBS}:
                out.append(f"DB_PATH={canonical}")
                replaced = True
                continue
        out.append(line)

    if not any(line.startswith("DB_PATH=") for line in out):
        out.append(f"DB_PATH={canonical}")
        replaced = True

    if replaced:
        ENV_LOCAL.write_text("\n".join(out) + "\n", encoding="utf-8")
        return "updated"
    return "unchanged (custom DB_PATH kept)"


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate DB to data/reseller.db")
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=ROOT_DIR / "backups",
        help="Backup directory (default: backups)",
    )
    parser.add_argument(
        "--no-update-env",
        action="store_true",
        help="Do not update DB_PATH in .env.local",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite canonical DB even if it already exists",
    )
    args = parser.parse_args()

    source = _pick_source()
    print(f"[info] source={source}")
    print(f"[info] canonical={CANONICAL_DB}")

    CANONICAL_DB.parent.mkdir(parents=True, exist_ok=True)

    if CANONICAL_DB.exists():
        if not args.force:
            print("[info] canonical DB already exists, skip copy (use --force to overwrite)")
        else:
            backup = _backup(CANONICAL_DB, args.backup_dir)
            shutil.copy2(source, CANONICAL_DB)
            print(f"[ok] canonical overwritten from source, backup={backup}")
    else:
        shutil.copy2(source, CANONICAL_DB)
        print("[ok] canonical DB created")

    if args.no_update_env:
        print("[info] .env.local update skipped (--no-update-env)")
    else:
        status = _update_env_local(CANONICAL_DB)
        print(f"[info] .env.local {status}")

    print("[done] migration complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

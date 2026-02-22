#!/usr/bin/env python3
"""Create a restorable local checkpoint before remote DB migration."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


ROOT_DIR = Path(__file__).resolve().parents[1]
CHECKPOINT_ROOT = ROOT_DIR / "backups" / "checkpoints"


@dataclass(frozen=True)
class TargetFile:
    src: Path
    rel: str
    required: bool = False


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _targets() -> List[TargetFile]:
    return [
        TargetFile(src=ROOT_DIR / ".env.local", rel=".env.local", required=True),
        TargetFile(src=ROOT_DIR / "data" / "reseller.db", rel="data/reseller.db"),
        TargetFile(src=ROOT_DIR / "data" / "operator.db", rel="data/operator.db"),
        TargetFile(
            src=ROOT_DIR / "data" / "approved_listing_exports" / "latest.jsonl",
            rel="data/approved_listing_exports/latest.jsonl",
        ),
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create local checkpoint for rollback.")
    parser.add_argument(
        "--tag",
        default="pre-supabase",
        help="Checkpoint tag prefix (default: pre-supabase)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be checkpointed without writing files.",
    )
    return parser


def _checkpoint_dir(tag: str) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_tag = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in tag.strip()) or "checkpoint"
    return CHECKPOINT_ROOT / f"{safe_tag}_{ts}"


def _collect_records(targets: List[TargetFile], dry_run: bool, dest_root: Path) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for target in targets:
        exists = target.src.exists()
        if target.required and not exists:
            raise FileNotFoundError(f"required file not found: {target.src}")
        if not exists:
            continue
        if not target.src.is_file():
            raise ValueError(f"not a file: {target.src}")

        rel_path = Path("files") / target.rel
        dst = dest_root / rel_path
        row = {
            "source": str(target.src),
            "relative_source": target.rel,
            "checkpoint_path": str(rel_path),
            "size_bytes": target.src.stat().st_size,
            "sha256": _sha256(target.src),
        }
        records.append(row)
        if dry_run:
            print(f"[dry-run] copy {target.src} -> {dst}")
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(target.src, dst)
        print(f"[ok] copied: {target.src} -> {dst}")
    return records


def main() -> int:
    args = build_parser().parse_args()
    dest = _checkpoint_dir(args.tag)
    if args.dry_run:
        print(f"[dry-run] checkpoint dir: {dest}")
    else:
        dest.mkdir(parents=True, exist_ok=False)

    targets = _targets()
    records = _collect_records(targets, bool(args.dry_run), dest)
    manifest = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tag": args.tag,
        "root_dir": str(ROOT_DIR),
        "checkpoint_dir": str(dest),
        "file_count": len(records),
        "files": records,
    }

    if args.dry_run:
        print("[dry-run] manifest:")
        print(json.dumps(manifest, ensure_ascii=False, indent=2))
        return 0

    manifest_path = dest / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ok] manifest: {manifest_path}")
    print(f"[done] checkpoint ready: {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


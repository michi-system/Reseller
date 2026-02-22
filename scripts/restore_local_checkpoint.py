#!/usr/bin/env python3
"""Restore files from a local checkpoint created by create_local_checkpoint.py."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Dict, List


ROOT_DIR = Path(__file__).resolve().parents[1]
CHECKPOINT_ROOT = ROOT_DIR / "backups" / "checkpoints"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Restore local checkpoint files.")
    parser.add_argument(
        "--checkpoint-dir",
        default="",
        help="Checkpoint directory path (default: latest checkpoint under backups/checkpoints)",
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Use latest checkpoint (same behavior as omitting --checkpoint-dir).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show restore actions without copying files.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually restore files. Required unless --dry-run is set.",
    )
    return parser


def _resolve_checkpoint(args) -> Path:
    if args.checkpoint_dir:
        candidate = Path(args.checkpoint_dir).expanduser().resolve()
        if not candidate.exists():
            raise FileNotFoundError(f"checkpoint dir not found: {candidate}")
        return candidate

    if not CHECKPOINT_ROOT.exists():
        raise FileNotFoundError(f"checkpoint root not found: {CHECKPOINT_ROOT}")
    dirs = sorted([p for p in CHECKPOINT_ROOT.iterdir() if p.is_dir()], key=lambda p: p.name)
    if not dirs:
        raise FileNotFoundError(f"no checkpoint found under: {CHECKPOINT_ROOT}")
    return dirs[-1]


def _load_manifest(checkpoint_dir: Path) -> Dict[str, object]:
    manifest_path = checkpoint_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest not found: {manifest_path}")
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("invalid manifest format")
    return data


def _validate_target_path(path: Path) -> None:
    try:
        path.relative_to(ROOT_DIR)
    except ValueError as exc:
        raise ValueError(f"target path outside repository: {path}") from exc


def _restore_files(manifest: Dict[str, object], checkpoint_dir: Path, dry_run: bool) -> int:
    files = manifest.get("files", [])
    if not isinstance(files, list):
        raise ValueError("manifest files field is not a list")

    restored = 0
    for row in files:
        if not isinstance(row, dict):
            continue
        rel_source = str(row.get("relative_source", "") or "").strip()
        checkpoint_path = str(row.get("checkpoint_path", "") or "").strip()
        if not rel_source or not checkpoint_path:
            continue

        src = checkpoint_dir / checkpoint_path
        dst = ROOT_DIR / rel_source
        _validate_target_path(dst)
        if not src.exists():
            raise FileNotFoundError(f"checkpoint file missing: {src}")

        if dry_run:
            print(f"[dry-run] restore {src} -> {dst}")
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            print(f"[ok] restored: {dst}")
        restored += 1
    return restored


def main() -> int:
    args = build_parser().parse_args()
    dry_run = bool(args.dry_run)
    if not dry_run and not args.apply:
        raise SystemExit("restore requires --apply (or use --dry-run)")

    checkpoint_dir = _resolve_checkpoint(args)
    manifest = _load_manifest(checkpoint_dir)
    print(f"[info] checkpoint: {checkpoint_dir}")
    restored = _restore_files(manifest, checkpoint_dir, dry_run)
    print(f"[done] files processed: {restored}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


#!/usr/bin/env python3
"""Apply learning from latest reviewed cycle into fetch-time blocklist."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.local"
BLOCKLIST_PATH = ROOT_DIR / "data" / "review_blocklist.json"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from reselling.env import load_dotenv
from reselling.live_review_fetch import _pair_signature
from reselling.review import get_review_candidate


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply reviewed-cycle reject learning to blocklist.")
    parser.add_argument(
        "--active-manifest",
        default=str(ROOT_DIR / "docs" / "review_cycle_active.json"),
        help="Path to active review cycle manifest.",
    )
    parser.add_argument(
        "--allow-empty-issue-target",
        action="store_true",
        help="Allow rejected rows without issue target and map them to 'other'.",
    )
    args = parser.parse_args()

    load_dotenv(ENV_PATH)
    manifest_path = Path(args.active_manifest)
    if not manifest_path.exists():
        raise FileNotFoundError(f"not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError("invalid manifest")
    cycle_id = str(manifest.get("cycle_id", "") or "")
    ids = [int(v) for v in (manifest.get("selected_candidate_ids") or [])]
    if not ids:
        existing_payload = _load_json(BLOCKLIST_PATH)
        existing_pairs = existing_payload.get("blocked_pairs", [])
        total = len(existing_pairs) if isinstance(existing_pairs, list) else 0
        print(f"Applied cycle improvements: cycle={cycle_id} added_blocked_pairs=0 total_blocked_pairs={total}")
        print(f"blocklist={BLOCKLIST_PATH} (no-op: empty batch)")
        return 0

    blocklist = _load_json(BLOCKLIST_PATH)
    blocked_pairs = blocklist.get("blocked_pairs", [])
    if not isinstance(blocked_pairs, list):
        blocked_pairs = []
    existing = {
        str(row.get("signature", "")).strip()
        for row in blocked_pairs
        if isinstance(row, dict) and str(row.get("signature", "")).strip()
    }

    added = 0
    skipped_no_issue = 0
    for cid in ids:
        candidate = get_review_candidate(cid)
        if candidate is None:
            continue
        if str(candidate.get("status", "")) != "rejected":
            continue
        rejections = candidate.get("rejections", [])
        latest = rejections[0] if isinstance(rejections, list) and rejections else {}
        issues = latest.get("issue_targets", []) if isinstance(latest, dict) else []
        issues = [str(v) for v in issues if str(v).strip()]
        if not issues:
            if bool(args.allow_empty_issue_target):
                issues = ["other"]
            else:
                skipped_no_issue += 1
                continue
        source_title = str(candidate.get("source_title", "") or "")
        market_title = str(candidate.get("market_title", "") or "")
        sig = _pair_signature(source_title, market_title)
        if not sig or sig in existing:
            continue
        blocked_pairs.append(
            {
                "signature": sig,
                "cycle_id": cycle_id,
                "candidate_id": cid,
                "issue_targets": issues,
                "reason_text": str(latest.get("reason_text", "") or ""),
                "source_site": candidate.get("source_site"),
                "market_site": candidate.get("market_site"),
                "source_title": source_title,
                "market_title": market_title,
                "created_at": _now_iso(),
            }
        )
        existing.add(sig)
        added += 1

    blocklist["blocked_pairs"] = blocked_pairs
    blocklist["updated_at"] = _now_iso()
    _save_json(BLOCKLIST_PATH, blocklist)
    print(
        f"Applied cycle improvements: cycle={cycle_id} added_blocked_pairs={added} "
        f"skipped_no_issue_target={skipped_no_issue} total_blocked_pairs={len(blocked_pairs)}"
    )
    print(f"blocklist={BLOCKLIST_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

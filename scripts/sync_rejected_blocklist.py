#!/usr/bin/env python3
"""Sync all rejected review candidates into review_blocklist.json."""

from __future__ import annotations

import json
import sqlite3
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
from reselling.config import load_settings


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
    load_dotenv(ENV_PATH)
    db_path = load_settings().db_path
    if not db_path.exists():
        raise FileNotFoundError(f"not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        WITH latest_rej AS (
          SELECT candidate_id, MAX(id) AS max_id
          FROM review_rejections
          GROUP BY candidate_id
        )
        SELECT
          rc.id AS candidate_id,
          rc.source_site,
          rc.market_site,
          rc.source_title,
          rc.market_title,
          rr.issue_targets_json,
          rr.reason_text,
          rr.created_at
        FROM review_candidates rc
        JOIN latest_rej lr ON lr.candidate_id = rc.id
        JOIN review_rejections rr ON rr.id = lr.max_id
        WHERE rc.status = 'rejected'
        ORDER BY rc.id ASC
        """
    ).fetchall()

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
    for row in rows:
        source_title = str(row["source_title"] or "")
        market_title = str(row["market_title"] or "")
        sig = _pair_signature(source_title, market_title)
        if not sig or sig in existing:
            continue
        issues: List[str] = []
        try:
            parsed = json.loads(str(row["issue_targets_json"] or "[]"))
            if isinstance(parsed, list):
                issues = [str(v).strip() for v in parsed if str(v).strip()]
        except Exception:
            issues = []
        blocked_pairs.append(
            {
                "signature": sig,
                "cycle_id": "all-rejected-sync",
                "candidate_id": int(row["candidate_id"]),
                "issue_targets": issues,
                "reason_text": str(row["reason_text"] or ""),
                "source_site": str(row["source_site"] or ""),
                "market_site": str(row["market_site"] or ""),
                "source_title": source_title,
                "market_title": market_title,
                "created_at": str(row["created_at"] or _now_iso()),
            }
        )
        existing.add(sig)
        added += 1

    blocklist["blocked_pairs"] = blocked_pairs
    blocklist["updated_at"] = _now_iso()
    _save_json(BLOCKLIST_PATH, blocklist)
    print(
        f"synced rejected candidates: rows={len(rows)} added={added} total_blocked_pairs={len(blocked_pairs)}"
    )
    print(f"blocklist={BLOCKLIST_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

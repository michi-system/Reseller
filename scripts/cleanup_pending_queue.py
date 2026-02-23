#!/usr/bin/env python3
"""Cleanup pending review queue by auto-rejecting obvious non-reviewable candidates."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.local"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from reselling.coerce import to_float as _to_float
from reselling.env import load_dotenv
from reselling.live_miner_fetch import _is_accessory_title, _is_new_listing
from reselling.miner import list_miner_queue, reject_miner_candidate

def judge(
    candidate: Dict[str, Any],
    *,
    min_profit_usd: float,
    min_margin_rate: float,
    min_match_score: float,
) -> Tuple[bool, List[str], str]:
    issues: List[str] = []
    reasons: List[str] = []
    source_title = str(candidate.get("source_title", "") or "")
    market_title = str(candidate.get("market_title", "") or "")
    profit = _to_float(candidate.get("expected_profit_usd"))
    margin = _to_float(candidate.get("expected_margin_rate"))
    score = _to_float(candidate.get("match_score"))

    if profit < min_profit_usd:
        issues.append("price")
        reasons.append(f"期待利益が閾値未満 ({profit:.2f} USD < {min_profit_usd:.2f} USD)")
    if margin < min_margin_rate:
        issues.append("price")
        reasons.append(f"粗利率が閾値未満 ({margin*100:.1f}% < {min_margin_rate*100:.1f}%)")
    if score < min_match_score:
        issues.append("model")
        reasons.append(f"一致スコアが閾値未満 ({score:.3f} < {min_match_score:.3f})")
    if not _is_new_listing(source_title) or not _is_new_listing(market_title):
        issues.append("condition")
        reasons.append("新品以外の可能性が高いタイトルを検知")
    if _is_accessory_title(source_title) or _is_accessory_title(market_title):
        issues.append("other")
        reasons.append("アクセサリ系（本体以外）を検知")

    should_reject = bool(issues)
    if not should_reject:
        return False, [], ""
    # dedupe order-preserving
    dedup_issues = list(dict.fromkeys(issues))
    reason = " / ".join(reasons)
    return True, dedup_issues, reason


def main() -> int:
    parser = argparse.ArgumentParser(description="Cleanup pending queue.")
    parser.add_argument("--apply", action="store_true", help="Apply rejection updates")
    parser.add_argument("--min-profit-usd", type=float, default=0.01)
    parser.add_argument("--min-margin-rate", type=float, default=0.03)
    parser.add_argument("--min-match-score", type=float, default=0.70)
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    load_dotenv(ENV_PATH)

    queue = list_miner_queue(status="pending", limit=max(1, int(args.limit)), offset=0)
    items = queue.get("items", [])
    flagged: List[Tuple[int, List[str], str]] = []
    for item in items:
        should_reject, issue_targets, reason = judge(
            item,
            min_profit_usd=float(args.min_profit_usd),
            min_margin_rate=float(args.min_margin_rate),
            min_match_score=float(args.min_match_score),
        )
        if should_reject:
            flagged.append((int(item["id"]), issue_targets, reason))

    print(f"pending={len(items)} flagged={len(flagged)} apply={args.apply}")
    for candidate_id, issue_targets, reason in flagged:
        print(f"- #{candidate_id} issues={issue_targets} reason={reason}")
        if args.apply:
            reject_miner_candidate(candidate_id, issue_targets=issue_targets, reason_text=reason)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

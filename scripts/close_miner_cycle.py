#!/usr/bin/env python3
"""Close an active miner cycle and generate tuning-focused summary."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.local"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from reselling.config import load_settings
from reselling.env import load_dotenv
from reselling.json_utils import load_json_dict
from reselling.metrics import safe_rate as _safe_rate
from reselling.models import connect, init_db
from reselling.time_utils import utc_iso as _now_iso


def _load_json(path: Path, *, required: bool = True) -> Dict[str, Any]:
    return load_json_dict(
        path,
        required=required,
        missing_message=f"not found: {path}",
        invalid_message="manifest must be a JSON object",
    )


def _recommendation_from_issue(issue: str) -> str:
    mapping = {
        "model": "型番トークン一致の閾値を上げ、コード一致なし候補のスコア上限を下げる",
        "condition": "新品判定辞書を拡張し、中古・在庫なしキーワード検知を強化する",
        "price": "仕入価格・送料の見積もり式を再点検し、最低利益閾値を引き上げる",
        "shipping": "送料抽出の優先キーを改善し、欠損時の保守的デフォルトを上げる",
        "fees": "eBay手数料・決済手数料の実績値で固定費率を再設定する",
        "fx": "為替更新頻度を上げ、算出時点レートとの差分監視を入れる",
        "other": "アクセサリ/本体の分類ルールを強化し、混在候補を事前除外する",
        "brand": "ブランド辞書を追加し、ブランド不一致を強く減点する",
        "color": "色属性抽出を追加し、色不一致を減点する",
        "size": "サイズ/容量属性抽出を追加し、型違い候補を除外する",
        "accessories": "付属品キーワードを抽出し、同梱差異が大きい候補を除外する",
    }
    return mapping.get(issue, "否認理由を分類し、判定ルールへ反映する")


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _series_stats(values: List[float]) -> Dict[str, Any]:
    if not values:
        return {"min": None, "median": None, "max": None}
    return {
        "min": round(min(values), 4),
        "median": round(float(statistics.median(values)), 4),
        "max": round(max(values), 4),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Close active miner cycle.")
    parser.add_argument(
        "--active-manifest",
        default=str(ROOT_DIR / "docs" / "miner_cycle_active.json"),
        help="Path to active cycle manifest JSON.",
    )
    parser.add_argument(
        "--output",
        default=str(ROOT_DIR / "docs" / "miner_cycle_close_report_latest.json"),
        help="Output report path.",
    )
    parser.add_argument(
        "--reject-floor",
        type=int,
        default=10,
        help="Required rejected count to mark cycle ready for tuning.",
    )
    parser.add_argument(
        "--min-reviewed-ratio",
        type=float,
        default=1.0,
        help="Minimum reviewed_count/batch_size ratio required for ready_for_tuning.",
    )
    parser.add_argument(
        "--min-reject-rate",
        type=float,
        default=0.10,
        help="Minimum rejected_count/reviewed_count ratio required for ready_for_tuning.",
    )
    parser.add_argument(
        "--min-reject-with-issue-count",
        type=int,
        default=1,
        help="Minimum rejected records with issue targets required for ready_for_tuning.",
    )
    parser.add_argument(
        "--auto-miner-report",
        "--auto-review-report",
        dest="auto_miner_report",
        default=str(ROOT_DIR / "docs" / "miner_cycle_auto_miner_latest.json"),
        help="Auto-miner report path for additional metrics.",
    )
    args = parser.parse_args()

    load_dotenv(ENV_PATH)
    manifest_path = Path(args.active_manifest)
    manifest = _load_json(manifest_path)
    candidate_ids = [int(v) for v in manifest.get("selected_candidate_ids", [])]

    auto_report_path = Path(args.auto_miner_report)
    auto_report = _load_json(auto_report_path, required=False)
    auto_counts: Dict[str, Any] = auto_report.get("counts", {}) if isinstance(auto_report.get("counts"), dict) else {}
    auto_approve = int(auto_counts.get("approve", 0) or 0)
    auto_reject = int(auto_counts.get("reject", 0) or 0)
    auto_total = auto_approve + auto_reject
    auto_reject_rate = _safe_rate(auto_reject, auto_total)

    if not candidate_ids:
        report = {
            "cycle_id": manifest.get("cycle_id"),
            "closed_at": _now_iso(),
            "manifest_path": str(manifest_path),
            "batch_size": 0,
            "reviewed_count": 0,
            "review_completion_rate": 0.0,
            "unresolved_count": 0,
            "unresolved_candidate_ids": [],
            "status_breakdown": {},
            "approved_count": 0,
            "rejected_count": 0,
            "rejected_with_reason_count": 0,
            "rejected_with_issue_count": 0,
            "reject_floor": int(args.reject_floor),
            "min_reviewed_ratio": float(args.min_reviewed_ratio),
            "min_reject_rate": float(args.min_reject_rate),
            "min_reject_with_issue_count": int(args.min_reject_with_issue_count),
            "reject_rate": 0.0,
            "avg_rejected_issue_count": 0.0,
            "profit_usd_stats": _series_stats([]),
            "score_stats": _series_stats([]),
            "auto_miner_counts": auto_counts,
            "auto_miner_reject_rate": round(auto_reject_rate, 4),
            "ready_for_tuning": False,
            "ready_for_light_tuning": False,
            "recommended_tuning_mode": "none",
            "top_issue_targets": [],
            "recommendations": [],
            "reason_samples": [],
            "note": "selected_candidate_ids is empty; nothing to close",
        }
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Saved close report: {output_path}")
        print(
            f"cycle={manifest.get('cycle_id')} reviewed=0/0 unresolved=0 rejected=0 "
            "ready_for_tuning=False ready_for_light_tuning=False"
        )
        return 0

    settings = load_settings()
    placeholders = ",".join("?" for _ in candidate_ids)
    with connect(settings.db_path) as conn:
        init_db(conn)
        candidates = conn.execute(
            f"""
            SELECT id, status, expected_profit_usd, match_score, source_site, market_site,
                   source_title, market_title, updated_at
            FROM miner_candidates
            WHERE id IN ({placeholders})
            ORDER BY created_at DESC, id DESC
            """,
            tuple(candidate_ids),
        ).fetchall()

        rejection_rows = conn.execute(
            f"""
            SELECT candidate_id, issue_targets_json, reason_text, created_at, id
            FROM miner_rejections
            WHERE candidate_id IN ({placeholders})
            ORDER BY candidate_id, id DESC
            """,
            tuple(candidate_ids),
        ).fetchall()

    status_counter: Counter[str] = Counter()
    unresolved_ids: List[int] = []
    profits: List[float] = []
    scores: List[float] = []
    for row in candidates:
        status = str(row["status"])
        status_counter[status] += 1
        if status == "pending":
            unresolved_ids.append(int(row["id"]))
        p = _float_or_none(row["expected_profit_usd"])
        s = _float_or_none(row["match_score"])
        if p is not None:
            profits.append(p)
        if s is not None:
            scores.append(s)

    latest_rejection_by_candidate: Dict[int, Dict[str, Any]] = {}
    for row in rejection_rows:
        cid = int(row["candidate_id"])
        if cid in latest_rejection_by_candidate:
            continue
        issue_targets_raw = row["issue_targets_json"] or "[]"
        try:
            issue_targets = json.loads(issue_targets_raw)
            if not isinstance(issue_targets, list):
                issue_targets = []
        except json.JSONDecodeError:
            issue_targets = []
        latest_rejection_by_candidate[cid] = {
            "issue_targets": [str(v) for v in issue_targets if str(v).strip()],
            "reason_text": str(row["reason_text"] or "").strip(),
            "created_at": str(row["created_at"] or ""),
        }

    issue_counter: Counter[str] = Counter()
    reason_samples: List[str] = []
    rejected_issue_counts: List[int] = []
    for _, item in latest_rejection_by_candidate.items():
        for issue in item["issue_targets"]:
            issue_counter[issue] += 1
        rejected_issue_counts.append(len(item["issue_targets"]))
        reason = item["reason_text"]
        if reason:
            reason_samples.append(reason)

    batch_size = len(candidate_ids)
    reviewed_count = batch_size - len(unresolved_ids)
    reviewed_ratio = _safe_rate(reviewed_count, batch_size)
    approved_count = int(status_counter.get("listed", 0)) + int(status_counter.get("approved", 0))
    rejected_count = int(status_counter.get("rejected", 0))
    reject_rate = _safe_rate(rejected_count, reviewed_count)
    rejected_with_reason_count = len([v for v in latest_rejection_by_candidate.values() if v["reason_text"]])
    rejected_with_issue_count = len([v for v in latest_rejection_by_candidate.values() if v["issue_targets"]])

    ready_for_tuning = (
        len(unresolved_ids) == 0
        and reviewed_ratio >= float(args.min_reviewed_ratio)
        and rejected_count >= int(args.reject_floor)
        and reject_rate >= float(args.min_reject_rate)
        and rejected_with_issue_count >= int(args.min_reject_with_issue_count)
    )
    ready_for_light_tuning = (
        len(unresolved_ids) == 0
        and reviewed_ratio >= float(args.min_reviewed_ratio)
        and rejected_with_issue_count >= int(args.min_reject_with_issue_count)
    )
    if ready_for_tuning:
        recommended_tuning_mode = "full"
    elif ready_for_light_tuning:
        recommended_tuning_mode = "light"
    else:
        recommended_tuning_mode = "none"

    top_issues = issue_counter.most_common(5)
    recommendations = [
        {"issue": issue, "count": count, "action": _recommendation_from_issue(issue)}
        for issue, count in top_issues
    ]
    if not recommendations:
        recommendations = [
            {
                "issue": "other",
                "count": 0,
                "action": "否認データが不足しています。対象件数を増やすか、指摘箇所付きで否認を集めてください。",
            }
        ]

    report = {
        "cycle_id": manifest.get("cycle_id"),
        "closed_at": _now_iso(),
        "manifest_path": str(manifest_path),
        "batch_size": batch_size,
        "reviewed_count": reviewed_count,
        "review_completion_rate": round(reviewed_ratio, 4),
        "unresolved_count": len(unresolved_ids),
        "unresolved_candidate_ids": unresolved_ids,
        "status_breakdown": dict(status_counter),
        "approved_count": approved_count,
        "rejected_count": rejected_count,
        "rejected_with_reason_count": rejected_with_reason_count,
        "rejected_with_issue_count": rejected_with_issue_count,
        "reject_floor": int(args.reject_floor),
        "min_reviewed_ratio": float(args.min_reviewed_ratio),
        "min_reject_rate": float(args.min_reject_rate),
        "min_reject_with_issue_count": int(args.min_reject_with_issue_count),
        "reject_rate": round(reject_rate, 4),
        "avg_rejected_issue_count": (
            round(float(statistics.mean(rejected_issue_counts)), 4) if rejected_issue_counts else 0.0
        ),
        "profit_usd_stats": _series_stats(profits),
        "score_stats": _series_stats(scores),
        "auto_miner_counts": auto_counts,
        "auto_miner_reject_rate": round(auto_reject_rate, 4),
        "ready_for_tuning": ready_for_tuning,
        "ready_for_light_tuning": ready_for_light_tuning,
        "recommended_tuning_mode": recommended_tuning_mode,
        "top_issue_targets": [{"issue": k, "count": v} for k, v in top_issues],
        "recommendations": recommendations,
        "reason_samples": reason_samples[:10],
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Saved close report: {output_path}")
    print(
        f"cycle={manifest.get('cycle_id')} reviewed={reviewed_count}/{batch_size} "
        f"unresolved={len(unresolved_ids)} rejected={rejected_count} "
        f"ready_for_tuning={ready_for_tuning} ready_for_light_tuning={ready_for_light_tuning}"
    )
    if unresolved_ids:
        print(f"unresolved_candidate_ids={unresolved_ids}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

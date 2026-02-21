#!/usr/bin/env python3
"""Run one full autonomous cycle: start -> auto-review -> close -> improve."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.local"

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from reselling.env import load_dotenv


def run_cmd(args: list[str]) -> None:
    print("$", " ".join(shlex.quote(a) for a in args))
    subprocess.run(args, cwd=str(ROOT_DIR), check=True)


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_rate(n: int, d: int) -> float:
    if d <= 0:
        return 0.0
    return float(n) / float(d)


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _validate_operation_policy(
    *,
    policy: Dict[str, Any],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []

    threshold_policy = policy.get("run_thresholds", {})
    if not isinstance(threshold_policy, dict):
        threshold_policy = {}
    floor_profit = _to_float(threshold_policy.get("min_profit_usd"), 0.01)
    floor_margin = _to_float(threshold_policy.get("min_margin_rate"), 0.03)
    floor_match = _to_float(threshold_policy.get("min_match_score"), 0.75)
    floor_ev90 = _to_float(threshold_policy.get("min_ev90_usd"), 0.0)

    if float(args.min_profit_usd) < floor_profit:
        errors.append(
            f"min_profit_usd={args.min_profit_usd:.4f} is below policy floor {floor_profit:.4f}"
        )
    if float(args.min_margin_rate) < floor_margin:
        errors.append(
            f"min_margin_rate={args.min_margin_rate:.4f} is below policy floor {floor_margin:.4f}"
        )
    if float(args.min_match_score) < floor_match:
        errors.append(
            f"min_match_score={args.min_match_score:.4f} is below policy floor {floor_match:.4f}"
        )
    if float(args.min_ev90_usd) < floor_ev90:
        errors.append(
            f"min_ev90_usd={args.min_ev90_usd:.4f} is below policy floor {floor_ev90:.4f}"
        )

    dod = policy.get("definition_of_done", {})
    if not isinstance(dod, dict):
        dod = {}
    target = _to_int(dod.get("cycle_target_reviewed_count"), 24)
    if int(args.target_count) != target:
        warnings.append(
            f"target_count={args.target_count} differs from DoD target {target} (validation cycle only if intentional)"
        )

    env_req = policy.get("env_requirements", {})
    if not isinstance(env_req, dict):
        env_req = {}
    expected_condition = str(env_req.get("ITEM_CONDITION", "new") or "new").strip().lower()
    actual_condition = (os.getenv("ITEM_CONDITION", "new") or "new").strip().lower()
    if expected_condition and actual_condition != expected_condition:
        errors.append(f"ITEM_CONDITION={actual_condition} (expected {expected_condition})")

    require_liquidity = str(env_req.get("LIQUIDITY_REQUIRE_SIGNAL", "1") or "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if require_liquidity and not _env_bool("LIQUIDITY_REQUIRE_SIGNAL", False):
        errors.append("LIQUIDITY_REQUIRE_SIGNAL must be enabled by policy")

    require_auto_liquidity = str(
        env_req.get("AUTO_REVIEW_REQUIRE_LIQUIDITY_SIGNAL", "1") or "1"
    ).strip().lower() in {"1", "true", "yes", "on"}
    if require_auto_liquidity and not _env_bool("AUTO_REVIEW_REQUIRE_LIQUIDITY_SIGNAL", False):
        errors.append("AUTO_REVIEW_REQUIRE_LIQUIDITY_SIGNAL must be enabled by policy")

    allowed_provider_modes = env_req.get("LIQUIDITY_PROVIDER_MODE_allowed", [])
    if not isinstance(allowed_provider_modes, list):
        allowed_provider_modes = []
    allowed_modes = {str(v or "").strip().lower() for v in allowed_provider_modes if str(v or "").strip()}
    actual_mode = (os.getenv("LIQUIDITY_PROVIDER_MODE", "none") or "none").strip().lower()
    if allowed_modes and actual_mode not in allowed_modes:
        errors.append(
            f"LIQUIDITY_PROVIDER_MODE={actual_mode} is not allowed by policy ({sorted(allowed_modes)})"
        )

    return {"ok": len(errors) == 0, "errors": errors, "warnings": warnings}


def _validate_cycle_reports(
    *,
    review_report: Dict[str, Any],
    auto_report: Dict[str, Any],
    close_report: Dict[str, Any],
) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []

    cycle_ids = {
        "review": str(review_report.get("cycle_id", "") or "").strip(),
        "auto": str(auto_report.get("cycle_id", "") or "").strip(),
        "close": str(close_report.get("cycle_id", "") or "").strip(),
    }
    non_empty_ids = {v for v in cycle_ids.values() if v}
    if len(non_empty_ids) != 1:
        errors.append(f"cycle_id mismatch: {cycle_ids}")

    batch_size = _to_int(review_report.get("batch_size"), 0)
    target_count = _to_int(review_report.get("target_count"), 0)
    cycle_ready = bool(review_report.get("cycle_ready", False))
    if cycle_ready and target_count > 0 and batch_size < target_count:
        errors.append(
            f"review report inconsistent: cycle_ready=true but batch_size={batch_size} < target_count={target_count}"
        )

    auto_counts = auto_report.get("counts", {}) if isinstance(auto_report.get("counts"), dict) else {}
    auto_approve = _to_int(auto_counts.get("approve"), 0)
    auto_reject = _to_int(auto_counts.get("reject"), 0)
    auto_skipped = _to_int(auto_counts.get("skipped"), 0)
    auto_total = auto_approve + auto_reject + auto_skipped
    auto_actions = auto_approve + auto_reject
    if auto_total != batch_size:
        errors.append(f"auto report count mismatch: approve+reject+skipped={auto_total}, batch_size={batch_size}")
    if batch_size > 0 and auto_actions <= 0:
        errors.append("auto review produced zero actions (approve/reject) despite non-empty batch")

    reviewed_count = _to_int(close_report.get("reviewed_count"), 0)
    unresolved_count = _to_int(close_report.get("unresolved_count"), 0)
    if reviewed_count != batch_size:
        errors.append(f"close report reviewed_count mismatch: reviewed_count={reviewed_count}, batch_size={batch_size}")
    if unresolved_count > 0:
        errors.append(f"close report unresolved_count must be 0, got {unresolved_count}")

    reject_rate = float(close_report.get("reject_rate", 0.0) or 0.0)
    if batch_size <= 0:
        warnings.append("batch_size=0 (no review candidates were processed)")
    if auto_actions > 0 and reject_rate >= 0.95:
        warnings.append(f"reject_rate is very high: {reject_rate:.3f}")

    return {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "metrics": {
            "cycle_id": next(iter(non_empty_ids), ""),
            "batch_size": batch_size,
            "target_count": target_count,
            "cycle_ready": cycle_ready,
            "auto_approve": auto_approve,
            "auto_reject": auto_reject,
            "auto_skipped": auto_skipped,
            "auto_actions": auto_actions,
            "auto_reject_rate": round(_safe_rate(auto_reject, auto_actions), 4) if auto_actions > 0 else 0.0,
            "reviewed_count": reviewed_count,
            "unresolved_count": unresolved_count,
            "close_ready": bool(close_report.get("ready_for_tuning", False)),
            "reject_rate": round(reject_rate, 4),
            "progressed": bool(auto_actions > 0),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run autonomous review cycle")
    parser.add_argument("--target-count", type=int, default=24)
    parser.add_argument("--hard-cap", type=int, default=30)
    parser.add_argument("--min-profit-usd", type=float, default=0.01)
    parser.add_argument("--min-margin-rate", type=float, default=0.03)
    parser.add_argument("--min-ev90-usd", type=float, default=0.0)
    parser.add_argument("--min-match-score", type=float, default=0.75)
    parser.add_argument("--min-auto-approve-score", type=float, default=0.90)
    parser.add_argument("--min-token-jaccard", type=float, default=0.62)
    parser.add_argument("--max-score-drift", type=float, default=0.25)
    parser.add_argument("--reject-floor", type=int, default=10)
    parser.add_argument("--close-min-reviewed-ratio", type=float, default=1.0)
    parser.add_argument("--close-min-reject-rate", type=float, default=0.10)
    parser.add_argument("--close-min-reject-with-issue-count", type=int, default=1)
    parser.add_argument(
        "--disable-light-tuning",
        action="store_true",
        help="Disable light tuning fallback when full tuning gate is not met.",
    )
    parser.add_argument(
        "--light-tuning-min-reject-with-issue-count",
        type=int,
        default=1,
        help="Minimum rejected_with_issue_count needed for light tuning apply.",
    )
    parser.add_argument("--max-rounds", type=int, default=4)
    parser.add_argument("--sleep-seconds", type=float, default=0.8)
    parser.add_argument("--max-zero-gain-strikes", type=int, default=2)
    parser.add_argument("--historical-min-attempts", type=int, default=3)
    parser.add_argument("--historical-min-network-calls", type=int, default=6)
    parser.add_argument("--historical-min-gain-per-network-call", type=float, default=0.05)
    parser.add_argument("--historical-retry-every-runs", type=int, default=4)
    parser.add_argument("--duplicate-heavy-ratio-threshold", type=float, default=0.70)
    parser.add_argument("--duplicate-heavy-min-evaluated", type=int, default=12)
    parser.add_argument("--duplicate-heavy-min-duplicates", type=int, default=8)
    parser.add_argument("--disable-duplicate-heavy-cooldown", action="store_true")
    parser.add_argument("--disable-query-reorder", action="store_true")
    parser.add_argument("--daily-budget-ebay", type=int, default=-1)
    parser.add_argument("--daily-budget-rakuten", type=int, default=-1)
    parser.add_argument("--daily-budget-yahoo", type=int, default=-1)
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--cache-ttl-seconds", type=int, default=-1)
    parser.add_argument(
        "--allow-partial-batch",
        action="store_true",
        help="Do not fail when target-count is not reached; continue with available batch.",
    )
    parser.add_argument("--skip-apply-when-not-ready", action="store_true")
    parser.add_argument("--no-sync-rejected-blocklist", action="store_true")
    parser.add_argument("--require-progress", action="store_true")
    parser.add_argument("--fail-on-validation-warning", action="store_true")
    parser.add_argument(
        "--policy-file",
        default=str(ROOT_DIR / "docs" / "OPERATION_POLICY.json"),
        help="Operation policy JSON for DoD/threshold guard.",
    )
    parser.add_argument(
        "--skip-policy-check",
        action="store_true",
        help="Skip operation policy guard (use only for explicit experiments).",
    )
    parser.add_argument(
        "--queries",
        type=str,
        default="",
        help="Comma-separated query list passed to run_review_cycle.py",
    )
    parser.add_argument(
        "--review-report",
        default=str(ROOT_DIR / "docs" / "review_cycle_report_latest.json"),
    )
    parser.add_argument(
        "--auto-review-report",
        default=str(ROOT_DIR / "docs" / "review_cycle_auto_review_latest.json"),
    )
    parser.add_argument(
        "--close-report",
        default=str(ROOT_DIR / "docs" / "review_cycle_close_report_latest.json"),
    )
    parser.add_argument(
        "--validation-report",
        default=str(ROOT_DIR / "docs" / "review_cycle_validation_latest.json"),
    )
    args = parser.parse_args()
    load_dotenv(ENV_PATH)

    if not bool(args.skip_policy_check):
        policy_path = Path(str(args.policy_file or "").strip() or str(ROOT_DIR / "docs" / "OPERATION_POLICY.json"))
        if not policy_path.exists():
            print(f"[cycle-error] policy file not found: {policy_path}")
            return 6
        policy = _load_json(policy_path)
        if not policy:
            print(f"[cycle-error] invalid policy JSON: {policy_path}")
            return 6
        policy_check = _validate_operation_policy(policy=policy, args=args)
        print(f"[cycle-summary] policy_file={policy_path} ok={policy_check.get('ok')}")
        for msg in policy_check.get("warnings", []):
            print(f"[cycle-warning] {msg}")
        for msg in policy_check.get("errors", []):
            print(f"[cycle-error] {msg}")
        if not bool(policy_check.get("ok", False)):
            return 6

    run_cmd(
        [
            "python3",
            "scripts/run_review_cycle.py",
            "--target-count",
            str(args.target_count),
            "--hard-cap",
            str(args.hard_cap),
            "--min-profit-usd",
            str(args.min_profit_usd),
            "--min-margin-rate",
            str(args.min_margin_rate),
            "--min-match-score",
            str(args.min_match_score),
            "--max-rounds",
            str(args.max_rounds),
            "--sleep-seconds",
            str(args.sleep_seconds),
            "--max-zero-gain-strikes",
            str(args.max_zero_gain_strikes),
            "--historical-min-attempts",
            str(args.historical_min_attempts),
            "--historical-min-network-calls",
            str(args.historical_min_network_calls),
            "--historical-min-gain-per-network-call",
            str(args.historical_min_gain_per_network_call),
            "--historical-retry-every-runs",
            str(args.historical_retry_every_runs),
            "--duplicate-heavy-ratio-threshold",
            str(args.duplicate_heavy_ratio_threshold),
            "--duplicate-heavy-min-evaluated",
            str(args.duplicate_heavy_min_evaluated),
            "--duplicate-heavy-min-duplicates",
            str(args.duplicate_heavy_min_duplicates),
            *(["--require-full-batch"] if not bool(args.allow_partial_batch) else []),
            *(["--disable-duplicate-heavy-cooldown"] if bool(args.disable_duplicate_heavy_cooldown) else []),
            *(["--disable-query-reorder"] if bool(args.disable_query_reorder) else []),
            *(["--daily-budget-ebay", str(args.daily_budget_ebay)] if int(args.daily_budget_ebay) >= 0 else []),
            *(
                ["--daily-budget-rakuten", str(args.daily_budget_rakuten)]
                if int(args.daily_budget_rakuten) >= 0
                else []
            ),
            *(["--daily-budget-yahoo", str(args.daily_budget_yahoo)] if int(args.daily_budget_yahoo) >= 0 else []),
            *(["--cache-only"] if bool(args.cache_only) else []),
            *(["--cache-ttl-seconds", str(args.cache_ttl_seconds)] if int(args.cache_ttl_seconds) >= 0 else []),
            *(["--queries", args.queries] if str(args.queries).strip() else []),
        ]
    )

    run_cmd(
        [
            "python3",
            "scripts/auto_review_cycle.py",
            "--min-profit-usd",
            str(args.min_profit_usd),
            "--min-margin-rate",
            str(args.min_margin_rate),
            "--min-ev90-usd",
            str(args.min_ev90_usd),
            "--min-match-score",
            str(args.min_match_score),
            "--min-auto-approve-score",
            str(args.min_auto_approve_score),
            "--min-token-jaccard",
            str(args.min_token_jaccard),
            "--max-score-drift",
            str(args.max_score_drift),
            "--output",
            str(args.auto_review_report),
        ]
    )

    run_cmd(
        [
            "python3",
            "scripts/close_review_cycle.py",
            "--reject-floor",
            str(args.reject_floor),
            "--min-reviewed-ratio",
            str(args.close_min_reviewed_ratio),
            "--min-reject-rate",
            str(args.close_min_reject_rate),
            "--min-reject-with-issue-count",
            str(args.close_min_reject_with_issue_count),
            "--auto-review-report",
            str(args.auto_review_report),
            "--output",
            str(args.close_report),
        ]
    )

    review_report = _load_json(Path(args.review_report))
    auto_report = _load_json(Path(args.auto_review_report))
    close_report = _load_json(Path(args.close_report))

    batch_size = int(review_report.get("batch_size", 0) or 0)
    cycle_ready = bool(review_report.get("cycle_ready", False))
    auto_counts = auto_report.get("counts", {}) if isinstance(auto_report.get("counts"), dict) else {}
    auto_approve = int(auto_counts.get("approve", 0) or 0)
    auto_reject = int(auto_counts.get("reject", 0) or 0)
    auto_total = auto_approve + auto_reject
    auto_reject_rate = _safe_rate(auto_reject, auto_total)
    close_ready = bool(close_report.get("ready_for_tuning", False))
    close_ready_light = bool(close_report.get("ready_for_light_tuning", False))
    rejected_with_issue_count = int(close_report.get("rejected_with_issue_count", 0) or 0)

    print(
        "[cycle-summary] "
        f"batch_size={batch_size} cycle_ready={cycle_ready} "
        f"auto(approve={auto_approve}, reject={auto_reject}, reject_rate={auto_reject_rate:.3f}) "
        f"close_ready={close_ready} close_ready_light={close_ready_light}"
    )

    validation = _validate_cycle_reports(
        review_report=review_report,
        auto_report=auto_report,
        close_report=close_report,
    )
    validation_path = Path(args.validation_report)
    validation_path.parent.mkdir(parents=True, exist_ok=True)
    validation_path.write_text(json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[cycle-summary] validation_report={validation_path} ok={validation.get('ok')}")
    for msg in validation.get("warnings", []):
        print(f"[cycle-warning] {msg}")
    for msg in validation.get("errors", []):
        print(f"[cycle-error] {msg}")
    if args.require_progress and not bool(validation.get("metrics", {}).get("progressed", False)):
        print("[cycle-error] require-progress enabled but this cycle made no actionable progress")
        return 4
    if not bool(validation.get("ok", False)):
        return 3
    if args.fail_on_validation_warning and validation.get("warnings"):
        print("[cycle-error] fail-on-validation-warning enabled")
        return 5

    allow_light_tuning = not bool(args.disable_light_tuning)
    light_threshold = max(1, int(args.light_tuning_min_reject_with_issue_count))
    can_apply_light = (
        allow_light_tuning
        and close_ready_light
        and rejected_with_issue_count >= light_threshold
    )
    if args.skip_apply_when_not_ready and not close_ready and not can_apply_light:
        print("[cycle-summary] skip apply_cycle_improvements (ready_for_tuning=false, light_tuning=false)")
    else:
        if not close_ready and can_apply_light:
            print(
                "[cycle-summary] apply_cycle_improvements in light mode "
                f"(rejected_with_issue_count={rejected_with_issue_count})"
            )
        run_cmd(["python3", "scripts/apply_cycle_improvements.py"])

    if not args.no_sync_rejected_blocklist:
        run_cmd(["python3", "scripts/sync_rejected_blocklist.py"])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

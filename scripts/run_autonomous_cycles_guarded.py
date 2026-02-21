#!/usr/bin/env python3
"""Run multiple autonomous cycles with fail-fast validation gates."""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT_DIR = Path(__file__).resolve().parents[1]


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _run_cmd(args: List[str]) -> subprocess.CompletedProcess[str]:
    print("$", " ".join(shlex.quote(a) for a in args))
    return subprocess.run(
        args,
        cwd=str(ROOT_DIR),
        text=True,
        capture_output=True,
        check=False,
    )


def _snapshot_file(src: Path, dst: Path) -> None:
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")


def _safe_rate(n: int, d: int) -> float:
    if d <= 0:
        return 0.0
    return float(n) / float(d)


def _extract_review_kpi(review_report: Dict[str, Any]) -> Dict[str, Any]:
    rows = review_report.get("runs", [])
    if not isinstance(rows, list):
        rows = []
    totals = {
        "created_count": 0,
        "skipped_low_ev90": 0,
        "skipped_low_liquidity": 0,
        "skipped_liquidity_unavailable": 0,
    }
    for row in rows:
        if not isinstance(row, dict):
            continue
        totals["created_count"] += _to_int(row.get("created_count"), 0)
        totals["skipped_low_ev90"] += _to_int(row.get("skipped_low_ev90"), 0)
        totals["skipped_low_liquidity"] += _to_int(row.get("skipped_low_liquidity"), 0)
        totals["skipped_liquidity_unavailable"] += _to_int(row.get("skipped_liquidity_unavailable"), 0)

    liq_d = totals["created_count"] + totals["skipped_low_liquidity"] + totals["skipped_liquidity_unavailable"]
    ev90_d = totals["created_count"] + totals["skipped_low_ev90"]
    totals["liquidity_gate_exclusion_rate"] = round(
        _safe_rate(totals["skipped_low_liquidity"] + totals["skipped_liquidity_unavailable"], liq_d), 4
    )
    totals["ev90_exclusion_rate"] = round(_safe_rate(totals["skipped_low_ev90"], ev90_d), 4)
    return totals


def main() -> int:
    parser = argparse.ArgumentParser(description="Run guarded multi-cycle autonomous loop")
    parser.add_argument("--cycles", type=int, default=5)
    parser.add_argument(
        "--results-dir",
        default=str(ROOT_DIR / "docs" / "autonomous_guarded_runs"),
        help="Directory for per-run snapshots and logs.",
    )
    parser.add_argument(
        "--stagnation-limit",
        type=int,
        default=2,
        help="Stop after N consecutive non-progress cycles.",
    )
    parser.add_argument(
        "--stop-on-empty-batch",
        action="store_true",
        help="Stop immediately when batch_size=0.",
    )
    parser.add_argument(
        "--allow-partial-batch",
        action="store_true",
        help="Pass --allow-partial-batch to run_autonomous_cycle.py.",
    )
    parser.add_argument(
        "--cycle-args",
        default="",
        help="Raw args passed to scripts/run_autonomous_cycle.py",
    )
    args = parser.parse_args()

    total_cycles = max(1, int(args.cycles))
    stagnation_limit = max(1, int(args.stagnation_limit))
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    latest_review = ROOT_DIR / "docs" / "review_cycle_report_latest.json"
    latest_auto = ROOT_DIR / "docs" / "review_cycle_auto_review_latest.json"
    latest_close = ROOT_DIR / "docs" / "review_cycle_close_report_latest.json"
    latest_validation = ROOT_DIR / "docs" / "review_cycle_validation_latest.json"

    cycle_args = [tok for tok in shlex.split(str(args.cycle_args or "").strip()) if tok]

    runs: List[Dict[str, Any]] = []
    previous_cycle_id = ""
    stagnation_streak = 0
    finished_reason = "completed"

    for idx in range(1, total_cycles + 1):
        started_at = _now_iso()
        print(f"\\n[guarded] cycle {idx}/{total_cycles} start")

        cmd = [
            "python3",
            "scripts/run_autonomous_cycle.py",
            "--validation-report",
            str(latest_validation),
            "--skip-apply-when-not-ready",
            *(["--allow-partial-batch"] if bool(args.allow_partial_batch) else []),
            *cycle_args,
        ]
        result = _run_cmd(cmd)

        run_log_path = results_dir / f"run_{idx}.log"
        log_text = []
        log_text.append(f"# run {idx} | started_at={started_at} | ended_at={_now_iso()} | returncode={result.returncode}")
        log_text.append("\n## stdout\n")
        log_text.append(result.stdout or "")
        log_text.append("\n## stderr\n")
        log_text.append(result.stderr or "")
        run_log_path.write_text("\n".join(log_text), encoding="utf-8")

        review = _load_json(latest_review)
        auto = _load_json(latest_auto)
        close = _load_json(latest_close)
        validation = _load_json(latest_validation)
        review_kpi = _extract_review_kpi(review)

        cycle_id = str(review.get("cycle_id", "") or "").strip()
        batch_size = _to_int(review.get("batch_size"), 0)
        validation_ok = bool(validation.get("ok", False))
        progressed = bool(validation.get("metrics", {}).get("progressed", False))

        if batch_size <= 0 or not progressed:
            stagnation_streak += 1
        else:
            stagnation_streak = 0

        status = "ok"
        stop_reason = ""
        if result.returncode != 0:
            status = "failed"
            stop_reason = f"run_autonomous_cycle exited with {result.returncode}"
        elif previous_cycle_id and cycle_id and cycle_id == previous_cycle_id:
            status = "failed"
            stop_reason = "cycle_id did not change (stale output)"
        elif not validation_ok:
            status = "failed"
            stop_reason = "validation failed"
        elif args.stop_on_empty_batch and batch_size <= 0:
            status = "stopped"
            stop_reason = "batch_size=0"
        elif stagnation_streak >= stagnation_limit:
            status = "stopped"
            stop_reason = f"stagnation streak reached {stagnation_streak}"

        run_summary = {
            "index": idx,
            "started_at": started_at,
            "ended_at": _now_iso(),
            "returncode": int(result.returncode),
            "status": status,
            "stop_reason": stop_reason,
            "cycle_id": cycle_id,
            "batch_size": batch_size,
            "cycle_ready": bool(review.get("cycle_ready", False)),
            "validation_ok": validation_ok,
            "validation_errors": validation.get("errors", []),
            "validation_warnings": validation.get("warnings", []),
            "progressed": progressed,
            "stagnation_streak": stagnation_streak,
            "auto_counts": auto.get("counts", {}),
            "close_ready": bool(close.get("ready_for_tuning", False)),
            "close_ready_light": bool(close.get("ready_for_light_tuning", False)),
            "recommended_tuning_mode": str(close.get("recommended_tuning_mode", "") or ""),
            "review_kpi": review_kpi,
            "log_path": str(run_log_path),
        }
        runs.append(run_summary)

        run_dir = results_dir / f"run_{idx}_artifacts"
        _snapshot_file(latest_review, run_dir / "review_cycle_report.json")
        _snapshot_file(latest_auto, run_dir / "review_cycle_auto_review.json")
        _snapshot_file(latest_close, run_dir / "review_cycle_close_report.json")
        _snapshot_file(latest_validation, run_dir / "review_cycle_validation.json")

        print(
            f"[guarded] cycle {idx} status={status} batch={batch_size} "
            f"progressed={progressed} validation_ok={validation_ok} "
            f"tuning={run_summary.get('recommended_tuning_mode', '') or 'none'}"
        )
        print(
            f"[guarded] kpi liquidity_excl={review_kpi.get('liquidity_gate_exclusion_rate', 0.0):.3f} "
            f"ev90_excl={review_kpi.get('ev90_exclusion_rate', 0.0):.3f}"
        )
        if stop_reason:
            print(f"[guarded] stop_reason={stop_reason}")

        previous_cycle_id = cycle_id

        if status in {"failed", "stopped"}:
            finished_reason = stop_reason or status
            break

    aggregate = {
        "created_count": 0,
        "skipped_low_ev90": 0,
        "skipped_low_liquidity": 0,
        "skipped_liquidity_unavailable": 0,
    }
    for run in runs:
        kpi = run.get("review_kpi", {}) if isinstance(run.get("review_kpi"), dict) else {}
        aggregate["created_count"] += _to_int(kpi.get("created_count"), 0)
        aggregate["skipped_low_ev90"] += _to_int(kpi.get("skipped_low_ev90"), 0)
        aggregate["skipped_low_liquidity"] += _to_int(kpi.get("skipped_low_liquidity"), 0)
        aggregate["skipped_liquidity_unavailable"] += _to_int(kpi.get("skipped_liquidity_unavailable"), 0)
    liq_d = aggregate["created_count"] + aggregate["skipped_low_liquidity"] + aggregate["skipped_liquidity_unavailable"]
    ev90_d = aggregate["created_count"] + aggregate["skipped_low_ev90"]
    aggregate["liquidity_gate_exclusion_rate"] = round(
        _safe_rate(aggregate["skipped_low_liquidity"] + aggregate["skipped_liquidity_unavailable"], liq_d), 4
    )
    aggregate["ev90_exclusion_rate"] = round(_safe_rate(aggregate["skipped_low_ev90"], ev90_d), 4)

    summary = {
        "started_at": runs[0]["started_at"] if runs else _now_iso(),
        "ended_at": _now_iso(),
        "requested_cycles": total_cycles,
        "executed_cycles": len(runs),
        "finished_reason": finished_reason,
        "stagnation_limit": stagnation_limit,
        "aggregate_review_kpi": aggregate,
        "runs": runs,
    }
    summary_path = results_dir / "summary_latest.json"
    _save_json(summary_path, summary)
    print(f"\\n[guarded] summary: {summary_path}")
    print(f"[guarded] executed={len(runs)} finished_reason={finished_reason}")

    if runs and runs[-1].get("status") == "failed":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

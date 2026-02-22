#!/usr/bin/env python3
"""Generate a daily markdown report from git history."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import subprocess
from pathlib import Path
from typing import Dict, List, Sequence


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT_DIR / "docs" / "daily_reports"


def _run_git(args: Sequence[str]) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=ROOT_DIR,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc.stdout.strip()


def _git_config(key: str) -> str:
    proc = subprocess.run(
        ["git", "config", "--get", key],
        cwd=ROOT_DIR,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def _parse_date(raw: str) -> dt.date:
    if not raw:
        return dt.date.today()
    return dt.date.fromisoformat(raw)


def _day_window(target_day: dt.date) -> tuple[str, str]:
    return (f"{target_day.isoformat()}T00:00:00", f"{target_day.isoformat()}T23:59:59")


def _collect_commits(target_day: dt.date) -> List[Dict[str, str]]:
    since, until = _day_window(target_day)
    out = _run_git(
        [
            "log",
            "--since",
            since,
            "--until",
            until,
            "--pretty=format:%H%x09%h%x09%an%x09%ae%x09%s",
        ]
    )
    commits: List[Dict[str, str]] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t", 4)
        if len(parts) != 5:
            continue
        full_hash, short_hash, author, email, subject = parts
        commits.append(
            {
                "hash": full_hash,
                "short": short_hash,
                "author": author,
                "email": email,
                "subject": subject,
            }
        )
    commits.reverse()  # oldest -> latest
    return commits


def _collect_changed_files(target_day: dt.date) -> List[str]:
    since, until = _day_window(target_day)
    out = _run_git(
        [
            "log",
            "--since",
            since,
            "--until",
            until,
            "--name-only",
            "--pretty=format:",
        ]
    )
    files = {line.strip() for line in out.splitlines() if line.strip()}
    return sorted(files)


def _detect_scope(files: List[str]) -> List[str]:
    scope = set()
    for path in files:
        if path.startswith("docs/"):
            scope.add("Docs")
        if path.startswith("reselling/") or path.startswith("scripts/"):
            scope.add("Miner")
        if path.startswith("operator/") or path.startswith("listing_ops/"):
            scope.add("Operator")
        if path.startswith(".github/"):
            scope.add("Shared")
    if not scope:
        scope.add("Shared")
    if "Miner" in scope and "Operator" in scope:
        scope.add("Shared")
    return sorted(scope)


def _operator_id(user_arg: str) -> str:
    value = (user_arg or "").strip()
    if value:
        return value
    actor = (os.getenv("GITHUB_ACTOR", "") or "").strip()
    if actor:
        return f"@{actor}" if not actor.startswith("@") else actor
    name = _git_config("user.name")
    return name or "unknown"


def _render_report(
    *,
    target_day: dt.date,
    operator: str,
    commits: List[Dict[str, str]],
    files: List[str],
    scope: List[str],
) -> str:
    authors = sorted({f"{c['author']} <{c['email']}>" for c in commits})
    if commits:
        commit_range = f"{commits[0]['short']}..{commits[-1]['short']}"
    else:
        commit_range = "none"

    lines: List[str] = []
    lines.append(f"# Daily Report {target_day.isoformat()}")
    lines.append("")
    lines.append("## 1. Metadata")
    lines.append(f"- date: `{target_day.isoformat()}`")
    lines.append(f"- operator: `{operator}`")
    lines.append(f"- github_actor: `{(os.getenv('GITHUB_ACTOR', '') or 'n/a')}`")
    lines.append(
        f"- git_author_default: `{_git_config('user.name') or 'n/a'} <{_git_config('user.email') or 'n/a'}>`"
    )
    lines.append(
        f"- generated_at: `{dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}`"
    )
    lines.append("")
    lines.append("## 2. Commit Summary")
    lines.append(f"- commit_count: `{len(commits)}`")
    lines.append(f"- commit_range: `{commit_range}`")
    lines.append("- authors:")
    if authors:
        for author in authors:
            lines.append(f"  - `{author}`")
    else:
        lines.append("  - `none`")
    lines.append("- primary_scope:")
    for item in scope:
        lines.append(f"  - `{item}`")
    lines.append("")
    lines.append("## 3. Commits")
    if commits:
        for c in commits:
            lines.append(f"- `{c['short']}` `{c['author']}` - {c['subject']}")
    else:
        lines.append("- `none`")
    lines.append("")
    lines.append("## 4. Changed Files")
    if files:
        for path in files:
            lines.append(f"- `{path}`")
    else:
        lines.append("- `none`")
    lines.append("")
    lines.append("## 5. Validation")
    lines.append("- tests:")
    lines.append("  - `command -> result`")
    lines.append("- manual_checks:")
    lines.append("  - `check -> result`")
    lines.append("")
    lines.append("## 6. Risks / Follow-ups")
    lines.append("- `明日の着手点 / 残課題`")
    lines.append("")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate daily report markdown.")
    parser.add_argument("--date", default="", help="Target date (YYYY-MM-DD). default=today")
    parser.add_argument(
        "--operator",
        default="",
        help="Operator id for report header (example: @tadamichikimura)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print generated markdown to stdout instead of file write.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    target_day = _parse_date(args.date)
    commits = _collect_commits(target_day)
    files = _collect_changed_files(target_day)
    scope = _detect_scope(files)
    operator = _operator_id(args.operator)

    report = _render_report(
        target_day=target_day,
        operator=operator,
        commits=commits,
        files=files,
        scope=scope,
    )

    if args.stdout:
        print(report)
        return 0

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{target_day.isoformat()}.md"
    output_path.write_text(report, encoding="utf-8")
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Summarize query width pilot reports into one actionable strategy doc."""

from __future__ import annotations

import argparse
import glob
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List


def load_reports(pattern: str) -> List[Dict]:
    reports: List[Dict] = []
    for path in sorted(glob.glob(pattern)):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        sites = data.get("sites", {})
        if {"ebay", "yahoo", "rakuten"}.issubset(sites.keys()):
            reports.append({"path": path, "data": data})
    return reports


def median(values: List[int]) -> float:
    if not values:
        return 0.0
    arr = sorted(values)
    n = len(arr)
    mid = n // 2
    if n % 2 == 1:
        return float(arr[mid])
    return (arr[mid - 1] + arr[mid]) / 2.0


def build_summary(reports: List[Dict]) -> Dict:
    summary: Dict = {
        "total_reports": len(reports),
        "site_recommendation_counts": {},
        "site_stage_medians": {},
        "cases": [],
    }

    stage_counts: Dict[str, Counter] = defaultdict(Counter)
    stage_medians: Dict[str, Dict[str, List[int]]] = defaultdict(lambda: defaultdict(list))

    for wrapped in reports:
        data = wrapped["data"]
        case = {
            "report": Path(wrapped["path"]).name,
            "brand": data["inputs"]["brand"],
            "model": data["inputs"]["model"],
            "sites": {},
        }
        for site in ("ebay", "yahoo", "rakuten"):
            site_data = data["sites"][site]
            rec = site_data.get("recommended_stage")
            if rec:
                stage_counts[site][rec] += 1
            for row in site_data.get("rows", []):
                count = row.get("count")
                if isinstance(count, int) and count >= 0:
                    stage_medians[site][row["stage"]].append(count)
            case["sites"][site] = {
                "recommended_stage": rec,
                "counts": {row["stage"]: row["count"] for row in site_data.get("rows", [])},
            }
        summary["cases"].append(case)

    for site, counts in stage_counts.items():
        summary["site_recommendation_counts"][site] = dict(counts)
    for site, stage_map in stage_medians.items():
        summary["site_stage_medians"][site] = {
            stage: median(values) for stage, values in stage_map.items()
        }

    return summary


def write_markdown(summary: Dict, out_path: Path) -> None:
    lines: List[str] = []
    lines.append("# Query Width Strategy Summary")
    lines.append("")
    lines.append(f"- Samples: {summary['total_reports']} watch model runs")
    lines.append("")
    lines.append("## Recommended default waterfall")
    lines.append("")
    lines.append("1. eBay: start `L1_precise_new`; if count < 20, expand to `L2_precise`; avoid `L4_broad`.")
    lines.append("2. Yahoo: start `L2_precise`; if count < 20, expand to `L3_mid`; avoid `L4_broad`.")
    lines.append("3. Rakuten: start `L2_precise`; if count < 10, expand to `L3_mid`; only then consider `L4_broad`.")
    lines.append("")
    lines.append("## Recommendation frequency")
    lines.append("")
    for site in ("ebay", "yahoo", "rakuten"):
        counts = summary["site_recommendation_counts"].get(site, {})
        lines.append(f"- {site}: {counts}")
    lines.append("")
    lines.append("## Median hit count per stage")
    lines.append("")
    for site in ("ebay", "yahoo", "rakuten"):
        med = summary["site_stage_medians"].get(site, {})
        lines.append(f"- {site}: {med}")
    lines.append("")
    lines.append("## Per-case result")
    lines.append("")
    for case in summary["cases"]:
        lines.append(f"- {case['brand']} {case['model']} ({case['report']})")
        for site in ("ebay", "yahoo", "rakuten"):
            site_row = case["sites"][site]
            lines.append(
                f"  - {site}: rec={site_row['recommended_stage']} counts={site_row['counts']}"
            )

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize query width reports.")
    parser.add_argument(
        "--pattern",
        default="docs/query_width_report_*.json",
        help="Glob pattern for report files.",
    )
    parser.add_argument(
        "--out-json",
        default="docs/query_width_summary.json",
        help="Summary JSON output path.",
    )
    parser.add_argument(
        "--out-md",
        default="docs/query_width_strategy.md",
        help="Summary Markdown output path.",
    )
    args = parser.parse_args()

    reports = load_reports(args.pattern)
    summary = build_summary(reports)

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    out_md = Path(args.out_md)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    write_markdown(summary, out_md)

    print(f"Saved: {out_json}")
    print(f"Saved: {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

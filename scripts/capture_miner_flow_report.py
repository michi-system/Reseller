#!/usr/bin/env python3
"""Capture one Miner exploration run (UI + backend timeline) and generate two reports."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = ROOT / "docs" / "cycle_diagnostics"


RUN_CODE_FN = r"""
async function(page){
  const wait = async (ms) => { await page.waitForTimeout(ms); };
  const out = {
    startedAt: Date.now(),
    endedAt: 0,
    durationMs: 0,
    ui: [],
    backend: [],
    events: [],
    errors: [],
    fetchResponseSummary: null,
    fetchResponseHints: [],
    postQueue: null,
  };
  const summarizeFetch = (payload) => {
    const p = payload && typeof payload === "object" ? payload : {};
    const timed = p.timed_fetch && typeof p.timed_fetch === "object" ? p.timed_fetch : {};
    const seedPool = p.seed_pool && typeof p.seed_pool === "object" ? p.seed_pool : {};
    return {
      query: p.query || "",
      created_count: Number(p.created_count || 0),
      stop_reason: String(timed.stop_reason || ""),
      passes_run: Number(timed.passes_run || 0),
      stage1_pass_total: Number(timed.stage1_pass_total || 0),
      stage2_runs: Number(timed.stage2_runs || 0),
      seed_count: Number(seedPool.seed_count || seedPool.selected_seed_count || seedPool.available_after_refill || 0),
      refill_reason: String((seedPool.refill && seedPool.refill.reason) || ""),
      rpa_daily_limit_reached: Boolean(p.rpa_daily_limit_reached),
      skipped_unprofitable: Number(p.skipped_unprofitable || 0),
      skipped_low_match: Number(p.skipped_low_match || 0),
      skipped_low_margin: Number(p.skipped_low_margin || 0),
      skipped_low_liquidity: Number(p.skipped_low_liquidity || 0),
      skipped_missing_sold_min: Number(p.skipped_missing_sold_min || 0),
    };
  };

  try {
    await page.goto("http://127.0.0.1:8012/miner", { waitUntil: "domcontentloaded" });
    await page.waitForSelector("#fetchQuery", { timeout: 30000 });
    await page.waitForFunction(() => {
      const el = document.querySelector("#fetchQuery");
      return !!(el && el.options && el.options.length > 0);
    }, { timeout: 30000 });

    const fetchResponses = [];
    page.on("response", async (resp) => {
      try {
        const u = String(resp.url() || "");
        if (!u.includes("/v1/miner/fetch")) return;
        if (String(resp.request().method() || "").toUpperCase() !== "POST") return;
        const payload = await resp.json();
        fetchResponses.push(payload);
      } catch (_) {
        // ignore parse errors
      }
    });

    const selected = await page.evaluate((cat) => {
      const sel = document.querySelector("#fetchQuery");
      if (!sel) return "";
      const options = Array.from(sel.options || []);
      if (options.some((o) => String(o.value) === cat)) {
        sel.value = cat;
      } else if (options.length > 0) {
        sel.value = String(options[0].value || "");
      }
      sel.dispatchEvent(new Event("change", { bubbles: true }));
      return String(sel.value || "");
    }, "watch");
    out.events.push({ t: Date.now(), type: "category_selected", value: selected });

    await wait(1800);
    await page.click("#fetchBtn");
    out.events.push({ t: Date.now(), type: "fetch_clicked" });

    let doneStable = 0;
    const runStarted = Date.now();
    for (let i = 0; i < 700; i++) {
      const t = Date.now();
      let uiRow = null;
      let backendRow = null;
      try {
        uiRow = await page.evaluate(() => {
          const text = (sel) => {
            const el = document.querySelector(sel);
            return el && typeof el.textContent === "string" ? el.textContent.trim() : "";
          };
          const hidden = !!(document.querySelector("#rpaProgressWrap") && document.querySelector("#rpaProgressWrap").hidden);
          const fill = document.querySelector("#rpaProgressFill")?.style?.width || "";
          return {
            fetch_btn: text("#fetchBtn"),
            headline: text("#fetchStatusHeadline"),
            progress_label: text("#rpaProgressLabel"),
            progress_percent: text("#rpaProgressPercent"),
            progress_detail: text("#rpaProgressDetail"),
            progress_hidden: hidden,
            progress_fill: fill,
            seed_summary: text("#seedPoolSummary"),
            header_seed_status: text("#headerSeedStatus"),
          };
        });
      } catch (err) {
        out.errors.push({ t, phase: "ui_eval", message: String(err) });
      }
      try {
        backendRow = await page.evaluate(async () => {
          const r = await fetch("/v1/system/fetch-progress");
          if (!r.ok) {
            return { http_status: r.status };
          }
          const j = await r.json();
          return {
            status: String(j.status || ""),
            phase: String(j.phase || ""),
            progress_percent: Number(j.progress_percent || 0),
            pass_index: Number(j.pass_index || 0),
            max_passes: Number(j.max_passes || 0),
            created_count: Number(j.created_count || 0),
            seed_count: Number(j.seed_count || j.selected_seed_count || j.pool_available || 0),
            current_seed_query: String(j.current_seed_query || ""),
            stage1_pass_total: Number(j.stage1_pass_total || 0),
            stage2_runs: Number(j.stage2_runs || 0),
            message: String(j.message || ""),
            updated_at_epoch: Number(j.updated_at_epoch || 0),
            updated_ago_sec: Number(j.updated_ago_sec || 0),
            rpa: j.rpa ? {
              status: String(j.rpa.status || ""),
              phase: String(j.rpa.phase || ""),
              progress_percent: Number(j.rpa.progress_percent || 0),
              query: String(j.rpa.query || ""),
              query_index: Number(j.rpa.query_index || 0),
              total_queries: Number(j.rpa.total_queries || 0),
              updated_ago_sec: Number(j.rpa.updated_ago_sec || 0),
            } : null,
          };
        });
      } catch (err) {
        out.errors.push({ t, phase: "backend_eval", message: String(err) });
      }

      out.ui.push({ t, ...(uiRow || {}) });
      out.backend.push({ t, ...(backendRow || {}) });

      const btn = String(uiRow?.fetch_btn || "");
      const status = String(backendRow?.status || "");
      if (btn == "探索開始" && ["completed", "failed", "idle", "stopped"].includes(status)) {
        doneStable += 1;
      } else {
        doneStable = 0;
      }
      if (doneStable >= 8 && (t - runStarted) >= 5000) {
        break;
      }
      await wait(250);
    }

    if (fetchResponses.length > 0) {
      const payload = fetchResponses[fetchResponses.length - 1];
      out.fetchResponseSummary = summarizeFetch(payload);
      const hints = Array.isArray(payload.hints) ? payload.hints.map((v) => String(v || "").trim()).filter(Boolean) : [];
      out.fetchResponseHints = hints;
    }

    try {
      const queueRow = await page.evaluate(async () => {
        const q = await fetch("/v1/miner/queue?status=pending&limit=5&min_profit_usd=0.01&min_margin_rate=0.03&min_match_score=0.72&condition=new");
        if (!q.ok) {
          return { http_status: q.status };
        }
        const j = await q.json();
        const items = Array.isArray(j.items) ? j.items : [];
        return {
          total: Number(j.total || 0),
          ids: items.map((it) => Number(it.id || 0)).filter((v) => Number.isFinite(v) && v > 0).slice(0, 5),
        };
      });
      out.postQueue = queueRow;
    } catch (err) {
      out.errors.push({ t: Date.now(), phase: "post_queue", message: String(err) });
    }

    out.endedAt = Date.now();
    out.durationMs = out.endedAt - out.startedAt;
    return out;
  } catch (err) {
    out.errors.push({ t: Date.now(), phase: "fatal", message: String(err), stack: String(err && err.stack || "") });
    out.endedAt = Date.now();
    out.durationMs = out.endedAt - out.startedAt;
    return out;
  }
}
""".strip()


def _run(cmd: Sequence[str], *, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(cmd),
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _extract_result_json(output_text: str) -> Dict[str, Any]:
    m = re.search(r"### Result\s*\n(.*?)\n### Ran Playwright code", output_text, re.S)
    if not m:
        raise RuntimeError("run-code output parsing failed: Result block not found")
    raw = m.group(1).strip()
    return json.loads(raw)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False))
            f.write("\n")


def _sec(ts_ms: int, base_ms: int) -> float:
    return round((float(ts_ms) - float(base_ms)) / 1000.0, 3)


def _compress_ui(rows: List[Dict[str, Any]], started_ms: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "sec": _sec(int(row.get("t", 0) or 0), started_ms),
                "fetch_btn": str(row.get("fetch_btn", "") or ""),
                "headline": str(row.get("headline", "") or ""),
                "progress_label": str(row.get("progress_label", "") or ""),
                "progress_percent": str(row.get("progress_percent", "") or ""),
                "progress_fill": str(row.get("progress_fill", "") or ""),
                "progress_detail": str(row.get("progress_detail", "") or ""),
                "progress_hidden": bool(row.get("progress_hidden", False)),
            }
        )
    return out


def _compress_backend(rows: List[Dict[str, Any]], started_ms: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        rpa = row.get("rpa") if isinstance(row.get("rpa"), dict) else {}
        out.append(
            {
                "sec": _sec(int(row.get("t", 0) or 0), started_ms),
                "status": str(row.get("status", "") or ""),
                "phase": str(row.get("phase", "") or ""),
                "progress_percent": float(row.get("progress_percent", 0.0) or 0.0),
                "pass": f"{int(row.get('pass_index', 0) or 0)}/{int(row.get('max_passes', 0) or 0)}",
                "seed_count": int(row.get("seed_count", 0) or 0),
                "created_count": int(row.get("created_count", 0) or 0),
                "stage1_pass_total": int(row.get("stage1_pass_total", 0) or 0),
                "stage2_runs": int(row.get("stage2_runs", 0) or 0),
                "current_seed_query": str(row.get("current_seed_query", "") or ""),
                "message": str(row.get("message", "") or ""),
                "rpa_status": str(rpa.get("status", "") or ""),
                "rpa_phase": str(rpa.get("phase", "") or ""),
                "rpa_progress_percent": float(rpa.get("progress_percent", 0.0) or 0.0),
                "rpa_query": str(rpa.get("query", "") or ""),
            }
        )
    return out


def _transitions(rows: List[Dict[str, Any]], keys: Sequence[str], started_ms: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    prev: Dict[str, Any] | None = None
    for row in rows:
        cur = {k: row.get(k) for k in keys}
        if prev is None or any(cur.get(k) != prev.get(k) for k in keys):
            rec = {"sec": _sec(int(row.get("t", 0) or 0), started_ms)}
            rec.update(cur)
            out.append(rec)
            prev = cur
    return out


def _fmt_dt(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return dt.isoformat()


def _build_raw_report(
    *,
    run_id: str,
    category: str,
    result: Dict[str, Any],
    ui_rows: List[Dict[str, Any]],
    backend_rows: List[Dict[str, Any]],
    ui_transitions: List[Dict[str, Any]],
    backend_transitions: List[Dict[str, Any]],
    paths: Dict[str, Path],
) -> str:
    started_ms = int(result.get("startedAt", 0) or 0)
    ended_ms = int(result.get("endedAt", 0) or 0)
    duration_ms = int(result.get("durationMs", 0) or 0)
    fetch_summary = result.get("fetchResponseSummary") if isinstance(result.get("fetchResponseSummary"), dict) else {}
    hints = result.get("fetchResponseHints") if isinstance(result.get("fetchResponseHints"), list) else []
    errors = result.get("errors") if isinstance(result.get("errors"), list) else []
    post_queue = result.get("postQueue") if isinstance(result.get("postQueue"), dict) else {}

    lines: List[str] = []
    lines.append(f"# Miner探索 実測レポート（完全版）: {run_id}")
    lines.append("")
    lines.append("## 0. 実行メタ")
    lines.append(f"- カテゴリ: `{category}`")
    lines.append(f"- 開始(UTC): `{_fmt_dt(started_ms)}`")
    lines.append(f"- 終了(UTC): `{_fmt_dt(ended_ms)}`")
    lines.append(f"- 実行時間: `{duration_ms} ms`")
    lines.append(f"- UIサンプル数: `{len(ui_rows)}`")
    lines.append(f"- バックエンドサンプル数: `{len(backend_rows)}`")
    lines.append(f"- UI遷移イベント数: `{len(ui_transitions)}`")
    lines.append(f"- バックエンド遷移イベント数: `{len(backend_transitions)}`")
    lines.append("")
    lines.append("## 1. fetchレスポンス要約")
    lines.append("```json")
    lines.append(json.dumps(fetch_summary, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## 2. fetch hints")
    lines.append("```json")
    lines.append(json.dumps(hints, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## 3. post queue (pending)")
    lines.append("```json")
    lines.append(json.dumps(post_queue, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## 4. UI遷移イベント（全件）")
    lines.append("```json")
    lines.append(json.dumps(ui_transitions, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## 5. バックエンド遷移イベント（全件）")
    lines.append("```json")
    lines.append(json.dumps(backend_transitions, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## 6. UI時系列（全サンプル）")
    lines.append("```json")
    lines.append(json.dumps(ui_rows, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## 7. バックエンド時系列（全サンプル）")
    lines.append("```json")
    lines.append(json.dumps(backend_rows, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## 8. errors")
    lines.append("```json")
    lines.append(json.dumps(errors, ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## 9. 生データファイル")
    lines.append(f"- result json: `{paths['result_json']}`")
    lines.append(f"- ui jsonl: `{paths['ui_jsonl']}`")
    lines.append(f"- backend jsonl: `{paths['backend_jsonl']}`")
    lines.append(f"- ui transitions: `{paths['ui_transitions_json']}`")
    lines.append(f"- backend transitions: `{paths['backend_transitions_json']}`")
    return "\n".join(lines) + "\n"


def _build_readable_report(
    *,
    run_id: str,
    category: str,
    result: Dict[str, Any],
    ui_transitions: List[Dict[str, Any]],
    backend_transitions: List[Dict[str, Any]],
    paths: Dict[str, Path],
) -> str:
    started_ms = int(result.get("startedAt", 0) or 0)
    ended_ms = int(result.get("endedAt", 0) or 0)
    duration_ms = int(result.get("durationMs", 0) or 0)
    fetch_summary = result.get("fetchResponseSummary") if isinstance(result.get("fetchResponseSummary"), dict) else {}
    hints = result.get("fetchResponseHints") if isinstance(result.get("fetchResponseHints"), list) else []
    post_queue = result.get("postQueue") if isinstance(result.get("postQueue"), dict) else {}

    # Pick readable milestones
    ui_key = [
        r for r in ui_transitions
        if str(r.get("progress_percent", "")).strip()
        or str(r.get("progress_label", "")).strip()
        or str(r.get("headline", "")).strip()
    ]
    backend_key = [
        r for r in backend_transitions
        if str(r.get("status", "")).strip()
        or str(r.get("phase", "")).strip()
        or float(r.get("progress_percent", 0.0) or 0.0) > 0.0
    ]

    lines: List[str] = []
    lines.append(f"# Miner探索 実測レポート（可読版）: {run_id}")
    lines.append("")
    lines.append("## 実行サマリ")
    lines.append(f"- カテゴリ: `{category}`")
    lines.append(f"- 実行時間: `{duration_ms} ms`")
    lines.append(f"- 開始(UTC): `{_fmt_dt(started_ms)}`")
    lines.append(f"- 終了(UTC): `{_fmt_dt(ended_ms)}`")
    lines.append(f"- 作成候補数: `{int(fetch_summary.get('created_count', 0) or 0)}`")
    lines.append(f"- 停止理由: `{str(fetch_summary.get('stop_reason', '') or '')}`")
    lines.append(f"- 実行パス数: `{int(fetch_summary.get('passes_run', 0) or 0)}`")
    lines.append(f"- Seed数: `{int(fetch_summary.get('seed_count', 0) or 0)}`")
    lines.append("")
    lines.append("## フロー概要（バックエンド）")
    lines.append("1. `timed_fetch_start` で探索開始")
    lines.append("2. `seed_pool_ready` でSeed数確定")
    lines.append("3. `stage1_running` で一次判定をSeedごとに進行")
    lines.append("4. `stage2_running` で最終再判定を実行")
    lines.append("5. `pass_completed` → `completed` で終了")
    lines.append("")
    lines.append("## バックエンド進捗の主要遷移")
    lines.append("| sec | status | phase | progress% | pass | created | seed_count | message |")
    lines.append("|---:|---|---|---:|---|---:|---:|---|")
    for row in backend_key[:200]:
        lines.append(
            f"| {row.get('sec')} | {row.get('status','')} | {row.get('phase','')} | "
            f"{row.get('progress_percent',0)} | {row.get('pass_index',0)}/{row.get('max_passes',0)} | "
            f"{row.get('created_count',0)} | {row.get('seed_count',0)} | {str(row.get('message','')).replace('|','/')} |"
        )
    lines.append("")
    lines.append("## UIゲージ遷移（表示の変化）")
    lines.append("| sec | ラベル | 表示% | ヘッドライン | 詳細 |")
    lines.append("|---:|---|---|---|---|")
    for row in ui_key[:220]:
        lines.append(
            f"| {row.get('sec')} | {str(row.get('progress_label','')).replace('|','/')} | "
            f"{str(row.get('progress_percent','')).replace('|','/')} | "
            f"{str(row.get('headline','')).replace('|','/')} | "
            f"{str(row.get('progress_detail','')).replace('|','/')} |"
        )
    lines.append("")
    lines.append("## 結果の読解ポイント")
    lines.append(f"- `一次通過`: `{int(fetch_summary.get('stage1_pass_total', 0) or 0)}`")
    lines.append(f"- `最終再判定`: `{int(fetch_summary.get('stage2_runs', 0) or 0)}`")
    lines.append(f"- `除外(低利益)`: `{int(fetch_summary.get('skipped_unprofitable', 0) or 0)}`")
    lines.append(f"- `除外(一致不足)`: `{int(fetch_summary.get('skipped_low_match', 0) or 0)}`")
    lines.append(f"- `除外(低粗利率)`: `{int(fetch_summary.get('skipped_low_margin', 0) or 0)}`")
    lines.append(f"- `除外(低流動性)`: `{int(fetch_summary.get('skipped_low_liquidity', 0) or 0)}`")
    lines.append("")
    lines.append("## 補足")
    lines.append("- hints:")
    for hint in hints[:20]:
        lines.append(f"  - {hint}")
    lines.append(f"- 実行後 pending total: `{int(post_queue.get('total', 0) or 0)}`")
    lines.append("")
    lines.append("## 参照ファイル")
    lines.append(f"- 完全版レポート: `{paths['raw_report']}`")
    lines.append(f"- result json: `{paths['result_json']}`")
    lines.append(f"- ui jsonl: `{paths['ui_jsonl']}`")
    lines.append(f"- backend jsonl: `{paths['backend_jsonl']}`")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture miner flow and generate reports.")
    parser.add_argument("--category", default="watch", help="Category key to select in UI (default: watch)")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%SZ")
    session = f"minerflow-{int(time.time())}"

    codex_home = Path(os.getenv("CODEX_HOME", str(Path.home() / ".codex")))
    pwcli = codex_home / "skills" / "playwright" / "scripts" / "playwright_cli.sh"
    if not pwcli.exists():
        raise SystemExit(f"playwright wrapper missing: {pwcli}")

    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    output_prefix = DOCS_DIR / f"miner_flow_capture_{run_id}"

    open_cmd = [str(pwcli), f"-s={session}", "open", "http://127.0.0.1:8012/miner"]
    open_res = _run(open_cmd, timeout=120)
    if open_res.returncode != 0:
        print(open_res.stdout)
        print(open_res.stderr, file=sys.stderr)
        raise SystemExit("playwright open failed")

    try:
        run_cmd = [str(pwcli), f"-s={session}", "run-code", RUN_CODE_FN]
        run_res = _run(run_cmd, timeout=600)
        if run_res.returncode != 0:
            print(run_res.stdout)
            print(run_res.stderr, file=sys.stderr)
            raise SystemExit("playwright run-code failed")
        result = _extract_result_json(run_res.stdout)
    finally:
        _run([str(pwcli), f"-s={session}", "close"], timeout=60)

    ui_samples = len(result.get("ui", []) if isinstance(result.get("ui"), list) else [])
    backend_samples = len(result.get("backend", []) if isinstance(result.get("backend"), list) else [])
    errors = result.get("errors", []) if isinstance(result.get("errors"), list) else []
    fatal_errors = [e for e in errors if isinstance(e, dict) and str(e.get("phase", "")) == "fatal"]
    if fatal_errors or ui_samples == 0 or backend_samples == 0:
        _write_json(output_prefix.with_suffix(".failed.result.json"), result)
        msg = (
            "capture failed: timeline not collected "
            f"(ui_samples={ui_samples}, backend_samples={backend_samples}, fatal_errors={len(fatal_errors)})"
        )
        raise SystemExit(msg)

    started_ms = int(result.get("startedAt", 0) or 0)
    ui_rows = _compress_ui(list(result.get("ui", []) if isinstance(result.get("ui"), list) else []), started_ms)
    backend_rows = _compress_backend(list(result.get("backend", []) if isinstance(result.get("backend"), list) else []), started_ms)
    ui_transitions = _transitions(
        list(result.get("ui", []) if isinstance(result.get("ui"), list) else []),
        ["fetch_btn", "headline", "progress_label", "progress_percent", "progress_detail", "progress_fill", "progress_hidden"],
        started_ms,
    )
    backend_transitions = _transitions(
        list(result.get("backend", []) if isinstance(result.get("backend"), list) else []),
        ["status", "phase", "progress_percent", "pass_index", "max_passes", "created_count", "seed_count", "current_seed_query", "stage1_pass_total", "stage2_runs", "message"],
        started_ms,
    )

    paths = {
        "result_json": output_prefix.with_suffix(".result.json"),
        "ui_jsonl": output_prefix.with_suffix(".ui.jsonl"),
        "backend_jsonl": output_prefix.with_suffix(".backend.jsonl"),
        "ui_transitions_json": output_prefix.with_suffix(".ui.transitions.json"),
        "backend_transitions_json": output_prefix.with_suffix(".backend.transitions.json"),
        "raw_report": output_prefix.with_suffix(".raw.md"),
        "readable_report": output_prefix.with_suffix(".readable.md"),
    }

    _write_json(paths["result_json"], result)
    _write_jsonl(paths["ui_jsonl"], ui_rows)
    _write_jsonl(paths["backend_jsonl"], backend_rows)
    _write_json(paths["ui_transitions_json"], ui_transitions)
    _write_json(paths["backend_transitions_json"], backend_transitions)

    raw_report = _build_raw_report(
        run_id=run_id,
        category=args.category,
        result=result,
        ui_rows=ui_rows,
        backend_rows=backend_rows,
        ui_transitions=ui_transitions,
        backend_transitions=backend_transitions,
        paths=paths,
    )
    readable_report = _build_readable_report(
        run_id=run_id,
        category=args.category,
        result=result,
        ui_transitions=ui_transitions,
        backend_transitions=backend_transitions,
        paths=paths,
    )
    paths["raw_report"].write_text(raw_report, encoding="utf-8")
    paths["readable_report"].write_text(readable_report, encoding="utf-8")

    print("capture completed")
    for k, v in paths.items():
        print(f"{k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

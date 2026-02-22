import json
import io
import os
import subprocess
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from reselling import live_miner_fetch as fetch_mod


class LiveReviewRpaRefreshTests(unittest.TestCase):
    def _env_patch(self, output_path: str) -> dict:
        return {
            "LIQUIDITY_PROVIDER_MODE": "rpa_json",
            "LIQUIDITY_RPA_AUTO_REFRESH": "1",
            "LIQUIDITY_RPA_RUN_ON_FETCH": "1",
            "LIQUIDITY_RPA_FETCH_MIN_INTERVAL_SECONDS": "300",
            "LIQUIDITY_RPA_FETCH_FORCE_ON_SIGNAL_MISS": "1",
            "LIQUIDITY_RPA_MAX_AGE_SECONDS": "604800",
            "LIQUIDITY_RPA_JSON_PATH": output_path,
        }

    def test_cooldown_overridden_when_query_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            output_path = Path(tmp) / "signals.jsonl"
            now_ts = int(time.time())
            query = "seiko sbdc101"
            key_raw = "|".join(sorted([query.lower()]))
            cache_key = fetch_mod.hashlib.sha1(key_raw.encode("utf-8")).hexdigest()
            state_path.write_text(
                json.dumps(
                    {
                        cache_key: {
                            "last_run_at": now_ts,
                            "queries": [query],
                        }
                    }
                ),
                encoding="utf-8",
            )
            output_path.write_text(
                json.dumps(
                    {
                        "query": "casio g-shock",
                        "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
                            "+00:00", "Z"
                        ),
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            with patch.object(fetch_mod, "_RPA_FETCH_STATE_PATH", state_path):
                with patch.dict(os.environ, self._env_patch(str(output_path)), clear=False):
                    with patch.object(
                        fetch_mod,
                        "_run_rpa_collect_for_fetch",
                        return_value={
                            "enabled": True,
                            "ran": True,
                            "reason": "ok",
                            "returncode": 0,
                            "queries": [query],
                            "query_count": 1,
                        },
                    ) as mocked:
                        result = fetch_mod._maybe_refresh_rpa_for_fetch([query], force=False)
            self.assertTrue(bool(result.get("ran")))
            self.assertEqual(result.get("cooldown_override_reason"), "signal_missing")
            self.assertEqual(result.get("missing_queries"), [query])
            mocked.assert_called_once()

    def test_cooldown_skip_when_query_exists_recently(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            output_path = Path(tmp) / "signals.jsonl"
            now_ts = int(time.time())
            query = "seiko sbdc101"
            key_raw = "|".join(sorted([query.lower()]))
            cache_key = fetch_mod.hashlib.sha1(key_raw.encode("utf-8")).hexdigest()
            state_path.write_text(
                json.dumps(
                    {
                        cache_key: {
                            "last_run_at": now_ts,
                            "queries": [query],
                        }
                    }
                ),
                encoding="utf-8",
            )
            output_path.write_text(
                json.dumps(
                    {
                        "query": query,
                        "sold_90d_count": 4,
                        "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
                            "+00:00", "Z"
                        ),
                        "metadata": {
                            "sold_sample": {
                                "item_url": "https://www.ebay.com/itm/123456789012",
                                "image_url": "https://i.ebayimg.com/images/g/sample/s-l1600.jpg",
                                "title": "SEIKO SBDC101",
                            }
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            with patch.object(fetch_mod, "_RPA_FETCH_STATE_PATH", state_path):
                with patch.dict(os.environ, self._env_patch(str(output_path)), clear=False):
                    with patch.object(fetch_mod, "_run_rpa_collect_for_fetch") as mocked:
                        result = fetch_mod._maybe_refresh_rpa_for_fetch([query], force=False)
            self.assertFalse(bool(result.get("ran")))
            self.assertEqual(str(result.get("reason", "")), "cooldown_skip")
            mocked.assert_not_called()

    def test_signal_miss_override_respects_backoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            output_path = Path(tmp) / "signals.jsonl"
            now_ts = int(time.time())
            query = "seiko sbdc101"
            key_raw = "|".join(sorted([query.lower()]))
            cache_key = fetch_mod.hashlib.sha1(key_raw.encode("utf-8")).hexdigest()
            state_path.write_text(
                json.dumps(
                    {
                        cache_key: {
                            "last_run_at": now_ts,
                            "last_signal_miss_override_at": now_ts,
                            "queries": [query],
                        }
                    }
                ),
                encoding="utf-8",
            )
            output_path.write_text("", encoding="utf-8")
            env = self._env_patch(str(output_path))
            env["LIQUIDITY_RPA_FETCH_FORCE_ON_SIGNAL_MISS_MIN_INTERVAL_SECONDS"] = "1800"
            with patch.object(fetch_mod, "_RPA_FETCH_STATE_PATH", state_path):
                with patch.dict(os.environ, env, clear=False):
                    with patch.object(fetch_mod, "_run_rpa_collect_for_fetch") as mocked:
                        result = fetch_mod._maybe_refresh_rpa_for_fetch([query], force=False)
            self.assertFalse(bool(result.get("ran")))
            self.assertEqual(str(result.get("reason", "")), "cooldown_skip_signal_miss_backoff")
            self.assertGreaterEqual(int(result.get("next_signal_miss_retry_sec", 0)), 1)
            mocked.assert_not_called()

    def test_recent_query_without_sold_sample_is_fresh_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "signals.jsonl"
            query = "seiko sbdc101"
            output_path.write_text(
                json.dumps(
                    {
                        "query": query,
                        "sold_90d_count": 3,
                        "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
                            "+00:00", "Z"
                        ),
                        "metadata": {},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "LIQUIDITY_RPA_FETCH_REQUIRE_SOLD_SAMPLE_FOR_FRESH": "0",
                    "LIQUIDITY_RPA_FETCH_REQUIRE_SOLD_COUNT_FOR_FRESH": "1",
                },
                clear=False,
            ):
                recent = fetch_mod._load_recent_rpa_queries(output_path, max_age_sec=604800)
        self.assertIn(query, recent)

    def test_recent_query_requires_sold_sample_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "signals.jsonl"
            query = "seiko sbdc101"
            output_path.write_text(
                json.dumps(
                    {
                        "query": query,
                        "sold_90d_count": 3,
                        "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
                            "+00:00", "Z"
                        ),
                        "metadata": {},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {
                    "LIQUIDITY_RPA_FETCH_REQUIRE_SOLD_SAMPLE_FOR_FRESH": "1",
                    "LIQUIDITY_RPA_FETCH_REQUIRE_SOLD_COUNT_FOR_FRESH": "1",
                },
                clear=False,
            ):
                recent = fetch_mod._load_recent_rpa_queries(output_path, max_age_sec=604800)
        self.assertNotIn(query, recent)

    def test_short_term_no_gain_accepts_sold_first_preselection_stop(self) -> None:
        result = {
            "created_count": 0,
            "fetched": {
                "ebay": {"ok": True, "stop_reason": "query_exhausted", "calls_made": 1},
                "rakuten": {"ok": True, "stop_reason": "skipped_by_sold_first_preselection", "calls_made": 0},
                "yahoo": {"ok": True, "stop_reason": "skipped_by_sold_first_preselection", "calls_made": 0},
                "source_budget_filter": {"enabled": True, "kept": 0, "dropped": 0},
            },
        }
        self.assertTrue(fetch_mod._is_short_term_no_gain_result(result))

    def test_site_scope_done_for_query_exhausted_and_preselection_skip(self) -> None:
        self.assertTrue(fetch_mod._is_site_scope_done({"ok": True, "stop_reason": "query_exhausted", "calls_made": 1}))
        self.assertFalse(fetch_mod._is_site_scope_done({"ok": True, "stop_reason": "query_exhausted", "calls_made": 0}))
        self.assertTrue(
            fetch_mod._is_site_scope_done({"ok": True, "stop_reason": "skipped_by_sold_first_preselection"})
        )

    def test_daily_limit_message_detection(self) -> None:
        text = "You've exceeded the number of requests allowed in one day. Please try again tomorrow."
        self.assertTrue(fetch_mod._contains_rpa_daily_limit_message(text))

    def test_parse_progress_line(self) -> None:
        payload = {"phase": "filters_done", "progress_percent": 67, "message": "ok"}
        line = f"[progress] {json.dumps(payload)}"
        parsed = fetch_mod._parse_rpa_progress_line(line)
        self.assertEqual(parsed.get("phase"), "filters_done")
        self.assertEqual(int(parsed.get("progress_percent")), 67)

    def test_rpa_progress_snapshot_defaults_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rpa_progress.json"
            with patch.object(fetch_mod, "_RPA_PROGRESS_PATH", path):
                snap = fetch_mod.get_rpa_progress_snapshot()
        self.assertEqual(str(snap.get("status")), "idle")
        self.assertIn("updated_ago_sec", snap)

    def test_run_rpa_collect_maps_daily_limit_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "signals.jsonl"

            class _FakePopen:
                def __init__(self, *_args, **_kwargs) -> None:
                    self.stdout = io.StringIO(
                        "You've exceeded the number of requests allowed in one day. Please try again tomorrow.\n"
                    )
                    self.stderr = io.StringIO("")

                def wait(self, timeout=None) -> int:
                    _ = timeout
                    return 1

                def kill(self) -> None:
                    return None

            with patch.object(fetch_mod, "_resolve_rpa_output_path", return_value=output_path):
                with patch.object(fetch_mod.subprocess, "Popen", side_effect=_FakePopen):
                    result = fetch_mod._run_rpa_collect_for_fetch(["seiko sbdc101"])
        self.assertEqual(str(result.get("reason", "")), "daily_limit_reached")
        self.assertTrue(bool(result.get("daily_limit_reached")))
        self.assertTrue(bool(result.get("ran")))

    def test_force_headless_appends_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "signals.jsonl"
            captured = {"cmd": []}

            class _FakePopen:
                def __init__(self, cmd, **_kwargs) -> None:
                    captured["cmd"] = list(cmd)
                    self.stdout = io.StringIO("")
                    self.stderr = io.StringIO("")

                def wait(self, timeout=None) -> int:
                    _ = timeout
                    return 0

                def kill(self) -> None:
                    return None

            env = self._env_patch(str(output_path))
            env["LIQUIDITY_RPA_HEADLESS"] = "0"
            env["LIQUIDITY_RPA_FORCE_HEADLESS"] = "1"
            with patch.dict(os.environ, env, clear=False):
                with patch.object(fetch_mod, "_resolve_rpa_output_path", return_value=output_path):
                    with patch.object(fetch_mod.subprocess, "Popen", side_effect=_FakePopen):
                        fetch_mod._run_rpa_collect_for_fetch(["seiko sbdc101"])
            self.assertIn("--headless", captured["cmd"])


if __name__ == "__main__":
    unittest.main()

import json
import os
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from reselling import live_review_fetch as fetch_mod


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


if __name__ == "__main__":
    unittest.main()

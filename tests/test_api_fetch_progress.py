import unittest
from unittest.mock import patch

from reselling import api_server


class ApiFetchProgressTests(unittest.TestCase):
    def tearDown(self) -> None:
        api_server._FETCH_PROGRESS_STATE.clear()
        api_server._FETCH_PROGRESS_STATE.update(api_server._default_fetch_progress())

    def test_snapshot_includes_rpa_payload(self) -> None:
        with patch.object(
            api_server,
            "get_rpa_progress_snapshot",
            return_value={"status": "idle", "phase": "idle", "progress_percent": 0},
        ):
            snap = api_server._get_fetch_progress_snapshot()
        self.assertEqual(str(snap.get("status")), "idle")
        self.assertIn("rpa", snap)
        self.assertEqual(str(snap["rpa"].get("status")), "idle")

    def test_running_snapshot_blends_rpa_progress(self) -> None:
        api_server._set_fetch_progress(
            {
                "status": "running",
                "phase": "pass_running",
                "message": "探索中",
                "progress_percent": 50,
            }
        )
        with patch.object(
            api_server,
            "get_rpa_progress_snapshot",
            return_value={
                "status": "running",
                "phase": "filters_done",
                "progress_percent": 90,
                "message": "フィルタ完了",
                "updated_ago_sec": 1,
            },
        ):
            snap = api_server._get_fetch_progress_snapshot()
        self.assertEqual(str(snap.get("status")), "running")
        self.assertGreater(float(snap.get("progress_percent", 0.0)), 50.0)

    def test_set_fetch_progress_is_monotonic_while_running_same_run(self) -> None:
        api_server._set_fetch_progress(
            {
                "status": "running",
                "phase": "pass_running",
                "message": "探索中",
                "progress_percent": 52,
                "run_id": "run-1",
            }
        )
        snap = api_server._set_fetch_progress(
            {
                "status": "running",
                "phase": "pass_running",
                "message": "探索中",
                "progress_percent": 31,
                "run_id": "run-1",
            }
        )
        self.assertGreaterEqual(float(snap.get("progress_percent", 0.0)), 52.0)

    def test_running_snapshot_ignores_stale_completed_rpa(self) -> None:
        api_server._set_fetch_progress(
            {
                "status": "running",
                "phase": "pass_running",
                "message": "探索中",
                "progress_percent": 33,
                "started_at_epoch": 1700000000,
                "run_id": "run-2",
            }
        )
        with patch.object(
            api_server,
            "get_rpa_progress_snapshot",
            return_value={
                "status": "completed",
                "phase": "completed",
                "progress_percent": 100,
                "message": "old run",
                "updated_ago_sec": 1,
                "started_at_epoch": 1699999900,
            },
        ):
            snap = api_server._get_fetch_progress_snapshot()
        self.assertEqual(str(snap.get("status")), "running")
        self.assertAlmostEqual(float(snap.get("progress_percent", 0.0)), 33.0, places=2)


if __name__ == "__main__":
    unittest.main()

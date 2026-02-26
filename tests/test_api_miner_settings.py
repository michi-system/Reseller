import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from reselling import api_server


class ApiMinerSettingsTests(unittest.TestCase):
    def test_load_defaults_when_not_saved(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "reseller.db"
            with patch("reselling.db_runtime.resolve_backend", return_value="sqlite"):
                loaded = api_server._load_miner_ui_settings(db_path)
            self.assertEqual(loaded, dict(api_server._MINER_UI_SETTINGS_DEFAULTS))

    def test_save_and_load_sanitized_settings(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "reseller.db"
            with patch("reselling.db_runtime.resolve_backend", return_value="sqlite"):
                saved = api_server._save_miner_ui_settings(
                    db_path,
                    {
                        "limitPerSite": 999,
                        "maxCandidates": -5,
                        "stageABigWordLimit": 51,
                        "stageAMinimizeTransitions": "0",
                        "stageBQueryMode": "invalid_mode",
                        "stageBMaxQueriesPerSite": 9,
                        "stageBTopMatchesPerSeed": 0,
                        "stageBApiMaxCallsPerRun": 3000,
                        "stageCMinSold90d": -1,
                        "stageCLiquidityRefreshEnabled": 0,
                        "stageCLiquidityRefreshBudget": 400,
                        "stageCAllowMissingSoldSample": "true",
                        "stageCEbayItemDetailEnabled": "false",
                        "stageCEbayItemDetailMaxFetch": 9999,
                        "minMatchScore": 0.1,
                        "minProfitUsd": -4,
                        "minMarginRate": 9,
                    },
                )
            self.assertEqual(saved["limitPerSite"], 30)
            self.assertEqual(saved["maxCandidates"], 1)
            self.assertEqual(saved["stageABigWordLimit"], 50)
            self.assertFalse(saved["stageAMinimizeTransitions"])
            self.assertEqual(saved["stageBQueryMode"], "seed_only")
            self.assertEqual(saved["stageBMaxQueriesPerSite"], 4)
            self.assertEqual(saved["stageBTopMatchesPerSeed"], 1)
            self.assertEqual(saved["stageBApiMaxCallsPerRun"], 2000)
            self.assertEqual(saved["stageCMinSold90d"], 0)
            self.assertFalse(saved["stageCLiquidityRefreshEnabled"])
            self.assertEqual(saved["stageCLiquidityRefreshBudget"], 200)
            self.assertTrue(saved["stageCAllowMissingSoldSample"])
            self.assertFalse(saved["stageCEbayItemDetailEnabled"])
            self.assertEqual(saved["stageCEbayItemDetailMaxFetch"], 500)
            self.assertEqual(saved["minMatchScore"], 0.5)
            self.assertEqual(saved["minProfitUsd"], 0.0)
            self.assertEqual(saved["minMarginRate"], 1.0)

            with patch("reselling.db_runtime.resolve_backend", return_value="sqlite"):
                loaded = api_server._load_miner_ui_settings(db_path)
            self.assertEqual(loaded, saved)


if __name__ == "__main__":
    unittest.main()

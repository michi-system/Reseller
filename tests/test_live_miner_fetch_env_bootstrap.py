import os
import unittest
from unittest.mock import patch

from reselling import live_miner_fetch


class LiveMinerFetchEnvBootstrapTests(unittest.TestCase):
    def test_first_env_loads_dotenv_when_missing(self) -> None:
        original_loaded = live_miner_fetch._LIVE_FETCH_DOTENV_LOADED
        try:
            live_miner_fetch._LIVE_FETCH_DOTENV_LOADED = False
            called = {"count": 0}

            def _fake_load_dotenv(_path) -> None:
                called["count"] += 1
                os.environ["YAHOO_APP_ID"] = "dotenv-yahoo-app-id"

            with patch.dict(os.environ, {"YAHOO_APP_ID": "", "YAHOO_CLIENT_ID": ""}, clear=False), patch.object(
                live_miner_fetch, "load_dotenv", side_effect=_fake_load_dotenv
            ):
                first = live_miner_fetch._first_env("YAHOO_APP_ID", "YAHOO_CLIENT_ID")
                second = live_miner_fetch._first_env("YAHOO_APP_ID", "YAHOO_CLIENT_ID")

            self.assertEqual(first, "dotenv-yahoo-app-id")
            self.assertEqual(second, "dotenv-yahoo-app-id")
            self.assertEqual(called["count"], 1)
        finally:
            live_miner_fetch._LIVE_FETCH_DOTENV_LOADED = original_loaded

    def test_first_env_does_not_load_dotenv_when_env_exists(self) -> None:
        original_loaded = live_miner_fetch._LIVE_FETCH_DOTENV_LOADED
        try:
            live_miner_fetch._LIVE_FETCH_DOTENV_LOADED = False
            with patch.dict(os.environ, {"RAKUTEN_APPLICATION_ID": "inline-app-id"}, clear=False), patch.object(
                live_miner_fetch, "load_dotenv"
            ) as mocked_load:
                value = live_miner_fetch._first_env("RAKUTEN_APPLICATION_ID")
            self.assertEqual(value, "inline-app-id")
            mocked_load.assert_not_called()
        finally:
            live_miner_fetch._LIVE_FETCH_DOTENV_LOADED = original_loaded


if __name__ == "__main__":
    unittest.main()

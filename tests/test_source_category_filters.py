import os
import time
import unittest
import urllib.parse
from unittest.mock import patch

from reselling import live_miner_fetch


class SourceCategoryFilterTests(unittest.TestCase):
    def setUp(self) -> None:
        live_miner_fetch._YAHOO_RATE_LIMIT_STATE["streak"] = 0
        live_miner_fetch._YAHOO_RATE_LIMIT_STATE["until_ts"] = 0.0

    def test_search_rakuten_applies_category_and_price_cap(self) -> None:
        seen_queries = []

        def fake_request(url: str, **_kwargs):
            parsed = urllib.parse.urlparse(url)
            q = urllib.parse.parse_qs(parsed.query)
            seen_queries.append(q)
            return (
                200,
                {},
                {
                    "count": 0,
                    "Items": [],
                },
            )

        with patch.dict(
            os.environ,
            {
                "RAKUTEN_APPLICATION_ID": "app-test",
                "RAKUTEN_PUBLIC_KEY": "public-key",
                "MINER_ACTIVE_SEED_MAX_PRICE_JPY": "18000",
            },
            clear=False,
        ), patch.object(
            live_miner_fetch,
            "_resolve_category_filter_for_site",
            return_value={"applied": True, "genreId": "558929", "source": "test"},
        ), patch.object(
            live_miner_fetch,
            "_request_with_retry",
            side_effect=fake_request,
        ):
            _items, info = live_miner_fetch._search_rakuten(
                "Seiko watch",
                limit=20,
                timeout=10,
                page=1,
                require_in_stock=True,
            )

        self.assertTrue(seen_queries)
        req = seen_queries[0]
        self.assertEqual(req.get("genreId", [""])[0], "558929")
        self.assertEqual(req.get("maxPrice", [""])[0], "18000")
        self.assertEqual(req.get("sort", [""])[0], "+itemPrice")
        self.assertTrue(bool((info.get("category_filter") or {}).get("applied")))

    def test_search_yahoo_applies_category_and_price_cap(self) -> None:
        seen_queries = []

        def fake_request(url: str, **_kwargs):
            parsed = urllib.parse.urlparse(url)
            q = urllib.parse.parse_qs(parsed.query)
            seen_queries.append(q)
            return (
                200,
                {},
                {
                    "totalResultsAvailable": 0,
                    "hits": [],
                },
            )

        with patch.dict(
            os.environ,
            {
                "YAHOO_APP_ID": "app-test",
                "MINER_ACTIVE_SEED_MAX_PRICE_JPY": "22000",
            },
            clear=False,
        ), patch.object(
            live_miner_fetch,
            "_resolve_category_filter_for_site",
            return_value={"applied": True, "genre_category_id": "2498", "source": "test"},
        ), patch.object(
            live_miner_fetch,
            "_request_with_retry",
            side_effect=fake_request,
        ):
            _items, info = live_miner_fetch._search_yahoo(
                "Seiko watch",
                limit=30,
                timeout=10,
                page=1,
                require_in_stock=True,
            )

        self.assertTrue(seen_queries)
        req = seen_queries[0]
        self.assertEqual(req.get("genre_category_id", [""])[0], "2498")
        self.assertEqual(req.get("price_to", [""])[0], "22000")
        self.assertEqual(req.get("sort", [""])[0], "+price")
        self.assertTrue(bool((info.get("category_filter") or {}).get("applied")))

    def test_search_yahoo_429_arms_rate_limit_circuit(self) -> None:
        def fake_request(_url: str, **_kwargs):
            return (
                429,
                {},
                {
                    "error": "rate_limited",
                },
            )

        with patch.dict(
            os.environ,
            {
                "YAHOO_APP_ID": "app-test",
                "YAHOO_RATE_LIMIT_CIRCUIT_ENABLED": "1",
                "YAHOO_RATE_LIMIT_CIRCUIT_HOLD_SECONDS": "45",
            },
            clear=False,
        ), patch.object(
            live_miner_fetch,
            "_resolve_category_filter_for_site",
            return_value={"applied": False},
        ), patch.object(
            live_miner_fetch,
            "_request_with_retry",
            side_effect=fake_request,
        ):
            with self.assertRaisesRegex(ValueError, r"Yahoo検索失敗: http=429 .*retry_after_sec=45"):
                live_miner_fetch._search_yahoo("Seiko watch", limit=30, timeout=10)

        self.assertGreater(float(live_miner_fetch._YAHOO_RATE_LIMIT_STATE.get("until_ts", 0.0) or 0.0), 0.0)

    def test_search_yahoo_uses_rate_limit_circuit_before_network(self) -> None:
        live_miner_fetch._YAHOO_RATE_LIMIT_STATE["streak"] = 1
        live_miner_fetch._YAHOO_RATE_LIMIT_STATE["until_ts"] = time.time() + 30.0

        with patch.dict(
            os.environ,
            {
                "YAHOO_APP_ID": "app-test",
                "YAHOO_RATE_LIMIT_CIRCUIT_ENABLED": "1",
            },
            clear=False,
        ), patch.object(
            live_miner_fetch,
            "_request_with_retry",
        ) as mocked_request:
            with self.assertRaisesRegex(ValueError, r"rate_limit_circuit_open"):
                live_miner_fetch._search_yahoo("Seiko watch", limit=30, timeout=10)

        mocked_request.assert_not_called()

    def test_resolve_yahoo_category_falls_back_to_item_hits(self) -> None:
        root_payload = {
            "ResultSet": {
                "0": {
                    "Result": {
                        "Categories": {
                            "Current": {"Id": "1", "Title": {"Short": "すべて"}},
                            "Children": {
                                "0": {"Id": "13457", "Title": {"Short": "ファッション"}},
                                "1": {"Id": "2500", "Title": {"Short": "家電"}},
                            },
                        }
                    }
                },
                "totalResultsReturned": "1",
            }
        }
        probe_payload = {
            "hits": [
                {
                    "genreCategory": {"id": 1650, "name": "腕時計パーツ", "depth": 4},
                    "parentGenreCategories": [
                        {"id": 13457, "name": "ファッション", "depth": 1},
                        {"id": 2496, "name": "腕時計、アクセサリー", "depth": 2},
                    ],
                },
                {
                    "genreCategory": {"id": 2497, "name": "腕時計", "depth": 3},
                    "parentGenreCategories": [
                        {"id": 13457, "name": "ファッション", "depth": 1},
                        {"id": 2496, "name": "腕時計、アクセサリー", "depth": 2},
                    ],
                },
            ]
        }

        def fake_request(url: str, **_kwargs):
            if "categorySearch" in url:
                return 200, {}, root_payload
            if "itemSearch" in url:
                return 200, {}, probe_payload
            raise AssertionError(f"unexpected url: {url}")

        watch_row = {
            "category_key": "watch",
            "display_name_ja": "腕時計",
            "aliases": ["watch", "腕時計"],
        }
        with patch.dict(os.environ, {"YAHOO_APP_ID": "app-test"}, clear=False), patch.object(
            live_miner_fetch,
            "_active_category_context",
            return_value=("watch", watch_row),
        ), patch.object(
            live_miner_fetch,
            "_load_category_site_filter_cache",
            return_value={},
        ), patch.object(
            live_miner_fetch,
            "_save_category_site_filter_cache",
            return_value=None,
        ), patch.object(
            live_miner_fetch,
            "_request_with_retry",
            side_effect=fake_request,
        ):
            resolved = live_miner_fetch._resolve_category_filter_for_site("yahoo", "watch")

        self.assertTrue(bool(resolved.get("applied")))
        self.assertEqual(str(resolved.get("source", "")), "auto_item_hits")
        self.assertEqual(str(resolved.get("genre_category_id", "")), "2497")

    def test_resolve_rakuten_category_accepts_non_numeric_app_id_with_access_key(self) -> None:
        watch_row = {
            "category_key": "watch",
            "display_name_ja": "腕時計",
            "aliases": ["watch", "腕時計"],
        }
        seen_urls: list[str] = []

        def fake_request(url: str, **_kwargs):
            seen_urls.append(url)
            if "IchibaGenre/Search" in url:
                return 200, {}, {"children": [{"child": {"genreId": "301981", "genreName": "メンズ腕時計"}}]}
            raise AssertionError(f"unexpected url: {url}")

        with patch.dict(
            os.environ,
            {
                "RAKUTEN_APPLICATION_ID": "app-test",
                "RAKUTEN_PUBLIC_KEY": "public-key",
            },
            clear=False,
        ), patch.object(
            live_miner_fetch,
            "_active_category_context",
            return_value=("watch", watch_row),
        ), patch.object(
            live_miner_fetch,
            "_load_category_site_filter_cache",
            return_value={},
        ), patch.object(
            live_miner_fetch,
            "_save_category_site_filter_cache",
            return_value=None,
        ), patch.object(
            live_miner_fetch,
            "_request_with_retry",
            side_effect=fake_request,
        ):
            resolved = live_miner_fetch._resolve_category_filter_for_site("rakuten", "watch")

        self.assertTrue(bool(resolved.get("applied")))
        self.assertEqual(str(resolved.get("source", "")), "auto_api")
        self.assertEqual(str(resolved.get("genreId", "")), "301981")
        self.assertTrue(any("accessKey=public-key" in url for url in seen_urls))

    def test_resolve_rakuten_category_falls_back_to_item_hits_when_genre_not_found(self) -> None:
        watch_row = {
            "category_key": "watch",
            "display_name_ja": "腕時計",
            "aliases": ["watch", "腕時計"],
        }
        call_count = {"value": 0}

        def fake_request(url: str, **_kwargs):
            call_count["value"] += 1
            if "IchibaGenre/Search" in url:
                return 200, {}, {"children": []}
            if "IchibaItem/Search" in url:
                return (
                    200,
                    {},
                    {
                        "Items": [
                            {"Item": {"genreId": "301981", "itemName": "CASIO G-SHOCK メンズ腕時計 GW-M5610U-1JF"}},
                            {"Item": {"genreId": "302145", "itemName": "G-SHOCK 用 交換バンド ベルト"}},
                        ]
                    },
                )
            raise AssertionError(f"unexpected url: {url}")

        with patch.dict(
            os.environ,
            {
                "RAKUTEN_APPLICATION_ID": "app-test",
                "RAKUTEN_PUBLIC_KEY": "public-key",
            },
            clear=False,
        ), patch.object(
            live_miner_fetch,
            "_active_category_context",
            return_value=("watch", watch_row),
        ), patch.object(
            live_miner_fetch,
            "_load_category_site_filter_cache",
            return_value={},
        ), patch.object(
            live_miner_fetch,
            "_save_category_site_filter_cache",
            return_value=None,
        ), patch.object(
            live_miner_fetch,
            "_request_with_retry",
            side_effect=fake_request,
        ):
            resolved = live_miner_fetch._resolve_category_filter_for_site("rakuten", "watch")

        self.assertTrue(bool(resolved.get("applied")))
        self.assertEqual(str(resolved.get("source", "")), "auto_item_hits")
        self.assertEqual(str(resolved.get("genreId", "")), "301981")
        self.assertGreaterEqual(int(call_count["value"]), 2)


if __name__ == "__main__":
    unittest.main()

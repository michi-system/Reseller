import os
import unittest
import urllib.parse
from unittest.mock import patch

from reselling import live_miner_fetch


class SourceCategoryFilterTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()

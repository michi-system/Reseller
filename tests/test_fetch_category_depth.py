import unittest
import os
from unittest.mock import patch

from reselling import live_miner_fetch


class FetchCategoryDepthTests(unittest.TestCase):
    @staticmethod
    def _make_item(query: str, idx: int) -> live_miner_fetch.MarketItem:
        return live_miner_fetch.MarketItem(
            site="ebay",
            item_id=f"{query}-{idx}",
            title=f"{query} model {idx}",
            item_url=f"https://example.com/{query}/{idx}",
            image_url="https://example.com/item.jpg",
            price=100.0 + idx,
            shipping=0.0,
            currency="USD",
            condition="new",
            identifiers={},
            raw={},
        )

    def test_category_fetch_enforces_min_query_span_before_target(self) -> None:
        profile = live_miner_fetch.SiteFetchProfile(
            site="ebay",
            max_calls=1,
            per_call_limit=80,
            target_items=20,
            min_new_items=1,
            max_pages_per_query=1,
            sleep_sec=0.0,
        )
        search_calls = []

        def fake_search(query: str, _limit: int, _timeout: int, _page: int, _require_in_stock: bool):
            search_calls.append(query)
            rows = [self._make_item(query, idx) for idx in range(10)]
            return rows, {
                "cache_hit": False,
                "budget_remaining": -1,
                "raw_total": 200,
                "raw_count": 200,
            }

        with patch.dict(os.environ, {"MINER_CATEGORY_RELEVANCE_FILTER_ENABLED": "0"}, clear=False), patch.object(
            live_miner_fetch,
            "_site_fetch_profile",
            return_value=profile,
        ), patch.object(
            live_miner_fetch,
            "_apply_fetch_tuner",
            return_value=(profile, {"enabled": True, "applied": True}),
        ), patch.object(
            live_miner_fetch,
            "_build_site_queries_with_meta",
            return_value=(
                ["Q1", "Q2", "Q3", "Q4"],
                {"applied": True, "category_key": "watch", "category_name": "腕時計"},
            ),
        ), patch.object(
            live_miner_fetch,
            "_search_ebay",
            side_effect=fake_search,
        ), patch.object(
            live_miner_fetch,
            "_load_fetch_cursor_entries",
            return_value={},
        ), patch.object(
            live_miner_fetch,
            "_save_fetch_cursor_entries",
            return_value=None,
        ):
            _items, details, err = live_miner_fetch._fetch_site_items_adaptive(
                site="ebay",
                query="watch",
                cap_site=5,
                timeout=5,
                require_in_stock=True,
            )

        self.assertIsNone(err)
        self.assertGreaterEqual(int(details.get("min_queries_before_target", 0)), 3)
        self.assertGreaterEqual(int(details.get("calls_made", 0)), 3)
        self.assertGreaterEqual(int(details.get("touched_query_count", 0)), 3)
        self.assertGreaterEqual(len(search_calls), 3)


if __name__ == "__main__":
    unittest.main()

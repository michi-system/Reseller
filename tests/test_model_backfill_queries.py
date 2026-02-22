import unittest
from unittest.mock import patch

from reselling.live_miner_fetch import (
    MarketItem,
    _collect_source_model_code_queries,
    _fetch_source_model_backfill_from_market,
    _is_specific_model_code,
    _should_skip_model_backfill_for_query,
)


def _item(title: str, price: float = 10000.0) -> MarketItem:
    return MarketItem(
        site="rakuten",
        item_id=title[:12],
        title=title,
        item_url="",
        image_url="",
        price=price,
        shipping=0.0,
        currency="JPY",
        condition="new",
        identifiers={},
        raw={},
    )


class ModelBackfillQueryTests(unittest.TestCase):
    def test_specific_model_code_accepts_common_jp_watch_codes(self) -> None:
        self.assertTrue(_is_specific_model_code("SBDC101"))
        self.assertTrue(_is_specific_model_code("SPB143"))

    def test_collects_specific_codes_with_brand_prefix(self) -> None:
        items = [
            _item("SEIKO PROSPEX SBDC101 メンズ 腕時計", 95000),
            _item("SEIKO プロスペックス SBEJ011 GMT", 120000),
            _item("SEIKO SBDC101 ダイバー", 93000),
        ]
        queries = _collect_source_model_code_queries(items, base_query="seiko", max_queries=3)
        self.assertGreaterEqual(len(queries), 2)
        self.assertEqual(queries[0], "SEIKO SBDC101")
        self.assertIn("SEIKO SBEJ011", queries)

    def test_ignores_non_specific_codes(self) -> None:
        items = [
            _item("SEIKO 5 SPORTS WATCH", 30000),
            _item("セイコー プロスペックス 腕時計", 70000),
        ]
        queries = _collect_source_model_code_queries(items, base_query="seiko", max_queries=3)
        self.assertEqual(queries, [])

    def test_source_backfill_uses_market_model_codes(self) -> None:
        market_items = [
            MarketItem(
                site="ebay",
                item_id="1",
                title="Seiko Prospex SBDC101 SPB143 New",
                item_url="",
                image_url="",
                price=650.0,
                shipping=0.0,
                currency="USD",
                condition="new",
                identifiers={},
                raw={},
            )
        ]

        def fake_search(query, _limit, _timeout, _page, _require_in_stock):
            row = _item(f"SEIKO {query} 国内在庫 新品", 90000)
            return [row], {"http": 200, "raw_total": 1, "cache_hit": False, "budget_remaining": -1}

        with patch("reselling.live_miner_fetch._search_rakuten", side_effect=fake_search):
            rows, summary = _fetch_source_model_backfill_from_market(
                market_items=market_items,
                source_sites=["rakuten"],
                timeout=5,
                cap_site=10,
                require_in_stock=True,
            )

        self.assertTrue(summary.get("ran"))
        self.assertGreaterEqual(int(summary.get("model_code_count", 0)), 1)
        self.assertGreaterEqual(len(rows), 1)

    def test_skip_model_backfill_for_specific_query_defaults_true(self) -> None:
        self.assertTrue(_should_skip_model_backfill_for_query({"GWM56101JF"}))

    def test_skip_model_backfill_can_be_disabled_by_env(self) -> None:
        with patch.dict("os.environ", {"MINER_FETCH_MODEL_BACKFILL_SKIP_ON_SPECIFIC_QUERY": "0"}, clear=False):
            self.assertFalse(_should_skip_model_backfill_for_query({"GWM56101JF"}))

    def test_skip_model_backfill_false_without_specific_codes(self) -> None:
        self.assertFalse(_should_skip_model_backfill_for_query(set()))


if __name__ == "__main__":
    unittest.main()

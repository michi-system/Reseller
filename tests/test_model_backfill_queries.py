import unittest

from reselling.live_review_fetch import MarketItem, _collect_source_model_code_queries


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


if __name__ == "__main__":
    unittest.main()

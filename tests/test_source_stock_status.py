import unittest

from reselling.live_review_fetch import MarketItem, _source_stock_status


def _item(site: str, raw: dict) -> MarketItem:
    return MarketItem(
        site=site,
        item_id="x",
        title="dummy",
        item_url="",
        image_url="",
        price=1.0,
        shipping=0.0,
        currency="JPY",
        condition="new",
        identifiers={},
        raw=raw,
    )


class SourceStockStatusTests(unittest.TestCase):
    def test_yahoo_bool_stock(self) -> None:
        self.assertEqual(_source_stock_status(_item("yahoo_shopping", {"inStock": True})), "在庫あり")
        self.assertEqual(_source_stock_status(_item("yahoo_shopping", {"inStock": False})), "在庫なし")

    def test_rakuten_availability(self) -> None:
        self.assertEqual(_source_stock_status(_item("rakuten", {"availability": 1})), "在庫あり")
        self.assertEqual(_source_stock_status(_item("rakuten", {"availability": 0})), "在庫なし")


if __name__ == "__main__":
    unittest.main()

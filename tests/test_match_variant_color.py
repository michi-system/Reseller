import unittest

from reselling.live_review_fetch import MarketItem, _match_score


def _mk(site: str, title: str) -> MarketItem:
    return MarketItem(
        site=site,
        item_id=title[:12],
        title=title,
        item_url="",
        image_url="",
        price=100.0,
        shipping=0.0,
        currency="USD" if site == "ebay" else "JPY",
        condition="new",
        identifiers={},
        raw={},
    )


class MatchVariantColorTests(unittest.TestCase):
    def test_model_code_match_not_dropped_by_variant_missing_source(self) -> None:
        source = _mk("rakuten", "CASIO G-SHOCK GWM5610U1JF メンズ 腕時計")
        market = _mk("ebay", "CASIO G-SHOCK GWM5610U1JF (B) NEW")
        score, reason = _match_score(source, market)
        self.assertGreaterEqual(score, 0.75)
        self.assertIn("model_code", reason)


if __name__ == "__main__":
    unittest.main()

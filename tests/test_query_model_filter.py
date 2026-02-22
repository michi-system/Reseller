import unittest

from reselling.live_miner_fetch import (
    MarketItem,
    _filter_items_by_query_codes,
    _preferred_liquidity_query,
    _query_specific_codes,
)


def _mk(site: str, title: str) -> MarketItem:
    return MarketItem(
        site=site,
        item_id=title[:12],
        title=title,
        item_url="",
        image_url="",
        price=100.0 if site == "ebay" else 10000.0,
        shipping=0.0,
        currency="USD" if site == "ebay" else "JPY",
        condition="new",
        identifiers={},
        raw={},
    )


class QueryModelCodeFilterTests(unittest.TestCase):
    def test_extracts_specific_query_codes(self) -> None:
        codes = _query_specific_codes("casio gwm5610-1jf watch")
        self.assertIn("GWM56101JF", codes)

    def test_filter_keeps_only_matching_model_codes(self) -> None:
        query_codes = _query_specific_codes("casio gwm5610-1jf watch")
        items = [
            _mk("rakuten", "CASIO G-SHOCK GWM5610-1JF メンズ 新品"),
            _mk("rakuten", "CASIO G-SHOCK GA-2100-1A1 メンズ 新品"),
        ]
        filtered, meta = _filter_items_by_query_codes(items=items, query_codes=query_codes)
        self.assertEqual(len(filtered), 1)
        self.assertIn("GWM5610-1JF", filtered[0].title)
        self.assertTrue(bool(meta.get("applied")))

    def test_filter_falls_back_when_all_dropped(self) -> None:
        query_codes = _query_specific_codes("seiko sbdc101 watch")
        items = [
            _mk("rakuten", "SEIKO SARY187 メンズ 新品"),
            _mk("rakuten", "SEIKO SBEJ009 メンズ 新品"),
        ]
        filtered, meta = _filter_items_by_query_codes(items=items, query_codes=query_codes)
        self.assertEqual(len(filtered), 2)
        self.assertFalse(bool(meta.get("applied")))
        self.assertTrue(bool(meta.get("fallback_no_match")))

    def test_filter_strict_mode_drops_all_when_no_code_match(self) -> None:
        query_codes = _query_specific_codes("seiko sbdc101 watch")
        items = [
            _mk("rakuten", "SEIKO SARY187 メンズ 新品"),
            _mk("rakuten", "SEIKO SBEJ009 メンズ 新品"),
        ]
        filtered, meta = _filter_items_by_query_codes(
            items=items,
            query_codes=query_codes,
            allow_fallback_no_match=False,
        )
        self.assertEqual(len(filtered), 0)
        self.assertTrue(bool(meta.get("applied")))
        self.assertTrue(bool(meta.get("strict_drop_all")))

    def test_filter_accepts_related_codes(self) -> None:
        query_codes = _query_specific_codes("citizen nb1050 watch")
        items = [
            _mk("ebay", "CITIZEN NB1050-59A NEW"),
            _mk("ebay", "CASIO GW-M5610U-1JF NEW"),
        ]
        filtered, meta = _filter_items_by_query_codes(
            items=items,
            query_codes=query_codes,
            allow_fallback_no_match=False,
        )
        self.assertEqual(len(filtered), 1)
        self.assertIn("NB1050-59A", filtered[0].title)
        self.assertTrue(bool(meta.get("applied")))

    def test_preferred_liquidity_query_uses_shared_model_code(self) -> None:
        source = _mk("rakuten", "CASIO G-SHOCK GW-M5610U-1CJF 国内正規品")
        market = _mk("ebay", "CASIO G-SHOCK GW-M5610U-1CJF New")
        out = _preferred_liquidity_query(
            source=source,
            market=market,
            base_query="watch",
            preferred_codes={"GWM5610U1CJF"},
        )
        self.assertEqual(out, "GWM5610U1CJF")

    def test_preferred_liquidity_query_does_not_fallback_to_unrelated_preferred_code(self) -> None:
        source = _mk("rakuten", "CASIO G-SHOCK 国内正規品")
        market = _mk("ebay", "CASIO G-SHOCK New")
        out = _preferred_liquidity_query(
            source=source,
            market=market,
            base_query="watch",
            preferred_codes={"SBDC101"},
        )
        self.assertEqual(out, "watch")


if __name__ == "__main__":
    unittest.main()

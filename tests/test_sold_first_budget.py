import unittest
import os
from unittest.mock import patch

from reselling.live_miner_fetch import (
    MarketItem,
    _build_sold_first_signal_lookup,
    _can_skip_source_fetch_after_preselection,
    _filter_sold_first_codes_for_query,
    _filter_source_items_by_purchase_ceiling,
    _is_related_model_code,
    _liquidity_query_key,
    _max_purchase_total_jpy_for_sale,
)


def _mk(title: str, price_jpy: float, shipping_jpy: float = 0.0) -> MarketItem:
    return MarketItem(
        site="rakuten",
        item_id=title[:12],
        title=title,
        item_url="",
        image_url="",
        price=price_jpy,
        shipping=shipping_jpy,
        currency="JPY",
        condition="new",
        identifiers={},
        raw={},
    )


class SoldFirstBudgetTests(unittest.TestCase):
    def test_liquidity_query_key_prefers_canonical_code(self) -> None:
        self.assertEqual(_liquidity_query_key("RN-AK0803Y"), "RNAK0803Y")
        self.assertEqual(_liquidity_query_key("orient classic watch"), "ORIENTCLASSICWATCH")

    def test_build_sold_first_signal_lookup_maps_code_keys(self) -> None:
        signals = {
            "RNAK0803Y": {"signal_key": "a", "query": "RN-AK0803Y"},
            "": {"signal_key": "b", "query": "ORIENT RN-AK0803Y"},
        }
        lookup = _build_sold_first_signal_lookup(signals)
        self.assertIn("RNAK0803Y", lookup)
        self.assertEqual(str(lookup["RNAK0803Y"].get("signal_key")), "a")

    def test_max_purchase_budget_positive(self) -> None:
        max_jpy = _max_purchase_total_jpy_for_sale(
            sale_total_usd=300.0,
            fx_rate=150.0,
            min_profit_usd=20.0,
            min_margin_rate=0.1,
            marketplace_fee_rate=0.13,
            payment_fee_rate=0.03,
            international_shipping_usd=18.0,
            customs_usd=0.0,
            packaging_usd=0.0,
            fixed_fee_usd=0.0,
        )
        self.assertGreater(max_jpy, 0)
        self.assertAlmostEqual(round(max_jpy, 2), 30600.0)

    def test_budget_filter_drops_over_budget(self) -> None:
        items = [
            _mk("CASIO GW-M5610U-1CJF 新品", 15000),
            _mk("CASIO GW-M5610U-1CJF 新品 高値", 22000),
            _mk("SEIKO SARY187 新品", 48000),
        ]
        with patch.dict(
            os.environ,
            {"MINER_FETCH_EBAY_SOLD_FIRST_BUDGET_SLACK_RATIO": "1.0"},
            clear=False,
        ):
            filtered, meta = _filter_source_items_by_purchase_ceiling(
                items=items,
                max_purchase_jpy_by_code={"GWM5610U1CJF": 18000.0},
                require_code_match=True,
            )
        self.assertEqual(len(filtered), 1)
        self.assertIn("GW-M5610U-1CJF", filtered[0].title)
        self.assertEqual(int(meta.get("dropped_over_budget", 0)), 1)
        self.assertEqual(int(meta.get("dropped_no_code", 0)), 1)

    def test_related_model_code_detects_prefix_variants(self) -> None:
        self.assertTrue(_is_related_model_code("NB105059A", "NB1050"))
        self.assertTrue(_is_related_model_code("FAC00009W0", "FAC00009N"))
        self.assertFalse(_is_related_model_code("GAE2100GC7AER", "GWM56101JF"))

    def test_filter_sold_first_codes_for_query_reduces_unrelated_codes(self) -> None:
        sold_codes = {"NB105059A", "GAE2100GC7AER", "NB105059H"}
        query_codes = {"NB1050"}
        filtered = _filter_sold_first_codes_for_query(sold_codes, query_codes)
        self.assertEqual(filtered, {"NB105059A", "NB105059H"})

    def test_skip_source_fetch_requires_coverage_by_multiple_sites(self) -> None:
        jp_items = [
            _mk("ORIENT RN-AK0803Y 新品", 40000),
            _mk("ORIENT RN-AK0803Y 新品 2", 42000),
        ]
        with patch.dict(
            os.environ,
            {"MINER_FETCH_SOLD_FIRST_MIN_SOURCE_SITES_BEFORE_SKIP": "2"},
            clear=False,
        ):
            skip = _can_skip_source_fetch_after_preselection(
                sold_first_codes={"RNAK0803Y"},
                jp_items=jp_items,
                cap_site=1,
                source_sites=["rakuten", "yahoo"],
            )
        self.assertFalse(skip)

    def test_skip_source_fetch_allows_when_sites_covered(self) -> None:
        jp_items = [
            MarketItem(
                site="rakuten",
                item_id="r1",
                title="ORIENT RN-AK0803Y",
                item_url="",
                image_url="",
                price=40000.0,
                shipping=0.0,
                currency="JPY",
                condition="new",
                identifiers={},
                raw={},
            ),
            MarketItem(
                site="yahoo",
                item_id="y1",
                title="ORIENT RN-AK0803Y",
                item_url="",
                image_url="",
                price=41000.0,
                shipping=0.0,
                currency="JPY",
                condition="new",
                identifiers={},
                raw={},
            ),
        ]
        with patch.dict(
            os.environ,
            {"MINER_FETCH_SOLD_FIRST_MIN_SOURCE_SITES_BEFORE_SKIP": "2"},
            clear=False,
        ):
            skip = _can_skip_source_fetch_after_preselection(
                sold_first_codes={"RNAK0803Y"},
                jp_items=jp_items,
                cap_site=1,
                source_sites=["rakuten", "yahoo"],
            )
        self.assertTrue(skip)


if __name__ == "__main__":
    unittest.main()

import unittest
import inspect
import os
from unittest.mock import patch

from reselling.live_review_fetch import (
    MarketItem,
    _has_sold_sample_reference,
    _is_strict_sold_min_basis_candidate,
    _is_implausible_sold_min,
    _query_skip_key,
    _sale_price_basis_from_signal,
    fetch_live_review_candidates,
)


class MarketPriceBasisTests(unittest.TestCase):
    def test_has_sold_sample_reference_requires_url_and_price(self) -> None:
        self.assertTrue(
            _has_sold_sample_reference(
                {
                    "item_url": "https://www.ebay.com/itm/123456789012",
                    "sold_price_usd": 120.0,
                }
            )
        )
        self.assertFalse(
            _has_sold_sample_reference(
                {
                    "item_url": "",
                    "sold_price_usd": 120.0,
                }
            )
        )
        self.assertFalse(
            _has_sold_sample_reference(
                {
                    "item_url": "https://www.ebay.com/itm/123456789012",
                    "sold_price_usd": 0.0,
                }
            )
        )

    def test_sale_basis_prefers_sold_min(self) -> None:
        market = MarketItem(
            site="ebay",
            item_id="1",
            title="sample",
            item_url="",
            image_url="",
            price=200.0,
            shipping=15.0,
            currency="USD",
            condition="new",
            identifiers={},
            raw={},
        )
        basis, basis_type, shipping = _sale_price_basis_from_signal(
            market,
            {"sold_price_median": 180.0, "metadata": {"sold_price_min": 150.0}},
        )
        self.assertEqual(basis_type, "sold_price_min_90d")
        self.assertAlmostEqual(basis, 150.0)
        self.assertAlmostEqual(shipping, 0.0)

    def test_sale_basis_uses_active_listing_when_liquidity_price_missing(self) -> None:
        market = MarketItem(
            site="ebay",
            item_id="1",
            title="sample",
            item_url="",
            image_url="",
            price=120.0,
            shipping=9.0,
            currency="USD",
            condition="new",
            identifiers={},
            raw={},
        )
        basis, basis_type, shipping = _sale_price_basis_from_signal(market, {})
        self.assertEqual(basis_type, "active_listing_price")
        self.assertAlmostEqual(basis, 120.0)
        self.assertAlmostEqual(shipping, 9.0)

    def test_sale_basis_skips_implausibly_low_min_vs_active(self) -> None:
        market = MarketItem(
            site="ebay",
            item_id="1",
            title="sample",
            item_url="",
            image_url="",
            price=220.0,
            shipping=10.0,
            currency="USD",
            condition="new",
            identifiers={},
            raw={},
        )
        basis, basis_type, shipping = _sale_price_basis_from_signal(
            market,
            {"sold_price_median": 185.0, "metadata": {"sold_price_min": 2.0}},
        )
        self.assertEqual(basis_type, "sold_price_median_fallback_90d")
        self.assertAlmostEqual(basis, 133.2)
        self.assertAlmostEqual(shipping, 0.0)

    def test_sale_basis_uses_safe_median_fallback_when_min_is_outlier(self) -> None:
        market = MarketItem(
            site="ebay",
            item_id="1",
            title="sample",
            item_url="",
            image_url="",
            price=230.0,
            shipping=12.0,
            currency="USD",
            condition="new",
            identifiers={},
            raw={},
        )
        with patch.dict(
            os.environ,
            {
                "LIQUIDITY_ALLOW_MEDIAN_FALLBACK_ON_OUTLIER": "1",
                "LIQUIDITY_MEDIAN_FALLBACK_RATIO": "0.70",
            },
            clear=False,
        ):
            basis, basis_type, shipping = _sale_price_basis_from_signal(
                market,
                {
                    "sold_price_median": 200.0,
                    "metadata": {
                        "sold_price_min": -1.0,
                        "sold_price_min_raw": 8.0,
                        "sold_price_min_outlier": True,
                    },
                },
            )
        self.assertEqual(basis_type, "sold_price_median_fallback_90d")
        self.assertAlmostEqual(basis, 140.0)
        self.assertAlmostEqual(shipping, 0.0)

    def test_sale_basis_outlier_fallback_can_be_disabled(self) -> None:
        market = MarketItem(
            site="ebay",
            item_id="1",
            title="sample",
            item_url="",
            image_url="",
            price=230.0,
            shipping=12.0,
            currency="USD",
            condition="new",
            identifiers={},
            raw={},
        )
        with patch.dict(
            os.environ,
            {
                "LIQUIDITY_ALLOW_MEDIAN_FALLBACK_ON_OUTLIER": "0",
                "LIQUIDITY_MEDIAN_FALLBACK_RATIO": "0.70",
            },
            clear=False,
        ):
            basis, basis_type, shipping = _sale_price_basis_from_signal(
                market,
                {
                    "sold_price_median": 200.0,
                    "metadata": {
                        "sold_price_min": -1.0,
                        "sold_price_min_raw": 8.0,
                        "sold_price_min_outlier": True,
                    },
                },
            )
        self.assertEqual(basis_type, "sold_price_median_90d")
        self.assertAlmostEqual(basis, 200.0)
        self.assertAlmostEqual(shipping, 0.0)

    def test_sale_basis_fallback_when_min_ratio_vs_median_is_too_low(self) -> None:
        market = MarketItem(
            site="ebay",
            item_id="1",
            title="sample",
            item_url="",
            image_url="",
            price=180.0,
            shipping=0.0,
            currency="USD",
            condition="new",
            identifiers={},
            raw={},
        )
        with patch.dict(
            os.environ,
            {
                "LIQUIDITY_ALLOW_MEDIAN_FALLBACK_ON_OUTLIER": "1",
                "LIQUIDITY_MEDIAN_FALLBACK_RATIO": "0.75",
                "LIQUIDITY_SOLD_MIN_RATIO_FLOOR_FOR_FALLBACK": "0.50",
            },
            clear=False,
        ):
            basis, basis_type, shipping = _sale_price_basis_from_signal(
                market,
                {"sold_price_median": 200.0, "metadata": {"sold_price_min": 80.0}},
            )
        self.assertEqual(basis_type, "sold_price_median_fallback_90d")
        self.assertAlmostEqual(basis, 150.0)
        self.assertAlmostEqual(shipping, 0.0)

    def test_strict_sold_min_basis_rejects_non_min_basis(self) -> None:
        self.assertFalse(
            _is_strict_sold_min_basis_candidate(
                sale_price_basis_type="sold_price_median_fallback_90d",
                sold_min_basis=53.49,
                sold_min_outlier=False,
            )
        )
        self.assertFalse(
            _is_strict_sold_min_basis_candidate(
                sale_price_basis_type="sold_price_min_90d",
                sold_min_basis=53.49,
                sold_min_outlier=True,
            )
        )
        self.assertTrue(
            _is_strict_sold_min_basis_candidate(
                sale_price_basis_type="sold_price_min_90d",
                sold_min_basis=53.49,
                sold_min_outlier=False,
            )
        )

    def test_implausible_sold_min_detects_too_low_ratio(self) -> None:
        reject, detail = _is_implausible_sold_min(
            sold_min_raw_usd=2.0,
            source_total_usd=95.0,
            active_total_usd=210.0,
            sold_min_outlier_flag=False,
        )
        self.assertTrue(reject)
        self.assertTrue(detail.get("too_low_source"))

    def test_query_skip_key_changes_when_stock_filter_changes(self) -> None:
        key_on = _query_skip_key(
            query="watch",
            market_site="ebay",
            source_sites=["rakuten", "yahoo"],
            limit_per_site=20,
            max_candidates=20,
            min_match_score=0.7,
            min_profit_usd=0.01,
            min_margin_rate=0.03,
            require_in_stock=True,
        )
        key_off = _query_skip_key(
            query="watch",
            market_site="ebay",
            source_sites=["rakuten", "yahoo"],
            limit_per_site=20,
            max_candidates=20,
            min_match_score=0.7,
            min_profit_usd=0.01,
            min_margin_rate=0.03,
            require_in_stock=False,
        )
        self.assertNotEqual(key_on, key_off)

    def test_fetch_default_min_match_score_is_policy_floor(self) -> None:
        signature = inspect.signature(fetch_live_review_candidates)
        default = signature.parameters["min_match_score"].default
        self.assertAlmostEqual(float(default), 0.75)


if __name__ == "__main__":
    unittest.main()

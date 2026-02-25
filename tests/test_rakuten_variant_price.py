import unittest
from unittest.mock import patch

from reselling.live_miner_fetch import (
    _extract_rakuten_variant_price_from_html,
    _request_text,
    _resolve_rakuten_variant_price_jpy,
    _specific_model_codes_in_title,
)


class RakutenVariantPriceTests(unittest.TestCase):
    def test_specific_model_codes_in_title_detects_multiple_codes(self) -> None:
        title = (
            "G-SHOCK 5600シリーズ DW-5600RL-1JF GW-M5610U-1JF "
            "DW-5600UE-1JF G-5600UE-1JF GW-M5610U-1BJF"
        )
        codes = _specific_model_codes_in_title(title)
        self.assertGreaterEqual(len(codes), 4)
        self.assertIn("GW-M5610U-1JF", codes)

    def test_extract_rakuten_variant_price_from_html_uses_target_model_price(self) -> None:
        html = """
        <div>型番： 01_DW-5600RL-1JF</div><div>11,440円</div>
        <div>型番： 02_GW-M5610U-1JF</div><div>19,360円</div>
        <div>型番： 03_DW-5600UE-1JF</div><div>12,980円</div>
        """
        price = _extract_rakuten_variant_price_from_html(
            html,
            target_code="GW-M5610U-1JF",
        )
        self.assertAlmostEqual(price, 19360.0)

    def test_extract_rakuten_variant_price_from_html_returns_negative_when_missing(self) -> None:
        html = "<div>型番： DW-5600RL-1JF</div><div>11,440円</div>"
        price = _extract_rakuten_variant_price_from_html(
            html,
            target_code="GW-M5610U-1JF",
        )
        self.assertLess(price, 0.0)

    def test_request_text_timeout_returns_internal_timeout_error(self) -> None:
        with patch("reselling.live_miner_fetch.urllib.request.urlopen", side_effect=TimeoutError()):
            status, headers, body = _request_text("https://example.com/rakuten/item", timeout=5)
        self.assertEqual(status, 0)
        self.assertEqual(body, "")
        self.assertEqual(str(headers.get("x-reseller-error", "")), "timeout")

    def test_resolve_rakuten_variant_price_uses_timeout_reason(self) -> None:
        with patch(
            "reselling.live_miner_fetch._request_text",
            return_value=(0, {"x-reseller-error": "timeout"}, ""),
        ):
            price, info = _resolve_rakuten_variant_price_jpy(
                item_url="https://example.com/rakuten/item",
                target_code="GW-M5610U-1JF",
                timeout=5,
            )
        self.assertLess(price, 0.0)
        self.assertEqual(str(info.get("reason", "")), "timeout")


if __name__ == "__main__":
    unittest.main()

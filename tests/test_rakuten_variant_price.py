import unittest

from reselling.live_review_fetch import (
    _extract_rakuten_variant_price_from_html,
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


if __name__ == "__main__":
    unittest.main()

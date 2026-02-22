import unittest

from reselling.live_miner_fetch import (
    MarketItem,
    _analyze_candidate_matches,
    _canonicalize_code,
)


def _item(site: str, title: str) -> MarketItem:
    return MarketItem(
        site=site,
        item_id=title[:16],
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


class AmbiguousTitleOverrideTests(unittest.TestCase):
    def test_ambiguous_title_can_pass_when_allowed_code_is_present(self) -> None:
        source = _item("rakuten", "CASIO G-SHOCK GW-M5610U-1CJF / GW-M5610U-1BJF 国内正規品")
        market = _item("ebay", "CASIO G-SHOCK GW-M5610U-1CJF New")

        blocked = _analyze_candidate_matches(
            jp_items=[source],
            ebay_items=[market],
            min_score=0.58,
            allow_ambiguous_codes=None,
        )
        self.assertEqual(int(blocked.get("skipped_ambiguous_model_title", 0)), 1)
        self.assertEqual(len(blocked.get("candidate_matches", [])), 0)

        allowed = _analyze_candidate_matches(
            jp_items=[source],
            ebay_items=[market],
            min_score=0.58,
            allow_ambiguous_codes={_canonicalize_code("GW-M5610U-1CJF")},
        )
        self.assertEqual(int(allowed.get("skipped_ambiguous_model_title", 0)), 0)
        self.assertEqual(len(allowed.get("candidate_matches", [])), 1)


if __name__ == "__main__":
    unittest.main()

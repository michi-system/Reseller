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

    def test_known_brand_conflict_is_rejected(self) -> None:
        source = _mk("rakuten", "ORIENT RA-AG0029N10B メンズ 腕時計 新品")
        market = _mk("ebay", "CASIO G-SHOCK GWM5610-1 NEW")
        score, reason = _match_score(source, market)
        self.assertEqual(score, 0.0)
        self.assertEqual(reason, "brand_conflict")

    def test_variant_color_missing_source_uses_soft_penalty(self) -> None:
        source = _mk("rakuten", "Nike Dunk Low メンズ スニーカー")
        market = _mk("ebay", "Nike Dunk Low Black White Men's Sneakers New")
        score, reason = _match_score(source, market)
        self.assertGreater(score, 0.45)
        self.assertIn(reason, {"variant_color_missing_source", "token_overlap"})

    def test_model_code_conflict_recovers_on_near_code(self) -> None:
        source = _mk("rakuten", "CASIO G-SHOCK GW-M5610U-1JF Tough Solar メンズ")
        market = _mk("ebay", "CASIO G-SHOCK GW-M5610U-1CJF Tough Solar New")
        score, reason = _match_score(source, market)
        self.assertGreaterEqual(score, 0.75)
        self.assertIn("model_code_conflict_recovered", reason)

    def test_model_code_conflict_recovers_on_multi_code_token_alignment(self) -> None:
        source = _mk("rakuten", "SEIKO PROSPEX DIVER 200M STAINLESS 6R35-00P0")
        market = _mk("ebay", "SEIKO PROSPEX DIVER 200M STAINLESS SBDC101 SPB143 NEW")
        score, reason = _match_score(source, market)
        self.assertGreaterEqual(score, 0.75)
        self.assertIn("model_code_conflict_recovered", reason)

    def test_model_code_conflict_stays_blocked_when_overlap_is_weak(self) -> None:
        source = _mk("rakuten", "CASIO G-SHOCK GW-M5610U-1JF")
        market = _mk("ebay", "CASIO G-SHOCK GAE-2100GC-7AER NEW")
        score, reason = _match_score(source, market)
        self.assertLess(score, 0.2)
        self.assertEqual(reason, "model_code_conflict")

    def test_near_code_recovery_handles_japanese_brand_notation(self) -> None:
        source = _mk("rakuten", "シチズン NB1050-59H メンズ 腕時計 新品")
        market = _mk("ebay", "CITIZEN NB1050-59A [CITIZEN COLLECTION] New")
        score, reason = _match_score(source, market)
        self.assertGreaterEqual(score, 0.75)
        self.assertIn("model_code_conflict_recovered_near_code", reason)


if __name__ == "__main__":
    unittest.main()

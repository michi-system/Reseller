import unittest

from reselling import miner_seed_pool


class SeedStage1MatchScoreTests(unittest.TestCase):
    def test_broad_seed_relaxes_when_candidate_has_model_code(self) -> None:
        score, reason = miner_seed_pool._seed_title_match_score(
            seed_query="PROSPEX",
            seed_source_title="SEIKO Prospex Diver",
            candidate_title="セイコー プロスペックス SBDC101 新品",
        )
        self.assertGreaterEqual(score, 0.64)
        self.assertEqual(reason, "token_overlap_relaxed_with_candidate_code")

    def test_specific_seed_keeps_model_code_mismatch_reject(self) -> None:
        score, reason = miner_seed_pool._seed_title_match_score(
            seed_query="SBDC101",
            seed_source_title="SEIKO SBDC101",
            candidate_title="セイコー プロスペックス SBDC103 新品",
        )
        self.assertEqual(score, 0.0)
        self.assertEqual(reason, "model_code_mismatch")

    def test_broad_seed_with_overlap_has_minimum_floor(self) -> None:
        score, reason = miner_seed_pool._seed_title_match_score(
            seed_query="G-SHOCK",
            seed_source_title="CASIO G-SHOCK",
            candidate_title="CASIO G-SHOCK GA-2100-1A1JF 新品",
        )
        self.assertGreaterEqual(score, 0.64)
        self.assertEqual(reason, "token_overlap")

    def test_liquidity_sold_min_uses_sample_fallback(self) -> None:
        signal = {
            "sold_price_min": None,
            "sold_price_median": None,
            "metadata": {"sold_sample": {"sold_price": 123.45}},
        }
        sold_min = miner_seed_pool._liquidity_sold_min_usd(signal)
        self.assertAlmostEqual(sold_min, 123.45)


if __name__ == "__main__":
    unittest.main()

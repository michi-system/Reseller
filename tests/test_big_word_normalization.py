import unittest
from unittest.mock import patch

from reselling import miner_seed_pool


class BigWordNormalizationTests(unittest.TestCase):
    def test_category_big_words_remove_new_noise(self) -> None:
        row = {
            "category_key": "watch",
            "display_name_ja": "腕時計",
            "aliases": ["watch", "腕時計", "G-SHOCK NEW"],
        }
        with patch.object(
            miner_seed_pool,
            "_build_category_seed_queries",
            return_value=(["G-SHOCK NEW", "PROSPEX NEW", "New Balance 574 NEW"], {"applied": True}),
        ):
            words = miner_seed_pool._category_big_words("watch", row)
        self.assertIn("G-SHOCK", words)
        self.assertIn("PROSPEX", words)
        self.assertIn("New Balance 574", words)
        self.assertNotIn("G-SHOCK NEW", words)
        self.assertNotIn("PROSPEX NEW", words)

    def test_category_big_words_dedupes_after_normalization(self) -> None:
        row = {
            "category_key": "watch",
            "display_name_ja": "腕時計",
            "aliases": ["G-SHOCK", "G-SHOCK NEW", "g-shock  new"],
        }
        with patch.object(
            miner_seed_pool,
            "_build_category_seed_queries",
            return_value=(["G-SHOCK NEW"], {"applied": True}),
        ):
            words = miner_seed_pool._category_big_words("watch", row)
        gshock_count = sum(1 for v in words if v.upper().replace(" ", "") == "G-SHOCK".replace(" ", ""))
        self.assertEqual(gshock_count, 1)


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import patch

from reselling import live_miner_fetch


class CategoryKnowledgeQueryTests(unittest.TestCase):
    def test_category_query_expands_with_knowledge(self) -> None:
        queries, meta = live_miner_fetch._build_site_queries_with_meta("watch", "ebay")
        self.assertTrue(meta.get("applied"))
        self.assertEqual(meta.get("category_key"), "watch")
        self.assertIn("GW-M5610U-1JF NEW", queries)

    def test_specific_model_query_does_not_trigger_category_expansion(self) -> None:
        _, meta = live_miner_fetch._build_site_queries_with_meta("seiko sbdc101 watch", "ebay")
        self.assertFalse(meta.get("applied"))

    def test_japanese_category_query_expands_with_knowledge(self) -> None:
        queries, meta = live_miner_fetch._build_site_queries_with_meta("腕時計", "yahoo")
        self.assertTrue(meta.get("applied"))
        self.assertEqual(meta.get("category_key"), "watch")
        self.assertTrue(any("GW-M5610U-1JF" in q for q in queries))

    def test_multi_word_category_query_matches(self) -> None:
        _, meta = live_miner_fetch._build_site_queries_with_meta("trading cards", "ebay")
        self.assertTrue(meta.get("applied"))
        self.assertEqual(meta.get("category_key"), "trading_cards")

    def test_seed_query_builder_handles_empty_models_without_crash(self) -> None:
        category_row = {
            "category_key": "dummy",
            "display_name_ja": "ダミー",
            "seed_brands": ["BrandA"],
            "seed_series": ["SeriesA"],
            "model_examples": [],
            "seasonality": [],
        }
        queries, meta = live_miner_fetch._build_category_seed_queries(category_row=category_row, site="ebay")
        self.assertTrue(isinstance(queries, list))
        self.assertGreaterEqual(len(queries), 1)
        self.assertTrue(meta.get("applied"))

    def test_seed_query_builder_avoids_cross_brand_series_pairing(self) -> None:
        category_row = {
            "category_key": "dummy_category",
            "display_name_ja": "ダミー",
            "seed_brands": ["BrandA", "BrandB"],
            "seed_series": ["SeriesB", "SeriesA"],
            "model_examples": [],
            "seasonality": [],
        }
        queries, _ = live_miner_fetch._build_category_seed_queries(category_row=category_row, site="ebay")
        self.assertIn("SeriesB NEW", queries)
        self.assertIn("SeriesA NEW", queries)
        self.assertIn("BrandA dummy category NEW", queries)
        self.assertIn("BrandB dummy category NEW", queries)
        self.assertNotIn("BrandA SeriesB", queries)
        self.assertNotIn("BrandB SeriesA", queries)

    def test_category_relevance_terms_include_model_and_brand(self) -> None:
        category_row = {
            "category_key": "watch",
            "display_name_ja": "腕時計",
            "seed_brands": ["CASIO"],
            "seed_series": ["G-SHOCK"],
            "model_examples": ["GWM5610-1JF", "set_number"],
            "seasonality": [],
        }
        terms = live_miner_fetch._build_category_relevance_terms(category_row)
        self.assertIn("GWM5610-1JF", terms)
        self.assertIn("CASIO", terms)
        self.assertNotIn("set_number", terms)

    def test_title_matches_category_terms(self) -> None:
        terms = ("CASIO", "GWM5610-1JF")
        self.assertTrue(live_miner_fetch._title_matches_category_terms("Casio GWM5610-1JF black", terms))
        self.assertFalse(live_miner_fetch._title_matches_category_terms("Apple Watch Series 9", terms))

    def test_match_category_row_uses_data_aliases(self) -> None:
        payload = {
            "categories": [
                {
                    "category_key": "watch",
                    "display_name_ja": "腕時計",
                    "aliases": ["メンズ 時計 カテゴリ"],
                }
            ]
        }
        with patch.object(live_miner_fetch, "_load_category_knowledge", return_value=payload):
            row = live_miner_fetch._match_category_row("メンズ 時計 カテゴリ")
        self.assertIsNotNone(row)
        self.assertEqual(str(row.get("category_key")), "watch")


if __name__ == "__main__":
    unittest.main()

import unittest

from reselling import live_review_fetch


class CategoryKnowledgeQueryTests(unittest.TestCase):
    def test_category_query_expands_with_knowledge(self) -> None:
        queries, meta = live_review_fetch._build_site_queries_with_meta("watch", "ebay")
        self.assertTrue(meta.get("applied"))
        self.assertEqual(meta.get("category_key"), "watch")
        self.assertIn("GWM5610-1JF NEW", queries)

    def test_specific_model_query_does_not_trigger_category_expansion(self) -> None:
        _, meta = live_review_fetch._build_site_queries_with_meta("seiko sbdc101 watch", "ebay")
        self.assertFalse(meta.get("applied"))

    def test_japanese_category_query_expands_with_knowledge(self) -> None:
        queries, meta = live_review_fetch._build_site_queries_with_meta("腕時計", "yahoo")
        self.assertTrue(meta.get("applied"))
        self.assertEqual(meta.get("category_key"), "watch")
        self.assertTrue(any("GWM5610-1JF" in q for q in queries))

    def test_multi_word_category_query_matches(self) -> None:
        _, meta = live_review_fetch._build_site_queries_with_meta("trading cards", "ebay")
        self.assertTrue(meta.get("applied"))
        self.assertEqual(meta.get("category_key"), "trading_cards")


if __name__ == "__main__":
    unittest.main()

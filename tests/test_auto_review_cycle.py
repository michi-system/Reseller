import importlib.util
import pathlib
import sys
import unittest


def _load_auto_review_module():
    root = pathlib.Path(__file__).resolve().parents[1]
    script_path = root / "scripts" / "auto_review_cycle.py"
    spec = importlib.util.spec_from_file_location("auto_review_cycle", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load auto_review_cycle.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class AutoReviewCycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = _load_auto_review_module()

    def test_extract_auto_part_tags_detects_jp_and_en(self) -> None:
        source_tags = self.mod._extract_auto_part_tags("トヨタ カムリ AXVH70 リヤガーニッシュ")
        market_tags = self.mod._extract_auto_part_tags("Toyota CAMRY AXVH70 Floor Mat Black")
        self.assertIn("garnish", source_tags)
        self.assertIn("floor_mat", market_tags)

    def test_evaluate_candidate_rejects_auto_part_conflict_even_when_model_matches(self) -> None:
        candidate = {
            "id": 999999,
            "status": "pending",
            "source_site": "yahoo_shopping",
            "market_site": "ebay",
            "source_item_id": "jp-1",
            "market_item_id": "v1|123456789012|0",
            "source_title": "トヨタ純正 リヤガーニッシュ ブラック カムリ AXVH70",
            "market_title": "Toyota CAMRY AXVH70 Floor Mat 70 Series 2WD 4WD Car Mat Black",
            "condition": "new",
            "match_score": 0.92,
            "expected_profit_usd": 25.0,
            "expected_margin_rate": 0.22,
            "metadata": {
                "source_item_url": "https://example.com/source",
                "market_item_url": "https://example.com/market",
                "source_image_url": "https://example.com/source.jpg",
                "market_image_url": "https://example.com/market.jpg",
                "source_price_jpy": 12000,
                "source_shipping_jpy": 0,
                "market_price_usd": 220,
                "market_shipping_usd": 0,
                "source_currency": "JPY",
                "market_currency": "USD",
                "source_condition": "new",
                "market_condition": "NEW",
                "source_identifiers": {},
                "market_identifiers": {},
                "match_reason": "model_code",
                "liquidity": {
                    "sold_90d_count": 6,
                    "metadata": {
                        "sold_price_min": 200,
                        "sold_price_min_raw": 200,
                        "sold_price_min_outlier": False,
                        "pass_label": "sold_min",
                    },
                },
                "ev90": {"score_usd": 18.0},
            },
        }
        decision, issues, reason, metrics = self.mod.evaluate_candidate(
            candidate,
            min_profit_usd=0.01,
            min_margin_rate=0.03,
            min_ev90_usd=0.0,
            min_match_score=0.75,
            min_auto_approve_score=0.90,
            min_token_jaccard=0.62,
            max_score_drift=0.25,
        )
        self.assertEqual(decision, "reject")
        self.assertIn("model", issues)
        self.assertIn("自動車部位タグが不一致", reason)
        self.assertTrue(bool(metrics.get("auto_part_conflict")))

    def test_evaluate_candidate_rejects_when_market_part_tag_missing(self) -> None:
        candidate = {
            "id": 999998,
            "status": "pending",
            "source_site": "yahoo_shopping",
            "market_site": "ebay",
            "source_item_id": "jp-2",
            "market_item_id": "v1|123456789013|0",
            "source_title": "トヨタ カムリ AXVH70 LED テール ランプ ハーネス キット",
            "market_title": "Toyota Camry AXVH70 Structural Investigation Series Custom Maintenance",
            "condition": "new",
            "match_score": 0.92,
            "expected_profit_usd": 20.0,
            "expected_margin_rate": 0.2,
            "metadata": {
                "source_price_jpy": 12000,
                "source_shipping_jpy": 0,
                "market_price_usd": 180,
                "market_shipping_usd": 0,
                "source_currency": "JPY",
                "market_currency": "USD",
                "source_condition": "new",
                "market_condition": "NEW",
                "source_identifiers": {},
                "market_identifiers": {},
                "match_reason": "model_code",
                "liquidity": {
                    "sold_90d_count": 6,
                    "metadata": {
                        "sold_price_min": 170,
                        "sold_price_min_raw": 170,
                        "sold_price_min_outlier": False,
                        "pass_label": "sold_min",
                    },
                },
                "ev90": {"score_usd": 14.0},
            },
        }
        decision, issues, reason, metrics = self.mod.evaluate_candidate(
            candidate,
            min_profit_usd=0.01,
            min_margin_rate=0.03,
            min_ev90_usd=0.0,
            min_match_score=0.75,
            min_auto_approve_score=0.90,
            min_token_jaccard=0.62,
            max_score_drift=0.25,
        )
        self.assertEqual(decision, "reject")
        self.assertIn("model", issues)
        self.assertIn("eBay側に自動車部位タグがなく", reason)
        self.assertTrue(bool(metrics.get("auto_part_missing_market")))


if __name__ == "__main__":
    unittest.main()

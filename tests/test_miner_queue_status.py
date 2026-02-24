import tempfile
import unittest
from pathlib import Path

from reselling.config import Settings
from reselling.miner import (
    approve_miner_candidate,
    create_miner_candidate,
    list_miner_queue,
    reject_miner_candidate,
)


def _settings_for(db_path: Path) -> Settings:
    return Settings(
        db_path=db_path,
        fx_provider="open_er_api",
        fx_rate_provider_url="",
        fx_rate_url_template="",
        fx_rate_json_path="rates.{QUOTE}",
        fx_api_key="",
        fx_base_ccy="USD",
        fx_quote_ccy="JPY",
        fx_usd_jpy_default=150.0,
        fx_refresh_seconds=3600,
        fx_cache_seconds=900,
    )


def _candidate_payload(idx: int) -> dict:
    return {
        "source_site": "rakuten",
        "market_site": "ebay",
        "source_item_id": f"src-{idx}",
        "market_item_id": f"mkt-{idx}",
        "source_title": f"source-{idx}",
        "market_title": f"market-{idx}",
        "condition": "new",
        "match_level": "L2_precise",
        "match_score": 0.95,
        "expected_profit_usd": 10.0,
        "expected_margin_rate": 0.1,
        "metadata": {
            "market_price_basis_type": "sold_price_min_90d",
            "ebay_sold_item_url": f"https://www.ebay.com/itm/{100000000000 + idx}",
        },
    }


class ReviewQueueStatusTests(unittest.TestCase):
    def test_reviewed_status_returns_only_reviewed_rows_and_total(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = _settings_for(Path(tmp) / "test.db")
            pending = create_miner_candidate(_candidate_payload(1), settings=settings)
            listed = create_miner_candidate(_candidate_payload(2), settings=settings)
            rejected = create_miner_candidate(_candidate_payload(3), settings=settings)

            approve_miner_candidate(int(listed["id"]), settings=settings)
            reject_miner_candidate(
                int(rejected["id"]),
                issue_targets=["price"],
                reason_text="too high",
                settings=settings,
            )

            reviewed_page = list_miner_queue(status="reviewed", limit=1, settings=settings)
            self.assertEqual(int(reviewed_page["total"]), 2)
            self.assertEqual(len(reviewed_page["items"]), 1)
            self.assertIn(str(reviewed_page["items"][0]["status"]), {"listed", "approved", "rejected"})

            pending_page = list_miner_queue(status="pending", limit=10, settings=settings)
            self.assertEqual(int(pending_page["total"]), 1)
            self.assertEqual(int(pending_page["items"][0]["id"]), int(pending["id"]))

    def test_pending_queue_keeps_non_min_price_basis(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = _settings_for(Path(tmp) / "test.db")
            ok_payload = _candidate_payload(10)
            bad_payload = _candidate_payload(11)
            bad_payload["metadata"] = {"market_price_basis_type": "sold_price_median_fallback_90d"}

            keep = create_miner_candidate(ok_payload, settings=settings)
            keep_non_min = create_miner_candidate(bad_payload, settings=settings)

            pending_page = list_miner_queue(status="pending", limit=10, settings=settings)
            ids = {int(row["id"]) for row in pending_page["items"]}
            self.assertEqual(int(pending_page["total"]), 2)
            self.assertIn(int(keep["id"]), ids)
            self.assertIn(int(keep_non_min["id"]), ids)

    def test_pending_queue_excludes_min_basis_without_sold_item_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = _settings_for(Path(tmp) / "test.db")
            keep_payload = _candidate_payload(20)
            bad_payload = _candidate_payload(21)
            bad_payload["metadata"] = {"market_price_basis_type": "sold_price_min_90d"}

            keep = create_miner_candidate(keep_payload, settings=settings)
            create_miner_candidate(bad_payload, settings=settings)

            pending_page = list_miner_queue(status="pending", limit=10, settings=settings)
            self.assertEqual(int(pending_page["total"]), 1)
            self.assertEqual(int(pending_page["items"][0]["id"]), int(keep["id"]))


if __name__ == "__main__":
    unittest.main()

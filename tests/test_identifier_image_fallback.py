import unittest
from unittest.mock import patch

from reselling.live_miner_fetch import (
    backfill_candidate_market_images,
    _extract_image_url,
    _extract_primary_model_code,
    _with_title_identifier_hints,
)


class IdentifierImageFallbackTests(unittest.TestCase):
    def test_extract_primary_model_code_ignores_numeric_coupon_token(self) -> None:
        title = "＼店内2000円OFF+さらに13倍／ シチズン BC0420-61A CITIZEN ホワイト"
        model = _extract_primary_model_code(title)
        self.assertEqual(model, "BC0420-61A")

    def test_with_title_identifier_hints_keeps_existing_and_adds_model(self) -> None:
        identifiers = _with_title_identifier_hints({"jan": "4974375454149"}, "CITIZEN BC0420-61A")
        self.assertEqual(identifiers.get("jan"), "4974375454149")
        self.assertEqual(identifiers.get("model"), "BC0420-61A")
        self.assertEqual(identifiers.get("mpn"), "BC0420-61A")

    def test_extract_image_url_reads_data_src_and_srcset(self) -> None:
        payload = {
            "thumbnailImage": {"data-src": "https://i.ebayimg.com/images/g/data-src/s-l500.webp"},
            "fallback": {"srcset": "https://i.ebayimg.com/images/g/srcset/s-l500.jpg 1x, https://i.ebayimg.com/images/g/srcset/s-l1600.jpg 2x"},
        }
        image_url = _extract_image_url(payload)
        self.assertEqual(image_url, "https://i.ebayimg.com/images/g/data-src/s-l500.webp")

    def test_backfill_candidate_market_images_uses_item_id_and_updates_metadata(self) -> None:
        candidate = {
            "id": 490,
            "market_site": "ebay",
            "market_item_id": "v1|314253529095|0",
            "metadata": {
                "market_item_url": "https://www.ebay.com/itm/314253529095",
                "market_image_url": "",
                "market_image_url_active": "",
            },
        }
        with patch("reselling.live_miner_fetch._ebay_access_token", return_value="dummy-token"), patch(
            "reselling.live_miner_fetch._ebay_fetch_item_image",
            return_value="https://i.ebayimg.com/images/g/fetched/s-l500.jpg",
        ):
            updated = backfill_candidate_market_images([candidate], timeout=8, max_calls=3)
        self.assertEqual(updated, 1)
        self.assertEqual(
            candidate["metadata"].get("market_image_url"),
            "https://i.ebayimg.com/images/g/fetched/s-l500.jpg",
        )


if __name__ == "__main__":
    unittest.main()

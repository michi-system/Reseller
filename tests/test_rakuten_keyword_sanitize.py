import os
import unittest
import urllib.parse
from unittest.mock import patch

from reselling import live_miner_fetch


class RakutenKeywordSanitizeTests(unittest.TestCase):
    def test_wrong_parameter_retries_with_sanitized_keyword(self) -> None:
        requested_keywords = []
        responses = [
            (
                400,
                {},
                {
                    "error": "wrong_parameter",
                    "error_description": "keyword is not valid",
                },
            ),
            (
                200,
                {},
                {
                    "count": 1,
                    "Items": [
                        {
                            "Item": {
                                "itemCode": "shop:test-1",
                                "itemName": "Seiko 5 Sports",
                                "itemUrl": "https://example.com/item",
                                "itemPrice": 12345,
                                "availability": 1,
                                "mediumImageUrls": [],
                                "smallImageUrls": [],
                            }
                        }
                    ],
                },
            ),
        ]

        def fake_request(url: str, **_kwargs):
            parsed = urllib.parse.urlparse(url)
            query = urllib.parse.parse_qs(parsed.query)
            requested_keywords.append(query.get("keyword", [""])[0])
            return responses.pop(0)

        with patch.dict(
            os.environ,
            {
                "RAKUTEN_APPLICATION_ID": "app-test",
                "RAKUTEN_API_BASE_URL": "https://example.com/rakuten",
            },
            clear=False,
        ), patch.object(live_miner_fetch, "_request_with_retry", side_effect=fake_request):
            items, info = live_miner_fetch._search_rakuten(
                "Seiko 5 Sports",
                limit=20,
                timeout=10,
                page=1,
                require_in_stock=True,
            )

        self.assertEqual(requested_keywords, ["Seiko 5 Sports", "Seiko5 Sports"])
        self.assertEqual(len(items), 1)
        self.assertTrue(bool(info.get("query_sanitized")))
        self.assertEqual(str(info.get("query_original", "")), "Seiko 5 Sports")
        self.assertEqual(str(info.get("query_used", "")), "Seiko5 Sports")

    def test_normal_keyword_uses_single_request(self) -> None:
        requested_keywords = []

        def fake_request(url: str, **_kwargs):
            parsed = urllib.parse.urlparse(url)
            query = urllib.parse.parse_qs(parsed.query)
            requested_keywords.append(query.get("keyword", [""])[0])
            return (
                200,
                {},
                {
                    "count": 0,
                    "Items": [],
                },
            )

        with patch.dict(
            os.environ,
            {
                "RAKUTEN_APPLICATION_ID": "app-test",
                "RAKUTEN_API_BASE_URL": "https://example.com/rakuten",
            },
            clear=False,
        ), patch.object(live_miner_fetch, "_request_with_retry", side_effect=fake_request):
            _items, info = live_miner_fetch._search_rakuten(
                "Promaster",
                limit=20,
                timeout=10,
                page=1,
                require_in_stock=True,
            )

        self.assertEqual(requested_keywords, ["Promaster"])
        self.assertFalse(bool(info.get("query_sanitized")))
        self.assertEqual(str(info.get("query_used", "")), "Promaster")

    def test_403_uses_web_fallback(self) -> None:
        def fake_request(_url: str, **_kwargs):
            return (
                403,
                {},
                {
                    "errors": {
                        "errorCode": 403,
                        "errorMessage": "CLIENT_IP_NOT_ALLOWED",
                    }
                },
            )

        fallback_html = """
        <html><head>
        <script type="application/ld+json">
        {
          "@context":"https://schema.org/",
          "@type":"ItemList",
          "itemListElement":[
            {
              "@type":"ListItem",
              "position":1,
              "item":{
                "@type":"Product",
                "name":"CASIO G-SHOCK GW-M5610U-1JF 新品",
                "url":"https://example.com/rakuten/item-1",
                "image":"https://example.com/rakuten/item-1.jpg",
                "offers":{"@type":"Offer","price":"19800","priceCurrency":"JPY"}
              }
            },
            {
              "@type":"ListItem",
              "position":2,
              "item":{
                "@type":"Product",
                "name":"G-SHOCK 用 保護フィルム",
                "url":"https://example.com/rakuten/item-2",
                "image":"https://example.com/rakuten/item-2.jpg",
                "offers":{"@type":"Offer","price":"900","priceCurrency":"JPY"}
              }
            }
          ]
        }
        </script>
        </head><body></body></html>
        """

        with patch.dict(
            os.environ,
            {
                "RAKUTEN_APPLICATION_ID": "app-test",
                "RAKUTEN_PUBLIC_KEY": "public-key",
                "RAKUTEN_API_BASE_URL": "https://example.com/rakuten",
                "RAKUTEN_WEB_FALLBACK_ENABLED": "1",
            },
            clear=False,
        ), patch.object(
            live_miner_fetch,
            "_request_with_retry",
            side_effect=fake_request,
        ), patch.object(
            live_miner_fetch,
            "_request_text",
            return_value=(200, {}, fallback_html),
        ):
            items, info = live_miner_fetch._search_rakuten(
                "G-SHOCK",
                limit=20,
                timeout=10,
                page=1,
                require_in_stock=True,
            )

        self.assertEqual(len(items), 1)
        self.assertIn("GW-M5610U-1JF", items[0].title)
        self.assertEqual(str(info.get("fallback_mode", "")), "rakuten_web_html")
        self.assertEqual(int(info.get("fallback_from_http", 0)), 403)


if __name__ == "__main__":
    unittest.main()

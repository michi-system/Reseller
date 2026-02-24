import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from reselling import liquidity


def _row(
    *,
    signal_key: str,
    query: str,
    sold_90d_count: int,
    sold_price_min: float,
    sold_price_median: float,
    sold_tab_selected: bool,
    lookback_selected: str,
    filtered_row_count: int,
    sold_sample: dict | None = None,
    url: str = "",
) -> dict:
    metadata = {
        "filter_state": {
            "sold_tab_selected": sold_tab_selected,
            "lookback_selected": lookback_selected,
        },
        "filtered_row_count": filtered_row_count,
    }
    if url:
        metadata["url"] = url
    if isinstance(sold_sample, dict):
        metadata["sold_sample"] = sold_sample
    return {
        "signal_key": signal_key,
        "query": query,
        "sold_90d_count": sold_90d_count,
        "active_count": -1,
        "sold_price_min": sold_price_min,
        "sold_price_median": sold_price_median,
        "sold_price_currency": "USD",
        "confidence": 0.9,
        "source": "ebay_product_research_rpa",
        "fetched_at": "2026-02-22T01:02:47Z",
        "metadata": metadata,
    }


class LiquidityRpaGuardTests(unittest.TestCase):
    def _call_provider(self, rows: list[dict], *, query: str, signal_key: str) -> tuple[dict | None, str]:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "signals.jsonl"
            path.write_text(
                "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n",
                encoding="utf-8",
            )
            env = {
                "LIQUIDITY_RPA_JSON_PATH": str(path),
                "LIQUIDITY_RPA_MAX_AGE_SECONDS": "604800",
                "LIQUIDITY_RPA_REQUIRE_STRICT_FILTERS": "1",
                "LIQUIDITY_RPA_REQUIRE_FILTERED_ROWS_FOR_POSITIVE_SOLD": "1",
                "LIQUIDITY_RPA_ALLOW_FUZZY_KEY_FALLBACK": "0",
            }
            with patch.dict(os.environ, env, clear=False):
                return liquidity._provider_rpa_json(
                    query=query,
                    signal_key=signal_key,
                    active_count_hint=-1,
                )

    def test_no_unrelated_fuzzy_match_by_default(self) -> None:
        signal, reason = self._call_provider(
            [
                _row(
                    signal_key="model:SRPG31",
                    query="SRPG31",
                    sold_90d_count=9,
                    sold_price_min=188.0,
                    sold_price_median=210.0,
                    sold_tab_selected=True,
                    lookback_selected="Last 90 days",
                    filtered_row_count=2,
                    sold_sample={
                        "item_url": "https://www.ebay.com/itm/123456789012",
                        "sold_price": 188.0,
                    },
                )
            ],
            query="BC0420-61A",
            signal_key="model:BC0420-61A",
        )
        self.assertIsNone(signal)
        self.assertEqual(reason, "rpa_json_no_match")

    def test_rejects_row_when_sold_tab_not_confirmed(self) -> None:
        signal, reason = self._call_provider(
            [
                _row(
                    signal_key="model:SRPG31",
                    query="SRPG31",
                    sold_90d_count=9,
                    sold_price_min=188.0,
                    sold_price_median=210.0,
                    sold_tab_selected=False,
                    lookback_selected="Last 90 days",
                    filtered_row_count=2,
                    sold_sample={
                        "item_url": "https://www.ebay.com/itm/123456789012",
                        "sold_price": 188.0,
                    },
                )
            ],
            query="SRPG31",
            signal_key="model:SRPG31",
        )
        self.assertIsNone(signal)
        self.assertEqual(reason, "rpa_json_not_strict_sold_filters")

    def test_rejects_positive_sold_without_filtered_rows_and_sample(self) -> None:
        signal, reason = self._call_provider(
            [
                _row(
                    signal_key="model:SRPG31",
                    query="SRPG31",
                    sold_90d_count=9,
                    sold_price_min=188.0,
                    sold_price_median=210.0,
                    sold_tab_selected=True,
                    lookback_selected="Last 90 days",
                    filtered_row_count=0,
                    sold_sample=None,
                )
            ],
            query="SRPG31",
            signal_key="model:SRPG31",
        )
        self.assertIsNone(signal)
        self.assertEqual(reason, "rpa_json_positive_sold_without_filtered_rows")

    def test_accepts_positive_sold_without_rows_when_url_confirms_sold_tab(self) -> None:
        signal, reason = self._call_provider(
            [
                _row(
                    signal_key="model:SRPG31",
                    query="SRPG31",
                    sold_90d_count=9,
                    sold_price_min=188.0,
                    sold_price_median=210.0,
                    sold_tab_selected=False,
                    lookback_selected="Last 90 days",
                    filtered_row_count=0,
                    sold_sample=None,
                    url="https://www.ebay.com/sh/research?keywords=SRPG31&tabName=SOLD&format=FIXED_PRICE",
                )
            ],
            query="SRPG31",
            signal_key="model:SRPG31",
        )
        self.assertEqual(reason, "")
        self.assertIsInstance(signal, dict)
        self.assertEqual(int(signal.get("sold_90d_count", -1)), 9)
        metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
        self.assertTrue(bool(metadata.get("accepted_without_filtered_rows")))

    def test_rejects_query_code_mismatch_even_when_key_matches(self) -> None:
        signal, reason = self._call_provider(
            [
                _row(
                    signal_key="model:BC0420-61A",
                    query="SRPG31",
                    sold_90d_count=9,
                    sold_price_min=188.0,
                    sold_price_median=210.0,
                    sold_tab_selected=True,
                    lookback_selected="Last 90 days",
                    filtered_row_count=2,
                    sold_sample={
                        "item_url": "https://www.ebay.com/itm/123456789012",
                        "sold_price": 188.0,
                    },
                )
            ],
            query="BC0420-61A",
            signal_key="model:BC0420-61A",
        )
        self.assertIsNone(signal)
        self.assertEqual(reason, "rpa_json_query_code_mismatch")

    def test_accepts_strict_row(self) -> None:
        signal, reason = self._call_provider(
            [
                _row(
                    signal_key="model:BC0420-61A",
                    query="BC0420-61A",
                    sold_90d_count=5,
                    sold_price_min=188.0,
                    sold_price_median=201.0,
                    sold_tab_selected=True,
                    lookback_selected="Last 90 days",
                    filtered_row_count=3,
                    sold_sample={
                        "item_url": "https://www.ebay.com/itm/123456789012",
                        "sold_price": 188.0,
                    },
                )
            ],
            query="BC0420-61A",
            signal_key="model:BC0420-61A",
        )
        self.assertEqual(reason, "")
        self.assertIsInstance(signal, dict)
        self.assertEqual(int(signal.get("sold_90d_count", -1)), 5)
        metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
        self.assertAlmostEqual(float(metadata.get("sold_price_min", -1.0)), 188.0)

    def test_accepts_mpn_key_by_aliasing_to_model_key(self) -> None:
        signal, reason = self._call_provider(
            [
                _row(
                    signal_key="model:BC0420-61A",
                    query="BC0420-61A",
                    sold_90d_count=4,
                    sold_price_min=188.0,
                    sold_price_median=201.0,
                    sold_tab_selected=True,
                    lookback_selected="Last 90 days",
                    filtered_row_count=2,
                    sold_sample={
                        "item_url": "https://www.ebay.com/itm/123456789012",
                        "sold_price": 188.0,
                    },
                )
            ],
            query="BC0420-61A",
            signal_key="mpn:BC0420-61A",
        )
        self.assertEqual(reason, "")
        self.assertIsInstance(signal, dict)
        self.assertEqual(int(signal.get("sold_90d_count", -1)), 4)

    def test_accepts_zero_sold_with_lookback_even_if_sold_tab_missing(self) -> None:
        signal, reason = self._call_provider(
            [
                _row(
                    signal_key="model:BC0420-61A",
                    query="BC0420-61A",
                    sold_90d_count=0,
                    sold_price_min=-1.0,
                    sold_price_median=-1.0,
                    sold_tab_selected=False,
                    lookback_selected="Last 90 days",
                    filtered_row_count=0,
                    sold_sample=None,
                )
            ],
            query="BC0420-61A",
            signal_key="model:BC0420-61A",
        )
        self.assertEqual(reason, "")
        self.assertIsInstance(signal, dict)
        self.assertEqual(int(signal.get("sold_90d_count", -1)), 0)


if __name__ == "__main__":
    unittest.main()

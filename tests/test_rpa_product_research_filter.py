import importlib.util
import pathlib
import sys
import unittest


def _load_rpa_module():
    root = pathlib.Path(__file__).resolve().parents[1]
    script_path = root / "scripts" / "rpa_ebay_product_research.py"
    spec = importlib.util.spec_from_file_location("rpa_ebay_product_research", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load rpa_ebay_product_research.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RpaProductResearchFilterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = _load_rpa_module()

    def test_filtered_rows_drop_accessories_for_model_query(self) -> None:
        html = """
        <div class="research-table-row">
          <div class="research-table-row__title"><div>SEIKO SBDC101 Watch</div></div>
          <div class="research-table-row__avgSoldPrice"><div>$150.00</div></div>
          <div class="research-table-row__dateLastSold"><div>Jan 10, 2026</div></div>
        </div>
        <div class="research-table-row">
          <div class="research-table-row__title"><div>SEIKO SBDC101 replacement band strap</div></div>
          <div class="research-table-row__avgSoldPrice"><div>$15.00</div></div>
          <div class="research-table-row__dateLastSold"><div>Jan 11, 2026</div></div>
        </div>
        <div class="research-table-row">
          <div class="research-table-row__title"><div>SEIKO SBDC101 Prospex</div></div>
          <div class="research-table-row__avgSoldPrice"><div>$148.00</div></div>
          <div class="research-table-row__dateLastSold"><div>Jan 12, 2026</div></div>
        </div>
        """
        codes = self.mod._extract_query_codes("SEIKO SBDC101")
        tokens = self.mod._extract_query_tokens("SEIKO SBDC101", codes)
        prices, sold_count, sold_sample = self.mod._extract_filtered_rows_from_html(
            html,
            query_codes=codes,
            query_tokens=tokens,
        )
        self.assertEqual(sold_count, 2)
        self.assertEqual(sorted(prices), [148.0, 150.0])
        self.assertTrue(isinstance(sold_sample, dict))

    def test_metric_accumulator_prefers_filtered_prices(self) -> None:
        html = """
        <div class="research-table-row">
          <div class="research-table-row__title"><div>SONY Wireless Speaker</div></div>
          <div class="research-table-row__avgSoldPrice"><div>$120.00</div></div>
          <div class="research-table-row__dateLastSold"><div>Jan 10, 2026</div></div>
        </div>
        <div class="research-table-row">
          <div class="research-table-row__title"><div>SONY Speaker Case Cover</div></div>
          <div class="research-table-row__avgSoldPrice"><div>$9.00</div></div>
          <div class="research-table-row__dateLastSold"><div>Jan 11, 2026</div></div>
        </div>
        """
        acc = self.mod.MetricAccumulator.create(query="sony speaker")
        acc.ingest_html(html)
        metrics = acc.finalize()
        self.assertEqual(int(metrics.get("sold_90d_count", -1)), 1)
        self.assertAlmostEqual(float(metrics.get("sold_price_min", -1.0)), 120.0)

    def test_metric_accumulator_uses_lowest_sold_sample(self) -> None:
        acc = self.mod.MetricAccumulator.create(query="seiko sbdc101")
        acc.filtered_sold_samples.append(
            {"title": "high", "sold_price": 390.0, "item_url": "https://www.ebay.com/itm/high"}
        )
        acc.filtered_sold_samples.append(
            {"title": "low", "sold_price": 53.49, "item_url": "https://www.ebay.com/itm/low"}
        )
        acc.filtered_row_prices.extend([390.0, 53.49, 120.0])
        metrics = acc.finalize()
        sold_sample = metrics.get("sold_sample") if isinstance(metrics.get("sold_sample"), dict) else {}
        self.assertEqual(str(sold_sample.get("title")), "low")
        self.assertAlmostEqual(float(sold_sample.get("sold_price", -1.0)), 53.49)

    def test_trim_low_price_outlier_from_payload_prices(self) -> None:
        acc = self.mod.MetricAccumulator.create(query="seiko sbdc101")
        acc.row_prices.extend([2.0, 149.0, 151.0, 153.0, 155.0])
        metrics = acc.finalize()
        self.assertGreater(float(metrics.get("sold_price_min", -1.0)), 100.0)

    def test_non_main_only_row_is_filtered(self) -> None:
        html = """
        <div class="research-table-row">
          <div class="research-table-row__title"><div>SEIKO SBDC101 EMPTY BOX ONLY</div></div>
          <div class="research-table-row__avgSoldPrice"><div>$3.00</div></div>
          <div class="research-table-row__dateLastSold"><div>Jan 12, 2026</div></div>
        </div>
        """
        codes = self.mod._extract_query_codes("SEIKO SBDC101")
        tokens = self.mod._extract_query_tokens("SEIKO SBDC101", codes)
        prices, sold_count, _ = self.mod._extract_filtered_rows_from_html(
            html,
            query_codes=codes,
            query_tokens=tokens,
        )
        self.assertEqual(prices, [])
        self.assertEqual(sold_count, 0)

    def test_extracts_sold_sample_link_and_image(self) -> None:
        html = """
        <div class="research-table-row">
          <a href="/itm/123456789012"><img src="https://i.ebayimg.com/images/g/sample/s-l1600.jpg" /></a>
          <div class="research-table-row__title"><div>SEIKO SBDC101 Prospex</div></div>
          <div class="research-table-row__avgSoldPrice"><div>$152.00</div></div>
          <div class="research-table-row__dateLastSold"><div>Jan 15, 2026</div></div>
        </div>
        """
        prices, sold_count, sold_sample = self.mod._extract_filtered_rows_from_html(
            html,
            query_codes=self.mod._extract_query_codes("SEIKO SBDC101"),
            query_tokens=self.mod._extract_query_tokens("SEIKO SBDC101", self.mod._extract_query_codes("SEIKO SBDC101")),
        )
        self.assertEqual(prices, [152.0])
        self.assertEqual(sold_count, 1)
        self.assertEqual(sold_sample.get("item_url"), "https://www.ebay.com/itm/123456789012")
        self.assertIn("ebayimg.com", str(sold_sample.get("image_url", "")))

    def test_extracts_sold_sample_image_from_data_src(self) -> None:
        html = """
        <div class="research-table-row">
          <a href="/itm/314253529095">
            <img data-src="https://i.ebayimg.com/images/g/example/s-l1600.jpg" />
          </a>
          <div class="research-table-row__title"><div>CITIZEN BC0420-61A pocket watch</div></div>
          <div class="research-table-row__avgSoldPrice"><div>$188.00</div></div>
          <div class="research-table-row__dateLastSold"><div>Jan 18, 2026</div></div>
        </div>
        """
        prices, sold_count, sold_sample = self.mod._extract_filtered_rows_from_html(
            html,
            query_codes=self.mod._extract_query_codes("CITIZEN BC0420-61A"),
            query_tokens=self.mod._extract_query_tokens(
                "CITIZEN BC0420-61A",
                self.mod._extract_query_codes("CITIZEN BC0420-61A"),
            ),
        )
        self.assertEqual(prices, [188.0])
        self.assertEqual(sold_count, 1)
        self.assertEqual(sold_sample.get("item_url"), "https://www.ebay.com/itm/314253529095")
        self.assertEqual(
            sold_sample.get("image_url"),
            "https://i.ebayimg.com/images/g/example/s-l1600.jpg",
        )

    def test_watch_case_material_row_is_not_misclassified_as_accessory(self) -> None:
        html = """
        <div class="research-table-row">
          <div class="research-table-row__title"><div>CASIO GW-5000U-1JF Stainless Steel Case Watch New</div></div>
          <div class="research-table-row__avgSoldPrice"><div>$265.00</div></div>
          <div class="research-table-row__dateLastSold"><div>Jan 20, 2026</div></div>
        </div>
        """
        codes = self.mod._extract_query_codes("GW-5000U-1JF")
        tokens = self.mod._extract_query_tokens("GW-5000U-1JF", codes)
        prices, sold_count, _ = self.mod._extract_filtered_rows_from_html(
            html,
            query_codes=codes,
            query_tokens=tokens,
        )
        self.assertEqual(prices, [265.0])
        self.assertEqual(sold_count, 1)

    def test_finalize_does_not_force_zero_sold_when_filtered_rows_missing(self) -> None:
        acc = self.mod.MetricAccumulator.create(query="gw5000u1jf")
        acc.sold_counts.append(12)
        acc.filtered_sold_counts.append(0)
        acc.row_prices.extend([240.0, 265.0, 279.0])
        metrics = acc.finalize()
        self.assertEqual(int(metrics.get("sold_90d_count", -1)), 12)

    def test_detects_daily_limit_message(self) -> None:
        text = "You've exceeded the number of requests allowed in one day. Please try again tomorrow."
        self.assertTrue(self.mod._contains_daily_limit_message(text))

    def test_detects_no_sold_message(self) -> None:
        text = "No sold items found for this search in Last 90 days."
        self.assertTrue(self.mod._contains_no_sold_message(text))

    def test_short_circuit_no_sold_only_for_model_query_on_90d(self) -> None:
        self.assertTrue(
            self.mod._should_short_circuit_no_sold(
                query="ORIENT RN-AK0803Y",
                lookback_days=90,
                no_sold_detected=True,
                lookback_selected="Last 90 days",
            )
        )
        self.assertFalse(
            self.mod._should_short_circuit_no_sold(
                query="orient watch",
                lookback_days=90,
                no_sold_detected=True,
                lookback_selected="Last 90 days",
            )
        )
        self.assertFalse(
            self.mod._should_short_circuit_no_sold(
                query="ORIENT RN-AK0803Y",
                lookback_days=30,
                no_sold_detected=True,
                lookback_selected="Last 30 days",
            )
        )

    def test_transient_navigation_error_detection(self) -> None:
        err = Exception("Page.goto: net::ERR_ABORTED at https://www.ebay.com/sh/research")
        self.assertTrue(self.mod._is_transient_navigation_error(err))
        self.assertFalse(self.mod._is_transient_navigation_error(Exception("unexpected hard failure")))


if __name__ == "__main__":
    unittest.main()

import importlib.util
import os
import pathlib
import sys
import urllib.parse
import unittest
from unittest.mock import patch


def _load_rpa_module():
    root = pathlib.Path(__file__).resolve().parents[1]
    script_path = root / "scripts" / "rpa_market_research.py"
    spec = importlib.util.spec_from_file_location("rpa_market_research", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load rpa_market_research.py")
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
        self.assertEqual(sold_sample.get("item_id"), "v1|123456789012|0")
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
        self.assertEqual(sold_sample.get("item_id"), "v1|314253529095|0")
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

    def test_apply_ui_filters_marks_fixed_price_unconfirmed_as_strict_blocked(self) -> None:
        class DummyPage:
            def wait_for_timeout(self, _ms: int) -> None:
                return None

        with patch.object(self.mod, "_detect_sold_tab_selected", return_value=True), patch.object(
            self.mod, "_set_lookback_days", return_value="Last 90 days"
        ), patch.object(
            self.mod, "_set_lock_selected_filters", return_value="enabled"
        ), patch.object(
            self.mod, "_click_first", return_value=False
        ), patch.object(
            self.mod, "_detect_fixed_price_selected", return_value=False
        ):
            state = self.mod._apply_ui_filters(
                DummyPage(),
                lookback_days=90,
                condition="any",
                strict_condition=False,
                fixed_price_only=True,
            )

        self.assertFalse(bool(state.get("format_fixed_price_selected")))
        self.assertTrue(bool(state.get("strict_blocked")))
        self.assertEqual(str(state.get("strict_reason", "")), "fixed_price_filter_not_confirmed")

    def test_detect_sold_filters_from_url_parses_fixed_and_condition(self) -> None:
        class DummyPage:
            url = (
                "https://www.ebay.com/sh/research?"
                "marketplace=EBAY-US&keywords=G-SHOCK&tabName=SOLD&format=fixed_price&conditionId=1000&sorting=datelastsold"
            )

        state = self.mod._detect_sold_filters_from_url(DummyPage())
        self.assertTrue(bool(state.get("tab_sold")))
        self.assertTrue(bool(state.get("fixed_price")))
        self.assertTrue(bool(state.get("condition_new")))
        self.assertEqual(str(state.get("sold_sort", "")), "recently_sold")

    def test_detect_sold_filters_from_url_parses_min_price(self) -> None:
        class DummyPage:
            url = (
                "https://www.ebay.com/sh/research?"
                "marketplace=EBAY-US&keywords=G-SHOCK&tabName=SOLD&minPrice=100&conditionId=1000"
            )

        state = self.mod._detect_sold_filters_from_url(DummyPage())
        self.assertAlmostEqual(float(state.get("min_price", 0.0)), 100.0)

    def test_detect_min_price_filter_selected_uses_url_min_price(self) -> None:
        class DummyPage:
            url = "https://www.ebay.com/sh/research?keywords=G-SHOCK&tabName=SOLD&minPrice=150"

        self.assertTrue(self.mod._detect_min_price_filter_selected(DummyPage(), 100.0))
        self.assertFalse(self.mod._detect_min_price_filter_selected(DummyPage(), 200.0))

    def test_ensure_result_offset_reapplies_offset_in_url(self) -> None:
        class DummyPage:
            def __init__(self) -> None:
                self.url = (
                    "https://www.ebay.com/sh/research?"
                    "marketplace=EBAY-US&keywords=G-SHOCK&conditionId=1000&format=FIXED_PRICE&minPrice=100&offset=0&limit=50&tabName=SOLD"
                )
                self.visited = []

            def goto(self, target_url: str, wait_until: str = "commit") -> None:
                self.visited.append((target_url, wait_until))
                self.url = target_url

        page = DummyPage()
        with patch.object(self.mod, "_wait_for_research_ready", return_value=True):
            state = self.mod._ensure_result_offset(page, 50, wait_seconds=2)

        self.assertTrue(bool(state.get("offset_reapplied")))
        self.assertTrue(bool(state.get("offset_confirmed")))
        self.assertEqual(int(state.get("offset_before", -1)), 0)
        self.assertEqual(int(state.get("offset_after", -1)), 50)
        self.assertTrue(page.visited)
        parsed = urllib.parse.urlparse(page.url)
        params = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(params.get("offset"), ["50"])
        self.assertEqual(params.get("format"), ["FIXED_PRICE"])

    def test_ensure_result_offset_noop_when_already_target(self) -> None:
        class DummyPage:
            def __init__(self) -> None:
                self.url = "https://www.ebay.com/sh/research?keywords=G-SHOCK&offset=50&limit=50&tabName=SOLD"
                self.visited = []

            def goto(self, target_url: str, wait_until: str = "commit") -> None:
                self.visited.append((target_url, wait_until))
                self.url = target_url

        page = DummyPage()
        state = self.mod._ensure_result_offset(page, 50, wait_seconds=2)
        self.assertFalse(bool(state.get("offset_reapplied")))
        self.assertTrue(bool(state.get("offset_confirmed")))
        self.assertEqual(int(state.get("offset_before", -1)), 50)
        self.assertEqual(int(state.get("offset_after", -1)), 50)
        self.assertEqual(page.visited, [])

    def test_search_and_wait_does_not_force_format_query_param(self) -> None:
        class DummyPage:
            def __init__(self) -> None:
                self.url = ""
                self.visited = []

            def goto(self, target_url: str, wait_until: str = "commit") -> None:
                self.visited.append((target_url, wait_until))
                self.url = target_url

            def wait_for_timeout(self, _ms: int) -> None:
                return None

        page = DummyPage()
        with patch.object(self.mod, "_wait_for_research_interactive", return_value=True):
            self.mod._search_and_wait(
                page,
                query="G-SHOCK",
                wait_seconds=1,
                result_offset=0,
                result_limit=50,
                category_id=31387,
                category_slug="wristwatches",
                fixed_price_only=True,
                condition="new",
                min_price_usd=100.0,
                sold_sort="default",
            )

        self.assertTrue(page.visited)
        parsed = urllib.parse.urlparse(page.visited[0][0])
        params = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(params.get("tabName"), ["SOLD"])
        self.assertEqual(params.get("conditionId"), ["1000"])
        self.assertEqual(params.get("minPrice"), ["100"])
        self.assertNotIn("format", params)
        self.assertNotIn("sort", params)

    def test_search_and_wait_prefills_recently_sold_sort_query_param(self) -> None:
        class DummyPage:
            def __init__(self) -> None:
                self.url = ""
                self.visited = []

            def goto(self, target_url: str, wait_until: str = "commit") -> None:
                self.visited.append((target_url, wait_until))
                self.url = target_url

            def wait_for_timeout(self, _ms: int) -> None:
                return None

        page = DummyPage()
        with patch.object(self.mod, "_wait_for_research_interactive", return_value=True):
            self.mod._search_and_wait(
                page,
                query="G-SHOCK",
                wait_seconds=1,
                result_offset=0,
                result_limit=50,
                category_id=31387,
                category_slug="wristwatches",
                fixed_price_only=True,
                condition="new",
                min_price_usd=100.0,
                sold_sort="recently_sold",
            )

        self.assertTrue(page.visited)
        parsed = urllib.parse.urlparse(page.visited[0][0])
        params = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(params.get("sorting"), ["datelastsold"])

    def test_resolve_filters_screenshot_path_expands_placeholders(self) -> None:
        path = self.mod._resolve_filters_screenshot_path(
            "tmp/{query}_{index}_{ts}.png",
            "G-SHOCK 5600",
            3,
        )
        self.assertTrue(str(path).endswith(".png"))
        self.assertIn("G-SHOCK_5600_3_", str(path))

    def test_resolve_filters_screenshot_path_appends_index_without_placeholders(self) -> None:
        path = self.mod._resolve_filters_screenshot_path("tmp/filters.png", "watch", 2)
        self.assertTrue(str(path).endswith("filters_2.png"))

    def test_wait_for_research_list_visible_returns_row_state(self) -> None:
        class DummyPage:
            def evaluate(self, _script: str):
                return {"rows": 9, "noSold": False, "busy": False}

            def wait_for_timeout(self, _ms: int) -> None:
                return None

        state = self.mod._wait_for_research_list_visible(DummyPage(), wait_seconds=1)
        self.assertEqual(int(state.get("rows", 0)), 9)
        self.assertFalse(bool(state.get("no_sold")))
        self.assertFalse(bool(state.get("busy")))

    def test_apply_ui_filters_min_price_ui_enabled_by_default(self) -> None:
        class DummyPage:
            def wait_for_timeout(self, _ms: int) -> None:
                return None

        min_state = {
            "price_filter_panel_opened": True,
            "min_price_input_applied": True,
            "min_price_option_label": "Min price",
            "min_price_selected": True,
        }
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LIQUIDITY_RPA_ENABLE_MIN_PRICE_FILTER_UI", None)
            with patch.object(self.mod, "_detect_sold_tab_selected", return_value=True), patch.object(
                self.mod, "_set_lookback_days", return_value="Last 90 days"
            ), patch.object(
                self.mod, "_set_lock_selected_filters", return_value="enabled"
            ), patch.object(
                self.mod, "_click_first", return_value=False
            ), patch.object(
                self.mod, "_click_button_by_text_tokens", return_value=False
            ), patch.object(
                self.mod, "_set_min_price_filter", return_value=min_state
            ) as min_filter_mock:
                state = self.mod._apply_ui_filters(
                    DummyPage(),
                    lookback_days=90,
                    condition="any",
                    strict_condition=False,
                    fixed_price_only=False,
                    min_price_usd=100.0,
                )

        min_filter_mock.assert_called_once()
        self.assertTrue(bool(state.get("min_price_ui_enabled")))
        self.assertTrue(bool(state.get("min_price_selected")))
        self.assertEqual(str(state.get("min_price_selection_source", "")), "ui")

    def test_apply_ui_filters_skips_condition_ui_when_url_prefilled(self) -> None:
        class DummyPage:
            def wait_for_timeout(self, _ms: int) -> None:
                return None

        with patch.object(self.mod, "_detect_sold_tab_selected", return_value=True), patch.object(
            self.mod, "_set_lookback_days", return_value="Last 90 days"
        ), patch.object(
            self.mod, "_set_lock_selected_filters", return_value="enabled"
        ), patch.object(
            self.mod, "_detect_sold_filters_from_url",
            return_value={"tab_sold": True, "fixed_price": False, "condition_new": True, "min_price": 0.0},
        ), patch.object(
            self.mod, "_click_first", return_value=False
        ) as click_first_mock, patch.object(
            self.mod, "_click_button_by_text_tokens", return_value=False
        ) as click_text_mock:
            state = self.mod._apply_ui_filters(
                DummyPage(),
                lookback_days=90,
                condition="new",
                strict_condition=True,
                fixed_price_only=False,
                min_price_usd=0.0,
            )

        click_first_mock.assert_not_called()
        click_text_mock.assert_not_called()
        self.assertIn("New(url_prefill)", list(state.get("condition_selected", [])))
        self.assertFalse(bool(state.get("condition_missing")))

    def test_apply_ui_filters_skips_min_price_ui_when_url_prefilled(self) -> None:
        class DummyPage:
            def wait_for_timeout(self, _ms: int) -> None:
                return None

        with patch.object(self.mod, "_detect_sold_tab_selected", return_value=True), patch.object(
            self.mod, "_set_lookback_days", return_value="Last 90 days"
        ), patch.object(
            self.mod, "_set_lock_selected_filters", return_value="enabled"
        ), patch.object(
            self.mod, "_detect_min_price_filter_selected", return_value=True
        ), patch.object(
            self.mod, "_set_min_price_filter", return_value={}
        ) as min_filter_mock:
            state = self.mod._apply_ui_filters(
                DummyPage(),
                lookback_days=90,
                condition="any",
                strict_condition=False,
                fixed_price_only=False,
                min_price_usd=100.0,
            )

        min_filter_mock.assert_not_called()
        self.assertTrue(bool(state.get("min_price_selected")))
        self.assertEqual(str(state.get("min_price_selection_source", "")), "url_prefill")

    def test_apply_ui_filters_skips_sold_sort_ui_when_url_prefilled(self) -> None:
        class DummyPage:
            def wait_for_timeout(self, _ms: int) -> None:
                return None

        with patch.object(self.mod, "_detect_sold_tab_selected", return_value=True), patch.object(
            self.mod, "_set_lookback_days", return_value="Last 90 days"
        ), patch.object(
            self.mod, "_set_lock_selected_filters", return_value="enabled"
        ), patch.object(
            self.mod, "_detect_sold_filters_from_url",
            return_value={
                "tab_sold": True,
                "fixed_price": False,
                "condition_new": False,
                "min_price": 0.0,
                "sold_sort": "price_desc",
                "sold_sort_raw": "PRICE_PLUS_SHIPPING_DESC",
            },
        ), patch.object(
            self.mod, "_set_sold_sort", return_value={}
        ) as sort_set_mock:
            state = self.mod._apply_ui_filters(
                DummyPage(),
                lookback_days=90,
                condition="any",
                strict_condition=False,
                fixed_price_only=False,
                min_price_usd=0.0,
                sold_sort="price_desc",
            )

        sort_set_mock.assert_not_called()
        self.assertTrue(bool(state.get("sort_selected")))
        self.assertEqual(str(state.get("sort_selection_source", "")), "url_prefill")

    def test_set_sold_sort_recently_sold_clicks_date_last_sold(self) -> None:
        class DummyPage:
            def wait_for_timeout(self, _ms: int) -> None:
                return None

        with patch.object(
            self.mod,
            "_get_sold_date_order_state",
            side_effect=[
                {"is_newest_first": False, "first_date": "Jan 27, 2026"},
                {"is_newest_first": True, "first_date": "Feb 25, 2026"},
            ],
        ), patch.object(
            self.mod, "_click_date_last_sold_header", return_value=True
        ) as click_mock, patch.object(
            self.mod, "_wait_for_research_ready", return_value=True
        ):
            state = self.mod._set_sold_sort(DummyPage(), "recently_sold")

        click_mock.assert_called_once()
        self.assertTrue(bool(state.get("sort_selected")))
        self.assertEqual(str(state.get("sort_selection_source", "")), "date_last_sold_click_1")

    def test_set_sold_sort_recently_sold_uses_header_desc_after_click(self) -> None:
        class DummyPage:
            def wait_for_timeout(self, _ms: int) -> None:
                return None

        with patch.object(
            self.mod,
            "_get_date_last_sold_header_state",
            side_effect=[
                {"has_header": True, "is_desc": False, "is_asc": True},
                {"has_header": True, "is_desc": True, "is_asc": False},
            ],
        ), patch.object(
            self.mod,
            "_get_sold_date_order_state",
            return_value={"is_newest_first": False, "first_date": "Jan 27, 2026"},
        ), patch.object(
            self.mod, "_click_date_last_sold_header", return_value=True
        ) as click_mock, patch.object(
            self.mod, "_wait_for_research_ready", return_value=True
        ):
            state = self.mod._set_sold_sort(DummyPage(), "recently_sold")

        click_mock.assert_called_once()
        self.assertTrue(bool(state.get("sort_selected")))
        self.assertEqual(str(state.get("sort_selection_source", "")), "date_last_sold_click_1")
        self.assertEqual(str(state.get("sort_option_label", "")), "date_last_sold_header_desc")

    def test_finalize_filter_state_two_stage_accepts_url_confirmation(self) -> None:
        class DummyPage:
            pass

        filter_state = {
            "condition_selected": ["New(url_prefill)"],
            "strict_blocked": False,
            "strict_reason": "",
        }
        with patch.object(
            self.mod,
            "_detect_sold_filters_from_url",
            return_value={
                "tab_sold": True,
                "fixed_price": True,
                "condition_new": True,
                "min_price": 100.0,
                "sold_sort": "recently_sold",
                "sold_sort_raw": "-datelastsold",
            },
        ), patch.object(
            self.mod, "_detect_sold_tab_selected", return_value=False
        ), patch.object(
            self.mod, "_detect_fixed_price_selected", return_value=False
        ), patch.object(
            self.mod, "_detect_min_price_filter_selected", return_value=False
        ), patch.object(
            self.mod,
            "_get_date_last_sold_header_state",
            return_value={"has_header": True, "is_desc": False, "is_asc": True},
        ), patch.object(
            self.mod,
            "_get_sold_date_order_state",
            return_value={"is_newest_first": False, "is_desc": False},
        ), patch.object(
            self.mod, "_detect_lock_selected_filters_enabled", return_value=True
        ):
            with patch.dict(
                "os.environ",
                {
                    "LIQUIDITY_RPA_REQUIRE_MIN_PRICE_FILTER": "1",
                    "LIQUIDITY_RPA_REQUIRE_SOLD_SORT": "1",
                    "LIQUIDITY_RPA_REQUIRE_LOCK_SELECTED_FILTERS": "1",
                },
                clear=False,
            ):
                state = self.mod._finalize_filter_state_two_stage(
                    DummyPage(),
                    filter_state,
                    condition="new",
                    strict_condition=True,
                    fixed_price_only=True,
                    min_price_usd=100.0,
                    sold_sort="recently_sold",
                )

        self.assertFalse(bool(state.get("strict_blocked")))
        confirm = state.get("confirmations") if isinstance(state.get("confirmations"), dict) else {}
        self.assertTrue(bool((confirm.get("condition") or {}).get("confirmed")))
        self.assertTrue(bool((confirm.get("format_fixed_price") or {}).get("confirmed")))
        self.assertTrue(bool((confirm.get("min_price") or {}).get("confirmed")))
        self.assertTrue(bool((confirm.get("sold_sort") or {}).get("confirmed")))

    def test_finalize_filter_state_two_stage_blocks_when_both_url_and_ui_missing(self) -> None:
        class DummyPage:
            pass

        with patch.object(
            self.mod,
            "_detect_sold_filters_from_url",
            return_value={
                "tab_sold": False,
                "fixed_price": False,
                "condition_new": False,
                "min_price": 0.0,
                "sold_sort": "default",
                "sold_sort_raw": "",
            },
        ), patch.object(
            self.mod, "_detect_sold_tab_selected", return_value=False
        ), patch.object(
            self.mod, "_detect_fixed_price_selected", return_value=False
        ), patch.object(
            self.mod, "_detect_min_price_filter_selected", return_value=False
        ), patch.object(
            self.mod,
            "_get_date_last_sold_header_state",
            return_value={"has_header": True, "is_desc": False, "is_asc": True},
        ), patch.object(
            self.mod,
            "_get_sold_date_order_state",
            return_value={"is_newest_first": False, "is_desc": False},
        ), patch.object(
            self.mod, "_detect_lock_selected_filters_enabled", return_value=False
        ):
            with patch.dict(
                "os.environ",
                {
                    "LIQUIDITY_RPA_REQUIRE_MIN_PRICE_FILTER": "1",
                    "LIQUIDITY_RPA_REQUIRE_SOLD_SORT": "1",
                    "LIQUIDITY_RPA_REQUIRE_LOCK_SELECTED_FILTERS": "1",
                },
                clear=False,
            ):
                state = self.mod._finalize_filter_state_two_stage(
                    DummyPage(),
                    {"condition_selected": [], "strict_blocked": False, "strict_reason": ""},
                    condition="new",
                    strict_condition=True,
                    fixed_price_only=True,
                    min_price_usd=100.0,
                    sold_sort="recently_sold",
                )

        self.assertTrue(bool(state.get("strict_blocked")))
        self.assertEqual(str(state.get("strict_reason", "")), "condition_filter_not_confirmed")

    def test_build_parser_default_pause_for_login_is_zero(self) -> None:
        parser = self.mod.build_parser()
        args = parser.parse_args([])
        self.assertEqual(int(getattr(args, "pause_for_login", -1)), 0)

    def test_build_parser_default_sold_sort_is_default(self) -> None:
        parser = self.mod.build_parser()
        args = parser.parse_args([])
        self.assertEqual(str(getattr(args, "sold_sort", "")), "default")


if __name__ == "__main__":
    unittest.main()

import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from reselling.config import Settings
from reselling import miner_seed_pool
from reselling.models import connect, init_db


def _dummy_settings(db_path: Path) -> Settings:
    return Settings(
        db_path=db_path,
        fx_provider="",
        fx_rate_provider_url="",
        fx_rate_url_template="",
        fx_rate_json_path="rates.JPY",
        fx_api_key="",
        fx_base_ccy="USD",
        fx_quote_ccy="JPY",
        fx_usd_jpy_default=150.0,
        fx_refresh_seconds=3600,
        fx_cache_seconds=900,
    )


class MinerSeedPoolTests(unittest.TestCase):
    def test_normalize_seed_query_strips_condition_suffix_but_keeps_new_balance(self) -> None:
        self.assertEqual(
            miner_seed_pool._normalize_seed_query("g-shock New"),
            "g-shock",
        )
        self.assertEqual(
            miner_seed_pool._normalize_seed_query("New Balance M990GL6"),
            "New Balance M990GL6",
        )

    def test_extract_seed_queries_from_title_prefers_model_code(self) -> None:
        seeds = miner_seed_pool._extract_seed_queries_from_title(
            "CASIO G-SHOCK GW-M5610U-1JF New",
            ["CASIO"],
        )
        keys = [miner_seed_pool._seed_key(v) for v in seeds]
        self.assertTrue(any(key.endswith("GWM5610U1JF") for key in keys))

    def test_extract_seed_queries_includes_valid_gtin(self) -> None:
        seeds = miner_seed_pool._extract_seed_queries_from_title(
            "Panasonic ER-GN70-K JAN 4549980658031 New",
            ["Panasonic"],
        )
        keys = [miner_seed_pool._seed_key(v) for v in seeds]
        self.assertTrue(any("4549980658031" in key for key in keys))

    def test_extract_seed_queries_drops_generic_tokens_when_specific_exists(self) -> None:
        seeds = miner_seed_pool._extract_seed_queries_from_title(
            "SEIKO 5 Sports SBSC009 Limited Item",
            ["SEIKO"],
        )
        keys = [miner_seed_pool._seed_key(v) for v in seeds]
        self.assertTrue(any("SBSC009" in key for key in keys))
        self.assertFalse(any(key == "SEIKO" for key in keys))
        self.assertFalse(any(key == "ITEM" for key in keys))

    def test_fallback_seed_phrases_avoids_broad_series_token(self) -> None:
        seeds = miner_seed_pool._extract_seed_queries_from_title(
            "Citizen Promaster Diver Watch",
            ["Citizen"],
        )
        keys = [miner_seed_pool._seed_key(v) for v in seeds]
        self.assertFalse(any(key == "CITIZENPROMASTER" for key in keys))
        self.assertFalse(any(key == "CITIZEN" for key in keys))

    def test_fallback_seed_phrases_keeps_non_broad_series_phrase(self) -> None:
        seeds = miner_seed_pool._extract_seed_queries_from_title(
            "Citizen Attesa Titanium Watch",
            ["Citizen"],
        )
        keys = [miner_seed_pool._seed_key(v) for v in seeds]
        self.assertTrue(any(key == "CITIZENATTESA" for key in keys))

    def test_pick_liquidity_query_prefers_model_when_jp_seed_is_gtin_only(self) -> None:
        picked = miner_seed_pool._pick_liquidity_query(
            seed_query="CASIO GW-M5610U-1JF",
            jp_seed_query="4971850995470",
        )
        self.assertEqual(picked, "GW-M5610U-1JF")

    def test_run_seeded_fetch_runs_stage_b_and_stage_c_pipeline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_test.db"
            settings = _dummy_settings(db_path)
            fake_page_row = {
                "query": "watch",
                "metadata": {
                    "filtered_result_rows": [
                        {"title": "CASIO GW-M5610U-1JF watch", "rank": 1},
                        {"title": "SEIKO SBDC101 watch", "rank": 2},
                    ]
                },
            }

            created_counter = {"value": 0}

            def _fake_search(query: str, limit: int, timeout: int, page: int = 1, require_in_stock: bool = True):
                item = miner_seed_pool.MarketItem(
                    site="rakuten",
                    item_id=f"rk-{query}-{page}",
                    title=f"{query} 新品 本体",
                    item_url=f"https://example.com/{query}/{page}",
                    image_url="https://example.com/image.jpg",
                    price=10000.0,
                    shipping=0.0,
                    currency="JPY",
                    condition="new",
                    identifiers={},
                    raw={},
                )
                return [item], {"status": 200, "category_filter": {"applied": True}, "cache_hit": False}

            def _fake_liquidity(**kwargs):
                return {
                    "sold_90d_count": 12,
                    "metadata": {
                        "sold_price_min": 180.0,
                        "sold_sample": {
                            "item_url": "https://www.ebay.com/itm/123456789012",
                            "title": "eBay sold title",
                            "image_url": "https://example.com/sold.jpg",
                            "sold_price": 180.0,
                        },
                    },
                    "source": "rpa_json",
                    "unavailable_reason": "",
                }

            def _fake_create(payload, settings=None):
                created_counter["value"] += 1
                cid = created_counter["value"]
                return {
                    "id": cid,
                    "source_title": payload.get("source_title", ""),
                    "market_title": payload.get("market_title", ""),
                    "match_score": payload.get("match_score", 0.0),
                    "expected_profit_usd": payload.get("expected_profit_usd", 0.0),
                }

            env = {
                "DB_BACKEND": "sqlite",
                "MINER_SEED_POOL_MAX_PAGES": "1",
                "MINER_SEED_POOL_TARGET_COUNT": "4",
                "MINER_SEED_POOL_SOFT_TARGET_RATIO": "0.8",
                "MINER_SEED_POOL_REFILL_THRESHOLD": "40",
                "MINER_SEED_POOL_RUN_BATCH_SIZE": "2",
                "MINER_SEED_POOL_PAGE_SIZE": "50",
            }
            with patch.dict("os.environ", env, clear=False):
                with patch.object(miner_seed_pool, "_run_rpa_page", return_value={"ok": True, "rows": [fake_page_row], "reason": "ok"}):
                    with patch.object(miner_seed_pool, "_search_rakuten", side_effect=_fake_search), patch.object(
                        miner_seed_pool, "_search_yahoo", side_effect=_fake_search
                    ), patch.object(miner_seed_pool, "get_liquidity_signal", side_effect=_fake_liquidity), patch.object(
                        miner_seed_pool, "create_miner_candidate", side_effect=_fake_create
                    ):
                        payload = miner_seed_pool.run_seeded_fetch(
                            category_query="watch",
                            source_sites=["rakuten", "yahoo"],
                            market_site="ebay",
                            limit_per_site=20,
                            max_candidates=20,
                            min_match_score=0.72,
                            min_profit_usd=0.01,
                            min_margin_rate=0.03,
                            require_in_stock=True,
                            timeout=10,
                            timed_mode=True,
                            min_target_candidates=1,
                            timebox_sec=60,
                            max_passes=4,
                            continue_after_target=False,
                            settings=settings,
                        )

            self.assertGreaterEqual(int(payload.get("created_count", 0)), 1)
            self.assertTrue(bool(payload.get("seed_pool")))
            self.assertEqual(str(payload.get("query")), "watch")
            timed = payload.get("timed_fetch", {}) if isinstance(payload, dict) else {}
            self.assertGreaterEqual(int(timed.get("stage1_pass_total", 0)), 1)
            self.assertGreaterEqual(int(timed.get("stage2_runs", 0)), 1)

    def test_taken_seed_is_reusable_and_pool_does_not_shrink(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_cycle.db"
            settings = _dummy_settings(db_path)
            now_ts = 1_700_000_000
            with patch.dict("os.environ", {"DB_BACKEND": "sqlite"}, clear=False):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    inserted = miner_seed_pool._insert_seed_rows(
                        conn,
                        category_key="watch",
                        rows=[
                            {
                                "seed_query": "CASIO GW-M5610U-1JF",
                                "source_title": "seed",
                                "source_item_url": "",
                                "source_rank": 1,
                                "metadata": {},
                            }
                        ],
                        ttl_days=7,
                    )
                    self.assertEqual(inserted, 1)
                    available_before = miner_seed_pool._count_available(conn, category_key="watch", now_ts=now_ts)
                    picked_1, _ = miner_seed_pool._take_seeds_for_run(
                        conn, category_key="watch", take_count=1, now_ts=now_ts
                    )
                    available_after_1 = miner_seed_pool._count_available(conn, category_key="watch", now_ts=now_ts)
                    picked_2, _ = miner_seed_pool._take_seeds_for_run(
                        conn, category_key="watch", take_count=1, now_ts=now_ts + 1
                    )
                    available_after_2 = miner_seed_pool._count_available(conn, category_key="watch", now_ts=now_ts)

            self.assertEqual(available_before, 1)
            self.assertEqual(len(picked_1), 1)
            self.assertEqual(available_after_1, 1)
            self.assertEqual(len(picked_2), 1)
            self.assertEqual(available_after_2, 1)

    def test_bootstrap_refill_can_supply_non_watch_without_rpa(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_bootstrap.db"
            settings = _dummy_settings(db_path)
            category_row = {
                "category_key": "sneakers",
                "display_name_ja": "スニーカー",
                "seed_brands": ["Nike", "adidas", "New Balance"],
                "seed_series": ["Air Jordan", "Dunk", "Air Max", "990"],
                "model_examples": ["DD1391-100", "M990GL6", "DZ5485-106"],
                "aliases": ["sneakers", "sneaker shoes"],
            }

            def _empty_search(*args, **kwargs):
                return [], {"status": 200, "cache_hit": False}

            env = {
                "DB_BACKEND": "sqlite",
                "MINER_SEED_POOL_REFILL_THRESHOLD": "5",
                "MINER_SEED_POOL_RUN_BATCH_SIZE": "2",
                "MINER_SEED_POOL_BOOTSTRAP_TARGET": "12",
                "MINER_SEED_POOL_BOOTSTRAP_ENABLED": "1",
                "MINER_SEED_POOL_MAX_PAGES": "2",
            }
            with patch.dict("os.environ", env, clear=False), patch.object(
                miner_seed_pool, "_match_category_row", return_value=category_row
            ), patch.object(
                miner_seed_pool, "_run_rpa_page", return_value={"ok": True, "rows": [], "reason": "ok"}
            ) as mocked_rpa, patch.object(
                miner_seed_pool, "_search_rakuten", side_effect=_empty_search
            ), patch.object(
                miner_seed_pool, "_search_yahoo", side_effect=_empty_search
            ):
                payload = miner_seed_pool.run_seeded_fetch(
                    category_query="sneakers",
                    source_sites=["rakuten", "yahoo"],
                    market_site="ebay",
                    limit_per_site=10,
                    max_candidates=10,
                    min_match_score=0.7,
                    min_profit_usd=0.01,
                    min_margin_rate=0.03,
                    require_in_stock=True,
                    timeout=8,
                    timed_mode=True,
                    min_target_candidates=1,
                    timebox_sec=30,
                    max_passes=2,
                    continue_after_target=False,
                    settings=settings,
                )

            self.assertGreaterEqual(int(payload.get("seed_pool", {}).get("available_after_refill", 0)), 2)
            self.assertGreaterEqual(int(payload.get("seed_pool", {}).get("selected_seed_count", 0)), 1)
            self.assertEqual(mocked_rpa.call_count, 0)

    def test_refill_skips_recent_page_and_fetches_next_offset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_cursor.db"
            settings = _dummy_settings(db_path)
            now_ts = 1_700_000_000
            fake_row = {
                "query": "watch",
                "sold_90d_count": 12,
                "sold_price_min": 120.0,
                "metadata": {
                    "filtered_result_rows": [
                        {"title": "CASIO GW-M5610U-1JF watch", "rank": 1},
                    ]
                },
            }
            called_offsets = []

            def _fake_run_rpa_page(*, query: str, offset: int, limit: int, **kwargs):
                called_offsets.append(offset)
                return {"ok": True, "rows": [fake_row], "reason": "ok", "daily_limit_reached": False}

            with patch.dict(
                "os.environ",
                {
                    "DB_BACKEND": "sqlite",
                    "MINER_SEED_POOL_REFILL_THRESHOLD": "60",
                    "MINER_SEED_POOL_PAGE_SIZE": "50",
                    "MINER_SEED_POOL_MAX_PAGES": "3",
                    "MINER_SEED_POOL_TARGET_COUNT": "1",
                    "MINER_SEED_POOL_SOFT_TARGET_RATIO": "0.8",
                    "MINER_SEED_POOL_PAGE_FRESH_DAYS": "7",
                },
                clear=False,
            ), patch.object(miner_seed_pool.time, "time", return_value=float(now_ts)), patch.object(
                miner_seed_pool, "_run_rpa_page", side_effect=_fake_run_rpa_page
            ):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    conn.execute(
                        """
                        INSERT INTO miner_seed_refill_pages (
                            category_key, query_key, page_offset, page_size, fetched_at, result_count, new_seed_count, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "watch",
                            "watch",
                            0,
                            50,
                            miner_seed_pool.utc_iso(now_ts - 3600),
                            50,
                            30,
                            miner_seed_pool.utc_iso(now_ts - 3600),
                        ),
                    )
                    summary = miner_seed_pool._refill_seed_pool(
                        conn,
                        category_key="watch",
                        category_label="腕時計",
                        category_row={},
                    )

            self.assertEqual(called_offsets[0], 50)
            self.assertEqual(int(summary.get("skipped_fresh_pages", 0)), 1)
            self.assertGreaterEqual(int(summary.get("added_count", 0)), 1)

    def test_refill_respects_stage_a_big_word_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_big_word_limit.db"
            settings = _dummy_settings(db_path)
            called_queries = []

            def _fake_run_rpa_page(*, query: str, offset: int, limit: int, **kwargs):
                called_queries.append(query)
                return {
                    "ok": True,
                    "rows": [{"query": query, "metadata": {"filtered_result_rows": []}}],
                    "reason": "ok",
                    "daily_limit_reached": False,
                }

            with patch.dict(
                "os.environ",
                {
                    "DB_BACKEND": "sqlite",
                    "MINER_SEED_POOL_REFILL_THRESHOLD": "60",
                    "MINER_SEED_POOL_PAGE_SIZE": "50",
                    "MINER_SEED_POOL_MAX_PAGES": "1",
                    "MINER_SEED_POOL_TARGET_COUNT": "100",
                    "MINER_SEED_POOL_REFILL_TIMEBOX_SEC": "300",
                },
                clear=False,
            ), patch.object(
                miner_seed_pool, "_category_big_words", return_value=["watch", "casio", "seiko"]
            ), patch.object(
                miner_seed_pool, "_run_rpa_page", side_effect=_fake_run_rpa_page
            ):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    summary = miner_seed_pool._refill_seed_pool(
                        conn,
                        category_key="watch",
                        category_label="腕時計",
                        category_row={},
                        stage_a_big_word_limit=2,
                    )

            self.assertEqual(called_queries, ["watch", "casio"])
            self.assertEqual(list(summary.get("queries", [])), ["watch", "casio"])
            self.assertEqual(int(summary.get("big_word_limit", 0)), 2)
            self.assertEqual(int(summary.get("big_word_count", 0)), 2)
            self.assertEqual(int(summary.get("big_word_total_count", 0)), 3)
            self.assertEqual(int(summary.get("target_count_base", 0)), 100)
            self.assertEqual(int(summary.get("target_count", 0)), 67)
            self.assertEqual(int(summary.get("timebox_base_sec", 0)), 300)
            self.assertEqual(int(summary.get("timebox_sec", 0)), 200)

    def test_refill_limits_pages_by_query_elapsed_hours(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_page_unlock_elapsed.db"
            settings = _dummy_settings(db_path)
            now_ts = 1_700_000_000
            called_offsets = []

            def _fake_run_rpa_page(*, query: str, offset: int, limit: int, **kwargs):
                called_offsets.append(offset)
                return {
                    "ok": True,
                    "rows": [
                        {
                            "query": query,
                            "sold_90d_count": 200,
                            "sold_price_min": 120.0,
                            "metadata": {
                                "filtered_result_rows": [
                                    {"title": f"CASIO GW-M5610U-{offset}JF watch", "rank": 1},
                                ]
                            },
                        }
                    ],
                    "reason": "ok",
                    "daily_limit_reached": False,
                }

            with patch.dict(
                "os.environ",
                {
                    "DB_BACKEND": "sqlite",
                    "MINER_SEED_POOL_REFILL_THRESHOLD": "60",
                    "MINER_SEED_POOL_PAGE_SIZE": "50",
                    "MINER_SEED_POOL_MAX_PAGES": "5",
                    "MINER_SEED_POOL_TARGET_COUNT": "100",
                    "MINER_SEED_POOL_PAGE_FRESH_DAYS": "0",
                    "MINER_STAGEA_QUERY_PAGE_UNLOCK_ENABLED": "1",
                    "MINER_STAGEA_QUERY_PAGE_UNLOCK_HOURS_DEFAULT": "24",
                    "MINER_STAGEA_QUERY_PAGE_UNLOCK_MIN_PAGES": "1",
                },
                clear=False,
            ), patch.object(miner_seed_pool.time, "time", return_value=float(now_ts)), patch.object(
                miner_seed_pool, "_run_rpa_page", side_effect=_fake_run_rpa_page
            ):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    conn.execute(
                        """
                        INSERT INTO miner_seed_refill_pages (
                            category_key, query_key, page_offset, page_size, fetched_at, result_count, new_seed_count, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "watch",
                            "watch",
                            0,
                            50,
                            miner_seed_pool.utc_iso(now_ts - (72 * 3600)),
                            50,
                            10,
                            miner_seed_pool.utc_iso(now_ts - (72 * 3600)),
                        ),
                    )
                    summary = miner_seed_pool._refill_seed_pool(
                        conn,
                        category_key="watch",
                        category_label="腕時計",
                        category_row={},
                    )

            self.assertEqual(called_offsets, [0, 50, 100])
            query_runs = summary.get("query_runs", []) if isinstance(summary, dict) else []
            self.assertTrue(isinstance(query_runs, list) and query_runs)
            first_run = query_runs[0] if isinstance(query_runs[0], dict) else {}
            unlock = first_run.get("page_unlock", {}) if isinstance(first_run.get("page_unlock"), dict) else {}
            self.assertEqual(int(unlock.get("fetch_quota_pages", -1)), 3)
            self.assertEqual(str(first_run.get("stop_reason", "")), "page_unlock_quota_reached")

    def test_refill_page_unlock_wait_when_elapsed_is_too_short(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_page_unlock_wait.db"
            settings = _dummy_settings(db_path)
            now_ts = 1_700_000_000

            with patch.dict(
                "os.environ",
                {
                    "DB_BACKEND": "sqlite",
                    "MINER_SEED_POOL_REFILL_THRESHOLD": "60",
                    "MINER_SEED_POOL_PAGE_SIZE": "50",
                    "MINER_SEED_POOL_MAX_PAGES": "3",
                    "MINER_SEED_POOL_TARGET_COUNT": "100",
                    "MINER_SEED_POOL_PAGE_FRESH_DAYS": "0",
                    "MINER_STAGEA_QUERY_PAGE_UNLOCK_ENABLED": "1",
                    "MINER_STAGEA_QUERY_PAGE_UNLOCK_HOURS_DEFAULT": "24",
                    "MINER_STAGEA_QUERY_PAGE_UNLOCK_MIN_PAGES": "0",
                },
                clear=False,
            ), patch.object(miner_seed_pool.time, "time", return_value=float(now_ts)), patch.object(
                miner_seed_pool,
                "_run_rpa_page",
                side_effect=AssertionError("page unlock wait ではRPA実行されない想定"),
            ):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    conn.execute(
                        """
                        INSERT INTO miner_seed_refill_pages (
                            category_key, query_key, page_offset, page_size, fetched_at, result_count, new_seed_count, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "watch",
                            "watch",
                            0,
                            50,
                            miner_seed_pool.utc_iso(now_ts - 3600),
                            50,
                            1,
                            miner_seed_pool.utc_iso(now_ts - 3600),
                        ),
                    )
                    summary = miner_seed_pool._refill_seed_pool(
                        conn,
                        category_key="watch",
                        category_label="腕時計",
                        category_row={},
                    )

            self.assertEqual(str(summary.get("reason", "")), "page_unlock_wait")
            self.assertTrue(str(summary.get("cooldown_until", "")).strip())

    def test_refill_uses_category_row_page_unlock_hours_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_page_unlock_category_override.db"
            settings = _dummy_settings(db_path)
            now_ts = 1_700_000_000
            called_offsets = []

            def _fake_run_rpa_page(*, query: str, offset: int, limit: int, **kwargs):
                called_offsets.append(offset)
                return {
                    "ok": True,
                    "rows": [
                        {
                            "query": query,
                            "sold_90d_count": 100,
                            "sold_price_min": 120.0,
                            "metadata": {
                                "filtered_result_rows": [
                                    {"title": f"CASIO GW-M5610U-{offset}JF watch", "rank": 1},
                                ]
                            },
                        }
                    ],
                    "reason": "ok",
                    "daily_limit_reached": False,
                }

            category_row = {
                "phase_a_big_words": ["G-SHOCK"],
                "phase_a_page_unlock_hours": {"G-SHOCK": 8},
            }

            with patch.dict(
                "os.environ",
                {
                    "DB_BACKEND": "sqlite",
                    "MINER_SEED_POOL_REFILL_THRESHOLD": "60",
                    "MINER_SEED_POOL_PAGE_SIZE": "50",
                    "MINER_SEED_POOL_MAX_PAGES": "5",
                    "MINER_SEED_POOL_TARGET_COUNT": "100",
                    "MINER_SEED_POOL_PAGE_FRESH_DAYS": "0",
                    "MINER_STAGEA_QUERY_PAGE_UNLOCK_ENABLED": "1",
                    "MINER_STAGEA_QUERY_PAGE_UNLOCK_HOURS_DEFAULT": "24",
                    "MINER_STAGEA_QUERY_PAGE_UNLOCK_MIN_PAGES": "1",
                },
                clear=False,
            ), patch.object(miner_seed_pool.time, "time", return_value=float(now_ts)), patch.object(
                miner_seed_pool, "_run_rpa_page", side_effect=_fake_run_rpa_page
            ):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    conn.execute(
                        """
                        INSERT INTO miner_seed_refill_pages (
                            category_key, query_key, page_offset, page_size, fetched_at, result_count, new_seed_count, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "watch",
                            "g-shock",
                            0,
                            50,
                            miner_seed_pool.utc_iso(now_ts - (16 * 3600)),
                            50,
                            5,
                            miner_seed_pool.utc_iso(now_ts - (16 * 3600)),
                        ),
                    )
                    summary = miner_seed_pool._refill_seed_pool(
                        conn,
                        category_key="watch",
                        category_label="腕時計",
                        category_row=category_row,
                        stage_a_big_word_limit=1,
                    )

            self.assertEqual(called_offsets, [0, 50])
            query_runs = summary.get("query_runs", []) if isinstance(summary, dict) else []
            self.assertTrue(isinstance(query_runs, list) and query_runs)
            first_run = query_runs[0] if isinstance(query_runs[0], dict) else {}
            unlock = first_run.get("page_unlock", {}) if isinstance(first_run.get("page_unlock"), dict) else {}
            self.assertEqual(int(unlock.get("fetch_quota_pages", -1)), 2)
            self.assertEqual(str(unlock.get("hours_source", "")), "category_row_query")

    def test_refill_minimize_transitions_uses_large_single_page(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_minimize_transitions.db"
            settings = _dummy_settings(db_path)
            called_pages = []

            def _fake_run_rpa_page(*, query: str, offset: int, limit: int, **kwargs):
                called_pages.append({"query": query, "offset": offset, "limit": limit})
                return {
                    "ok": True,
                    "rows": [{"query": query, "metadata": {"filtered_result_rows": []}}],
                    "reason": "ok",
                    "daily_limit_reached": False,
                }

            with patch.dict(
                "os.environ",
                {
                    "DB_BACKEND": "sqlite",
                    "MINER_SEED_POOL_REFILL_THRESHOLD": "60",
                    "MINER_SEED_POOL_PAGE_SIZE": "50",
                    "MINER_SEED_POOL_MAX_PAGES": "4",
                    "MINER_STAGEA_TRANSITION_PAGE_SIZE": "200",
                    "MINER_STAGEA_TRANSITION_MAX_PAGES_PER_QUERY": "1",
                },
                clear=False,
            ), patch.object(miner_seed_pool, "_category_big_words", return_value=["watch"]), patch.object(
                miner_seed_pool, "_run_rpa_page", side_effect=_fake_run_rpa_page
            ):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    summary = miner_seed_pool._refill_seed_pool(
                        conn,
                        category_key="watch",
                        category_label="腕時計",
                        category_row={},
                        stage_a_minimize_transitions=True,
                    )

            self.assertEqual(len(called_pages), 1)
            self.assertEqual(called_pages[0], {"query": "watch", "offset": 0, "limit": 200})
            self.assertTrue(bool(summary.get("minimize_transitions")))
            self.assertEqual(int(summary.get("transition_page_size", 0)), 200)
            self.assertEqual(int(summary.get("transition_max_pages_per_query", 0)), 1)

    def test_take_seeds_for_run_prefers_oldest_created(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_oldest.db"
            settings = _dummy_settings(db_path)
            now_ts = 1_700_000_000
            with patch.dict("os.environ", {"DB_BACKEND": "sqlite"}, clear=False):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    conn.execute(
                        """
                        INSERT INTO miner_seed_pool (
                            category_key, seed_query, seed_key, source_title, source_item_url,
                            source_page, source_offset, source_rank, created_at, expires_at, metadata_json
                        )
                        VALUES (?, ?, ?, ?, ?, 1, 0, 10, ?, ?, '{}')
                        """,
                        (
                            "watch",
                            "NEWER SEED",
                            "NEWERSEED",
                            "newer",
                            "",
                            miner_seed_pool.utc_iso(now_ts - 100),
                            miner_seed_pool.utc_iso(now_ts + 86400),
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO miner_seed_pool (
                            category_key, seed_query, seed_key, source_title, source_item_url,
                            source_page, source_offset, source_rank, created_at, expires_at, metadata_json
                        )
                        VALUES (?, ?, ?, ?, ?, 1, 0, 20, ?, ?, '{}')
                        """,
                        (
                            "watch",
                            "OLDER SEED",
                            "OLDERSEED",
                            "older",
                            "",
                            miner_seed_pool.utc_iso(now_ts - 1000),
                            miner_seed_pool.utc_iso(now_ts + 86400),
                        ),
                    )
                    rows, _ = miner_seed_pool._take_seeds_for_run(
                        conn, category_key="watch", take_count=1, now_ts=now_ts
                    )

            self.assertEqual(len(rows), 1)
            self.assertEqual(str(rows[0].get("seed_key")), "OLDERSEED")

    def test_take_seeds_normalizes_trailing_new_variant_and_dedupes_existing_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_variant.db"
            settings = _dummy_settings(db_path)
            now_ts = 1_700_000_000
            with patch.dict("os.environ", {"DB_BACKEND": "sqlite"}, clear=False):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    conn.execute(
                        """
                        INSERT INTO miner_seed_pool (
                            category_key, seed_query, seed_key, source_title, source_item_url,
                            source_page, source_offset, source_rank, created_at, expires_at, metadata_json
                        )
                        VALUES (?, ?, ?, ?, ?, 1, 0, 1, ?, ?, '{}')
                        """,
                        (
                            "sneakers",
                            "DD1391-100 NEW",
                            "DD1391100NEW",
                            "seed-new",
                            "",
                            miner_seed_pool.utc_iso(now_ts - 200),
                            miner_seed_pool.utc_iso(now_ts + 86400),
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO miner_seed_pool (
                            category_key, seed_query, seed_key, source_title, source_item_url,
                            source_page, source_offset, source_rank, created_at, expires_at, metadata_json
                        )
                        VALUES (?, ?, ?, ?, ?, 1, 0, 2, ?, ?, '{}')
                        """,
                        (
                            "sneakers",
                            "DD1391-100",
                            "DD1391100",
                            "seed-base",
                            "",
                            miner_seed_pool.utc_iso(now_ts - 100),
                            miner_seed_pool.utc_iso(now_ts + 86400),
                        ),
                    )
                    rows, skipped = miner_seed_pool._take_seeds_for_run(
                        conn,
                        category_key="sneakers",
                        take_count=1,
                        now_ts=now_ts,
                    )
                    remaining = conn.execute(
                        "SELECT seed_query, seed_key FROM miner_seed_pool WHERE category_key = ?",
                        ("sneakers",),
                    ).fetchall()

            self.assertEqual(len(rows), 1)
            self.assertEqual(str(rows[0].get("seed_key")), "DD1391100")
            self.assertEqual(str(rows[0].get("seed_query")), "DD1391-100")
            self.assertEqual(int(skipped), 0)
            self.assertEqual(len(remaining), 1)

    def test_take_seeds_skips_active_low_liquidity_cooldown_seed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_cooldown_skip.db"
            settings = _dummy_settings(db_path)
            now_ts = 1_700_000_000
            with patch.dict("os.environ", {"DB_BACKEND": "sqlite"}, clear=False):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    inserted = miner_seed_pool._insert_seed_rows(
                        conn,
                        category_key="watch",
                        rows=[
                            {
                                "seed_query": "CASIO GW-M5610U-1JF",
                                "source_title": "seed",
                                "source_item_url": "",
                                "source_rank": 1,
                                "metadata": {},
                            }
                        ],
                        ttl_days=7,
                    )
                    self.assertEqual(inserted, 1)
                    saved = miner_seed_pool._upsert_low_liquidity_cooldowns(
                        conn,
                        category_key="watch",
                        rows=[
                            {
                                "seed_query": "CASIO GW-M5610U-1JF",
                                "seed_key": "CASIOGWM5610U1JF",
                                "sold_90d_count": 0,
                                "min_required": 3,
                                "metadata": {"note": "test"},
                            }
                        ],
                        now_ts=now_ts,
                    )
                    self.assertEqual(saved, 1)
                    preview_count, preview_skipped = miner_seed_pool._preview_seeds_for_run(
                        conn,
                        category_key="watch",
                        take_count=5,
                        now_ts=now_ts,
                    )
                    rows, skipped = miner_seed_pool._take_seeds_for_run(
                        conn,
                        category_key="watch",
                        take_count=5,
                        now_ts=now_ts,
                    )

            self.assertEqual(preview_count, 0)
            self.assertEqual(preview_skipped, 1)
            self.assertEqual(len(rows), 0)
            self.assertEqual(skipped, 1)

    def test_get_seed_pool_status_returns_current_snapshot_without_refill(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_status.db"
            settings = _dummy_settings(db_path)
            with patch.dict("os.environ", {"DB_BACKEND": "sqlite"}, clear=False):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    inserted = miner_seed_pool._insert_seed_rows(
                        conn,
                        category_key="watch",
                        rows=[
                            {
                                "seed_query": "CASIO GW-M5610U-1JF",
                                "source_title": "seed",
                                "source_item_url": "",
                                "source_rank": 1,
                                "metadata": {"seed_quality_score": 80},
                            }
                        ],
                        ttl_days=7,
                    )
                    self.assertEqual(inserted, 1)

                payload = miner_seed_pool.get_seed_pool_status(
                    category_query="watch",
                    settings=settings,
                )

            seed_pool = payload.get("seed_pool", {}) if isinstance(payload, dict) else {}
            refill = seed_pool.get("refill", {}) if isinstance(seed_pool, dict) else {}
            self.assertEqual(str(payload.get("query")), "watch")
            self.assertGreaterEqual(int(seed_pool.get("available_after_refill", 0)), 1)
            self.assertGreaterEqual(int(seed_pool.get("selected_seed_count", 0)), 1)
            self.assertIn(str(refill.get("reason", "")), {"snapshot", "threshold_not_reached", "refilled"})

    def test_reset_seed_pool_category_state_clears_wait_state_and_page_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_reset.db"
            settings = _dummy_settings(db_path)
            now_ts = 1_700_000_000
            with patch.dict("os.environ", {"DB_BACKEND": "sqlite"}, clear=False):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    inserted = miner_seed_pool._insert_seed_rows(
                        conn,
                        category_key="watch",
                        rows=[
                            {
                                "seed_query": "CASIO GW-M5610U-1JF",
                                "source_title": "seed",
                                "source_item_url": "",
                                "source_rank": 1,
                                "metadata": {},
                            }
                        ],
                        ttl_days=7,
                    )
                    self.assertEqual(inserted, 1)
                    conn.execute(
                        """
                        INSERT INTO miner_seed_refill_state (
                            category_key, last_refill_at, last_refill_status, last_refill_message,
                            last_rank_checked, cooldown_until, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "watch",
                            miner_seed_pool.utc_iso(now_ts - 600),
                            "rank_limit_cooldown",
                            "watch: cooldown active",
                            2000,
                            miner_seed_pool.utc_iso(now_ts + 86400),
                            miner_seed_pool.utc_iso(now_ts - 600),
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO miner_seed_refill_pages (
                            category_key, query_key, page_offset, page_size, fetched_at, result_count, new_seed_count, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "watch",
                            "watch",
                            0,
                            50,
                            miner_seed_pool.utc_iso(now_ts - 300),
                            50,
                            5,
                            miner_seed_pool.utc_iso(now_ts - 300),
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO miner_seed_liquidity_cooldowns (
                            category_key, seed_key, seed_query, reason_code, sold_90d_count, min_required,
                            blocked_until, last_rejected_at, reject_count, metadata_json, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "watch",
                            "CASIOGWM5610U1JF",
                            "CASIO GW-M5610U-1JF",
                            "sold_zero",
                            0,
                            3,
                            miner_seed_pool.utc_iso(now_ts + 86400),
                            miner_seed_pool.utc_iso(now_ts - 120),
                            1,
                            "{}",
                            miner_seed_pool.utc_iso(now_ts - 120),
                            miner_seed_pool.utc_iso(now_ts - 120),
                        ),
                    )
                    conn.commit()

                payload = miner_seed_pool.reset_seed_pool_category_state(
                    category_query="watch",
                    settings=settings,
                )

                self.assertEqual(str(payload.get("category_key")), "watch")
                self.assertGreaterEqual(int(payload.get("cleared_page_windows", 0)), 1)
                self.assertGreaterEqual(int(payload.get("cleared_liquidity_cooldowns", 0)), 1)
                self.assertTrue(bool(payload.get("had_refill_state")))
                self.assertEqual(int(payload.get("available_after", 0)), 1)

                with connect(settings.db_path) as conn:
                    init_db(conn)
                    row_state = conn.execute(
                        "SELECT 1 FROM miner_seed_refill_state WHERE category_key = ?",
                        ("watch",),
                    ).fetchone()
                    row_pages = conn.execute(
                        "SELECT COUNT(*) AS c FROM miner_seed_refill_pages WHERE category_key = ?",
                        ("watch",),
                    ).fetchone()
                    row_pool = conn.execute(
                        "SELECT COUNT(*) AS c FROM miner_seed_pool WHERE category_key = ?",
                        ("watch",),
                    ).fetchone()
                    row_cooldowns = conn.execute(
                        "SELECT COUNT(*) AS c FROM miner_seed_liquidity_cooldowns WHERE category_key = ?",
                        ("watch",),
                    ).fetchone()

                self.assertIsNone(row_state)
                self.assertEqual(int(row_pages["c"] if row_pages else 0), 0)
                self.assertEqual(int(row_pool["c"] if row_pool else 0), 1)
                self.assertEqual(int(row_cooldowns["c"] if row_cooldowns else 0), 0)

    def test_reset_seed_pool_category_state_clear_history_removes_category_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_reset_history.db"
            settings = _dummy_settings(db_path)
            journal_path = Path(tmp) / "miner_seed_run_journal.jsonl"
            now_ts = 1_700_000_000
            with patch.dict("os.environ", {"DB_BACKEND": "sqlite"}, clear=False), patch.object(
                miner_seed_pool, "_SEED_RUN_JOURNAL_PATH", journal_path
            ):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    inserted = miner_seed_pool._insert_seed_rows(
                        conn,
                        category_key="watch",
                        rows=[
                            {
                                "seed_query": "CASIO GW-M5610U-1JF",
                                "source_title": "seed",
                                "source_item_url": "",
                                "source_rank": 1,
                                "metadata": {},
                            }
                        ],
                        ttl_days=7,
                    )
                    self.assertEqual(inserted, 1)
                    seed_row = conn.execute(
                        "SELECT id FROM miner_seed_pool WHERE category_key = ? LIMIT 1",
                        ("watch",),
                    ).fetchone()
                    seed_id = int(seed_row["id"]) if seed_row is not None else 0
                    self.assertGreater(seed_id, 0)

                    candidate_cur = conn.execute(
                        """
                        INSERT INTO miner_candidates (
                            source_site, market_site, source_item_id, market_item_id,
                            source_title, market_title, condition, match_level, match_score,
                            expected_profit_usd, expected_margin_rate,
                            fx_rate, fx_source, status, listing_state, metadata_json,
                            created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "rakuten",
                            "ebay",
                            "source-1",
                            "market-1",
                            "CASIO GW-M5610U-1JF",
                            "CASIO GW-M5610U-1JF sold",
                            "new",
                            "L2_precise",
                            0.9,
                            10.0,
                            0.1,
                            150.0,
                            "test",
                            "pending",
                            "dummy_pending",
                            '{"seed_pool":{"id":%d,"seed_query":"CASIO GW-M5610U-1JF","category_key":"watch"}}' % seed_id,
                            miner_seed_pool.utc_iso(now_ts - 60),
                            miner_seed_pool.utc_iso(now_ts - 60),
                        ),
                    )
                    candidate_id = int(candidate_cur.lastrowid)
                    conn.execute(
                        """
                        INSERT INTO miner_rejections (candidate_id, issue_targets_json, reason_text, created_at)
                        VALUES (?, ?, ?, ?)
                        """,
                        (
                            candidate_id,
                            "[]",
                            "test reject",
                            miner_seed_pool.utc_iso(now_ts - 30),
                        ),
                    )
                    conn.commit()

                journal_path.write_text(
                    "\n".join(
                        [
                            '{"run_at":"2026-02-24T00:00:00Z","category_key":"watch","created_count":1}',
                            '{"run_at":"2026-02-24T00:00:01Z","category_key":"sneakers","created_count":1}',
                        ]
                    )
                    + "\n",
                    encoding="utf-8",
                )

                payload = miner_seed_pool.reset_seed_pool_category_state(
                    category_query="watch",
                    settings=settings,
                    clear_history=True,
                )

                self.assertEqual(int(payload.get("cleared_seed_rows", 0)), 1)
                self.assertEqual(int(payload.get("cleared_candidate_rows", 0)), 1)
                self.assertEqual(int(payload.get("cleared_rejection_rows", 0)), 1)
                self.assertEqual(int(payload.get("cleared_seed_journal_rows", 0)), 1)
                self.assertEqual(int(payload.get("available_after", 0)), 0)

                with connect(settings.db_path) as conn:
                    init_db(conn)
                    row_pool = conn.execute(
                        "SELECT COUNT(*) AS c FROM miner_seed_pool WHERE category_key = ?",
                        ("watch",),
                    ).fetchone()
                    row_candidates = conn.execute(
                        "SELECT COUNT(*) AS c FROM miner_candidates",
                    ).fetchone()
                    row_rejections = conn.execute(
                        "SELECT COUNT(*) AS c FROM miner_rejections",
                    ).fetchone()

                self.assertEqual(int(row_pool["c"] if row_pool else 0), 0)
                self.assertEqual(int(row_candidates["c"] if row_candidates else 0), 0)
                self.assertEqual(int(row_rejections["c"] if row_rejections else 0), 0)
                remaining_journal = journal_path.read_text(encoding="utf-8")
                self.assertIn('"category_key":"sneakers"', remaining_journal)
                self.assertNotIn('"category_key":"watch"', remaining_journal)

    def test_refill_filters_accessory_title(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_accessory.db"
            settings = _dummy_settings(db_path)
            fake_row = {
                "query": "watch",
                "sold_90d_count": 10,
                "sold_price_min": 120.0,
                "metadata": {
                    "filtered_result_rows": [
                        {"title": "CASIO G-SHOCK replacement band", "item_url": "https://www.ebay.com/itm/123456789012", "rank": 1},
                    ]
                },
            }
            with patch.dict(
                "os.environ",
                {
                    "DB_BACKEND": "sqlite",
                    "MINER_SEED_POOL_REFILL_THRESHOLD": "60",
                    "MINER_SEED_POOL_PAGE_SIZE": "50",
                    "MINER_SEED_POOL_MAX_PAGES": "1",
                    "MINER_SEED_POOL_TARGET_COUNT": "5",
                    "MINER_SEED_API_SUPPLEMENT_ENABLED": "0",
                },
                clear=False,
            ), patch.object(miner_seed_pool, "_run_rpa_page", return_value={"ok": True, "rows": [fake_row], "reason": "ok"}):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    summary = miner_seed_pool._refill_seed_pool(
                        conn,
                        category_key="watch",
                        category_label="腕時計",
                        category_row={"seed_brands": ["CASIO"]},
                    )

            self.assertEqual(int(summary.get("added_count", 0)), 0)
            self.assertGreaterEqual(int(summary.get("accessory_filtered_count", 0)), 1)

    def test_refill_uses_api_backfill_when_title_is_weak(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_api_backfill.db"
            settings = _dummy_settings(db_path)
            fake_row = {
                "query": "watch",
                "sold_90d_count": 10,
                "sold_price_min": 150.0,
                "metadata": {
                    "filtered_result_rows": [
                        {"title": "Seiko watch new", "item_url": "https://www.ebay.com/itm/123456789012", "rank": 1},
                    ]
                },
            }
            with patch.dict(
                "os.environ",
                {
                    "DB_BACKEND": "sqlite",
                    "MINER_SEED_POOL_REFILL_THRESHOLD": "60",
                    "MINER_SEED_POOL_PAGE_SIZE": "50",
                    "MINER_SEED_POOL_MAX_PAGES": "1",
                    "MINER_SEED_POOL_TARGET_COUNT": "5",
                    "MINER_SEED_API_SUPPLEMENT_ENABLED": "1",
                },
                clear=False,
            ), patch.object(
                miner_seed_pool, "_run_rpa_page", return_value={"ok": True, "rows": [fake_row], "reason": "ok"}
            ), patch.object(
                miner_seed_pool,
                "_api_seed_candidates_from_item_url",
                return_value=(["SEIKO SBDC101"], "ok"),
            ):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    summary = miner_seed_pool._refill_seed_pool(
                        conn,
                        category_key="watch",
                        category_label="腕時計",
                        category_row={"seed_brands": ["SEIKO"]},
                    )

            self.assertGreaterEqual(int(summary.get("added_count", 0)), 1)
            backfill = summary.get("seed_api_backfill", {})
            self.assertGreaterEqual(int(backfill.get("attempts", 0)), 1)
            self.assertGreaterEqual(int(backfill.get("hits", 0)), 1)

    def test_refill_uses_raw_result_rows_fallback_when_filtered_rows_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_raw_fallback.db"
            settings = _dummy_settings(db_path)
            fake_row = {
                "query": "watch",
                "sold_90d_count": 9,
                "sold_price_min": 130.0,
                "metadata": {
                    "raw_row_count": 50,
                    "filtered_row_count": 0,
                    "filtered_result_rows": [],
                    "raw_result_rows": [
                        {
                            "title": "CASIO G-SHOCK GW-M5610U-1JF watch",
                            "item_url": "https://www.ebay.com/itm/123456789012",
                            "rank": 1,
                        }
                    ],
                },
            }
            with patch.dict(
                "os.environ",
                {
                    "DB_BACKEND": "sqlite",
                    "MINER_SEED_POOL_REFILL_THRESHOLD": "60",
                    "MINER_SEED_POOL_PAGE_SIZE": "50",
                    "MINER_SEED_POOL_MAX_PAGES": "1",
                    "MINER_SEED_POOL_TARGET_COUNT": "5",
                    "MINER_SEED_API_SUPPLEMENT_ENABLED": "0",
                },
                clear=False,
            ), patch.object(miner_seed_pool, "_run_rpa_page", return_value={"ok": True, "rows": [fake_row], "reason": "ok"}):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    summary = miner_seed_pool._refill_seed_pool(
                        conn,
                        category_key="watch",
                        category_label="腕時計",
                        category_row={"seed_brands": ["CASIO"]},
                    )

            self.assertGreaterEqual(int(summary.get("added_count", 0)), 1)

    def test_refill_uses_query_fallback_when_raw_row_count_positive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_query_fallback.db"
            settings = _dummy_settings(db_path)
            fake_row = {
                "query": "GW-M5610U-1JF",
                "sold_90d_count": 7,
                "sold_price_min": 120.0,
                "metadata": {
                    "raw_row_count": 50,
                    "filtered_row_count": 0,
                    "filtered_result_rows": [],
                },
            }
            with patch.dict(
                "os.environ",
                {
                    "DB_BACKEND": "sqlite",
                    "MINER_SEED_POOL_REFILL_THRESHOLD": "60",
                    "MINER_SEED_POOL_PAGE_SIZE": "50",
                    "MINER_SEED_POOL_MAX_PAGES": "1",
                    "MINER_SEED_POOL_TARGET_COUNT": "5",
                    "MINER_SEED_API_SUPPLEMENT_ENABLED": "0",
                },
                clear=False,
            ), patch.object(miner_seed_pool, "_run_rpa_page", return_value={"ok": True, "rows": [fake_row], "reason": "ok"}):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    summary = miner_seed_pool._refill_seed_pool(
                        conn,
                        category_key="watch",
                        category_label="腕時計",
                        category_row={"seed_brands": ["CASIO"]},
                    )

            self.assertGreaterEqual(int(summary.get("added_count", 0)), 1)

    def test_stage2_liquidity_retry_on_miss_can_recover_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_liquidity_retry.db"
            settings = _dummy_settings(db_path)
            fake_page_row = {
                "query": "watch",
                "metadata": {
                    "filtered_result_rows": [
                        {"title": "CASIO GW-M5610U-1JF watch", "rank": 1},
                    ]
                },
            }
            created_counter = {"value": 0}
            liquidity_calls = {"count": 0}
            refresh_calls = {"count": 0}

            def _fake_search(query: str, limit: int, timeout: int, page: int = 1, require_in_stock: bool = True):
                item = miner_seed_pool.MarketItem(
                    site="rakuten",
                    item_id=f"rk-{query}-{page}",
                    title=f"{query} 新品 本体",
                    item_url=f"https://example.com/{query}/{page}",
                    image_url="https://example.com/image.jpg",
                    price=10000.0,
                    shipping=0.0,
                    currency="JPY",
                    condition="new",
                    identifiers={"mpn": "GW-M5610U-1JF"},
                    raw={},
                )
                return [item], {"status": 200, "category_filter": {"applied": True}, "cache_hit": False}

            def _fake_liquidity(**kwargs):
                liquidity_calls["count"] += 1
                if liquidity_calls["count"] == 1:
                    return {"sold_90d_count": -1, "source": "rpa_json", "unavailable_reason": "rpa_json_no_match", "metadata": {}}
                return {
                    "sold_90d_count": 12,
                    "metadata": {
                        "sold_price_min": 180.0,
                        "sold_sample": {
                            "item_url": "https://www.ebay.com/itm/123456789012",
                            "title": "eBay sold title",
                            "image_url": "https://example.com/sold.jpg",
                            "sold_price": 180.0,
                        },
                    },
                    "source": "rpa_json",
                    "unavailable_reason": "",
                }

            def _fake_refresh(*args, **kwargs):
                refresh_calls["count"] += 1
                return {"enabled": True, "ran": True, "reason": "ok", "daily_limit_reached": False, "queries": ["GW-M5610U-1JF"]}

            def _fake_create(payload, settings=None):
                created_counter["value"] += 1
                cid = created_counter["value"]
                return {
                    "id": cid,
                    "source_title": payload.get("source_title", ""),
                    "market_title": payload.get("market_title", ""),
                    "match_score": payload.get("match_score", 0.0),
                    "expected_profit_usd": payload.get("expected_profit_usd", 0.0),
                }

            env = {
                "DB_BACKEND": "sqlite",
                "LIQUIDITY_PROVIDER_MODE": "rpa_json",
                "LIQUIDITY_RPA_AUTO_REFRESH": "1",
                "LIQUIDITY_RPA_RUN_ON_FETCH": "1",
                "MINER_STAGE2_LIQUIDITY_REFRESH_ON_MISS_ENABLED": "1",
                "MINER_STAGE2_LIQUIDITY_REFRESH_ON_MISS_BUDGET": "2",
                "MINER_STAGE2_LIQUIDITY_PREFETCH_MAX_QUERIES": "1",
                "MINER_SEED_POOL_MAX_PAGES": "1",
                "MINER_SEED_POOL_TARGET_COUNT": "1",
                "MINER_SEED_POOL_REFILL_THRESHOLD": "0",
                "MINER_SEED_POOL_RUN_BATCH_SIZE": "1",
                "MINER_SEED_POOL_PAGE_SIZE": "50",
            }
            with patch.dict("os.environ", env, clear=False), patch.object(
                miner_seed_pool, "_run_rpa_page", return_value={"ok": True, "rows": [fake_page_row], "reason": "ok"}
            ), patch.object(
                miner_seed_pool, "_search_rakuten", side_effect=_fake_search
            ), patch.object(
                miner_seed_pool, "_search_yahoo", side_effect=_fake_search
            ), patch.object(
                miner_seed_pool, "get_liquidity_signal", side_effect=_fake_liquidity
            ), patch.object(
                miner_seed_pool, "_refresh_liquidity_rpa", side_effect=_fake_refresh
            ), patch.object(
                miner_seed_pool, "create_miner_candidate", side_effect=_fake_create
            ):
                payload = miner_seed_pool.run_seeded_fetch(
                    category_query="watch",
                    source_sites=["rakuten"],
                    market_site="ebay",
                    limit_per_site=20,
                    max_candidates=5,
                    min_match_score=0.72,
                    min_profit_usd=0.01,
                    min_margin_rate=0.03,
                    require_in_stock=True,
                    timeout=10,
                    timed_mode=True,
                    min_target_candidates=1,
                    timebox_sec=60,
                    max_passes=2,
                    continue_after_target=False,
                    settings=settings,
                )

            self.assertGreaterEqual(int(payload.get("created_count", 0)), 1)
            self.assertGreaterEqual(int(refresh_calls["count"]), 1)

    def test_refill_stops_on_rpa_timeout_guard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_timeout_guard.db"
            settings = _dummy_settings(db_path)
            with patch.dict(
                "os.environ",
                {
                    "DB_BACKEND": "sqlite",
                    "MINER_SEED_POOL_REFILL_THRESHOLD": "60",
                    "MINER_SEED_POOL_PAGE_SIZE": "50",
                    "MINER_SEED_POOL_MAX_PAGES": "3",
                    "MINER_SEED_POOL_TARGET_COUNT": "100",
                    "MINER_SEED_POOL_MAX_TIMEOUT_PAGES_PER_RUN": "1",
                },
                clear=False,
            ), patch.object(
                miner_seed_pool,
                "_run_rpa_page",
                return_value={"ok": False, "rows": [], "reason": "rpa_failed", "returncode": -9},
            ):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    summary = miner_seed_pool._refill_seed_pool(
                        conn,
                        category_key="watch",
                        category_label="腕時計",
                        category_row={},
                    )

        self.assertEqual(str(summary.get("reason")), "rpa_timeout_guard")
        self.assertGreaterEqual(int(summary.get("rpa_timeout_pages", 0)), 1)

    def test_refill_records_diagnostics_and_tuning_on_rpa_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_diag_rpa_failure.db"
            settings = _dummy_settings(db_path)
            with patch.dict(
                "os.environ",
                {
                    "DB_BACKEND": "sqlite",
                    "MINER_SEED_POOL_REFILL_THRESHOLD": "60",
                    "MINER_SEED_POOL_PAGE_SIZE": "50",
                    "MINER_SEED_POOL_MAX_PAGES": "1",
                    "MINER_SEED_POOL_TARGET_COUNT": "100",
                },
                clear=False,
            ), patch.object(
                miner_seed_pool,
                "_category_big_words",
                return_value=["watch"],
            ), patch.object(
                miner_seed_pool,
                "_run_rpa_page",
                return_value={
                    "ok": False,
                    "rows": [],
                    "reason": "rpa_failed",
                    "returncode": 1,
                    "daily_limit_reached": False,
                    "stdout_tail": ["rpa error"],
                    "stderr_tail": ["stack"],
                },
            ):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    summary = miner_seed_pool._refill_seed_pool(
                        conn,
                        category_key="watch",
                        category_label="腕時計",
                        category_row={},
                    )

        diagnostics = summary.get("diagnostics", {}) if isinstance(summary, dict) else {}
        tuning = summary.get("tuning_recommendations", []) if isinstance(summary, dict) else []
        self.assertEqual(int(diagnostics.get("rpa_failed_pages", 0)), 1)
        self.assertEqual(int((diagnostics.get("page_reason_counts") or {}).get("rpa_failed", 0)), 1)
        self.assertGreaterEqual(len(diagnostics.get("failure_samples", [])), 1)
        self.assertTrue(any(str(row.get("code", "")) == "stabilize_rpa_fetch" for row in tuning if isinstance(row, dict)))

    def test_refill_fresh_window_skip_has_tuning_recommendation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_diag_fresh_skip.db"
            settings = _dummy_settings(db_path)
            now_ts = 1_700_000_000
            with patch.dict(
                "os.environ",
                {
                    "DB_BACKEND": "sqlite",
                    "MINER_SEED_POOL_REFILL_THRESHOLD": "60",
                    "MINER_SEED_POOL_PAGE_SIZE": "50",
                    "MINER_SEED_POOL_MAX_PAGES": "1",
                    "MINER_SEED_POOL_TARGET_COUNT": "100",
                    "MINER_SEED_POOL_PAGE_FRESH_DAYS": "7",
                },
                clear=False,
            ), patch.object(
                miner_seed_pool.time, "time", return_value=float(now_ts)
            ), patch.object(
                miner_seed_pool,
                "_category_big_words",
                return_value=["watch"],
            ), patch.object(
                miner_seed_pool,
                "_run_rpa_page",
                side_effect=AssertionError("fresh skip ではRPA実行されない想定"),
            ):
                with connect(settings.db_path) as conn:
                    init_db(conn)
                    conn.execute(
                        """
                        INSERT INTO miner_seed_refill_pages (
                            category_key, query_key, page_offset, page_size, fetched_at, result_count, new_seed_count, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "watch",
                            "watch",
                            0,
                            50,
                            miner_seed_pool.utc_iso(now_ts - 3600),
                            40,
                            0,
                            miner_seed_pool.utc_iso(now_ts - 3600),
                        ),
                    )
                    summary = miner_seed_pool._refill_seed_pool(
                        conn,
                        category_key="watch",
                        category_label="腕時計",
                        category_row={},
                    )

        tuning = summary.get("tuning_recommendations", []) if isinstance(summary, dict) else []
        self.assertEqual(str(summary.get("reason", "")), "fresh_window_skip")
        self.assertTrue(any(str(row.get("code", "")) == "reduce_page_fresh_window" for row in tuning if isinstance(row, dict)))

    def test_timebox_respects_min_stage1_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "seed_pool_min_attempts.db"
            settings = _dummy_settings(db_path)
            fake_page_row = {
                "query": "watch",
                "sold_90d_count": 12,
                "sold_price_min": 120.0,
                "metadata": {
                    "filtered_result_rows": [
                        {"title": "CASIO GW-M5610U-1JF watch", "rank": 1},
                        {"title": "SEIKO SBDC101 watch", "rank": 2},
                        {"title": "CITIZEN NB1060-12L watch", "rank": 3},
                    ]
                },
            }

            def _empty_search(*args, **kwargs):
                return [], {"status": 200, "cache_hit": False}

            monotonic_counter = {"value": 0}

            def _fake_monotonic() -> float:
                monotonic_counter["value"] += 1
                return float(monotonic_counter["value"] * 11)

            env = {
                "DB_BACKEND": "sqlite",
                "MINER_SEED_POOL_REFILL_THRESHOLD": "60",
                "MINER_SEED_POOL_RUN_BATCH_SIZE": "3",
                "MINER_SEED_POOL_PAGE_SIZE": "50",
                "MINER_SEED_POOL_MAX_PAGES": "1",
                "MINER_SEED_POOL_TARGET_COUNT": "3",
                "MINER_TIMED_FETCH_MIN_STAGE1_ATTEMPTS": "2",
            }
            with patch.dict("os.environ", env, clear=False), patch.object(
                miner_seed_pool, "_run_rpa_page", return_value={"ok": True, "rows": [fake_page_row], "reason": "ok"}
            ), patch.object(
                miner_seed_pool, "_search_rakuten", side_effect=_empty_search
            ), patch.object(
                miner_seed_pool, "_search_yahoo", side_effect=_empty_search
            ), patch.object(
                miner_seed_pool.time, "monotonic", side_effect=_fake_monotonic
            ):
                payload = miner_seed_pool.run_seeded_fetch(
                    category_query="watch",
                    source_sites=["rakuten", "yahoo"],
                    market_site="ebay",
                    limit_per_site=10,
                    max_candidates=10,
                    min_match_score=0.7,
                    min_profit_usd=0.01,
                    min_margin_rate=0.03,
                    require_in_stock=True,
                    timeout=8,
                    timed_mode=True,
                    min_target_candidates=1,
                    timebox_sec=10,
                    max_passes=3,
                    continue_after_target=True,
                    settings=settings,
                )

        timed = payload.get("timed_fetch", {}) if isinstance(payload, dict) else {}
        self.assertEqual(str(timed.get("stop_reason", "")), "timebox_reached")
        self.assertGreaterEqual(int(timed.get("passes_run", 0)), 2)

    def test_run_rpa_page_enforces_phase_a_filter_requirements_by_default(self) -> None:
        captured: dict = {}

        def _fake_subprocess_run(*args, **kwargs):
            captured["cmd"] = list(args[0]) if args else []
            captured["env"] = dict(kwargs.get("env", {}))
            return subprocess.CompletedProcess(args[0], 0, stdout="", stderr="")

        with patch.dict("os.environ", {"DB_BACKEND": "sqlite"}, clear=False):
            os.environ.pop("LIQUIDITY_RPA_REQUIRE_LOCK_SELECTED_FILTERS", None)
            os.environ.pop("LIQUIDITY_RPA_REQUIRE_SOLD_SORT", None)
            os.environ.pop("LIQUIDITY_RPA_REQUIRE_MIN_PRICE_FILTER", None)
            os.environ.pop("LIQUIDITY_RPA_ENABLE_LOCK_SELECTED_FILTERS", None)
            os.environ.pop("LIQUIDITY_RPA_ENABLE_MIN_PRICE_FILTER_UI", None)
            os.environ.pop("LIQUIDITY_RPA_PRIMARY_SOLD_SORT", None)
            with patch.object(miner_seed_pool.subprocess, "run", side_effect=_fake_subprocess_run):
                result = miner_seed_pool._run_rpa_page(
                    query="G-SHOCK",
                    offset=0,
                    limit=50,
                    category_id=31387,
                    category_slug="wristwatches",
                    min_price_usd=100.0,
                )

        self.assertTrue(bool(result.get("ok")))
        self.assertIn("--min-price-usd", captured.get("cmd", []))
        self.assertIn("--sold-sort", captured.get("cmd", []))
        sold_sort_index = captured.get("cmd", []).index("--sold-sort")
        self.assertEqual(str(captured.get("cmd", [])[sold_sort_index + 1]), "recently_sold")
        self.assertEqual(str(captured.get("env", {}).get("LIQUIDITY_RPA_REQUIRE_LOCK_SELECTED_FILTERS")), "1")
        self.assertEqual(str(captured.get("env", {}).get("LIQUIDITY_RPA_REQUIRE_SOLD_SORT")), "1")
        self.assertEqual(str(captured.get("env", {}).get("LIQUIDITY_RPA_ENABLE_LOCK_SELECTED_FILTERS")), "1")
        self.assertEqual(str(captured.get("env", {}).get("LIQUIDITY_RPA_REQUIRE_MIN_PRICE_FILTER")), "1")
        self.assertEqual(str(captured.get("env", {}).get("LIQUIDITY_RPA_ENABLE_MIN_PRICE_FILTER_UI")), "1")
        search_params = result.get("rpa_search_params", {}) if isinstance(result, dict) else {}
        self.assertTrue(bool(search_params.get("require_lock_selected_filters")))
        self.assertTrue(bool(search_params.get("require_sold_sort")))
        self.assertTrue(bool(search_params.get("require_min_price_filter")))
        self.assertEqual(str(search_params.get("sold_sort", "")), "recently_sold")
        self.assertAlmostEqual(float(search_params.get("min_price_usd", 0.0)), 100.0)

    def test_run_rpa_page_passes_temporary_screenshot_template(self) -> None:
        captured: dict = {}

        def _fake_subprocess_run(*args, **kwargs):
            captured["cmd"] = list(args[0]) if args else []
            return subprocess.CompletedProcess(args[0], 0, stdout="", stderr="")

        with patch.dict(
            "os.environ",
            {
                "DB_BACKEND": "sqlite",
                "MINER_SEED_POOL_RPA_SCREENSHOT_TEMPLATE": "/tmp/phasea_{query}_{offset}_{ts}.png",
            },
            clear=False,
        ):
            with patch.object(miner_seed_pool.subprocess, "run", side_effect=_fake_subprocess_run):
                result = miner_seed_pool._run_rpa_page(
                    query="G-SHOCK",
                    offset=150,
                    limit=50,
                    category_id=31387,
                    category_slug="wristwatches",
                    min_price_usd=100.0,
                )

        cmd = captured.get("cmd", [])
        self.assertIn("--screenshot-after-filters", cmd)
        idx = cmd.index("--screenshot-after-filters")
        self.assertIn("_150_", str(cmd[idx + 1]))
        search_params = result.get("rpa_search_params", {}) if isinstance(result, dict) else {}
        self.assertIn("_150_", str(search_params.get("screenshot_after_filters", "")))

    def test_collect_row_entries_includes_sold_price(self) -> None:
        row = {
            "sold_90d_count": 44,
            "sold_price_min": 100.0,
            "metadata": {
                "raw_row_count": 1,
                "filtered_result_rows": [
                    {
                        "title": "CASIO G-SHOCK GA2100-1A",
                        "item_id": "v1|123456789012|0",
                        "item_url": "https://www.ebay.com/itm/123456789012",
                        "sold_price": 179.86,
                        "rank": 1,
                    }
                ],
            },
        }
        entries = miner_seed_pool._collect_row_entries(row)
        self.assertEqual(len(entries), 1)
        self.assertAlmostEqual(float(entries[0].get("sold_price", 0.0)), 179.86)


if __name__ == "__main__":
    unittest.main()

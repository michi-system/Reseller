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

    def test_run_seeded_fetch_uses_seed_pool_and_disables_per_seed_rpa_refresh(self) -> None:
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

            def _fake_fetch(**kwargs):
                created_counter["value"] += 1
                cid = created_counter["value"]
                return {
                    "created_count": 1,
                    "created_ids": [cid],
                    "created": [{"id": cid, "source_title": kwargs.get("query", ""), "market_title": kwargs.get("query", "")}],
                    "fetched": {
                        "ebay": {"count": 12, "calls_made": 2, "network_calls": 2, "cache_hits": 0, "stop_reason": "ok"},
                        "rakuten": {"count": 8, "calls_made": 2, "network_calls": 1, "cache_hits": 1, "stop_reason": "ok"},
                        "yahoo": {"count": 6, "calls_made": 2, "network_calls": 1, "cache_hits": 1, "stop_reason": "ok"},
                    },
                    "errors": [],
                    "hints": [],
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
                    with patch.object(miner_seed_pool, "fetch_live_miner_candidates", side_effect=_fake_fetch) as mocked_fetch:
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
            self.assertGreaterEqual(mocked_fetch.call_count, 2)
            self.assertTrue(any(bool(call.kwargs.get("run_rpa_refresh", False)) for call in mocked_fetch.call_args_list))
            self.assertTrue(any(not bool(call.kwargs.get("run_rpa_refresh", True)) for call in mocked_fetch.call_args_list))
            self.assertTrue(any(not bool(call.kwargs.get("persist_candidates", True)) for call in mocked_fetch.call_args_list))
            self.assertTrue(any(bool(call.kwargs.get("persist_candidates", False)) for call in mocked_fetch.call_args_list))

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

            def _fake_fetch(**kwargs):
                return {
                    "created_count": 0,
                    "created_ids": [],
                    "created": [],
                    "fetched": {},
                    "errors": [],
                    "hints": [],
                }

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
                miner_seed_pool, "fetch_live_miner_candidates", side_effect=_fake_fetch
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

            def _fake_run_rpa_page(*, query: str, offset: int, limit: int):
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

    def test_take_seeds_keeps_trailing_new_variant_as_distinct_seed(self) -> None:
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

            self.assertEqual(len(rows), 1)
            self.assertIn(str(rows[0].get("seed_key")), {"DD1391100", "DD1391100NEW"})
            self.assertEqual(int(skipped), 0)

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


if __name__ == "__main__":
    unittest.main()

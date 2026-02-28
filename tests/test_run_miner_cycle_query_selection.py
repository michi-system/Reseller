import unittest

from scripts import run_miner_cycle


class RunMinerCycleQuerySelectionTests(unittest.TestCase):
    def test_default_selection_is_capped_to_12(self) -> None:
        selected = run_miner_cycle._select_default_queries_oldest_first(stats_rows={})
        self.assertEqual(len(selected), run_miner_cycle.DEFAULT_QUERY_LIMIT)

    def test_default_selection_orders_by_oldest_last_attempt(self) -> None:
        stats_rows = {
            query: {"last_attempt_run_seq": 100 + idx}
            for idx, query in enumerate(run_miner_cycle.DEFAULT_QUERIES)
        }
        stats_rows[run_miner_cycle.DEFAULT_QUERIES[5]] = {"last_attempt_run_seq": 0}
        stats_rows[run_miner_cycle.DEFAULT_QUERIES[7]] = {"last_attempt_run_seq": 0}
        stats_rows[run_miner_cycle.DEFAULT_QUERIES[1]] = {"last_attempt_run_seq": 1}

        selected = run_miner_cycle._select_default_queries_oldest_first(stats_rows=stats_rows)

        self.assertEqual(selected[0], run_miner_cycle.DEFAULT_QUERIES[5])
        self.assertEqual(selected[1], run_miner_cycle.DEFAULT_QUERIES[7])
        self.assertEqual(selected[2], run_miner_cycle.DEFAULT_QUERIES[1])

    def test_explicit_queries_are_not_capped(self) -> None:
        explicit = run_miner_cycle.DEFAULT_QUERIES[:15]
        plan = run_miner_cycle._resolve_run_queries(
            cli_queries=",".join(explicit),
            disable_query_reorder=False,
            stats_rows={},
        )
        queries = plan.get("queries", [])
        self.assertEqual(plan.get("query_selection_mode"), "explicit_queries")
        self.assertEqual(len(queries), 15)
        self.assertEqual(queries, explicit)

    def test_explicit_queries_keep_reorder_behavior(self) -> None:
        stats_rows = {
            "a": {"attempts": 2, "network_calls": 10, "queue_gain_total": 0},
            "b": {"attempts": 2, "network_calls": 10, "queue_gain_total": 2},
            "c": {"attempts": 0, "network_calls": 0, "queue_gain_total": 0},
        }
        plan = run_miner_cycle._resolve_run_queries(
            cli_queries="a,b,c",
            disable_query_reorder=False,
            stats_rows=stats_rows,
        )
        self.assertEqual(plan.get("queries"), ["c", "b", "a"])
        self.assertTrue(bool(plan.get("query_reordered")))

        no_reorder_plan = run_miner_cycle._resolve_run_queries(
            cli_queries="a,b,c",
            disable_query_reorder=True,
            stats_rows=stats_rows,
        )
        self.assertEqual(no_reorder_plan.get("queries"), ["a", "b", "c"])
        self.assertFalse(bool(no_reorder_plan.get("query_reordered")))


if __name__ == "__main__":
    unittest.main()

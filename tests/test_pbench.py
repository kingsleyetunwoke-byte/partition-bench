import unittest

from pbench.explore import run, search, shrink
from pbench.model import contiguous, round_robin


class StrategyTest(unittest.TestCase):
    def test_strategies_cover_every_partition_once(self):
        members = ["w3", "w1", "w2"]
        for strategy in (round_robin, contiguous):
            shares = strategy(members, list(range(7)))
            assigned = sorted(p for share in shares.values() for p in share)
            self.assertEqual(assigned, list(range(7)))
            self.assertEqual(sorted(shares), sorted(members))

    def test_contiguous_gives_larger_blocks_first(self):
        self.assertEqual(
            contiguous(["a", "b", "c"], list(range(7))),
            {"a": [0, 1, 2], "b": [3, 4], "c": [5, 6]},
        )


class ExploreTest(unittest.TestCase):
    def test_replay_is_deterministic(self):
        first = run("naive", seed=7)
        again = run("naive", prefix=first.choices)
        self.assertEqual(first.trace, again.trace)
        self.assertEqual(first.kind, again.kind)

    def test_fenced_survives_random_schedules(self):
        for strategy in ("contiguous", "round_robin"):
            report = search("fenced", 300, strategy=strategy)
            self.assertEqual(report["failures"], {}, strategy)

    def test_naive_late_rollback_is_found_and_shrunk(self):
        failing = run("naive", seed=1)
        self.assertEqual(failing.kind, "safety")
        minimal = shrink("naive", failing)
        self.assertLessEqual(len(minimal.choices), 5)
        self.assertIn("stale, releasing", "\n".join(minimal.trace))

    def test_waiting_attempts_must_check_staleness(self):
        report = search("fenced-no-wait-check", 100)
        self.assertIn("liveness", report["failures"])

    def test_commit_must_check_connectivity(self):
        report = search("fenced-no-connection-checks", 300)
        self.assertIn("safety", report["failures"])


if __name__ == "__main__":
    unittest.main()

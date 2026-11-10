from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import competitor_social_benchmark_collector


class CompetitorSocialBenchmarkCollectorTests(unittest.TestCase):
    def test_build_competitor_social_benchmark_uses_snapshot_posts(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot_state = root / "state" / "competitor_social_snapshots.json"
            snapshot_state.parent.mkdir(parents=True, exist_ok=True)
            snapshot_state.write_text(
                json.dumps(
                    {
                        "posts": [
                            {
                                "account_handle": "wilderkind.studio",
                                "post_format": "reel",
                                "theme": "music",
                                "hook_family": "engagement_prompt",
                                "hour_bucket": "evening",
                                "engagement_score": 10.5,
                            },
                            {
                                "account_handle": "wilderkind.studio",
                                "post_format": "reel",
                                "theme": "music",
                                "hook_family": "engagement_prompt",
                                "hour_bucket": "evening",
                                "engagement_score": 14.5,
                            },
                            {
                                "account_handle": "duck3dprint.shop",
                                "post_format": "image",
                                "theme": "decor",
                                "hook_family": "statement_showcase",
                                "hour_bucket": "morning",
                                "engagement_score": 6.0,
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            own_rollups = root / "state" / "social_performance_rollups.json"
            own_rollups.write_text(
                json.dumps(
                    {
                        "rollups": {
                            "by_theme": [{"label": "decor"}],
                            "by_content_type": [{"label": "image"}],
                            "by_time_window": [{"label": "morning"}],
                            "by_workflow": [{"label": "meme"}],
                        }
                    }
                ),
                encoding="utf-8",
            )
            state_path = root / "state" / "competitor_social_benchmark.json"
            history_path = root / "state" / "competitor_social_benchmark_history.json"
            operator_json = root / "output" / "operator" / "competitor_social_benchmark.json"
            markdown = root / "output" / "operator" / "competitor_social_benchmark.md"

            with patch.object(competitor_social_benchmark_collector, "SNAPSHOT_STATE_PATH", snapshot_state), patch.object(
                competitor_social_benchmark_collector, "SOCIAL_ROLLUPS_PATH", own_rollups
            ), patch.object(
                competitor_social_benchmark_collector, "BENCHMARK_STATE_PATH", state_path
            ), patch.object(
                competitor_social_benchmark_collector, "BENCHMARK_HISTORY_PATH", history_path
            ), patch.object(
                competitor_social_benchmark_collector, "BENCHMARK_OPERATOR_JSON_PATH", operator_json
            ), patch.object(
                competitor_social_benchmark_collector, "BENCHMARK_OUTPUT_MD_PATH", markdown
            ):
                payload = competitor_social_benchmark_collector.build_competitor_social_benchmark()
                self.assertTrue(state_path.exists())
                self.assertTrue(history_path.exists())
                self.assertTrue(operator_json.exists())
                self.assertTrue(markdown.exists())

        self.assertEqual(payload["summary"]["post_count"], 3)
        self.assertEqual(payload["top_accounts"][0]["account_handle"], "wilderkind.studio")
        self.assertEqual(payload["by_format"][0]["label"], "reel")
        self.assertTrue(payload["current_learnings"])
        self.assertTrue(payload["ideas_to_test"])


if __name__ == "__main__":
    unittest.main()


class SocialPriorsAccountFloorTests(unittest.TestCase):
    """A label must be visible on >=3 DISTINCT accounts before it steers a
    prompt. Median ranking defends against one viral POST; nothing defended
    against one dominant ACCOUNT, and `cta_merchandising` won the live hook
    prior on 12 posts from 2 accounts (92% one 104k-follower account)."""

    @staticmethod
    def _posts(rows: list[tuple[str, str, str, float]]) -> list[dict[str, object]]:
        return [
            {"account_handle": handle, "post_format": fmt, "hook_family": hook,
             "theme": "decor", "hour_bucket": "morning", "engagement_score": score}
            for handle, fmt, hook, score in rows
        ]

    def test_a_label_concentrated_on_one_account_is_not_a_prior(self) -> None:
        # 12 posts, 2 accounts, 11 of them one account -- the live shape.
        rows = [("f3dprinted", "reel", "cta_merchandising", 3000.0)] * 11
        rows += [("duck3dprint.shop", "reel", "cta_merchandising", 2500.0)]
        # A broad but lower-scoring family, present on 4 accounts.
        rows += [(f"acct{i}", "image", "statement_showcase", 26.0) for i in range(4)]
        posts = self._posts(rows)
        by_hook = competitor_social_benchmark_collector._top_dimensions(posts, "hook_family")
        priors = competitor_social_benchmark_collector._social_priors(
            posts, [], by_hook, [])
        self.assertIsNotNone(priors)
        self.assertNotEqual(priors["winning_hook_family"], "cta_merchandising")
        self.assertEqual(priors["winning_hook_family"], "statement_showcase")

    def test_concentration_is_measured_not_inferred_from_post_count(self) -> None:
        posts = self._posts(
            [("f3dprinted", "reel", "cta_merchandising", 3000.0)] * 11
            + [("duck3dprint.shop", "reel", "cta_merchandising", 2500.0)]
        )
        row = competitor_social_benchmark_collector._top_dimensions(posts, "hook_family")[0]
        self.assertEqual(row["post_count"], 12)
        self.assertEqual(row["distinct_account_count"], 2)
        self.assertAlmostEqual(row["top_account_share"], 0.917, places=2)

    def test_no_family_clearing_the_floor_yields_no_hook_prior(self) -> None:
        # Every family sits on 2 accounts. The honest answer is no prior at
        # all, NOT the least-concentrated of a bad set.
        posts = self._posts(
            [("a", "reel", "cta_merchandising", 50.0)] * 3
            + [("b", "reel", "cta_merchandising", 40.0)] * 3
            + [("c", "image", "launch_reveal", 30.0)] * 3
            + [("d", "image", "launch_reveal", 20.0)] * 3
        )
        by_hook = competitor_social_benchmark_collector._top_dimensions(posts, "hook_family")
        priors = competitor_social_benchmark_collector._social_priors(posts, [], by_hook, [])
        self.assertIsNone(priors["winning_hook_family"])

    def test_a_broad_format_still_clears_the_floor(self) -> None:
        # The format prior must SURVIVE the floor -- reel is visible on 7
        # accounts live, and suppressing it too would discard the one
        # finding that replicated in our own sample.
        posts = self._posts(
            [(f"acct{i}", "reel", "statement_showcase", 44.0) for i in range(7)]
            + [(f"acct{i}", "carousel", "statement_showcase", 9.0) for i in range(4)]
        )
        by_format = competitor_social_benchmark_collector._top_dimensions(posts, "post_format")
        priors = competitor_social_benchmark_collector._social_priors(posts, by_format, [], [])
        self.assertEqual(priors["winning_format"], "reel")

    def test_sample_hooks_only_ever_illustrate_the_winning_family(self) -> None:
        posts = self._posts(
            [(f"acct{i}", "reel", "statement_showcase", 26.0) for i in range(10)])
        by_hook = competitor_social_benchmark_collector._top_dimensions(posts, "hook_family")
        viral = [
            {"visible_hook": "ONION SLICER link in bio", "hook_family": "cta_merchandising"},
            {"visible_hook": "a real showcase hook", "hook_family": "statement_showcase"},
        ]
        priors = competitor_social_benchmark_collector._social_priors(
            posts, [], by_hook, viral)
        self.assertEqual(priors["sample_hooks"], ["a real showcase hook"])

    def test_a_suppressed_family_carries_no_examples(self) -> None:
        posts = self._posts([("solo", "reel", "cta_merchandising", 3000.0)] * 12)
        by_hook = competitor_social_benchmark_collector._top_dimensions(posts, "hook_family")
        viral = [{"visible_hook": "ONION SLICER link in bio",
                  "hook_family": "cta_merchandising"}]
        priors = competitor_social_benchmark_collector._social_priors(
            posts, [], by_hook, viral)
        self.assertIsNone(priors["winning_hook_family"])
        self.assertEqual(priors["sample_hooks"], [])

    def test_a_thin_sample_still_yields_no_priors_at_all(self) -> None:
        posts = self._posts([("a", "reel", "statement_showcase", 10.0)] * 4)
        self.assertIsNone(
            competitor_social_benchmark_collector._social_priors(posts, [], [], []))


class SocialPriorsMarginGateTests(unittest.TestCase):
    """A prior worth steering with has to be able to lose. Live 2026-11-08,
    once the account floor removed cta_merchandising, statement_showcase led
    engagement_prompt 26.46 to 25.02 on 36 vs 30 posts -- 5.8%, noise -- and
    it would have told every caption prompt that statements beat asks, the
    opposite of our own measurement."""

    @staticmethod
    def _rows(pairs: list[tuple[str, float, int]]) -> list[dict[str, object]]:
        return [{"label": label, "post_count": 12, "distinct_account_count": accounts,
                 "median_engagement_score": median} for label, median, accounts in pairs]

    def _priors(self, rows):
        posts = [{"engagement_score": 1.0}] * 20  # clears _PRIORS_MIN_POSTS
        return competitor_social_benchmark_collector._social_priors(posts, [], rows, [])

    def test_a_photo_finish_is_not_a_prior(self) -> None:
        rows = self._rows([("statement_showcase", 26.46, 7), ("engagement_prompt", 25.02, 6)])
        self.assertIsNone(self._priors(rows)["winning_hook_family"])

    def test_a_clear_lead_is_a_prior(self) -> None:
        rows = self._rows([("statement_showcase", 40.0, 7), ("engagement_prompt", 25.0, 6)])
        self.assertEqual(self._priors(rows)["winning_hook_family"], "statement_showcase")

    def test_an_unopposed_label_needs_no_margin(self) -> None:
        # Only one label survives the account floor -- there is no runner-up
        # to be indistinguishable from, so the margin gate must not fire.
        rows = self._rows([("statement_showcase", 26.46, 7), ("cta_merchandising", 3003.0, 2)])
        self.assertEqual(self._priors(rows)["winning_hook_family"], "statement_showcase")

    def test_an_all_zero_board_is_not_a_prior(self) -> None:
        rows = self._rows([("statement_showcase", 0.0, 7), ("engagement_prompt", 0.0, 6)])
        self.assertIsNone(self._priors(rows)["winning_hook_family"])

    def test_the_margin_is_relative_not_absolute(self) -> None:
        # +1.0 is decisive at median 2 and noise at median 100. A fixed
        # absolute threshold would get one of those two wrong.
        self.assertEqual(
            self._priors(self._rows([("a", 3.0, 7), ("b", 2.0, 6)]))["winning_hook_family"], "a")
        self.assertIsNone(
            self._priors(self._rows([("a", 101.0, 7), ("b", 100.0, 6)]))["winning_hook_family"])

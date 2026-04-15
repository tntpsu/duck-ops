from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
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

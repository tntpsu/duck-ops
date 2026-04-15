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

import competitor_benchmark_collector


class CompetitorBenchmarkCollectorTests(unittest.TestCase):
    def test_build_competitor_benchmark_uses_recent_reports(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            competitor_state = root / "runs" / "2026-04-15" / "state_competitor.json"
            competitor_state.parent.mkdir(parents=True, exist_ok=True)
            competitor_state.write_text(
                json.dumps(
                    {
                        "competitor_report": {
                            "report_date": "2026-04-15",
                            "total_competitor_shops": 3,
                            "total_competitor_listings": 120,
                            "new_competitor_listings": 12,
                            "shop_snapshots": [
                                {
                                    "shop_id": "1",
                                    "shop_name": "Duckorama",
                                    "momentum_score": 50.0,
                                    "growth_rate": 3.5,
                                    "listing_active_count": 40,
                                    "transaction_sold_count": 1200,
                                }
                            ],
                            "listing_snapshots": [
                                {
                                    "listing_id": "a",
                                    "title": "Cowgirl Duck Artist Duck",
                                    "created_ts": "2026-04-14T11:00:00",
                                    "views": 800,
                                    "num_favorers": 110,
                                    "tags": ["cowgirl duck", "artist duck"],
                                }
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )
            social_rollups = root / "state" / "social_performance_rollups.json"
            social_rollups.parent.mkdir(parents=True, exist_ok=True)
            social_rollups.write_text(
                json.dumps({"rollups": {"by_theme": [{"label": "meme"}]}, "top_posts": []}),
                encoding="utf-8",
            )

            state_path = root / "state" / "social_competitor_benchmark.json"
            history_path = root / "state" / "social_competitor_benchmark_history.json"
            operator_json = root / "output" / "operator" / "competitor_benchmark.json"
            markdown = root / "output" / "operator" / "competitor_benchmark.md"

            with patch.object(competitor_benchmark_collector, "COMPETITOR_STATE_PATH", state_path), patch.object(
                competitor_benchmark_collector, "COMPETITOR_HISTORY_PATH", history_path
            ), patch.object(
                competitor_benchmark_collector, "COMPETITOR_OPERATOR_JSON_PATH", operator_json
            ), patch.object(
                competitor_benchmark_collector, "COMPETITOR_OUTPUT_MD_PATH", markdown
            ), patch.object(
                competitor_benchmark_collector, "SOCIAL_ROLLUPS_PATH", social_rollups
            ), patch.object(
                competitor_benchmark_collector, "_competitor_state_paths", return_value=[competitor_state]
            ):
                payload = competitor_benchmark_collector.build_competitor_benchmark(window_days=30)

            self.assertEqual(payload["summary"]["observation_days"], 1)
            self.assertEqual(payload["top_momentum_shops"][0]["shop_name"], "Duckorama")
            self.assertTrue(payload["emergent_motifs"])
            self.assertTrue(payload["ideas_to_test"])
            self.assertTrue(state_path.exists())
            self.assertTrue(history_path.exists())
            self.assertTrue(operator_json.exists())
            self.assertTrue(markdown.exists())


if __name__ == "__main__":
    unittest.main()

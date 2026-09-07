"""Surface 66.6 — the trend digest must not print the same items twice and
must not show the saturated single-source confidence as if it were signal."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import trend_ranker  # noqa: E402
import notifier  # noqa: E402


def _item(artifact_id: str, theme: str, *, confidence: float = 0.58) -> dict:
    return {
        "artifact_id": artifact_id,
        "theme": theme,
        "title": theme.title(),
        "decision": "worth_acting_on",
        "action_frame": "build",
        "review_status": "pending",
        "score": 80,
        "confidence": confidence,
        "reasoning": ["Commercial signal 30/30 from sold 7d `14`."],
        "trend_metadata": {"catalog_status": "gap", "distinct_days": 62, "matching_products": []},
    }


class TrendDigestTests(unittest.TestCase):
    def test_pending_section_excludes_new_items(self) -> None:
        new = [_item("a", "nurse duck"), _item("b", "bass duck")]
        pending = [_item("a", "nurse duck"), _item("b", "bass duck"), _item("c", "goat duck")]
        carried = trend_ranker.carried_over_items(new, pending)
        self.assertEqual([i["artifact_id"] for i in carried], ["c"])

    def test_digest_hides_saturated_confidence(self) -> None:
        line = trend_ranker.trend_digest_item_line(_item("a", "nurse duck"))
        self.assertNotIn("confidence", line)
        self.assertIn("observed `62` day(s)", line)
        self.assertIn("score `80`", line)

    def test_html_card_and_stats_count_the_same_population(self) -> None:
        payload = {
            "generated_at": "2026-09-06T12:00:00-04:00",
            "pending_review_count": 2,
            "background_watch_count": 0,
            "active_counts": {"worth_acting_on": 17, "ignore": 96},
            "items": [_item("a", "nurse duck"), _item("b", "bass duck")],
            "background_watch_items": [],
        }
        html = notifier.render_notifier_html("trend_digest", "[OpenClaw Trends] 2026-09-06", "body", payload)
        self.assertIn("Surfaced for review (2)", html)
        self.assertIn("Worth acting on (all)", html)
        self.assertNotIn("confidence", html)
        self.assertIn("observed 62 day(s)", html)


if __name__ == "__main__":
    unittest.main()

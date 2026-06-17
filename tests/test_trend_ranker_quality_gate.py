from __future__ import annotations

import sys
import unittest
from datetime import date, timedelta
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import trend_ranker


def _aggregate(theme: str, *, sold_7d: int = 5, sold_30d: int = 10) -> dict:
    # Dates are anchored to "today" rather than hard-coded so the gate's live
    # staleness check (>21 days → needs_reframe) never trips as wall-clock time
    # advances past the fixture. Keeps 5 distinct observed days over a 5-day span.
    _today = date.today()
    _days = [_today - timedelta(days=offset) for offset in (5, 4, 3, 2, 0)]
    run_ids = [d.isoformat() for d in _days]
    latest = run_ids[-1]
    first = run_ids[0]
    return {
        "artifact_id": f"trend::{theme.replace(' ', '-')}::{latest}",
        "artifact_type": "trend",
        "theme": theme,
        "first_seen_at": f"{first}T00:00:00-04:00",
        "latest_observed_at": f"{latest}T00:00:00-04:00",
        "source_refs": [
            {
                "path": "/tmp/state_competitor.json",
                "source_type": "state_competitor",
                "run_id": run_id,
                "listing_id": "listing-1",
            }
            for run_id in run_ids
        ],
        "source_types": ["state_competitor"],
        "competitor_run_ids": run_ids,
        "observed_dates": run_ids,
        "signal_summaries": [
            {
                "sold_last_7d": sold_7d,
                "sold_last_30d": sold_30d,
                "quantity": 40,
                "previous_quantity": 45,
                "trending_score": 1200,
            }
        ],
        "catalog_match": {"status": "gap", "matching_products": [], "publication_coverage": []},
        "input_confidence_cap": 0.75,
    }


class TrendRankerQualityGateTests(unittest.TestCase):
    def test_public_safe_gap_build_gets_concept_brief(self) -> None:
        decision = trend_ranker.evaluate_trend(_aggregate("greyhound duck"))

        self.assertEqual(decision["action_frame"], "build")
        self.assertEqual(decision["title"], "Greyhound Duck")
        self.assertEqual(decision["trend_quality_gate"]["status"], "ready")
        self.assertEqual(decision["concept_design_brief"]["concept_title"], "Greyhound Duck")
        self.assertIn("greyhound dog-breed", decision["concept_design_brief"]["semantic_identity"])

    def test_gendered_role_build_is_cleaned_before_generation(self) -> None:
        decision = trend_ranker.evaluate_trend(_aggregate("female nurse duck"))

        self.assertEqual(decision["action_frame"], "build")
        self.assertEqual(decision["title"], "Nurse Duck")
        self.assertEqual(decision["raw_title"], "Female Nurse Duck")
        self.assertEqual(decision["trend_quality_gate"]["status"], "needs_reframe")
        self.assertEqual(decision["concept_design_brief"]["concept_title"], "Nurse Duck")

    def test_ip_sensitive_theme_cannot_become_build_candidate(self) -> None:
        decision = trend_ranker.evaluate_trend(_aggregate("tennessee vols duck"))

        self.assertEqual(decision["decision"], "watch")
        self.assertEqual(decision["action_frame"], "wait")
        self.assertEqual(decision["trend_quality_gate"]["status"], "blocked_by_policy")
        self.assertFalse(decision["trend_quality_gate"]["generation_ready"])
        self.assertTrue(any("public-safe" in item for item in decision["improvement_suggestions"]))


if __name__ == "__main__":
    unittest.main()

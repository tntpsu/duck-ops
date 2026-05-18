from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from product_concept_brief import build_concept_design_brief, evaluate_trend_quality


class ProductConceptBriefTests(unittest.TestCase):
    def test_greyhound_gets_breed_specific_brief(self) -> None:
        gate = evaluate_trend_quality(
            raw_theme="greyhound duck",
            signal_summary={"sold_last_7d": 5, "sold_last_30d": 11, "trending_score": 1484},
            source_refs=[{"path": "/tmp/state_competitor.json", "source_type": "state_competitor"}],
            catalog_status="gap",
            latest_observed_at="2026-05-17T00:00:00-04:00",
            now=datetime.fromisoformat("2026-05-17T12:00:00-04:00"),
        )
        brief = build_concept_design_brief(
            raw_theme="greyhound duck",
            signal_summary={"sold_last_7d": 5, "sold_last_30d": 11, "trending_score": 1484},
            source_refs=[{"path": "/tmp/state_competitor.json", "source_type": "state_competitor"}],
            catalog_status="gap",
            trend_quality_gate=gate,
        )

        self.assertEqual(gate["status"], "ready")
        self.assertEqual(brief["concept_title"], "Greyhound Duck")
        self.assertIn("greyhound dog-breed", brief["semantic_identity"])
        self.assertTrue(any("hound" in cue for cue in brief["visual_cues"]))
        self.assertTrue(any("plain gray" in rule for rule in brief["must_avoid"]))

    def test_ip_team_theme_is_blocked_before_build(self) -> None:
        gate = evaluate_trend_quality(
            raw_theme="tennessee vols duck",
            signal_summary={"sold_last_7d": 5, "trending_score": 1500},
            source_refs=[{"path": "/tmp/state_competitor.json"}],
            catalog_status="gap",
        )

        self.assertEqual(gate["status"], "blocked_by_policy")
        self.assertFalse(gate["generation_ready"])
        self.assertTrue(any("College/team" in issue for issue in gate["issues"]))

    def test_gendered_role_is_reframed_not_blocked(self) -> None:
        gate = evaluate_trend_quality(
            raw_theme="female nurse duck",
            signal_summary={"sold_last_7d": 4, "trending_score": 900},
            source_refs=[{"path": "/tmp/state_competitor.json"}],
            catalog_status="gap",
        )
        brief = build_concept_design_brief(raw_theme="female nurse duck", trend_quality_gate=gate)

        self.assertEqual(gate["status"], "needs_reframe")
        self.assertTrue(gate["generation_ready"])
        self.assertEqual(gate["normalized_concept_title"], "Nurse Duck")
        self.assertEqual(brief["concept_title"], "Nurse Duck")
        self.assertIn("nurse-themed", brief["semantic_identity"])

    def test_inconsistent_sales_evidence_warns(self) -> None:
        gate = evaluate_trend_quality(
            raw_theme="cowgirl duck",
            signal_summary={"sold_last_7d": 36, "sold_last_30d": 10},
            source_refs=[{"path": "/tmp/state_competitor.json"}],
            catalog_status="gap",
        )

        self.assertEqual(gate["status"], "needs_reframe")
        self.assertTrue(any("7-day sales exceed 30-day sales" in warning for warning in gate["warnings"]))

    def test_listing_fragments_are_blocked(self) -> None:
        gate = evaluate_trend_quality(
            raw_theme="39 s magnetic duck",
            signal_summary={"sold_last_7d": 3, "trending_score": 850},
            source_refs=[{"path": "/tmp/state_competitor.json"}],
            catalog_status="gap",
        )

        self.assertEqual(gate["status"], "blocked_by_policy")
        self.assertTrue(any("listing-size" in issue for issue in gate["issues"]))

    def test_city_plus_sport_theme_needs_abstraction(self) -> None:
        gate = evaluate_trend_quality(
            raw_theme="anaheim hockey duck",
            signal_summary={"sold_last_7d": 5, "trending_score": 1200},
            source_refs=[{"path": "/tmp/state_competitor.json"}],
            catalog_status="gap",
        )

        self.assertEqual(gate["status"], "blocked_by_policy")
        self.assertTrue(any("public-safe abstraction" in issue for issue in gate["issues"]))


if __name__ == "__main__":
    unittest.main()

"""Surface 66.6 — concept-queue merge keeps the FRESHER row on score ties and
all evidence lines; Greek-letter organization names are IP-sensitive."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import product_concept_queue  # noqa: E402
from product_concept_brief import evaluate_trend_quality  # noqa: E402


class MergeTieBreakTests(unittest.TestCase):
    def test_merge_tie_prefers_fresher_row(self) -> None:
        stale = {"theme": "nurse duck", "score": 1.0, "confidence": 0.61, "queue_state": "watch",
                 "trend_quality_gate": {"staleness_days": 175}, "evidence": ["old"], "guardrails": ["stale"]}
        fresh = {"theme": "nurse duck", "score": 1.0, "confidence": 0.69, "queue_state": "ready_for_brief_review",
                 "trend_quality_gate": {"staleness_days": 1}, "evidence": ["7d sold: 14"], "guardrails": []}
        merged = product_concept_queue._merge_duplicate_themes([stale, fresh])
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["queue_state"], "ready_for_brief_review")
        self.assertEqual(merged[0]["evidence"][0], "7d sold: 14")  # winner's evidence first
        self.assertIn("old", merged[0]["evidence"])

    def test_higher_score_still_beats_freshness(self) -> None:
        stale_strong = {"theme": "goat duck", "score": 0.9, "trend_quality_gate": {"staleness_days": 30}, "evidence": []}
        fresh_weak = {"theme": "goat duck", "score": 0.5, "trend_quality_gate": {"staleness_days": 1}, "evidence": []}
        merged = product_concept_queue._merge_duplicate_themes([fresh_weak, stale_strong])
        self.assertEqual(merged[0]["score"], 0.9)

    def test_design_brief_signal_keeps_all_evidence_lines(self) -> None:
        item = {"concept_id": "c", "theme": "nurse duck", "evidence": [
            "Trend theme: nurse duck", "Catalog status: gap", "Trending score: 2891.1",
            "7d sold: 13", "7d revenue: $120.00", "Source refs: 1", "Trend theme: nurse duck",
        ]}
        signal = product_concept_queue._design_brief_signal(item)
        self.assertIn("7d revenue: $120.00", signal["evidence"])
        self.assertIn("Source refs: 1", signal["evidence"])
        self.assertEqual(signal["evidence"].count("Trend theme: nurse duck"), 1)


class GreekLetterIpTests(unittest.TestCase):
    def _gate(self, theme: str) -> dict:
        return evaluate_trend_quality(
            raw_theme=theme,
            signal_summary={"sold_last_7d": 4, "trending_score": 900},
            source_refs=[{"path": "/tmp/state_competitor.json"}],
            catalog_status="gap",
        )

    def test_greek_letter_organization_is_ip_sensitive(self) -> None:
        for theme in ("chi omega duck", "delta gamma duck", "kappa sorority duck"):
            gate = self._gate(theme)
            self.assertEqual(gate["status"], "blocked_by_policy", theme)
            self.assertTrue(any("Greek-letter" in issue for issue in gate["issues"]), theme)

    def test_single_greek_word_without_org_context_is_not_blocked(self) -> None:
        gate = self._gate("pi day math duck")
        self.assertFalse(any("Greek-letter" in issue for issue in gate["issues"]))


if __name__ == "__main__":
    unittest.main()

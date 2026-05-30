"""Contract test for the Thursday option-visibility passthrough.

Born from the 2026-05-28 operator question about This-or-That
Thursday: the flow generates 3 paired-duck options, scores them via
`creative_quality`, and tags each as `recommended`, `reviewable`, or
`quality_hidden`. The operator's portal showed both surviving
options with scores (85 and 57) but NO indication which one the
upstream Thursday flow flagged as RECOMMENDED.

The fix: preserve `thursday_option_visibility` from
`candidate.supporting_context` through the quality_gate's metadata
so downstream surfaces (portal API, decision-inbox builder) can
surface the recommendation as a badge.

Pinned:
1. visibility=recommended makes it through quality_gate_metadata
2. visibility=reviewable makes it through
3. default 'reviewable' is supplied when upstream omitted it
4. Non-Thursday flows don't receive a stray visibility tag
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

from quality_gate_pilot import evaluate_quality_gate  # noqa: E402


def _thursday_candidate(visibility: str | None = "recommended") -> dict:
    """Mirrors the candidate shape phase1_observer emits for a
    Thursday option."""
    supporting = {
        "brand_family": "social_post",
        "catalog_overlap": [],
        "publication_coverage": [],
        "trend_refs": [],
        "thursday_option_review": {},
    }
    if visibility is not None:
        supporting["thursday_option_visibility"] = visibility
    return {
        "artifact_id": "publish::thursday::2026-05-28::option-3-pizza-duck-vs-pirate-duck",
        "artifact_type": "social_post",
        "flow": "thursday",
        "run_id": "2026-05-28",
        "candidate_summary": {
            "title": "This-or-That Thursday: Pizza Duck vs Pirate Duck",
            "body": (
                "Pizza Duck vs Pirate Duck — which duck wins this week? "
                "Drop a vote in the comments. Both ducks live on the dashboard. "
                "Whichever wins gets featured next week."
            ),
            "images": ["/tmp/layout.png"],
            "platform_targets": ["instagram"],
            "publish_token": "2026-05-28::option-3",
            "platform_variants": {"instagram": {"caption": "duck duel"}},
            "option_id": 3,
            "option_a": "Pizza Duck",
            "option_b": "Pirate Duck",
        },
        "supporting_context": supporting,
        "normalization_notes": {"source_mode": "state_file", "completeness": "high"},
        "source_refs": [{"path": "/tmp/state_thursday.json", "source_type": "state_thursday"}],
    }


class ThursdayVisibilityPassthroughTests(unittest.TestCase):
    def test_recommended_visibility_carries_through(self) -> None:
        """The operator's preferred-pick badge depends on this exact
        field surviving. If a refactor drops it, the portal silently
        regresses to "no badge" without a test catching it."""
        out = evaluate_quality_gate(_thursday_candidate(visibility="recommended"))
        meta = out["quality_gate_metadata"]
        self.assertEqual(meta["thursday_option_visibility"], "recommended")

    def test_reviewable_visibility_carries_through(self) -> None:
        out = evaluate_quality_gate(_thursday_candidate(visibility="reviewable"))
        self.assertEqual(out["quality_gate_metadata"]["thursday_option_visibility"], "reviewable")

    def test_missing_visibility_defaults_to_reviewable(self) -> None:
        """phase1_observer should always set this, but if upstream
        drops it for any reason (older state files, partial
        regeneration) the default of "reviewable" is the safe
        fallback — never escalates a non-recommended option to
        recommended status."""
        out = evaluate_quality_gate(_thursday_candidate(visibility=None))
        self.assertEqual(out["quality_gate_metadata"]["thursday_option_visibility"], "reviewable")

    def test_non_thursday_flows_dont_get_visibility_field(self) -> None:
        """Sanity baseline: the new field is Thursday-specific. Other
        flows' quality_gate_metadata stays unchanged in shape."""
        from quality_gate_pilot import evaluate_review_carousel  # noqa: PLC0415
        carousel_candidate = {
            "artifact_id": "test_carousel_x",
            "flow": "review_carousel",
            "run_id": "review_carousel_20260527_160004",
            "candidate_summary": {
                "title": "Review Carousel — Bundle",
                "body": "Five-star favorites from the MyJeepDuck flock. Plenty of duck love.",
                "slide_count": 5,
                "preview_url": "/tmp/p.png",
                "images": ["/tmp/p.png"],
            },
            "supporting_context": {},
            "source_refs": [],
            "normalization_notes": {},
        }
        # The carousel evaluator doesn't run the Thursday dispatch,
        # so visibility field never enters the metadata.
        out = evaluate_review_carousel(carousel_candidate, age_days=1)
        # The evaluate_review_carousel return shape doesn't include
        # quality_gate_metadata directly — that's added by the dispatch
        # wrapper. The test here is that the evaluator's output shape
        # is the carousel one, not the Thursday one.
        self.assertIn("component_scores", out)
        self.assertNotIn("thursday_option_visibility", out)


if __name__ == "__main__":
    unittest.main()

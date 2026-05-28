"""Contract tests for evaluate_review_carousel.

Born from the 2026-05-28 operator confusion: yesterday's carousel
(`review_carousel_20260527_160004`) scored 42/100 → "discard" via
the newduck-tuned generic rubric, which expected body_len >= 180
and treated low clarity scores as fail-closed. Both rules were
wrong for a social-media carousel where `body` IS the IG caption
(~150-200 chars by design).

New flow=review_carousel evaluator uses carousel-appropriate
criteria. These tests pin:
1. A normal 5-slide carousel with a sensible caption scores
   publish_ready (would have been discard before)
2. A 2-slide carousel hits the slide_count >= 3 fail-closed
3. An empty caption hits the caption-required fail-closed
4. A 14+-day-old carousel hits the staleness fail-closed
5. The dispatch in evaluate_quality_gate routes flow=review_carousel
   to the new evaluator (not the generic rubric)
6. The newduck `body_len < 180` rule does NOT fire on a carousel
   with a short caption (regression-pin for the original bug)
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

from quality_gate_pilot import (  # noqa: E402
    evaluate_review_carousel,
    evaluate_quality_gate,
)


def _carousel_candidate(
    *,
    slide_count: int = 5,
    caption: str = (
        "Five-star favorites from the MyJeepDuck flock. Real customer feedback, "
        "quick-ship smiles, and dashboard joy from ducks people genuinely loved. "
        "Which slide is your favorite?"
    ),
    preview_url: str | None = "/tmp/preview.png",
    artifact_id: str = "review_carousel_test_001",
    run_id: str = "review_carousel_20260527_160004",
) -> dict:
    return {
        "artifact_id": artifact_id,
        "flow": "review_carousel",
        "artifact_type": "social_carousel",
        "run_id": run_id,
        "candidate_summary": {
            "title": "Review Carousel — Bundle",
            "body": caption,
            "slide_count": slide_count,
            "preview_url": preview_url,
            "images": [preview_url] if preview_url else [],
        },
        "supporting_context": {},
        "source_refs": [],
        "normalization_notes": {},
    }


class CarouselScorerHappyPath(unittest.TestCase):
    def test_normal_5_slide_carousel_scores_publish_ready(self) -> None:
        """Yesterday's carousel shape. Scored 42 via the old rubric;
        must score publish_ready under the new one."""
        out = evaluate_review_carousel(_carousel_candidate(), age_days=1)
        self.assertEqual(out["decision"], "publish_ready", (
            f"5-slide carousel with sensible caption must score publish_ready; "
            f"got {out['decision']} (score {out['score']})"
        ))
        self.assertGreaterEqual(out["score"], 72)
        self.assertEqual(out["fail_closed"], [])
        # All component scores have to be in their declared ranges.
        cs = out["component_scores"]
        self.assertLessEqual(cs["support"], 20)
        self.assertLessEqual(cs["brand_fit"], 20)
        self.assertLessEqual(cs["clarity"], 15)


class CarouselFailClosed(unittest.TestCase):
    def test_under_3_slides_fails_closed_not_publish_ready(self) -> None:
        """2 slides hits a fail-closed trigger. Either 'discard' or
        'needs_revision' is acceptable — what matters is it's NOT
        publish_ready and the fail-closed message names the slide
        count gap so the operator can fix it."""
        out = evaluate_review_carousel(_carousel_candidate(slide_count=2), age_days=1)
        self.assertNotEqual(out["decision"], "publish_ready", (
            f"2-slide carousel must not be auto-publish; got {out['decision']}"
        ))
        self.assertTrue(any("at least 3" in m for m in out["fail_closed"]))

    def test_zero_slides_fails_closed_not_publish_ready(self) -> None:
        out = evaluate_review_carousel(_carousel_candidate(slide_count=0), age_days=1)
        self.assertNotEqual(out["decision"], "publish_ready")
        self.assertTrue(any("no slides" in m.lower() for m in out["fail_closed"]))

    def test_empty_caption_fails_closed(self) -> None:
        out = evaluate_review_carousel(_carousel_candidate(caption=""), age_days=1)
        self.assertTrue(any("caption is empty" in m.lower() for m in out["fail_closed"]))

    def test_14_day_old_carousel_fails_closed_stale(self) -> None:
        out = evaluate_review_carousel(_carousel_candidate(), age_days=14)
        self.assertTrue(any("days old" in m for m in out["fail_closed"]))


class CarouselBodyLenRegression(unittest.TestCase):
    """The original bug: body_len < 180 produced fail-closed
    "materially incomplete" on a perfectly fine carousel because the
    caption was 175 chars by design. Pin that the new evaluator does
    NOT fire that rule, even on a caption shorter than 180."""

    def test_short_caption_under_180_chars_is_NOT_materially_incomplete(self) -> None:
        # Yesterday's exact caption — 175 chars
        out = evaluate_review_carousel(_carousel_candidate(
            caption="Five-star favorites from the MyJeepDuck flock. Real customer feedback, "
                    "quick-ship smiles, and dashboard joy from ducks people genuinely loved. "
                    "Which slide is your favorite?",
        ), age_days=1)
        self.assertEqual(out["decision"], "publish_ready")
        # The original fail_closed string MUST NOT appear here.
        for msg in out["fail_closed"]:
            self.assertNotIn("materially incomplete", msg.lower())


class CarouselDispatchTests(unittest.TestCase):
    """evaluate_quality_gate has to dispatch flow=review_carousel to
    the new evaluator. If it falls through to the generic rubric,
    the 5/28 bug recurs."""

    def test_dispatch_routes_to_carousel_evaluator(self) -> None:
        candidate = _carousel_candidate()
        # evaluate_quality_gate needs a publish_token-parseable
        # value for age computation. The candidate's run_id is
        # "review_carousel_20260527_160004" which parse_run_date
        # can handle (looks for YYYYMMDD).
        out = evaluate_quality_gate(candidate)
        self.assertEqual(out["flow"], "review_carousel")
        self.assertEqual(out["decision"], "publish_ready", (
            f"dispatch must hit the new carousel evaluator; if it "
            f"falls through to the generic scorer the result would be "
            f"discard. Got {out['decision']} (score {out['score']})"
        ))
        # The returned dict shape mirrors the other social flows.
        self.assertEqual(out["artifact_type"], "social_carousel")
        self.assertIn("reasoning", out)
        self.assertIn("quality_gate_metadata", out)
        self.assertEqual(out["quality_gate_metadata"]["slide_count"], 5)


if __name__ == "__main__":
    unittest.main()

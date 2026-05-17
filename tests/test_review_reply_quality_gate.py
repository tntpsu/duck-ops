from __future__ import annotations

import sys
import unittest
import json
import tempfile
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import quality_gate_pilot
import phase1_observer


class ReviewReplyQualityGateTests(unittest.TestCase):
    def test_warm_specific_public_reply_can_publish_without_literal_thanks(self) -> None:
        outcome = quality_gate_pilot.evaluate_review_reply(
            {
                "source_refs": ["daily-summary"],
                "candidate_summary": {
                    "customer_review": "Definitely a high quality item! I absolutely love it!",
                    "body": "It's wonderful to hear that you absolutely love it! I'm glad the quality came through clearly.",
                },
            },
            age_days=0,
            private_mode=False,
        )

        self.assertEqual(outcome["decision"], "publish_ready")
        self.assertGreaterEqual(outcome["score"], 78)

    def test_public_reply_with_expectation_mismatch_needs_rewrite(self) -> None:
        outcome = quality_gate_pilot.evaluate_review_reply(
            {
                "source_refs": ["daily-summary"],
                "candidate_summary": {
                    "customer_review": "I thought they were plastic which would have been better.",
                    "body": "Thanks so much for the kind review! Thanks again for the kind review.",
                },
            },
            age_days=0,
            private_mode=False,
        )

        self.assertEqual(outcome["decision"], "needs_revision")
        self.assertEqual(outcome["reply_contract"]["classification"]["issue_type"], "material_expectation")
        self.assertTrue(outcome["reply_contract"]["classification"]["needs_rewrite"])
        self.assertTrue(
            any("generic positive phrase" in message for message in outcome["fail_closed"]),
            outcome["fail_closed"],
        )

    def test_review_story_requires_renderable_story_asset(self) -> None:
        decision = quality_gate_pilot.evaluate_quality_gate(
            {
                "artifact_id": "publish::reviews_story::2026-05-07::review-story",
                "artifact_type": "social_post",
                "flow": "reviews_story",
                "run_id": "2026-05-07",
                "source_refs": [{"path": "mailbox://inbox/1"}],
                "normalization_notes": {"source_mode": "review_summary_email"},
                "supporting_context": {
                    "review_stats": {"five_star_reviews": 5, "low_rating_reviews": 0},
                },
                "candidate_summary": {
                    "title": "Etsy Review Story 2026-05-07",
                    "selected_review": "This is super cute and great quality.",
                    "story_ai_score": 9,
                    "images": ["🎯"],
                },
            }
        )

        self.assertEqual(decision["decision"], "needs_revision")
        self.assertIsNone(decision["preview"]["asset_url"])
        self.assertIn("Story candidate does not include a final story image.", decision["quality_gate_metadata"]["fail_closed"])

    def test_review_story_accepts_local_internal_renderer_asset(self) -> None:
        decision = quality_gate_pilot.evaluate_quality_gate(
            {
                "artifact_id": "publish::reviews_story::2026-05-07::review-story",
                "artifact_type": "social_post",
                "flow": "reviews_story",
                "run_id": "2026-05-07",
                "source_refs": [{"path": "mailbox://inbox/1"}],
                "normalization_notes": {"source_mode": "review_summary_email"},
                "supporting_context": {
                    "review_stats": {"five_star_reviews": 5, "low_rating_reviews": 0},
                },
                "candidate_summary": {
                    "title": "Etsy Review Story 2026-05-07",
                    "selected_review": "This is super cute and great quality.",
                    "story_ai_score": 9,
                    "template_id": "review_story_card",
                    "images": ["/tmp/review_story_preview.png"],
                },
            }
        )

        self.assertEqual(decision["decision"], "publish_ready")
        self.assertEqual(decision["preview"]["asset_url"], "/tmp/review_story_preview.png")
        self.assertNotIn("Story candidate does not include a final story image.", decision["quality_gate_metadata"]["fail_closed"])

    def test_review_story_candidate_uses_local_state_asset_when_email_has_placeholder(self) -> None:
        original_runs_dir = phase1_observer.DUCKAGENT_RUNS_DIR
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_root = Path(tmp_dir)
            runs_dir = tmp_root / "runs"
            run_dir = runs_dir / "2026-05-07"
            run_dir.mkdir(parents=True, exist_ok=True)
            preview = tmp_root / "review-story.png"
            preview.write_bytes(b"fake image")
            (run_dir / "state_reviews.json").write_text(
                json.dumps(
                    {
                        "story_image": {
                            "image_url": "",
                            "local_path": str(preview),
                            "status": "success",
                        }
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            phase1_observer.DUCKAGENT_RUNS_DIR = runs_dir
            try:
                candidate = phase1_observer.build_reviews_story_candidate_from_email(
                    {
                        "registry_key": "mailbox://inbox/1",
                        "folder": "INBOX",
                        "uid": 1,
                        "message_id": "message-1",
                        "subject": "Daily Etsy Review Summary - 2026-05-07 (Story Ready!)",
                        "review_summary_metadata": {"run_id": "2026-05-07", "story_ready": True, "story_status": "Story Ready!"},
                        "body_text": "Selected Review: This is super cute and great quality.\nAI Score: 9/10\nTemplate: review_story_card\nImage URL: 🎯",
                    }
                )
            finally:
                phase1_observer.DUCKAGENT_RUNS_DIR = original_runs_dir

            self.assertIsNotNone(candidate)
            self.assertEqual(candidate["candidate_summary"]["images"], [str(preview)])
            self.assertEqual(candidate["normalization_notes"]["completeness"], "high")


if __name__ == "__main__":
    unittest.main()

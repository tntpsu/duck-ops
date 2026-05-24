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

    def test_publish_ready_reply_has_no_boilerplate_style_notes(self) -> None:
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
        # publish_ready items must not carry the generic flow-level style
        # guidance — see REVIEW_REPLY_INBOX_UX_PLAN.md Slice H.
        for suggestion in outcome["improvement_suggestions"]:
            self.assertNotIn("Keep public replies short", suggestion)
            self.assertNotIn("Avoid overlong responses", suggestion)

    def test_needs_revision_reply_keeps_flow_default_guidance(self) -> None:
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
        # For revision-needed items, defaults still flow as helpful starting guidance.
        joined = " ".join(outcome["improvement_suggestions"])
        self.assertIn("Keep public replies short", joined)

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

    def test_complete_meme_package_is_publish_ready_with_non_blocking_support_warning(self) -> None:
        run_date = quality_gate_pilot.now_iso()[:10]
        decision = quality_gate_pilot.evaluate_quality_gate(
            {
                "artifact_id": f"publish::meme::{run_date}::monster-truck-duck",
                "artifact_type": "social_post",
                "flow": "meme",
                "run_id": run_date,
                "source_refs": [{"path": "/tmp/state_meme.json", "source_type": "state_meme"}],
                "normalization_notes": {"source_mode": "state_file", "completeness": "high", "input_confidence_cap": 0.85},
                "supporting_context": {"trend_refs": []},
                "candidate_summary": {
                    "title": "Meme Monday: Monster Truck Duck",
                    "body": (
                        "EXPECTATION: Epic Off-Road Adventure\n"
                        "REALITY: Dashboard Ducking\n\n"
                        "Meme Monday! Tag your duck-loving friends. #MemeMonday #MyJeepDuck"
                    ),
                    "images": ["https://cdn.example.com/meme.png", "/tmp/meme.png"],
                    "platform_targets": ["instagram", "facebook"],
                    "platform_variants": {
                        "instagram": {"caption": "Meme Monday!"},
                        "facebook": {"caption": "Meme Monday!"},
                    },
                },
            }
        )

        contract = decision["quality_gate_metadata"]["flow_review_contract"]
        self.assertEqual(decision["decision"], "publish_ready")
        self.assertGreaterEqual(decision["score"], 82)
        self.assertEqual(contract["reviewer"], "meme_publish_package")
        self.assertEqual(contract["hard_blockers"], [])
        self.assertTrue(any("Trend/support evidence is thin" in warning for warning in contract["warnings"]))
        self.assertTrue(any(check["label"] == "Final meme image is attached" and check["status"] == "pass" for check in contract["checks"]))

    def test_meme_package_missing_final_image_stays_blocked(self) -> None:
        run_date = quality_gate_pilot.now_iso()[:10]
        decision = quality_gate_pilot.evaluate_quality_gate(
            {
                "artifact_id": f"publish::meme::{run_date}::missing-image",
                "artifact_type": "social_post",
                "flow": "meme",
                "run_id": run_date,
                "source_refs": [{"path": "/tmp/state_meme.json", "source_type": "state_meme"}],
                "normalization_notes": {"source_mode": "state_file", "completeness": "high"},
                "candidate_summary": {
                    "title": "Meme Monday: Missing Image",
                    "body": (
                        "EXPECTATION: Jeep life looks rugged and dramatic\n"
                        "REALITY: Duck life makes the dashboard the main character\n\n"
                        "Meme Monday! #MemeMonday #MyJeepDuck"
                    ),
                    "images": ["🎯"],
                    "platform_targets": ["instagram"],
                },
            }
        )

        contract = decision["quality_gate_metadata"]["flow_review_contract"]
        self.assertEqual(decision["decision"], "needs_revision")
        self.assertIn("Final meme image is missing or is not a renderable image URL/path.", contract["hard_blockers"])
        self.assertIsNone(decision["preview"]["asset_url"])

    def test_complete_jeepfact_package_is_publish_ready_with_carousel_contract(self) -> None:
        run_date = quality_gate_pilot.now_iso()[:10]
        decision = quality_gate_pilot.evaluate_quality_gate(
            {
                "artifact_id": f"publish::jeepfact::{run_date}::jeep-fact-wednesday",
                "artifact_type": "social_post",
                "flow": "jeepfact",
                "run_id": run_date,
                "source_refs": [{"path": "/tmp/state_jeepfact.json", "source_type": "state_jeepfact"}],
                "normalization_notes": {"source_mode": "state_file", "completeness": "high", "input_confidence_cap": 0.85},
                "supporting_context": {"trend_refs": []},
                "candidate_summary": {
                    "title": "Jeep Fact Wednesday",
                    "body": (
                        "Jeep Fact Wednesday!\n\n"
                        "Fact #1: Early Jeeps were designed for rugged utility.\n"
                        "Fact #2: Jeep clubs helped popularize trail culture.\n"
                        "Fact #3: The Wrangler kept removable-door adventure alive.\n"
                        "Fact #4: Jeep ducking became a friendly owner-to-owner ritual.\n\n"
                        "Which fact surprised you most? Shop our Jeep ducks: myjeepduck.com\n"
                        "#JeepFactWednesday #JeepLife"
                    ),
                    "images": [
                        "https://cdn.example.com/jeepfact-cover.png",
                        "https://cdn.example.com/jeepfact-1.png",
                        "https://cdn.example.com/jeepfact-2.png",
                        "https://cdn.example.com/jeepfact-3.png",
                        "https://cdn.example.com/jeepfact-4.png",
                        "https://cdn.example.com/jeepfact-cta.png",
                    ],
                    "platform_targets": ["instagram", "facebook"],
                    "platform_variants": {
                        "instagram": {"caption": "Jeep Fact Wednesday!"},
                        "facebook": {"caption": "Jeep Fact Wednesday!"},
                    },
                },
            }
        )

        contract = decision["quality_gate_metadata"]["flow_review_contract"]
        self.assertEqual(decision["decision"], "publish_ready")
        self.assertGreaterEqual(decision["score"], 82)
        self.assertEqual(contract["reviewer"], "jeepfact_carousel_package")
        self.assertEqual(contract["hard_blockers"], [])
        self.assertEqual(decision["preview"]["asset_url"], "https://cdn.example.com/jeepfact-cover.png")
        self.assertEqual(len(decision["preview"]["asset_urls"]), 6)
        self.assertTrue(any(check["label"] == "Carousel slides are attached" and check["status"] == "pass" for check in contract["checks"]))
        self.assertTrue(any(check["label"] == "Jeep Fact framing is recognizable" and check["status"] == "pass" for check in contract["checks"]))

    def test_jeepfact_package_missing_carousel_images_stays_blocked(self) -> None:
        run_date = quality_gate_pilot.now_iso()[:10]
        decision = quality_gate_pilot.evaluate_quality_gate(
            {
                "artifact_id": f"publish::jeepfact::{run_date}::jeep-fact-wednesday",
                "artifact_type": "social_post",
                "flow": "jeepfact",
                "run_id": run_date,
                "source_refs": [{"path": "/tmp/state_jeepfact.json", "source_type": "state_jeepfact"}],
                "normalization_notes": {"source_mode": "state_file", "completeness": "high"},
                "candidate_summary": {
                    "title": "Jeep Fact Wednesday",
                    "body": (
                        "Jeep Fact Wednesday!\n"
                        "Fact #1: Jeeps helped define trail culture.\n"
                        "Fact #2: Jeep ducking makes dashboards friendlier.\n"
                        "Fact #3: Wranglers kept outdoor customization visible.\n"
                        "Fact #4: Jeep waves remain a community signal.\n"
                    ),
                    "images": ["🎯"],
                    "platform_targets": ["instagram", "facebook"],
                },
            }
        )

        contract = decision["quality_gate_metadata"]["flow_review_contract"]
        self.assertEqual(decision["decision"], "needs_revision")
        self.assertIn("Jeep Fact carousel images are missing or not renderable.", contract["hard_blockers"])
        self.assertIsNone(decision["preview"]["asset_url"])

    def test_complete_thursday_package_is_publish_ready_with_vote_contract(self) -> None:
        run_date = quality_gate_pilot.now_iso()[:10]
        decision = quality_gate_pilot.evaluate_quality_gate(
            {
                "artifact_id": f"publish::thursday::{run_date}::option-3-pirate-duck-vs-cowgirl-duck",
                "artifact_type": "social_post",
                "flow": "thursday",
                "run_id": run_date,
                "source_refs": [{"path": "/tmp/state_thursday.json", "source_type": "state_thursday"}],
                "normalization_notes": {"source_mode": "state_file", "completeness": "high", "thursday_option_id": 3},
                "supporting_context": {
                    "trend_refs": [],
                    "thursday_option_review": {"summary": "Publishable pair.", "failures": [], "warnings": []},
                },
                "candidate_summary": {
                    "title": "This-or-That Thursday: Pirate Duck vs Cowgirl Duck",
                    "body": (
                        "This or That Thursday! Option A: Pirate Duck. Option B: Cowgirl Duck. "
                        "Which one gets your vote? Drop your favorite in the comments. #ThisOrThatThursday"
                    ),
                    "images": ["https://cdn.example.com/thursday.png"],
                    "platform_targets": ["instagram"],
                    "platform_variants": {"instagram": {"caption": "This or That Thursday!"}},
                    "option_id": 3,
                    "option_a": "Pirate Duck",
                    "option_b": "Cowgirl Duck",
                },
            }
        )

        contract = decision["quality_gate_metadata"]["flow_review_contract"]
        self.assertEqual(decision["decision"], "publish_ready")
        self.assertGreaterEqual(decision["score"], 82)
        self.assertEqual(contract["reviewer"], "thursday_vote_package")
        self.assertEqual(contract["hard_blockers"], [])
        self.assertEqual(decision["quality_gate_metadata"]["thursday_option_id"], 3)
        self.assertTrue(any(check["label"] == "Vote image is attached" and check["status"] == "pass" for check in contract["checks"]))
        self.assertTrue(any(check["label"] == "Vote labels are clean" and check["status"] == "pass" for check in contract["checks"]))

    def test_thursday_package_blocks_source_listing_name_leakage(self) -> None:
        run_date = quality_gate_pilot.now_iso()[:10]
        decision = quality_gate_pilot.evaluate_quality_gate(
            {
                "artifact_id": f"publish::thursday::{run_date}::option-1-jesus-90min-creative-tonie-duck-vs-couple-duck",
                "artifact_type": "social_post",
                "flow": "thursday",
                "run_id": run_date,
                "source_refs": [{"path": "/tmp/state_thursday.json", "source_type": "state_thursday"}],
                "normalization_notes": {"source_mode": "state_file", "completeness": "high", "thursday_option_id": 1},
                "supporting_context": {
                    "thursday_option_review": {
                        "summary": "Publishable pair with small cautions to review.",
                        "failures": [],
                        "warnings": ["A used fallback normalization"],
                    },
                },
                "candidate_summary": {
                    "title": "This-or-That Thursday: Jesus 90Min Creative Tonie Duck vs Couple Duck",
                    "body": (
                        "This or That Thursday! Option A: Jesus 90Min Creative Tonie Duck. "
                        "Option B: Couple Duck. Which one gets your vote? #ThisOrThatThursday"
                    ),
                    "images": ["https://cdn.example.com/thursday.png"],
                    "platform_targets": ["instagram"],
                    "option_id": 1,
                    "option_a": "Jesus 90Min Creative Tonie Duck",
                    "option_b": "Couple Duck",
                },
            }
        )

        contract = decision["quality_gate_metadata"]["flow_review_contract"]
        self.assertEqual(decision["decision"], "needs_revision")
        self.assertTrue(any("source-listing wording" in item for item in contract["hard_blockers"]))
        self.assertTrue(any(check["label"] == "Vote labels are clean" and check["status"] == "fail" for check in contract["checks"]))

    def test_thursday_state_builds_option_specific_publish_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            state_path = root / "runs" / "2026-05-21" / "state_thursday.json"
            state_path.parent.mkdir(parents=True)
            payload = {
                "thursday_publishable_batch_options": [
                    {
                        "id": 2,
                        "duck_a_display": "Nurse Duck",
                        "duck_b_display": "Pizza Duck",
                        "duck_a": {"title": "Nurse Duck", "source_type": "duck_title"},
                        "duck_b": {"title": "Pizza Duck", "source_type": "fallback_pool"},
                        "caption": "This or That Thursday! Option A: Nurse Duck. Option B: Pizza Duck. Vote below.",
                        "preview_url": "https://cdn.example.com/thursday.png",
                        "layout_path": "runs/2026-05-21/option_2/thursday_comparison_image.png",
                        "review": {"summary": "Publishable pair.", "failures": [], "warnings": []},
                    }
                ],
            }

            original_runs_dir = phase1_observer.DUCKAGENT_RUNS_DIR
            phase1_observer.DUCKAGENT_RUNS_DIR = root / "runs"
            try:
                candidates = phase1_observer.build_thursday_publish_candidates_from_state(state_path, payload, {}, {}, [])
            finally:
                phase1_observer.DUCKAGENT_RUNS_DIR = original_runs_dir

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate["flow"], "thursday")
        self.assertIn("::option-2-", candidate["artifact_id"])
        self.assertEqual(candidate["candidate_summary"]["option_id"], 2)
        self.assertEqual(candidate["candidate_summary"]["option_a"], "Nurse Duck")
        self.assertEqual(candidate["candidate_summary"]["option_b"], "Pizza Duck")
        self.assertEqual(candidate["candidate_summary"]["images"][0], "https://cdn.example.com/thursday.png")
        self.assertTrue(candidate["candidate_summary"]["images"][1].endswith("runs/2026-05-21/option_2/thursday_comparison_image.png"))

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

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import review_loop
import review_reply_contract


class ReviewRewriteContractTests(unittest.TestCase):
    """Tests against the deterministic rule-based rewriter. The LLM path is
    explicitly disabled so these tests stay deterministic regardless of whether
    OPENAI_API_KEY is set in the local environment. LLM-path tests live in
    test_review_reply_rewriter_llm.py."""

    def setUp(self) -> None:
        self._prev_provider = os.environ.get("DUCK_REVIEW_REWRITE_PROVIDER")
        os.environ["DUCK_REVIEW_REWRITE_PROVIDER"] = "disabled"

    def tearDown(self) -> None:
        if self._prev_provider is None:
            os.environ.pop("DUCK_REVIEW_REWRITE_PROVIDER", None)
        else:
            os.environ["DUCK_REVIEW_REWRITE_PROVIDER"] = self._prev_provider

    def test_rewrite_suggestion_keeps_gift_and_shipping_anchors(self) -> None:
        rewrite = review_loop.build_rewrite_suggestion_text(
            {
                "artifact_type": "review_reply",
                "flow": "reviews_reply_positive",
                "preview": {
                    "context_text": "Very cute gift and fast shipping. My friend loved it on her Jeep dash!",
                    "proposed_text": "Thank you so much for the kind review! I'm so glad it made such a great gift for your friend. I'm so glad it arrived quickly.",
                },
            }
        )

        self.assertIsNotNone(rewrite)
        self.assertIn("gift for your friend", rewrite.lower())
        self.assertIn("arrived quickly", rewrite.lower())
        self.assertNotIn("small business", rewrite.lower())
        self.assertLessEqual(rewrite.lower().count("i'm so glad"), 1)

    def test_public_rewrite_handles_material_expectation_mismatch(self) -> None:
        rewrite = review_loop.build_rewrite_suggestion_text(
            {
                "artifact_type": "review_reply",
                "flow": "reviews_reply_positive",
                "preview": {
                    "context_text": "I thought they were plastic which would have been better.",
                    "proposed_text": "Thank you for your feedback! Our 3D printed ducks have a unique feel, and we hope you still enjoy their charm.",
                },
            }
        )

        self.assertIsNotNone(rewrite)
        lowered = rewrite.lower()
        self.assertIn("honest feedback", lowered)
        self.assertIn("material", lowered)
        self.assertIn("not what you expected", lowered)
        self.assertNotIn("kind review", lowered)
        self.assertNotIn("loved", lowered)

    def test_contract_flags_generic_positive_language_against_mixed_review(self) -> None:
        contract = review_reply_contract.build_review_reply_contract(
            "I thought they were plastic which would have been better.",
            "Thanks so much for the kind review! Thanks again for the kind review.",
            private_mode=False,
        )

        self.assertEqual(contract["classification"]["sentiment"], "complaint_adjacent")
        self.assertEqual(contract["classification"]["issue_type"], "material_expectation")
        self.assertTrue(contract["classification"]["needs_rewrite"])
        failed_ids = {check["id"] for check in contract["failed_checks"]}
        self.assertIn("no_generic_positive_reply_for_mixed_review", failed_ids)

    def test_contract_does_not_treat_positive_3d_printed_review_as_issue(self) -> None:
        contract = review_reply_contract.build_review_reply_contract(
            "Great 3D printed duck and fast shipping.",
            "Thanks so much for the kind review! I'm happy it arrived quickly.",
            private_mode=False,
        )

        self.assertEqual(contract["classification"]["sentiment"], "positive")
        self.assertEqual(contract["classification"]["issue_type"], "none")
        self.assertFalse(contract["classification"]["needs_rewrite"])


if __name__ == "__main__":
    unittest.main()

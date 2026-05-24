"""Tests for the LLM-rewrite sanity gate."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import review_reply_rewriter_sanity as sanity


NEPHEW_REVIEW = (
    "Once again, an amazing job. I ordered ducks for my nephew with his facial "
    "features — one in a Walter Payton Bears jersey and another in a Cubs cap "
    "and jersey. These are truly incredible, right down to his dimples."
)


class SanityChecksTests(unittest.TestCase):
    def test_passes_on_specific_warm_reply(self) -> None:
        rewrite = (
            "So glad the ducks captured your nephew's features down to the dimples — "
            "that's exactly what I was hoping for when you ordered them. Means a lot "
            "that they brought a little happiness."
        )
        report = sanity.evaluate_sanity(rewrite, review_text=NEPHEW_REVIEW)
        self.assertTrue(report["passed"], msg=report["failures"])

    def test_fails_when_too_short(self) -> None:
        report = sanity.evaluate_sanity("Thanks!", review_text=NEPHEW_REVIEW)
        self.assertFalse(report["passed"])
        self.assertIn("length_in_band", report["failures"])

    def test_fails_when_too_long(self) -> None:
        rewrite = "Thanks so much! " * 80  # > 600 chars
        report = sanity.evaluate_sanity(rewrite, review_text=NEPHEW_REVIEW)
        self.assertFalse(report["passed"])
        self.assertIn("length_in_band", report["failures"])

    def test_fails_when_placeholder_present(self) -> None:
        rewrite = (
            "Thanks {customer_name}! So glad the ducks for your nephew with the dimples turned out well."
        )
        report = sanity.evaluate_sanity(rewrite, review_text=NEPHEW_REVIEW)
        self.assertFalse(report["passed"])
        self.assertIn("no_placeholders", report["failures"])

    def test_fails_when_uppercase_token_placeholder_present(self) -> None:
        rewrite = (
            "Thanks [NAME]! So glad the ducks for your nephew with the dimples turned out well."
        )
        report = sanity.evaluate_sanity(rewrite, review_text=NEPHEW_REVIEW)
        self.assertFalse(report["passed"])
        self.assertIn("no_placeholders", report["failures"])

    def test_fails_on_url(self) -> None:
        rewrite = (
            "Thanks so much! The ducks for your nephew with the dimples — see more at "
            "https://example.com/ducks for sure."
        )
        report = sanity.evaluate_sanity(rewrite, review_text=NEPHEW_REVIEW)
        self.assertFalse(report["passed"])
        self.assertIn("no_external_references", report["failures"])

    def test_fails_on_email(self) -> None:
        rewrite = (
            "Thanks so much! Email me at hello@example.com if your nephew wants more ducks."
        )
        report = sanity.evaluate_sanity(rewrite, review_text=NEPHEW_REVIEW)
        self.assertFalse(report["passed"])
        self.assertIn("no_external_references", report["failures"])

    def test_fails_on_emoji(self) -> None:
        rewrite = (
            "So glad the ducks captured your nephew's features 🎯 down to the dimples!"
        )
        report = sanity.evaluate_sanity(rewrite, review_text=NEPHEW_REVIEW)
        self.assertFalse(report["passed"])
        self.assertIn("no_emoji", report["failures"])

    def test_fails_on_refusal(self) -> None:
        rewrite = (
            "I'm sorry, I can't help with that request. As an AI language model, I cannot "
            "generate a reply for your nephew's review with dimples without more details."
        )
        report = sanity.evaluate_sanity(rewrite, review_text=NEPHEW_REVIEW)
        self.assertFalse(report["passed"])
        self.assertIn("not_a_refusal", report["failures"])

    def test_fails_when_no_review_word_echoed(self) -> None:
        rewrite = (
            "Thanks so much for the kind review! Thanks again for the kind review — it really helps."
        )
        report = sanity.evaluate_sanity(rewrite, review_text=NEPHEW_REVIEW)
        self.assertFalse(report["passed"])
        self.assertIn("echo_check", report["failures"])

    def test_passes_when_one_review_word_echoed(self) -> None:
        rewrite = (
            "So glad the ducks turned out exactly the way you'd hoped for your nephew. "
            "Thank you for trusting me with this project."
        )
        report = sanity.evaluate_sanity(rewrite, review_text=NEPHEW_REVIEW)
        self.assertTrue(report["passed"], msg=report["failures"])


if __name__ == "__main__":
    unittest.main()

"""Tests for the LLM weekly_sale rewriter (Bullet 3)."""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import weekly_sale_rewriter_llm as wsr


BASE_PLAYBOOK_TEXT = (
    "Theme of the week: Outdoor Adventure | 20% off | Shopify\n"
    "Market match: Camping Duck, Hiking Boot Duck, Trailhead Cooler Duck\n"
    "Momentum boosters: Tent Glamping Duck shipped 8 this week\n"
    "Re-engagement: Email lapsed buyers about the Glamping Duck\n"
    "Etsy clearance: Wreath Duck, Snowflake Duck"
)


def _success(text: str) -> dict:
    return {
        "choices": [{"message": {"content": text}}],
        "usage": {"prompt_tokens": 320, "completion_tokens": 90},
        "elapsed_seconds": 0.6,
    }


class WeeklySaleRewriterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.env_patch = patch.dict(os.environ, {
            "OPENAI_API_KEY": "test-key",
            "DUCK_WEEKLY_SALE_REWRITE_PROVIDER": "openai",
        }, clear=False)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

    def _item(self) -> dict:
        return {"flow": "weekly_sale", "artifact_id": "test::weekly_sale"}

    def test_returns_none_when_disabled(self) -> None:
        with patch.dict(os.environ, {"DUCK_WEEKLY_SALE_REWRITE_PROVIDER": "disabled"}, clear=False):
            result = wsr.generate_weekly_sale_rewrite_via_llm(
                self._item(), base_text=BASE_PLAYBOOK_TEXT, hint="tighter",
            )
        self.assertIsNone(result)

    def test_returns_none_for_wrong_flow(self) -> None:
        item = {"flow": "review_reply", "artifact_id": "x"}
        result = wsr.generate_weekly_sale_rewrite_via_llm(item, base_text=BASE_PLAYBOOK_TEXT)
        self.assertIsNone(result)

    def test_returns_none_when_base_text_empty(self) -> None:
        result = wsr.generate_weekly_sale_rewrite_via_llm(self._item(), base_text="")
        self.assertIsNone(result)

    def test_returns_none_on_api_error(self) -> None:
        with patch.object(wsr, "_call_openai", return_value={"error": "http_500", "body": "boom"}):
            result = wsr.generate_weekly_sale_rewrite_via_llm(
                self._item(), base_text=BASE_PLAYBOOK_TEXT, hint="warmer",
            )
        self.assertIsNone(result)

    def test_returns_none_when_output_fails_echo(self) -> None:
        # Output that shares no >=4-char tokens with the base playbook.
        # Single-syllable filler so nothing accidentally overlaps with
        # 'theme', 'week', 'duck', etc.
        with patch.object(wsr, "_call_openai", return_value=_success(
            "Push hard. Sell more. Win big. Move fast. Grow now. Ship soon. Stay calm."
        )):
            result = wsr.generate_weekly_sale_rewrite_via_llm(
                self._item(), base_text=BASE_PLAYBOOK_TEXT,
            )
        self.assertIsNone(result)

    def test_success_returns_structured_result(self) -> None:
        with patch.object(wsr, "_call_openai", return_value=_success(
            "Theme: Outdoor Adventure (20% off, Shopify).\n"
            "Market match: lean on Camping Duck and Hiking Boot Duck for traffic.\n"
            "Momentum: Glamping Duck shipped 8 — repeat the angle.\n"
            "Re-engagement: email lapsed buyers about the Glamping Duck this week."
        )):
            result = wsr.generate_weekly_sale_rewrite_via_llm(
                self._item(), base_text=BASE_PLAYBOOK_TEXT, hint="warmer",
            )
        self.assertIsNotNone(result)
        self.assertEqual(result["source"], "llm")
        self.assertIn("Glamping Duck", result["text"])
        self.assertTrue(result["sanity"]["passed"])

    def test_sanity_rejects_emoji(self) -> None:
        result = wsr.evaluate_sanity(
            "Theme: Outdoor Adventure 🚀 with Camping Duck pushing strong.",
            base_text=BASE_PLAYBOOK_TEXT,
        )
        self.assertFalse(result["passed"])
        self.assertIn("no_emoji", result["failures"])

    def test_sanity_rejects_url(self) -> None:
        result = wsr.evaluate_sanity(
            "Theme: Outdoor Adventure with Camping Duck — see https://example.com/sale for details.",
            base_text=BASE_PLAYBOOK_TEXT,
        )
        self.assertFalse(result["passed"])
        self.assertIn("no_external_references", result["failures"])

    def test_sanity_accepts_grounded_rewrite(self) -> None:
        result = wsr.evaluate_sanity(
            "Theme: Outdoor Adventure (20% off). Push Camping Duck. Email lapsed buyers about the Glamping Duck.",
            base_text=BASE_PLAYBOOK_TEXT,
        )
        self.assertTrue(result["passed"])


if __name__ == "__main__":
    unittest.main()

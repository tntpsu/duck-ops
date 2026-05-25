"""Tests for the LLM jeepfact hint parser (Bullet 3)."""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import jeepfact_rewriter_llm as jf


def _success(text: str) -> dict:
    return {
        "choices": [{"message": {"content": text}}],
        "usage": {"prompt_tokens": 180, "completion_tokens": 60},
        "elapsed_seconds": 0.4,
    }


class JeepfactParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.env_patch = patch.dict(os.environ, {
            "OPENAI_API_KEY": "test-key",
            "DUCK_JEEPFACT_REWRITE_PROVIDER": "openai",
        }, clear=False)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

    def _item(self) -> dict:
        return {"flow": "jeepfact", "artifact_id": "test::jeepfact"}

    def test_returns_none_for_empty_hint(self) -> None:
        result = jf.generate_jeepfact_config_via_llm(self._item(), hint="   ")
        self.assertIsNone(result)

    def test_returns_none_when_disabled(self) -> None:
        with patch.dict(os.environ, {"DUCK_JEEPFACT_REWRITE_PROVIDER": "disabled"}, clear=False):
            result = jf.generate_jeepfact_config_via_llm(self._item(), hint="make it funny")
        self.assertIsNone(result)

    def test_strips_code_fence_from_json(self) -> None:
        with patch.object(jf, "_call_openai", return_value=_success(
            '```json\n{"hook_style": "funny", "caption_tone": "shorter"}\n```'
        )):
            result = jf.generate_jeepfact_config_via_llm(self._item(), hint="keep it light and brief")
        self.assertIsNotNone(result)
        self.assertEqual(result["config"]["hook_style"], "funny")
        self.assertEqual(result["config"]["caption_tone"], "shorter")

    def test_validator_drops_unknown_values(self) -> None:
        raw = {
            "selection_mode": "magic_mode",  # not in enum → dropped
            "hook_style": "punchy",          # valid
            "prefer_tags": ["seasonal", "made_up_tag"],  # filter
            "avoid_tags": ["sports"],
            "operator_note": "Keep it light.",
        }
        cleaned = jf._validate_config(raw)
        self.assertNotIn("selection_mode", cleaned)
        self.assertEqual(cleaned["hook_style"], "punchy")
        self.assertEqual(cleaned["prefer_tags"], ["seasonal"])
        self.assertEqual(cleaned["avoid_tags"], ["sports"])
        self.assertEqual(cleaned["operator_note"], "Keep it light.")

    def test_validator_handles_empty_dict(self) -> None:
        self.assertEqual(jf._validate_config({}), {})

    def test_returns_none_on_unparseable_json(self) -> None:
        with patch.object(jf, "_call_openai", return_value=_success("Sure, here's the config — selection_mode reroll_all")):
            result = jf.generate_jeepfact_config_via_llm(self._item(), hint="reroll everything")
        self.assertIsNone(result)

    def test_returns_validated_config_on_success(self) -> None:
        with patch.object(jf, "_call_openai", return_value=_success(
            '{"selection_mode": "new_ducks_same_facts", "prefer_tags": ["holidays", "winter"], '
            '"operator_note": "Fresh slate with a holiday angle."}'
        )):
            result = jf.generate_jeepfact_config_via_llm(
                self._item(),
                hint="give me new ducks with a holiday angle",
            )
        self.assertIsNotNone(result)
        config = result["config"]
        self.assertEqual(config["selection_mode"], "new_ducks_same_facts")
        self.assertIn("holidays", config["prefer_tags"])
        self.assertIn("winter", config["prefer_tags"])
        self.assertEqual(result["source"], "llm")


if __name__ == "__main__":
    unittest.main()

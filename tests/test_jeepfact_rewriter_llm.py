"""Tests for the LLM jeepfact hint parser (Bullet 3 + 2026-05-30
acknowledged_terms schema tightening)."""
from __future__ import annotations

import json
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


def _success_json(*, acknowledged_terms: list[str], **extra) -> dict:
    """JSON-mode-compliant response builder. Every happy-path test uses
    this so acknowledged_terms (the required field) is always present
    and the schema gate is exercised end-to-end."""
    payload = {"acknowledged_terms": acknowledged_terms, **extra}
    return _success(json.dumps(payload))


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
        # Now includes the required acknowledged_terms field.
        with patch.object(jf, "_call_openai", return_value=_success(
            '```json\n{"acknowledged_terms": ["light", "brief"], '
            '"hook_style": "funny", "caption_tone": "shorter"}\n```'
        )):
            result = jf.generate_jeepfact_config_via_llm(self._item(), hint="keep it light and brief")
        self.assertIsNotNone(result)
        self.assertEqual(result["config"]["hook_style"], "funny")
        self.assertEqual(result["config"]["caption_tone"], "shorter")
        self.assertEqual(result["acknowledged_terms"], ["light", "brief"])

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
        with patch.object(jf, "_call_openai", return_value=_success_json(
            acknowledged_terms=["new ducks", "holiday angle"],
            selection_mode="new_ducks_same_facts",
            prefer_tags=["holidays", "winter"],
            operator_note="Fresh slate with a holiday angle.",
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
        self.assertEqual(
            result["acknowledged_terms"], ["new ducks", "holiday angle"],
        )


class JeepfactSchemaGateTests(unittest.TestCase):
    """Pin the 2026-05-30 acknowledged_terms schema tightening. Phase 0
    inventory flagged this as the jeepfact-lane top gap: a bare `{}`
    was a valid JSON response, making it indistinguishable from "the
    LLM lost the prompt" — operator hints could vanish silently."""

    def setUp(self) -> None:
        self.env_patch = patch.dict(os.environ, {
            "OPENAI_API_KEY": "test-key",
            "DUCK_JEEPFACT_REWRITE_PROVIDER": "openai",
        }, clear=False)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

    def _item(self) -> dict:
        return {"flow": "jeepfact", "artifact_id": "test::jeepfact_schema"}

    def _patch_log(self, captured: list):
        return patch.object(jf, "_log_llm_call", side_effect=lambda payload: captured.append(payload))

    def _logged_rewrite(self, captured: list) -> dict:
        for entry in reversed(captured):
            if entry.get("kind") == "jeepfact_rewrite":
                return entry
        return {}

    def test_bare_empty_object_routes_to_sanity_failed(self) -> None:
        """Pre-fix behavior: `{}` returned a passing result with an
        empty config dict, and the operator's hint silently
        disappeared. Post-fix: bare `{}` lacks acknowledged_terms →
        schema failure → sanity_failed → caller falls back to the
        rule-based hint parser instead of dropping the hint."""
        captured: list = []
        with patch.object(jf, "_call_openai", return_value=_success("{}")), \
             self._patch_log(captured):
            result = jf.generate_jeepfact_config_via_llm(
                self._item(),
                hint="keep it brief and skip the sports angle",
            )
        self.assertIsNone(result, (
            "Bare {} must route to sanity_failed so the caller's "
            "fallback runs against the operator's hint instead of "
            "silently ignoring it."
        ))
        entry = self._logged_rewrite(captured)
        self.assertEqual(entry.get("outcome"), "sanity_failed")
        self.assertIn("missing_acknowledged_terms", entry.get("sanity_failures") or [])

    def test_empty_acknowledged_terms_array_rejected(self) -> None:
        """`{"acknowledged_terms": []}` is the same failure repackaged
        — explicitly empty acknowledgment leaves the hint invisible.
        Pin the empty-array rejection separately from the missing-key
        rejection so a future "tolerate empty arrays" tweak fails the
        test loudly."""
        captured: list = []
        with patch.object(jf, "_call_openai", return_value=_success(
            '{"acknowledged_terms": []}'
        )), self._patch_log(captured):
            result = jf.generate_jeepfact_config_via_llm(
                self._item(), hint="make it more interesting",
            )
        self.assertIsNone(result)
        entry = self._logged_rewrite(captured)
        self.assertIn("acknowledged_terms_empty", entry.get("sanity_failures") or [])

    def test_acknowledged_terms_wrong_type_rejected(self) -> None:
        """Non-list types in the field violate the schema even if the
        intent might be readable. Pin the failure so a future "tolerate
        a string in acknowledged_terms" change fails the test."""
        captured: list = []
        with patch.object(jf, "_call_openai", return_value=_success(
            '{"acknowledged_terms": "shorter caption"}'
        )), self._patch_log(captured):
            result = jf.generate_jeepfact_config_via_llm(
                self._item(), hint="shorten the caption please",
            )
        self.assertIsNone(result)
        entry = self._logged_rewrite(captured)
        self.assertIn("acknowledged_terms_wrong_type", entry.get("sanity_failures") or [])

    def test_long_terms_filtered_but_short_ones_kept(self) -> None:
        """The acknowledged_terms validator caps per-term word count so
        the model can't dump paraphrased prose into the field. If the
        cap drops every term, the schema fails (empty list); if it
        leaves at least one valid term, schema passes."""
        # 6-word "term" exceeds the 5-word cap and gets filtered.
        # "brief caption" survives.
        captured: list = []
        with patch.object(jf, "_call_openai", return_value=_success(
            '{"acknowledged_terms": ["brief caption", "this is a very long paraphrase here"], '
            '"caption_tone": "shorter"}'
        )), self._patch_log(captured):
            result = jf.generate_jeepfact_config_via_llm(
                self._item(), hint="keep the caption brief",
            )
        self.assertIsNotNone(result, (
            "At least one valid term survives the per-term word cap, "
            "so the schema gate must pass."
        ))
        self.assertEqual(result["acknowledged_terms"], ["brief caption"])
        self.assertEqual(result["config"]["caption_tone"], "shorter")

    def test_response_format_is_json_object(self) -> None:
        """Native JSON mode is requested from the provider, not just
        hinted at in the prompt. Same wiring as the rewriter + scorer
        refactors shipped earlier today."""
        captured: dict = {}
        def fake_call(prompt: str, **kwargs):
            captured["kwargs"] = kwargs
            return _success_json(acknowledged_terms=["funny"], hook_style="funny")
        with patch.object(jf, "_call_openai", side_effect=fake_call):
            jf.generate_jeepfact_config_via_llm(self._item(), hint="be funnier")
        self.assertEqual(
            captured["kwargs"].get("response_format"),
            {"type": "json_object"},
        )


if __name__ == "__main__":
    unittest.main()

"""Tests for the catalog-dedup LLM disambiguator (M.4)."""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import catalog_dedup_llm as cd


CANDIDATES = [
    {
        "product_id": "8021019656375",
        "title": "Flamingo Duck",
        "handle": "flamingo-duck",
        "matched_tokens": ["flamingo"],
    },
    {
        "product_id": "7967436177591",
        "title": "Poodle Duck",
        "handle": "poodle-duck",
        "matched_tokens": ["duck"],
    },
]


def _success(text: str) -> dict:
    return {
        "choices": [{"message": {"content": text}}],
        "usage": {"prompt_tokens": 250, "completion_tokens": 60},
        "elapsed_seconds": 0.6,
    }


class CatalogDedupLLMTests(unittest.TestCase):
    def setUp(self) -> None:
        self.env_patch = patch.dict(os.environ, {
            "OPENAI_API_KEY": "test-key",
            "DUCK_CATALOG_DEDUP_PROVIDER": "openai",
        }, clear=False)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

    def test_returns_none_when_disabled(self) -> None:
        with patch.dict(os.environ, {"DUCK_CATALOG_DEDUP_PROVIDER": "disabled"}, clear=False):
            result = cd.disambiguate_catalog_match(
                proposed_theme="tropical flamingo duck", candidates=CANDIDATES,
            )
        self.assertIsNone(result)

    def test_returns_none_with_empty_theme(self) -> None:
        result = cd.disambiguate_catalog_match(proposed_theme="   ", candidates=CANDIDATES)
        self.assertIsNone(result)

    def test_returns_none_with_no_candidates(self) -> None:
        result = cd.disambiguate_catalog_match(proposed_theme="flamingo duck", candidates=[])
        self.assertIsNone(result)

    def test_returns_none_on_api_error(self) -> None:
        with patch.object(cd, "_call_openai", return_value={"error": "http_500"}):
            result = cd.disambiguate_catalog_match(
                proposed_theme="tropical flamingo duck", candidates=CANDIDATES,
            )
        self.assertIsNone(result)

    def test_returns_none_when_matched_id_not_in_input_list(self) -> None:
        # LLM hallucinates a product_id we didn't give it.
        with patch.object(cd, "_call_openai", return_value=_success(
            '{"is_duplicate": true, "matched_product_id": "99999", "confidence": 90, "reasoning": "x"}'
        )):
            result = cd.disambiguate_catalog_match(
                proposed_theme="tropical flamingo duck", candidates=CANDIDATES,
            )
        self.assertIsNone(result)

    def test_returns_none_for_non_bool_is_duplicate(self) -> None:
        with patch.object(cd, "_call_openai", return_value=_success(
            '{"is_duplicate": "yes", "matched_product_id": "8021019656375", "confidence": 90, "reasoning": "x"}'
        )):
            result = cd.disambiguate_catalog_match(
                proposed_theme="tropical flamingo duck", candidates=CANDIDATES,
            )
        self.assertIsNone(result)

    def test_strips_code_fence_from_json(self) -> None:
        with patch.object(cd, "_call_openai", return_value=_success(
            '```json\n{"is_duplicate": true, "matched_product_id": "8021019656375", "confidence": 92, "reasoning": "same concept, different phrasing"}\n```'
        )):
            result = cd.disambiguate_catalog_match(
                proposed_theme="tropical flamingo duck", candidates=CANDIDATES,
            )
        self.assertIsNotNone(result)
        self.assertTrue(result["is_duplicate"])
        self.assertEqual(result["matched_product_id"], "8021019656375")
        self.assertEqual(result["confidence"], 92)

    def test_returns_verdict_when_distinct(self) -> None:
        with patch.object(cd, "_call_openai", return_value=_success(
            '{"is_duplicate": false, "matched_product_id": "", "confidence": 75, "reasoning": "summer wedding is a distinct audience from beach"}'
        )):
            result = cd.disambiguate_catalog_match(
                proposed_theme="summer wedding duck", candidates=CANDIDATES,
            )
        self.assertIsNotNone(result)
        self.assertFalse(result["is_duplicate"])
        self.assertEqual(result["matched_product_id"], "")
        self.assertEqual(result["confidence"], 75)
        self.assertEqual(result["source"], "llm")

    def test_clamps_confidence_to_0_100(self) -> None:
        with patch.object(cd, "_call_openai", return_value=_success(
            '{"is_duplicate": true, "matched_product_id": "8021019656375", "confidence": 150, "reasoning": "x"}'
        )):
            result = cd.disambiguate_catalog_match(
                proposed_theme="t", candidates=CANDIDATES,
            )
        self.assertEqual(result["confidence"], 100)

    def test_truncates_long_reasoning(self) -> None:
        long_reasoning = "A" * 500
        with patch.object(cd, "_call_openai", return_value=_success(
            f'{{"is_duplicate": false, "matched_product_id": "", "confidence": 70, "reasoning": "{long_reasoning}"}}'
        )):
            result = cd.disambiguate_catalog_match(
                proposed_theme="t", candidates=CANDIDATES,
            )
        self.assertLess(len(result["reasoning"]), cd.MAX_REASONING_LENGTH + 5)

    def test_returns_none_on_unparseable_json(self) -> None:
        with patch.object(cd, "_call_openai", return_value=_success(
            "Sure — looks like a duplicate to me."
        )):
            result = cd.disambiguate_catalog_match(
                proposed_theme="t", candidates=CANDIDATES,
            )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()

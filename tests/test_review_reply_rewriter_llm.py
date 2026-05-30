"""Tests for the LLM rewriter orchestration (mocks the OpenAI HTTP call)."""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import review_reply_rewriter_llm as llm


def _item(review: str, draft: str, *, artifact_id: str = "test::reviews_reply_positive::2026-05-23::review-1") -> dict:
    return {
        "artifact_id": artifact_id,
        "artifact_type": "review_reply",
        "flow": "reviews_reply_positive",
        "preview": {
            "context_text": review,
            "proposed_text": draft,
        },
    }


NEPHEW_REVIEW = (
    "Once again, an amazing job. I ordered ducks for my nephew with his facial "
    "features. These are truly incredible, right down to his dimples."
)
NEPHEW_DRAFT = "Thank you so much for your kind words! I'm thrilled to hear the ducks captured your nephew's features."


def _success_response(text: str) -> dict:
    """Wrap a raw text payload as if it were an OpenAI completion. Used
    only for negative tests that exercise the JSON-parse/contract path
    with intentionally non-conforming content."""
    return {
        "choices": [{"message": {"content": text}}],
        "usage": {"prompt_tokens": 200, "completion_tokens": 50},
        "elapsed_seconds": 0.5,
    }


def _success_json_response(*, specific_detail_echoed: str, reply_text: str) -> dict:
    """Build the JSON-mode-compliant completion shape the production
    rewriter expects post-refactor. Every happy-path test uses this so
    the test surface mirrors the live contract."""
    payload = json.dumps({
        "specific_detail_echoed": specific_detail_echoed,
        "reply_text": reply_text,
    })
    return _success_response(payload)


class LLMRewriterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.env_patches = [
            patch.dict(llm.os.environ, {
                "OPENAI_API_KEY": "test-key",
                "DUCK_REVIEW_REWRITE_PROVIDER": "openai",
            }, clear=False),
        ]
        for p in self.env_patches:
            p.start()
        self.addCleanup(self._stop_env_patches)
        # Block accidental real HTTP — every call needs to mock _call_openai.

    def _stop_env_patches(self) -> None:
        for p in self.env_patches:
            p.stop()

    def test_returns_none_when_disabled(self) -> None:
        with patch.dict(llm.os.environ, {"DUCK_REVIEW_REWRITE_PROVIDER": "disabled"}, clear=False):
            result = llm.generate_rewrite_via_llm(_item(NEPHEW_REVIEW, NEPHEW_DRAFT))
        self.assertIsNone(result)

    def test_returns_none_when_artifact_type_is_not_review_reply(self) -> None:
        item = _item(NEPHEW_REVIEW, NEPHEW_DRAFT)
        item["artifact_type"] = "social_post"
        result = llm.generate_rewrite_via_llm(item)
        self.assertIsNone(result)

    def test_returns_none_on_missing_review_text(self) -> None:
        item = _item("", NEPHEW_DRAFT)
        result = llm.generate_rewrite_via_llm(item)
        self.assertIsNone(result)

    def test_returns_none_when_api_returns_error(self) -> None:
        with patch.object(llm, "_call_openai", return_value={"error": "http_500", "body": "boom"}):
            result = llm.generate_rewrite_via_llm(_item(NEPHEW_REVIEW, NEPHEW_DRAFT))
        self.assertIsNone(result)

    def test_returns_none_when_output_fails_sanity_echo(self) -> None:
        # The historical "Thanks so much for the kind review" stock
        # reply now fails at the JSON contract layer because
        # specific_detail_echoed="kind review" tokenizes to all
        # stopwords. Pre-refactor this was caught one layer later
        # by echo_check; post-refactor the schema rejects it before
        # evaluate_sanity even runs. Either failure mode returns
        # None, so the caller-side contract is preserved.
        with patch.object(llm, "_call_openai", return_value=_success_json_response(
            specific_detail_echoed="kind review",
            reply_text="Thanks so much for the kind review! Thanks again for the kind review. Means a lot to me.",
        )):
            result = llm.generate_rewrite_via_llm(_item(NEPHEW_REVIEW, NEPHEW_DRAFT))
        self.assertIsNone(result)

    def test_returns_structured_result_on_success(self) -> None:
        good_rewrite = (
            "So glad the ducks captured your nephew down to the dimples — that's exactly "
            "what I was hoping for when you ordered them. Means a lot."
        )
        with patch.object(llm, "_call_openai", return_value=_success_json_response(
            specific_detail_echoed="dimples",
            reply_text=good_rewrite,
        )):
            result = llm.generate_rewrite_via_llm(_item(NEPHEW_REVIEW, NEPHEW_DRAFT), hint="mention the nephew")
        self.assertIsNotNone(result)
        self.assertEqual(result["text"], good_rewrite)
        self.assertEqual(result["source"], "llm")
        self.assertEqual(result["provider"], "openai")
        self.assertEqual(result["hint"], "mention the nephew")
        self.assertEqual(result["specific_detail_echoed"], "dimples")
        self.assertTrue(result["sanity"]["passed"])
        self.assertEqual(result["usage"]["prompt_tokens"], 200)

    def test_strips_outer_quotes_from_llm_output(self) -> None:
        quoted = (
            '"So glad the ducks captured your nephew down to the dimples — exactly what I '
            'was hoping for. Means a lot."'
        )
        with patch.object(llm, "_call_openai", return_value=_success_json_response(
            specific_detail_echoed="dimples",
            reply_text=quoted,
        )):
            result = llm.generate_rewrite_via_llm(_item(NEPHEW_REVIEW, NEPHEW_DRAFT))
        self.assertIsNotNone(result)
        self.assertFalse(result["text"].startswith('"'))
        self.assertFalse(result["text"].endswith('"'))

    def test_prompt_contains_review_draft_and_hint(self) -> None:
        captured: dict = {}
        def fake_call(prompt: str, **kwargs) -> dict:
            captured["prompt"] = prompt
            captured["kwargs"] = kwargs
            return _success_json_response(
                specific_detail_echoed="dimples",
                reply_text=(
                    "So glad your nephew's ducks turned out incredible — the dimples detail "
                    "is the kind of thing that makes this project worth doing. Thanks again."
                ),
            )
        with patch.object(llm, "_call_openai", side_effect=fake_call):
            llm.generate_rewrite_via_llm(
                _item(NEPHEW_REVIEW, NEPHEW_DRAFT),
                hint="lean into the personal/emotional angle",
            )
        prompt = captured["prompt"]
        self.assertIn("nephew", prompt)
        self.assertIn("dimples", prompt)
        self.assertIn(NEPHEW_DRAFT, prompt)
        self.assertIn("lean into the personal/emotional angle", prompt)
        # The prompt now leads with a CRITICAL RULE that demands a
        # specific-detail echo. Pin both halves so a future prompt
        # tweak can't accidentally water this down — the absence of
        # this instruction was the original 19% echo_check failure mode.
        self.assertIn("MUST reference at least one specific detail", prompt)
        self.assertIn("will be REJECTED", prompt)
        # The OUTPUT CONTRACT line is the load-bearing instruction
        # for the JSON-mode refactor — pin its two required keys.
        self.assertIn("specific_detail_echoed", prompt)
        self.assertIn("reply_text", prompt)
        # response_format flows through to call_openai so native
        # JSON mode is actually requested from the provider, not
        # just hinted at in the prompt.
        self.assertEqual(
            captured["kwargs"].get("response_format"),
            {"type": "json_object"},
            "response_format must be json_object — without it the API "
            "can return prose and the schema becomes prompt-only.",
        )


class JSONContractTests(unittest.TestCase):
    """Pin the schema contract added by the 2026-05-30 JSON-mode
    refactor. Every failure mode here used to slip through the
    free-text + post-hoc echo_check path. The whole point of moving
    to JSON-mode is that these checks fire BEFORE the reply text is
    cleaned, scored, or fed to evaluate_sanity — schema is the
    load-bearing gate."""

    def setUp(self) -> None:
        self.env_patches = [
            patch.dict(llm.os.environ, {
                "OPENAI_API_KEY": "test-key",
                "DUCK_REVIEW_REWRITE_PROVIDER": "openai",
            }, clear=False),
        ]
        for p in self.env_patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in self.env_patches])

    def _logged(self, captured_log: list) -> dict:
        """Return the most recent log entry of kind review_reply_rewrite.
        Filters out the missing_api_key / unsupported_provider entries
        that don't carry a kind."""
        for entry in reversed(captured_log):
            if entry.get("kind") == "review_reply_rewrite":
                return entry
        return {}

    def _patch_log(self, captured_log: list):
        return patch.object(llm, "_log_llm_call", side_effect=lambda payload: captured_log.append(payload))

    def test_json_unparseable_routes_to_sanity_failed(self) -> None:
        """A response that's not parseable JSON at all is bucketed as
        sanity_failed, NOT api_failure — the provider responded fine;
        the model's output is broken."""
        captured: list = []
        with patch.object(llm, "_call_openai", return_value=_success_response("not json at all, just prose")), \
             self._patch_log(captured):
            result = llm.generate_rewrite_via_llm(_item(NEPHEW_REVIEW, NEPHEW_DRAFT))
        self.assertIsNone(result)
        entry = self._logged(captured)
        self.assertEqual(entry.get("outcome"), "sanity_failed")
        self.assertIn("json_unparseable", entry.get("sanity_failures") or [])

    def test_missing_specific_detail_echoed_fails(self) -> None:
        captured: list = []
        with patch.object(llm, "_call_openai", return_value=_success_response(
            json.dumps({"reply_text": "So glad the dimples turned out — thanks for sharing photos."})
        )), self._patch_log(captured):
            result = llm.generate_rewrite_via_llm(_item(NEPHEW_REVIEW, NEPHEW_DRAFT))
        self.assertIsNone(result)
        entry = self._logged(captured)
        self.assertIn("missing_specific_detail_echoed", entry.get("sanity_failures") or [])

    def test_missing_reply_text_fails(self) -> None:
        captured: list = []
        with patch.object(llm, "_call_openai", return_value=_success_response(
            json.dumps({"specific_detail_echoed": "dimples"})
        )), self._patch_log(captured):
            result = llm.generate_rewrite_via_llm(_item(NEPHEW_REVIEW, NEPHEW_DRAFT))
        self.assertIsNone(result)
        entry = self._logged(captured)
        self.assertIn("missing_reply_text", entry.get("sanity_failures") or [])

    def test_specific_detail_must_be_non_stopword(self) -> None:
        """If specific_detail_echoed tokenizes entirely to stopwords
        (e.g. "kind review"), the schema rejects it — the model
        dodged the instruction by quoting filler. This is the exact
        2026-05-26 / 2026-05-28 failure pattern the refactor targets."""
        captured: list = []
        with patch.object(llm, "_call_openai", return_value=_success_response(
            json.dumps({
                "specific_detail_echoed": "kind review",
                "reply_text": "Thanks so much for the kind review — means a lot to me.",
            })
        )), self._patch_log(captured):
            result = llm.generate_rewrite_via_llm(_item(NEPHEW_REVIEW, NEPHEW_DRAFT))
        self.assertIsNone(result)
        entry = self._logged(captured)
        self.assertIn("specific_detail_too_generic", entry.get("sanity_failures") or [])

    def test_specific_detail_must_appear_in_review(self) -> None:
        """The echo claim must be grounded — if the model invents a
        detail that isn't in the review (e.g. 'spaceship' for a review
        about nephews and dimples), the schema rejects it. Catches
        hallucination at the schema layer."""
        captured: list = []
        with patch.object(llm, "_call_openai", return_value=_success_response(
            json.dumps({
                "specific_detail_echoed": "spaceship adventure",
                "reply_text": "So glad the spaceship adventure paid off — thrilled it landed well.",
            })
        )), self._patch_log(captured):
            result = llm.generate_rewrite_via_llm(_item(NEPHEW_REVIEW, NEPHEW_DRAFT))
        self.assertIsNone(result)
        entry = self._logged(captured)
        self.assertIn("specific_detail_not_in_review", entry.get("sanity_failures") or [])

    def test_code_fence_wrapped_json_is_accepted(self) -> None:
        """OpenAI native JSON mode usually returns bare JSON, but some
        responses (especially under markdown-heavy prompts) still wrap
        the payload in ```json fences. _strip_code_fence tolerates it
        so the schema check runs on the inner object."""
        good_rewrite = (
            "So glad the ducks captured your nephew down to the dimples — "
            "exactly what I was hoping for. Means a lot."
        )
        fenced = "```json\n" + json.dumps({
            "specific_detail_echoed": "dimples",
            "reply_text": good_rewrite,
        }) + "\n```"
        with patch.object(llm, "_call_openai", return_value=_success_response(fenced)):
            result = llm.generate_rewrite_via_llm(_item(NEPHEW_REVIEW, NEPHEW_DRAFT))
        self.assertIsNotNone(result)
        self.assertEqual(result["text"], good_rewrite)
        self.assertEqual(result["specific_detail_echoed"], "dimples")


class FewShotAnchoringTests(unittest.TestCase):
    """Approved past rewrites in the feedback log should appear as few-shot
    examples in the LLM prompt (Slice K.3)."""

    def setUp(self) -> None:
        import tempfile
        from pathlib import Path as _Path
        self.tmp = tempfile.TemporaryDirectory()
        self._feedback_path_patch = patch.object(
            llm,
            "REVIEW_REPLY_FEEDBACK_PATH",
            _Path(self.tmp.name) / "review_reply_feedback.jsonl",
        )
        self._feedback_path_patch.start()
        self.addCleanup(self._feedback_path_patch.stop)
        self.addCleanup(self.tmp.cleanup)

    def _write_feedback(self, entries: list[dict]) -> None:
        with open(llm.REVIEW_REPLY_FEEDBACK_PATH, "w", encoding="utf-8") as fh:
            for e in entries:
                fh.write(json.dumps(e) + "\n")

    def test_empty_feedback_log_produces_no_few_shot_block(self) -> None:
        examples = llm._load_approved_examples()
        self.assertEqual(examples, [])
        self.assertEqual(llm._format_few_shot_block(examples), "")

    def test_approved_entries_become_few_shot_examples(self) -> None:
        self._write_feedback([
            {
                "at": "2026-05-20T10:00:00-04:00",
                "operator_action": "approve",
                "customer_review": "So cute! Arrived safe and sound!",
                "approved_reply_text": "So glad it arrived safe — thank you!",
            },
            {
                "at": "2026-05-21T10:00:00-04:00",
                "operator_action": "approve",
                "customer_review": "Great graduation gift!",
                "approved_reply_text": "Thank you — so glad it made a fun graduation gift.",
            },
        ])
        examples = llm._load_approved_examples()
        self.assertEqual(len(examples), 2)
        # Newest first.
        self.assertIn("graduation", examples[0]["review"])
        block = llm._format_few_shot_block(examples)
        self.assertIn("graduation gift", block)
        self.assertIn("Approved past examples", block)

    def test_non_approve_entries_are_skipped(self) -> None:
        self._write_feedback([
            {"operator_action": "discard", "customer_review": "X", "approved_reply_text": "Y"},
            {"operator_action": "needs_changes", "customer_review": "X2", "approved_reply_text": "Y2"},
            {"operator_action": "approve", "customer_review": "Real review!", "approved_reply_text": "Real reply!"},
        ])
        examples = llm._load_approved_examples()
        self.assertEqual(len(examples), 1)
        self.assertEqual(examples[0]["review"], "Real review!")

    def test_few_shot_block_appears_in_prompt(self) -> None:
        self._write_feedback([
            {
                "operator_action": "approve",
                "customer_review": "Tiny but mighty duck!",
                "approved_reply_text": "Thanks — so glad the little guy made you smile.",
            },
        ])
        prompt = llm._build_prompt(NEPHEW_REVIEW, NEPHEW_DRAFT, hint="")
        self.assertIn("Approved past examples", prompt)
        self.assertIn("Tiny but mighty", prompt)
        self.assertIn("little guy made you smile", prompt)


if __name__ == "__main__":
    unittest.main()

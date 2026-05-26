"""Retry-behavior tests for llm_call_helpers.call_openai.

The original implementation made one attempt and returned http_500 on
any transient OpenAI failure — directly responsible for the 19%
api_failure rate that put Review Reply Rewriter in the OS "Repair
now" bucket. The retry layer added in this commit retries 429/5xx up
to _MAX_RETRIES times with exponential backoff; these tests pin the
contract so a future refactor can't quietly drop it.
"""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import llm_call_helpers  # noqa: E402


class _StubResponse:
    """Minimal stand-in for requests.Response used in tests."""

    def __init__(self, status_code: int, body: str = '{"choices": [{"message": {"content": "ok"}}]}'):
        self.status_code = status_code
        self.text = body
        self._body = body

    def json(self):
        import json
        return json.loads(self._body)


class CallOpenAIRetryTests(unittest.TestCase):
    def setUp(self) -> None:
        os.environ["OPENAI_API_KEY"] = "test-key"
        # Skip the backoff sleeps so the test is fast and deterministic.
        self._sleep_patch = patch.object(llm_call_helpers.time, "sleep", lambda *_: None)
        self._sleep_patch.start()

    def tearDown(self) -> None:
        self._sleep_patch.stop()

    def _patch_post(self, responses):
        """Each call to requests.post returns the next entry. Entries
        can be a _StubResponse to return, or an Exception to raise."""
        iterator = iter(responses)

        def fake_post(*args, **kwargs):
            nxt = next(iterator)
            if isinstance(nxt, Exception):
                raise nxt
            return nxt

        return patch("requests.post", side_effect=fake_post)

    def test_success_first_try_no_retry(self) -> None:
        with self._patch_post([_StubResponse(200)]):
            result = llm_call_helpers.call_openai("hi")
        self.assertEqual(result["retry_count"], 0)
        self.assertNotIn("error", result)

    def test_500_then_success_retries(self) -> None:
        with self._patch_post([_StubResponse(500, "oops"), _StubResponse(200)]):
            result = llm_call_helpers.call_openai("hi")
        self.assertEqual(result["retry_count"], 1)
        self.assertNotIn("error", result)

    def test_500_exhausts_retries(self) -> None:
        responses = [_StubResponse(500, "oops")] * llm_call_helpers._MAX_RETRIES
        with self._patch_post(responses):
            result = llm_call_helpers.call_openai("hi")
        self.assertEqual(result["error"], "http_500")
        self.assertEqual(result["retry_count"], llm_call_helpers._MAX_RETRIES - 1)

    def test_400_returns_immediately(self) -> None:
        # 400 is a client error; retrying is pointless and would mask
        # the real bug (bad prompt, bad model name, etc).
        with self._patch_post([_StubResponse(400, "bad request")]):
            result = llm_call_helpers.call_openai("hi")
        self.assertEqual(result["error"], "http_400")
        self.assertEqual(result["retry_count"], 0)

    def test_429_retried(self) -> None:
        with self._patch_post([_StubResponse(429), _StubResponse(200)]):
            result = llm_call_helpers.call_openai("hi")
        self.assertEqual(result["retry_count"], 1)
        self.assertNotIn("error", result)

    def test_request_exception_retried(self) -> None:
        import requests
        with self._patch_post([requests.ConnectionError("network blip"), _StubResponse(200)]):
            result = llm_call_helpers.call_openai("hi")
        self.assertEqual(result["retry_count"], 1)
        self.assertNotIn("error", result)


if __name__ == "__main__":
    unittest.main()

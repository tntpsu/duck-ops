from __future__ import annotations

import sys
import unittest
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from review_reply_discovery import parse_eval_json, review_surface_url


class ReviewReplyDiscoveryTests(unittest.TestCase):
    def test_parse_eval_json_decodes_nested_json_object_string(self) -> None:
        output = '### Result\n"{\\"ok\\":true,\\"count\\":2}"\n### Ran Playwright code\n```js\n```\n'
        parsed = parse_eval_json(output)
        self.assertEqual(parsed, {"ok": True, "count": 2})

    def test_parse_eval_json_decodes_nested_json_array_string(self) -> None:
        output = '### Result\n"[{\\"href\\":\\"https://www.etsy.com/messages/1\\",\\"text\\":\\"R Henderson\\"}]"\n### Ran Playwright code\n```js\n```\n'
        parsed = parse_eval_json(output)
        self.assertEqual(parsed, [{"href": "https://www.etsy.com/messages/1", "text": "R Henderson"}])

    def test_review_surface_url_canonicalizes_shop_anchor_to_reviews_page(self) -> None:
        url = "https://www.etsy.com/shop/myJeepDuck#reviews"
        self.assertEqual(
            review_surface_url(url),
            "https://www.etsy.com/shop/myJeepDuck/reviews?ref=pagination&page=1",
        )

    def test_review_surface_url_preserves_existing_review_page_and_adds_defaults(self) -> None:
        url = "https://www.etsy.com/shop/myJeepDuck/reviews?page=3"
        self.assertEqual(
            review_surface_url(url),
            "https://www.etsy.com/shop/myJeepDuck/reviews?page=3&ref=pagination",
        )


if __name__ == "__main__":
    unittest.main()


class CaptureTargetReviewScreenshotTests(unittest.TestCase):
    """Evidence capture is a receipt, never the action: it must not raise, and
    when the ephemeral marker is gone it falls back to the stable
    data-review-region row, then the full viewport (2026-08-14 regression:
    a green dry-run fill was marked failed by a marker-only screenshot)."""

    def _run(self, tmp_path: Path, responses: list[Exception | str], transaction_id: str | None):
        import review_reply_discovery as disc

        calls: list[str] = []

        def fake_run_pw_command(session: str, command: str, code: str) -> str:
            calls.append(code)
            outcome = responses[min(len(calls), len(responses)) - 1]
            if isinstance(outcome, Exception):
                raise outcome
            path = code.split('path: "')[1].split('"')[0]
            Path(path).write_bytes(b"png")
            return outcome

        original = disc.run_pw_command
        disc.run_pw_command = fake_run_pw_command
        try:
            result = disc.capture_target_review_screenshot(
                "esd", tmp_path, transaction_id=transaction_id
            )
        finally:
            disc.run_pw_command = original
        return result, calls

    def test_marker_gone_falls_back_to_transaction_row(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            result, calls = self._run(
                Path(tmp), [RuntimeError("marker timeout"), "ok"], "5143613131"
            )
        self.assertIsNotNone(result)
        self.assertEqual(len(calls), 2)
        self.assertIn('data-review-region=\\"5143613131\\"', calls[1].replace('\\"', '\\"'))
        self.assertIn("5143613131", calls[1])

    def test_all_selectors_fail_returns_none_without_raising(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            result, calls = self._run(
                Path(tmp),
                [RuntimeError("a"), RuntimeError("b"), RuntimeError("c")],
                "5143613131",
            )
        self.assertIsNone(result)
        self.assertEqual(len(calls), 3)

    def test_no_transaction_id_still_tries_viewport_fallback(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            result, calls = self._run(Path(tmp), [RuntimeError("marker"), "ok"], None)
        self.assertIsNotNone(result)
        self.assertEqual(len(calls), 2)
        self.assertIn("page.screenshot", calls[1])

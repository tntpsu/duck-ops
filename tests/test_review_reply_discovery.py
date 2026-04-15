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

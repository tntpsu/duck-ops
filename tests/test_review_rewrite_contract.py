from __future__ import annotations

import sys
import unittest
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import review_loop


class ReviewRewriteContractTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()

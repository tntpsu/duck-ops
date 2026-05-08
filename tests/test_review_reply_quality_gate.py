from __future__ import annotations

import sys
import unittest
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import quality_gate_pilot


class ReviewReplyQualityGateTests(unittest.TestCase):
    def test_warm_specific_public_reply_can_publish_without_literal_thanks(self) -> None:
        outcome = quality_gate_pilot.evaluate_review_reply(
            {
                "source_refs": ["daily-summary"],
                "candidate_summary": {
                    "customer_review": "Definitely a high quality item! I absolutely love it!",
                    "body": "It's wonderful to hear that you absolutely love it! I'm glad the quality came through clearly.",
                },
            },
            age_days=0,
            private_mode=False,
        )

        self.assertEqual(outcome["decision"], "publish_ready")
        self.assertGreaterEqual(outcome["score"], 78)


if __name__ == "__main__":
    unittest.main()

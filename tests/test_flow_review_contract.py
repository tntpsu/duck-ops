from __future__ import annotations

import sys
import unittest
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from flow_review_contract import FLOW_REVIEW_SCHEMA_VERSION, build_flow_review_contract, flow_review_check


class FlowReviewContractTests(unittest.TestCase):
    def test_build_flow_review_contract_preserves_required_shape(self) -> None:
        contract = build_flow_review_contract(
            reviewer="meme_publish_package",
            hard_blockers=[],
            warnings=["Trend support is thin.", "Trend support is thin."],
            checks=[flow_review_check("pass", "Final image is attached", "https://example.com/image.png")],
            operator_summary="Ready with a warning.",
            approval_summary="Approve this Meme Monday post.",
            recommended_action="approve",
        )

        self.assertEqual(contract["schema_version"], FLOW_REVIEW_SCHEMA_VERSION)
        self.assertEqual(contract["reviewer"], "meme_publish_package")
        self.assertEqual(contract["hard_blockers"], [])
        self.assertEqual(contract["warnings"], ["Trend support is thin."])
        self.assertEqual(contract["checks"][0]["status"], "pass")
        self.assertEqual(contract["checks"][0]["label"], "Final image is attached")
        self.assertEqual(contract["operator_summary"], "Ready with a warning.")
        self.assertEqual(contract["approval_summary"], "Approve this Meme Monday post.")
        self.assertEqual(contract["recommended_action"], "approve")
        self.assertIn("request_revision", contract["operator_actions"])

    def test_flow_review_contract_defaults_to_request_revision_when_blocked(self) -> None:
        contract = build_flow_review_contract(
            reviewer="jeepfact_carousel_package",
            hard_blockers=["Cover slide is missing."],
            checks=[flow_review_check("unknown", "Cover slide is attached")],
        )

        self.assertEqual(contract["recommended_action"], "request_revision")
        self.assertEqual(contract["checks"][0]["status"], "warn")
        self.assertEqual(contract["operator_summary"], "OpenClaw found blocking issues that need revision.")


if __name__ == "__main__":
    unittest.main()


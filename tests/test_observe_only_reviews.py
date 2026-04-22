from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import data_model_governance_review
import documentation_governance_review
import reliability_review
import tech_debt_triage


class ObserveOnlyReviewTests(unittest.TestCase):
    def test_tech_debt_triage_builds_ranked_items(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "tech_debt_triage.json"
            output_path = root / "output" / "operator" / "tech_debt_triage.md"

            with patch.object(tech_debt_triage, "TECH_DEBT_STATE_PATH", state_path), patch.object(
                tech_debt_triage, "TECH_DEBT_OUTPUT_PATH", output_path
            ), patch.object(
                tech_debt_triage,
                "health_payload",
                return_value={"failures": {"artifact_failures": [{"run_id": "run-1", "label": "export", "reason": "boom"}]}},
            ), patch.object(
                tech_debt_triage,
                "health_alerts",
                return_value=[
                    {"flow_id": "weekly_sale_monitor", "label": "Weekly Sale Monitor", "status": "bad", "last_run_state": "stale_input"},
                    {"flow_id": "weekly_campaign_coordination", "label": "Weekly Coordination", "status": "warn", "last_run_state": "publication_lane_ready"},
                ],
            ), patch.object(
                tech_debt_triage,
                "repo_status",
                side_effect=[
                    {"repo": "duckAgent", "modified_count": 2, "untracked_count": 1},
                    {"repo": "duck-ops", "modified_count": 0, "untracked_count": 0},
                ],
            ), patch.object(
                tech_debt_triage,
                "load_json",
                return_value={"findings": [{"title": "Working trees are not clean"}]},
            ):
                payload = tech_debt_triage.build_tech_debt_triage()

            self.assertGreaterEqual(payload["item_count"], 3)
            self.assertEqual(payload["items"][0]["priority"], "P1")
            self.assertTrue(output_path.exists())

    def test_reliability_review_marks_bad_lanes_as_no_go(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "reliability_review.json"
            output_path = root / "output" / "operator" / "reliability_review.md"

            with patch.object(reliability_review, "RELIABILITY_STATE_PATH", state_path), patch.object(
                reliability_review, "RELIABILITY_OUTPUT_PATH", output_path
            ), patch.object(
                reliability_review,
                "health_alerts",
                return_value=[
                    {
                        "flow_id": "weekly_sale_monitor",
                        "label": "Weekly Sale Monitor",
                        "status": "bad",
                        "last_run_state": "stale_input",
                        "last_run_at": "2026-04-15T06:00:00-04:00",
                        "last_run_path": "/tmp/week.json",
                        "success_rate_label": "stale input",
                    }
                ],
            ):
                payload = reliability_review.build_reliability_review()

            self.assertEqual(payload["review_count"], 1)
            self.assertEqual(payload["reviews"][0]["go_decision"], "no-go")
            self.assertTrue(output_path.exists())

    def test_data_model_governance_detects_out_of_sync_json(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "data_model_governance_review.json"
            output_path = root / "output" / "operator" / "data_model_governance_review.md"
            state_json = root / "state" / "surface.json"
            operator_json = root / "output" / "operator" / "surface.json"
            markdown = root / "output" / "operator" / "surface.md"
            state_json.parent.mkdir(parents=True, exist_ok=True)
            operator_json.parent.mkdir(parents=True, exist_ok=True)
            state_json.write_text(json.dumps({"generated_at": "2026-04-15T06:00:00-04:00"}), encoding="utf-8")
            operator_json.write_text(json.dumps({"generated_at": "2026-04-14T06:00:00-04:00"}), encoding="utf-8")
            markdown.write_text("# Surface\n", encoding="utf-8")

            with patch.object(data_model_governance_review, "DATA_MODEL_STATE_PATH", state_path), patch.object(
                data_model_governance_review, "DATA_MODEL_OUTPUT_PATH", output_path
            ), patch.object(
                data_model_governance_review,
                "SURFACES",
                [
                    {
                        "surface": "surface",
                        "state_json": state_json,
                        "operator_json": operator_json,
                        "markdown": markdown,
                    }
                ],
            ):
                payload = data_model_governance_review.build_data_model_governance_review()

            self.assertEqual(payload["issue_count"], 1)
            self.assertIn("out of sync", payload["surfaces"][0]["issues"][0].lower())
            self.assertTrue(output_path.exists())

    def test_documentation_governance_review_detects_missing_schedule(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "documentation_governance_review.json"
            output_path = root / "output" / "operator" / "documentation_governance_review.md"
            roadmap_path = root / "output" / "operator" / "master_roadmap.md"
            roadmap_path.parent.mkdir(parents=True, exist_ok=True)
            roadmap_path.write_text(
                "\n".join(
                    [
                        "# Master Roadmap",
                        "## Completed Major Work",
                        "## Active Operational Lanes",
                        "## Highest-Value Open Work",
                        "## Recommended Next 3 Steps",
                        "duck-documentation-governance",
                    ]
                ),
                encoding="utf-8",
            )
            schedule_path = root / "launchd" / "documentation_governance.plist"
            schedule_path.parent.mkdir(parents=True, exist_ok=True)

            with patch.object(documentation_governance_review, "DOCUMENTATION_GOVERNANCE_STATE_PATH", state_path), patch.object(
                documentation_governance_review, "DOCUMENTATION_GOVERNANCE_OUTPUT_PATH", output_path
            ), patch.object(
                documentation_governance_review,
                "CANONICAL_DOC_SPECS",
                [
                    {
                        "review_id": "master_roadmap",
                        "label": "Duck Ops master roadmap",
                        "path": roadmap_path,
                        "required_fragments": [
                            ("completed major work section", "## Completed Major Work"),
                            ("active operational lanes section", "## Active Operational Lanes"),
                            ("highest-value open work section", "## Highest-Value Open Work"),
                            ("recommended next steps section", "## Recommended Next 3 Steps"),
                        ],
                    }
                ],
            ), patch.object(
                documentation_governance_review,
                "LAUNCH_AGENT_SPECS",
                [
                    {
                        "review_id": "documentation_governance_weekly_schedule",
                        "label": "Weekly documentation governance schedule",
                        "path": schedule_path,
                    }
                ],
            ):
                payload = documentation_governance_review.build_documentation_governance_review()

            self.assertEqual(payload["issue_count"], 1)
            schedule_review = next(item for item in payload["reviews"] if item["review_kind"] == "local_schedule")
            self.assertIn("missing", schedule_review["issues"][0].lower())
            self.assertTrue(output_path.exists())


if __name__ == "__main__":
    unittest.main()

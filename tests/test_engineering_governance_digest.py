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

import engineering_governance_digest


class EngineeringGovernanceDigestTests(unittest.TestCase):
    def test_build_digest_captures_missing_skills_and_repo_status(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "engineering_governance_digest.json"
            output_path = root / "output" / "operator" / "engineering_governance_digest.md"
            health_path = root / "system_health.json"
            health_path.write_text(
                json.dumps(
                    {
                        "overall_status": "bad",
                        "overall_label": "Degraded",
                        "overall_summary": "Core operator health is degraded.",
                        "flow_health": [
                            {
                                "flow_id": "weekly_sale_monitor",
                                "label": "Weekly Sale Monitor",
                                "status": "bad",
                                "last_run_state": "stale_input",
                                "last_run_at": "2026-04-14T01:00:00-04:00",
                                "success_rate_label": "stale input",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(engineering_governance_digest, "DIGEST_STATE_PATH", state_path), patch.object(
                engineering_governance_digest, "DIGEST_OUTPUT_PATH", output_path
            ), patch.object(engineering_governance_digest, "SYSTEM_HEALTH_PATH", health_path), patch.object(
                engineering_governance_digest,
                "_skill_statuses",
                return_value=[
                    {"name": "duck-change-planner", "present": True},
                    {"name": "duck-reliability-review", "present": False},
                ],
            ), patch.object(
                engineering_governance_digest,
                "_repo_status",
                side_effect=[
                    {"repo": "duckAgent", "modified_count": 2, "untracked_count": 1, "status_lines": [" M src/main_agent.py"]},
                    {"repo": "duck-ops", "modified_count": 0, "untracked_count": 0, "status_lines": []},
                ],
            ):
                payload = engineering_governance_digest.build_engineering_governance_digest()

            self.assertEqual(payload["phase_focus"], "Phase 1: governance control layer (complete enough to use)")
            self.assertTrue(any("Missing skills" in item["summary"] for item in payload["findings"]))
            self.assertTrue(any("degraded" in item["summary"].lower() for item in payload["findings"]))
            self.assertTrue(state_path.exists())
            self.assertTrue(output_path.exists())

    def test_email_render_includes_findings(self) -> None:
        subject, text_body, html_body = engineering_governance_digest.render_engineering_governance_email(
            {
                "phase_focus": "Phase 1: governance control layer",
                "findings": [
                    {
                        "priority": "P1",
                        "kind": "observe",
                        "title": "Operator health is currently degraded",
                        "summary": "There are visible failures.",
                        "next_action": "Review the top bad flows first.",
                    }
                ],
                "skill_statuses": [{"name": "duck-change-planner", "present": False}],
                "health_findings": [{"label": "Weekly Sale Monitor"}],
            },
            render_report_email=lambda **kwargs: kwargs.get("body_html", ""),
            report_badge=lambda text, color: f"{text}:{color}",
            report_card=lambda _title, body, **kwargs: body,
            report_link=lambda href, label: f"{label}:{href}",
        )

        self.assertIn("engineering_governance", subject)
        self.assertIn("Operator health is currently degraded", text_body)
        self.assertIn("Review the top bad flows first.", html_body)


if __name__ == "__main__":
    unittest.main()

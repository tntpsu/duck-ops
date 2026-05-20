from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import refresh_operator_surfaces


class RefreshOperatorSurfacesTests(unittest.TestCase):
    def test_refresh_runs_core_steps_in_order_without_email_by_default(self) -> None:
        calls: list[str] = []

        def governance() -> dict:
            calls.append("governance")
            return {"generated_at": "2026-05-20T07:30:00-04:00", "findings": [], "review_recommendations": []}

        def roi(*, write_outputs: bool) -> dict:
            calls.append(f"roi:{write_outputs}")
            return {"generated_at": "2026-05-20T07:31:00-04:00", "summary": {"candidate_count": 1, "top_title": "Pilot"}}

        def desk() -> dict:
            calls.append("desk")
            return {
                "business_operator_desk": {
                    "generated_at": "2026-05-20T07:32:00-04:00",
                    "counts": {"roi_triage_candidates": 1},
                }
            }

        with patch.object(refresh_operator_surfaces, "build_engineering_governance_digest", side_effect=governance), patch.object(
            refresh_operator_surfaces, "send_engineering_governance_digest_email"
        ) as send_email, patch.object(refresh_operator_surfaces, "build_roi_triage", side_effect=roi), patch.object(
            refresh_operator_surfaces, "rebuild_customer_outputs", side_effect=desk
        ), patch.object(
            refresh_operator_surfaces,
            "build_repo_ci_status",
            side_effect=lambda **kwargs: calls.append("repo_ci") or {"generated_at": "now", "summary": {}},
        ), patch.object(
            refresh_operator_surfaces,
            "build_scheduler_health",
            side_effect=lambda **kwargs: calls.append("scheduler") or {"generated_at": "now", "summary": {}},
        ), patch.object(
            refresh_operator_surfaces,
            "build_dependency_health",
            side_effect=lambda **kwargs: calls.append("dependency") or {"generated_at": "now", "summary": {}},
        ), patch.object(
            refresh_operator_surfaces,
            "build_tech_debt_triage",
            side_effect=lambda: calls.append("tech_debt") or {"generated_at": "now", "summary": {}},
        ), patch.object(
            refresh_operator_surfaces,
            "build_reliability_review",
            side_effect=lambda: calls.append("reliability") or {"generated_at": "now", "summary": {}},
        ):
            payload = refresh_operator_surfaces.build_operator_surface_refresh(write_outputs=False)

        self.assertEqual(calls, ["repo_ci", "scheduler", "dependency", "tech_debt", "reliability", "governance", "roi:True", "desk"])
        send_email.assert_not_called()
        self.assertEqual(payload["status"], "ok")
        self.assertFalse(payload["send_email"])
        self.assertFalse(payload["run_repo_ci"])

    def test_refresh_can_run_repo_ci_before_governance(self) -> None:
        calls: list[str] = []

        with patch.object(
            refresh_operator_surfaces,
            "_run_repo_ci",
            side_effect=lambda repos: calls.append("repo_ci") or {"repos": repos},
        ), patch.object(
            refresh_operator_surfaces,
            "build_engineering_governance_digest",
            side_effect=lambda: calls.append("governance") or {"generated_at": "now", "findings": [], "review_recommendations": []},
        ), patch.object(
            refresh_operator_surfaces,
            "build_roi_triage",
            side_effect=lambda write_outputs: calls.append("roi") or {"generated_at": "now", "summary": {}},
        ), patch.object(
            refresh_operator_surfaces,
            "rebuild_customer_outputs",
            side_effect=lambda: calls.append("desk") or {"business_operator_desk": {"counts": {}}},
        ), patch.object(
            refresh_operator_surfaces,
            "build_scheduler_health",
            side_effect=lambda **kwargs: calls.append("scheduler") or {"generated_at": "now", "summary": {}},
        ), patch.object(
            refresh_operator_surfaces,
            "build_dependency_health",
            side_effect=lambda **kwargs: calls.append("dependency") or {"generated_at": "now", "summary": {}},
        ), patch.object(
            refresh_operator_surfaces,
            "build_tech_debt_triage",
            side_effect=lambda: calls.append("tech_debt") or {"generated_at": "now", "summary": {}},
        ), patch.object(
            refresh_operator_surfaces,
            "build_reliability_review",
            side_effect=lambda: calls.append("reliability") or {"generated_at": "now", "summary": {}},
        ):
            payload = refresh_operator_surfaces.build_operator_surface_refresh(
                run_repo_ci=True,
                repo_names=["duck-ops"],
                write_outputs=False,
            )

        self.assertEqual(calls, ["repo_ci", "scheduler", "dependency", "tech_debt", "reliability", "governance", "roi", "desk"])
        self.assertEqual(payload["repo_names"], ["duck-ops"])
        self.assertTrue(payload["run_repo_ci"])

    def test_refresh_stops_when_governance_fails(self) -> None:
        with patch.object(
            refresh_operator_surfaces,
            "build_engineering_governance_digest",
            side_effect=RuntimeError("governance unavailable"),
        ), patch.object(refresh_operator_surfaces, "build_roi_triage") as roi, patch.object(
            refresh_operator_surfaces, "rebuild_customer_outputs"
        ) as desk, patch.object(
            refresh_operator_surfaces,
            "build_repo_ci_status",
            return_value={"generated_at": "now", "summary": {}},
        ), patch.object(
            refresh_operator_surfaces,
            "build_scheduler_health",
            return_value={"generated_at": "now", "summary": {}},
        ), patch.object(
            refresh_operator_surfaces,
            "build_dependency_health",
            return_value={"generated_at": "now", "summary": {}},
        ), patch.object(
            refresh_operator_surfaces,
            "build_tech_debt_triage",
            return_value={"generated_at": "now", "summary": {}},
        ), patch.object(
            refresh_operator_surfaces,
            "build_reliability_review",
            return_value={"generated_at": "now", "summary": {}},
        ):
            payload = refresh_operator_surfaces.build_operator_surface_refresh(write_outputs=False)

        self.assertEqual(payload["status"], "failed")
        self.assertEqual(
            [step["name"] for step in payload["steps"]],
            [
                "repo_ci_status",
                "scheduler_health",
                "dependency_health",
                "tech_debt_triage",
                "reliability_review",
                "engineering_governance_digest",
            ],
        )
        roi.assert_not_called()
        desk.assert_not_called()

    def test_markdown_summarizes_steps_and_outputs(self) -> None:
        markdown = refresh_operator_surfaces.render_operator_surface_refresh_markdown(
            {
                "generated_at": "2026-05-20T07:30:00-04:00",
                "status": "ok",
                "send_email": False,
                "run_repo_ci": False,
                "steps": [
                    {
                        "name": "roi_triage",
                        "status": "ok",
                        "duration_seconds": 0.2,
                        "details": {"top_title": "Pilot"},
                    }
                ],
                "outputs": {"roi_triage": "/tmp/roi.md"},
            }
        )

        self.assertIn("# Operator Surface Refresh", markdown)
        self.assertIn("`roi_triage`: `ok`", markdown)
        self.assertIn("Top ROI: Pilot", markdown)
        self.assertIn("/tmp/roi.md", markdown)


if __name__ == "__main__":
    unittest.main()

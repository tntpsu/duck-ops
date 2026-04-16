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
    def test_competitor_snapshot_status_classifies_live_cached_and_hard_failing(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "competitor_social_snapshots.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)

            live_payload = {
                "generated_at": "2026-04-15T10:00:00-04:00",
                "summary": {
                    "collected_account_count": 3,
                    "live_account_count": 3,
                    "cached_account_count": 0,
                    "failed_account_count": 0,
                    "degraded_account_count": 0,
                },
            }
            cached_payload = {
                "generated_at": "2026-04-15T10:00:00-04:00",
                "summary": {
                    "collected_account_count": 3,
                    "live_account_count": 2,
                    "cached_account_count": 1,
                    "failed_account_count": 0,
                    "degraded_account_count": 1,
                },
            }
            staggered_payload = {
                "generated_at": "2026-04-15T10:00:00-04:00",
                "summary": {
                    "collected_account_count": 3,
                    "live_account_count": 1,
                    "cached_account_count": 2,
                    "failed_account_count": 0,
                    "degraded_account_count": 0,
                    "scheduled_skip_account_count": 2,
                    "active_refresh_target_count": 1,
                },
            }
            profile_only_backoff_payload = {
                "generated_at": "2026-04-15T10:00:00-04:00",
                "summary": {
                    "collected_account_count": 3,
                    "live_account_count": 1,
                    "cached_account_count": 2,
                    "failed_account_count": 0,
                    "degraded_account_count": 0,
                    "scheduled_skip_account_count": 2,
                    "profile_only_backoff_account_count": 1,
                    "active_refresh_target_count": 1,
                },
            }
            canary_payload = {
                "generated_at": "2026-04-15T10:00:00-04:00",
                "summary": {
                    "collected_account_count": 3,
                    "live_account_count": 0,
                    "cached_account_count": 3,
                    "failed_account_count": 0,
                    "degraded_account_count": 0,
                    "scheduled_skip_account_count": 3,
                    "live_canary_limited_account_count": 2,
                    "live_canary_target_count": 1,
                    "max_live_canary_targets": 1,
                    "active_refresh_target_count": 1,
                },
            }
            hard_failed_payload = {
                "generated_at": "2026-04-15T10:00:00-04:00",
                "summary": {
                    "collected_account_count": 0,
                    "live_account_count": 0,
                    "cached_account_count": 0,
                    "failed_account_count": 2,
                    "degraded_account_count": 2,
                },
            }

            with patch.object(engineering_governance_digest, "COMPETITOR_SOCIAL_SNAPSHOTS_PATH", state_path), patch.object(
                engineering_governance_digest, "age_hours", return_value=1.5
            ):
                for payload, expected_key, expected_label in [
                    (live_payload, "healthy_live", "HEALTHY LIVE"),
                    (cached_payload, "degraded_cached_fallback", "DEGRADED CACHED FALLBACK"),
                    (staggered_payload, "healthy_staggered", "HEALTHY STAGGERED"),
                    (profile_only_backoff_payload, "degraded_cached_fallback", "DEGRADED CACHED FALLBACK"),
                    (canary_payload, "healthy_staggered", "HEALTHY STAGGERED"),
                    (hard_failed_payload, "hard_failing", "HARD FAILING"),
                ]:
                    state_path.write_text(json.dumps(payload), encoding="utf-8")
                    status = engineering_governance_digest._competitor_social_snapshot_status()
                    self.assertEqual(status["status_key"], expected_key)
                    self.assertEqual(status["status_label"], expected_label)
                    self.assertIn("generated 1.5h ago", status["summary"])

                state_path.unlink()
                missing_status = engineering_governance_digest._competitor_social_snapshot_status()
                self.assertEqual(missing_status["status_key"], "hard_failing")
                self.assertFalse(missing_status["present"])

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

    def test_build_digest_surfaces_competitor_snapshot_status(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            digest_state_path = root / "state" / "engineering_governance_digest.json"
            output_path = root / "output" / "operator" / "engineering_governance_digest.md"
            competitor_state_path = root / "state" / "competitor_social_snapshots.json"
            competitor_state_path.parent.mkdir(parents=True, exist_ok=True)
            competitor_state_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-15T10:00:00-04:00",
                        "summary": {
                            "collected_account_count": 2,
                            "live_account_count": 2,
                            "cached_account_count": 0,
                            "failed_account_count": 0,
                            "degraded_account_count": 0,
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(engineering_governance_digest, "DIGEST_STATE_PATH", digest_state_path), patch.object(
                engineering_governance_digest, "DIGEST_OUTPUT_PATH", output_path
            ), patch.object(
                engineering_governance_digest, "COMPETITOR_SOCIAL_SNAPSHOTS_PATH", competitor_state_path
            ), patch.object(
                engineering_governance_digest,
                "_skill_statuses",
                return_value=[
                    {"name": "duck-change-planner", "present": True},
                    {"name": "duck-reliability-review", "present": True},
                ],
            ), patch.object(
                engineering_governance_digest,
                "_repo_status",
                side_effect=[
                    {"repo": "duckAgent", "modified_count": 0, "untracked_count": 0, "status_lines": []},
                    {"repo": "duck-ops", "modified_count": 0, "untracked_count": 0, "status_lines": []},
                ],
            ), patch.object(
                engineering_governance_digest, "_top_health_findings", return_value=({"overall_status": "ok"}, [])
            ):
                payload = engineering_governance_digest.build_engineering_governance_digest()

            competitor_review = next(
                item for item in payload["observe_review_statuses"] if item["name"] == "competitor_social_snapshots"
            )
            self.assertEqual(competitor_review["status_key"], "healthy_live")
            self.assertEqual(competitor_review["status_label"], "HEALTHY LIVE")
            self.assertIn("Collector is live", competitor_review["summary"])
            self.assertTrue(digest_state_path.exists())
            self.assertTrue(output_path.exists())
            self.assertIn("HEALTHY LIVE", output_path.read_text(encoding="utf-8"))

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

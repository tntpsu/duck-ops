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
    def test_review_recommendations_merge_observe_only_reviews(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            tech_debt_path = root / "state" / "tech_debt_triage.json"
            reliability_path = root / "state" / "reliability_review.json"
            data_model_path = root / "state" / "data_model_governance_review.json"
            documentation_path = root / "state" / "documentation_governance_review.json"
            tech_debt_path.parent.mkdir(parents=True, exist_ok=True)
            tech_debt_path.write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "priority": "P1",
                                "title": "Review Execution debt review",
                                "symptom": "Review Execution is reporting bad with state `failed`.",
                                "root_cause": "Health is noisy until the lane is hardened.",
                                "recommended_fix_type": "reliability hardening",
                                "suggested_owner_skill": "duck-tech-debt-triage",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            reliability_path.write_text(
                json.dumps(
                    {
                        "reviews": [
                            {
                                "label": "Review Execution",
                                "status": "bad",
                                "go_decision": "no-go",
                                "lane_summary": "Review Execution is currently `bad` with last run state `failed`.",
                                "required_rollout_fixes": ["Add clearer retry and recovery receipts."],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            data_model_path.write_text(
                json.dumps(
                    {
                        "surfaces": [
                            {
                                "surface": "business_operator_desk",
                                "issues": ["State and operator JSON are out of sync."],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            documentation_path.write_text(
                json.dumps(
                    {
                        "reviews": [
                            {
                                "review_kind": "canonical_doc",
                                "label": "Roadmap execution sequence",
                                "exists": True,
                                "issues": ["Required canonical coverage is missing: weekly documentation cadence."],
                                "recommended_updates": ["Update the canonical document and any dependent guidance so the roadmap/policy/runbook truth stays aligned."],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(engineering_governance_digest, "TECH_DEBT_TRIAGE_PATH", tech_debt_path), patch.object(
                engineering_governance_digest, "RELIABILITY_REVIEW_PATH", reliability_path
            ), patch.object(
                engineering_governance_digest, "DATA_MODEL_GOVERNANCE_REVIEW_PATH", data_model_path
            ), patch.object(
                engineering_governance_digest, "DOCUMENTATION_GOVERNANCE_REVIEW_PATH", documentation_path
            ):
                recommendations = engineering_governance_digest._review_recommendations()

        self.assertEqual(len(recommendations), 4)
        self.assertEqual(recommendations[0]["priority"], "P1")
        self.assertTrue(any(item["source"] == "tech_debt_triage" for item in recommendations))
        self.assertTrue(any(item["source"] == "reliability_review" for item in recommendations))
        self.assertTrue(any(item["source"] == "data_model_governance_review" for item in recommendations))
        self.assertTrue(any(item["source"] == "documentation_governance_review" for item in recommendations))

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

    def test_build_digest_surfaces_business_desk_highlights(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            digest_state_path = root / "state" / "engineering_governance_digest.json"
            output_path = root / "output" / "operator" / "engineering_governance_digest.md"
            business_desk_path = root / "state" / "business_operator_desk.json"
            business_desk_path.parent.mkdir(parents=True, exist_ok=True)
            business_desk_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-16T08:00:00-04:00",
                        "counts": {
                            "customer_attention_items": 4,
                            "orders_to_pack_units": 3,
                            "review_queue_items": 1,
                            "governance_top_priority_items": 2,
                        },
                        "next_actions": [
                            {
                                "lane": "packing",
                                "title": "Orange Cat Duck",
                                "summary": "Aging order waiting to be packed.",
                                "command": "Pack this duck tonight.",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(engineering_governance_digest, "DIGEST_STATE_PATH", digest_state_path), patch.object(
                engineering_governance_digest, "DIGEST_OUTPUT_PATH", output_path
            ), patch.object(
                engineering_governance_digest, "BUSINESS_OPERATOR_DESK_PATH", business_desk_path
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

            desk = payload["business_desk_highlights"]
            self.assertTrue(desk["available"])
            self.assertEqual(desk["counts"][0]["label"], "Customer attention")
            self.assertEqual(desk["counts"][0]["count"], 4)
            self.assertEqual(desk["next_actions"][0]["lane"], "packing")
            self.assertIn("Business Desk Highlights", output_path.read_text(encoding="utf-8"))

    def test_build_digest_surfaces_learning_change_highlights(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            digest_state_path = root / "state" / "engineering_governance_digest.json"
            output_path = root / "output" / "operator" / "engineering_governance_digest.md"
            current_learnings_path = root / "state" / "current_learnings.json"
            current_learnings_path.parent.mkdir(parents=True, exist_ok=True)
            current_learnings_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-16T08:00:00-04:00",
                        "changes_since_previous": [
                            {"source": "weekly_strategy", "kind": "weekly_strategy_slot_missed", "headline": "Slot 3 has no observed post yet."}
                        ],
                        "change_notifier": {
                            "available": True,
                            "headline": "1 attention-level learning change needs review in the next planning pass.",
                            "change_count": 1,
                            "material_change_count": 1,
                            "attention_change_count": 1,
                            "recommended_action": "review current_learnings + weekly_strategy_recommendation_packet",
                            "items": [
                                {
                                    "source": "weekly_strategy",
                                    "kind": "weekly_strategy_slot_missed",
                                    "urgency": "attention",
                                    "headline": "Slot 3 has no observed post yet.",
                                }
                            ],
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(engineering_governance_digest, "DIGEST_STATE_PATH", digest_state_path), patch.object(
                engineering_governance_digest, "DIGEST_OUTPUT_PATH", output_path
            ), patch.object(
                engineering_governance_digest, "CURRENT_LEARNINGS_PATH", current_learnings_path
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

            learning_changes = payload["learning_change_highlights"]
            self.assertTrue(learning_changes["available"])
            self.assertEqual(learning_changes["material_change_count"], 1)
            self.assertEqual(learning_changes["items"][0]["urgency"], "attention")
            self.assertIn("Learning Change Highlights", output_path.read_text(encoding="utf-8"))

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

    def test_email_render_includes_business_desk_highlights(self) -> None:
        subject, text_body, html_body = engineering_governance_digest.render_engineering_governance_email(
            {
                "phase_focus": "Phase 1: governance control layer",
                "findings": [],
                "skill_statuses": [],
                "health_findings": [],
                "business_desk_highlights": {
                    "available": True,
                    "generated_at": "2026-04-16T08:00:00-04:00",
                    "counts": [
                        {"label": "Customer attention", "count": 4},
                        {"label": "Pack tonight", "count": 3},
                    ],
                    "next_actions": [
                        {
                            "lane": "packing",
                            "title": "Orange Cat Duck",
                            "summary": "Aging order waiting to be packed.",
                            "command": "Pack this duck tonight.",
                        }
                    ],
                },
            },
            render_report_email=lambda **kwargs: kwargs.get("body_html", ""),
            report_badge=lambda text, color: f"{text}:{color}",
            report_card=lambda title, body, **kwargs: f"{title}\n{body}",
            report_link=lambda href, label: f"{label}:{href}",
        )

        self.assertIn("engineering_governance", subject)
        self.assertIn("Business desk highlights:", text_body)
        self.assertIn("Customer attention: 4", text_body)
        self.assertIn("Business Desk Highlights", html_body)
        self.assertIn("Orange Cat Duck", html_body)

    def test_email_render_includes_learning_change_highlights(self) -> None:
        subject, text_body, html_body = engineering_governance_digest.render_engineering_governance_email(
            {
                "phase_focus": "Phase 1: governance control layer",
                "findings": [],
                "skill_statuses": [],
                "health_findings": [],
                "learning_change_highlights": {
                    "available": True,
                    "headline": "1 attention-level learning change needs review in the next planning pass.",
                    "change_count": 2,
                    "material_change_count": 1,
                    "attention_change_count": 1,
                    "recommended_action": "review current_learnings + weekly_strategy_recommendation_packet",
                    "items": [
                        {
                            "source": "weekly_strategy",
                            "urgency": "attention",
                            "headline": "Slot 3 has no observed post yet.",
                        }
                    ],
                },
            },
            render_report_email=lambda **kwargs: kwargs.get("body_html", ""),
            report_badge=lambda text, color: f"{text}:{color}",
            report_card=lambda title, body, **kwargs: f"{title}\n{body}",
            report_link=lambda href, label: f"{label}:{href}",
        )

        self.assertIn("engineering_governance", subject)
        self.assertIn("Learning change highlights:", text_body)
        self.assertIn("Material changes: 1", text_body)
        self.assertIn("Learning Change Highlights", html_body)
        self.assertIn("Slot 3 has no observed post yet.", html_body)


if __name__ == "__main__":
    unittest.main()

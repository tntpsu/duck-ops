from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from business_operator_desk import (
    build_business_operator_desk,
    render_business_operator_desk_markdown,
    render_business_section,
)


class BusinessOperatorDeskTests(unittest.TestCase):
    def test_operator_desk_shortens_pack_and_sale_titles(self) -> None:
        payload = build_business_operator_desk(
            customer_packets={"items": []},
            nightly_summary={
                "counts": {"orders_to_pack_units": 3},
                "sections": {
                    "orders_to_pack": [
                        {
                            "product_title": "Dachshund Duck Rubber Duck Figurine Gift for Dog Lovers Desk Decor",
                            "urgency_label": "Today",
                            "order_count": 2,
                            "buyer_count": 2,
                            "total_quantity": 3,
                            "by_channel": {"etsy": 1, "shopify": 2},
                        }
                    ]
                },
            },
            etsy_browser_sync={"items": []},
            custom_build_candidates={"items": []},
            print_queue_candidates=[],
            weekly_sale_monitor={
                "items": [
                    {
                        "product_title": "Dachshund Duck Rubber Duck Figurine Gift for Dog Lovers Desk Decor",
                        "discount": "15% off",
                        "effectiveness": "weak",
                        "sales_7d": 1,
                        "sales_30d": 3,
                        "marketing_recommendation": "Try a simpler hero angle.",
                        "recommendation": "Rotate or rewrite.",
                    }
                ]
            },
            review_queue={"items": []},
        )

        next_actions = payload.get("next_actions") or []
        packing_action = next(item for item in next_actions if item.get("lane") == "packing")
        weekly_action = next(item for item in next_actions if item.get("lane") == "weekly_sale")

        self.assertEqual(packing_action.get("title"), "Dachshund Duck")
        self.assertEqual(weekly_action.get("title"), "Dachshund Duck")

    def test_operator_desk_markdown_shortens_visible_titles(self) -> None:
        markdown = render_business_operator_desk_markdown(
            {
                "generated_at": "2026-04-11T21:00:00-04:00",
                "counts": {
                    "customer_attention_items": 0,
                    "replacement_labels_now": 0,
                    "etsy_browser_threads": 0,
                    "threads_with_staged_reply": 0,
                    "threads_waiting_on_customer": 0,
                    "custom_build_candidates": 0,
                    "custom_build_tasks_live": 0,
                    "orders_to_pack_units": 3,
                    "stock_print_candidates": 1,
                    "active_weekly_sale_items": 1,
                    "weak_weekly_sale_items": 1,
                    "review_queue_items": 0,
                    "review_queue_backlog": 0,
                    "usps_live_customer_items": 0,
                },
                "next_actions": [],
                "sections": {
                    "customer_packets": [],
                    "etsy_browser_threads": [],
                    "custom_build_candidates": [],
                    "orders_to_pack": [
                        {
                            "product_title": "Dachshund Duck Rubber Duck Figurine Gift for Dog Lovers Desk Decor",
                            "urgency_label": "Today",
                            "total_quantity": 3,
                            "by_channel": {"etsy": 1, "shopify": 2},
                        }
                    ],
                    "stock_print_candidates": [
                        {
                            "product_title": "Michigan Wolverines Duck – Officially Licensed Duck with Team Spirit & M Pride",
                            "priority": "high",
                            "recent_demand": 4,
                            "why_now": "Inventory is low.",
                        }
                    ],
                    "weekly_sale_monitor": [
                        {
                            "product_title": "Dachshund Duck Rubber Duck Figurine Gift for Dog Lovers Desk Decor",
                            "discount": "15% off",
                            "effectiveness": "weak",
                            "sales_7d": 1,
                            "sales_30d": 3,
                            "recommendation": "Rotate or rewrite.",
                            "marketing_recommendation": "Try a simpler hero angle.",
                        }
                    ],
                    "review_queue": [],
                },
            }
        )

        self.assertIn("Dachshund Duck", markdown)
        self.assertNotIn("Gift for Dog Lovers Desk Decor", markdown)
        self.assertIn("Michigan Wolverines Duck", markdown)
        self.assertNotIn("Officially Licensed Duck with Team Spirit", markdown)

    def test_operator_desk_packing_summary_uses_unknown_buyer_display(self) -> None:
        payload = build_business_operator_desk(
            customer_packets={"items": []},
            nightly_summary={
                "counts": {"orders_to_pack_units": 1},
                "sections": {
                    "orders_to_pack": [
                        {
                            "product_title": "Patrick Star Duck – Goofy Underwater Duck Collectible",
                            "urgency_label": "Open",
                            "order_count": 1,
                            "buyer_count": 0,
                            "buyer_count_display": "Hidden by Shopify",
                            "total_quantity": 1,
                            "by_channel": {"etsy": 0, "shopify": 1},
                        }
                    ]
                },
            },
            etsy_browser_sync={"items": []},
            custom_build_candidates={"items": []},
            print_queue_candidates=[],
            weekly_sale_monitor={"items": []},
            review_queue={"items": []},
        )

        packing_action = next(item for item in (payload.get("next_actions") or []) if item.get("lane") == "packing")
        self.assertIn("Hidden by Shopify buyer", packing_action.get("summary") or "")

    def test_operator_desk_pack_section_shows_choices(self) -> None:
        markdown = render_business_operator_desk_markdown(
            {
                "generated_at": "2026-04-11T21:00:00-04:00",
                "counts": {"orders_to_pack_units": 2},
                "next_actions": [],
                "sections": {
                    "customer_packets": [],
                    "etsy_browser_threads": [],
                    "custom_build_candidates": [],
                    "orders_to_pack": [
                        {
                            "product_title": "Duckzilla Monster Duck",
                            "urgency_label": "Open",
                            "total_quantity": 2,
                            "buyer_count_display": "1",
                            "option_summary": "Color: Blue, Color: Pink",
                            "by_channel": {"etsy": 2, "shopify": 0},
                        }
                    ],
                    "stock_print_candidates": [],
                    "weekly_sale_monitor": [],
                    "review_queue": [],
                    "workflow_followthrough": [],
                },
            }
        )

        self.assertIn("Choices: Color: Blue, Color: Pink", markdown)

    def test_operator_desk_includes_workflow_followthrough(self) -> None:
        payload = build_business_operator_desk(
            customer_packets={"items": []},
            nightly_summary={"counts": {}, "sections": {}},
            etsy_browser_sync={"items": []},
            custom_build_candidates={"items": []},
            print_queue_candidates=[],
            weekly_sale_monitor={"items": []},
            review_queue={"items": []},
            workflow_followthrough=[
                {
                    "lane": "weekly",
                    "title": "Spring Ducks",
                    "summary": "stale input | article 123",
                    "next_action": "Refresh the weekly draft",
                }
            ],
        )

        markdown = render_business_operator_desk_markdown(payload)

        self.assertEqual(payload["counts"]["workflow_followthrough_items"], 1)
        self.assertIn("## Workflow Follow-Through", markdown)
        self.assertIn("weekly: Spring Ducks", markdown)
        self.assertIn("Refresh the weekly draft", markdown)

    def test_operator_desk_surfaces_scheduler_health_attention(self) -> None:
        with patch(
            "business_operator_desk._load_scheduler_health_surface",
            return_value={
                "available": True,
                "path": "/tmp/scheduler_health.md",
                "source": "duckagent_launchd_scheduler",
                "status": "bad",
                "headline": "Scheduler health needs attention.",
                "recommended_action": "Resolve the stuck run.",
                "tracked_jobs": 2,
                "attention_count": 1,
                "bad_count": 1,
                "warn_count": 0,
                "items": [
                    {
                        "job_name": "jeepfact_wednesday",
                        "status": "hung",
                        "severity": "bad",
                        "summary": "Run started 1h ago and has not finished.",
                        "recommended_action": "Inspect the PID and scheduler log.",
                        "attention_needed": True,
                        "last_start_at": "2026-04-22T09:00:00-04:00",
                        "expected_at": "2026-04-22T09:00:00-04:00",
                        "log_path": "/tmp/duckagent_scheduler.log",
                    }
                ],
            },
        ):
            payload = build_business_operator_desk(
                customer_packets={"items": []},
                nightly_summary={"counts": {}, "sections": {}},
                etsy_browser_sync={"items": []},
                custom_build_candidates={"items": []},
                print_queue_candidates=[],
                weekly_sale_monitor={"items": []},
                review_queue={"items": []},
            )

        markdown = render_business_operator_desk_markdown(payload)
        next_actions = payload.get("next_actions") or []

        self.assertEqual(payload["counts"]["scheduler_attention_jobs"], 1)
        self.assertIn("## Scheduler Health", markdown)
        self.assertIn("jeepfact_wednesday", markdown)
        self.assertTrue(any(item.get("lane") == "scheduler_health" for item in next_actions))

    def test_operator_desk_workflow_followthrough_shows_root_cause(self) -> None:
        payload = build_business_operator_desk(
            customer_packets={"items": []},
            nightly_summary={"counts": {}, "sections": {}},
            etsy_browser_sync={"items": []},
            custom_build_candidates={"items": []},
            print_queue_candidates=[],
            weekly_sale_monitor={"items": []},
            review_queue={"items": []},
            workflow_followthrough=[
                {
                    "lane": "meme",
                    "title": "Meme 2026-04-06",
                    "summary": "execution failed",
                    "root_cause": "Facebook object id is invalid.",
                    "fix_hint": "Fix the Meta target.",
                    "next_action": "Retry publish",
                }
            ],
        )

        markdown = render_business_operator_desk_markdown(payload)

        self.assertIn("Why: Facebook object id is invalid.", markdown)
        self.assertIn("Fix: Fix the Meta target.", markdown)

    def test_operator_desk_surfaces_engineering_governance_digest(self) -> None:
        with patch(
            "business_operator_desk._load_governance_surface",
            return_value={
                "available": True,
                "path": "/tmp/engineering_governance_digest.md",
                "phase_focus": "Phase 2: observe-only engineering reviews",
                "next_step": "Run the top reliability recommendation before scheduling more overnight review work.",
                "finding_count": 2,
                "recommendation_count": 2,
                "top_priority_count": 1,
                "findings": [
                    {
                        "priority": "P1",
                        "title": "Operator health is currently degraded",
                        "summary": "Review execution is still failing.",
                    }
                ],
                "recommendations": [
                    {
                        "priority": "P1",
                        "source": "reliability_review",
                        "mode": "observe-only",
                        "title": "Review Execution rollout guardrail",
                        "summary": "Review Execution is currently `bad` with last run state `failed`.",
                        "next_action": "Add clearer retry and recovery receipts.",
                        "recommendation_type": "reliability hardening",
                        "suggested_owner_skill": "duck-reliability-review",
                    },
                    {
                        "priority": "P2",
                        "source": "tech_debt_triage",
                        "mode": "propose-only",
                        "title": "Weekly Sale Monitor debt review",
                        "summary": "Weekly Sale Monitor is reporting warn with state `weak_items_present`.",
                        "next_action": "Workflow cleanup via duck-tech-debt-triage.",
                        "recommendation_type": "workflow cleanup",
                        "suggested_owner_skill": "duck-tech-debt-triage",
                    },
                ],
            },
        ), patch(
            "business_operator_desk._load_learning_surface",
            return_value={"available": False, "path": "/tmp/current_learnings.md", "items": [], "change_count": 0, "idea_count": 0},
        ), patch(
            "business_operator_desk._load_weekly_strategy_packet",
            return_value={"available": False, "path": "/tmp/weekly_strategy_recommendation_packet.md", "recommendations": [], "watchouts": [], "social_plan": {}},
        ), patch(
            "business_operator_desk._load_seo_outcome_surface",
            return_value={"available": False, "path": "/tmp/shopify_seo_outcomes.md", "attention_items": [], "recent_wins": []},
        ):
            payload = build_business_operator_desk(
                customer_packets={"items": []},
                nightly_summary={"counts": {}, "sections": {}},
                etsy_browser_sync={"items": []},
                custom_build_candidates={"items": []},
                print_queue_candidates=[],
                weekly_sale_monitor={"items": []},
                review_queue={"items": []},
                workflow_followthrough=[],
            )

        markdown = render_business_operator_desk_markdown(payload)
        governance_section = render_business_section(payload, "governance")
        self.assertEqual(payload["counts"]["governance_recommendations"], 2)
        self.assertEqual(payload["counts"]["governance_top_priority_items"], 1)
        governance_action = next(item for item in (payload.get("next_actions") or []) if item.get("lane") == "engineering_governance")
        self.assertEqual(governance_action["title"], "Review Execution rollout guardrail")
        self.assertIn("duck-reliability-review", governance_action["secondary_command"])
        self.assertIn("## Engineering Governance", markdown)
        self.assertIn("Review Execution rollout guardrail", markdown)
        self.assertIn("Duck Ops Engineering Governance", governance_section)
        self.assertIn("Top-priority recommendations: 1", governance_section)

    def test_operator_desk_surfaces_repo_ci_status(self) -> None:
        with patch(
            "business_operator_desk._load_repo_ci_surface",
            return_value={
                "available": True,
                "path": "/tmp/repo_ci_status.md",
                "source": "local_repo_ci_mirror",
                "headline": "1 repo CI mirror result is behind the current commit.",
                "recommended_action": "Rerun the local CI mirror for the repo that moved after the last check.",
                "repo_count": 2,
                "attention_count": 1,
                "failing_count": 0,
                "dirty_count": 0,
                "outdated_count": 1,
                "not_run_count": 0,
                "stale_count": 0,
                "passed_count": 1,
                "items": [
                    {
                        "repo": "duckAgent",
                        "status": "outdated",
                        "status_label": "OUTDATED",
                        "headline": "duckAgent has new commits after the last CI mirror result.",
                        "summary": "clean workspace | last check covered `abc1234` but current head is `def5678`",
                        "recommended_action": "Rerun `python3 runtime/repo_ci_status.py --run-checks --repo duckAgent` so the CI mirror matches the current head.",
                        "status_source_note": "Private repo: business desk reflects the local mirror.",
                        "attention_needed": True,
                        "rerun_command": "python3 runtime/repo_ci_status.py --run-checks --repo duckAgent",
                        "branch": "codex/test-agent",
                        "head_sha_short": "def5678",
                        "check_finished_at": "2026-04-20T20:00:03-04:00",
                    },
                    {
                        "repo": "duck-ops",
                        "status": "passed",
                        "status_label": "PASSED",
                        "headline": "duck-ops local CI mirror is green.",
                        "summary": "clean workspace | Compiled 47 Python files.",
                        "recommended_action": "No immediate action is needed.",
                        "status_source_note": "Public repo: business desk still uses the local mirror.",
                        "attention_needed": False,
                        "rerun_command": "python3 runtime/repo_ci_status.py --run-checks --repo duck-ops",
                        "branch": "codex/test-ops",
                        "head_sha_short": "aaa1111",
                        "check_finished_at": "2026-04-20T20:00:05-04:00",
                    },
                ],
            },
        ), patch(
            "business_operator_desk._load_learning_surface",
            return_value={"available": False, "path": "/tmp/current_learnings.md", "items": [], "change_count": 0, "idea_count": 0},
        ), patch(
            "business_operator_desk._load_weekly_strategy_packet",
            return_value={"available": False, "path": "/tmp/weekly_strategy_recommendation_packet.md", "recommendations": [], "watchouts": [], "social_plan": {}},
        ), patch(
            "business_operator_desk._load_seo_outcome_surface",
            return_value={"available": False, "path": "/tmp/shopify_seo_outcomes.md", "attention_items": [], "recent_wins": []},
        ), patch(
            "business_operator_desk._load_governance_surface",
            return_value={"available": False, "path": "/tmp/engineering_governance_digest.md", "findings": [], "recommendations": []},
        ):
            payload = build_business_operator_desk(
                customer_packets={"items": []},
                nightly_summary={"counts": {}, "sections": {}},
                etsy_browser_sync={"items": []},
                custom_build_candidates={"items": []},
                print_queue_candidates=[],
                weekly_sale_monitor={"items": []},
                review_queue={"items": []},
                workflow_followthrough=[],
            )

        markdown = render_business_operator_desk_markdown(payload)
        ci_section = render_business_section(payload, "ci")
        self.assertEqual(payload["counts"]["repo_ci_tracked_repos"], 2)
        self.assertEqual(payload["counts"]["repo_ci_attention_items"], 1)
        self.assertEqual(payload["counts"]["repo_ci_failing_repos"], 0)
        action = next(item for item in (payload.get("next_actions") or []) if item.get("lane") == "repo_ci")
        self.assertIn("duckAgent", action["summary"])
        self.assertIn("OUTDATED", action["summary"])
        self.assertEqual(action["command"], "python3 runtime/repo_ci_status.py --run-checks --repo duckAgent")
        self.assertIn("## Repo CI Status", markdown)
        self.assertIn("duckAgent | `OUTDATED`", markdown)
        self.assertIn("local_repo_ci_mirror", markdown)
        self.assertIn("Duck Ops Repo CI Status", ci_section)
        self.assertIn("Need attention: 1", ci_section)
        self.assertIn("Private repo: business desk reflects the local mirror.", ci_section)

    def test_operator_desk_surfaces_roi_triage(self) -> None:
        with patch(
            "business_operator_desk._load_roi_triage_surface",
            return_value={
                "available": True,
                "path": "/tmp/roi_triage.md",
                "generated_at": "2026-04-26T08:00:00-04:00",
                "candidate_count": 2,
                "top_priority_count": 1,
                "top_score": 4.4,
                "headline": "Top ROI slice: Semantic visual QA.",
                "recommended_action": "Run the checker.",
                "recommendations": [
                    {
                        "rank": 1,
                        "title": "Semantic visual QA",
                        "why_now": "Image drift needs a real gate.",
                        "recommended_next_slice": "Run the checker.",
                        "score_breakdown": {"roi_score": 4.4, "impact": 5},
                        "owner_skill": "duck-reliability-review",
                    }
                ],
            },
        ):
            payload = build_business_operator_desk(
                customer_packets={"items": []},
                nightly_summary={"counts": {}, "sections": {}},
                etsy_browser_sync={"items": []},
                custom_build_candidates={"items": []},
                print_queue_candidates=[],
                weekly_sale_monitor={"items": []},
                review_queue={"items": []},
                workflow_followthrough=[],
            )

        markdown = render_business_operator_desk_markdown(payload)
        roi_section = render_business_section(payload, "roi")
        action = next(item for item in (payload.get("next_actions") or []) if item.get("lane") == "roi_triage")

        self.assertEqual(payload["counts"]["roi_triage_candidates"], 2)
        self.assertEqual(payload["counts"]["roi_triage_top_priority_items"], 1)
        self.assertEqual(action["title"], "Semantic visual QA")
        self.assertIn("## ROI Triage", markdown)
        self.assertIn("Semantic visual QA", roi_section)

    def test_operator_desk_surfaces_maintenance_freshness(self) -> None:
        with patch(
            "business_operator_desk._load_maintenance_freshness_surface",
            return_value={
                "available": True,
                "generated_at": "2026-04-26T08:00:00-04:00",
                "item_count": 2,
                "attention_count": 1,
                "stale_count": 1,
                "missing_count": 0,
                "unknown_count": 0,
                "headline": "1 maintenance surface is stale.",
                "recommended_action": "Refresh the stale producer.",
                "items": [
                    {
                        "surface_id": "roi_triage",
                        "label": "ROI triage",
                        "status": "stale",
                        "attention_needed": True,
                        "age_hours": 55.0,
                        "max_age_hours": 48,
                        "summary": "Last update is stale.",
                        "recommended_action": "Refresh ROI triage.",
                        "path": "/tmp/roi_triage.md",
                    }
                ],
            },
        ):
            payload = build_business_operator_desk(
                customer_packets={"items": []},
                nightly_summary={"counts": {}, "sections": {}},
                etsy_browser_sync={"items": []},
                custom_build_candidates={"items": []},
                print_queue_candidates=[],
                weekly_sale_monitor={"items": []},
                review_queue={"items": []},
                workflow_followthrough=[],
            )

        markdown = render_business_operator_desk_markdown(payload)
        freshness_section = render_business_section(payload, "freshness")
        action = next(item for item in (payload.get("next_actions") or []) if item.get("lane") == "maintenance_freshness")

        self.assertEqual(payload["counts"]["maintenance_freshness_attention_items"], 1)
        self.assertEqual(payload["counts"]["maintenance_freshness_stale_items"], 1)
        self.assertIn("## Maintenance Freshness", markdown)
        self.assertIn("ROI triage", freshness_section)
        self.assertEqual(action["title"], "Refresh maintenance surface: ROI triage")

    def test_operator_desk_surfaces_current_learnings(self) -> None:
        with patch(
            "business_operator_desk._load_learning_surface",
            return_value={
                "available": True,
                "path": "/tmp/current_learnings.md",
                "items": [{"headline": "Evening is the current best-performing posting window."}],
                "change_count": 1,
                "idea_count": 2,
                "material_change_count": 1,
                "change_notifier": {
                    "available": True,
                    "headline": "1 attention-level learning change needs review in the next planning pass.",
                    "recommended_action": "review current_learnings + weekly_strategy_recommendation_packet",
                    "items": [{"headline": "Slot 3 has no observed post yet for the planned `jeepfact` slot."}],
                },
            },
        ):
            payload = build_business_operator_desk(
                customer_packets={"items": []},
                nightly_summary={"counts": {}, "sections": {}},
                etsy_browser_sync={"items": []},
                custom_build_candidates={"items": []},
                print_queue_candidates=[],
                weekly_sale_monitor={"items": []},
                review_queue={"items": []},
                workflow_followthrough=[],
            )

        markdown = render_business_operator_desk_markdown(payload)
        self.assertEqual(payload["counts"]["learning_beliefs"], 1)
        self.assertEqual(payload["counts"]["learning_changes"], 1)
        self.assertEqual(payload["counts"]["learning_material_changes"], 1)
        self.assertIn("## Learning Surface", markdown)
        self.assertIn("Evening is the current best-performing posting window.", markdown)
        self.assertIn("Material learning changes", markdown)
        self.assertIn("Slot 3 has no observed post yet", markdown)

    def test_operator_desk_surfaces_seo_outcomes(self) -> None:
        with patch(
            "business_operator_desk._load_learning_surface",
            return_value={
                "available": False,
                "path": "/tmp/current_learnings.md",
                "items": [],
                "change_count": 0,
                "idea_count": 0,
            },
        ), patch(
            "business_operator_desk._load_seo_outcome_surface",
            return_value={
                "available": True,
                "path": "/tmp/shopify_seo_outcomes.md",
                "applied_item_count": 3,
                "stable_count": 1,
                "monitoring_count": 1,
                "issue_still_present_count": 1,
                "missing_from_audit_count": 0,
                "awaiting_audit_refresh_count": 0,
                "traffic_signal_available_count": 0,
                "traffic_signal_note": "No search-click or traffic collector is wired into Duck Ops yet.",
                "verification_truth": {
                    "headline": "Some applied SEO fixes are not staying resolved cleanly.",
                    "note": "`1` targeted issue is still present in the latest audit.",
                    "recommended_action": "Prioritize the reopening categories before sending more broad SEO apply batches.",
                },
                "category_guidance": [
                    {
                        "category_label": "Missing SEO titles",
                        "decision": "fix_now",
                        "summary": "This category still shows targeted SEO issues after an apply, so the current copy or apply path needs inspection before more volume.",
                    },
                    {
                        "category_label": "SEO titles too short",
                        "decision": "watch_window",
                        "summary": "The targeted issues are currently cleared, but the fixes are still too fresh to call them durable.",
                    },
                ],
                "attention_items": [
                    {
                        "title": "Open Duck",
                        "category_label": "Missing SEO titles",
                        "status": "issue_still_present",
                        "verification_note": "The latest SEO audit still reports `missing_seo_title` for this resource.",
                    }
                ],
                "recent_wins": [
                    {
                        "title": "Stable Duck",
                        "category_label": "Missing SEO titles",
                        "status": "stable",
                        "verification_note": "The targeted SEO issue is cleared and has stayed clean for `12.0` day(s).",
                    }
                ],
            },
        ):
            payload = build_business_operator_desk(
                customer_packets={"items": []},
                nightly_summary={"counts": {}, "sections": {}},
                etsy_browser_sync={"items": []},
                custom_build_candidates={"items": []},
                print_queue_candidates=[],
                weekly_sale_monitor={"items": []},
                review_queue={"items": []},
                workflow_followthrough=[],
            )

        markdown = render_business_operator_desk_markdown(payload)
        self.assertEqual(payload["counts"]["seo_outcome_items"], 3)
        self.assertEqual(payload["counts"]["seo_outcome_attention_items"], 1)
        self.assertEqual(payload["counts"]["seo_outcome_stable_items"], 1)
        self.assertIn("## SEO Outcomes", markdown)
        self.assertIn("Open Duck", markdown)
        self.assertIn("issue_still_present", markdown)
        self.assertIn("No search-click or traffic collector is wired", markdown)
        self.assertIn("Outcome truth:", markdown)
        self.assertIn("Category guidance:", markdown)
        self.assertIn("fix_now", markdown)

    def test_operator_desk_surfaces_weekly_strategy_packet(self) -> None:
        with patch(
            "business_operator_desk._load_learning_surface",
            return_value={
                "available": False,
                "path": "/tmp/current_learnings.md",
                "items": [],
                "change_count": 0,
                "idea_count": 0,
            },
        ), patch(
            "business_operator_desk._load_weekly_strategy_packet",
            return_value={
                "available": True,
                "path": "/tmp/weekly_strategy_recommendation_packet.md",
                "own_signal_confidence": "low",
                "competitor_signal_confidence": "low_medium",
                "own_signal_note": "Own-post coverage is still sparse.",
                "competitor_signal_note": "Competitor coverage is relying on cached fallback.",
                "competitor_stability_note": "`f3dprinted` stayed on top.",
                "stable_pattern_count": 1,
                "experimental_idea_count": 1,
                "do_not_copy_count": 1,
                "strategy_frames": {
                    "stable_patterns": {"headline": "Stable patterns are the defaults to keep this week."},
                    "experimental_ideas": {"headline": "Experimental ideas are one bounded tests, not a whole-calendar rewrite."},
                    "do_not_copy_patterns": {"headline": "Do-not-copy items are guardrails, not inspiration."},
                },
                "recommendation_count": 2,
                "watchout_count": 1,
                "recommendations": [{"title": "Keep testing the `evening` posting window"}],
                "social_plan": {
                    "headline": "Keep meme in evening and run one bounded music test.",
                    "anchor_window": "evening",
                    "anchor_workflow": "meme",
                    "watch_account": "f3dprinted",
                    "current_focus": {
                        "label": "next_up",
                        "headline": "Next up: Wednesday evening is the next planned move.",
                        "primary_move": "Review the last few hooks and formats from `f3dprinted` before drafting one bounded post test.",
                        "operator_brief": "Run Meme Flow, then reply `publish` to the review email after the content looks right.",
                        "status_label": "upcoming",
                    },
                    "at_a_glance": [
                        {
                            "calendar_label": "Monday evening",
                            "primary_move": "Run one `meme` post in the `evening` window to keep the week grounded in our best current signal.",
                            "operator_brief": "Run Meme Flow, then reply `publish` to the review email after the content looks right.",
                            "status_label": "completed strong",
                        },
                        {
                            "calendar_label": "Wednesday evening",
                            "primary_move": "Review the last few hooks and formats from `f3dprinted` before drafting one bounded post test.",
                            "operator_brief": "Run Meme Flow, then reply `publish` to the review email after the content looks right.",
                            "backup_move": "`jeepfact` if the borrowed account pattern needs more story than `meme` can carry cleanly, move the concept into `jeepfact` instead.",
                            "status_label": "upcoming",
                        },
                    ],
                    "slot_count": 2,
                    "readiness_counts": {
                        "ready_now": 0,
                        "ready_with_approval": 2,
                        "manual_experiment": 0,
                        "not_supported_yet": 0,
                    },
                    "execution_feedback": {
                        "recommended_lane_executed": 1,
                        "alternate_lane_executed": 1,
                        "different_lane_executed": 0,
                        "awaiting_slot": 0,
                        "no_post_observed": 0,
                        "review_slot": 0,
                    },
                    "execution_truth": {
                        "label": "mixed_on_plan",
                        "headline": "The weekly plan is landing, but some slots are resolving through fallbacks.",
                        "note": "`1` planned slot landed cleanly and `1` slot resolved through a planned fallback.",
                    },
                    "lane_guidance_summary": {
                        "ready_to_scale": 0,
                        "keep_anchor": 1,
                        "fallback_only": 1,
                        "experiment_only": 0,
                        "pull_back": 0,
                    },
                    "lane_guidance": [
                        {
                            "lane": "meme",
                            "decision": "keep_anchor",
                            "summary": "This lane still has enough direct proof to stay in the mix, but the evidence is not clean enough to expand it aggressively.",
                        },
                        {
                            "lane": "jeepfact",
                            "decision": "fallback_only",
                            "summary": "Keep this lane available as a fallback or rescue lane until it lands clean planned-slot wins of its own.",
                        },
                    ],
                    "ready_this_week": [
                        {
                            "slot": "Slot 1",
                            "calendar_label": "Monday evening",
                            "suggested_lane": "meme",
                            "execution_readiness": "ready_with_approval",
                            "operator_action_label": "Run Meme Flow",
                            "schedule_reference": "Monday 09:00 scheduled flow",
                            "next_step": "Run the meme flow or wait for the scheduled run, then use the normal review/publish reply loop.",
                            "command_hint": "python src/main_agent.py --flow meme --all",
                            "approval_followthrough": "Reply `publish` to the review email after the content looks right.",
                            "lane_fit_strength": "strong",
                            "lane_fit_reason": "`meme` is still our safest baseline lane, so this slot should protect the strongest own-post signal before we experiment.",
                            "tracking_status": "recommended_lane_executed",
                            "performance_label": "strong",
                            "performance_note": "This landed in the top third of the current social window at rank 1 of 3 observed posts.",
                        }
                    ],
                    "slots": [
                        {
                            "slot": "Slot 1",
                            "timing_hint": "Early week · evening",
                            "workflow": "meme",
                            "suggested_lane": "meme",
                            "content_family": "meme",
                            "execution_mode": "standard_lane",
                            "calendar_date": "2026-04-13",
                            "calendar_label": "Monday evening",
                            "cadence_reason": "This lines up with the recurring Meme Monday lane while keeping the stronger evening window in view.",
                            "execution_readiness": "ready_with_approval",
                            "operator_action_label": "Run Meme Flow",
                            "schedule_reference": "Monday 09:00 scheduled flow",
                            "next_step": "Run the meme flow or wait for the scheduled run, then use the normal review/publish reply loop.",
                            "command_hint": "python src/main_agent.py --flow meme --all",
                            "approval_followthrough": "Reply `publish` to the review email after the content looks right.",
                            "goal": "Anchor with the strongest proven workflow",
                            "action": "Run one `meme` post in the `evening` window to keep the week grounded in our best current signal.",
                            "lane_fit_strength": "strong",
                            "lane_fit_reason": "`meme` is still our safest baseline lane, so this slot should protect the strongest own-post signal before we experiment.",
                            "tracking_status": "recommended_lane_executed",
                            "tracking_note": "The recommended lane `meme` was observed on `2026-04-13`.",
                            "actual_lane": "meme",
                            "actual_platforms": ["instagram"],
                            "performance_label": "strong",
                            "performance_note": "This landed in the top third of the current social window at rank 1 of 3 observed posts.",
                        },
                        {
                            "slot": "Slot 2",
                            "timing_hint": "Midweek · evening",
                            "workflow": "meme",
                            "suggested_lane": "meme",
                            "content_family": "meme",
                            "execution_mode": "standard_lane",
                            "calendar_date": "2026-04-15",
                            "calendar_label": "Wednesday evening",
                            "cadence_reason": "This is the midweek test slot, so it should not steal focus from the Monday anchor post.",
                            "execution_readiness": "ready_with_approval",
                            "operator_action_label": "Run Meme Flow",
                            "schedule_reference": "Monday 09:00 scheduled flow",
                            "next_step": "Run the meme flow or wait for the scheduled run, then use the normal review/publish reply loop.",
                            "command_hint": "python src/main_agent.py --flow meme --all",
                            "approval_followthrough": "Reply `publish` to the review email after the content looks right.",
                            "goal": "Competitor-inspired hook test",
                            "action": "Review the last few hooks and formats from `f3dprinted` before drafting one bounded post test.",
                            "watch_account": "f3dprinted",
                            "lane_fit_strength": "strong",
                            "lane_fit_reason": "This is a bounded competitor-style borrow, so keeping it inside `meme` lets us test the signal without changing the production lane.",
                            "alternate_lane": "jeepfact",
                            "alternate_lane_reason": "If the borrowed account pattern needs more story than `meme` can carry cleanly, move the concept into `jeepfact` instead.",
                            "tracking_status": "alternate_lane_executed",
                            "tracking_note": "The primary lane `meme` did not land, but the planned fallback `jeepfact` was observed on `2026-04-15`.",
                            "actual_lane": "jeepfact",
                            "actual_platforms": ["instagram"],
                            "performance_label": "watch",
                            "performance_note": "This landed in the middle of the current social window at rank 2 of 3 observed posts.",
                        },
                    ],
                    "items": [
                        "Anchor the week around `meme` in the `evening` window.",
                        "Use `f3dprinted` as the competitor account to watch before drafting one new post.",
                    ],
                },
                "watchouts": ["Competitor coverage relied on cached fallback."],
            },
        ):
            payload = build_business_operator_desk(
                customer_packets={"items": []},
                nightly_summary={"counts": {}, "sections": {}},
                etsy_browser_sync={"items": []},
                custom_build_candidates={"items": []},
                print_queue_candidates=[],
                weekly_sale_monitor={"items": []},
                review_queue={"items": []},
                workflow_followthrough=[],
            )

        markdown = render_business_operator_desk_markdown(payload)
        self.assertEqual(payload["counts"]["strategy_recommendations"], 1)
        self.assertEqual(payload["counts"]["strategy_watchouts"], 1)
        self.assertEqual(payload["counts"]["strategy_plan_items"], 2)
        self.assertIn("## Weekly Strategy Packet", markdown)
        self.assertIn("Keep testing the `evening` posting window", markdown)
        self.assertIn("Competitor coverage relied on cached fallback.", markdown)
        self.assertIn("Stable-pattern rule:", markdown)
        self.assertIn("Experimental ideas are one bounded tests", markdown)
        self.assertIn("## This Week's Social Plan", markdown)
        self.assertIn("Keep meme in evening and run one bounded music test.", markdown)
        self.assertIn("Current focus:", markdown)
        self.assertIn("Next up: Wednesday evening is the next planned move.", markdown)
        self.assertIn("Week at a glance:", markdown)
        self.assertIn("Monday evening", markdown)
        self.assertIn("Slot 1: Early week", markdown)
        self.assertIn("Lane: `meme`", markdown)
        self.assertIn("Date: `2026-04-13`", markdown)
        self.assertIn("Calendar: `Monday evening`", markdown)
        self.assertIn("Fit: `strong`", markdown)
        self.assertIn("Lane reason:", markdown)
        self.assertIn("Alternate: `jeepfact`", markdown)
        self.assertIn("Execution feedback: `recommended=1`, `alternate=1`", markdown)
        self.assertIn("Execution truth:", markdown)
        self.assertIn("Lane guidance:", markdown)
        self.assertIn("Lane calls:", markdown)
        self.assertIn("fallback_only", markdown)
        self.assertIn("Outcome: `alternate_lane_executed`", markdown)
        self.assertIn("Performance: `watch`", markdown)
        self.assertIn("Readiness: `ready_with_approval`", markdown)
        self.assertIn("Ready this week:", markdown)
        self.assertIn("Use: Run Meme Flow", markdown)
        self.assertIn("Then: Reply `publish` to the review email", markdown)

    def test_operator_desk_next_actions_include_social_plan_slot(self) -> None:
        with patch(
            "business_operator_desk._load_learning_surface",
            return_value={
                "available": False,
                "path": "/tmp/current_learnings.md",
                "items": [],
                "change_count": 0,
                "idea_count": 0,
            },
        ), patch(
            "business_operator_desk._load_weekly_strategy_packet",
            return_value={
                "available": True,
                "path": "/tmp/weekly_strategy_recommendation_packet.md",
                "recommendations": [],
                "watchouts": [],
                "social_plan": {
                    "headline": "Keep meme in evening and run one bounded music test.",
                    "readiness_counts": {
                        "ready_now": 0,
                        "ready_with_approval": 1,
                        "manual_experiment": 0,
                        "not_supported_yet": 0,
                    },
                    "ready_this_week": [
                        {
                            "slot": "Slot 1",
                            "calendar_label": "Monday evening",
                            "suggested_lane": "meme",
                            "execution_readiness": "ready_with_approval",
                            "operator_action_label": "Run Meme Flow",
                            "command_hint": "python src/main_agent.py --flow meme --all",
                            "approval_followthrough": "Reply `publish` to the review email after the content looks right.",
                            "goal": "Anchor with the strongest proven workflow",
                        }
                    ],
                    "slots": [],
                    "items": [],
                },
            },
        ):
            payload = build_business_operator_desk(
                customer_packets={"items": []},
                nightly_summary={"counts": {}, "sections": {}},
                etsy_browser_sync={"items": []},
                custom_build_candidates={"items": []},
                print_queue_candidates=[],
                weekly_sale_monitor={"items": []},
                review_queue={"items": []},
                workflow_followthrough=[],
            )

        self.assertEqual(payload["counts"]["strategy_ready_slots"], 1)
        social_action = next(item for item in (payload.get("next_actions") or []) if item.get("lane") == "social_plan")
        self.assertEqual(social_action["title"], "Slot 1: meme")
        self.assertIn("Monday evening", social_action["summary"])
        self.assertIn("ready_with_approval", social_action["summary"])
        self.assertEqual(social_action["command"], "python src/main_agent.py --flow meme --all")
        self.assertIn("Reply `publish`", social_action["secondary_command"])

    def test_operator_desk_surfaces_weekly_sale_policy_promotion_readiness(self) -> None:
        with (
            patch(
                "business_operator_desk._load_weekly_sale_policy_surface",
                return_value={
                    "available": True,
                    "path": "/tmp/weekly_sale_execution.json",
                    "mode": "approval_gated",
                    "promotion_threshold": 3,
                    "clean_gated_streak": 3,
                    "clean_gated_recent_count": 3,
                    "blocked_recent_count": 0,
                    "auto_apply_eligible_recent_count": 0,
                    "promote_ready": True,
                    "readiness_headline": "Weekly sale policy is ready for promotion after 3 clean gated run(s).",
                    "recommended_action": "Flip `weekly_sale_execution.json` from `approval_gated` to `auto_apply_shopify`, then supervise the next Sunday run.",
                    "recent_runs": [
                        {
                            "title": "Spring Ducks",
                            "decision": "manual_review_required",
                            "state_reason": "awaiting_sale_review",
                            "manual_review_reasons": ["approval_gated_mode"],
                            "blockers": [],
                            "updated_at": "2026-04-19T09:00:00-04:00",
                        }
                    ],
                },
            ),
            patch(
                "business_operator_desk._load_meme_policy_surface",
                return_value={"available": False},
            ),
            patch(
                "business_operator_desk._load_review_carousel_policy_surface",
                return_value={"available": False},
            ),
            patch(
                "business_operator_desk._load_jeepfact_policy_surface",
                return_value={"available": False},
            ),
            patch(
                "business_operator_desk._load_review_reply_execution_surface",
                return_value={"available": False},
            ),
            patch(
                "business_operator_desk._load_seo_outcome_surface",
                return_value={"available": False},
            ),
        ):
            payload = build_business_operator_desk(
                customer_packets={"items": []},
                nightly_summary={"counts": {}, "sections": {}},
                etsy_browser_sync={"items": []},
                custom_build_candidates={"items": []},
                print_queue_candidates=[],
                weekly_sale_monitor={"items": []},
                review_queue={"items": []},
                workflow_followthrough=[],
            )

        markdown = render_business_operator_desk_markdown(payload)
        policy_section = render_business_section(payload, "policy")

        self.assertEqual(payload["counts"]["weekly_sale_policy_clean_streak"], 3)
        self.assertEqual(payload["counts"]["weekly_sale_policy_promote_ready"], 1)
        self.assertEqual(payload["counts"]["promotion_candidates"], 1)
        self.assertEqual(payload["counts"]["promotion_ready_candidates"], 1)
        candidate = payload["promotion_watch_surface"]["items"][0]
        self.assertEqual(candidate["promotion_owner"], "duckAgent")
        self.assertEqual(candidate["promotion_allowed_tier"], "Tier 3 after explicit operator promotion")
        self.assertEqual(candidate["promotion_risk_class"], "approval-gated production mutation")
        self.assertEqual(candidate["promotion_side_effect"], "Shopify sale updates")
        self.assertEqual(candidate["current_mode"], "approval_gated")
        self.assertEqual(candidate["target_mode"], "auto_apply_shopify")
        self.assertTrue(candidate["promotion_requires_operator_approval"])
        self.assertFalse(candidate["promotion_can_self_promote"])
        self.assertIn("Duck Ops may recommend promotion", candidate["approval_boundary"])
        action = next(item for item in (payload.get("next_actions") or []) if item.get("lane") == "weekly_sale_policy")
        self.assertEqual(action["title"], "Promote weekly sale auto-apply")
        self.assertIn("3 clean gated run", action["summary"])
        self.assertIn("## Promotion Watch", markdown)
        self.assertIn("1 promotion candidate(s) are ready to promote.", markdown)
        self.assertIn("## Weekly Sale Policy", markdown)
        self.assertIn("ready for promotion after 3 clean gated run", markdown)
        promotion_section = render_business_section(payload, "promotion")
        self.assertIn("Duck Ops Promotion Watch", promotion_section)
        self.assertIn("Ready to promote: 1", promotion_section)
        self.assertIn("Control: Tier 3 after explicit operator promotion", promotion_section)
        self.assertIn("Boundary: Duck Ops may recommend promotion", promotion_section)
        self.assertIn("Duck Ops Weekly Sale Policy", policy_section)
        self.assertIn("Clean gated streak: 3", policy_section)

    def test_operator_desk_surfaces_meme_policy_promotion_readiness(self) -> None:
        with (
            patch(
                "business_operator_desk._load_weekly_sale_policy_surface",
                return_value={"available": False},
            ),
            patch(
                "business_operator_desk._load_meme_policy_surface",
                return_value={
                    "available": True,
                    "path": "/tmp/meme_execution.json",
                    "mode": "approval_gated",
                    "promotion_threshold": 3,
                    "clean_gated_streak": 3,
                    "blocked_recent_count": 0,
                    "auto_schedule_eligible_recent_count": 0,
                    "promote_ready": True,
                    "readiness_headline": "Meme Monday policy is ready for promotion after 3 clean gated run(s).",
                    "recommended_action": "Flip `meme_execution.json` from `approval_gated` to `auto_schedule_meta`, then supervise the next Monday run.",
                    "recent_runs": [
                        {
                            "title": "Monster Truck Duck",
                            "decision": "manual_review_required",
                            "state_reason": "awaiting_review",
                            "manual_review_reasons": ["approval_gated_mode"],
                            "blockers": [],
                            "updated_at": "2026-04-20T09:00:00-04:00",
                        }
                    ],
                },
            ),
            patch(
                "business_operator_desk._load_review_reply_execution_surface",
                return_value={"available": False},
            ),
            patch(
                "business_operator_desk._load_seo_outcome_surface",
                return_value={"available": False},
            ),
            patch(
                "business_operator_desk._load_review_carousel_policy_surface",
                return_value={"available": False},
            ),
            patch(
                "business_operator_desk._load_jeepfact_policy_surface",
                return_value={"available": False},
            ),
        ):
            payload = build_business_operator_desk(
                customer_packets={"items": []},
                nightly_summary={"counts": {}, "sections": {}},
                etsy_browser_sync={"items": []},
                custom_build_candidates={"items": []},
                print_queue_candidates=[],
                weekly_sale_monitor={"items": []},
                review_queue={"items": []},
                workflow_followthrough=[],
            )

        markdown = render_business_operator_desk_markdown(payload)
        meme_policy_section = render_business_section(payload, "meme_policy")

        self.assertEqual(payload["counts"]["meme_policy_clean_streak"], 3)
        self.assertEqual(payload["counts"]["meme_policy_promote_ready"], 1)
        self.assertEqual(payload["counts"]["promotion_candidates"], 1)
        self.assertEqual(payload["counts"]["promotion_ready_candidates"], 1)
        action = next(item for item in (payload.get("next_actions") or []) if item.get("lane") == "meme_policy")
        self.assertEqual(action["title"], "Promote Meme Monday auto-schedule")
        self.assertIn("3 clean gated run", action["summary"])
        self.assertIn("## Meme Monday Policy", markdown)
        self.assertIn("ready for promotion after 3 clean gated run", markdown)
        self.assertIn("Duck Ops Meme Monday Policy", meme_policy_section)
        self.assertIn("Clean gated streak: 3", meme_policy_section)

    def test_operator_desk_adds_learning_next_action_when_material_changes_exist(self) -> None:
        with patch(
            "business_operator_desk._load_learning_surface",
            return_value={
                "available": True,
                "path": "/tmp/current_learnings.md",
                "change_count": 3,
                "idea_count": 2,
                "material_change_count": 2,
                "items": [{"headline": "Evening jeepfacts stayed strong."}],
                "change_notifier": {
                    "available": True,
                    "headline": "2 meaningful learning change(s) landed since the previous snapshot.",
                    "recommended_action": "review current_learnings + weekly_strategy_recommendation_packet",
                    "items": [
                        {
                            "urgency": "attention",
                            "headline": "Slot 3 has no observed post yet for the planned jeepfact slot.",
                            "source": "weekly_strategy",
                            "kind": "weekly_strategy_slot_missed",
                        }
                    ],
                },
            },
        ):
            payload = build_business_operator_desk(
                customer_packets={"items": []},
                nightly_summary={"counts": {}, "sections": {}},
                etsy_browser_sync={"items": []},
                custom_build_candidates={"items": []},
                print_queue_candidates=[],
                weekly_sale_monitor={"items": []},
                review_queue={"items": []},
                workflow_followthrough=[],
            )

        action = next(item for item in (payload.get("next_actions") or []) if item.get("lane") == "learning_surface")
        self.assertIn("2 material change", action["summary"])
        self.assertIn("Slot 3 has no observed post yet", action["summary"])
        self.assertEqual(action["command"], "review current_learnings + weekly_strategy_recommendation_packet")

    def test_render_business_section_learning_uses_payload_items_without_crashing(self) -> None:
        output = render_business_section(
            {
                "learning_surface": {
                    "available": True,
                    "path": "/tmp/current_learnings.md",
                    "change_count": 2,
                    "idea_count": 3,
                    "items": [{"headline": "Fallback belief should not be used."}],
                },
                "sections": {
                    "learning_surface": [
                        {"headline": "Evening posts still outperform midday posts."},
                    ]
                },
            },
            "learning",
        )

        self.assertIn("Duck Ops Current Learnings", output)
        self.assertIn("Evening posts still outperform midday posts.", output)
        self.assertNotIn("Fallback belief should not be used.", output)

    def test_render_business_section_strategy_packet_includes_recommendations_and_watchouts(self) -> None:
        output = render_business_section(
            {
                "weekly_strategy_packet": {
                    "available": True,
                    "path": "/tmp/weekly_strategy_recommendation_packet.md",
                    "own_signal_confidence": "low",
                    "competitor_signal_confidence": "low_medium",
                    "stable_pattern_count": 1,
                    "experimental_idea_count": 1,
                    "do_not_copy_count": 1,
                    "recommendation_count": 1,
                    "watchout_count": 1,
                    "own_signal_note": "Own-post coverage is still sparse.",
                    "competitor_signal_note": "Competitor coverage is relying on cached fallback.",
                    "competitor_stability_note": "`f3dprinted` stayed on top.",
                    "recommendations": [
                        {
                            "priority": "P1",
                            "category": "timing",
                            "title": "Keep testing the `evening` posting window",
                            "recommendation": "Schedule one more evening post this week.",
                            "evidence": "2 observed posts with the best current score.",
                        }
                    ],
                    "watchouts": ["Competitor coverage relied on cached fallback."],
                },
                "sections": {
                    "weekly_strategy_packet": [
                        {
                            "priority": "P1",
                            "category": "timing",
                            "title": "Keep testing the `evening` posting window",
                            "recommendation": "Schedule one more evening post this week.",
                            "evidence": "2 observed posts with the best current score.",
                        }
                    ]
                },
            },
            "packet",
        )

        self.assertIn("Duck Ops Weekly Strategy Packet", output)
        self.assertIn("Keep testing the `evening` posting window", output)
        self.assertIn("Watchouts:", output)
        self.assertIn("Competitor coverage relied on cached fallback.", output)
        self.assertIn("Stable patterns: 1", output)
        self.assertIn("Experimental ideas: 1", output)
        self.assertIn("Do-not-copy guardrails: 1", output)

    def test_render_business_section_seo_outcomes_includes_attention_items(self) -> None:
        output = render_business_section(
            {
                "seo_outcomes": {
                    "available": True,
                    "path": "/tmp/shopify_seo_outcomes.md",
                    "applied_item_count": 3,
                    "stable_count": 1,
                    "monitoring_count": 1,
                    "issue_still_present_count": 1,
                    "missing_from_audit_count": 0,
                    "awaiting_audit_refresh_count": 0,
                    "traffic_signal_available_count": 0,
                    "traffic_signal_note": "No search-click or traffic collector is wired into Duck Ops yet.",
                    "verification_truth": {
                        "headline": "Some applied SEO fixes are not staying resolved cleanly.",
                        "note": "`1` targeted issue is still present in the latest audit.",
                        "recommended_action": "Prioritize the reopening categories before sending more broad SEO apply batches.",
                    },
                    "category_guidance": [
                        {
                            "category_label": "Missing SEO titles",
                            "decision": "fix_now",
                            "summary": "This category still shows targeted SEO issues after an apply, so the current copy or apply path needs inspection before more volume.",
                        }
                    ],
                    "attention_items": [
                        {
                            "title": "Open Duck",
                            "category_label": "Missing SEO titles",
                            "status": "issue_still_present",
                            "verification_note": "The latest SEO audit still reports `missing_seo_title` for this resource.",
                        }
                    ],
                },
                "sections": {
                    "seo_outcomes": [
                        {
                            "title": "Open Duck",
                            "category_label": "Missing SEO titles",
                            "status": "issue_still_present",
                            "verification_note": "The latest SEO audit still reports `missing_seo_title` for this resource.",
                        }
                    ]
                },
            },
            "seo",
        )

        self.assertIn("Duck Ops SEO Outcomes", output)
        self.assertIn("Applied fixes tracked: 3", output)
        self.assertIn("Outcome truth:", output)
        self.assertIn("Category guidance:", output)
        self.assertIn("Open Duck | Missing SEO titles | issue_still_present", output)
        self.assertIn("Signal note:", output)

    def test_render_business_section_social_plan_includes_plan_items(self) -> None:
        output = render_business_section(
            {
                "weekly_strategy_packet": {
                    "available": True,
                    "social_plan": {
                        "headline": "Keep meme in evening and run one bounded music test.",
                        "anchor_window": "evening",
                        "anchor_workflow": "meme",
                        "watch_account": "f3dprinted",
                        "current_focus": {
                            "label": "next_up",
                            "headline": "Next up: Wednesday evening is the next planned move.",
                            "primary_move": "Use `f3dprinted` as the competitor account to watch before drafting one new post.",
                            "operator_brief": "Run Meme Flow, then reply `publish` to the review email after the content looks right.",
                            "status_label": "upcoming",
                        },
                        "at_a_glance": [
                            {
                                "calendar_label": "Monday evening",
                                "primary_move": "Run one `meme` post in the `evening` window to keep the week grounded in our best current signal.",
                                "operator_brief": "Run Meme Flow, then reply `publish` to the review email after the content looks right.",
                                "status_label": "completed strong",
                            },
                            {
                                "calendar_label": "Wednesday evening",
                                "primary_move": "Use `f3dprinted` as the competitor account to watch before drafting one new post.",
                                "operator_brief": "Run Meme Flow, then reply `publish` to the review email after the content looks right.",
                                "backup_move": "`jeepfact` if the borrowed account pattern needs more story than `meme` can carry cleanly, move the concept into `jeepfact` instead.",
                                "status_label": "upcoming",
                            },
                        ],
                        "readiness_counts": {
                            "ready_now": 0,
                            "ready_with_approval": 2,
                            "manual_experiment": 0,
                            "not_supported_yet": 0,
                        },
                        "execution_feedback": {
                            "recommended_lane_executed": 1,
                            "alternate_lane_executed": 1,
                            "different_lane_executed": 0,
                            "awaiting_slot": 0,
                            "no_post_observed": 0,
                            "review_slot": 0,
                        },
                        "ready_this_week": [
                            {
                                "slot": "Slot 1",
                                "calendar_label": "Monday evening",
                                "suggested_lane": "meme",
                                "execution_readiness": "ready_with_approval",
                                "operator_action_label": "Run Meme Flow",
                                "lane_fit_strength": "strong",
                                "lane_fit_reason": "`meme` is still our safest baseline lane, so this slot should protect the strongest own-post signal before we experiment.",
                                "tracking_status": "recommended_lane_executed",
                                "performance_label": "strong",
                            }
                        ],
                        "slots": [
                            {
                                "slot": "Slot 1",
                                "timing_hint": "Early week · evening",
                                "workflow": "meme",
                                "suggested_lane": "meme",
                                "content_family": "meme",
                                "execution_mode": "standard_lane",
                                "calendar_date": "2026-04-13",
                                "calendar_label": "Monday evening",
                                "cadence_reason": "This lines up with the recurring Meme Monday lane while keeping the stronger evening window in view.",
                                "execution_readiness": "ready_with_approval",
                                "operator_action_label": "Run Meme Flow",
                                "schedule_reference": "Monday 09:00 scheduled flow",
                                "next_step": "Run the meme flow or wait for the scheduled run, then use the normal review/publish reply loop.",
                                "command_hint": "python src/main_agent.py --flow meme --all",
                                "approval_followthrough": "Reply `publish` to the review email after the content looks right.",
                                "goal": "Anchor with the strongest proven workflow",
                                "action": "Run one `meme` post in the `evening` window to keep the week grounded in our best current signal.",
                                "lane_fit_strength": "strong",
                                "lane_fit_reason": "`meme` is still our safest baseline lane, so this slot should protect the strongest own-post signal before we experiment.",
                                "tracking_status": "recommended_lane_executed",
                                "tracking_note": "The recommended lane `meme` was observed on `2026-04-13`.",
                                "actual_lane": "meme",
                                "actual_platforms": ["instagram"],
                                "performance_label": "strong",
                                "performance_note": "This landed in the top third of the current social window at rank 1 of 3 observed posts.",
                            },
                            {
                                "slot": "Slot 2",
                                "timing_hint": "Midweek · evening",
                                "workflow": "meme",
                                "suggested_lane": "meme",
                                "content_family": "meme",
                                "execution_mode": "standard_lane",
                                "calendar_date": "2026-04-15",
                                "calendar_label": "Wednesday evening",
                                "cadence_reason": "This is the midweek test slot, so it should not steal focus from the Monday anchor post.",
                                "execution_readiness": "ready_with_approval",
                                "operator_action_label": "Run Meme Flow",
                                "schedule_reference": "Monday 09:00 scheduled flow",
                                "next_step": "Run the meme flow or wait for the scheduled run, then use the normal review/publish reply loop.",
                                "command_hint": "python src/main_agent.py --flow meme --all",
                                "approval_followthrough": "Reply `publish` to the review email after the content looks right.",
                                "goal": "Competitor-inspired hook test",
                                "action": "Use `f3dprinted` as the competitor account to watch before drafting one new post.",
                                "watch_account": "f3dprinted",
                            },
                        ],
                        "items": [
                            "Anchor the week around `meme` in the `evening` window.",
                            "Use `f3dprinted` as the competitor account to watch before drafting one new post.",
                        ],
                    },
                },
                "sections": {
                    "weekly_social_plan": [
                        {
                            "slot": "Slot 1",
                            "timing_hint": "Early week · evening",
                            "workflow": "meme",
                            "suggested_lane": "meme",
                            "content_family": "meme",
                            "execution_mode": "standard_lane",
                            "calendar_date": "2026-04-13",
                            "calendar_label": "Monday evening",
                            "cadence_reason": "This lines up with the recurring Meme Monday lane while keeping the stronger evening window in view.",
                            "execution_readiness": "ready_with_approval",
                            "operator_action_label": "Run Meme Flow",
                            "schedule_reference": "Monday 09:00 scheduled flow",
                            "next_step": "Run the meme flow or wait for the scheduled run, then use the normal review/publish reply loop.",
                            "command_hint": "python src/main_agent.py --flow meme --all",
                            "approval_followthrough": "Reply `publish` to the review email after the content looks right.",
                            "goal": "Anchor with the strongest proven workflow",
                            "action": "Run one `meme` post in the `evening` window to keep the week grounded in our best current signal.",
                            "lane_fit_strength": "strong",
                            "lane_fit_reason": "`meme` is still our safest baseline lane, so this slot should protect the strongest own-post signal before we experiment.",
                            "tracking_status": "recommended_lane_executed",
                            "tracking_note": "The recommended lane `meme` was observed on `2026-04-13`.",
                            "actual_lane": "meme",
                            "actual_platforms": ["instagram"],
                            "performance_label": "strong",
                            "performance_note": "This landed in the top third of the current social window at rank 1 of 3 observed posts.",
                        },
                        {
                            "slot": "Slot 2",
                            "timing_hint": "Midweek · evening",
                            "workflow": "meme",
                            "suggested_lane": "meme",
                            "content_family": "meme",
                            "execution_mode": "standard_lane",
                            "calendar_date": "2026-04-15",
                            "calendar_label": "Wednesday evening",
                            "cadence_reason": "This is the midweek test slot, so it should not steal focus from the Monday anchor post.",
                            "execution_readiness": "ready_with_approval",
                            "operator_action_label": "Run Meme Flow",
                            "schedule_reference": "Monday 09:00 scheduled flow",
                            "next_step": "Run the meme flow or wait for the scheduled run, then use the normal review/publish reply loop.",
                            "command_hint": "python src/main_agent.py --flow meme --all",
                            "approval_followthrough": "Reply `publish` to the review email after the content looks right.",
                            "goal": "Competitor-inspired hook test",
                            "action": "Use `f3dprinted` as the competitor account to watch before drafting one new post.",
                            "watch_account": "f3dprinted",
                            "lane_fit_strength": "strong",
                            "lane_fit_reason": "This is a bounded competitor-style borrow, so keeping it inside `meme` lets us test the signal without changing the production lane.",
                            "alternate_lane": "jeepfact",
                            "alternate_lane_reason": "If the borrowed account pattern needs more story than `meme` can carry cleanly, move the concept into `jeepfact` instead.",
                            "tracking_status": "alternate_lane_executed",
                            "tracking_note": "The primary lane `meme` did not land, but the planned fallback `jeepfact` was observed on `2026-04-15`.",
                            "actual_lane": "jeepfact",
                            "actual_platforms": ["instagram"],
                            "performance_label": "watch",
                            "performance_note": "This landed in the middle of the current social window at rank 2 of 3 observed posts.",
                        },
                        ]
                    },
                },
            "social_plan",
        )

        self.assertIn("Duck Ops This Week's Social Plan", output)
        self.assertIn("Keep meme in evening and run one bounded music test.", output)
        self.assertIn("Anchor window: evening", output)
        self.assertIn("Watch account: f3dprinted", output)
        self.assertIn("Current focus: Next up: Wednesday evening is the next planned move.", output)
        self.assertIn("Week at a glance:", output)
        self.assertIn("Monday evening | Run one `meme` post", output)
        self.assertIn("Execution feedback: recommended=1, alternate=1", output)
        self.assertIn("Slot 1: Early week", output)
        self.assertIn("Lane: meme", output)
        self.assertIn("Date: 2026-04-13", output)
        self.assertIn("Calendar: Monday evening", output)
        self.assertIn("Fit: strong", output)
        self.assertIn("Lane reason:", output)
        self.assertIn("Alternate: jeepfact", output)
        self.assertIn("Outcome: alternate_lane_executed", output)
        self.assertIn("Performance: watch", output)
        self.assertIn("Readiness: ready_with_approval", output)
        self.assertIn("Ready this week:", output)
        self.assertIn("Use: Run Meme Flow", output)
        self.assertIn("Then: Reply `publish` to the review email", output)

    def test_render_business_section_reviews_includes_decision_command(self) -> None:
        output = render_business_section(
            {
                "counts": {"review_queue_backlog": 1},
                "sections": {
                    "review_queue": [
                        {
                            "short_id": "221",
                            "decision": "publish_ready",
                            "title": "Review carousel for spring buyers",
                            "detail_command": "why 221",
                            "approve_command": "approve 221 because ...",
                        }
                    ]
                },
            },
            "reviews",
        )

        self.assertIn("Detail: why 221", output)
        self.assertIn("Decide: approve 221 because ...", output)

    def test_render_business_section_surfaces_approval_chains(self) -> None:
        output = render_business_section(
            {
                "approval_chain_surface": {
                    "available": True,
                    "awaiting_review_count": 1,
                    "ready_count": 1,
                    "blocked_count": 0,
                    "active_count": 0,
                    "observing_count": 1,
                    "headline": "One approval chain is waiting on a human reply.",
                    "recommended_action": "Reply to the open review email first.",
                },
                "sections": {
                    "approval_chains": [
                        {
                            "title": "Shopify SEO category chain",
                            "chain_state": "awaiting_review",
                            "progress_label": "Duplicate SEO titles",
                            "summary": "Duplicate SEO titles is currently waiting for a reply apply decision.",
                            "recommended_action": "Reply `apply` to the current Shopify SEO review email.",
                        }
                    ]
                },
            },
            "approval",
        )

        self.assertIn("Duck Ops Approval Chains", output)
        self.assertIn("Awaiting review: 1", output)
        self.assertIn("Shopify SEO category chain | awaiting_review | Duplicate SEO titles", output)
        self.assertIn("Next: Reply `apply` to the current Shopify SEO review email.", output)

    def test_render_business_section_seo_includes_review_chain_summary(self) -> None:
        output = render_business_section(
            {
                "seo_outcomes": {
                    "available": True,
                    "path": "/tmp/shopify_seo_outcomes.md",
                    "applied_item_count": 2,
                    "stable_count": 1,
                    "monitoring_count": 1,
                    "issue_still_present_count": 0,
                    "missing_from_audit_count": 0,
                    "awaiting_audit_refresh_count": 0,
                    "writeback_receipt_count": 0,
                    "writeback_failed_count": 0,
                    "traffic_signal_available_count": 0,
                    "verification_truth": {"headline": "SEO cleanup is moving in the right direction."},
                    "review_chain": {
                        "available": True,
                        "headline": "Duplicate SEO titles is currently waiting for a reply apply decision.",
                        "current_review": {
                            "run_id": "shopify_seo_duplicate_title_1",
                            "category_label": "Duplicate SEO titles",
                            "status": "awaiting_review",
                            "item_count": 2,
                        },
                        "last_applied": {
                            "run_id": "shopify_seo_missing_title_1",
                            "category_label": "Missing SEO titles",
                            "item_count": 5,
                        },
                        "remaining_count": 4,
                    },
                    "category_guidance": [],
                    "attention_items": [],
                    "recent_wins": [],
                },
                "sections": {"seo_outcomes": []},
            },
            "seo",
        )

        self.assertIn("Duck Ops SEO Outcomes", output)
        self.assertIn("Review chain: Duplicate SEO titles is currently waiting for a reply apply decision.", output)
        self.assertIn("Current review: Duplicate SEO titles | awaiting_review | items 2", output)
        self.assertIn("Last applied: Missing SEO titles | items 5", output)
        self.assertIn("Remaining SEO categories in audit: 4", output)

    def test_operator_desk_surfaces_shared_interface_contract_summary(self) -> None:
        with patch(
            "business_operator_desk._load_interface_contract_surface",
            return_value={
                "available": True,
                "path": "/tmp/operator_interface_contracts.py",
                "surface_version": 1,
                "source_label": "Duck Ops local",
                "ducks_to_pack_today": 4,
                "customers_to_reply": 2,
                "pending_approvals_count": 1,
                "trend_ideas_count": 2,
                "top_tasks_count": 1,
                "pending_approvals": [
                    {
                        "title": "Orange Cat Duck Meme",
                        "flow": "meme",
                        "body_preview": "Fresh orange cat duck energy.",
                    }
                ],
                "top_tasks": [
                    {
                        "id": "C301",
                        "type": "reply",
                        "summary": "Need a quick order update.",
                    }
                ],
                "trend_ideas": [
                    {"title": "Orange pet ducks", "score": 9.0, "status": "partial"}
                ],
            },
        ):
            payload = build_business_operator_desk(
                customer_packets={"items": []},
                nightly_summary={"counts": {}, "sections": {}},
                etsy_browser_sync={"items": []},
                custom_build_candidates={"items": []},
                print_queue_candidates=[],
                weekly_sale_monitor={"items": []},
                review_queue={"items": []},
            )
            markdown = render_business_operator_desk_markdown(payload)
            section_output = render_business_section(payload, "widget")

        self.assertEqual(payload["counts"]["interface_contract_pending_approvals"], 1)
        self.assertEqual(payload["counts"]["interface_contract_ducks_to_pack_today"], 4)
        self.assertIn("## Interface Contracts", markdown)
        self.assertIn("Orange Cat Duck Meme", markdown)
        self.assertIn("Duck Ops Interface Contracts", section_output)
        self.assertIn("Ducks to pack today: 4", section_output)
        self.assertIn("Orange pet ducks | score 9.0 | partial", section_output)


if __name__ == "__main__":
    unittest.main()

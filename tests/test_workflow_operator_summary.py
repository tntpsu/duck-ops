from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from workflow_operator_summary import build_workflow_followthrough_items


class WorkflowOperatorSummaryTests(unittest.TestCase):
    def test_build_workflow_followthrough_items_filters_latest_actionable_lanes(self) -> None:
        with self.subTest("latest actionable lanes only"):
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as tmp:
                state_dir = Path(tmp)
                receipt_dir = state_dir / "receipts"
                receipt_dir.mkdir(parents=True, exist_ok=True)
                meme_receipt_path = receipt_dir / "meme-receipt.json"
                meme_receipt_path.write_text(
                    json.dumps(
                        {
                            "payload": {
                                "publish_result": {
                                    "details": [
                                        "Facebook: Unsupported post request. Object with ID '123456789012345' does not exist.",
                                        "Instagram: [meta] IG /media failed [500]: transient OAuthException.",
                                    ]
                                }
                            }
                        }
                    ),
                    encoding="utf-8",
                )
                (state_dir / "weekly-old.json").write_text(
                    json.dumps(
                        {
                            "lane": "weekly",
                            "display_label": "Weekly old",
                            "state": "observed",
                            "state_reason": "draft_ready",
                            "next_action": "Old action",
                            "updated_at": "2026-04-11T10:00:00-04:00",
                            "metadata": {},
                        }
                    ),
                    encoding="utf-8",
                )
                (state_dir / "weekly-new.json").write_text(
                    json.dumps(
                        {
                            "lane": "weekly",
                            "display_label": "Weekly new",
                            "state": "blocked",
                            "state_reason": "stale_input",
                            "next_action": "Refresh weekly inputs",
                            "updated_at": "2026-04-12T10:00:00-04:00",
                            "metadata": {"theme_name": "Spring Ducks"},
                            "input_freshness": {"stale_sources": ["weekly_email_pkg"]},
                        }
                    ),
                    encoding="utf-8",
                )
                (state_dir / "reviews.json").write_text(
                    json.dumps(
                        {
                            "lane": "reviews",
                            "display_label": "Reviews",
                            "state": "proposed",
                            "state_reason": "awaiting_review",
                            "requires_confirmation": True,
                            "next_action": "Approve the review email",
                            "updated_at": "2026-04-12T09:00:00-04:00",
                            "metadata": {},
                            "latest_receipt": {
                                "receipt_id": "20260412130000-review-email",
                                "recorded_at": "2026-04-12T13:00:00-04:00",
                            },
                            "history": [
                                {"state_reason": "awaiting_review"},
                                {"state_reason": "draft_ready"},
                            ],
                        }
                    ),
                    encoding="utf-8",
                )
                (state_dir / "meme.json").write_text(
                    json.dumps(
                        {
                            "lane": "meme",
                            "display_label": "Meme",
                            "state": "blocked",
                            "state_reason": "execution_failed",
                            "next_action": "Retry meme publish",
                            "updated_at": "2026-04-12T08:00:00-04:00",
                            "metadata": {},
                            "latest_receipt": {
                                "receipt_id": "20260412080000-publish",
                                "recorded_at": "2026-04-12T08:00:00-04:00",
                                "path": str(meme_receipt_path),
                            },
                        }
                    ),
                    encoding="utf-8",
                )
                (state_dir / "notifier.json").write_text(
                    json.dumps(
                        {
                            "lane": "notifier",
                            "display_label": "Notifier",
                            "state": "verified",
                            "state_reason": "operator_push_sent",
                            "next_action": "No action",
                            "updated_at": "2026-04-12T09:00:00-04:00",
                            "metadata": {},
                        }
                    ),
                    encoding="utf-8",
                )

                items = build_workflow_followthrough_items(limit=5, state_dir=state_dir)

                self.assertEqual([item["lane"] for item in items], ["weekly", "meme", "reviews"])
                self.assertEqual(items[0]["title"], "Spring Ducks")
                self.assertIn("stale: weekly email pkg", items[0]["summary"])
                self.assertIn("python src/main_agent.py --all --flow weekly --force", items[0]["command"])
                self.assertIn("Unsupported post request", items[1]["root_cause"])
                self.assertIn("Facebook page/object permissions", items[1]["fix_hint"])
                self.assertEqual(items[2]["latest_receipt"], "review email at Apr 12, 1:00 PM")
                self.assertEqual(items[2]["recent_history"], "awaiting review -> draft ready")

    def test_include_all_blocked_keeps_all_failed_lanes_inline(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            for idx in range(3):
                (state_dir / f"blocked-{idx}.json").write_text(
                    json.dumps(
                        {
                            "lane": f"lane{idx}",
                            "display_label": f"Lane {idx}",
                            "state": "blocked",
                            "state_reason": "execution_failed",
                            "next_action": f"Fix lane {idx}",
                            "updated_at": f"2026-04-12T0{idx}:00:00-04:00",
                            "metadata": {},
                        }
                    ),
                    encoding="utf-8",
                )
            (state_dir / "warn.json").write_text(
                json.dumps(
                    {
                        "lane": "weekly",
                        "display_label": "Weekly",
                        "state": "approved",
                        "state_reason": "awaiting_review",
                        "next_action": "Approve weekly",
                        "updated_at": "2026-04-12T09:00:00-04:00",
                        "metadata": {},
                    }
                ),
                encoding="utf-8",
            )

            items = build_workflow_followthrough_items(limit=1, include_all_blocked=True, state_dir=state_dir)

            self.assertEqual([item["lane"] for item in items], ["lane2", "lane1", "lane0", "weekly"])

    def test_quality_gate_items_include_inline_urgent_details(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp) / "workflow"
            state_dir.mkdir(parents=True, exist_ok=True)
            quality_gate_state_path = Path(tmp) / "quality_gate_state.json"
            (state_dir / "quality-gate.json").write_text(
                json.dumps(
                    {
                        "lane": "quality_gate",
                        "display_label": "Quality Gate",
                        "state": "blocked",
                        "state_reason": "alerts_pending",
                        "next_action": "Review the urgent quality gate alerts and clear or archive them.",
                        "updated_at": "2026-04-13T10:00:00-04:00",
                        "metadata": {},
                    }
                ),
                encoding="utf-8",
            )
            quality_gate_state_path.write_text(
                json.dumps(
                    {
                        "alerts": {
                            "publish::weekly_sale::2026-04-13::sale-playbook::abc123": {
                                "created_at": "2026-04-13T09:30:00-04:00",
                            }
                        },
                        "artifacts": {
                            "publish::weekly_sale::2026-04-13::sale-playbook": {
                                "decision": {
                                    "title": "Weekly Sale Playbook",
                                    "decision": "discard",
                                    "priority": "high",
                                    "improvement_suggestions": [
                                        "Re-run the weekly flow so the sale playbook reflects the current week before publishing."
                                    ],
                                    "quality_gate_metadata": {
                                        "fail_closed": [
                                            "Weekly sale playbook is stale for a publish decision and should not be acted on as-is."
                                        ]
                                    },
                                }
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            items = build_workflow_followthrough_items(
                limit=5,
                state_dir=state_dir,
                quality_gate_state_path=quality_gate_state_path,
            )

            self.assertEqual(items[0]["lane"], "quality_gate")
            self.assertIn("urgent quality gate", items[0]["root_cause"])
            self.assertEqual(items[0]["urgent_items"][0]["title"], "Weekly Sale Playbook")
            self.assertIn("archive or rerun", items[0]["fix_hint"])

    def test_weekly_sale_monitor_stale_input_explains_auto_refresh(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            (state_dir / "weekly-sale-monitor.json").write_text(
                json.dumps(
                    {
                        "lane": "weekly_sale_monitor",
                        "display_label": "Weekly Sale Monitor",
                        "state": "blocked",
                        "state_reason": "stale_input",
                        "next_action": "Refresh the weekly sale monitor before using it to steer the next sale or campaign.",
                        "updated_at": "2026-04-13T10:00:00-04:00",
                        "metadata": {},
                    }
                ),
                encoding="utf-8",
            )

            items = build_workflow_followthrough_items(limit=5, state_dir=state_dir)

            self.assertEqual(items[0]["lane"], "weekly_sale_monitor")
            self.assertIn("refreshes the sale monitor automatically", items[0]["next_action"])
            self.assertIsNone(items[0]["command"])
            self.assertFalse(items[0]["actionable"])

    def test_weekly_stale_input_surfaces_snapshot_refresh_root_cause(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            receipt_dir = state_dir / "receipts"
            receipt_dir.mkdir(parents=True, exist_ok=True)
            receipt_path = receipt_dir / "weekly-stale.json"
            receipt_path.write_text(
                json.dumps(
                    {
                        "payload": {
                            "refresh_errors": {
                                "sale_monitor_snapshot": "No module named 'workflow_control'",
                                "campaign_coordination_snapshot": "No module named 'workflow_control'",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            (state_dir / "weekly.json").write_text(
                json.dumps(
                    {
                        "lane": "weekly",
                        "display_label": "Weekly",
                        "state": "blocked",
                        "state_reason": "stale_input",
                        "next_action": "Fix weekly snapshot refresh errors, then rerun the weekly flow.",
                        "updated_at": "2026-04-19T18:54:52-04:00",
                        "metadata": {"theme_name": "Special Offers"},
                        "latest_receipt": {
                            "receipt_id": "20260419185452-weekly-sale-playbook-stale",
                            "recorded_at": "2026-04-19T18:54:52-04:00",
                            "path": str(receipt_path),
                        },
                    }
                ),
                encoding="utf-8",
            )

            items = build_workflow_followthrough_items(limit=5, state_dir=state_dir)

            self.assertEqual(items[0]["lane"], "weekly")
            self.assertIn("Weekly snapshot refresh failed.", items[0]["root_cause"])
            self.assertIn("sale monitor: No module named 'workflow_control'", items[0]["root_cause"])

    def test_newduck_seo_writeback_failure_surfaces_root_cause_and_fix_hint(self) -> None:
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            receipt_dir = state_dir / "receipts"
            receipt_dir.mkdir(parents=True, exist_ok=True)
            receipt_path = receipt_dir / "newduck-seo.json"
            receipt_path.write_text(
                json.dumps(
                    {
                        "payload": {
                            "shopify_product_id": 8546991767735,
                            "receipt_id": "receipt-123",
                            "failure_codes": ["seo_title_mismatch"],
                            "updates_applied": ["refreshed Shopify SEO"],
                        }
                    }
                ),
                encoding="utf-8",
            )
            (state_dir / "newduck.json").write_text(
                json.dumps(
                    {
                        "lane": "newduck",
                        "display_label": "Orange Cat Duck",
                        "state": "blocked",
                        "state_reason": "seo_writeback_verification_failed",
                        "next_action": "Repair the Shopify SEO or body link writeback before activating this product.",
                        "updated_at": "2026-04-21T23:06:13-04:00",
                        "metadata": {"duck_name": "Orange Cat Duck"},
                        "last_verification": {
                            "kind": "shopify_seo_writeback",
                            "status": "failed",
                            "receipt_id": "receipt-123",
                            "failure_codes": ["seo_title_mismatch"],
                        },
                        "latest_receipt": {
                            "receipt_id": "receipt-123",
                            "recorded_at": "2026-04-21T23:06:13-04:00",
                            "path": str(receipt_path),
                        },
                    }
                ),
                encoding="utf-8",
            )

            items = build_workflow_followthrough_items(limit=5, state_dir=state_dir)

            self.assertEqual(items[0]["lane"], "newduck")
            self.assertIn("seo_title_mismatch", items[0]["root_cause"])
            self.assertIn("rerun Shopify activation", items[0]["fix_hint"])


if __name__ == "__main__":
    unittest.main()

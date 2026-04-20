from __future__ import annotations

import sys
import unittest
from pathlib import Path


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import notifier  # noqa: E402


class NotifierEmailRenderingTests(unittest.TestCase):
    def test_digest_html_uses_review_cards(self) -> None:
        html = notifier.render_notifier_html(
            "digest",
            "[OpenClaw Digest] 2026-04-11",
            "plain body",
            {
                "generated_at": "2026-04-11T20:51:46.269468-04:00",
                "pending_review_count": 2,
                "active_counts": {"publish_ready": 26, "needs_revision": 39, "discard": 3},
                "pending_items": [
                    {
                        "title": "Etsy Review Reply 2026-04-07 #1",
                        "flow": "reviews_reply_positive",
                        "review_status": "pending",
                        "decision": "needs_revision",
                        "priority": "medium",
                        "score": 77,
                        "confidence": 0.65,
                        "preview": {
                            "context_label": "Customer review",
                            "context_text": "Very cute and perfect size.",
                            "proposed_label": "Draft reply",
                            "proposed_text": "Thank you so much for your kind words!",
                        },
                        "improvement_suggestions": ["Keep this shorter and more specific."],
                    }
                ],
            },
        )

        self.assertIn("Pending review", html)
        self.assertIn("Etsy Review Reply 2026-04-07 #1", html)
        self.assertIn("Customer review", html)
        self.assertIn("Draft reply", html)
        self.assertNotIn("<pre", html)

    def test_trend_digest_html_uses_signal_cards(self) -> None:
        html = notifier.render_notifier_html(
            "trend_digest",
            "[OpenClaw Trends] 2026-04-11",
            "plain body",
            {
                "generated_at": "2026-04-11T20:51:45.961440-04:00",
                "background_watch_count": 24,
                "new_background_watch_count": 22,
                "active_counts": {"worth_acting_on": 6, "ignore": 56},
                "background_watch_items": [
                    {
                        "title": "Highland Cow Duck",
                        "decision": "watch",
                        "action_frame": "wait",
                        "review_status": "pending",
                        "score": 73,
                        "confidence": 0.58,
                        "reasoning": ["Commercial signal is strong this week."],
                        "improvement_suggestions": ["Queue this if the signal persists."],
                        "trend_metadata": {"catalog_status": "gap", "matching_products": []},
                    }
                ],
            },
        )

        self.assertIn("Background watch", html)
        self.assertIn("Highland Cow Duck", html)
        self.assertIn("Commercial signal is strong this week.", html)
        self.assertNotIn("<pre", html)

    def test_urgent_html_uses_alert_card(self) -> None:
        html = notifier.render_notifier_html(
            "urgent",
            "[OpenClaw Urgent] publish::weekly_sale::2026-03-22::sale-playbook",
            "plain body",
            {
                "generated_at": "2026-03-22T18:35:38.005107-04:00",
                "decision": {
                    "title": "Weekly Sale Playbook",
                    "flow": "weekly_sale",
                    "run_id": "2026-03-22",
                    "decision": "needs_revision",
                    "priority": "high",
                    "score": 64,
                    "confidence": 0.6,
                    "reasoning": ["Artifact is too unclear to mark publish-ready."],
                    "improvement_suggestions": ["Preserve the sale playbook as structured state."],
                    "evidence_refs": ["state_weekly.json"],
                },
            },
        )

        self.assertIn("Weekly Sale Playbook", html)
        self.assertIn("Artifact is too unclear to mark publish-ready.", html)
        self.assertIn("Preserve the sale playbook as structured state.", html)
        self.assertNotIn("<pre", html)

    def test_phase_readiness_html_uses_scorecard(self) -> None:
        html = notifier.render_notifier_html(
            "phase_readiness",
            "[OpenClaw Phase Readiness] 2026-15",
            "plain body",
            {
                "generated_at": "2026-04-11T20:00:00-04:00",
                "current_phase": "phase_2_pilot",
                "readiness_decision": "stay_in_current_phase",
                "confidence": 0.66,
                "recommended_next_phase": "phase_2_pilot",
                "evidence": ["Collected 20 quality-gate decisions in the last 7 days."],
                "blockers": ["Urgent alerts are still firing in the last 7 days."],
                "metrics": {"pending_items": 2, "urgent_alert_count": 1},
            },
        )

        self.assertIn("Phase recommendation", html)
        self.assertIn("Collected 20 quality-gate decisions in the last 7 days.", html)
        self.assertIn("Urgent alerts are still firing in the last 7 days.", html)
        self.assertNotIn("<pre", html)

    def test_promotion_readiness_html_uses_candidate_cards(self) -> None:
        html = notifier.render_notifier_html(
            "promotion_readiness",
            "[Duck Ops Promotion Ready] 2026-04-19",
            "plain body",
            {
                "generated_at": "2026-04-19T08:00:00-04:00",
                "source": "business_desk",
                "item_count": 1,
                "ready_item_count": 1,
                "items": [
                    {
                        "promotion_id": "weekly_sale_auto_apply",
                        "title": "Weekly sale auto-apply",
                        "promotion_state": "ready",
                        "progress_label": "3/3 clean gated run(s)",
                        "summary": "Weekly sale policy is ready for promotion after 3 clean gated run(s).",
                        "recommended_action": "Flip the mode to auto_apply_shopify and supervise the next Sunday run.",
                        "source_path": "/tmp/weekly_sale_execution.json",
                        "evidence": [
                            "Clean gated streak 3/3.",
                            "Mode is approval_gated.",
                        ],
                    }
                ],
            },
        )

        self.assertIn("Weekly sale auto-apply", html)
        self.assertIn("3/3 clean gated run(s)", html)
        self.assertIn("Flip the mode to auto_apply_shopify", html)
        self.assertNotIn("<pre", html)

    def test_learning_change_html_uses_change_cards(self) -> None:
        html = notifier.render_notifier_html(
            "learning_change_digest",
            "[Duck Ops Learnings Changed] 2026-04-20",
            "plain body",
            {
                "generated_at": "2026-04-20T08:00:00-04:00",
                "source": "current_learnings",
                "material_change_count": 2,
                "attention_change_count": 1,
                "items": [
                    {
                        "source": "weekly_strategy",
                        "kind": "weekly_strategy_slot_missed",
                        "urgency": "attention",
                        "headline": "Slot 3 has no observed post yet for the planned jeepfact slot.",
                        "detail": "No post was observed for Thursday evening.",
                    }
                ],
            },
        )

        self.assertIn("Slot 3 has no observed post yet", html)
        self.assertIn("weekly_strategy_slot_missed", html)
        self.assertIn("No post was observed for Thursday evening.", html)
        self.assertNotIn("<pre", html)


if __name__ == "__main__":
    unittest.main()

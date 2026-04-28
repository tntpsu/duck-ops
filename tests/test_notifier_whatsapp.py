from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import notifier  # noqa: E402


class NotifierWhatsAppTests(unittest.TestCase):
    def test_load_cached_order_refresh_artifacts_prefers_saved_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            packing_path = Path(tmpdir) / "packing_summary.json"
            refresh_path = Path(tmpdir) / "order_snapshot_refresh.json"
            packing_path.write_text(
                """
                {
                  "generated_at": "2026-04-16T20:12:42-04:00",
                  "counts": {"non_custom_titles": 1},
                  "orders_to_pack": [{"product_title": "Duck", "total_quantity": 2}],
                  "custom_orders_to_make": []
                }
                """,
                encoding="utf-8",
            )
            refresh_path.write_text(
                """
                {
                  "generated_at": "2026-04-16T20:12:42-04:00",
                  "state": "verified",
                  "state_reason": "order_snapshots_fresh",
                  "next_action": "Use the saved packing summary.",
                  "sources": {
                    "etsy": {"status": "live", "generated_at": "2026-04-16T20:12:41-04:00"},
                    "shopify": {"status": "live", "generated_at": "2026-04-16T20:12:42-04:00"}
                  }
                }
                """,
                encoding="utf-8",
            )

            with (
                mock.patch.object(notifier, "PACKING_SUMMARY_PATH", packing_path),
                mock.patch.object(notifier, "ORDER_SNAPSHOT_REFRESH_STATE_PATH", refresh_path),
            ):
                result = notifier.load_cached_order_refresh_artifacts()

        self.assertEqual(result["refresh_state"]["state"], "verified")
        self.assertEqual(result["packing_summary"]["orders_to_pack"][0]["product_title"], "Duck")
        self.assertEqual(result["packing_summary"]["snapshot_refresh"]["state"], "verified")

    def test_load_cached_order_refresh_artifacts_returns_missing_defaults_when_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            packing_path = Path(tmpdir) / "packing_summary.json"
            refresh_path = Path(tmpdir) / "order_snapshot_refresh.json"
            with (
                mock.patch.object(notifier, "PACKING_SUMMARY_PATH", packing_path),
                mock.patch.object(notifier, "ORDER_SNAPSHOT_REFRESH_STATE_PATH", refresh_path),
            ):
                result = notifier.load_cached_order_refresh_artifacts()

        self.assertEqual(result["refresh_state"]["state"], "missing")
        self.assertEqual(result["packing_summary"]["orders_to_pack"], [])
        self.assertEqual(result["packing_summary"]["custom_orders_to_make"], [])

    def test_unique_media_urls_dedupes_and_strips(self) -> None:
        result = notifier.unique_media_urls(
            [" https://example.com/a.png ", "", "https://example.com/a.png", "https://example.com/b.png"]
        )
        self.assertEqual(result, ["https://example.com/a.png", "https://example.com/b.png"])

    @mock.patch.object(notifier, "build_whatsapp_collage")
    def test_prepare_whatsapp_media_urls_prefers_collage(self, build_collage: mock.Mock) -> None:
        build_collage.return_value = Path("/tmp/collage.png")
        settings = {"whatsapp": {"enabled": True}}
        result = notifier.prepare_whatsapp_media_urls(
            settings,
            media_urls=["https://example.com/a.png", "https://example.com/b.png"],
            media_title="Jeep Fact Wednesday",
        )
        self.assertEqual(result, ["/tmp/collage.png"])
        build_collage.assert_called_once()

    @mock.patch.object(notifier.subprocess, "run")
    def test_stage_whatsapp_media_for_container_copies_local_files(self, run_mock: mock.Mock) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            media_path = Path(tmpdir) / "collage.png"
            media_path.write_bytes(b"png")
            result = notifier.stage_whatsapp_media_for_container({"whatsapp": {}}, str(media_path))
        self.assertTrue(result.startswith("/home/node/.openclaw/media/outbound/"))
        self.assertEqual(run_mock.call_count, 2)

    def test_build_message_adds_html_alternative_for_nightly_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            md_path = Path(tmpdir) / "nightly.md"
            json_path = Path(tmpdir) / "nightly.json"
            md_path.write_text("# Duck Ops Tonight\n", encoding="utf-8")
            artifact = {
                "kind": "nightly_action_summary",
                "json_path": json_path,
                "md_path": md_path,
                "payload": {
                    "generated_at": "2026-04-11T20:51:47-04:00",
                    "counts": {
                        "customer_attention_items": 1,
                        "replacement_labels_now": 0,
                        "orders_to_pack_titles": 1,
                        "orders_to_pack_units": 2,
                        "custom_order_lines": 0,
                    },
                    "sections": {
                        "customer_issues_needing_attention": [],
                        "buy_replacement_labels_now": [],
                        "orders_to_pack": [],
                        "custom_novel_ducks_to_make": {},
                        "watch_list": [],
                    },
                },
            }
            msg = notifier.build_message(
                {
                    "subjects": {"nightly_action_summary": "[Duck Ops Tonight] <date>"},
                    "user": "sender@example.com",
                    "to": "ops@example.com",
                },
                artifact,
            )
        html_part = msg.get_body(preferencelist=("html",))
        self.assertIsNotNone(html_part)
        self.assertIn("Duck Ops Tonight", html_part.get_content())
        self.assertEqual(msg["To"], "ops@example.com")

    def test_preview_message_text_reads_plain_part_from_multipart_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            md_path = Path(tmpdir) / "nightly.md"
            json_path = Path(tmpdir) / "nightly.json"
            md_path.write_text("# Duck Ops Tonight\nPlain body preview.\n", encoding="utf-8")
            artifact = {
                "kind": "nightly_action_summary",
                "json_path": json_path,
                "md_path": md_path,
                "payload": {
                    "generated_at": "2026-04-11T20:51:47-04:00",
                    "counts": {
                        "customer_attention_items": 1,
                        "replacement_labels_now": 0,
                        "orders_to_pack_titles": 1,
                        "orders_to_pack_units": 2,
                        "custom_order_lines": 0,
                    },
                    "sections": {
                        "customer_issues_needing_attention": [],
                        "buy_replacement_labels_now": [],
                        "orders_to_pack": [],
                        "custom_novel_ducks_to_make": {},
                        "watch_list": [],
                    },
                },
            }
            msg = notifier.build_message(
                {
                    "subjects": {"nightly_action_summary": "[Duck Ops Tonight] <date>"},
                    "user": "sender@example.com",
                    "to": "ops@example.com",
                },
                artifact,
            )
            preview = notifier.preview_message_text(msg, artifact)

        self.assertIn("Duck Ops Tonight", preview)
        self.assertIn("Plain body preview", preview)

    def test_business_desk_whatsapp_push_uses_business_operator_desk_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            desk_path = Path(tmpdir) / "business_operator_desk.json"
            desk_path.write_text(
                """
                {
                  "generated_at": "2026-04-15T07:20:00-04:00",
                  "counts": {
                    "customer_packets": 2,
                    "etsy_browser_threads": 1,
                    "custom_build_candidates": 0,
                    "orders_to_pack_units": 3,
                    "review_queue_items": 1,
                    "strategy_ready_slots": 1,
                    "workflow_followthrough_items": 2
                  },
                  "next_actions": [
                    {
                      "lane": "customer",
                      "summary": "Answer a buyer question",
                      "command": "desk show customer"
                    }
                  ]
                }
                """,
                encoding="utf-8",
            )

            with mock.patch.object(notifier, "BUSINESS_OPERATOR_DESK_PATH", desk_path):
                result = notifier.build_business_desk_whatsapp_operator_push({})

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["kind"], "operator_whatsapp")
        self.assertEqual(result["media_title"], "Duck Ops Business Desk")
        self.assertIn(notifier.WHATSAPP_PUSH_SENTINEL, result["message"])
        self.assertIn("Customer actions: 2", result["message"])
        self.assertIn("Social plan ready: 1", result["message"])
        self.assertIn("desk show customer", result["message"])

    def test_business_desk_whatsapp_push_sends_for_social_only_desk(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            desk_path = Path(tmpdir) / "business_operator_desk.json"
            desk_path.write_text(
                """
                {
                  "generated_at": "2026-04-15T07:20:00-04:00",
                  "counts": {
                    "customer_packets": 0,
                    "etsy_browser_threads": 0,
                    "custom_build_candidates": 0,
                    "orders_to_pack_units": 0,
                    "review_queue_items": 0,
                    "strategy_ready_slots": 1,
                    "workflow_followthrough_items": 0
                  },
                  "next_actions": [
                    {
                      "lane": "social_plan",
                      "summary": "Monday evening | ready_with_approval | Anchor with the strongest proven workflow",
                      "command": "python src/main_agent.py --flow meme --all",
                      "secondary_command": "Reply publish to the review email"
                    }
                  ]
                }
                """,
                encoding="utf-8",
            )

            with mock.patch.object(notifier, "BUSINESS_OPERATOR_DESK_PATH", desk_path):
                result = notifier.build_business_desk_whatsapp_operator_push({})

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIn("Social plan ready: 1", result["message"])
        self.assertIn("python src/main_agent.py --flow meme --all", result["message"])

    def test_load_sendable_artifacts_includes_promotion_readiness_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            digest_path = Path(tmpdir) / "promotion_readiness.json"
            digest_path.write_text(
                """
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
                      "recommended_action": "Flip the mode and supervise the next run."
                    }
                  ]
                }
                """,
                encoding="utf-8",
            )
            with mock.patch.object(notifier, "PROMOTION_READINESS_DIGEST_PATH", digest_path):
                artifacts = notifier.load_sendable_artifacts({"sent": {}})
                self.assertEqual([item["kind"] for item in artifacts if item["kind"] == "promotion_readiness"], ["promotion_readiness"])
                promotion_artifact = next(item for item in artifacts if item["kind"] == "promotion_readiness")
                signature = promotion_artifact["promotion_readiness_signature"]
                artifacts_again = notifier.load_sendable_artifacts(
                    {
                        "sent": {},
                        "last_promotion_readiness_signature": signature,
                        "last_promotion_readiness_signature_version": notifier.PROMOTION_READINESS_SIGNATURE_VERSION,
                    }
                )
        self.assertFalse(any(item["kind"] == "promotion_readiness" for item in artifacts_again))

    def test_promotion_readiness_sends_state_change_without_ready_candidate(self) -> None:
        payload = {
            "generated_at": "2026-04-19T08:00:00-04:00",
            "source": "business_desk",
            "item_count": 1,
            "ready_item_count": 0,
            "state_change_count": 1,
            "state_changes": [
                {
                    "promotion_id": "meme_auto_schedule",
                    "title": "Meme Monday auto-schedule",
                    "previous_state": "observing",
                    "promotion_state": "blocked",
                    "progress_label": "0/3 clean gated run(s)",
                    "summary": "Meme Monday is blocked by an upstream render dependency.",
                    "recommended_action": "Wait for the upstream quota reset, then rerun the flow.",
                }
            ],
            "items": [
                {
                    "promotion_id": "meme_auto_schedule",
                    "title": "Meme Monday auto-schedule",
                    "previous_state": "observing",
                    "promotion_state": "blocked",
                    "progress_label": "0/3 clean gated run(s)",
                    "summary": "Meme Monday is blocked by an upstream render dependency.",
                    "recommended_action": "Wait for the upstream quota reset, then rerun the flow.",
                }
            ],
        }

        should_send, reason, signature = notifier.should_send_promotion_readiness({}, payload)

        self.assertTrue(should_send)
        self.assertEqual(reason, "promotion_state_changed")
        self.assertTrue(signature)

    def test_build_operator_whatsapp_summary_skips_business_desk_when_disabled(self) -> None:
        settings = {
            "whatsapp": {
                "enabled": True,
                "review_operator_push_enabled": True,
                "business_desk_operator_push_enabled": False,
            }
        }
        with (
            mock.patch.object(notifier, "build_reviews_whatsapp_operator_push", return_value=None) as review_push,
            mock.patch.object(notifier, "build_business_desk_whatsapp_operator_push", return_value={"kind": "operator_whatsapp"}) as desk_push,
        ):
            result = notifier.build_operator_whatsapp_summary(settings, {})

        self.assertIsNone(result)
        review_push.assert_called_once_with({})
        desk_push.assert_not_called()

    def test_build_operator_whatsapp_summary_prefers_review_push_before_business_desk(self) -> None:
        settings = {
            "whatsapp": {
                "enabled": True,
                "review_operator_push_enabled": True,
                "business_desk_operator_push_enabled": True,
            }
        }
        expected = {"kind": "operator_whatsapp", "signature": "abc"}
        with (
            mock.patch.object(notifier, "build_reviews_whatsapp_operator_push", return_value=expected) as review_push,
            mock.patch.object(notifier, "build_business_desk_whatsapp_operator_push", return_value={"kind": "operator_whatsapp"}) as desk_push,
        ):
            result = notifier.build_operator_whatsapp_summary(settings, {})

        self.assertEqual(result, expected)
        review_push.assert_called_once_with({})
        desk_push.assert_not_called()

    def test_build_reviews_whatsapp_operator_push_skips_non_review_quality_gate_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            operator_current_path = Path(tmpdir) / "current_review.json"
            quality_gate_path = Path(tmpdir) / "quality_gate_state.json"
            operator_current_path.write_text("{}", encoding="utf-8")
            quality_gate_path.write_text(
                """
                {
                  "artifacts": {
                    "sale-1": {
                      "decision": {
                        "artifact_id": "sale-1",
                        "flow": "weekly_sale",
                        "title": "Weekly sale draft",
                        "review_status": "pending",
                        "reasoning": ["Looks good"],
                        "confidence": 0.88,
                        "priority": "high",
                        "run_id": "run-001"
                      }
                    },
                    "review-1": {
                      "decision": {
                        "artifact_id": "review-1",
                        "flow": "reviews_reply_positive",
                        "title": "Reply to customer review",
                        "review_status": "pending",
                        "reasoning": ["Helpful and polite"],
                        "confidence": 0.93,
                        "priority": "high",
                        "run_id": "run-002"
                      }
                    }
                  }
                }
                """,
                encoding="utf-8",
            )

            with (
                mock.patch.object(notifier, "OPERATOR_CURRENT_PATH", operator_current_path),
                mock.patch.object(notifier, "QUALITY_GATE_STATE_PATH", quality_gate_path),
            ):
                result = notifier.build_reviews_whatsapp_operator_push({})

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIn("Reply to customer review", result["message"])
        self.assertNotIn("Weekly sale draft", result["message"])

    def test_build_reviews_whatsapp_operator_push_allows_trend_current_item(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            operator_current_path = Path(tmpdir) / "current_review.json"
            quality_gate_path = Path(tmpdir) / "quality_gate_state.json"
            operator_current_path.write_text(
                """
                {
                  "message": "Trend approval needed",
                  "current": {
                    "artifact_id": "trend-1",
                    "artifact_type": "trend",
                    "flow": "trend_ranker",
                    "title": "Monster truck duck trend",
                    "review_status": "pending",
                    "preview": {
                      "asset_url": "https://example.com/trend.png"
                    }
                  }
                }
                """,
                encoding="utf-8",
            )
            quality_gate_path.write_text("{\"artifacts\": {}}", encoding="utf-8")

            with (
                mock.patch.object(notifier, "OPERATOR_CURRENT_PATH", operator_current_path),
                mock.patch.object(notifier, "QUALITY_GATE_STATE_PATH", quality_gate_path),
            ):
                result = notifier.build_reviews_whatsapp_operator_push({})

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIn("Trend approval needed", result["message"])
        self.assertEqual(result["media_urls"], ["https://example.com/trend.png"])

    def test_build_reviews_whatsapp_operator_push_skips_non_review_current_item(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            operator_current_path = Path(tmpdir) / "current_review.json"
            quality_gate_path = Path(tmpdir) / "quality_gate_state.json"
            operator_current_path.write_text(
                """
                {
                  "message": "Weekly sale approval needed",
                  "current": {
                    "artifact_id": "sale-1",
                    "flow": "weekly_sale",
                    "title": "Weekly sale draft",
                    "review_status": "pending",
                    "preview": {
                      "asset_url": "https://example.com/sale.png"
                    }
                  }
                }
                """,
                encoding="utf-8",
            )
            quality_gate_path.write_text("{\"artifacts\": {}}", encoding="utf-8")

            with (
                mock.patch.object(notifier, "OPERATOR_CURRENT_PATH", operator_current_path),
                mock.patch.object(notifier, "QUALITY_GATE_STATE_PATH", quality_gate_path),
            ):
                result = notifier.build_reviews_whatsapp_operator_push({})

        self.assertIsNone(result)

    @mock.patch.object(notifier, "build_business_desk_whatsapp_operator_push", return_value=None)
    @mock.patch.object(notifier, "build_reviews_whatsapp_operator_push", return_value=None)
    @mock.patch.object(notifier, "load_sendable_artifacts", return_value=[])
    @mock.patch.object(notifier, "maybe_auto_approve_weekly_sales", return_value={"changed": False, "results": []})
    @mock.patch.object(notifier, "refresh_learning_change_artifact")
    @mock.patch.object(notifier, "refresh_promotion_readiness_artifact")
    @mock.patch.object(notifier, "refresh_phase_readiness_artifact")
    @mock.patch.object(notifier, "refresh_nightly_action_summary_sources")
    @mock.patch.object(notifier, "notifier_settings", return_value={})
    def test_main_passes_skip_order_refresh_flag(
        self,
        notifier_settings_mock: mock.Mock,
        refresh_summary_mock: mock.Mock,
        refresh_phase_mock: mock.Mock,
        refresh_promotion_mock: mock.Mock,
        refresh_learning_mock: mock.Mock,
        auto_approve_mock: mock.Mock,
        load_artifacts_mock: mock.Mock,
        reviews_push_mock: mock.Mock,
        business_push_mock: mock.Mock,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "notifier_state.json"
            state_path.write_text('{"sent": {}}', encoding="utf-8")
            with (
                mock.patch.object(notifier, "STATE_PATH", state_path),
                mock.patch.object(sys, "argv", ["notifier.py", "--dry-run", "--skip-order-refresh"]),
            ):
                result = notifier.main()

        self.assertEqual(result, 0)
        refresh_summary_mock.assert_called_once_with(
            skip_order_refresh=True,
            skip_customer_refresh_preflight=True,
        )


if __name__ == "__main__":
    unittest.main()

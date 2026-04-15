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
        self.assertIn("desk show customer", result["message"])


if __name__ == "__main__":
    unittest.main()

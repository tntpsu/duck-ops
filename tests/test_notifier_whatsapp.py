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


if __name__ == "__main__":
    unittest.main()

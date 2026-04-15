from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import competitor_social_phase1  # noqa: E402


class CompetitorSocialPhase1Tests(unittest.TestCase):
    def test_build_phase1_payload_surfaces_pending_handles(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config_path = root / "config" / "competitor_social_sources.json"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "platform_scope": ["instagram"],
                        "collection_boundary": {
                            "mode": "public_observe_only",
                            "latest_posts_per_account": 12,
                            "login_allowed": False,
                            "interaction_allowed": False,
                            "bounded_scroll_only": True,
                        },
                        "snapshot_schema": {
                            "required_fields": ["account_name", "post_url"],
                            "optional_fields": ["notes"],
                            "comparison_dimensions": ["theme"],
                        },
                        "seed_accounts": [
                            {
                                "brand_key": "confirmed",
                                "display_name": "Confirmed Brand",
                                "instagram_handle": "confirmed.brand",
                                "verification_status": "confirmed",
                                "confidence": "high",
                                "category": "direct",
                                "reason": "Overlap",
                            },
                            {
                                "brand_key": "pending",
                                "display_name": "Pending Brand",
                                "instagram_handle": None,
                                "verification_status": "needs_exact_handle",
                                "confidence": "medium",
                                "category": "adjacent",
                                "reason": "Need handle",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            state_path = root / "state" / "competitor_social_phase1.json"
            operator_json_path = root / "output" / "operator" / "competitor_social_phase1.json"
            output_md_path = root / "output" / "operator" / "competitor_social_phase1.md"

            with patch.object(competitor_social_phase1, "CONFIG_PATH", config_path), patch.object(
                competitor_social_phase1, "STATE_PATH", state_path
            ), patch.object(competitor_social_phase1, "OPERATOR_JSON_PATH", operator_json_path), patch.object(
                competitor_social_phase1, "OUTPUT_MD_PATH", output_md_path
            ):
                payload = competitor_social_phase1.build_competitor_social_phase1()
                self.assertTrue(state_path.exists())
                self.assertTrue(operator_json_path.exists())
                self.assertTrue(output_md_path.exists())

        self.assertEqual(payload["summary"]["seed_account_count"], 2)
        self.assertEqual(payload["summary"]["confirmed_handle_count"], 1)
        self.assertEqual(payload["summary"]["verification_needed_count"], 1)
        self.assertEqual(payload["open_verification_items"][0]["display_name"], "Pending Brand")

    def test_render_markdown_mentions_remaining_cleanups(self) -> None:
        markdown = competitor_social_phase1.render_competitor_social_phase1_markdown(
            {
                "generated_at": "2026-04-15T10:00:00-04:00",
                "summary": {
                    "seed_account_count": 2,
                    "confirmed_handle_count": 1,
                    "verification_needed_count": 1,
                    "latest_posts_per_account": 12,
                    "headline": "Phase 1 foundation",
                },
                "collection_boundary": {
                    "mode": "public_observe_only",
                    "login_allowed": False,
                    "interaction_allowed": False,
                    "bounded_scroll_only": True,
                },
                "seed_accounts": [
                    {
                        "display_name": "Confirmed Brand",
                        "instagram_handle": "confirmed.brand",
                        "verification_status": "confirmed",
                        "confidence": "high",
                        "category": "direct",
                        "reason": "Overlap",
                    }
                ],
                "snapshot_schema": {
                    "required_fields": ["account_name"],
                    "optional_fields": ["notes"],
                    "comparison_dimensions": ["theme"],
                },
                "open_verification_items": [
                    {"display_name": "Pending Brand", "verification_status": "needs_exact_handle"}
                ],
                "recommended_next_step": {"title": "Build collector", "notes": ["Use confirmed handles only."]},
            }
        )
        self.assertIn("Remaining Account Cleanups", markdown)
        self.assertIn("Pending Brand", markdown)
        self.assertIn("Build collector", markdown)


if __name__ == "__main__":
    unittest.main()

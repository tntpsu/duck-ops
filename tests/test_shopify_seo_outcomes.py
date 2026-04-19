from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import shopify_seo_outcomes


class ShopifySeoOutcomesTests(unittest.TestCase):
    def test_target_issue_codes_expand_new_category_batches(self) -> None:
        codes = shopify_seo_outcomes._target_issue_codes(
            {"seo_category": "weak_title"},
            {
                "issues": [{"code": "seo_title_matches_raw_title"}],
                "apply_seo_title": True,
                "apply_seo_description": False,
            },
        )
        self.assertEqual(codes, ["seo_title_matches_raw_title", "weak_generic_seo_title"])

    def test_build_shopify_seo_outcomes_classifies_recent_stable_and_open_items(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            audit_path = root / "state" / "shopify_seo_audit.json"
            review_run_dir = root / "state" / "shopify_seo_review" / "runs"
            state_path = root / "state" / "shopify_seo_outcomes.json"
            operator_json_path = root / "output" / "operator" / "shopify_seo_outcomes.json"
            markdown_path = root / "output" / "operator" / "shopify_seo_outcomes.md"
            review_run_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now().astimezone()
            older_applied_at = (now - timedelta(days=12)).isoformat()
            recent_applied_at = (now - timedelta(days=2)).isoformat()
            open_applied_at = (now - timedelta(days=9)).isoformat()

            audit_path.write_text(
                json.dumps(
                    {
                        "generated_at": now.isoformat(),
                        "resources": [
                            {
                                "id": "gid://shopify/Product/1",
                                "title": "Stable Duck",
                                "resource_url": "/products/stable-duck",
                                "seo_title": "Stable Duck | MyJeepDuck Collectible Duck Gift",
                                "seo_description": "A stable duck description.",
                                "issues": [],
                            },
                            {
                                "id": "gid://shopify/Product/2",
                                "title": "Recent Duck",
                                "resource_url": "/products/recent-duck",
                                "seo_title": "Recent Duck | MyJeepDuck Collectible Duck Gift",
                                "seo_description": "A recent duck description.",
                                "issues": [],
                            },
                            {
                                "id": "gid://shopify/Product/3",
                                "title": "Open Duck",
                                "resource_url": "/products/open-duck",
                                "seo_title": "",
                                "seo_description": "Open issue description.",
                                "issues": [
                                    {
                                        "code": "missing_seo_title",
                                        "severity": "high",
                                        "message": "Missing SEO title.",
                                    }
                                ],
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )

            review_run_dir.joinpath("stable.json").write_text(
                json.dumps(
                    {
                        "run_id": "stable",
                        "seo_category": "missing_title",
                        "category_label": "Missing SEO titles",
                        "status": "applied",
                        "apply_result": {"applied_at": older_applied_at},
                        "items": [
                            {
                                "id": "gid://shopify/Product/1",
                                "kind": "product",
                                "title": "Stable Duck",
                                "resource_url": "/products/stable-duck",
                                "issues": [{"code": "missing_seo_title"}],
                                "apply_seo_title": True,
                                "apply_seo_description": False,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            review_run_dir.joinpath("recent.json").write_text(
                json.dumps(
                    {
                        "run_id": "recent",
                        "seo_category": "short_title",
                        "category_label": "SEO titles too short",
                        "status": "applied",
                        "apply_result": {"applied_at": recent_applied_at},
                        "items": [
                            {
                                "id": "gid://shopify/Product/2",
                                "kind": "product",
                                "title": "Recent Duck",
                                "resource_url": "/products/recent-duck",
                                "issues": [{"code": "short_seo_title"}],
                                "apply_seo_title": True,
                                "apply_seo_description": False,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            review_run_dir.joinpath("open.json").write_text(
                json.dumps(
                    {
                        "run_id": "open",
                        "seo_category": "missing_title",
                        "category_label": "Missing SEO titles",
                        "status": "applied",
                        "apply_result": {"applied_at": open_applied_at},
                        "items": [
                            {
                                "id": "gid://shopify/Product/3",
                                "kind": "product",
                                "title": "Open Duck",
                                "resource_url": "/products/open-duck",
                                "issues": [{"code": "missing_seo_title"}],
                                "apply_seo_title": True,
                                "apply_seo_description": False,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(shopify_seo_outcomes, "SEO_AUDIT_PATH", audit_path), patch.object(
                shopify_seo_outcomes, "SEO_REVIEW_RUN_DIR", review_run_dir
            ), patch.object(
                shopify_seo_outcomes, "SEO_OUTCOMES_STATE_PATH", state_path
            ), patch.object(
                shopify_seo_outcomes, "SEO_OUTCOMES_OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                shopify_seo_outcomes, "SEO_OUTCOMES_MD_PATH", markdown_path
            ):
                payload = shopify_seo_outcomes.build_shopify_seo_outcomes()

            self.assertEqual(payload["summary"]["applied_item_count"], 3)
            self.assertEqual(payload["summary"]["stable_count"], 1)
            self.assertEqual(payload["summary"]["monitoring_count"], 1)
            self.assertEqual(payload["summary"]["issue_still_present_count"], 1)
            self.assertEqual(payload["summary"]["traffic_signal_available_count"], 0)
            self.assertEqual(payload["attention_items"][0]["title"], "Open Duck")
            self.assertEqual(payload["attention_items"][0]["status"], "issue_still_present")
            self.assertEqual(payload["recent_wins"][0]["status"], "stable")
            self.assertTrue(state_path.exists())
            self.assertTrue(operator_json_path.exists())
            self.assertTrue(markdown_path.exists())
            markdown = markdown_path.read_text(encoding="utf-8")
            self.assertIn("## Needs Attention", markdown)
            self.assertIn("Open Duck", markdown)
            self.assertIn("## Recent Wins", markdown)
            self.assertIn("Stable Duck", markdown)
            self.assertIn("Traffic signals available: `0`", markdown)

    def test_build_shopify_seo_outcomes_marks_items_awaiting_audit_refresh(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            audit_path = root / "state" / "shopify_seo_audit.json"
            review_run_dir = root / "state" / "shopify_seo_review" / "runs"
            state_path = root / "state" / "shopify_seo_outcomes.json"
            operator_json_path = root / "output" / "operator" / "shopify_seo_outcomes.json"
            markdown_path = root / "output" / "operator" / "shopify_seo_outcomes.md"
            review_run_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now().astimezone()
            audit_generated_at = (now - timedelta(days=1)).isoformat()
            applied_at = now.isoformat()

            audit_path.write_text(
                json.dumps(
                    {
                        "generated_at": audit_generated_at,
                        "resources": [
                            {
                                "id": "gid://shopify/Product/9",
                                "title": "Audit Refresh Duck",
                                "resource_url": "/products/audit-refresh-duck",
                                "seo_title": "Audit Refresh Duck",
                                "seo_description": "Old audit payload.",
                                "issues": [{"code": "missing_seo_title"}],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            review_run_dir.joinpath("awaiting.json").write_text(
                json.dumps(
                    {
                        "run_id": "awaiting",
                        "seo_category": "missing_title",
                        "status": "applied",
                        "apply_result": {"applied_at": applied_at},
                        "items": [
                            {
                                "id": "gid://shopify/Product/9",
                                "kind": "product",
                                "title": "Audit Refresh Duck",
                                "resource_url": "/products/audit-refresh-duck",
                                "issues": [{"code": "missing_seo_title"}],
                                "apply_seo_title": True,
                                "apply_seo_description": False,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(shopify_seo_outcomes, "SEO_AUDIT_PATH", audit_path), patch.object(
                shopify_seo_outcomes, "SEO_REVIEW_RUN_DIR", review_run_dir
            ), patch.object(
                shopify_seo_outcomes, "SEO_OUTCOMES_STATE_PATH", state_path
            ), patch.object(
                shopify_seo_outcomes, "SEO_OUTCOMES_OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                shopify_seo_outcomes, "SEO_OUTCOMES_MD_PATH", markdown_path
            ):
                payload = shopify_seo_outcomes.build_shopify_seo_outcomes()

            self.assertEqual(payload["summary"]["awaiting_audit_refresh_count"], 1)
            self.assertEqual(payload["attention_items"][0]["status"], "awaiting_audit_refresh")
            self.assertIn("has not been rechecked yet", payload["attention_items"][0]["verification_note"])


if __name__ == "__main__":
    unittest.main()

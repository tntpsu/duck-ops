from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import review_loop


class ReviewLoopReconciliationTests(unittest.TestCase):
    def test_reconcile_quality_gate_archives_superseded_newduck_runs(self) -> None:
        state = {
            "artifacts": {
                "publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck": {
                    "artifact_id": "publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck",
                    "decision": {
                        "artifact_id": "publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck",
                        "artifact_type": "listing",
                        "flow": "newduck",
                        "run_id": "orange-cat-duck-2026-04-21-2116",
                        "review_status": "pending",
                        "created_at": "2026-04-21T21:48:29-04:00",
                        "title": "Orange Cat Duck",
                    },
                }
            }
        }

        workflow_states = [
            {
                "workflow_id": "newduck::orange-cat-duck-2026-04-21-2116",
                "lane": "newduck",
                "run_id": "orange-cat-duck-2026-04-21-2116",
                "state": "proposed",
                "updated_at": "2026-04-21T21:48:29-04:00",
                "_path": "/tmp/newduck-orange-cat-duck-2026-04-21-2116.json",
            },
            {
                "workflow_id": "newduck::orange-cat-duck-2026-04-21-2308",
                "lane": "newduck",
                "run_id": "orange-cat-duck-2026-04-21-2308",
                "state": "verified",
                "state_reason": "shopify_activated",
                "updated_at": "2026-04-21T23:49:35-04:00",
                "_path": "/tmp/newduck-orange-cat-duck-2026-04-21-2308.json",
            },
        ]

        with (
            patch.object(review_loop, "latest_override_index", return_value={}),
            patch.object(review_loop, "duckagent_publish_reconciliation", return_value=None),
            patch.object(review_loop, "list_workflow_states", return_value=workflow_states),
            patch.object(review_loop, "now_iso", return_value="2026-04-22T00:10:00-04:00"),
        ):
            changed = review_loop.reconcile_quality_gate_state(state)

        self.assertTrue(changed)
        decision = state["artifacts"]["publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck"]["decision"]
        self.assertEqual(decision["review_status"], "archived")
        self.assertEqual(decision["archive_reason"], "superseded by newer newduck run")
        self.assertEqual(decision["superseded_by_run_id"], "orange-cat-duck-2026-04-21-2308")
        self.assertEqual(decision["human_review"]["resolution"], "superseded")
        self.assertIn("newer newduck run `orange-cat-duck-2026-04-21-2308`", decision["human_review"]["note"])

    def test_reconcile_quality_gate_leaves_unrelated_newduck_runs_pending(self) -> None:
        state = {
            "artifacts": {
                "publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck": {
                    "artifact_id": "publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck",
                    "decision": {
                        "artifact_id": "publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck",
                        "artifact_type": "listing",
                        "flow": "newduck",
                        "run_id": "orange-cat-duck-2026-04-21-2116",
                        "review_status": "pending",
                        "created_at": "2026-04-21T21:48:29-04:00",
                        "title": "Orange Cat Duck",
                    },
                }
            }
        }

        workflow_states = [
            {
                "workflow_id": "newduck::monster-truck-duck-2026-04-21-2308",
                "lane": "newduck",
                "run_id": "monster-truck-duck-2026-04-21-2308",
                "state": "verified",
                "state_reason": "shopify_activated",
                "updated_at": "2026-04-21T23:49:35-04:00",
                "_path": "/tmp/newduck-monster-truck-duck-2026-04-21-2308.json",
            }
        ]

        with (
            patch.object(review_loop, "latest_override_index", return_value={}),
            patch.object(review_loop, "duckagent_publish_reconciliation", return_value=None),
            patch.object(review_loop, "list_workflow_states", return_value=workflow_states),
        ):
            changed = review_loop.reconcile_quality_gate_state(state)

        self.assertFalse(changed)
        decision = state["artifacts"]["publish::newduck::orange-cat-duck-2026-04-21-2116::orange-cat-duck"]["decision"]
        self.assertEqual(decision["review_status"], "pending")

    def test_reconcile_quality_gate_archives_same_product_hardened_run_ids(self) -> None:
        state = {
            "artifacts": {
                "publish::newduck::2026-04-19_monstertruckduck_hardened9::monster-truck-duck": {
                    "artifact_id": "publish::newduck::2026-04-19_monstertruckduck_hardened9::monster-truck-duck",
                    "decision": {
                        "artifact_id": "publish::newduck::2026-04-19_monstertruckduck_hardened9::monster-truck-duck",
                        "artifact_type": "listing",
                        "flow": "newduck",
                        "run_id": "2026-04-19_monstertruckduck_hardened9",
                        "review_status": "pending",
                        "created_at": "2026-04-19T20:11:39-04:00",
                        "title": "Monster Truck Duck - 3D Printed Collectible Duck for Display",
                    },
                }
            }
        }

        workflow_states = [
            {
                "workflow_id": "newduck::2026-04-19_monstertruckduck_publish1",
                "lane": "newduck",
                "run_id": "2026-04-19_monstertruckduck_publish1",
                "state": "verified",
                "state_reason": "shopify_activated",
                "updated_at": "2026-04-21T23:49:35-04:00",
                "metadata": {"duck_name": "Monster Truck Duck", "shopify_activated": True},
                "_path": "/tmp/newduck-monster-truck-publish1.json",
            },
        ]

        with (
            patch.object(review_loop, "latest_override_index", return_value={}),
            patch.object(review_loop, "duckagent_publish_reconciliation", return_value=None),
            patch.object(review_loop, "list_workflow_states", return_value=workflow_states),
            patch.object(review_loop, "now_iso", return_value="2026-04-22T00:10:00-04:00"),
        ):
            changed = review_loop.reconcile_quality_gate_state(state)

        self.assertTrue(changed)
        decision = state["artifacts"]["publish::newduck::2026-04-19_monstertruckduck_hardened9::monster-truck-duck"]["decision"]
        self.assertEqual(decision["review_status"], "archived")
        self.assertEqual(decision["archive_reason"], "superseded by newer newduck run")
        self.assertEqual(decision["superseded_by_run_id"], "2026-04-19_monstertruckduck_publish1")

    def test_reconcile_quality_gate_archives_older_same_product_review_artifact(self) -> None:
        state = {
            "artifacts": {
                "publish::newduck::2026-05-05-mrcleanduck-v2::mr-clean-duck": {
                    "artifact_id": "publish::newduck::2026-05-05-mrcleanduck-v2::mr-clean-duck",
                    "decision": {
                        "artifact_id": "publish::newduck::2026-05-05-mrcleanduck-v2::mr-clean-duck",
                        "artifact_type": "listing",
                        "flow": "newduck",
                        "run_id": "2026-05-05-mrcleanduck-v2",
                        "review_status": "pending",
                        "created_at": "2026-05-05T12:00:00-04:00",
                        "title": "Mr Clean Duck",
                    },
                },
                "publish::newduck::2026-05-05-mrcleanduck-v6::mr-clean-duck": {
                    "artifact_id": "publish::newduck::2026-05-05-mrcleanduck-v6::mr-clean-duck",
                    "decision": {
                        "artifact_id": "publish::newduck::2026-05-05-mrcleanduck-v6::mr-clean-duck",
                        "artifact_type": "listing",
                        "flow": "newduck",
                        "run_id": "2026-05-05-mrcleanduck-v6",
                        "review_status": "pending",
                        "created_at": "2026-05-05T13:00:00-04:00",
                        "title": "Cleaning Duck Figurine",
                    },
                },
            }
        }

        with (
            patch.object(review_loop, "latest_override_index", return_value={}),
            patch.object(review_loop, "duckagent_publish_reconciliation", return_value=None),
            patch.object(review_loop, "list_workflow_states", return_value=[]),
            patch.object(review_loop, "now_iso", return_value="2026-05-05T14:00:00-04:00"),
        ):
            changed = review_loop.reconcile_quality_gate_state(state)

        self.assertTrue(changed)
        older = state["artifacts"]["publish::newduck::2026-05-05-mrcleanduck-v2::mr-clean-duck"]["decision"]
        newer = state["artifacts"]["publish::newduck::2026-05-05-mrcleanduck-v6::mr-clean-duck"]["decision"]
        self.assertEqual(older["review_status"], "archived")
        self.assertEqual(older["archive_reason"], "superseded by newer newduck review artifact")
        self.assertEqual(older["superseded_by_artifact_id"], "publish::newduck::2026-05-05-mrcleanduck-v6::mr-clean-duck")
        self.assertEqual(newer["review_status"], "pending")

    def test_build_review_items_suppresses_raw_relationship_trend_names(self) -> None:
        state_bundle = {
            "quality_gate": {"artifacts": {}},
            "trend_ranker": {
                "artifacts": {
                    "trend::child-maternal-love-duck::2026-05-08": {
                        "artifact_id": "trend::child-maternal-love-duck::2026-05-08",
                        "decision": {
                            "artifact_id": "trend::child-maternal-love-duck::2026-05-08",
                            "artifact_type": "trend",
                            "theme": "child maternal love duck",
                            "title": "Child Maternal Love Duck",
                            "decision": "worth_acting_on",
                            "action_frame": "build",
                            "review_status": "pending",
                            "score": 80,
                            "confidence": 0.6,
                            "priority": "high",
                            "created_at": "2026-05-08T09:00:00-04:00",
                            "trend_metadata": {"catalog_status": "gap"},
                        },
                    },
                    "trend::greyhound-duck::2026-05-08": {
                        "artifact_id": "trend::greyhound-duck::2026-05-08",
                        "decision": {
                            "artifact_id": "trend::greyhound-duck::2026-05-08",
                            "artifact_type": "trend",
                            "theme": "greyhound duck",
                            "title": "Greyhound Duck",
                            "decision": "worth_acting_on",
                            "action_frame": "build",
                            "review_status": "pending",
                            "score": 80,
                            "confidence": 0.6,
                            "priority": "high",
                            "created_at": "2026-05-08T09:00:00-04:00",
                            "trend_metadata": {"catalog_status": "gap"},
                        },
                    },
                }
            },
        }

        items = review_loop.build_review_items(state_bundle)
        titles = {item["title"] for item in items}

        self.assertIn("Greyhound Duck", titles)
        self.assertNotIn("Child Maternal Love Duck", titles)


if __name__ == "__main__":
    unittest.main()

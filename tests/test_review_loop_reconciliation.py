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

import review_loop


def write_duckagent_state(root: Path, run_id: str, filename: str, payload: dict) -> Path:
    state_path = root / run_id / filename
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return state_path


class ReviewLoopReconciliationTests(unittest.TestCase):
    def test_duckagent_publish_reconciliation_detects_recurring_social_proofs(self) -> None:
        cases = [
            (
                {"flow": "meme", "artifact_type": "social_post", "run_id": "meme_20260518"},
                "state_meme.json",
                {"meme_publish_status": "scheduled", "meme_scheduled_at": "2026-05-18T19:00:00-04:00"},
                "meme as scheduled",
            ),
            (
                {"flow": "jeepfact", "artifact_type": "social_post", "run_id": "jeepfact_20260520"},
                "state_jeepfact.json",
                {"jeepfact_publish_status": "partial", "jeepfact_scheduled_at": "2026-05-20T19:00:00-04:00"},
                "Jeep Fact post as scheduled",
            ),
            (
                {"flow": "thursday", "artifact_type": "social_post", "run_id": "thursday_20260521"},
                "state_thursday.json",
                {"thursday_published": True, "thursday_publish_time": "2026-05-21T19:00:00-04:00"},
                "Thursday post as published",
            ),
            (
                {"flow": "gtdf", "artifact_type": "social_post", "run_id": "gtdf_20260523"},
                "state_gtdf.json",
                {"gtdf_scheduled_at": "2026-05-23T11:00:00-04:00"},
                "GTDF post as scheduled",
            ),
            (
                {"flow": "reviews_story", "artifact_type": "social_post", "run_id": "reviews_20260517"},
                "state_reviews.json",
                {"reviews_story_published": True, "reviews_story_published_at": "2026-05-17T09:15:00-04:00"},
                "review story as sent",
            ),
        ]

        with TemporaryDirectory() as temp_dir:
            runs_root = Path(temp_dir)
            for decision, filename, payload, expected_note in cases:
                write_duckagent_state(runs_root, decision["run_id"], filename, payload)

            with patch.object(review_loop, "DUCK_AGENT_RUNS_DIR", runs_root):
                for decision, _filename, _payload, expected_note in cases:
                    with self.subTest(flow=decision["flow"]):
                        result = review_loop.duckagent_publish_reconciliation(decision)

                    self.assertIsNotNone(result)
                    self.assertEqual(result["resolution"], "approve")
                    self.assertIn(expected_note, result["note"])
                    self.assertIn(str(runs_root / decision["run_id"]), result["source"])

    def test_duckagent_publish_reconciliation_ignores_failed_or_unproven_state(self) -> None:
        with TemporaryDirectory() as temp_dir:
            runs_root = Path(temp_dir)
            write_duckagent_state(
                runs_root,
                "meme_20260518",
                "state_meme.json",
                {"meme_publish_status": "failed", "meme_scheduled_at": "2026-05-18T19:00:00-04:00"},
            )
            write_duckagent_state(
                runs_root,
                "jeepfact_20260520",
                "state_jeepfact.json",
                {"jeepfact_publish_status": "blocked"},
            )

            with patch.object(review_loop, "DUCK_AGENT_RUNS_DIR", runs_root):
                self.assertIsNone(
                    review_loop.duckagent_publish_reconciliation(
                        {"flow": "meme", "artifact_type": "social_post", "run_id": "meme_20260518"}
                    )
                )
                self.assertIsNone(
                    review_loop.duckagent_publish_reconciliation(
                        {"flow": "jeepfact", "artifact_type": "social_post", "run_id": "jeepfact_20260520"}
                    )
                )

    def test_reconcile_quality_gate_approves_email_published_social_decision(self) -> None:
        state = {
            "artifacts": {
                "publish::jeepfact::2026-05-20::jeep-fact-wednesday": {
                    "artifact_id": "publish::jeepfact::2026-05-20::jeep-fact-wednesday",
                    "decision": {
                        "artifact_id": "publish::jeepfact::2026-05-20::jeep-fact-wednesday",
                        "artifact_type": "social_post",
                        "flow": "jeepfact",
                        "run_id": "2026-05-20",
                        "review_status": "pending",
                        "created_at": "2026-05-20T09:00:00-04:00",
                        "title": "Jeep Fact Wednesday",
                    },
                }
            }
        }

        with TemporaryDirectory() as temp_dir:
            runs_root = Path(temp_dir)
            state_path = write_duckagent_state(
                runs_root,
                "2026-05-20",
                "state_jeepfact.json",
                {"jeepfact_publish_status": "scheduled", "jeepfact_scheduled_at": "2026-05-20T19:00:00-04:00"},
            )

            with (
                patch.object(review_loop, "DUCK_AGENT_RUNS_DIR", runs_root),
                patch.object(review_loop, "latest_override_index", return_value={}),
                patch.object(review_loop, "now_iso", return_value="2026-05-20T19:05:00-04:00"),
            ):
                changed = review_loop.reconcile_quality_gate_state(state)

        self.assertTrue(changed)
        decision = state["artifacts"]["publish::jeepfact::2026-05-20::jeep-fact-wednesday"]["decision"]
        self.assertEqual(decision["review_status"], "approved")
        self.assertEqual(decision["human_review"]["resolution"], "approve")
        self.assertEqual(decision["reconciled_resolution"]["source"], str(state_path))

    def test_record_decision_and_dispatch_records_email_decision_before_duckagent_handoff(self) -> None:
        state_bundle = {
            "quality_gate": {
                "artifacts": {
                    "publish::jeepfact::2026-05-20::jeep-fact-wednesday": {
                        "artifact_id": "publish::jeepfact::2026-05-20::jeep-fact-wednesday",
                        "decision": {
                            "artifact_id": "publish::jeepfact::2026-05-20::jeep-fact-wednesday",
                            "artifact_type": "social_post",
                            "flow": "jeepfact",
                            "run_id": "2026-05-20",
                            "review_status": "pending",
                            "decision": "publish_ready",
                            "created_at": "2026-05-20T09:00:00-04:00",
                            "title": "Jeep Fact Wednesday",
                        },
                    }
                }
            },
            "trend_ranker": {"artifacts": {}},
        }
        dispatch_calls: list[dict] = []

        def fake_invoke(**kwargs):
            dispatch_calls.append(kwargs)
            return {"ok": True, "returncode": 0, "stdout": "scheduled", "stderr": "", "command": ["duckagent"]}

        with (
            patch.object(review_loop, "load_state_bundle", return_value=state_bundle),
            patch.object(review_loop, "load_operator_state", return_value={}),
            patch.object(review_loop, "reconcile_state_bundle", return_value=False),
            patch.object(review_loop, "write_state_source", return_value=None),
            patch.object(review_loop, "write_review_queue", return_value=None),
            patch.object(review_loop, "write_operator_state", return_value=None),
            patch.object(review_loop, "write_decision", return_value={}),
            patch.object(review_loop, "invoke_duckagent_mail_event", side_effect=fake_invoke),
            patch.object(review_loop, "now_iso", return_value="2026-05-20T19:05:00-04:00"),
        ):
            result = review_loop.record_decision_and_dispatch(
                flow="jeepfact",
                run_id="2026-05-20",
                action="publish",
                note="publish",
                channel="email",
            )

        decision = state_bundle["quality_gate"]["artifacts"]["publish::jeepfact::2026-05-20::jeep-fact-wednesday"]["decision"]
        self.assertTrue(result["handled"])
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "recorded_and_dispatched")
        self.assertEqual(decision["review_status"], "approved")
        self.assertEqual(decision["decision_gateway"]["channel"], "email")
        self.assertEqual(decision["decision_gateway"]["operator_action"], "approve")
        self.assertEqual(decision["execution_state"], "publish_requested")
        self.assertEqual(dispatch_calls[0]["flow"], "jeepfact")
        self.assertEqual(dispatch_calls[0]["action"], "publish")

    def test_record_decision_and_dispatch_does_not_duplicate_resolved_decision(self) -> None:
        state_bundle = {
            "quality_gate": {
                "artifacts": {
                    "publish::meme::2026-05-18::meme-monday": {
                        "artifact_id": "publish::meme::2026-05-18::meme-monday",
                        "decision": {
                            "artifact_id": "publish::meme::2026-05-18::meme-monday",
                            "artifact_type": "social_post",
                            "flow": "meme",
                            "run_id": "2026-05-18",
                            "review_status": "approved",
                            "decision": "publish_ready",
                            "title": "Meme Monday",
                        },
                    }
                }
            },
            "trend_ranker": {"artifacts": {}},
        }

        with (
            patch.object(review_loop, "load_state_bundle", return_value=state_bundle),
            patch.object(review_loop, "load_operator_state", return_value={}),
            patch.object(review_loop, "reconcile_state_bundle", return_value=False),
            patch.object(review_loop, "invoke_duckagent_mail_event") as invoke,
            patch.object(review_loop, "now_iso", return_value="2026-05-18T19:05:00-04:00"),
        ):
            result = review_loop.record_decision_and_dispatch(
                flow="meme",
                run_id="2026-05-18",
                action="publish",
                note="publish",
                channel="email",
            )

        self.assertTrue(result["handled"])
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "already_resolved")
        invoke.assert_not_called()

    def test_invoke_duckagent_mail_event_sets_gateway_bypass_env(self) -> None:
        captured: dict = {}

        class FakeCompleted:
            returncode = 0
            stdout = "ok"
            stderr = ""

        def fake_run(command, **kwargs):
            captured["command"] = command
            captured["env"] = kwargs.get("env") or {}
            return FakeCompleted()

        with patch.object(review_loop.subprocess, "run", side_effect=fake_run):
            result = review_loop.invoke_duckagent_mail_event(
                flow="jeepfact",
                run_id="2026-05-20",
                title="Jeep Fact Wednesday",
                action="publish",
                note="publish",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(captured["env"].get("DUCK_OPS_DECISION_GATEWAY_BYPASS"), "1")
        self.assertIn("src/main_agent.py", " ".join(captured["command"]))

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

    def test_handle_operator_text_skips_recurring_social_without_rejecting(self) -> None:
        state_bundle = {
            "quality_gate": {
                "artifacts": {
                    "publish::jeepfact::2026-05-13::jeep-fact-wednesday": {
                        "artifact_id": "publish::jeepfact::2026-05-13::jeep-fact-wednesday",
                        "decision": {
                            "artifact_id": "publish::jeepfact::2026-05-13::jeep-fact-wednesday",
                            "artifact_type": "social_post",
                            "flow": "jeepfact",
                            "run_id": "2026-05-13",
                            "review_status": "pending",
                            "decision": "publish_ready",
                            "score": 85,
                            "confidence": 0.6,
                            "priority": "medium",
                            "created_at": "2026-05-13T09:00:00-04:00",
                            "title": "Jeep Fact Wednesday",
                        },
                    },
                },
            },
            "trend_ranker": {"artifacts": {}},
        }
        operator_state: dict = {}

        with (
            patch.object(review_loop, "write_review_queue", return_value=None),
            patch.object(review_loop, "write_state_source", return_value=None),
            patch.object(review_loop, "write_decision", return_value={}),
            patch.object(review_loop, "now_iso", return_value="2026-05-15T19:00:00-04:00"),
        ):
            response = review_loop.handle_operator_text(state_bundle, operator_state, "skip")

        decision = state_bundle["quality_gate"]["artifacts"]["publish::jeepfact::2026-05-13::jeep-fact-wednesday"]["decision"]
        self.assertIn("-> skip this occurrence.", response)
        self.assertEqual(decision["review_status"], "archived")
        self.assertEqual(decision["human_review"]["action"], "archive")
        self.assertEqual(decision["human_review"]["resolution"], "skip")
        self.assertEqual(decision["operator_resolution"]["action"], "skip")
        self.assertEqual(decision["archive_reason"], "Skipped this occurrence so the next scheduled run can generate a fresh candidate.")

    def test_handle_operator_text_records_channel_receipt_metadata(self) -> None:
        state_bundle = {
            "quality_gate": {
                "artifacts": {
                    "publish::jeepfact::2026-05-13::jeep-fact-wednesday": {
                        "artifact_id": "publish::jeepfact::2026-05-13::jeep-fact-wednesday",
                        "decision": {
                            "artifact_id": "publish::jeepfact::2026-05-13::jeep-fact-wednesday",
                            "artifact_type": "social_post",
                            "flow": "jeepfact",
                            "run_id": "2026-05-13",
                            "review_status": "pending",
                            "decision": "publish_ready",
                            "score": 85,
                            "confidence": 0.6,
                            "priority": "medium",
                            "created_at": "2026-05-13T09:00:00-04:00",
                            "title": "Jeep Fact Wednesday",
                        },
                    },
                },
            },
            "trend_ranker": {"artifacts": {}},
        }
        operator_state: dict = {}

        with (
            patch.object(review_loop, "write_review_queue", return_value=None),
            patch.object(review_loop, "write_state_source", return_value=None),
            patch.object(review_loop, "write_decision", return_value={}),
            patch.object(review_loop, "now_iso", return_value="2026-05-17T11:00:00-04:00"),
        ):
            response = review_loop.handle_operator_text(state_bundle, operator_state, "skip", channel="portal")

        decision = state_bundle["quality_gate"]["artifacts"]["publish::jeepfact::2026-05-13::jeep-fact-wednesday"]["decision"]
        self.assertIn("Recorded:", response)
        self.assertEqual(decision["human_review"]["channel"], "portal")
        self.assertEqual(decision["operator_resolution"]["channel"], "portal")


if __name__ == "__main__":
    unittest.main()

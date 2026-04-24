from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import etsy_browser_batch  # noqa: E402


class EtsyBrowserBatchTests(unittest.TestCase):
    def test_build_daily_schedule_creates_three_slots_and_one_relist_slot(self) -> None:
        schedule = etsy_browser_batch.build_daily_schedule(
            now=datetime.fromisoformat("2026-04-23T00:10:00-04:00"),
            rng=__import__("random").Random(7),
        )

        self.assertEqual(schedule["date_local"], "2026-04-23")
        self.assertEqual(len(schedule["slots"]), 3)
        relist_slots = [slot for slot in schedule["slots"] if slot.get("relist_slot")]
        self.assertEqual(len(relist_slots), 1)
        self.assertEqual(schedule["relist_slot_id"], relist_slots[0]["slot_id"])

    def test_check_and_run_launches_due_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            schedule_path = root / "etsy_browser_schedule.json"
            latest_path = root / "etsy_browser_batch_latest.json"
            operator_json = root / "operator_schedule.json"
            operator_md = root / "operator_schedule.md"
            history_path = root / "etsy_browser_batch_history.jsonl"
            batch_runner = root / "run_duck_ops_etsy_browser_batch.sh"
            batch_runner.write_text("#!/bin/zsh\n", encoding="utf-8")

            schedule = {
                "generated_at": "2026-04-23T00:10:00-04:00",
                "date_local": "2026-04-23",
                "timezone": "EDT",
                "checker_interval_minutes": 15,
                "due_grace_minutes": 20,
                "relist_slot_id": "morning",
                "slots": [
                    {
                        "slot_id": "morning",
                        "label": "Morning",
                        "window_start": "2026-04-23T09:00:00-04:00",
                        "window_end": "2026-04-23T10:30:00-04:00",
                        "scheduled_for": "2026-04-23T09:15:00-04:00",
                        "status": "pending",
                        "relist_slot": True,
                    }
                ],
            }
            schedule_path.write_text(json.dumps(schedule), encoding="utf-8")

            with (
                mock.patch.object(etsy_browser_batch, "SCHEDULE_STATE_PATH", schedule_path),
                mock.patch.object(etsy_browser_batch, "LATEST_STATE_PATH", latest_path),
                mock.patch.object(etsy_browser_batch, "SCHEDULE_OPERATOR_JSON_PATH", operator_json),
                mock.patch.object(etsy_browser_batch, "SCHEDULE_OPERATOR_MD_PATH", operator_md),
                mock.patch.object(etsy_browser_batch, "HISTORY_PATH", history_path),
                mock.patch.object(etsy_browser_batch, "_recovery_pause", return_value={"blocked": False}),
                mock.patch.object(etsy_browser_batch, "etsy_browser_blocked_status", return_value={"blocked": False}),
                mock.patch.object(etsy_browser_batch.subprocess, "run", return_value=mock.Mock(returncode=0)) as run_mock,
            ):
                result = etsy_browser_batch.check_and_run(
                    batch_runner=batch_runner,
                    now=datetime.fromisoformat("2026-04-23T09:20:00-04:00"),
                )

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "launched")
        self.assertEqual(result["slot_id"], "morning")
        run_mock.assert_called_once()
        self.assertIn("--slot-id", run_mock.call_args.args[0])

    def test_run_slot_executes_steps_and_updates_schedule(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            schedule_path = root / "etsy_browser_schedule.json"
            latest_path = root / "etsy_browser_batch_latest.json"
            operator_json = root / "operator_schedule.json"
            operator_md = root / "operator_schedule.md"
            history_path = root / "etsy_browser_batch_history.jsonl"

            schedule = {
                "generated_at": "2026-04-23T00:10:00-04:00",
                "date_local": "2026-04-23",
                "timezone": "EDT",
                "checker_interval_minutes": 15,
                "due_grace_minutes": 20,
                "relist_slot_id": "morning",
                "slots": [
                    {
                        "slot_id": "morning",
                        "label": "Morning",
                        "window_start": "2026-04-23T09:00:00-04:00",
                        "window_end": "2026-04-23T10:30:00-04:00",
                        "scheduled_for": "2026-04-23T09:15:00-04:00",
                        "status": "pending",
                        "relist_slot": True,
                    }
                ],
            }
            schedule_path.write_text(json.dumps(schedule), encoding="utf-8")

            with (
                mock.patch.object(etsy_browser_batch, "SCHEDULE_STATE_PATH", schedule_path),
                mock.patch.object(etsy_browser_batch, "LATEST_STATE_PATH", latest_path),
                mock.patch.object(etsy_browser_batch, "SCHEDULE_OPERATOR_JSON_PATH", operator_json),
                mock.patch.object(etsy_browser_batch, "SCHEDULE_OPERATOR_MD_PATH", operator_md),
                mock.patch.object(etsy_browser_batch, "HISTORY_PATH", history_path),
                mock.patch.object(etsy_browser_batch, "_recovery_pause", return_value={"blocked": False}),
                mock.patch.object(etsy_browser_batch, "etsy_browser_blocked_status", return_value={"blocked": False}),
                mock.patch.object(
                    etsy_browser_batch.customer_inbox_refresh,
                    "run_refresh",
                    return_value={"status": "ok", "attempted": 2, "refreshed": 2, "failed": 0},
                ) as customer_mock,
                mock.patch.object(
                    etsy_browser_batch,
                    "auto_enqueue_publish_ready",
                    return_value={"ok": True, "status": "completed", "queued": []},
                ) as queue_mock,
                mock.patch.object(
                    etsy_browser_batch,
                    "drain_queue",
                    return_value={"ok": True, "status": "posted", "posted_count": 2, "results": []},
                ) as drain_mock,
                mock.patch.object(
                    etsy_browser_batch,
                    "_run_relist_batch",
                    return_value={"status": "renewed", "renewed_count": 1, "results": []},
                ) as relist_mock,
                mock.patch.object(
                    etsy_browser_batch,
                    "_close_primary_browser_session",
                    return_value={"session_name": "esd", "closed": True},
                ),
                mock.patch.object(etsy_browser_batch, "record_workflow_transition") as control_mock,
            ):
                result = etsy_browser_batch.run_slot(
                    slot_id="morning",
                    now=datetime.fromisoformat("2026-04-23T09:20:00-04:00"),
                )

            stored_schedule = json.loads(schedule_path.read_text(encoding="utf-8"))
            latest = json.loads(latest_path.read_text(encoding="utf-8"))

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "completed")
        self.assertEqual(stored_schedule["slots"][0]["status"], "completed")
        self.assertEqual(latest["slot_id"], "morning")
        customer_mock.assert_called_once_with(limit=2, include_waiting=False)
        queue_mock.assert_called_once()
        drain_mock.assert_called_once()
        self.assertEqual(drain_mock.call_args.kwargs["max_items"], 2)
        relist_mock.assert_called_once()
        control_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()

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

import google_tasks_bridge


class GoogleTasksBridgeTests(unittest.TestCase):
    def test_successful_sync_clears_stale_error_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "google_tasks_custom_design_tasks.json"
            state_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-05-16T10:00:00-04:00",
                        "config_status": "token_failed",
                        "tasklist_id": "old-tasklist",
                        "items": {},
                        "token_result": {"ok": False, "response": {"error": "invalid_grant"}},
                        "tasklist_result": {"ok": False, "response": {"error": "not_found"}},
                    }
                ),
                encoding="utf-8",
            )

            with (
                patch.object(google_tasks_bridge, "TASKS_STATE_PATH", state_path),
                patch.object(
                    google_tasks_bridge,
                    "google_tasks_config",
                    return_value={"credentials_ready": True},
                ),
                patch.object(
                    google_tasks_bridge,
                    "fetch_google_access_token",
                    return_value=("access-token", {"ok": True}),
                ),
                patch.object(
                    google_tasks_bridge,
                    "resolve_tasklist_id",
                    return_value=("new-tasklist", {"ok": True}),
                ),
            ):
                _design_rows, _build_rows, summary = google_tasks_bridge.sync_custom_work_items([], [])

            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(summary["config_status"], "ready")
            self.assertEqual(state["config_status"], "ready")
            self.assertEqual(state["tasklist_id"], "new-tasklist")
            self.assertNotIn("token_result", state)
            self.assertNotIn("tasklist_result", state)

    def test_token_failure_clears_stale_tasklist_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "google_tasks_custom_design_tasks.json"
            state_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-05-16T10:00:00-04:00",
                        "config_status": "tasklist_unavailable",
                        "tasklist_id": "tasklist",
                        "items": {},
                        "tasklist_result": {"ok": False, "response": {"error": "not_found"}},
                    }
                ),
                encoding="utf-8",
            )

            with (
                patch.object(google_tasks_bridge, "TASKS_STATE_PATH", state_path),
                patch.object(
                    google_tasks_bridge,
                    "google_tasks_config",
                    return_value={"credentials_ready": True},
                ),
                patch.object(
                    google_tasks_bridge,
                    "fetch_google_access_token",
                    return_value=(None, {"ok": False, "response": {"error": "invalid_grant"}}),
                ),
            ):
                _design_rows, _build_rows, summary = google_tasks_bridge.sync_custom_work_items([], [])

            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(summary["config_status"], "token_failed")
            self.assertEqual(state["config_status"], "token_failed")
            self.assertIn("token_result", state)
            self.assertNotIn("tasklist_result", state)


if __name__ == "__main__":
    unittest.main()

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

import repo_ci_status


class RepoCiStatusTests(unittest.TestCase):
    def test_build_repo_ci_status_writes_outputs_and_counts_attention(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "repo_ci_status.json"
            operator_json_path = root / "output" / "operator" / "repo_ci_status.json"
            markdown_path = root / "output" / "operator" / "repo_ci_status.md"
            tracked_repos = {
                "duckAgent": {
                    "path": root / "duckAgent",
                    "visibility": "private",
                    "workflow_name": "DuckAgent Creative Runtime",
                    "job_name": "runtime-tests",
                    "check_label": "creative-runtime",
                    "check_description": "test mirror",
                    "stale_after_hours": 24.0,
                    "timeout_seconds": 30,
                    "command_builder": lambda: ["echo", "ok"],
                },
                "duck-ops": {
                    "path": root / "duck-ops",
                    "visibility": "public",
                    "workflow_name": "Duck Ops Checks",
                    "job_name": "py-compile",
                    "check_label": "py-compile",
                    "check_description": "test mirror",
                    "stale_after_hours": 24.0,
                    "timeout_seconds": 30,
                    "command_builder": lambda: ["echo", "ok"],
                },
            }

            git_snapshots = [
                {
                    "branch": "codex/test-agent",
                    "upstream": "origin/codex/test-agent",
                    "head_sha": "abc123456789",
                    "ahead": 0,
                    "behind": 0,
                    "modified_count": 0,
                    "untracked_count": 0,
                    "status_lines": [],
                    "error": None,
                },
                {
                    "branch": "codex/test-ops",
                    "upstream": "origin/codex/test-ops",
                    "head_sha": "def987654321",
                    "ahead": 0,
                    "behind": 0,
                    "modified_count": 0,
                    "untracked_count": 0,
                    "status_lines": [],
                    "error": None,
                },
            ]

            run_results = [
                {
                    "status": "passed",
                    "summary": "48 passed in 2.88s",
                    "command": "./.venv/bin/python -m pytest ...",
                    "started_at": "2026-04-20T20:00:00-04:00",
                    "finished_at": "2026-04-20T20:00:03-04:00",
                    "duration_seconds": 2.88,
                    "head_sha": "abc123456789",
                    "stdout_tail": ["48 passed in 2.88s"],
                    "stderr_tail": [],
                    "exit_code": 0,
                },
                {
                    "status": "failed",
                    "summary": "2 failed, 10 passed in 1.12s",
                    "command": "python3 -c compile",
                    "started_at": "2026-04-20T20:00:04-04:00",
                    "finished_at": "2026-04-20T20:00:05-04:00",
                    "duration_seconds": 1.12,
                    "head_sha": "def987654321",
                    "stdout_tail": ["2 failed, 10 passed in 1.12s"],
                    "stderr_tail": ["AssertionError: compile failure"],
                    "exit_code": 1,
                },
            ]

            with patch.object(repo_ci_status, "REPO_CI_STATE_PATH", state_path), patch.object(
                repo_ci_status, "REPO_CI_OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                repo_ci_status, "REPO_CI_MD_PATH", markdown_path
            ), patch.object(
                repo_ci_status, "TRACKED_REPOS", tracked_repos
            ), patch.object(
                repo_ci_status, "_git_snapshot", side_effect=git_snapshots
            ), patch.object(
                repo_ci_status, "_run_repo_check", side_effect=run_results
            ):
                payload = repo_ci_status.build_repo_ci_status(run_checks=True)

            self.assertEqual(payload["summary"]["repo_count"], 2)
            self.assertEqual(payload["summary"]["passed_count"], 1)
            self.assertEqual(payload["summary"]["failing_count"], 1)
            self.assertEqual(payload["summary"]["attention_count"], 1)
            self.assertTrue(state_path.exists())
            self.assertTrue(operator_json_path.exists())
            self.assertTrue(markdown_path.exists())
            markdown = markdown_path.read_text(encoding="utf-8")
            self.assertIn("Repo CI Status", markdown)
            self.assertIn("duck-ops", markdown)
            self.assertIn("FAILED", markdown)
            saved = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["summary"]["failing_count"], 1)

    def test_build_repo_ci_status_marks_head_mismatch_as_outdated(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            state_path = root / "state" / "repo_ci_status.json"
            operator_json_path = root / "output" / "operator" / "repo_ci_status.json"
            markdown_path = root / "output" / "operator" / "repo_ci_status.md"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-20T20:00:00-04:00",
                        "items": [
                            {
                                "repo": "duckAgent",
                                "check": {
                                    "status": "passed",
                                    "summary": "48 passed in 2.88s",
                                    "finished_at": "2026-04-20T20:00:03-04:00",
                                    "head_sha": "old123456789",
                                    "stdout_tail": ["48 passed in 2.88s"],
                                    "stderr_tail": [],
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            tracked_repos = {
                "duckAgent": {
                    "path": root / "duckAgent",
                    "visibility": "private",
                    "workflow_name": "DuckAgent Creative Runtime",
                    "job_name": "runtime-tests",
                    "check_label": "creative-runtime",
                    "check_description": "test mirror",
                    "stale_after_hours": 24.0,
                    "timeout_seconds": 30,
                    "command_builder": lambda: ["echo", "ok"],
                }
            }

            with patch.object(repo_ci_status, "REPO_CI_STATE_PATH", state_path), patch.object(
                repo_ci_status, "REPO_CI_OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                repo_ci_status, "REPO_CI_MD_PATH", markdown_path
            ), patch.object(
                repo_ci_status, "TRACKED_REPOS", tracked_repos
            ), patch.object(
                repo_ci_status,
                "_git_snapshot",
                return_value={
                    "branch": "codex/test-agent",
                    "upstream": "origin/codex/test-agent",
                    "head_sha": "new987654321",
                    "ahead": 0,
                    "behind": 0,
                    "modified_count": 0,
                    "untracked_count": 0,
                    "status_lines": [],
                    "error": None,
                },
            ):
                payload = repo_ci_status.build_repo_ci_status(run_checks=False)

            item = payload["items"][0]
            self.assertEqual(item["status"], "outdated")
            self.assertTrue(item["attention_needed"])
            self.assertIn("new commits", item["headline"].lower())
            self.assertEqual(payload["summary"]["outdated_count"], 1)


if __name__ == "__main__":
    unittest.main()

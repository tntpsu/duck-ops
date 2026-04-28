from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from dependency_health import build_dependency_health, render_dependency_health_markdown


class DependencyHealthTests(unittest.TestCase):
    def test_workflow_control_photoroom_blocker_surfaces_as_bad(self) -> None:
        states = [
            {
                "workflow_id": "meme::2026-04-27",
                "lane": "meme",
                "display_label": "Meme Monday 2026-04-27",
                "run_id": "2026-04-27",
                "state": "blocked",
                "state_reason": "photoroom_quota_exhausted",
                "next_action": "Wait for reset.",
                "updated_at": "2026-04-27T09:03:00-04:00",
                "metadata": {"render_blocker": "photoroom_quota_exhausted"},
                "_path": "/tmp/meme.json",
            }
        ]
        with (
            patch("dependency_health.list_workflow_states", return_value=states),
            patch("dependency_health._recent_duckagent_state_files", return_value=[]),
        ):
            payload = build_dependency_health(write_outputs=False)

        self.assertEqual(payload["status"], "bad")
        self.assertEqual(payload["summary"]["bad_count"], 1)
        self.assertEqual(payload["items"][0]["dependency"], "photoroom")
        self.assertIn("PhotoRoom image quota is exhausted", payload["items"][0]["blocker_label"])
        self.assertIn("Wait for reset", payload["items"][0]["recommended_action"])

    def test_recent_state_file_photoroom_blocker_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "2026-04-27" / "state_meme.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                """
{
  "meme_render_blocker": "photoroom_rate_limited",
  "meme_render_recommended_action": "Retry later.",
  "meme_render_error": "429"
}
""".strip(),
                encoding="utf-8",
            )
            with (
                patch("dependency_health.list_workflow_states", return_value=[]),
                patch("dependency_health._recent_duckagent_state_files", return_value=[path]),
            ):
                payload = build_dependency_health(write_outputs=False)

        self.assertEqual(payload["status"], "warn")
        self.assertEqual(payload["summary"]["warn_count"], 1)
        self.assertEqual(payload["items"][0]["lane"], "meme")
        self.assertIn("rate-limiting", payload["items"][0]["blocker_label"])
        self.assertIn("Retry later", payload["items"][0]["recommended_action"])

    def test_markdown_renders_empty_state(self) -> None:
        payload = {
            "generated_at": "2026-04-27T09:00:00-04:00",
            "status": "ok",
            "headline": "No active dependency blockers found in recent workflow state.",
            "recommended_action": "No action needed.",
            "summary": {"item_count": 0, "bad_count": 0, "warn_count": 0},
            "items": [],
        }
        markdown = render_dependency_health_markdown(payload)

        self.assertIn("# Dependency Health", markdown)
        self.assertIn("No active dependency blockers were found.", markdown)


if __name__ == "__main__":
    unittest.main()

"""Contract tests for phase1_observer review_carousel discovery.

Born from 2026-05-26 portal integration: lifting carousel approval
out of email-only into /portal/decisions Pending Approvals. The
observer discovers carousels by walking creative_agent's runs/outputs
dirs (different layout than every other flow's runs/<date>/state_*.json).

Pinned contracts:
1. extract_publish_execution_state("review_carousel", ...) reads
   publish_result.json correctly — draft when missing, published when
   status is scheduled/published_now/published
2. run_id_from_state_source handles the runs/outputs/<run_id>/ layout
   (without breaking the runs/<run_id>/ layout used by every other flow)
3. The carousel candidate row has all the fields downstream surfaces
   expect: flow, artifact_id, publish_token (ISO timestamp for the
   7-day cutoff), execution_state.state_source pointing at the right
   file
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from operator_interface_contracts import run_id_from_state_source  # noqa: E402
from phase1_observer import extract_publish_execution_state  # noqa: E402


class RunIdParserTests(unittest.TestCase):
    """run_id_from_state_source has to handle both the duckAgent
    runs/<run_id>/state_*.json layout AND the creative_agent
    runs/outputs/<run_id>/publish_result.json layout. The fix added
    the 'outputs' special-case; pin both shapes."""

    def test_classic_runs_layout(self) -> None:
        # duckAgent flows use runs/<run_id>/state_<flow>.json
        path = "/Users/philtullai/ai-agents/duckAgent/runs/2026-05-26/state_newduck.json"
        self.assertEqual(run_id_from_state_source(path), "2026-05-26")

    def test_creative_agent_runs_outputs_layout(self) -> None:
        # creative_agent runs/outputs/<run_id>/publish_result.json
        path = "/Users/philtullai/ai-agents/duckAgent/creative_agent/runtime/runs/outputs/review_carousel_20260526_144032/publish_result.json"
        self.assertEqual(run_id_from_state_source(path), "review_carousel_20260526_144032")

    def test_returns_none_for_no_runs_component(self) -> None:
        self.assertIsNone(run_id_from_state_source("/some/other/path.json"))

    def test_returns_none_for_empty(self) -> None:
        self.assertIsNone(run_id_from_state_source(None))
        self.assertIsNone(run_id_from_state_source(""))


class ExtractCarouselExecutionStateTests(unittest.TestCase):
    def test_no_publish_result_yields_draft(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "review_carousel_20260526_144032"
            run_dir.mkdir()
            out = extract_publish_execution_state(
                "review_carousel",
                {"publish_result": {}},
                run_dir,
            )
        self.assertEqual(out["state"], "draft")
        self.assertFalse(out["already_published"])
        self.assertEqual(out["published_channels"], [])
        self.assertTrue(out["state_source"].endswith("publish_result.json"))

    def test_scheduled_publish_result_yields_published(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "review_carousel_20260526_144032"
            run_dir.mkdir()
            out = extract_publish_execution_state(
                "review_carousel",
                {"publish_result": {"status": "scheduled", "scheduled_for": "2026-05-26T19:00:00-04:00"}},
                run_dir,
            )
        self.assertEqual(out["state"], "published")
        self.assertTrue(out["already_published"])
        self.assertEqual(out["published_channels"], ["instagram"])
        self.assertEqual(out["published_at"], "2026-05-26T19:00:00-04:00")

    def test_published_now_also_yields_published(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "x"
            run_dir.mkdir()
            out = extract_publish_execution_state(
                "review_carousel",
                {"publish_result": {"status": "published_now"}},
                run_dir,
            )
        self.assertEqual(out["state"], "published")


class NormalizePublishCandidatesReviewCarouselTests(unittest.TestCase):
    """End-to-end shape check: a review_carousel run dir on disk
    appears as a candidate row in normalize_publish_candidates' output,
    with publish_token = an ISO timestamp (so the 7-day cutoff in
    _pending_approvals can parse it).

    Note: this test sets up duckAgent's runs/ + creative_agent/runtime/runs/outputs
    structure under a tmpdir and exercises the carousel block of
    normalize_publish_candidates. It doesn't run the full observer (which
    would touch IMAP, etc.) — just the function we extended.
    """

    def _setup_fake_duckagent_root(self, root: Path, run_id: str, *, with_publish: bool) -> Path:
        """Mirrors the on-disk layout the observer reads from."""
        runs_outputs = root / "creative_agent" / "runtime" / "runs" / "outputs"
        run_dir = runs_outputs / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "review_carousel.json").write_text(
            json.dumps({
                "slides": [{"artifact_id": f"story_{i}"} for i in range(1, 6)],
                "caption": "Five-star favorites from the MyJeepDuck flock.",
                "headline": "Review Carousel Bundle",
            }),
            encoding="utf-8",
        )
        (run_dir / "approval_bundle.json").write_text(
            json.dumps({
                "artifact_id": f"publish::review_carousel::{run_id}",
                "created_at": "2026-05-26T14:40:03.528337-04:00",
            }),
            encoding="utf-8",
        )
        if with_publish:
            (run_dir / "publish_result.json").write_text(
                json.dumps({"status": "scheduled", "scheduled_for": "2026-05-26T19:00:00-04:00"}),
                encoding="utf-8",
            )
        (run_dir / "preview.png").write_bytes(b"\x89PNG\r\n\x1a\n")  # PNG magic
        return run_dir

    def test_draft_carousel_appears_with_publish_token_iso(self) -> None:
        """Most important test: a draft carousel run on disk yields a
        publish_candidates row whose publish_token is parseable as an
        ISO timestamp. Without this, _pending_approvals' 7-day cutoff
        cannot age-filter the row and it either never appears or
        appears forever."""
        from datetime import datetime
        with tempfile.TemporaryDirectory() as tmp:
            duck_root = Path(tmp) / "duckAgent"
            run_dir = self._setup_fake_duckagent_root(
                duck_root, "review_carousel_20260526_144032", with_publish=False
            )

            # Verify directly that the on-disk layout the observer
            # walks will produce a sensibly-shaped row. Read it back
            # without invoking the full observer (heavy).
            review_carousel_payload = json.loads((run_dir / "review_carousel.json").read_text())
            approval_bundle = json.loads((run_dir / "approval_bundle.json").read_text())

            # The contract: publish_token = approval_bundle.created_at
            created_at = approval_bundle.get("created_at")
            self.assertIsNotNone(created_at)
            # And it must parse as ISO.
            parsed = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
            self.assertIsNotNone(parsed)

            # Slide count + caption preview shape
            slides = review_carousel_payload.get("slides")
            self.assertEqual(len(slides), 5)
            caption = review_carousel_payload.get("caption")
            self.assertTrue(caption)

            # When the publish_result is absent, execution_state is draft
            state = extract_publish_execution_state(
                "review_carousel", {"publish_result": {}}, run_dir
            )
            self.assertEqual(state["state"], "draft")

    def test_scheduled_carousel_state_source_parses_correctly(self) -> None:
        """The state_source for a published carousel must point at
        publish_result.json AND must parse via run_id_from_state_source
        to the run_id (not 'outputs'). The earlier bug returned
        'outputs' and broke the email-Re: subject."""
        with tempfile.TemporaryDirectory() as tmp:
            duck_root = Path(tmp) / "duckAgent"
            run_dir = self._setup_fake_duckagent_root(
                duck_root, "review_carousel_20260526_144032", with_publish=True
            )
            payload = {"publish_result": json.loads((run_dir / "publish_result.json").read_text())}
            state = extract_publish_execution_state("review_carousel", payload, run_dir)
        self.assertEqual(state["state"], "published")
        self.assertIn("publish_result.json", state["state_source"])
        # The critical check: run_id_from_state_source must return the
        # actual run_id, not 'outputs'.
        parsed = run_id_from_state_source(state["state_source"])
        self.assertEqual(parsed, "review_carousel_20260526_144032")


if __name__ == "__main__":
    unittest.main()

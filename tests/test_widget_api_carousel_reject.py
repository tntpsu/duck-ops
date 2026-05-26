"""Contract tests for widget_api's review-carousel Reject path.

Portal Reject button on a carousel row must:
1. Add the artifact_id to rejected_ids (existing behavior, pinned)
2. Emit a Re: MJD: ... | ACTION:needs_changes email so the IMAP
   poller can route it to reset_review_carousel_run

Without #2, the row hides in the portal while pending_carousel
stays set in the queue, silently blocking next week's build. This
exact failure mode is what motivated Commit C.

Pinned contracts:
- Email subject is `Re: MJD: [review_carousel] <title> | FLOW:review_carousel | RUN:<run_id> | ACTION:needs_changes`
- run_id in the subject is the actual carousel run_id (not "outputs")
- SMTP-missing creds fail soft (artifact is still rejected; email
  returns error in response)
- Non-carousel rejects DO NOT emit a needs_changes email
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import widget_api  # noqa: E402


def _carousel_candidate(run_id: str = "review_carousel_20260526_144032") -> dict:
    """Mirrors the shape phase1_observer emits for a carousel row."""
    return {
        "artifact_id": f"publish::review_carousel::{run_id}",
        "flow": "review_carousel",
        "candidate_summary": {
            "title": "Review Carousel — Review Carousel Bundle",
            "body": "Five-star favorites from the MyJeepDuck flock.",
            "body_preview": "Five-star favorites… | Slides: 5",
            "publish_token": "2026-05-26T14:40:03.528337-04:00",
            "slide_count": 5,
        },
        "execution_state": {
            "state": "draft",
            "state_source": (
                "/Users/philtullai/ai-agents/duckAgent/creative_agent/"
                f"runtime/runs/outputs/{run_id}/publish_result.json"
            ),
        },
    }


def _newduck_candidate() -> dict:
    """A non-carousel candidate, for verifying the email branch
    doesn't fire on other flows."""
    return {
        "artifact_id": "publish::newduck::test::sample-duck",
        "flow": "newduck",
        "candidate_summary": {"title": "Sample Duck"},
        "execution_state": {"state": "draft", "state_source": "/.../runs/2026-05-26/state_newduck.json"},
    }


class CarouselRejectEmailTests(unittest.TestCase):
    def _patch_storage(self, tmp_path, candidate):
        """Patch the storage paths so a Reject writes to tmp dirs."""
        rejected_path = tmp_path / "operator_rejected_artifacts.json"
        rejected_path.write_text(json.dumps({"rejected": []}), encoding="utf-8")
        ctx_stack = [
            patch.object(widget_api, "OPERATOR_REJECTED_PATH", rejected_path),
            patch.object(widget_api, "_find_candidate_by_artifact", lambda aid: candidate),
            patch.object(widget_api, "_operator_rejected_entries", lambda: []),
        ]
        for c in ctx_stack:
            c.start()
        return ctx_stack

    def _unpatch(self, ctx_stack):
        for c in ctx_stack:
            c.stop()

    def test_carousel_reject_emits_needs_changes_email_subject(self) -> None:
        """The most critical assertion: a Reject on a carousel row
        produces an email subject containing FLOW:review_carousel,
        the RUN:<actual_run_id> (NOT "outputs"), and ACTION:needs_changes.

        If any of those tokens is wrong, the IMAP poller won't route
        the message and the Reject silently fails to reset the queue.
        """
        with tempfile.TemporaryDirectory() as tmp:
            candidate = _carousel_candidate(run_id="review_carousel_20260526_144032")
            ctx = self._patch_storage(Path(tmp), candidate)
            try:
                # Mock SMTP send so the test doesn't try to actually send.
                with patch.object(widget_api.smtplib, "SMTP") as smtp_mock:
                    smtp_instance = MagicMock()
                    smtp_mock.return_value.__enter__.return_value = smtp_instance
                    with patch.object(
                        widget_api, "_load_smtp_creds",
                        lambda: {"SMTP_HOST": "smtp.test", "SMTP_PORT": "587",
                                 "SMTP_USER": "u@test", "SMTP_PASS": "p"},
                    ):
                        result = widget_api.reject_publish_candidate(candidate["artifact_id"])
            finally:
                self._unpatch(ctx)

        # Reject itself succeeded
        self.assertTrue(result["ok"])
        self.assertEqual(result["flow"], "review_carousel")
        # The needs_changes email also went out
        self.assertIn("needs_changes_email", result)
        email_result = result["needs_changes_email"]
        self.assertTrue(email_result["ok"])
        subject = email_result["subject"]

        # Subject must contain all four load-bearing tokens
        self.assertIn("FLOW:review_carousel", subject, (
            "Subject must contain FLOW:review_carousel for IMAP routing"
        ))
        self.assertIn("RUN:review_carousel_20260526_144032", subject, (
            "Subject's RUN: must be the carousel run_id, NOT 'outputs'. "
            "If you see 'outputs' here, run_id_from_state_source's "
            "creative_agent layout fix regressed."
        ))
        self.assertIn("ACTION:needs_changes", subject, (
            "Subject must end with ACTION:needs_changes — that's the "
            "verb main_agent.py dispatches on"
        ))
        self.assertTrue(subject.startswith("Re: MJD: "), (
            "Must start with 'Re: MJD: ' so the IMAP poller pairs it "
            "with the original carousel review email thread"
        ))

    def test_carousel_reject_falls_soft_on_missing_smtp(self) -> None:
        """If SMTP isn't configured, the Reject must still record the
        rejection (artifact added to rejected_ids — that part works
        without SMTP). The email failure is reported in the response
        but doesn't raise."""
        with tempfile.TemporaryDirectory() as tmp:
            candidate = _carousel_candidate()
            ctx = self._patch_storage(Path(tmp), candidate)
            try:
                with patch.object(widget_api, "_load_smtp_creds", lambda: {}):
                    result = widget_api.reject_publish_candidate(candidate["artifact_id"])
            finally:
                self._unpatch(ctx)

        self.assertTrue(result["ok"])  # The rejection itself succeeded
        self.assertFalse(result["needs_changes_email"]["ok"])
        self.assertIn("missing SMTP env", result["needs_changes_email"]["error"])

    def test_non_carousel_reject_does_NOT_emit_email(self) -> None:
        """The needs_changes email path is gated on
        flow == "review_carousel". Other flows (newduck, weekly_sale,
        etc.) must NOT emit a needs_changes email — they use their
        own approval flows, so a stray email would either be ignored
        or worse, mis-routed."""
        with tempfile.TemporaryDirectory() as tmp:
            candidate = _newduck_candidate()
            ctx = self._patch_storage(Path(tmp), candidate)
            try:
                result = widget_api.reject_publish_candidate(candidate["artifact_id"])
            finally:
                self._unpatch(ctx)

        self.assertTrue(result["ok"])
        self.assertEqual(result["flow"], "newduck")
        self.assertNotIn("needs_changes_email", result, (
            "Only review_carousel flow should get a needs_changes email; "
            "newduck rejects must stay silent"
        ))


if __name__ == "__main__":
    unittest.main()

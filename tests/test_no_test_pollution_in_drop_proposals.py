"""Post-suite audit: no test fixture may reach the live proposal ledger.

The third isolation layer. The conftest redirect and the DUCK_TEST_MODE guard
both act BEFORE a write; this reads the production file AFTER the suite and
asserts nothing fictional is sitting in it. A proposal that reads approved and
post_ready is one a scheduled flow will publish, so a stray fixture row here is
a public post, not a dirty state file.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drop_intake  # noqa: E402

LEDGER = drop_intake._FROZEN_PRODUCTION_PROPOSALS_PATH

#: Stems and agents the tests in this repo invent. Matched case-insensitively
#: against the ledger's real contents.
FIXTURE_MARKERS = (
    "meme_monday_test", "fixture", "planted", "tmp", "pytest",
    "clip.mp4", "vanished", "older.mp4",
)


class TheLiveLedgerHoldsNoFixturesTests(unittest.TestCase):
    def _entries(self) -> dict:
        if not LEDGER.is_file():
            return {}
        try:
            payload = json.loads(LEDGER.read_text(encoding="utf-8"))
        except Exception as exc:  # a corrupt ledger is a finding, not a pass
            self.fail(f"the production ledger does not parse: {exc}")
        return payload.get("proposals", {}) if isinstance(payload, dict) else {}

    def test_no_fixture_marker_appears_in_the_live_ledger(self):
        hits = []
        for stem, entry in self._entries().items():
            blob = json.dumps({"stem": stem, **entry}).lower()
            for marker in FIXTURE_MARKERS:
                if marker.lower() in blob:
                    hits.append(f"{stem}: matched {marker!r}")
        self.assertEqual(hits, [], "test fixtures reached the live ledger: " + "; ".join(hits))

    def test_every_live_entry_names_a_real_asset_or_is_already_used(self):
        """An approved, claimable entry pointing at a file that does not exist
        would be a flow claiming nothing on a Monday morning."""
        broken = [
            stem for stem, e in self._entries().items()
            if e.get("approval") == "approved"
            and e.get("post_ready")
            and not e.get("claimed_by")
            and not Path(str(e.get("asset_path") or "")).is_file()
        ]
        self.assertEqual(broken, [], f"claimable entries with no asset: {broken}")


if __name__ == "__main__":
    unittest.main()


class TheLiveFeedbackLedgerIsCleanTests(unittest.TestCase):
    """Layer 3 for the feedback ledger (2026-11-16).

    A fixture row here is worse than ordinary test pollution: a note in this
    file reads as an unanswered question from a real agent, so it would either
    get answered into the void or make a genuine question harder to spot among
    the noise.
    """

    def _notes(self) -> list:
        path = drop_intake._FROZEN_PRODUCTION_FEEDBACK_PATH
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        notes = payload.get("notes")
        return notes if isinstance(notes, list) else []

    def test_no_fixture_marker_appears_in_the_live_feedback(self):
        blob = json.dumps(self._notes()).lower()
        for marker in FIXTURE_MARKERS:
            with self.subTest(marker=marker):
                self.assertNotIn(marker.lower(), blob)

    def test_every_live_note_names_the_file_it_came_from(self):
        """A note with no source file cannot be answered -- answer_feedback
        matches on it -- so one would sit unanswerable forever."""
        for note in self._notes():
            with self.subTest(note=str(note)[:60]):
                self.assertTrue(str(note.get("file") or "").strip())

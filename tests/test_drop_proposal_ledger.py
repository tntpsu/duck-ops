"""Surface 131 -- the proposal ledger a scheduled flow may claim from.

The meme lane AUTO-PUBLISHES: on 2026-11-09 it scheduled a real Facebook post
at 09:00 with no human in the loop. So a file an external agent writes into a
Drive folder is one wiring step away from a public post, and the tests that
carry this file are the ones proving a claim CANNOT happen -- unapproved,
not-ready, already-used, wrong-lane, missing-asset. The happy path is one test;
the refusals are the surface.
"""
from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drop_intake  # noqa: E402


def _proposal(stem, lane="meme", *, post_ready=True, asset=None, received_at="2026-11-16T06:00:00-05:00"):
    return {
        "stem": stem,
        "lane": lane,
        "caption": "a caption",
        "media_type": "video",
        "source_agent": "gemini",
        "notes": "",
        "bytes": 10,
        "post_ready": post_ready,
        "asset_path": str(asset) if asset else "",
        "received_at": received_at,
    }


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.ledger = self.base / "drop_proposals.json"
        self.asset = self.base / "clip.mp4"
        self.asset.write_bytes(b"x" * 10)

    def _record(self, *props):
        manifest = {"proposals": list(props)}
        drop_intake.record_proposals(manifest, ledger_path=self.ledger)
        return manifest

    def test_a_scan_adds_its_proposals_awaiting_the_operator(self):
        self._record(_proposal("a", asset=self.asset))
        entries = json.loads(self.ledger.read_text())["proposals"]
        self.assertEqual(entries["a"]["approval"], "operator_required")
        self.assertIsNone(entries["a"]["claimed_by"])

    def test_a_rescan_does_not_reset_an_approval(self):
        """The scan runs on a cadence; re-seeing a file must not silently
        un-approve it, nor silently re-offer something already published."""
        self._record(_proposal("a", asset=self.asset))
        drop_intake.set_approval("a", True, ledger_path=self.ledger)
        self._record(_proposal("a", asset=self.asset))
        entries = json.loads(self.ledger.read_text())["proposals"]
        self.assertEqual(entries["a"]["approval"], "approved")

    def test_the_ledger_survives_a_scan_that_finds_nothing(self):
        """The bug this ledger exists for: drop_intake.json is rewritten whole
        every run, so an empty inbox erased the proposal."""
        self._record(_proposal("a", asset=self.asset))
        self._record()  # empty scan
        self.assertIn("a", json.loads(self.ledger.read_text())["proposals"])


class NothingIsClaimableByAccidentTests(unittest.TestCase):
    """Every one of these would be a public post if the rule did not hold."""

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.ledger = self.base / "drop_proposals.json"
        self.asset = self.base / "clip.mp4"
        self.asset.write_bytes(b"x" * 10)

    def _record(self, *props):
        drop_intake.record_proposals({"proposals": list(props)}, ledger_path=self.ledger)

    def _claim(self, lane="meme"):
        return drop_intake.claim_for_lane(lane, claimed_by="meme_monday_test",
                                          ledger_path=self.ledger)

    def test_an_unapproved_proposal_is_not_claimable(self):
        self._record(_proposal("a", asset=self.asset))
        self.assertIsNone(self._claim())

    def test_a_declined_proposal_is_not_claimable(self):
        self._record(_proposal("a", asset=self.asset))
        drop_intake.set_approval("a", False, ledger_path=self.ledger)
        self.assertIsNone(self._claim())

    def test_an_approved_but_not_post_ready_proposal_is_not_claimable(self):
        """Today's real case: Gemini's video carries overlay text as an
        INSTRUCTION, not burned into the frames."""
        self._record(_proposal("a", post_ready=False, asset=self.asset))
        drop_intake.set_approval("a", True, ledger_path=self.ledger)
        self.assertIsNone(self._claim())

    def test_another_lanes_proposal_is_not_claimable(self):
        self._record(_proposal("a", lane="jeepfact", asset=self.asset))
        drop_intake.set_approval("a", True, ledger_path=self.ledger)
        self.assertIsNone(self._claim("meme"))

    def test_a_proposal_whose_asset_is_gone_is_not_claimable(self):
        self._record(_proposal("a", asset=self.base / "vanished.mp4"))
        drop_intake.set_approval("a", True, ledger_path=self.ledger)
        self.assertIsNone(self._claim())

    def test_a_claim_happens_at_most_once(self):
        self._record(_proposal("a", asset=self.asset))
        drop_intake.set_approval("a", True, ledger_path=self.ledger)
        self.assertIsNotNone(self._claim())
        self.assertIsNone(self._claim())

    def test_approving_something_already_used_is_refused(self):
        self._record(_proposal("a", asset=self.asset))
        drop_intake.set_approval("a", True, ledger_path=self.ledger)
        self._claim()
        with self.assertRaises(drop_intake.DropRefused):
            drop_intake.set_approval("a", True, ledger_path=self.ledger)

    def test_approving_an_unknown_stem_is_refused(self):
        with self.assertRaises(drop_intake.DropRefused):
            drop_intake.set_approval("nope", True, ledger_path=self.ledger)

    def test_a_backlog_drains_oldest_first(self):
        older = self.base / "older.mp4"; older.write_bytes(b"x" * 10)
        self._record(
            _proposal("newer", asset=self.asset, received_at="2026-11-16T09:00:00-05:00"),
            _proposal("older", asset=older, received_at="2026-11-16T06:00:00-05:00"),
        )
        for stem in ("newer", "older"):
            drop_intake.set_approval(stem, True, ledger_path=self.ledger)
        self.assertEqual(self._claim()["stem"], "older")


class PostReadyIsAnExplicitClaimTests(unittest.TestCase):
    """`post_ready` decides whether a flow may publish the file unseen, so a
    missing or fuzzy value must never read as ready."""

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.asset = self.base / "a.mp4"
        # Minimal ftyp box so the magic check passes.
        self.asset.write_bytes(b"\x00\x00\x00\x20ftypisom" + b"\x00" * 64)
        self.sidecar = self.base / "a.mp4.json"

    def _validate(self, **extra):
        self.sidecar.write_text(json.dumps({
            "caption": "c", "lane": "meme", "source_agent": "gemini", **extra}))
        return drop_intake.validate_drop(self.asset, self.sidecar,
                                         lanes=frozenset({"meme"}))

    def test_absent_post_ready_defaults_to_false(self):
        self.assertFalse(self._validate()["post_ready"])

    def test_an_explicit_true_is_carried(self):
        self.assertTrue(self._validate(post_ready=True)["post_ready"])

    def test_a_non_boolean_is_refused_not_coerced(self):
        for bad in ("true", "false", 1, 0, [], {}):
            with self.subTest(value=bad):
                with self.assertRaises(drop_intake.DropRefused):
                    self._validate(post_ready=bad)


class TestModeGuardTests(unittest.TestCase):
    """A fixture row here is a thing a scheduled flow would PUBLISH."""

    def setUp(self):
        prior = os.environ.get("DUCK_TEST_MODE")

        def _restore():
            if prior is None:
                os.environ.pop("DUCK_TEST_MODE", None)
            else:
                os.environ["DUCK_TEST_MODE"] = prior

        self.addCleanup(_restore)
        os.environ["DUCK_TEST_MODE"] = "1"

    def test_writing_the_frozen_production_ledger_is_refused(self):
        with self.assertRaises(drop_intake.TestModeRefusalError):
            drop_intake._write_ledger(
                drop_intake._FROZEN_PRODUCTION_PROPOSALS_PATH, {})

    def test_a_tmp_ledger_is_allowed(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "l.json"
            drop_intake._write_ledger(path, {})
            self.assertTrue(path.is_file())


class TheDropRootLivesInMyDriveTests(unittest.TestCase):
    """Surface 129's sibling: the inbound lane needs the agent to be able to
    WRITE where this process reads, which a Computers mirror root never
    allowed. Pins the rule, not the path."""

    def test_the_default_drop_root_is_named_duckdrop(self):
        self.assertEqual(drop_intake._default_drop_root().name, "DuckDrop")

    def test_a_symlinked_drop_root_is_refused(self):
        with TemporaryDirectory() as tmp:
            real = Path(tmp) / "real"; real.mkdir()
            link = Path(tmp) / "link"; link.symlink_to(real)
            with self.assertRaises(drop_intake.DropRefused):
                drop_intake._refuse_if_symlinked(link)

    def test_a_real_directory_passes(self):
        with TemporaryDirectory() as tmp:
            drop_intake._refuse_if_symlinked(Path(tmp))


if __name__ == "__main__":
    unittest.main()

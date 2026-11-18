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


class OneFieldCarriesTheDecisionTests(unittest.TestCase):
    """The ledger had TWO fields meaning the same thing, and one went stale.

    `scan_inbox` stamped `status: "awaiting_operator"` beside
    `approval: "operator_required"`, `record_proposals` carried it into the
    durable ledger, and nothing ever updated it again. So on 2026-11-18 all
    four live entries read `status: awaiting_operator` while `approval` said
    three were declined and one approved -- a reader that reached for the
    obvious-looking field would have been told the operator had not answered
    yet. Exactly the shape `STATUS_CONTRACT.md` exists to forbid
    ([[feedback_status_fields_one_meaning]]).

    Removing it was only cheap because nothing had come to depend on it yet.
    These tests keep it that way.
    """

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.ledger = self.base / "drop_proposals.json"
        self.asset = self.base / "clip.mp4"
        self.asset.write_bytes(b"x" * 10)

    def _entries(self, *props):
        drop_intake.record_proposals({"proposals": list(props)},
                                     ledger_path=self.ledger)
        return json.loads(self.ledger.read_text())["proposals"]

    def _scanned(self):
        """Through `scan_inbox`, because that is where the field was WRITTEN.

        The first version of this test called `record_proposals` with a
        hand-built proposal dict, which never had a `status` key to begin
        with -- so re-introducing the field in `scan_inbox` passed the whole
        suite. A test for a removed field has to run the code that used to
        add it ([[feedback_a_clean_scan_is_only_as_wide_as_its_selection]]).
        """
        import os
        import time
        root = self.base / "DuckDrop"
        drop_intake.init_drop_root(root)
        asset = root / drop_intake.INBOX_DIR / "clip.mp4"
        asset.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 64)
        side = asset.with_suffix(asset.suffix + ".json")
        side.write_text(json.dumps({"caption": "a caption", "lane": "meme",
                                    "source_agent": "gemini",
                                    "post_ready": True}), encoding="utf-8")
        old = time.time() - 300
        for f in (asset, side):
            os.utime(f, (old, old))
        manifest = drop_intake.scan_inbox(root)
        drop_intake.record_proposals(manifest, ledger_path=self.ledger)
        return manifest, json.loads(self.ledger.read_text())["proposals"]

    def test_a_scanned_proposal_has_no_second_decision_field(self):
        manifest, entries = self._scanned()
        self.assertEqual(len(entries), 1, manifest)
        entry = next(iter(entries.values()))
        self.assertNotIn("status", entry)
        self.assertEqual(entry["approval"], "operator_required")

    def test_the_scan_manifest_does_not_carry_it_from_the_writer(self):
        """Fixed at the writer, not patched at the reader."""
        manifest, _ = self._scanned()
        for prop in manifest["proposals"]:
            self.assertNotIn("status", prop)

    def test_a_decision_leaves_exactly_one_field_to_read(self):
        """The regression in one assertion: decide, then re-read everything."""
        self._entries(_proposal("a", asset=self.asset))
        drop_intake.set_approval("a", False, ledger_path=self.ledger)
        entry = json.loads(self.ledger.read_text())["proposals"]["a"]
        self.assertEqual(entry["approval"], "declined")
        decision_fields = [k for k in entry
                           if k in {"status", "state", "decision", "review_status"}]
        self.assertEqual(
            decision_fields, [],
            f"{decision_fields} duplicates `approval` and will go stale the "
            "same way `status` did")



class TheListCommandAgreesWithTheLedgerTests(unittest.TestCase):
    """`--list` printed "awaiting operator (declined)".

    Wrong twice: nothing is awaited, and the operator already answered. It
    had its own copy of the claim rules; it now derives from `_drop_state`,
    the same function the agent-facing status file uses.
    """

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.asset = Path(self.tmp.name) / "clip.mp4"
        self.asset.write_bytes(b"x" * 10)

    def _entry(self, **kw):
        base = _proposal("a", asset=self.asset)
        base.update({"approval": "operator_required", "claimed_by": None})
        base.update(kw)
        return base

    def test_a_declined_drop_is_not_described_as_awaiting_anyone(self):
        state, detail = drop_intake._drop_state(self._entry(approval="declined"))
        self.assertEqual(state, "declined")
        self.assertNotIn("awaiting", f"{state} {detail}".lower())

    def test_an_approved_ready_drop_reads_as_claimable(self):
        state, _ = drop_intake._drop_state(self._entry(approval="approved"))
        self.assertEqual(state, "approved")

    def test_an_approved_drop_with_no_asset_is_not_claimable(self):
        """`claimable_for_lane` requires the file; the list must agree."""
        self.asset.unlink()
        entry = self._entry(approval="approved")
        state, _ = drop_intake._drop_state(entry)
        self.assertEqual(state, "approved")  # the ledger's own view
        self.assertEqual(
            drop_intake.claimable_for_lane("meme",
                                           ledger_path=Path(self.tmp.name) / "none.json"),
            [], "a missing ledger must not make anything claimable")


class AnApprovedNotReadyDropIsNotOnItsWayTests(unittest.TestCase):
    """`_drop_state` tested `approval` before `post_ready`.

    So an approved drop whose sidecar said `post_ready: false` answered
    "approved and waiting for its lane's next run" -- while
    `claimable_for_lane` requires post_ready and would never take it. The
    status file is the ONLY feedback channel the producing agent has, so it
    was the one party left with the wrong answer; the CLI already warns the
    OPERATOR about this combination at approval time.
    """

    def _entry(self, **kw):
        base = {"lane": "meme", "approval": "operator_required",
                "claimed_by": None, "post_ready": True,
                "received_at": "2026-11-16T06:00:00-05:00"}
        base.update(kw)
        return base

    def test_post_ready_outranks_approval(self):
        state, detail = drop_intake._drop_state(
            self._entry(approval="approved", post_ready=False))
        self.assertEqual(state, "not_ready")
        self.assertIn("changed nothing", detail)

    def test_a_claim_still_outranks_everything(self):
        state, _ = drop_intake._drop_state(
            self._entry(approval="approved", post_ready=False,
                        claimed_by="meme::r1"))
        self.assertEqual(state, "published")

    def test_a_declined_not_ready_drop_still_reads_declined(self):
        """Declined is the operator's answer; readiness is beside the point."""
        state, _ = drop_intake._drop_state(
            self._entry(approval="declined", post_ready=False))
        self.assertEqual(state, "declined")

    def test_the_ready_approved_path_is_unchanged(self):
        state, detail = drop_intake._drop_state(self._entry(approval="approved"))
        self.assertEqual(state, "approved")
        self.assertIn("next run", detail)

    def test_it_agrees_with_what_can_actually_be_claimed(self):
        """The rule, not the reading: whatever `_drop_state` calls on-its-way
        must be something `claimable_for_lane` would really hand over."""
        with TemporaryDirectory() as tmp:
            base = Path(tmp)
            asset = base / "clip.mp4"
            asset.write_bytes(b"x" * 10)
            ledger = base / "l.json"
            drop_intake.record_proposals(
                {"proposals": [_proposal("ready", asset=asset),
                               _proposal("unready", asset=asset, post_ready=False)]},
                ledger_path=ledger)
            for stem in ("ready", "unready"):
                drop_intake.set_approval(stem, True, ledger_path=ledger)
            entries = json.loads(ledger.read_text())["proposals"]
            claimable = {d["stem"] for d in
                         drop_intake.claimable_for_lane("meme", ledger_path=ledger)}
            for stem, entry in entries.items():
                with self.subTest(stem=stem):
                    state, _ = drop_intake._drop_state(entry)
                    self.assertEqual(
                        state == "approved", stem in claimable,
                        f"{stem}: _drop_state says {state!r} but claimable="
                        f"{stem in claimable}")


class TheListOutputItselfTests(unittest.TestCase):
    """Covering the operator-facing string, not just the helper under it.

    `_drop_state` was correct while `--list` kept its own copy of the rules
    and printed "awaiting operator (declined)". A test on the helper alone
    would have stayed green through that, so this one drives the CLI.
    """

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.ledger = self.base / "drop_proposals.json"
        self.asset = self.base / "clip.mp4"
        self.asset.write_bytes(b"x" * 10)
        drop_intake.record_proposals(
            {"proposals": [_proposal("yes", asset=self.asset),
                           _proposal("no", asset=self.asset),
                           _proposal("untouched", asset=self.asset)]},
            ledger_path=self.ledger)
        drop_intake.set_approval("yes", True, ledger_path=self.ledger)
        drop_intake.set_approval("no", False, ledger_path=self.ledger)

    def _list_output(self) -> str:
        import contextlib
        import io
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = drop_intake.main(["--list", "--ledger-path", str(self.ledger)])
        self.assertEqual(rc, 0)
        return buf.getvalue()

    def test_a_declined_drop_is_never_called_awaiting(self):
        for line in self._list_output().splitlines():
            if "awaiting" in line.lower():
                with self.subTest(line=line.strip()):
                    self.assertNotIn(
                        "declin", line.lower(),
                        "the pre-fix bug: 'awaiting operator (declined)'")

    def test_each_drop_is_named_with_a_state_and_a_reason(self):
        out = self._list_output()
        self.assertIn("declined:", out)
        self.assertIn("CLAIMABLE:", out)
        self.assertIn("waiting_for_a_human:", out)

    def test_the_undecided_drop_is_the_only_one_awaiting_anyone(self):
        """Each drop prints as a stem line followed by its state line."""
        lines = [ln for ln in self._list_output().splitlines() if ln.strip()]
        states = {lines[i].strip(): lines[i + 1].split("->", 1)[1].strip()
                  for i in range(0, len(lines) - 1, 2)}
        self.assertEqual(
            {stem for stem, state in states.items()
             if state.startswith("waiting_for_a_human")},
            {"untouched"})
        self.assertTrue(states["yes"].startswith("CLAIMABLE"))
        self.assertTrue(states["no"].startswith("declined"))

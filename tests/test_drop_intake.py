"""Surface 124 -- the return leg: finished posts dropped by an outside agent.

The mirror lets data out. This lets work back in, which inverts the threat
model again: the drop folder is an UNTRUSTED channel whose contents are
headed for the operator's public feed. The tests that carry this file are the
refusals, and the positive control is that a well-formed drop still gets
through -- a validator asserted only by "it rejected something" is the
fail-closed bug wearing a test.

The three incidents this was written against, before any of them happened:
  * a file read while Drive was still writing it (truncated, passes every
    check applied to the bytes that arrived)
  * a drop folder placed inside ~/DuckData, which `rsync --delete` erases
  * an extension that disagrees with the file's actual contents
"""
from __future__ import annotations

import json
import tempfile
import os
import sys
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drop_intake  # noqa: E402

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
JPG = b"\xff\xd8\xff\xe0" + b"\x00" * 64
MP4 = b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 64

GOOD_SIDECAR = {
    "caption": "Five-star favourites from the flock.",
    "lane": "meme",
    "source_agent": "gemini-post-writer",
}


class _DropRoot(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = TemporaryDirectory()
        self.root = Path(self.ctx.name) / "DuckDrop"
        drop_intake.init_drop_root(self.root)
        self.inbox = self.root / drop_intake.INBOX_DIR

    def tearDown(self) -> None:
        self.ctx.cleanup()

    def drop(self, name: str, data: bytes = PNG, sidecar: dict | None = GOOD_SIDECAR,
             *, age: int = 300) -> Path:
        asset = self.inbox / name
        asset.write_bytes(data)
        if sidecar is not None:
            side = asset.with_suffix(asset.suffix + ".json")
            side.write_text(json.dumps(sidecar), encoding="utf-8")
            old = time.time() - age
            os.utime(side, (old, old))
        old = time.time() - age
        os.utime(asset, (old, old))
        return asset

    def scan(self) -> dict:
        return drop_intake.scan_inbox(self.root)


class AcceptanceTests(_DropRoot):
    def test_a_well_formed_image_drop_is_accepted(self) -> None:
        # Positive control. Without this the suite passes by refusing
        # everything, which is the fail-closed bug wearing a test.
        self.drop("good.png")
        out = self.scan()
        self.assertEqual(out["accepted_count"], 1)
        self.assertEqual(out["rejected_count"], 0)
        self.assertEqual(out["accepted"][0]["lane"], "meme")
        self.assertEqual(out["accepted"][0]["media_type"], "image")

    def test_a_well_formed_video_drop_is_accepted(self) -> None:
        self.drop("clip.mp4", MP4)
        out = self.scan()
        self.assertEqual(out["accepted_count"], 1)
        self.assertEqual(out["accepted"][0]["media_type"], "video")

    def test_an_accepted_drop_is_a_proposal_and_never_approved_work(self) -> None:
        # The lane may not promote itself to auto-action. Anything reading
        # this manifest has to see the gate.
        self.drop("good.png")
        proposal = self.scan()["proposals"][0]
        self.assertEqual(proposal["approval"], "operator_required")
        # `approval` is the only field carrying the decision. There used to be
        # a `status: "awaiting_operator"` here too, which the durable ledger
        # inherited and never updated -- see
        # test_drop_proposal_ledger.OneFieldCarriesTheDecisionTests.
        self.assertNotIn("status", proposal)

    def test_an_accepted_file_moves_out_of_the_inbox(self) -> None:
        self.drop("good.png")
        self.scan()
        self.assertEqual([], list(self.inbox.iterdir()))
        moved = list((self.root / drop_intake.ACCEPTED_DIR).glob("good.png"))
        self.assertEqual(len(moved), 1)

    def test_a_sidecar_named_without_the_double_extension_also_works(self) -> None:
        asset = self.inbox / "alt.png"
        asset.write_bytes(PNG)
        (self.inbox / "alt.json").write_text(json.dumps(GOOD_SIDECAR), encoding="utf-8")
        old = time.time() - 300
        os.utime(asset, (old, old))
        os.utime(self.inbox / "alt.json", (old, old))
        self.assertEqual(self.scan()["accepted_count"], 1)


class RefusalTests(_DropRoot):
    def _reason(self, out: dict) -> str:
        self.assertEqual(out["rejected_count"], 1, out)
        return out["rejected"][0]["reason"]

    def test_a_file_still_being_written_is_left_alone(self) -> None:
        # Drive writes downloads progressively. Reading one mid-sync yields a
        # truncated file that passes every check applied to what arrived.
        self.drop("fresh.png", age=0)
        out = self.scan()
        self.assertEqual(out["waiting_count"], 1)
        self.assertEqual(out["accepted_count"], 0)
        self.assertEqual(out["rejected_count"], 0)
        self.assertTrue((self.inbox / "fresh.png").exists())

    def test_a_settled_asset_with_an_arriving_sidecar_also_waits(self) -> None:
        asset = self.drop("pair.png", age=300)
        side = asset.with_suffix(asset.suffix + ".json")
        now = time.time()
        os.utime(side, (now, now))
        self.assertEqual(self.scan()["waiting_count"], 1)

    def test_contents_that_disagree_with_the_extension_are_refused(self) -> None:
        self.drop("trojan.png", b"#!/bin/sh\nrm -rf /\n" + b"\x00" * 64)
        self.assertIn("extension and the data disagree", self._reason(self.scan()))

    def test_an_executable_disguised_as_video_is_refused(self) -> None:
        self.drop("payload.mp4", b"\xcf\xfa\xed\xfe" + b"\x00" * 64)
        self.assertIn("disagree", self._reason(self.scan()))

    def test_an_unaccepted_type_is_refused(self) -> None:
        self.drop("notes.pdf", b"%PDF-1.4\n" + b"\x00" * 64)
        self.assertIn("not an accepted type", self._reason(self.scan()))

    def test_a_missing_sidecar_is_refused(self) -> None:
        self.drop("lonely.png", sidecar=None)
        self.assertIn("no sidecar", self._reason(self.scan()))

    def test_unparseable_sidecar_json_is_refused(self) -> None:
        asset = self.drop("bad.png")
        side = asset.with_suffix(asset.suffix + ".json")
        side.write_text("{not json", encoding="utf-8")
        old = time.time() - 300
        os.utime(side, (old, old))
        self.assertIn("does not parse", self._reason(self.scan()))

    def test_an_unknown_lane_is_refused(self) -> None:
        self.drop("x.png", sidecar={**GOOD_SIDECAR, "lane": "not_a_lane"})
        self.assertIn("not a known lane", self._reason(self.scan()))

    def test_a_missing_caption_is_refused(self) -> None:
        self.drop("x.png", sidecar={"lane": "meme", "source_agent": "a"})
        self.assertIn("caption", self._reason(self.scan()))

    def test_an_over_long_caption_is_refused(self) -> None:
        self.drop("x.png", sidecar={**GOOD_SIDECAR, "caption": "a" * 3000})
        self.assertIn("limit is 2200", self._reason(self.scan()))

    def test_an_unnamed_source_agent_is_refused(self) -> None:
        # A drop whose author is unknown cannot be judged by the operator.
        self.drop("x.png", sidecar={"caption": "hi", "lane": "meme"})
        self.assertIn("source_agent", self._reason(self.scan()))

    def test_an_empty_file_is_refused(self) -> None:
        self.drop("empty.png", b"")
        self.assertIn("empty", self._reason(self.scan()))

    def test_an_oversized_image_is_refused(self) -> None:
        self.drop("huge.png", PNG + b"\x00" * (drop_intake.MAX_IMAGE_BYTES + 1))
        self.assertIn("exceeds", self._reason(self.scan()))

    def test_a_symlink_is_refused(self) -> None:
        target = Path(self.ctx.name) / "outside.png"
        target.write_bytes(PNG)
        link = self.inbox / "link.png"
        link.symlink_to(target)
        (self.inbox / "link.png.json").write_text(
            json.dumps(GOOD_SIDECAR), encoding="utf-8")
        old = time.time() - 300
        os.utime(self.inbox / "link.png.json", (old, old))
        os.utime(link, (old, old), follow_symlinks=False)
        self.assertIn("symlink", self._reason(self.scan()))

    def test_a_symlink_is_refused_before_it_is_asked_to_settle(self) -> None:
        # Pins the ORDERING, not just the outcome. Without the scan-level
        # check a fresh symlink parks in `waiting` and is never judged -- and
        # one pointing at a file that keeps changing waits forever.
        target = Path(self.ctx.name) / "moving.png"
        target.write_bytes(PNG)
        link = self.inbox / "fresh_link.png"
        link.symlink_to(target)
        side = self.inbox / "fresh_link.png.json"
        side.write_text(json.dumps(GOOD_SIDECAR), encoding="utf-8")
        old_t = time.time() - 300
        os.utime(side, (old_t, old_t))
        # The link itself is brand new: the settle gate alone would park it.
        out = self.scan()
        self.assertEqual(out["waiting_count"], 0, out)
        self.assertIn("symlink", self._reason(out))

    def test_validate_drop_refuses_a_symlink_on_its_own(self) -> None:
        # The scan-level check and this one cover each other, so each needs
        # its own test or a mutation to either survives.
        target = Path(self.ctx.name) / "outside2.png"
        target.write_bytes(PNG)
        link = self.inbox / "direct_link.png"
        link.symlink_to(target)
        side = self.inbox / "direct_link.png.json"
        side.write_text(json.dumps(GOOD_SIDECAR), encoding="utf-8")
        with self.assertRaises(drop_intake.DropRefused) as caught:
            drop_intake.validate_drop(link, side)
        self.assertIn("symlink", str(caught.exception))

    def test_control_characters_are_stripped_from_a_caption(self) -> None:
        self.drop("x.png", sidecar={**GOOD_SIDECAR,
                                    "caption": "clean\x07\x1b[31mtext"})
        out = self.scan()
        self.assertEqual(out["accepted_count"], 1)
        self.assertNotIn("\x07", out["proposals"][0]["caption"])
        self.assertNotIn("\x1b", out["proposals"][0]["caption"])

    def test_a_refusal_writes_the_reason_back_for_the_agent_to_read(self) -> None:
        self.drop("trojan.png", b"not a png at all" + b"\x00" * 64)
        self.scan()
        reasons = list((self.root / drop_intake.REJECTED_DIR).glob("*.reason.txt"))
        self.assertEqual(len(reasons), 1)
        self.assertIn("disagree", reasons[0].read_text(encoding="utf-8"))

    def test_a_refusal_never_deletes_the_file(self) -> None:
        self.drop("trojan.png", b"nope" + b"\x00" * 64)
        self.scan()
        kept = list((self.root / drop_intake.REJECTED_DIR).glob("trojan.png"))
        self.assertEqual(len(kept), 1)

    def test_one_bad_drop_does_not_stop_a_good_one(self) -> None:
        self.drop("good.png")
        self.drop("bad.png", b"nope" + b"\x00" * 64)
        out = self.scan()
        self.assertEqual(out["accepted_count"], 1)
        self.assertEqual(out["rejected_count"], 1)


class MirrorCollisionTests(unittest.TestCase):
    def test_a_drop_root_inside_the_mirror_is_refused_loudly(self) -> None:
        # The mirror is published with `rsync --delete`. A drop folder under
        # it would be erased on the next mirror run, silently.
        with TemporaryDirectory() as tmp:
            mirror = Path(tmp) / "DuckData"
            (mirror / "inbound").mkdir(parents=True)
            original = drop_intake._mirror_root
            drop_intake._mirror_root = lambda: mirror
            try:
                with self.assertRaises(drop_intake.DropRefused) as caught:
                    drop_intake.scan_inbox(mirror / "inbound")
                self.assertIn("rsync --delete", str(caught.exception))
            finally:
                drop_intake._mirror_root = original

    def test_the_mirror_root_itself_is_refused(self) -> None:
        with TemporaryDirectory() as tmp:
            mirror = Path(tmp) / "DuckData"
            mirror.mkdir(parents=True)
            original = drop_intake._mirror_root
            drop_intake._mirror_root = lambda: mirror
            try:
                with self.assertRaises(drop_intake.DropRefused):
                    drop_intake.scan_inbox(mirror)
            finally:
                drop_intake._mirror_root = original


class LaneRegistryTests(unittest.TestCase):
    def test_lane_names_come_from_the_flow_registry(self) -> None:
        # A private second list of lane names is how a drop for a real lane
        # gets refused after someone renames it.
        lanes = drop_intake.allowed_lanes()
        self.assertIn("meme", lanes)
        self.assertIn("jeepfact", lanes)
        self.assertIn("thursday", lanes)


class MissingFolderTests(unittest.TestCase):
    def test_an_absent_drop_root_is_reported_not_raised(self) -> None:
        with TemporaryDirectory() as tmp:
            out = drop_intake.scan_inbox(Path(tmp) / "nope")
            self.assertFalse(out["available"])
            self.assertIn("does not exist yet", out["detail"])


if __name__ == "__main__":
    unittest.main()


class AgentFacingReadmeTests(unittest.TestCase):
    """The README IS the contract an outside agent reads. A stale or broken
    one is a silent integration failure: the agent writes something plausible,
    the validator refuses it, and nobody is watching either end."""

    def _readme(self) -> str:
        with tempfile.TemporaryDirectory() as tmp:
            root = drop_intake.init_drop_root(Path(tmp) / "DuckDrop")
            return (root / "README.md").read_text(encoding="utf-8")

    def test_the_readme_lists_every_lane_that_can_take_a_drop(self):
        text = self._readme()
        for lane in drop_intake.consuming_lanes():
            with self.subTest(lane=lane):
                self.assertIn(lane, text)

    def test_the_readme_does_not_advertise_a_lane_that_cannot_take_one(self):
        """The defect this replaced. The README listed every lane the system
        PUBLISHES -- eleven of them -- while exactly one could consume a drop.
        An agent reading it would write a jeepfact post in good faith, the
        operator would approve it, and it would never run."""
        text = self._readme()
        listed = {ln.strip(" -") for ln in text.splitlines()
                  if ln.strip().startswith("- ") and " " not in ln.strip(" -")}
        unconsumable = drop_intake.allowed_lanes() - drop_intake.consuming_lanes()
        for lane in sorted(unconsumable):
            with self.subTest(lane=lane):
                self.assertNotIn(
                    lane, listed,
                    f"README offers {lane}, which would be accepted and then "
                    "never published")

    def test_the_readme_says_what_to_do_when_your_lane_is_missing(self):
        """A refusal with no next step is how an agent guesses -- and the
        cheapest wrong guess here is naming a lane that IS listed, which
        publishes to the wrong audience on the wrong day."""
        text = self._readme().lower()
        self.assertIn("not listed", text)
        self.assertIn("wired", text)

    def test_the_readme_has_no_unrendered_placeholder(self):
        """`{lanes}` surviving into the published file would hand the agent a
        template instead of a contract."""
        with tempfile.TemporaryDirectory() as tmp:
            root = drop_intake.init_drop_root(Path(tmp) / "DuckDrop")
            text = (root / "README.md").read_text(encoding="utf-8")
        self.assertNotIn("{lanes}", text)
        self.assertNotIn("{", text.replace('{\n', '').split('`lane`')[0] or "{")

    def test_the_readme_points_at_the_data_folder_and_its_caveats(self):
        """An agent told only how to WRITE will read the data folder naively.
        Most of that tree is a snapshot, not a history."""
        with tempfile.TemporaryDirectory() as tmp:
            root = drop_intake.init_drop_root(Path(tmp) / "DuckDrop")
            text = (root / "README.md").read_text(encoding="utf-8")
        self.assertIn("DuckData", text)
        self.assertIn("snapshot", text)


class TheOnScreenHookIsAContractFieldTests(_DropRoot):
    """2026-11-16: the text burned INTO the clip, separate from the caption.

    Until this landed, `post_ready: true` required the pixels to be final,
    because overlay text in `notes` was prose with nothing to execute it. A
    structured field is executable, so the lane composites it -- which makes
    the field part of the safety contract, not a convenience.
    """

    def _accept(self, sidecar: dict) -> dict:
        self.drop("clip.mp4", MP4, sidecar)
        manifest = self.scan()
        self.assertEqual(len(manifest["accepted"]), 1,
                         manifest.get("proposals") or manifest)
        return manifest["proposals"][0]

    def test_the_hook_is_carried_through_to_the_proposal(self) -> None:
        hook = "POV: someone honks 0.02 seconds after the light turns green"
        prop = self._accept({**GOOD_SIDECAR, "on_screen_hook_text": hook})
        self.assertEqual(prop["on_screen_hook_text"], hook)

    def test_an_absent_hook_is_empty_not_missing(self) -> None:
        # The lane reads this field unconditionally; absent must mean "no text
        # to burn", never a KeyError on Monday morning.
        prop = self._accept(dict(GOOD_SIDECAR))
        self.assertEqual(prop["on_screen_hook_text"], "")

    def test_a_non_string_hook_is_refused(self) -> None:
        # Refused, never coerced: str(12) would silently burn "12" onto the
        # frame, and True would burn "True". Same reasoning as post_ready.
        for value in (12, ["a"], {"text": "a"}, True):
            with self.subTest(value=value):
                self.drop("clip.mp4", MP4,
                          {**GOOD_SIDECAR, "on_screen_hook_text": value})
                out = self.scan()
                self.assertEqual(out["rejected_count"], 1, out)
                self.assertIn("on_screen_hook_text",
                              out["rejected"][0]["reason"])

    def test_a_hook_too_long_to_stay_legible_is_refused(self) -> None:
        self.drop("clip.mp4", MP4,
                  {**GOOD_SIDECAR,
                   "on_screen_hook_text": "x" * (drop_intake.MAX_HOOK_CHARS + 1)})
        out = self.scan()
        self.assertEqual(out["rejected_count"], 1, out)
        self.assertIn("legible", out["rejected"][0]["reason"])
        # ...and exactly at the limit is fine, so the boundary is pinned on
        # both sides rather than only on the loud one.
        self.drop("clip2.mp4", MP4,
                  {**GOOD_SIDECAR,
                   "on_screen_hook_text": "x" * drop_intake.MAX_HOOK_CHARS})
        self.assertEqual(len(self.scan()["accepted"]), 1)

    def test_control_characters_are_stripped_from_the_hook(self) -> None:
        # This string is composited into an ffmpeg filter and rendered to a
        # text card; a stray control byte has no legitimate place in it.
        prop = self._accept({**GOOD_SIDECAR,
                             "on_screen_hook_text": "clean\x00\x07 text  "})
        self.assertEqual(prop["on_screen_hook_text"], "clean text")

    def test_the_readme_documents_the_field_and_its_limit(self) -> None:
        # An integration contract the counterparty cannot read is not a
        # contract -- the whole reason Gemini's first drop was malformed.
        readme = drop_intake.MIRROR_DROP_README
        self.assertIn("on_screen_hook_text", readme)
        self.assertIn(str(drop_intake.MAX_HOOK_CHARS), readme)
        # The old rule said the pixels had to be final. If that sentence
        # survives beside the new field the contract reads as self-
        # contradictory to whoever has to follow it.
        self.assertNotIn("ALREADY BURNED INTO THE FRAMES", readme)

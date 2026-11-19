"""Surface 133 -- the drop contract may only offer lanes that can use a drop.

The defect: `allowed_lanes()` returned the eleven lanes this system publishes,
and exactly one of them -- meme -- had a claim wired into it. A drop naming
any of the other ten validated, entered the ledger, could be approved by the
operator, and was then never published. Nothing errored, nothing logged, and
the README promised the opposite in as many words
([[feedback_fold_defer_is_a_handoff_contract]]).

`DROP_CONSUMING_LANES` fixes the contract, but it is a hand-maintained claim
about code in ANOTHER repo, which is the same shape of thing that just went
wrong. So it is checked against the flows themselves, in both directions:

  * a lane listed here that does not claim  -> the original bug, back again
  * a lane that claims but is not listed    -> wiring done, contract still
    refusing it, so the work is invisible

Scanned by AST rather than grep: a mention of `claim_for_lane` in a comment,
a docstring or an import is not a call site ([[feedback_real_generation_test_beats_mocks]] --
imported is not invoked).
"""
from __future__ import annotations

import ast
import sys
import unittest
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drop_intake  # noqa: E402

from conftest import DUCKAGENT_ROOT, requires_duckagent  # noqa: E402

#: The functions that actually take a drop, at every layer a lane might call:
#: `agent_drop_lane.claim` is the shared helper, `claim_for_lane` is duck-ops'
#: API underneath it, and `_claim_agent_drop` is a lane's own wrapper.
#:
#: All three are listed because the call SHAPE has already changed once: when
#: jeepfact became the second consumer the logic moved into a shared helper,
#: and a scanner that knew only the old name reported that no lane claimed
#: anything -- including meme, which plainly did. A scan that can return a
#: confident empty set needs a positive control, which is why
#: `test_the_scan_finds_the_lane_we_know_is_wired` exists and why it failed
#: loudly rather than letting both direction checks pass vacuously.
_CLAIM_CALLS = {"claim", "claim_for_lane", "_claim_agent_drop"}


def _module_string_constants(tree: ast.Module) -> dict[str, str]:
    """Module-level `NAME = "literal"` bindings.

    A claim call may pass a module constant rather than a literal --
    `claim_for_lane(EXPRESS_LANE, ...)` -- and a scan that counted only
    literals reported that lane as unconsumed while its consumer sat right
    there. Resolving the constant is strictly safer than the alternative,
    which is spelling the lane name twice in the consumer: a rename would then
    leave the literal at the claim site behind and the lane would claim
    something else entirely.

    Module level only, and only from a plain string literal. Anything computed
    is not resolved, because a scan that guesses is worse than one that admits
    it cannot see.
    """
    out: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not (isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                out[target.id] = node.value.value
    return out


def _lane_names_claimed_in(path: Path) -> set[str]:
    """Lane names passed to a claim call in this file.

    A string literal, or a module-level constant bound to one.
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError):  # pragma: no cover - unreadable flow
        return set()
    constants = _module_string_constants(tree)
    found: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        name = (fn.attr if isinstance(fn, ast.Attribute)
                else fn.id if isinstance(fn, ast.Name) else None)
        if name not in _CLAIM_CALLS:
            continue
        # `claim` is a common word; only count it on the drop helper.
        if name == "claim" and not (
                isinstance(fn, ast.Attribute)
                and isinstance(fn.value, ast.Name)
                and fn.value.id == "agent_drop_lane"):
            continue
        def _as_lane(node_value: ast.expr) -> str | None:
            if isinstance(node_value, ast.Constant) and isinstance(node_value.value, str):
                return node_value.value
            if isinstance(node_value, ast.Name):
                return constants.get(node_value.id)
            return None

        for arg in node.args:
            name = _as_lane(arg)
            if name:
                found.add(name)
        for kw in node.keywords:
            if kw.arg == "lane":
                name = _as_lane(kw.value)
                if name:
                    found.add(name)
    return found


def _lanes_that_claim() -> set[str]:
    """Every lane in duckAgent whose steps.py calls a claim with its own name.

    The directory name is the lane, so a flow that claims a DIFFERENT lane's
    drops shows up as a mismatch rather than being silently credited.
    """
    claiming: set[str] = set()
    flows_dir = DUCKAGENT_ROOT / "flows"
    for steps in sorted(flows_dir.glob("*/steps.py")):
        lane = steps.parent.name
        if lane in _lane_names_claimed_in(steps):
            claiming.add(lane)
    return claiming


def _declared_consumer(lane: str) -> str:
    return str((drop_intake.LANE_DROP_SPECS.get(lane) or {}).get("consumer") or "")


def _lanes_claimed_by_declared_consumers() -> set[str]:
    """Lanes claimed by the duckAgent file their own spec NAMES.

    Needed because a consumer is not always a flow. `express` has no
    flows/express/steps.py and never will: it is a standalone post with no
    generation step and no weekly slot, so it rides the ten-minute
    social_publish_due sidecar instead. Scanning only flows/*/steps.py would
    report it as unconsumed -- which is the *right* answer for a lane with no
    consumer and the wrong one for a lane whose consumer lives elsewhere.

    So the spec declares WHERE, and this checks the claim is actually there.
    That is strictly stronger than the directory convention it replaces for
    these lanes: a consumer that moves file has to update its own spec.
    """
    claiming: set[str] = set()
    for lane in sorted(drop_intake.DROP_CONSUMING_LANES):
        declared = _declared_consumer(lane)
        if not declared:
            continue
        path = DUCKAGENT_ROOT / declared
        if not path.is_file():
            continue
        if lane in _lane_names_claimed_in(path):
            claiming.add(lane)
    return claiming


@requires_duckagent
class TheRegistryMatchesTheFlowsTests(unittest.TestCase):

    def test_every_listed_lane_actually_claims(self):
        claiming = _lanes_that_claim() | _lanes_claimed_by_declared_consumers()
        for lane in sorted(drop_intake.DROP_CONSUMING_LANES):
            with self.subTest(lane=lane):
                self.assertIn(
                    lane, claiming,
                    f"DROP_CONSUMING_LANES offers {lane!r}, but neither "
                    f"flows/{lane}/steps.py nor its declared consumer "
                    f"({_declared_consumer(lane) or 'none declared'}) calls a "
                    "claim. A drop naming it would be accepted, approved, and "
                    "never published.")

    def test_every_lane_declares_where_its_consumer_lives(self):
        """A spec with no `consumer` cannot be checked at all.

        The scan above would pass vacuously for such a lane -- it would find
        no claim and fall back to the flows convention, which is exactly how
        a non-flow consumer slips through unverified.
        """
        for lane in sorted(drop_intake.DROP_CONSUMING_LANES):
            with self.subTest(lane=lane):
                declared = _declared_consumer(lane)
                self.assertTrue(
                    declared,
                    f"LANE_DROP_SPECS[{lane!r}] has no 'consumer' naming the "
                    "duckAgent file that claims it")
                self.assertTrue(
                    (DUCKAGENT_ROOT / declared).is_file(),
                    f"{lane!r} declares consumer {declared!r}, which does not "
                    "exist in duckAgent")

    def test_the_declared_consumer_scan_finds_a_non_flow_consumer(self):
        """Positive control for the half of the scan the flows walk cannot do.

        Without it, a scan that silently matched nothing would let
        `test_every_listed_lane_actually_claims` pass on the flows half alone
        and the express wiring could rot unnoticed -- the same vacuous-pass
        trap `test_the_scan_finds_the_lane_we_know_is_wired` exists for.
        """
        self.assertIn(drop_intake.EXPRESS_LANE,
                      _lanes_claimed_by_declared_consumers())
        self.assertNotIn(drop_intake.EXPRESS_LANE, _lanes_that_claim(),
                         "express has grown a flow; collapse the two scans")

    def test_every_claiming_lane_is_listed(self):
        for lane in sorted(_lanes_that_claim()):
            with self.subTest(lane=lane):
                self.assertIn(
                    lane, drop_intake.DROP_CONSUMING_LANES,
                    f"flows/{lane}/steps.py claims a drop but the contract "
                    "still refuses that lane, so the wiring is unreachable.")

    def test_the_scan_finds_the_lane_we_know_is_wired(self):
        """Positive control. Without it an AST walk that silently matched
        nothing would make both tests above pass by vacuous agreement."""
        self.assertIn("meme", _lanes_that_claim())

    def test_a_comment_mentioning_the_call_is_not_a_call_site(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            fake = Path(tmp) / "steps.py"
            fake.write_text(
                "# claim_for_lane('jeepfact') would go here one day\n"
                '"""docstring mentioning claim_for_lane("gtdf")."""\n'
                "from x import claim_for_lane\n"
                "other.claim('newduck')\n",  # a `claim` that is not ours
                encoding="utf-8")
            self.assertEqual(_lane_names_claimed_in(fake), set())

    def test_a_module_constant_resolves_but_a_computed_name_does_not(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            fake = Path(tmp) / "consumer.py"
            fake.write_text(
                'LANE = "express"\n'
                'COMPUTED = "exp" + "ress2"\n'
                "claim_for_lane(LANE, claimed_by='x')\n"
                "claim_for_lane(COMPUTED, claimed_by='x')\n"
                "claim_for_lane(unknown_name, claimed_by='x')\n",
                encoding="utf-8")
            self.assertEqual(_lane_names_claimed_in(fake), {"express"})


class TheContractRefusesAnUnconsumableLaneTests(unittest.TestCase):

    def test_consuming_is_a_subset_of_allowed(self):
        self.assertTrue(
            drop_intake.consuming_lanes() <= drop_intake.allowed_lanes(),
            "a consumable lane that is not a known lane cannot be published")

    def test_a_known_but_unconsumable_lane_is_refused_with_a_next_step(self):
        unconsumable = sorted(drop_intake.allowed_lanes()
                              - drop_intake.consuming_lanes())
        if not unconsumable:  # pragma: no cover - every lane wired
            self.skipTest("every known lane consumes drops")
        lane = unconsumable[0]
        import json, os, tempfile, time
        with tempfile.TemporaryDirectory() as tmp:
            root = drop_intake.init_drop_root(Path(tmp) / "DuckDrop")
            inbox = root / drop_intake.INBOX_DIR
            asset = inbox / "clip.mp4"
            asset.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 64)
            side = asset.with_suffix(asset.suffix + ".json")
            side.write_text(json.dumps({
                "caption": "A post for a lane with no consumer.",
                "lane": lane,
                "source_agent": "test",
            }), encoding="utf-8")
            old = time.time() - 300
            for f in (asset, side):
                os.utime(f, (old, old))
            out = drop_intake.scan_inbox(root)
        self.assertEqual(out["rejected_count"], 1, out)
        reason = out["rejected"][0]["reason"]
        self.assertIn(lane, reason)
        self.assertIn("cannot take a drop yet", reason)
        # A refusal with no next step is how an agent guesses, and the
        # cheapest wrong guess is naming a lane that IS offered.
        self.assertIn("wired", reason)
        self.assertIn("meme", reason, "name the lanes that DO work")


class TheReadmeTellsTheAgentWhatEachLaneWantsTests(unittest.TestCase):
    """A generic contract was not enough.

    The lanes differ on the day they publish, the media they accept, and --
    the one that matters most -- whether the operator's approval ALONE sends
    the post or a second reply is still required. An agent cannot guess any of
    it, and guessing wrong publishes Wednesday's content on Monday.
    """

    def _readme(self) -> str:
        """Whitespace-normalised: the file is hard-wrapped, so asserting a
        phrase that happens to straddle a line break tests the wrapping
        rather than the contract."""
        import re, tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = drop_intake.init_drop_root(Path(tmp) / "DuckDrop")
            raw = (root / "README.md").read_text(encoding="utf-8")
        return re.sub(r"\s+", " ", raw)

    def test_every_consuming_lane_is_documented(self):
        """Wiring a lane forces documenting it in the same change -- the
        alternative is a lane an agent can use but knows nothing about."""
        for lane in sorted(drop_intake.consuming_lanes()):
            with self.subTest(lane=lane):
                self.assertIn(lane, drop_intake.LANE_DROP_SPECS,
                              f"{lane} takes drops but has no LANE_DROP_SPECS entry")

    def test_each_spec_carries_every_field_the_readme_renders(self):
        for lane, spec in drop_intake.LANE_DROP_SPECS.items():
            for field in ("what", "day", "media_types", "publishes"):
                with self.subTest(lane=lane, field=field):
                    self.assertTrue(spec.get(field), f"{lane} is missing {field}")

    def test_the_readme_says_which_lanes_publish_without_a_second_look(self):
        text = self._readme()
        self.assertIn("LAST human step", text)
        self.assertIn("published mistake", text)

    def test_the_readme_carries_the_production_rules_that_bit_us(self):
        text = self._readme().lower()
        for rule in ("80 characters", "portrait", "verbatim", "one asset per drop"):
            with self.subTest(rule=rule):
                self.assertIn(rule, text)


class TheMediaTypeClaimIsEnforcedTests(unittest.TestCase):
    """The README says duckvideo is "video only". A README that claims
    something the validator does not check is the exact shape of lie this
    surface was just fixed for, so the prose is DERIVED from the enforced
    tuple rather than written beside it."""

    def _drop(self, lane: str, name: str, data: bytes):
        import json, os, tempfile, time
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = drop_intake.init_drop_root(Path(tmp.name) / "DuckDrop")
        asset = root / drop_intake.INBOX_DIR / name
        asset.write_bytes(data)
        side = asset.with_suffix(asset.suffix + ".json")
        side.write_text(json.dumps({
            "caption": "x", "lane": lane, "source_agent": "test",
        }), encoding="utf-8")
        old = time.time() - 300
        for f in (asset, side):
            os.utime(f, (old, old))
        return drop_intake.scan_inbox(root)

    def test_an_image_for_a_video_only_lane_is_refused(self):
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
        out = self._drop("duckvideo", "still.png", png)
        self.assertEqual(out["rejected_count"], 1, out)
        reason = out["rejected"][0]["reason"]
        self.assertIn("video only", reason)
        self.assertIn("fail when the lane tried to use it", reason)

    def test_a_video_for_a_video_only_lane_is_accepted(self):
        mp4 = b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 64
        out = self._drop("duckvideo", "clip.mp4", mp4)
        self.assertEqual(len(out["accepted"]), 1, out)

    def test_an_image_is_fine_for_a_lane_that_takes_both(self):
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
        out = self._drop("meme", "panel.png", png)
        self.assertEqual(len(out["accepted"]), 1, out)

    def test_the_prose_is_derived_from_the_enforced_tuple(self):
        # Not asserted as a string pair: the point is that one produces the
        # other, so they cannot drift.
        for lane, spec in drop_intake.LANE_DROP_SPECS.items():
            with self.subTest(lane=lane):
                phrase = drop_intake._media_phrase(tuple(spec["media_types"]))
                for t in spec["media_types"]:
                    self.assertIn(t, phrase)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

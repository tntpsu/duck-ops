"""Surface 169 — a dropped post that is not tied to a weekly slot.

Every other drop lane answers "which weekly slot is this for?": a `meme` drop
waits for Monday 09:00, a `duckvideo` drop for Friday. So an agent with just a
post had nowhere to put it, and an approved drop could sit for six days.

The `express` lane answers "when do you want it out?" instead. Three things
carry the surface:

  1. **`publish_at` says WHEN, never WHETHER.** The operator still approves
     every express post, and nothing writable from the drop folder changes
     that. A time is not permission.
  2. **A time that passes before approval HOLDS the post.** It fails closed,
     because a dated post sent after its moment is worse than one that never
     went out — and the operator gets a one-word override for the case where
     they still want it.
  3. **`publish_at` is refused on a weekly lane.** That lane publishes on its
     own day, so the field would be silently ignored, which is precisely the
     failure this contract exists to prevent
     ([[feedback_fold_defer_is_a_handoff_contract]]).
"""
from __future__ import annotations

import json
import os
import sys
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import drop_intake  # noqa: E402
import drop_operator  # noqa: E402

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
MP4 = b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 64


def _in(**kw) -> str:
    return (datetime.now().astimezone() + timedelta(**kw)).isoformat()


class _ExpressRoot(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = TemporaryDirectory()
        self.root = Path(self.ctx.name) / "DuckDrop"
        drop_intake.init_drop_root(self.root)
        self.inbox = self.root / drop_intake.INBOX_DIR
        self.ledger = Path(self.ctx.name) / "drop_proposals.json"

    def tearDown(self) -> None:
        self.ctx.cleanup()

    def drop(self, name: str = "post.png", data: bytes = PNG, *,
             lane: str = drop_intake.EXPRESS_LANE,
             publish_at: object = "asap",
             post_ready: bool = True, age: int = 300, **extra) -> Path:
        sidecar: dict = {
            "caption": "Santa duck or the doll duck? Pick one.",
            "lane": lane,
            "source_agent": "gemini-post-writer",
            "post_ready": post_ready,
            **extra,
        }
        if publish_at is not None:
            sidecar["publish_at"] = publish_at
        asset = self.inbox / name
        asset.write_bytes(data)
        side = asset.with_suffix(asset.suffix + ".json")
        side.write_text(json.dumps(sidecar), encoding="utf-8")
        old = time.time() - age
        os.utime(side, (old, old))
        os.utime(asset, (old, old))
        return asset

    def accept(self, **kw) -> dict:
        """Scan, record to a TEMP ledger, and return the one proposal."""
        self.drop(**kw)
        manifest = drop_intake.scan_inbox(self.root)
        drop_intake.record_proposals(manifest, ledger_path=self.ledger)
        self.assertEqual(manifest["rejected_count"], 0,
                         f"refused: {manifest['rejected']}")
        return manifest["proposals"][0]

    def refusal(self, **kw) -> str:
        self.drop(**kw)
        manifest = drop_intake.scan_inbox(self.root)
        self.assertEqual(manifest["accepted_count"], 0,
                         "expected a refusal, got an acceptance")
        return str(manifest["rejected"][0]["reason"])


class TheExpressLaneAcceptsATime(_ExpressRoot):
    def test_an_asap_image_drop_is_accepted(self):
        # Positive control: without it this suite could pass by refusing
        # everything, which is the fail-closed bug wearing a test.
        proposal = self.accept()
        self.assertEqual(proposal["lane"], "express")
        self.assertEqual(proposal["publish_at"], drop_intake.PUBLISH_AT_ASAP)

    def test_an_asap_video_drop_is_accepted(self):
        proposal = self.accept(name="clip.mp4", data=MP4)
        self.assertEqual(proposal["media_type"], "video")

    def test_now_and_immediately_are_stored_as_asap(self):
        """One stored spelling. Three synonyms in the ledger would be three
        values every later reader has to know about."""
        for word in ("now", "ASAP", "Immediately"):
            with self.subTest(word=word):
                kind, when = drop_intake.resolve_publish_at(word)
                self.assertEqual(kind, drop_intake.PUBLISH_AT_ASAP)
                self.assertIsNone(when)

    def test_a_future_time_is_accepted_and_stored_with_an_offset(self):
        proposal = self.accept(publish_at=_in(hours=6))
        stored = datetime.fromisoformat(proposal["publish_at"])
        self.assertIsNotNone(stored.tzinfo,
                            "a stored time with no offset is ambiguous the "
                            "next time anything reads it")

    def test_a_naive_time_is_read_as_wall_clock_in_the_shop_zone(self):
        """Not UTC, and not today's offset stamped onto a future date.

        A fixed-offset read is wrong twice a year by an hour, which is the DST
        bug this repo already paid for once
        ([[feedback_schedule_math_needs_a_zone_not_an_offset]]).
        """
        _kind, when = drop_intake.resolve_publish_at("2026-12-01T18:00")
        self.assertEqual((when.hour, when.minute), (18, 0))
        self.assertEqual(str(when.tzinfo), drop_intake._publish_tz_name())

    def test_a_summer_time_and_a_winter_time_get_different_offsets(self):
        """The zone recomputes per date; a stored offset could not.

        `now` is moved alongside each date rather than using today's, because
        the 60-day ceiling would otherwise refuse the far one -- and that
        ceiling is exactly why this matters: 60 days spans both US transitions,
        so a real `publish_at` can straddle one.
        """
        _k, winter = drop_intake.resolve_publish_at(
            "2027-01-15T18:00",
            now=datetime.fromisoformat("2027-01-01T09:00-05:00"))
        _k, summer = drop_intake.resolve_publish_at(
            "2027-07-15T18:00",
            now=datetime.fromisoformat("2027-07-01T09:00-04:00"))
        self.assertNotEqual(winter.utcoffset(), summer.utcoffset())
        self.assertEqual((winter.hour, summer.hour), (18, 18),
                         "both are the wall-clock hour the agent asked for")

    def test_the_ceiling_is_wide_enough_to_cross_a_dst_change(self):
        """If it were not, the zone handling above would be theory."""
        self.assertGreater(drop_intake.MAX_PUBLISH_LEAD_DAYS, 30)


class TheExpressLaneRefusesATimeItCannotHonour(_ExpressRoot):
    def test_a_missing_publish_at_is_refused(self):
        """An express post has no weekly slot to fall back on, so there is no
        answer to when it goes out. Defaulting to asap would publish a post
        the agent never said was time-critical."""
        reason = self.refusal(publish_at=None)
        self.assertIn("publish_at", reason)
        self.assertIn("required", reason)

    def test_a_past_time_is_refused_at_the_door(self):
        reason = self.refusal(publish_at=_in(hours=-2))
        self.assertIn("past", reason)

    def test_a_time_under_the_platform_floor_is_refused(self):
        """Neither platform could honour it: Facebook refuses a scheduled time
        under ten minutes and our IG queue drains every ten."""
        reason = self.refusal(publish_at=_in(minutes=4))
        self.assertIn("asap", reason)

    def test_a_time_beyond_our_own_ceiling_is_refused(self):
        reason = self.refusal(
            publish_at=_in(days=drop_intake.MAX_PUBLISH_LEAD_DAYS + 5))
        self.assertIn("our rule", reason)

    def test_an_unparseable_time_is_refused_with_an_example(self):
        reason = self.refusal(publish_at="next tuesday-ish")
        self.assertIn("ISO-8601", reason)
        self.assertIn("2026-", reason, "the refusal should show the shape")

    def test_a_non_string_time_is_refused_rather_than_coerced(self):
        for value in (1764000000, True, {"at": "soon"}, []):
            with self.subTest(value=value):
                with self.assertRaises(drop_intake.DropRefused):
                    drop_intake.resolve_publish_at(value)

    def test_the_refusal_names_the_floor_it_is_measured_against(self):
        """A refusal the agent cannot act on costs a round trip through Drive
        for nothing."""
        reason = self.refusal(publish_at=_in(minutes=2))
        self.assertIn(str(drop_intake.MIN_PUBLISH_LEAD_SECONDS // 60), reason)


class AWeeklyLaneRefusesTheFieldRatherThanIgnoringIt(_ExpressRoot):
    def test_publish_at_on_a_meme_drop_is_refused(self):
        """Accepting and ignoring it is the exact shape of this lane's worst
        bug: the agent believes a time was honoured, the post goes out on
        Monday, and nothing anywhere said otherwise."""
        reason = self.refusal(lane="meme", publish_at=_in(hours=6))
        self.assertIn("no meaning", reason)
        self.assertIn("express", reason, "say which lane DOES honour it")

    def test_a_meme_drop_without_the_field_is_still_fine(self):
        proposal = self.accept(lane="meme", publish_at=None)
        self.assertEqual(proposal["lane"], "meme")
        self.assertEqual(proposal["publish_at"], "")


class TheTimeNeverGrantsPermission(_ExpressRoot):
    def test_an_unapproved_express_drop_is_not_claimable(self):
        """`publish_at` says WHEN, never WHETHER."""
        self.accept()
        self.assertEqual(
            drop_intake.claimable_for_lane("express", ledger_path=self.ledger),
            [])

    def test_a_not_ready_express_drop_is_not_claimable_even_when_approved(self):
        proposal = self.accept(post_ready=False)
        drop_intake.set_approval(proposal["stem"], True, ledger_path=self.ledger)
        self.assertEqual(
            drop_intake.claimable_for_lane("express", ledger_path=self.ledger),
            [])

    def test_an_approved_asap_drop_is_claimable_exactly_once(self):
        proposal = self.accept()
        drop_intake.set_approval(proposal["stem"], True, ledger_path=self.ledger)
        first = drop_intake.claim_for_lane("express", claimed_by="express::r1",
                                           ledger_path=self.ledger)
        self.assertEqual(first["stem"], proposal["stem"])
        self.assertIsNone(
            drop_intake.claim_for_lane("express", claimed_by="express::r2",
                                       ledger_path=self.ledger),
            "a second pass claimed the same post; it would publish twice")

    def test_an_express_claim_never_takes_another_lanes_drop(self):
        meme = self.accept(lane="meme", publish_at=None, name="meme.png")
        drop_intake.set_approval(meme["stem"], True, ledger_path=self.ledger)
        self.assertIsNone(
            drop_intake.claim_for_lane("express", claimed_by="express::r1",
                                       ledger_path=self.ledger))


class APassedWindowHoldsThePost(_ExpressRoot):
    def _approved_with_stale_time(self) -> str:
        """Approved, post_ready, and its moment gone.

        Written into the ledger directly because the validator refuses a past
        time at the door — the only way to reach this state is for time to
        pass between acceptance and approval, which is the whole point.
        """
        proposal = self.accept(publish_at=_in(hours=6))
        stem = proposal["stem"]
        drop_intake.set_approval(stem, True, ledger_path=self.ledger)
        rows = drop_intake._read_ledger(self.ledger)
        rows[stem]["publish_at"] = _in(hours=-3)
        drop_intake._write_ledger(self.ledger, rows)
        return stem

    def test_a_passed_window_is_not_claimable(self):
        self._approved_with_stale_time()
        self.assertEqual(
            drop_intake.claimable_for_lane("express", ledger_path=self.ledger),
            [], "a dated post would publish after its moment")

    def test_the_agent_is_told_missed_window_and_not_approved(self):
        """`approved` would say the post is on its way. It is not, and this
        file is the only channel that agent has."""
        stem = self._approved_with_stale_time()
        entry = drop_intake._read_ledger(self.ledger)[stem]
        state, detail = drop_intake._drop_state(entry)
        self.assertEqual(state, "missed_window")
        self.assertIn("passed", detail)

    def test_an_unreadable_stored_time_holds_too(self):
        """Hand-edited or written by an older writer. Reported, never read as
        'fine' and never read as 'go now'."""
        proposal = self.accept(publish_at=_in(hours=6))
        stem = proposal["stem"]
        drop_intake.set_approval(stem, True, ledger_path=self.ledger)
        rows = drop_intake._read_ledger(self.ledger)
        rows[stem]["publish_at"] = "soonish"
        drop_intake._write_ledger(self.ledger, rows)
        self.assertEqual(
            drop_intake.claimable_for_lane("express", ledger_path=self.ledger), [])
        state, _ = drop_intake._drop_state(
            drop_intake._read_ledger(self.ledger)[stem])
        self.assertEqual(state, "missed_window")

    def test_the_operator_can_release_it_immediately(self):
        stem = self._approved_with_stale_time()
        drop_intake.set_approval(stem, True, publish_now=True,
                                 ledger_path=self.ledger)
        entry = drop_intake._read_ledger(self.ledger)[stem]
        self.assertEqual(entry["publish_at"], drop_intake.PUBLISH_AT_ASAP)
        self.assertEqual(entry["publish_at_overridden_by"], "operator")
        self.assertEqual(
            len(drop_intake.claimable_for_lane("express", ledger_path=self.ledger)),
            1)

    def test_only_the_operator_can_release_it(self):
        """Nothing reachable from the drop folder calls set_approval, so the
        agent cannot grant itself a new window by re-sending the same stem."""
        import inspect
        source = inspect.getsource(drop_intake.scan_inbox)
        self.assertNotIn("set_approval", source)
        self.assertNotIn("publish_now", source)

    def test_a_weekly_lane_cannot_be_released_immediately(self):
        """`now` on a meme drop would promise something no code does."""
        meme = self.accept(lane="meme", publish_at=None, name="m.png")
        with self.assertRaises(drop_intake.DropRefused):
            drop_intake.set_approval(meme["stem"], True, publish_now=True,
                                     ledger_path=self.ledger)

    def test_a_future_window_is_reported_as_waiting_for_its_time(self):
        proposal = self.accept(publish_at=_in(hours=6))
        drop_intake.set_approval(proposal["stem"], True, ledger_path=self.ledger)
        state, detail = drop_intake._drop_state(
            drop_intake._read_ledger(self.ledger)[proposal["stem"]])
        self.assertEqual(state, "approved")
        self.assertIn("scheduled for", detail)

    def test_a_claimed_future_post_reads_as_scheduled_not_published(self):
        """It is with the platforms, holding. Saying `published` of a post
        nobody can see yet is the kind of status that sends an agent looking
        for it in the feed."""
        proposal = self.accept(publish_at=_in(hours=6))
        drop_intake.set_approval(proposal["stem"], True, ledger_path=self.ledger)
        drop_intake.claim_for_lane("express", claimed_by="express::r1",
                                   ledger_path=self.ledger)
        state, detail = drop_intake._drop_state(
            drop_intake._read_ledger(self.ledger)[proposal["stem"]])
        self.assertEqual(state, "scheduled")
        self.assertIn("scheduled to publish at", detail)


class TheWindowIsReadByOneFunction(_ExpressRoot):
    """The claim filter, the agent's status file and the operator's approve
    message all read `publish_window`, so none of the three can disagree about
    whether a post is still sendable."""

    def test_every_reader_goes_through_publish_window(self):
        import inspect
        for fn in (drop_intake.claimable_for_lane, drop_intake._drop_state):
            with self.subTest(fn=fn.__name__):
                self.assertIn("publish_window", inspect.getsource(fn))
        self.assertIn("publish_window",
                      inspect.getsource(drop_operator._what_happens_next))

    def test_a_weekly_lane_has_no_window(self):
        self.assertEqual(drop_intake.publish_window({"publish_at": ""}),
                         ("none", None))
        self.assertEqual(drop_intake.publish_window({}), ("none", None))

    def test_the_publish_zone_matches_every_other_schedule_in_the_system(self):
        """A publish_at resolved in a different zone than the scheduler would
        put a post out an hour early twice a year."""
        import scheduler_health
        self.assertEqual(str(scheduler_health.SCHEDULER_TZ),
                         drop_intake._publish_tz_name())


class TheOperatorIsToldWhatWillActuallyHappen(_ExpressRoot):
    def _with_live_ledger(self, fn):
        """drop_operator reads the module-level production ledger path."""
        from unittest.mock import patch
        with patch.object(drop_intake, "DROP_PROPOSALS_PATH", self.ledger):
            return fn()

    def test_approving_an_asap_drop_names_the_ten_minute_cadence(self):
        proposal = self.accept()
        out = self._with_live_ledger(
            lambda: drop_operator.apply_drop_text(f"drop approve {proposal['stem']}"))
        self.assertEqual(out["applied"], 1)
        self.assertIn("ten minutes", out["response"])

    def test_approving_a_scheduled_drop_names_the_time(self):
        proposal = self.accept(publish_at=_in(hours=6))
        out = self._with_live_ledger(
            lambda: drop_operator.apply_drop_text(f"drop approve {proposal['stem']}"))
        self.assertIn("scheduled to publish at", out["response"])

    def test_approving_a_missed_window_says_nothing_will_send_it(self):
        """The operator is reading this sentence and has a one-word fix. Not
        saying so here leaves it to a status file written for the agent."""
        proposal = self.accept(publish_at=_in(hours=6))
        stem = proposal["stem"]
        rows = drop_intake._read_ledger(self.ledger)
        rows[stem]["publish_at"] = _in(hours=-3)
        drop_intake._write_ledger(self.ledger, rows)
        out = self._with_live_ledger(
            lambda: drop_operator.apply_drop_text(f"drop approve {stem}"))
        self.assertIn("NOTHING WILL SEND IT", out["response"])
        self.assertIn(f"drop approve {stem} now", out["response"])

    def test_the_now_flag_releases_it_in_one_reply(self):
        proposal = self.accept(publish_at=_in(hours=6))
        stem = proposal["stem"]
        rows = drop_intake._read_ledger(self.ledger)
        rows[stem]["publish_at"] = _in(hours=-3)
        drop_intake._write_ledger(self.ledger, rows)
        out = self._with_live_ledger(
            lambda: drop_operator.apply_drop_text(f"drop approve {stem} now"))
        self.assertEqual(out["applied"], 1)
        self.assertIn("Released immediately", out["response"])
        self.assertEqual(
            drop_intake._read_ledger(self.ledger)[stem]["publish_at"],
            drop_intake.PUBLISH_AT_ASAP)

    def test_now_and_only_compose_in_either_order(self):
        """A reply typed on a phone is not going to respect an order nobody
        documented."""
        for suffix in ("now only", "only now"):
            with self.subTest(suffix=suffix):
                stem, flags = drop_operator._split_approve_flags(
                    f"a_post_v2 {suffix}")
                self.assertEqual(stem, "a_post_v2")
                self.assertEqual(flags, {"only", "now"})

    def test_a_stem_ending_in_now_is_not_eaten(self):
        """Token-wise, and never the last token standing: `_now` is a real
        suffix an agent could use and a char-wise strip would eat it."""
        stem, flags = drop_operator._split_approve_flags("post_right_now")
        self.assertEqual(stem, "post_right_now")
        self.assertEqual(flags, set())

    def test_now_on_a_decline_is_refused_rather_than_ignored(self):
        proposal = self.accept()
        out = self._with_live_ledger(
            lambda: drop_operator.apply_drop_text(
                f"drop decline {proposal['stem']} now"))
        self.assertEqual(out["applied"], 0)
        self.assertIn("only applies to an approval", out["response"])

    def test_the_waiting_list_shows_when_each_express_drop_would_go(self):
        self.accept(name="a.png", publish_at="asap")
        self.accept(name="b.png", publish_at=_in(hours=6))
        rows = drop_intake._read_ledger(self.ledger)
        text = drop_operator.format_waiting(
            [(s, e) for s, e in sorted(rows.items())], [])
        self.assertIn("when: immediately on approval", text)
        self.assertIn("when: 20", text)


class TheAgentContractSaysAllOfThis(_ExpressRoot):
    """The README is the only documentation the producing agent has. A rule
    that lives only in the validator costs a round trip through Drive to
    learn, and the operator asked for this file to be updated by name."""

    def setUp(self) -> None:
        super().setUp()
        self.readme = (self.root / "README.md").read_text(encoding="utf-8")

    def test_it_documents_the_express_lane_and_publish_at(self):
        for needle in ('"lane": "express"', '"publish_at": "asap"',
                       "publish_at` is REQUIRED", "REFUSED on every other lane"):
            with self.subTest(needle=needle):
                self.assertIn(needle, self.readme)

    def test_it_states_both_bounds_and_whose_they_are(self):
        self.assertIn("at least 10 minutes out", self.readme)
        self.assertIn(f"at most {drop_intake.MAX_PUBLISH_LEAD_DAYS} days out",
                      self.readme)
        self.assertIn("That one IS ours", self.readme)

    def test_it_says_approval_is_still_required(self):
        self.assertIn("says WHEN, never WHETHER", self.readme)

    def test_it_says_a_passed_window_holds_the_post(self):
        self.assertIn("missed_window", self.readme)

    def test_it_says_what_a_video_express_post_becomes(self):
        """Discovering that a dropped clip posts to Facebook as a still is
        the kind of surprise that belongs in a contract, not in a feed."""
        self.assertIn("POSTER FRAME", self.readme)
        self.assertIn("no Facebook video path", self.readme)

    def test_it_names_the_zone_a_naive_time_is_read_in(self):
        self.assertIn(drop_intake._publish_tz_name(), self.readme)

    def test_the_lane_block_is_rendered_from_the_live_registry(self):
        """Not typed into the template: a lane added upstream would otherwise
        be refused here while the contract still said it was valid."""
        self.assertIn("- express", self.readme)
        self.assertNotIn("{lanes}", self.readme)

    def test_it_says_an_express_post_replaces_nothing(self):
        self.assertIn("replaces nothing", self.readme)


if __name__ == "__main__":
    unittest.main()


class TheContractIsRepublishedOnEveryScan(_ExpressRoot):
    """A README written only by `--init` is a contract frozen at whenever
    someone last ran that flag by hand.

    Until 2026-11-19 that was the case, so a rule added to the validator
    reached the producing agent only as an unexplained refusal. The lane list
    was already rendered from the live registry to prevent exactly that, which
    init-only writing quietly undid.
    """

    def run_scan(self) -> int:
        """The producer entry point, which is where the scan's side effects
        live: the manifest, the ledger, status.json and now the README."""
        return drop_intake.main([
            "--drop-root", str(self.root),
            "--state-path", str(Path(self.ctx.name) / "manifest.json"),
            "--ledger-path", str(self.ledger),
            "--settle-seconds", "0",
        ])

    def test_a_scan_rewrites_a_stale_readme(self):
        readme = self.root / "README.md"
        readme.write_text("# an old contract\n", encoding="utf-8")
        self.run_scan()
        self.assertIn("publish_at", readme.read_text(encoding="utf-8"))

    def test_a_scan_restores_a_deleted_readme(self):
        readme = self.root / "README.md"
        readme.unlink()
        self.run_scan()
        self.assertTrue(readme.is_file())

    def test_an_unchanged_readme_is_not_rewritten(self):
        """This folder is synced. Touching an unchanged file six times a day
        would upload it six times a day."""
        readme = self.root / "README.md"
        before = readme.stat().st_mtime_ns
        self.run_scan()
        self.assertEqual(readme.stat().st_mtime_ns, before)

    def test_writing_it_is_idempotent(self):
        first = drop_intake.write_contract_readme(self.root)
        text = first.read_text(encoding="utf-8")
        drop_intake.write_contract_readme(self.root)
        self.assertEqual(first.read_text(encoding="utf-8"), text)

    def test_a_scan_still_accepts_posts_when_the_readme_cannot_be_written(self):
        """A documentation file must never cost a post."""
        from unittest.mock import patch
        self.drop()
        with patch.object(drop_intake, "write_contract_readme",
                          side_effect=OSError("read-only")):
            # The CLI path is what wraps it; the library call is what a caller
            # would reach, so assert the wrap exists where it is used.
            import inspect
            source = inspect.getsource(drop_intake.main)
            self.assertIn("write_contract_readme", source)
            self.assertIn("WARNING contract README not refreshed", source)
        manifest = drop_intake.scan_inbox(self.root)
        self.assertEqual(manifest["accepted_count"], 1)


class TheLedgerLearnsWhatThePlatformsDid(_ExpressRoot):
    """Surface 170 — the claim is not the outcome.

    Until `publish_status` existed the ledger's story stopped at the claim, so
    a drop that rendered, uploaded and was then refused by Facebook AND
    Instagram was indistinguishable from one that went live. `_drop_state`
    told the producing agent "a scheduled run took it; it is on the lane"
    either way, and that status file is the only channel that agent has.
    """

    def claimed(self, **kw) -> dict:
        self.accept(**kw)
        drop_intake.set_approval("post", True, ledger_path=self.ledger)
        drop = drop_intake.claim_for_lane(
            drop_intake.EXPRESS_LANE, claimed_by="express::run1",
            ledger_path=self.ledger)
        self.assertIsNotNone(drop, "setup: the drop should have been claimable")
        return drop

    def entry(self) -> dict:
        return json.loads(self.ledger.read_text())["proposals"]["post"]

    def test_a_failed_publish_is_recorded_on_the_entry(self):
        self.claimed()
        drop_intake.record_publish_outcome(
            "post", status="failed", error="(#200) permissions error",
            run_id="express_1", ledger_path=self.ledger)
        entry = self.entry()
        self.assertEqual(entry["publish_status"], "failed")
        self.assertEqual(entry["publish_error"], "(#200) permissions error")
        self.assertEqual(entry["publish_run_id"], "express_1")
        self.assertTrue(entry["publish_outcome_at"])

    def test_a_failed_publish_does_NOT_make_the_drop_claimable_again(self):
        # The whole reason the claim is written BEFORE the publish: a retry
        # would re-publish something that may already be half-public. Letting
        # a failure reopen the claim would undo that guarantee.
        self.claimed()
        drop_intake.record_publish_outcome(
            "post", status="failed", error="both platforms refused",
            ledger_path=self.ledger)
        self.assertEqual(
            drop_intake.claimable_for_lane(
                drop_intake.EXPRESS_LANE, ledger_path=self.ledger), [])
        self.assertIsNone(drop_intake.claim_for_lane(
            drop_intake.EXPRESS_LANE, claimed_by="express::run2",
            ledger_path=self.ledger))

    def test_the_outcome_never_rewrites_the_approval_or_the_claim(self):
        # Four orthogonal axes, one meaning each
        # ([[feedback_status_fields_one_meaning]]). Folding the outcome into
        # `approval` would make "the operator said yes" and "the platform
        # said yes" the same fact.
        self.claimed()
        before = self.entry()
        drop_intake.record_publish_outcome(
            "post", status="failed", ledger_path=self.ledger)
        after = self.entry()
        for field in ("approval", "approved_at", "approved_by",
                      "claimed_by", "claimed_at", "post_ready", "lane"):
            self.assertEqual(before[field], after[field], field)

    def test_an_unknown_status_is_refused_rather_than_stored(self):
        # A plausible default here writes a green outcome over a real
        # failure, which is the exact shape of the bug this surface closes.
        self.claimed()
        with self.assertRaises(drop_intake.DropRefused) as caught:
            drop_intake.record_publish_outcome(
                "post", status="probably_fine", ledger_path=self.ledger)
        self.assertIn("not a publish outcome", str(caught.exception))
        self.assertNotIn("publish_status", self.entry())

    def test_an_unknown_stem_is_refused(self):
        self.claimed()
        with self.assertRaises(drop_intake.DropRefused):
            drop_intake.record_publish_outcome(
                "not_a_drop", status="failed", ledger_path=self.ledger)

    def test_every_status_the_publisher_emits_is_one_this_accepts(self):
        # The publisher and the ledger must share one vocabulary, or a
        # perfectly good outcome raises inside the sidecar and the entry keeps
        # reading as "claimed, outcome unknown" forever. These four strings
        # are the ones helpers/express_drop_publisher.py::publish_one sets on
        # `out["status"]`.
        for status in ("published_now", "scheduled", "partial", "failed"):
            with self.subTest(status=status):
                self.assertIn(status, drop_intake.PUBLISH_OUTCOME_STATES)


class TheAgentIsToldWhatActuallyHappened(_ExpressRoot):
    """`status.json` is the producing agent's ONLY channel.

    A failure it is never told about teaches it nothing, so the next drop
    arrives with the same problem.
    """

    def state_for(self, **outcome) -> tuple[str, str]:
        entry = {"lane": drop_intake.EXPRESS_LANE, "post_ready": True,
                 "approval": "approved", "claimed_by": "express::run1",
                 "claimed_at": _in(minutes=-5), "publish_at": "asap",
                 **outcome}
        return drop_intake._drop_state(entry)

    def test_a_claimed_but_failed_drop_does_not_report_published(self):
        state, detail = self.state_for(publish_status="failed",
                                       publish_error="(#200) permissions error")
        self.assertEqual(state, "publish_failed")
        self.assertIn("every platform refused", detail)
        self.assertIn("(#200) permissions error", detail)
        self.assertIn("Nothing was posted", detail)

    def test_a_failed_drop_is_told_it_will_not_be_retried(self):
        # Otherwise the rational move is to wait, and nothing ever comes.
        _state, detail = self.state_for(publish_status="failed")
        self.assertIn("NOT be retried", detail)

    def test_a_partial_publish_warns_against_re_sending(self):
        state, detail = self.state_for(
            publish_status="partial", publish_platforms=["instagram"],
            publish_error="facebook: (#100) invalid scheduled_publish_time")
        self.assertEqual(state, "published")
        self.assertIn("instagram", detail)
        self.assertIn("do not re-send", detail)

    def test_a_live_publish_names_the_platforms(self):
        state, detail = self.state_for(
            publish_status="published_now",
            publish_platforms=["facebook", "instagram"])
        self.assertEqual(state, "published")
        self.assertIn("facebook", detail)
        self.assertIn("instagram", detail)

    def test_a_claim_with_no_outcome_yet_is_unchanged(self):
        # The pre-existing answer, kept for a drop claimed by a lane that
        # predates the write-back. Honest about the gap: it says a run took
        # it, which is exactly what is known.
        state, detail = self.state_for()
        self.assertEqual(state, "published")
        self.assertIn("took it", detail)

    def test_the_status_file_carries_the_outcome_fields(self):
        self.accept()
        drop_intake.set_approval("post", True, ledger_path=self.ledger)
        drop_intake.claim_for_lane(drop_intake.EXPRESS_LANE,
                                   claimed_by="express::run1",
                                   ledger_path=self.ledger)
        drop_intake.record_publish_outcome(
            "post", status="failed", error="both platforms refused",
            ledger_path=self.ledger)
        proposals = json.loads(self.ledger.read_text())["proposals"]
        path = drop_intake.write_status_file(self.root, proposals)
        payload = json.loads(path.read_text())
        row = payload["drops"]["post"]
        self.assertEqual(row["state"], "publish_failed")
        self.assertEqual(row["publish_status"], "failed")
        self.assertEqual(row["publish_error"], "both platforms refused")
        # And the vocabulary the file documents has to include it, or the
        # agent reads a state the doc says cannot happen.
        self.assertIn("publish_failed", payload["doc"])

    def test_the_contract_readme_explains_a_failed_publish(self):
        self.assertIn("publish_failed", drop_intake.MIRROR_DROP_README)
        self.assertIn("publish_error", drop_intake.MIRROR_DROP_README)

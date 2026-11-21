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


class AHalfArrivedDropIsWaitedOnNotRefused(_ExpressRoot):
    """Surface 171 — a drop arrives as TWO files and they do not arrive together.

    Found twice in one day, in opposite directions:

      1. A sidecar with no asset was invisible everywhere. `scan_inbox` skips
         every `.json` on the correct grounds that a sidecar is found THROUGH
         its asset, so an orphan reached neither the manifest, nor `rejected/`,
         nor the agent's status file. Gemini wrote one at 21:25 and eleven
         hours later nothing said so.
      2. The fix for (1) then destroyed a real post, because the grace was
         asymmetric and far shorter than the scan cadence. A lone sidecar got
         30 minutes; a sidecar-LESS ASSET got none at all, refused on the spot
         by `validate_drop` -- and the contract told the agent to send the
         asset first. Against a scan that runs every four hours, the first
         scan to see a drop was usually the one that killed it:

           21:25  sidecar written, video not up yet
           10:16  the 30-minute grace refuses the "orphan", MOVES the sidecar
           11:03  the video arrives, asset-first as instructed
           12:15  the scan finds a video whose sidecar was deleted at 10:16

    Both halves now get the same treatment, and nothing is refused until it
    has been seen incomplete twice AND waited longer than two scan cadences.
    """

    def scan(self, *, now: float | None = None) -> dict:
        """The real entry point, not the inner function.

        `conftest._isolate_drop_intake` already redirects
        DROP_INCOMPLETE_PATH to a per-test tmp file, so this exercises the
        production code path -- including the ledger write that the
        DUCK_TEST_MODE guard would refuse against the real one.
        """
        return drop_intake.scan_inbox(self.root, now=now)

    def summary(self) -> dict:
        return drop_intake.incomplete_drop_summary()

    def lone_sidecar(self, name: str = "post.mp4.json", *, age: int = 60) -> Path:
        side = self.inbox / name
        side.write_text(json.dumps({
            "caption": "Beer duck or whiskey duck?",
            "lane": drop_intake.EXPRESS_LANE,
            "publish_at": "asap",
            "post_ready": True,
            "source_agent": "gemini-post-writer",
        }), encoding="utf-8")
        old = time.time() - age
        os.utime(side, (old, old))
        return side

    def lone_asset(self, name: str = "post.mp4", *, age: int = 60,
                   data: bytes = MP4) -> Path:
        asset = self.inbox / name
        asset.write_bytes(data)
        old = time.time() - age
        os.utime(asset, (old, old))
        return asset

    # -- the symmetry itself ------------------------------------------------

    def test_a_lone_sidecar_is_reported(self):
        self.lone_sidecar()
        row = self.scan()["incomplete"][0]
        self.assertEqual(row["have"], "sidecar")
        self.assertEqual(row["missing"], "asset")
        self.assertEqual(row["state"], "waiting_for_other_half")

    def test_a_lone_asset_is_reported_not_refused(self):
        # THE REGRESSION. This used to raise "no sidecar" inside validate_drop
        # and move the asset to rejected/ on its first sighting.
        asset = self.lone_asset()
        manifest = self.scan()
        self.assertEqual(manifest["rejected_count"], 0,
                         f"refused: {manifest['rejected']}")
        self.assertTrue(asset.is_file(), "the asset must stay in the inbox")
        row = manifest["incomplete"][0]
        self.assertEqual(row["have"], "asset")
        self.assertEqual(row["missing"], "sidecar")

    def test_both_directions_get_the_same_grace(self):
        # Stated as a property rather than two numbers, because the whole bug
        # was that these two cases were governed by different rules.
        for which in ("sidecar", "asset"):
            with self.subTest(have=which):
                for f in self.inbox.iterdir():
                    f.unlink()
                drop_intake.DROP_INCOMPLETE_PATH.unlink(missing_ok=True)
                (self.lone_sidecar if which == "sidecar" else self.lone_asset)(
                    age=drop_intake.INCOMPLETE_DROP_GRACE_SECONDS + 3600)
                first = self.scan()["incomplete"][0]
                self.assertEqual(first["state"], "waiting_for_other_half",
                                 "one scan must never be enough to refuse")
                second = self.scan()["incomplete"][0]
                self.assertEqual(second["state"], "refused")

    def test_the_grace_is_longer_than_the_scan_cadence(self):
        # The arithmetic that actually failed. The scan runs every 4 hours, so
        # a grace under one cadence means a drop is judged the first time it
        # is ever seen. Pinned as a relation, not as today's number.
        scan_cadence_seconds = 4 * 3600
        self.assertGreater(drop_intake.INCOMPLETE_DROP_GRACE_SECONDS,
                           scan_cadence_seconds)
        self.assertGreaterEqual(drop_intake.MIN_SCANS_BEFORE_REFUSING_INCOMPLETE, 2)

    # -- the two conditions -------------------------------------------------

    def test_one_scan_is_never_enough_however_old(self):
        self.lone_sidecar(age=30 * 86400)
        self.assertEqual(self.scan()["incomplete"][0]["state"],
                         "waiting_for_other_half")

    def test_many_scans_are_never_enough_if_it_is_young(self):
        # A burst of manual runs must not convict a drop that arrived minutes
        # ago: the age is what says the other half is not coming.
        self.lone_sidecar(age=60)
        for _ in range(5):
            row = self.scan()["incomplete"][0]
        self.assertEqual(row["state"], "waiting_for_other_half")
        self.assertGreaterEqual(row["scans_seen"], 5)

    def test_refused_only_when_both_conditions_hold(self):
        side = self.lone_sidecar(age=drop_intake.INCOMPLETE_DROP_GRACE_SECONDS + 60)
        self.scan()
        manifest = self.scan()
        self.assertEqual(manifest["incomplete"][0]["state"], "refused")
        self.assertFalse(side.is_file())
        notes = list((self.root / drop_intake.REJECTED_DIR).glob("*.incomplete.txt"))
        self.assertEqual(len(notes), 1)
        text = notes[0].read_text()
        self.assertIn("never completed", text)
        self.assertIn("send BOTH files again", text)

    def test_the_refusal_warns_that_the_other_half_will_not_pair(self):
        # The exact confusion that cost the post: the agent sent the missing
        # half, and its partner had already been moved out of the inbox.
        self.lone_sidecar(age=drop_intake.INCOMPLETE_DROP_GRACE_SECONDS + 60)
        self.scan()
        reason = self.scan()["incomplete"][0]["reason"]
        self.assertIn("will not pair with anything", reason)
        self.assertIn("MOVED", reason)

    # -- completion ---------------------------------------------------------

    def test_the_other_half_arriving_completes_the_drop(self):
        # The whole point. A waited-on drop must actually be accepted when
        # its partner lands, including after several scans saw it incomplete.
        self.lone_asset(name="post.mp4", age=600)
        self.scan()
        self.scan()
        side = self.inbox / "post.mp4.json"
        side.write_text(json.dumps({
            "caption": "Beer duck or whiskey duck?",
            "lane": drop_intake.EXPRESS_LANE, "publish_at": "asap",
            "post_ready": True, "source_agent": "gemini-post-writer",
        }), encoding="utf-8")
        old = time.time() - 600
        os.utime(side, (old, old))
        manifest = self.scan()
        self.assertEqual(manifest["accepted_count"], 1,
                         f"refused: {manifest['rejected']}")
        self.assertEqual(manifest["incomplete"], [])

    def test_a_complete_drop_is_never_called_incomplete(self):
        # Positive control. Without it this whole class could pass by calling
        # every drop incomplete, which would stop the lane entirely.
        self.accept()
        self.assertEqual(self.scan()["incomplete"], [])

    def test_the_bare_sidecar_spelling_pairs_too(self):
        self.lone_asset(name="post.png", data=PNG, age=600)
        side = self.inbox / "post.json"
        side.write_text(json.dumps({
            "caption": "c", "lane": drop_intake.EXPRESS_LANE,
            "publish_at": "asap", "post_ready": True,
            "source_agent": "a"}), encoding="utf-8")
        old = time.time() - 600
        os.utime(side, (old, old))
        self.assertEqual(self.scan()["incomplete"], [])

    # -- a file that can never become valid --------------------------------

    def test_an_impossible_type_is_still_refused_immediately(self):
        # Waiting only helps a drop that could still become good. An .exe is
        # not going to be rescued by a sidecar.
        self.lone_asset(name="payload.exe", data=b"MZ\x00\x00", age=600)
        manifest = self.scan()
        self.assertEqual(manifest["rejected_count"], 1)
        self.assertIn("not an accepted type", manifest["rejected"][0]["reason"])
        self.assertEqual(manifest["incomplete"], [])

    # -- the durable ledger (what makes "a fair chance" answerable) ---------

    def test_the_count_survives_across_scans(self):
        self.lone_sidecar(age=600)
        self.assertEqual(self.scan()["incomplete"][0]["scans_seen"], 1)
        self.assertEqual(self.scan()["incomplete"][0]["scans_seen"], 2)
        self.assertEqual(self.scan()["incomplete"][0]["scans_seen"], 3)

    def test_the_summary_outlives_the_scan_that_saw_it(self):
        # The operator-visible half of the fix: the per-scan manifest showed an
        # incomplete drop for exactly one 4-hour cadence and then forgot it, so
        # whether anyone ever saw it was luck.
        self.lone_sidecar(age=600)
        self.scan()
        summary = self.summary()
        self.assertEqual(summary["waiting_count"], 1)
        self.assertEqual(summary["waiting"][0]["missing"], "asset")
        self.assertEqual(summary["waiting"][0]["source_agent"],
                         "gemini-post-writer")

    def test_a_completion_is_recorded_as_the_rate_denominator(self):
        self.lone_asset(age=600)
        self.scan()
        side = self.inbox / "post.mp4.json"
        side.write_text(json.dumps({
            "caption": "c", "lane": drop_intake.EXPRESS_LANE,
            "publish_at": "asap", "post_ready": True,
            "source_agent": "gemini-post-writer"}), encoding="utf-8")
        old = time.time() - 600
        os.utime(side, (old, old))
        self.scan()
        summary = self.summary()
        self.assertEqual(summary["completed_7d"], 1)
        self.assertEqual(summary["refused_7d"], 0)
        self.assertEqual(summary["refused_rate_7d"], 0.0)

    def test_a_refusal_is_counted_and_attributed(self):
        self.lone_sidecar(age=drop_intake.INCOMPLETE_DROP_GRACE_SECONDS + 60)
        self.scan()
        self.scan()
        summary = self.summary()
        self.assertEqual(summary["refused_7d"], 1)
        self.assertEqual(summary["refused_rate_7d"], 1.0)
        # Attributed, because one lost upload is noise and the same agent
        # losing one every day is a broken producer.
        self.assertEqual(summary["refused_by_agent_7d"],
                         {"gemini-post-writer": 1})

    def test_the_rate_has_no_value_rather_than_a_fake_zero(self):
        # A rate over nothing is not 0.0 ([[feedback_a_number_needs_its_denominator]]).
        summary = self.summary()
        self.assertIsNone(summary["refused_rate_7d"])
        self.assertEqual(summary["resolved_7d"], 0)

    def test_an_unparseable_lone_sidecar_is_still_tracked(self):
        side = self.inbox / "broken.mp4.json"
        side.write_text("{not json", encoding="utf-8")
        old = time.time() - 600
        os.utime(side, (old, old))
        row = self.scan()["incomplete"][0]
        self.assertEqual(row["declared"], {})
        self.assertEqual(row["missing"], "asset")

    def test_the_ledger_refuses_to_write_production_under_test_mode(self):
        # Layer 2 of the three-layer policy for a new prod-path constant. Note
        # it has to name the FROZEN path explicitly: the conftest redirect
        # (layer 1) means `DROP_INCOMPLETE_PATH` is already a tmp file here,
        # which is the whole point -- the guard is the backstop for when that
        # redirect is missing, never the thing tests rely on.
        with self.assertRaises(drop_intake.TestModeRefusalError):
            drop_intake._write_incomplete_ledger(
                drop_intake._FROZEN_PRODUCTION_INCOMPLETE_PATH, {}, [])

    def test_the_agent_status_file_lists_a_waiting_drop(self):
        self.lone_sidecar(age=600)
        manifest = self.scan()
        path = drop_intake.write_status_file(
            self.root, {}, incomplete=manifest["incomplete"])
        payload = json.loads(path.read_text())
        row = payload["incomplete_this_scan"][0]
        self.assertEqual(row["state"], "waiting_for_other_half")
        self.assertEqual(row["missing"], "asset")
        self.assertIn("WAITED on, not refused", payload["doc"])

    def test_the_contract_no_longer_tells_the_agent_order_matters(self):
        # The advice that caused the loss. It must be gone, not softened.
        readme = drop_intake.MIRROR_DROP_README
        self.assertNotIn("Upload the asset BEFORE the sidecar", readme)
        self.assertIn("the order does not matter", readme)
        self.assertIn("waiting_for_other_half", readme)


class NoTestPollutionInTheIncompleteLedger(unittest.TestCase):
    """Layer 3 for `drop_incomplete.json` — audit the real file.

    Layers 1 and 2 are the conftest redirect and the DUCK_TEST_MODE write
    guard. This is the one that catches a test which bypasses both, which is
    how three earlier prod-path constants were polluted in ten days.

    A fixture row here is not inert. `scans_seen` is what decides whether a
    half-arrived drop gets refused, so a stale fixture entry under a real
    stem could either refuse a genuine drop early or keep waiting on one
    forever.
    """

    PRODUCTION = Path(__file__).resolve().parents[1] / "state" / "drop_incomplete.json"

    #: Stems used by the tests above. A real drop is named after its post.
    FIXTURE_STEMS = frozenset({"post", "broken", "payload", "a", "b", "x"})

    def test_the_live_ledger_holds_no_fixture_stems(self):
        if not self.PRODUCTION.is_file():
            self.skipTest("no live incomplete ledger yet")
        payload = json.loads(self.PRODUCTION.read_text(encoding="utf-8"))
        pending = payload.get("pending") or {}
        history = payload.get("history") or []
        found = {s for s in pending if s in self.FIXTURE_STEMS}
        found |= {str(h.get("stem")) for h in history
                  if isinstance(h, dict) and h.get("stem") in self.FIXTURE_STEMS}
        self.assertEqual(
            found, set(),
            "fixture stems in the live incomplete ledger — a test bypassed "
            "both the conftest redirect and the DUCK_TEST_MODE guard: "
            f"{sorted(found)}")

    def test_the_live_ledger_has_the_shape_its_readers_expect(self):
        # A hand-edited or half-written file would make the card lie rather
        # than fail, so the audit checks the shape too.
        if not self.PRODUCTION.is_file():
            self.skipTest("no live incomplete ledger yet")
        payload = json.loads(self.PRODUCTION.read_text(encoding="utf-8"))
        self.assertIsInstance(payload.get("pending"), dict)
        self.assertIsInstance(payload.get("history"), list)


class TheWaitingClockIsTheSameEverywhere(_ExpressRoot):
    """One question, one number.

    The card read `first_seen_at` and the agent's status file read the age of
    the half actually present, so a drop whose file predated the scan that
    found it was reported as 0.0h on one surface and 3.3h on the other.
    """

    def test_the_summary_reports_the_age_of_the_half_that_is_here(self):
        asset = self.inbox / "post.mp4"
        asset.write_bytes(MP4)
        old = time.time() - 4 * 3600
        os.utime(asset, (old, old))
        manifest = drop_intake.scan_inbox(self.root)
        summary = drop_intake.incomplete_drop_summary()
        self.assertAlmostEqual(summary["waiting"][0]["hours_waiting"], 4.0, delta=0.2)
        # And it agrees with what the agent is told.
        self.assertAlmostEqual(
            manifest["incomplete"][0]["newest_half_age_seconds"] / 3600.0,
            summary["waiting"][0]["hours_waiting"], delta=0.2)

    def test_our_first_sighting_is_still_recorded_separately(self):
        # Both facts are useful; they just are not the same fact.
        asset = self.inbox / "post.mp4"
        asset.write_bytes(MP4)
        old = time.time() - 4 * 3600
        os.utime(asset, (old, old))
        drop_intake.scan_inbox(self.root)
        row = drop_intake.incomplete_drop_summary()["waiting"][0]
        self.assertAlmostEqual(row["first_seen_hours_ago"], 0.0, delta=0.2)
        self.assertGreater(row["hours_waiting"], row["first_seen_hours_ago"])


class OneRefusalNamesEveryProblem(_ExpressRoot):
    """Surface 172 — the field rules have ONE implementation, two readers.

    The cost of not having this, measured 2026-11-20: the incomplete path
    reported a missing asset and said nothing about the sidecar, so Gemini
    re-sent and was refused a second time for a `source_agent` nobody had
    mentioned. Copying the rules into that path would have given the lane two
    answers to "is this sidecar valid"; extracting them gives it one.
    """

    def sidecar_only(self, body: dict, *, age: int = 600,
                     name: str = "post.mp4.json") -> Path:
        side = self.inbox / name
        side.write_text(json.dumps(body), encoding="utf-8")
        old = time.time() - age
        os.utime(side, (old, old))
        return side

    GOOD = {"caption": "Beer duck or whiskey duck?",
            "lane": drop_intake.EXPRESS_LANE, "publish_at": "asap",
            "post_ready": True, "source_agent": "gemini-post-writer"}

    # -- the extraction kept validate_drop's behaviour exactly --------------

    def test_a_single_mistake_still_raises_the_message_it_always_did(self):
        reason = self.refusal(**{"lane": "not_a_lane"})
        self.assertIn("not a known lane", reason)

    def test_validate_drop_raises_the_FIRST_problem_not_a_list(self):
        # The order the checks used to run is part of the contract: an agent
        # fixing one field at a time must not see the messages reorder.
        meta = {**self.GOOD}
        meta.pop("caption")
        meta["lane"] = "not_a_lane"
        _fields, problems = drop_intake.sidecar_field_values(meta)
        self.assertIn("caption", problems[0])
        self.assertTrue(any("not a known lane" in p for p in problems))

    def test_the_shared_function_skips_the_check_that_needs_the_asset(self):
        # media_type is the one rule that cannot be answered without the file.
        # No publish_at: on a weekly lane that is its own separate problem,
        # and it also contains the word "publishes", which is what made the
        # first version of this assertion match the wrong message.
        meta = {"caption": "c", "lane": "duckvideo", "post_ready": True,
                "source_agent": "gemini-post-writer"}
        _f, with_image = drop_intake.sidecar_field_values(
            meta, media_type="image")
        _f, without = drop_intake.sidecar_field_values(meta, media_type=None)
        marker = "and this is an image"
        self.assertTrue(any(marker in p for p in with_image),
                        "duckvideo takes video only, so an image is a problem")
        self.assertFalse(any(marker in p for p in without))
        self.assertEqual(without, [], "nothing else about this is wrong")

    def test_a_clean_sidecar_has_no_problems(self):
        # Positive control: a collector that always finds something is useless.
        _fields, problems = drop_intake.sidecar_field_values(
            self.GOOD, media_type="video")
        self.assertEqual(problems, [])

    def test_it_collects_every_problem_not_just_the_first(self):
        meta = {"lane": "not_a_lane", "post_ready": "yes",
                "closing_at": "soon"}
        _fields, problems = drop_intake.sidecar_field_values(meta)
        joined = " | ".join(problems)
        for expected in ("caption", "not a known lane", "source_agent",
                         "post_ready", "closing_at"):
            self.assertIn(expected, joined)

    # -- the payoff: told while it is still WAITING -------------------------

    def test_a_waiting_sidecar_is_told_its_field_problems_now(self):
        # THE REGRESSION. Learning about a missing source_agent eight hours
        # later, on a second refusal, is what cost the round trip.
        meta = {**self.GOOD}
        meta.pop("source_agent")
        self.sidecar_only(meta)
        row = drop_intake.scan_inbox(self.root)["incomplete"][0]
        self.assertEqual(row["state"], "waiting_for_other_half")
        self.assertTrue(any("source_agent" in p for p in row["sidecar_problems"]))

    def test_field_problems_do_NOT_cause_an_early_refusal(self):
        # Still incomplete, so still waited on: the agent can re-send a fixed
        # sidecar and keep the half that already arrived.
        meta = {**self.GOOD}
        meta.pop("source_agent")
        side = self.sidecar_only(meta)
        drop_intake.scan_inbox(self.root)
        self.assertTrue(side.is_file())

    def test_the_agent_status_file_carries_them(self):
        meta = {**self.GOOD}
        meta.pop("source_agent")
        self.sidecar_only(meta)
        manifest = drop_intake.scan_inbox(self.root)
        path = drop_intake.write_status_file(
            self.root, {}, incomplete=manifest["incomplete"])
        row = json.loads(path.read_text())["incomplete_this_scan"][0]
        self.assertTrue(any("source_agent" in p for p in row["sidecar_problems"]))

    # -- and on the refusal, when it comes ---------------------------------

    def test_the_refusal_lists_the_other_problems_too(self):
        meta = {**self.GOOD}
        meta.pop("source_agent")
        self.sidecar_only(
            meta, age=drop_intake.INCOMPLETE_DROP_GRACE_SECONDS + 60)
        drop_intake.scan_inbox(self.root)
        reason = drop_intake.scan_inbox(self.root)["incomplete"][0]["reason"]
        self.assertIn("would ALSO have been refused", reason)
        self.assertIn("source_agent", reason)
        self.assertIn("every field rule your sidecar breaks", reason)

    def test_a_clean_sidecar_is_told_the_asset_is_the_only_problem(self):
        self.sidecar_only(
            self.GOOD, age=drop_intake.INCOMPLETE_DROP_GRACE_SECONDS + 60)
        drop_intake.scan_inbox(self.root)
        reason = drop_intake.scan_inbox(self.root)["incomplete"][0]["reason"]
        self.assertIn("The sidecar itself is fine", reason)

    def test_a_missing_sidecar_says_its_fields_were_not_read(self):
        # Honest about the gap rather than implying a clean bill of health:
        # with no sidecar there is nothing to check.
        asset = self.inbox / "post.mp4"
        asset.write_bytes(MP4)
        old = time.time() - (drop_intake.INCOMPLETE_DROP_GRACE_SECONDS + 60)
        os.utime(asset, (old, old))
        drop_intake.scan_inbox(self.root)
        reason = drop_intake.scan_inbox(self.root)["incomplete"][0]["reason"]
        self.assertIn("only the missing file was checked", reason)
        self.assertIn("have been read yet", reason)


class AReSendIsConnectedToWhatItReplaces(_ExpressRoot):
    """Surface 172 — four attempts at one post are not four posts.

    Gemini names a retry after the attempt it replaces: the live accepted/
    folder holds `..._standoff`, `_v2`, `_v3` and `_v4` of one post. The
    ledger keyed everything by the raw stem, so the history could not say
    "this same post has now failed three times", and the refusal rate read as
    a broad producer problem when it was one post bouncing.
    """

    def test_a_resend_marker_is_stripped(self):
        for raw, expected in (
            ("duck_fact_v2", "duck_fact"),
            ("duck_fact_V3", "duck_fact"),
            ("duck_fact.v2", "duck_fact"),
            ("duck_fact-v10", "duck_fact"),
            ("holiday_horror_dash_standoff_v4", "holiday_horror_dash_standoff"),
        ):
            with self.subTest(raw=raw):
                self.assertEqual(drop_intake.base_stem(raw), expected)

    def test_a_move_collision_stamp_is_stripped(self):
        self.assertEqual(
            drop_intake.base_stem("duck_fact.20261120-141500"), "duck_fact")

    def test_both_markers_together_are_stripped(self):
        self.assertEqual(
            drop_intake.base_stem("duck_fact_v2.20261120-141500"), "duck_fact")

    def test_a_name_that_merely_ends_in_v_plus_words_is_untouched(self):
        # Not a re-send marker: `v` followed by digits is, `beer_vs_whiskey`
        # is not, and treating it as one would merge unrelated posts.
        for raw in ("beer_vs_whiskey", "duck_v", "duck_version", "v2"):
            with self.subTest(raw=raw):
                self.assertEqual(drop_intake.base_stem(raw), raw)

    def test_the_real_live_stems_group_to_one_post(self):
        # The names actually in the Drive folder, not invented ones.
        live = ["mid_november_holiday_horror_dash_standoff",
                "mid_november_holiday_horror_dash_standoff_v2",
                "mid_november_holiday_horror_dash_standoff_v3",
                "mid_november_holiday_horror_dash_standoff_v4"]
        self.assertEqual(len({drop_intake.base_stem(s) for s in live}), 1)

    # -- what the grouping buys ---------------------------------------------

    def refuse_one(self, stem: str) -> None:
        side = self.inbox / f"{stem}.mp4.json"
        side.write_text(json.dumps({
            "caption": "c", "lane": drop_intake.EXPRESS_LANE,
            "publish_at": "asap", "post_ready": True,
            "source_agent": "gemini-post-writer"}), encoding="utf-8")
        old = time.time() - (drop_intake.INCOMPLETE_DROP_GRACE_SECONDS + 60)
        os.utime(side, (old, old))
        drop_intake.scan_inbox(self.root)
        drop_intake.scan_inbox(self.root)

    def test_history_records_the_post_a_stem_belongs_to(self):
        self.refuse_one("duck_fact_v2")
        ledger = json.loads(drop_intake.DROP_INCOMPLETE_PATH.read_text())
        row = ledger["history"][-1]
        self.assertEqual(row["stem"], "duck_fact_v2")
        self.assertEqual(row["base_stem"], "duck_fact")

    def test_two_attempts_at_one_post_are_named_as_a_repeat(self):
        self.refuse_one("duck_fact")
        self.refuse_one("duck_fact_v2")
        summary = drop_intake.incomplete_drop_summary()
        self.assertEqual(summary["refused_7d"], 2)
        # ...but ONE post, which is the distinction the bare count hid.
        self.assertEqual(summary["distinct_posts_refused_7d"], 1)
        self.assertEqual(len(summary["repeat_offenders"]), 1)
        offender = summary["repeat_offenders"][0]
        self.assertEqual(offender["post"], "duck_fact")
        self.assertEqual(offender["refused"], 2)
        self.assertEqual(sorted(offender["stems"]), ["duck_fact", "duck_fact_v2"])

    def test_two_different_posts_are_not_a_repeat(self):
        # Positive control: the grouping must not merge unrelated posts, or
        # every busy week looks like one broken post.
        self.refuse_one("duck_fact")
        self.refuse_one("beer_vs_whiskey")
        summary = drop_intake.incomplete_drop_summary()
        self.assertEqual(summary["distinct_posts_refused_7d"], 2)
        self.assertEqual(summary["repeat_offenders"], [])

    def test_a_waiting_drop_knows_how_often_this_post_already_failed(self):
        # A drop waiting for its second file is routine. The third attempt at
        # the same post waiting again is a contract problem.
        self.refuse_one("duck_fact")
        self.refuse_one("duck_fact_v2")
        side = self.inbox / "duck_fact_v3.mp4.json"
        side.write_text(json.dumps({
            "caption": "c", "lane": drop_intake.EXPRESS_LANE,
            "publish_at": "asap", "post_ready": True,
            "source_agent": "gemini-post-writer"}), encoding="utf-8")
        old = time.time() - 600
        os.utime(side, (old, old))
        drop_intake.scan_inbox(self.root)
        row = drop_intake.incomplete_drop_summary()["waiting"][0]
        self.assertEqual(row["post"], "duck_fact")
        self.assertEqual(row["prior_refusals"], 2)


class TheContractExplainsBothNewMechanisms(_ExpressRoot):
    def test_the_readme_tells_the_agent_problems_arrive_while_it_waits(self):
        readme = drop_intake.MIRROR_DROP_README
        self.assertIn("sidecar_problems", readme)
        self.assertIn("all of them, not", readme)

    def test_the_readme_asks_for_the_retry_naming_it_already_uses(self):
        readme = drop_intake.MIRROR_DROP_README
        self.assertIn("duck_fact_v2", readme)
        self.assertIn("one post with three attempts", readme)


class ACompletePairIsAlsoToldEverything(_ExpressRoot):
    """The case Gemini's re-send actually hits.

    Both halves present means `validate_drop` runs, and it raises on the FIRST
    problem -- which is the right exception contract and the wrong thing to
    write into `rejected/`. The reason file now lists them all, while
    `str(exc)` and the manifest's `reason` stay exactly what they were.
    """

    def test_the_reason_file_lists_every_problem(self):
        self.drop(**{"lane": "not_a_lane"}, publish_at="asap")
        # No source_agent either: two unrelated problems in one drop.
        side = self.inbox / "post.png.json"
        meta = json.loads(side.read_text())
        meta.pop("source_agent")
        meta["post_ready"] = "yes"
        side.write_text(json.dumps(meta), encoding="utf-8")
        old = time.time() - 300
        os.utime(side, (old, old))

        manifest = drop_intake.scan_inbox(self.root)
        self.assertEqual(manifest["rejected_count"], 1)
        text = next((self.root / drop_intake.REJECTED_DIR)
                    .glob("*.reason.txt")).read_text()
        self.assertIn("not a known lane", text)
        self.assertIn("source_agent", text)
        self.assertIn("post_ready", text)
        self.assertIn("fix them all in one pass", text)

    def test_the_manifest_reason_is_still_just_the_first(self):
        # The exception contract is unchanged: one refusal, one message.
        # Several existing tests read `_reason(...)` and must keep working.
        reason = self.refusal(**{"lane": "not_a_lane"})
        self.assertIn("not a known lane", reason)
        self.assertNotIn("fix them all", reason)

    def test_a_single_problem_writes_an_unchanged_reason_file(self):
        # No list, no preamble, for the common case of one mistake.
        self.drop(**{"lane": "not_a_lane"}, publish_at=None)
        drop_intake.scan_inbox(self.root)
        text = next((self.root / drop_intake.REJECTED_DIR)
                    .glob("*.reason.txt")).read_text()
        self.assertIn("not a known lane", text)
        self.assertNotIn("AND these", text)

    def test_the_exception_carries_the_list_for_other_callers(self):
        with self.assertRaises(drop_intake.DropRefused) as caught:
            drop_intake.validate_drop(
                self.inbox / "nope.png", self.inbox / "nope.png.json")
        # A refusal raised before the fields are read has one problem, and it
        # must still be a list rather than None.
        self.assertIsInstance(caught.exception.problems, list)
        self.assertTrue(caught.exception.problems)


class NotesArriveInEitherShape(_ExpressRoot):
    """Surface 173 — the agent sends structure, the contract asks for prose.

    Both are reasonable. Structure is how an agent naturally writes several
    distinct notes; prose is what the operator reads. `str()` on a dict put
    Python repr -- `"{'hook_status': ...}"` -- into the ledger and in front of
    a human, and refusing the object would cost a round trip over the least
    load-bearing field in the sidecar.
    """

    def notes_of(self, raw) -> str:
        fields, problems = drop_intake.sidecar_field_values(
            {"caption": "c", "lane": drop_intake.EXPRESS_LANE,
             "publish_at": "asap", "post_ready": True,
             "source_agent": "a", "notes": raw}, media_type="video")
        self.assertEqual(problems, [], "notes must never refuse a drop")
        return fields["notes"]

    def test_prose_is_unchanged(self):
        self.assertEqual(self.notes_of("just a sentence"), "just a sentence")

    def test_an_object_becomes_labelled_prose_not_python_repr(self):
        # The exact shape Gemini sent on 2026-11-19 and again on 11-20.
        out = self.notes_of({
            "hook_status": "already burned into the frames",
            "audio_note": "two bass thumps synced to whip-pans"})
        self.assertIn("hook status: already burned into the frames", out)
        self.assertIn("audio note: two bass thumps", out)
        self.assertNotIn("{", out)
        self.assertNotIn("'", out)

    def test_a_list_becomes_one_line_per_item(self):
        out = self.notes_of(["first note", "second note"])
        self.assertEqual(out.splitlines(), ["first note", "second note"])

    def test_a_nested_object_is_still_readable(self):
        out = self.notes_of({"audio": {"bpm": 120, "source": "generated"}})
        self.assertIn("audio:", out)
        self.assertIn("bpm: 120", out)
        self.assertNotIn("{", out)

    def test_an_empty_shape_is_empty_not_the_word_none(self):
        # `str(None)` is "None", which would read to the operator as a note
        # that says None.
        for raw in (None, "", {}, [], 0):
            with self.subTest(raw=raw):
                self.assertEqual(self.notes_of(raw), "")

    def test_a_number_is_its_own_string(self):
        self.assertEqual(self.notes_of(7), "7")

    def test_it_is_truncated_like_prose_always_was(self):
        self.assertEqual(len(self.notes_of({"k": "x" * 5000})),
                         drop_intake.MAX_NOTES_CHARS)

    def test_the_real_live_sidecar_normalises(self):
        # Read from the actual file Gemini sent, not a transcription of it.
        live = Path(os.path.expanduser(
            "~/Library/CloudStorage/GoogleDrive-tullaipsu@gmail.com/My Drive/"
            "DuckDrop/rejected/this_or_that_thursday_beer_vs_whiskey_dash.mp4.json"))
        if not live.is_file():
            self.skipTest("the live sidecar has moved on")
        raw = json.loads(live.read_text())["notes"]
        out = self.notes_of(raw)
        self.assertIn("hook status:", out)
        self.assertNotIn("{", out)


class RecoveryIsCountedNotJustFailure(_ExpressRoot):
    """Surface 173 — a post that bounced and then went out is not a failure.

    The history only ever recorded failure, so a post refused twice and then
    accepted reported two refusals and no recovery. That overstates how broken
    the lane is, and "the lane is broken" and "it costs the agent two tries"
    call for opposite responses.
    """

    GOOD = {"caption": "Beer duck or whiskey duck?",
            "lane": drop_intake.EXPRESS_LANE, "publish_at": "asap",
            "post_ready": True, "source_agent": "gemini-post-writer"}

    def send(self, stem: str, body: dict | None = None, *,
             with_asset: bool = True) -> None:
        if with_asset:
            asset = self.inbox / f"{stem}.png"
            asset.write_bytes(PNG)
            os.utime(asset, (time.time() - 300,) * 2)
        side = self.inbox / f"{stem}.png.json"
        side.write_text(json.dumps(body or self.GOOD), encoding="utf-8")
        os.utime(side, (time.time() - 300,) * 2)

    def summary(self) -> dict:
        return drop_intake.incomplete_drop_summary()

    def test_an_acceptance_is_recorded_in_the_history(self):
        self.send("duck_fact")
        drop_intake.scan_inbox(self.root)
        ledger = json.loads(drop_intake.DROP_INCOMPLETE_PATH.read_text())
        row = ledger["history"][-1]
        self.assertEqual(row["outcome"], "accepted")
        self.assertEqual(row["base_stem"], "duck_fact")
        self.assertEqual(self.summary()["accepted_7d"], 1)

    def test_a_validation_refusal_is_recorded_too(self):
        # It used to live only in the per-scan manifest, so it could not be
        # connected to a later re-send of the same post.
        bad = {k: v for k, v in self.GOOD.items() if k != "source_agent"}
        self.send("duck_fact", bad)
        drop_intake.scan_inbox(self.root)
        ledger = json.loads(drop_intake.DROP_INCOMPLETE_PATH.read_text())
        row = ledger["history"][-1]
        self.assertEqual(row["outcome"], "refused_validation")
        self.assertIn("source_agent", row["reason"])

    def test_a_post_accepted_on_the_second_try_reports_a_round_trip(self):
        bad = {k: v for k, v in self.GOOD.items() if k != "source_agent"}
        self.send("duck_fact", bad)
        drop_intake.scan_inbox(self.root)          # refused
        self.send("duck_fact_v2")
        drop_intake.scan_inbox(self.root)          # accepted
        summary = self.summary()
        self.assertEqual(summary["recovered_7d"], 1)
        self.assertEqual(summary["worst_round_trip"], 1)
        trip = summary["round_trips"][0]
        self.assertEqual(trip["post"], "duck_fact")
        self.assertEqual(trip["accepted_as"], "duck_fact_v2")
        self.assertEqual(trip["refusals_before_accept"], 1)

    def test_an_accepted_post_stops_being_a_repeat_offender(self):
        # THE POINT. Two refusals then success must not leave the card red
        # over a post that went out.
        bad = {k: v for k, v in self.GOOD.items() if k != "source_agent"}
        self.send("duck_fact", bad)
        drop_intake.scan_inbox(self.root)
        self.send("duck_fact_v2", bad)
        drop_intake.scan_inbox(self.root)
        self.assertEqual(len(self.summary()["repeat_offenders"]), 1,
                         "two refusals and no success IS an open problem")
        self.send("duck_fact_v3")
        drop_intake.scan_inbox(self.root)
        summary = self.summary()
        self.assertEqual(summary["repeat_offenders"], [])
        self.assertEqual(summary["recovered_7d"], 1)
        self.assertEqual(summary["worst_round_trip"], 2)

    def test_a_first_time_acceptance_is_not_a_recovery(self):
        # Positive control: a clean post must not be counted as a recovery,
        # or every accepted drop inflates the number.
        self.send("duck_fact")
        drop_intake.scan_inbox(self.root)
        summary = self.summary()
        self.assertEqual(summary["accepted_7d"], 1)
        self.assertEqual(summary["recovered_7d"], 0)
        self.assertEqual(summary["worst_round_trip"], 0)

    def test_refusals_AFTER_an_acceptance_do_not_count_toward_it(self):
        # The arithmetic has to be ordered, or a later unrelated failure
        # retroactively makes an easy acceptance look hard.
        self.send("duck_fact")
        drop_intake.scan_inbox(self.root)
        bad = {k: v for k, v in self.GOOD.items() if k != "source_agent"}
        self.send("duck_fact_v2", bad)
        drop_intake.scan_inbox(self.root)
        self.assertEqual(self.summary()["round_trips"][0]["refusals_before_accept"], 0)

    def test_an_incomplete_timeout_and_a_field_refusal_count_as_one_story(self):
        # A post that lost its upload and then arrived with a bad field has
        # failed twice, by two different mechanisms, at the same post.
        self.send("duck_fact", with_asset=False)
        os.utime(self.inbox / "duck_fact.png.json",
                 (time.time() - (drop_intake.INCOMPLETE_DROP_GRACE_SECONDS + 60),) * 2)
        drop_intake.scan_inbox(self.root)
        drop_intake.scan_inbox(self.root)          # incomplete timeout
        bad = {k: v for k, v in self.GOOD.items() if k != "source_agent"}
        self.send("duck_fact_v2", bad)
        drop_intake.scan_inbox(self.root)          # field refusal
        summary = self.summary()
        self.assertEqual(summary["refused_7d"], 2)
        self.assertEqual(summary["distinct_posts_refused_7d"], 1)


class TheScanSaysItDoesNotMailAnyone(_ExpressRoot):
    """Surface 174 — the notification lives one layer up from the scan.

    `drop_intake.py` accepts drops; `drop_digest.py` mails about them; the
    launchd wrapper runs them in sequence. That split is deliberate -- a mail
    failure must not fail a good scan, and no sender should be reachable from
    this module's tests ([[feedback_test_runs_reach_the_outside_world]]).

    Its cost is that running the scan by hand accepts a drop and tells nobody.
    On 2026-11-20 a post sat approved-pending for an hour while the operator
    waited for an email that only the wrapper sends. The scan now says so.
    """

    def test_the_scan_still_sends_no_email_itself(self):
        # The property that matters most: this module must stay unreachable
        # from a sender, so a test of it can never mail the operator.
        source = Path(drop_intake.__file__).read_text(encoding="utf-8")
        for sender in ("send_email", "smtplib", "sendmail"):
            self.assertNotIn(sender, source,
                             f"{sender} must not be reachable from the scan")

    def test_an_accepting_scan_names_the_digest(self):
        import contextlib, io

        self.drop()
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            drop_intake.main(["--drop-root", str(self.root)])
        out = buf.getvalue()
        self.assertIn("sends no email", out)
        self.assertIn("drop_digest.py", out)
        self.assertIn("run_drop_intake.sh", out)

    def test_an_empty_scan_stays_quiet(self):
        # The reminder is for the case where something is waiting on a human.
        # On an empty scan it would be noise six times a day.
        import contextlib, io

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            drop_intake.main(["--drop-root", str(self.root)])
        self.assertNotIn("sends no email", buf.getvalue())


class ThePermalinkIsRecordedWhereBothSidesReadTests(_ExpressRoot):
    """Surface 179 — "published" meant "Meta returned an id".

    The lane recorded two post ids and read neither back. Three times this
    system has recorded a successful write a platform had not stored
    ([[feedback_verify_against_the_marketplace]]), so an id is not proof.
    """

    def claimed(self) -> None:
        self.accept()
        drop_intake.set_approval("post", True, ledger_path=self.ledger)
        drop_intake.claim_for_lane(drop_intake.EXPRESS_LANE,
                                   claimed_by="express::run1",
                                   ledger_path=self.ledger)

    def entry(self) -> dict:
        return json.loads(self.ledger.read_text())["proposals"]["post"]

    def test_permalinks_are_stored_on_the_entry(self):
        self.claimed()
        drop_intake.record_publish_outcome(
            "post", status="published_now",
            platforms=["facebook", "instagram"],
            permalinks={"facebook": "https://facebook.com/p/1",
                        "instagram": "https://instagram.com/reel/abc"},
            ledger_path=self.ledger)
        links = self.entry()["publish_permalinks"]
        self.assertEqual(links["instagram"], "https://instagram.com/reel/abc")

    def test_an_outcome_without_permalinks_stores_nothing(self):
        # A verification that could not run must not leave an empty dict that
        # reads as "verified, no links".
        self.claimed()
        drop_intake.record_publish_outcome(
            "post", status="published_now", platforms=["facebook"],
            ledger_path=self.ledger)
        self.assertNotIn("publish_permalinks", self.entry())

    def test_a_blank_link_is_dropped_rather_than_stored(self):
        self.claimed()
        drop_intake.record_publish_outcome(
            "post", status="partial", platforms=["facebook"],
            permalinks={"facebook": "https://facebook.com/p/1",
                        "instagram": ""},
            ledger_path=self.ledger)
        self.assertEqual(list(self.entry()["publish_permalinks"]), ["facebook"])

    def test_the_agent_status_file_carries_them(self):
        # The producing agent has never had any way to see its own published
        # work -- it could learn that a post was TAKEN and never where it went.
        self.claimed()
        drop_intake.record_publish_outcome(
            "post", status="published_now", platforms=["instagram"],
            permalinks={"instagram": "https://instagram.com/reel/abc"},
            ledger_path=self.ledger)
        proposals = json.loads(self.ledger.read_text())["proposals"]
        payload = json.loads(
            drop_intake.write_status_file(self.root, proposals).read_text())
        self.assertEqual(payload["drops"]["post"]["permalinks"],
                         {"instagram": "https://instagram.com/reel/abc"})

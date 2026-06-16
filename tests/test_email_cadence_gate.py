"""Contract tests for the shared email cadence gate.

Five operator-facing intel surfaces (profit, recommendations,
reviews, learnings, competitors) consult email_cadence_gate.
should_send_email before firing their daily email. These tests pin
the four cadence paths (daily / weekly_monday / weekly_monday +
bypass / manual) and the receipt-log shape so a regression here can't
silently start spamming the operator's inbox again.
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

_RUNTIME = Path(__file__).resolve().parents[1] / "runtime"
if str(_RUNTIME) not in sys.path:
    sys.path.insert(0, str(_RUNTIME))

from email_cadence_gate import (  # noqa: E402  (sys.path injection above)
    CadencePolicy,
    POLICIES,
    UnknownSurfaceError,
    known_surfaces,
    log_cadence_decision,
    require_policy,
    should_send_email,
)


def _at(date_iso: str) -> datetime:
    """Build a tz-aware noon-local datetime on the given ISO date.
    Cadence decisions are weekday-based; the hour doesn't matter, but
    we keep them aware to match production callers."""
    return datetime.fromisoformat(date_iso + "T12:00:00").replace(tzinfo=timezone.utc)


class PolicyRegistryTests(unittest.TestCase):
    def test_all_surfaces_registered(self) -> None:
        # business_digest added 2026-06-12 (Surface 15.5) — the Monday rollup
        # the other eight fold into when DUCK_EMAIL_DIGEST_MODE=1.
        self.assertEqual(
            known_surfaces(),
            (
                "business_digest",
                "business_intelligence",
                "competitors",
                "engineering_governance",
                "learnings",
                "profit",
                "recommendations",
                "reviews",
                "shopify_seo",
            ),
        )

    def test_require_policy_raises_on_unknown(self) -> None:
        with self.assertRaises(UnknownSurfaceError):
            require_policy("does-not-exist")

    def test_business_intelligence_is_off(self) -> None:
        # 2026-06-16: turned off. It was weekly_monday with a
        # bypass_keys=("action_items_count",) that fired EVERY day (insights
        # always have action items), so the "weekly" email arrived daily.
        from datetime import datetime as _dt
        # Action items present + a Monday: must still NOT send.
        monday = _dt(2026, 6, 15, 8, 0).astimezone()
        dec = should_send_email("business_intelligence", {"action_items_count": 9}, now=monday)
        self.assertFalse(dec.should_send)
        # A regular weekday: also off.
        tuesday = _dt(2026, 6, 16, 4, 0).astimezone()
        self.assertFalse(should_send_email("business_intelligence", {"action_items_count": 9}, now=tuesday).should_send)

    def test_require_policy_is_case_insensitive(self) -> None:
        self.assertIs(require_policy("Profit"), POLICIES["profit"])


class WeeklyMondayCadenceTests(unittest.TestCase):
    """The default cadence after the daily→portal migration."""

    def test_monday_sends(self) -> None:
        # 2026-05-25 is a Monday.
        decision = should_send_email("reviews", {}, now=_at("2026-05-25"))
        self.assertTrue(decision.should_send)
        self.assertEqual(decision.cadence, "weekly_monday")
        self.assertIn("Monday", decision.reason)
        # next_send_iso skips today, returns next Monday.
        self.assertEqual(decision.next_send_iso, "2026-06-01")

    def test_non_monday_defers(self) -> None:
        # 2026-05-26 is a Tuesday.
        decision = should_send_email("reviews", {}, now=_at("2026-05-26"))
        self.assertFalse(decision.should_send)
        self.assertEqual(decision.next_send_iso, "2026-06-01")
        # Reason carries the surface's deferred_note for the receipt log.
        self.assertIn("Daily reviews snapshot", decision.reason)


class BypassConditionTests(unittest.TestCase):
    """Each weekly_monday surface has an escape hatch for urgent
    same-day situations: an anomaly, a low-rating review, a new build
    candidate. These are the path the operator actually needs."""

    def test_low_rating_bypass_on_reviews(self) -> None:
        decision = should_send_email(
            "reviews",
            {"low_rating_count": 2},
            now=_at("2026-05-26"),  # Tuesday
        )
        self.assertTrue(decision.should_send)
        self.assertTrue(decision.bypass_active)
        self.assertEqual(decision.bypass_keys_matched, ("low_rating_count",))
        self.assertIn("low_rating_count", decision.reason)

    def test_zero_low_rating_does_not_bypass(self) -> None:
        decision = should_send_email(
            "reviews",
            {"low_rating_count": 0},
            now=_at("2026-05-26"),
        )
        self.assertFalse(decision.should_send)
        self.assertFalse(decision.bypass_active)

    def test_dotted_path_bypass_resolves_nested(self) -> None:
        # Profit's bypass is "anomaly.triggered" — nested.
        decision = should_send_email(
            "profit",
            {"anomaly": {"triggered": True, "reasons": ["margin drop"]}},
            now=_at("2026-05-26"),
        )
        self.assertTrue(decision.should_send)
        self.assertEqual(decision.bypass_keys_matched, ("anomaly.triggered",))

    def test_missing_nested_segment_does_not_crash(self) -> None:
        # No "anomaly" key at all — bypass-check must shrug, not throw.
        decision = should_send_email(
            "profit",
            {"summary": "ok"},
            now=_at("2026-05-26"),
        )
        self.assertFalse(decision.should_send)
        self.assertFalse(decision.bypass_active)

    def test_competitors_ducks_to_build_bypass(self) -> None:
        decision = should_send_email(
            "competitors",
            {"ducks_to_build_count": 3},
            now=_at("2026-05-26"),
        )
        self.assertTrue(decision.should_send)
        self.assertEqual(decision.bypass_keys_matched, ("ducks_to_build_count",))

    def test_learnings_attention_change_bypass(self) -> None:
        decision = should_send_email(
            "learnings",
            {"change_notifier": {"attention_change_count": 1}},
            now=_at("2026-05-26"),
        )
        self.assertTrue(decision.should_send)

    def test_monday_with_no_bypass_still_sends(self) -> None:
        decision = should_send_email("profit", {}, now=_at("2026-05-25"))
        self.assertTrue(decision.should_send)
        self.assertFalse(decision.bypass_active)


class DailyAndManualCadenceTests(unittest.TestCase):
    """The policy registry can be re-declared mid-test to exercise the
    daily and manual paths without bolting a fake surface onto the
    POLICIES dict (which would leak across tests)."""

    def test_daily_cadence_always_sends(self) -> None:
        policy = CadencePolicy(surface_name="test_daily", cadence="daily")
        POLICIES["test_daily"] = policy
        try:
            decision = should_send_email("test_daily", {}, now=_at("2026-05-27"))
            self.assertTrue(decision.should_send)
            self.assertEqual(decision.cadence, "daily")
        finally:
            POLICIES.pop("test_daily", None)

    def test_manual_cadence_never_sends(self) -> None:
        policy = CadencePolicy(surface_name="test_manual", cadence="manual")
        POLICIES["test_manual"] = policy
        try:
            decision = should_send_email("test_manual", {}, now=_at("2026-05-25"))
            self.assertFalse(decision.should_send)
            self.assertIsNone(decision.next_send_iso)
        finally:
            POLICIES.pop("test_manual", None)


class ReceiptLogTests(unittest.TestCase):
    def test_appends_jsonl_record(self) -> None:
        decision = should_send_email("reviews", {}, now=_at("2026-05-26"))
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "decisions.jsonl"
            log_cadence_decision(decision, log_path=log_path, extra={"run_date": "2026-05-26"})
            log_cadence_decision(decision, log_path=log_path)
            lines = log_path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 2)
        first = json.loads(lines[0])
        self.assertEqual(first["surface"], "reviews")
        self.assertFalse(first["should_send"])
        self.assertEqual(first["cadence"], "weekly_monday")
        self.assertEqual(first["run_date"], "2026-05-26")
        self.assertIn("at", first)

    def test_logging_does_not_raise_on_unwritable_path(self) -> None:
        decision = should_send_email("reviews", {}, now=_at("2026-05-26"))
        # /dev/null is not a directory; this would normally error, but
        # the log call must be best-effort.
        log_cadence_decision(decision, log_path=Path("/dev/null/cannot/exist.jsonl"))


if __name__ == "__main__":
    unittest.main()

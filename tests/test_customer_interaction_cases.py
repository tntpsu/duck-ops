"""Custom-design-case detection must NOT treat the system's own operator
emails (newduck/shopify_seo reviews + replies) or Etsy notifications as
customer custom-design requests. Regression for the 2026-06-15 nightly digest
that surfaced 32 false-positive "Waiting on clarification" items, all of which
were self-generated mail mis-matched on the content term "custom duck"."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

RUNTIME = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import customer_interaction_cases as cic  # noqa: E402

# The exact false positives observed in the nightly digest. Each body contains
# "custom duck" so it WOULD match the content heuristic — the exclusion must
# win on the system-mail / notification markers regardless.
SYSTEM_MAIL_FALSE_POSITIVES = [
    "MJD: [shopify_seo] Missing SEO backfill review | FLOW:shopify_seo | RUN:shopify_seo_missing_20260414_175243 | ACTION:review (What colors should the custom duck use?; Is there a target date or deadline?)",
    "MJD: [newduck] Everything Is Fine Duck | FLOW:newduck | RUN:browser_newduck_20260414_1 | ACTION:review",
    "Re: MJD: [shopify_seo] Missing SEO backfill review | FLOW:shopify_seo | RUN:shopify_seo_missing_20260414_175553 | ACTION:review",
    "You have unread messages! 💌",
]

REAL_CUSTOM_REQUESTS = [
    ("Custom duck request", "Hi! Can you make a custom duck with our team colors for a mascot? Need it by July."),
    ("Personalized duck for a gift", "I would like a personalized duck for my brother's birthday — looking for a custom design."),
]


def test_system_emails_are_not_custom_design_requests():
    for subject in SYSTEM_MAIL_FALSE_POSITIVES:
        # Body deliberately contains the content term that used to false-match.
        assert cic._looks_like_custom_design_request(
            subject, "What colors should the custom duck use?", "ops@myjeepduck.com"
        ) is False, f"system/notification mail wrongly flagged: {subject[:50]}"


def test_real_customer_requests_still_detected():
    for subject, body in REAL_CUSTOM_REQUESTS:
        assert cic._looks_like_custom_design_request(subject, body, "buyer@gmail.com") is True, \
            f"real request missed: {subject}"


def test_build_custom_design_cases_drops_system_mail_keeps_real():
    items = [
        {"uid": f"sys-{i}", "subject": s, "body_text": "custom duck mentioned here", "from": "ops@myjeepduck.com"}
        for i, s in enumerate(SYSTEM_MAIL_FALSE_POSITIVES)
    ] + [
        {"uid": "real-1", "subject": "Custom duck request",
         "body_text": "Can you make a custom duck with team colors for a mascot? Need it by July.",
         "from": "buyer@gmail.com"},
    ]
    cases = cic.build_custom_design_cases(items)
    uids = {c.get("uid") or c.get("source_uid") or c.get("id") for c in cases}
    # Exactly the real one survives; none of the system mails become cases.
    assert len(cases) == 1, f"expected 1 case, got {len(cases)}: {[c.get('summary') for c in cases]}"


def test_marketing_and_shipping_still_excluded():
    # Pre-existing exclusions must still hold (no regression).
    assert cic._looks_like_custom_design_request("You made a sale!", "custom duck", "etsy@etsy.com") is False
    assert cic._looks_like_custom_design_request("Newsletter", "custom duck unsubscribe", "news@x.com") is False


class _Lane:
    """Minimal stand-in for an etsy_message_lane posture, so these tests state
    the posture they describe instead of inheriting the operator's current one."""

    def __init__(self, off: bool):
        self.off = off
        self.reads_enabled = not off
        self.replies_enabled = not off
        self.note = "Grok bot answers these." if off else None
        self.resume_hint = None
        self.known = True


def _queue(message_lane):
    """One conversation needing a reply, plus one refund and one brief, so the
    suppression can be shown to take ONLY the reply."""
    cases = [
        {
            "artifact_id": "customer_case::mail::1",
            "channel": "etsy_conversation",
            "recommended_action": "reply_with_context",
            "customer_summary": "where is my duck",
            "priority": "medium",
        },
        {
            "artifact_id": "customer_case::mail::2",
            "channel": "etsy_conversation",
            "recommended_action": "refund_review",
            "customer_summary": "arrived broken",
            "priority": "high",
        },
    ]
    return cic.build_customer_interaction_queue(cases, [], [], message_lane=message_lane)


class EtsyMessageRepliesLeaveTheQueueWhenTheLaneIsOff(unittest.TestCase):
    """2026-11-19: 363 of 459 operator-facing queue items were
    `reply_with_context` -- a wall of apparently neglected customers that
    nobody on this side can work, because the operator's Grok bot answers Etsy
    messages now. Suppressed and counted, never silently dropped."""

    def test_with_the_lane_on_nothing_is_suppressed(self):
        q = _queue(_Lane(off=False))
        actions = {str(i.get("recommended_action")) for i in q["items"]}
        self.assertIn("reply_with_context", actions)
        self.assertEqual(q["counts"]["etsy_message_replies_suppressed"], 0)
        self.assertIs(q["counts"]["etsy_message_lane_off"], False)
        self.assertIsNone(q["etsy_message_lane_note"])

    def test_with_the_lane_off_only_the_reply_items_go(self):
        q = _queue(_Lane(off=True))
        actions = [str(i.get("recommended_action")) for i in q["items"]]
        self.assertNotIn("reply_with_context", actions)
        self.assertIn("refund_review", actions,
                      "refunds are order-driven and are still ours")
        self.assertEqual(q["counts"]["etsy_message_replies_suppressed"], 1)
        self.assertIs(q["counts"]["etsy_message_lane_off"], True)

    def test_the_suppressed_count_is_never_silent(self):
        q = _queue(_Lane(off=True))
        self.assertEqual(
            q["counts"]["operator_queue_items"] + q["counts"]["etsy_message_replies_suppressed"],
            len(_queue(_Lane(off=False))["items"]),
            "every item is either shown or counted as suppressed")
        self.assertIn("Grok bot answers these", q["etsy_message_lane_note"])

    def test_the_markdown_surface_names_the_suppression_and_the_reason(self):
        md = cic.render_customer_interaction_queue_markdown(_queue(_Lane(off=True)))
        self.assertIn("Etsy message replies suppressed", md)
        self.assertIn("Grok bot answers these", md)

    def test_the_markdown_surface_stays_quiet_when_nothing_is_suppressed(self):
        md = cic.render_customer_interaction_queue_markdown(_queue(_Lane(off=False)))
        self.assertNotIn("suppressed", md)

    def test_a_posture_object_it_does_not_understand_suppresses_nothing(self):
        # Cross-repo / older-provider shape: no `off` attribute. Fail toward
        # SHOWING the work, never toward hiding it.
        class Odd:
            pass

        q = cic.build_customer_interaction_queue(
            [{"artifact_id": "customer_case::mail::1", "channel": "etsy_conversation",
              "recommended_action": "reply_with_context", "customer_summary": "x",
              "priority": "medium"}], [], [], message_lane=Odd())
        self.assertEqual(q["counts"]["etsy_message_replies_suppressed"], 0)
        self.assertEqual(len(q["items"]), 1)

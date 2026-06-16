"""Custom-design-case detection must NOT treat the system's own operator
emails (newduck/shopify_seo reviews + replies) or Etsy notifications as
customer custom-design requests. Regression for the 2026-06-15 nightly digest
that surfaced 32 false-positive "Waiting on clarification" items, all of which
were self-generated mail mis-matched on the content term "custom duck"."""
from __future__ import annotations

import sys
from pathlib import Path

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
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

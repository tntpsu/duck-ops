"""navigate_to_reviews_surface must ALWAYS force-navigate to a real shop reviews
URL and wait for the rows to render — never trust a reused session's current
page. Regression for 2026-07-11: the shared esd session was left on about:blank
by the customer-inbox sync; the old code trusted that page and hunted for a
reviews link on it (about:blank has none), so every locate returned a false
"Exact review row could not be found" and the drain posted 0 — even for rows
sitting on page 1. See TESTS.md Surface 57 fix 0."""
from __future__ import annotations

import re
import sys
from pathlib import Path

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import review_reply_discovery as rrd  # noqa: E402


class _Session:
    """Fakes run_pw_command for a browser session. Tracks navigations and flips
    the snapshot page once a location.assign to a reviews URL happens. `rows`
    controls the sequence of row counts wait_for_review_rows sees."""

    def __init__(self, start_url: str, rows_sequence=(14,)):
        self.current = start_url
        self.assigns: list[str] = []
        self.rows_sequence = list(rows_sequence)
        self._row_calls = 0

    def run(self, session, verb, *args):
        if verb == "snapshot":
            return f"### Page\n- Page URL: {self.current}\n- Page Title: Reviews - myJeepDuck\n"
        if verb == "eval":
            script = " ".join(str(a) for a in args)
            m = re.search(r"location\.assign\(\"([^\"]+)\"\)", script)
            if m:
                self.assigns.append(m.group(1))
                self.current = m.group(1)  # navigation lands
                return "navigating"
            if "data-review-region" in script:  # the wait_for_review_rows poll
                n = self.rows_sequence[min(self._row_calls, len(self.rows_sequence) - 1)]
                self._row_calls += 1
                return f'{{"rows": {n}, "signin": false}}'
            return "null"
        return ""


def _wire(monkeypatch, sess):
    monkeypatch.setattr(rrd, "run_pw_command", sess.run)
    monkeypatch.setattr(rrd.time, "sleep", lambda *_a, **_k: None)


def test_navigates_to_canonical_reviews_from_about_blank(monkeypatch):
    sess = _Session("about:blank", rows_sequence=(14,))
    _wire(monkeypatch, sess)
    result = rrd.navigate_to_reviews_surface("esd")
    # It must have force-navigated to the canonical public reviews page.
    assert rrd.SHOP_PUBLIC_REVIEWS_URL in sess.assigns
    assert result["rows_rendered"] == 14
    assert result["signin_wall"] is False
    assert "/shop/myJeepDuck/reviews" in result["landed_url"]


def test_waits_for_rows_before_returning(monkeypatch):
    # Rows aren't rendered on the first two polls (the exact race that produced
    # the false "row not found"), then appear. It must keep polling, not bail.
    sess = _Session("about:blank", rows_sequence=(0, 0, 14))
    _wire(monkeypatch, sess)
    result = rrd.navigate_to_reviews_surface("esd")
    assert result["rows_rendered"] == 14


def test_already_on_reviews_does_not_renavigate(monkeypatch):
    sess = _Session(rrd.SHOP_PUBLIC_REVIEWS_URL, rows_sequence=(14,))
    _wire(monkeypatch, sess)
    result = rrd.navigate_to_reviews_surface("esd")
    assert sess.assigns == []  # already on target → no location.assign
    assert result["strategy"] == "already_on_shop_reviews_surface"
    assert result["rows_rendered"] == 14


def test_signin_wall_is_surfaced(monkeypatch):
    sess = _Session("about:blank", rows_sequence=(0,))
    # Force the poll to report a sign-in wall instead of rows.
    def run(session, verb, *args):
        if verb == "eval" and "data-review-region" in " ".join(str(a) for a in args):
            return '{"rows": 0, "signin": true}'
        return sess.run(session, verb, *args)
    monkeypatch.setattr(rrd, "run_pw_command", run)
    monkeypatch.setattr(rrd.time, "sleep", lambda *_a, **_k: None)
    result = rrd.navigate_to_reviews_surface("esd")
    assert result["signin_wall"] is True

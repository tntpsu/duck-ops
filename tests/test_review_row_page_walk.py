"""prepare_review_row_for_execution must paginate to find the row carrying the
EXACT expected transaction id, instead of accepting a text-neighbor on page 1.
Regression for 2026-06-15/16: older target reviews sit one page over, the
page-1-only search matched a wrong neighbor, and the guard rejected the post."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import review_reply_executor as rre  # noqa: E402

PAGE1_URL = "https://www.etsy.com/shop/myJeepDuck/reviews?ref=pagination&page=1"
DECISION = {
    "review_target": {"transaction_id": "TARGET", "listing_id": "L1"},
    "preview": {"context_text": "Great duck, loved it!"},
}


def _wire(monkeypatch, locate_returns):
    """Stub the browser externals; return the list of paginated URLs visited."""
    visited: list[str] = []
    monkeypatch.setattr(rre, "ensure_authenticated_session",
                        lambda *a, **k: {"current_url": PAGE1_URL})
    monkeypatch.setattr(rre, "navigate_to_reviews_surface",
                        lambda *a, **k: {"landed_url": PAGE1_URL, "strategy": "shop_reviews", "page_title": "Page 1"})

    def _nav(session, url, wait_seconds=1.0):
        visited.append(url)
        return url, "Page X"
    monkeypatch.setattr(rre, "navigate_within_session", _nav)

    calls = {"n": 0}

    def _locate(*a, **k):
        idx = min(calls["n"], len(locate_returns) - 1)
        calls["n"] += 1
        return dict(locate_returns[idx])
    monkeypatch.setattr(rre, "locate_review_block", _locate)
    monkeypatch.setattr(rre, "click_reply_button", lambda *a, **k: {"ok": True})
    monkeypatch.setattr(rre, "load_execution_policy",
                        lambda: {"review_page_walk_max_pages": 3})
    return visited


def test_paginates_to_exact_transaction_on_page2(monkeypatch):
    wrong = {"found": True, "matchedTransactionId": "WRONG", "matchedListingId": "L1", "replyBoxVisible": True}
    exact = {"found": True, "matchedTransactionId": "TARGET", "matchedListingId": "L1", "replyBoxVisible": True}
    # page1 -> wrong neighbor; page2 -> exact; final post-click locate -> exact
    visited = _wire(monkeypatch, [wrong, exact, exact])
    attempt: dict = {}
    txn, listing = rre.prepare_review_row_for_execution("esd", DECISION, attempt, {})
    assert (txn, listing) == ("TARGET", "L1")
    # It paginated (page 2 was visited) and recorded an exact probe hit.
    assert any("page=2" in u for u in visited)
    probes = attempt.get("review_page_probes") or []
    assert any(p.get("exact") for p in probes)


def test_no_pagination_when_exact_on_page1(monkeypatch):
    exact = {"found": True, "matchedTransactionId": "TARGET", "matchedListingId": "L1", "replyBoxVisible": True}
    visited = _wire(monkeypatch, [exact, exact])
    attempt: dict = {}
    txn, listing = rre.prepare_review_row_for_execution("esd", DECISION, attempt, {})
    assert (txn, listing) == ("TARGET", "L1")
    assert visited == []  # never paginated
    assert "review_page_probes" not in attempt


def test_fails_closed_when_exact_never_found(monkeypatch):
    # Every page returns a found-but-WRONG-transaction neighbor -> must raise
    # the transaction-mismatch error (fail closed, never posts to a neighbor).
    wrong = {"found": True, "matchedTransactionId": "WRONG", "matchedListingId": "L1", "replyBoxVisible": True}
    _wire(monkeypatch, [wrong])  # single return reused for every locate call
    attempt: dict = {}
    with pytest.raises(RuntimeError, match="did not keep the expected transaction_id"):
        rre.prepare_review_row_for_execution("esd", DECISION, attempt, {})


def test_not_found_anywhere_raises_row_not_found(monkeypatch):
    missing = {"found": False}
    _wire(monkeypatch, [missing])
    attempt: dict = {}
    with pytest.raises(RuntimeError, match="could not be found"):
        rre.prepare_review_row_for_execution("esd", DECISION, attempt, {})

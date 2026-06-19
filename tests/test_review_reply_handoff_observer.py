"""Surface 24 Phase B (duck-ops): the observer builds review-reply post-queue
candidates from the structured handoff (canonical ids) instead of parsing the
folded reviews email. Isolated via a tmp DUCKAGENT_RUNS_DIR — no prod reads."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import phase1_observer as obs  # noqa: E402


def _reply(**over):
    base = {
        "index": 1, "transaction_id": 5099739654, "listing_id": 4296762041,
        "shop_id": 54025273, "rating": 5, "customer_review": "Perfect gift!",
        "generated_response": "Thanks so much!", "review_date": "2026-06-16 10:00:00",
        "response_kind": "public_thank_you",
    }
    base.update(over)
    return base


@pytest.fixture
def runs(tmp_path, monkeypatch):
    monkeypatch.setattr(obs, "DUCKAGENT_RUNS_DIR", tmp_path)
    def _write(run_id, replies):
        d = tmp_path / run_id
        d.mkdir(parents=True, exist_ok=True)
        (d / "state_reviews.json").write_text(json.dumps(
            {"reviews_reply_handoff": {"run_id": run_id, "generated_at": "x", "replies": replies}}))
    return _write


# ---- deterministic targeting -------------------------------------------------

def test_resolve_target_short_circuits_on_canonical_ids():
    t = obs.resolve_review_target("2026-06-19", _reply(), "tx-5099739654")
    assert t["match_quality"] == "api_exact"
    assert t["transaction_id"] == "5099739654" and t["listing_id"] == "4296762041"


def test_resolve_target_without_ids_does_not_claim_api_exact():
    # no listing_id -> can't short-circuit; falls through to fuzzy (missing here)
    t = obs.resolve_review_target("2026-06-19", _reply(transaction_id="", listing_id=""), "review-1")
    assert t["match_quality"] != "api_exact"


# ---- handoff enumeration -----------------------------------------------------

def test_recent_handoffs_found(runs):
    from datetime import date
    runs(date.today().isoformat(), [_reply()])
    found = list(obs.recent_reviews_reply_handoffs())
    assert len(found) == 1 and found[0][1]["replies"][0]["transaction_id"] == 5099739654


def test_old_and_nondate_dirs_ignored(runs, tmp_path):
    runs("2026-01-01", [_reply()])          # >10 days old
    (tmp_path / "TEST-RUN").mkdir()          # non-date dir
    assert list(obs.recent_reviews_reply_handoffs()) == []


# ---- candidate building (fail-closed) ---------------------------------------

def test_handoff_builds_candidate_with_api_exact_target(runs):
    from datetime import date
    runs(date.today().isoformat(), [_reply()])
    candidates: dict = {}
    obs._merge_review_reply_handoff_candidates(candidates)
    assert len(candidates) == 1
    cand = next(iter(candidates.values()))
    assert cand["flow"] == "reviews_reply_positive"
    assert cand["artifact_id"].endswith("::tx-5099739654")
    assert cand["review_target"]["match_quality"] == "api_exact"
    assert cand["review_target"]["listing_id"] == "4296762041"
    assert cand["candidate_summary"]["body"] == "Thanks so much!"


def test_reply_without_ids_is_skipped(runs):
    from datetime import date
    runs(date.today().isoformat(), [_reply(transaction_id="", listing_id="")])
    candidates: dict = {}
    obs._merge_review_reply_handoff_candidates(candidates)
    assert candidates == {}  # fail-closed: never target by guess


def test_reply_without_draft_is_skipped(runs):
    from datetime import date
    runs(date.today().isoformat(), [_reply(generated_response="")])
    candidates: dict = {}
    obs._merge_review_reply_handoff_candidates(candidates)
    assert candidates == {}


def test_non_public_kind_is_skipped(runs):
    from datetime import date
    runs(date.today().isoformat(), [_reply(response_kind="private_recovery")])
    candidates: dict = {}
    obs._merge_review_reply_handoff_candidates(candidates)
    assert candidates == {}

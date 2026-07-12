"""Surface 58 — customer_ask_scout unit tests (LLM injected; no API).

Drives scan_customer_asks with the golden fixture converted to signal shape and
a fixture-driven fake classifier, asserting: own-mail/notification signals are
excluded BEFORE classification, variant/logistics questions never mint, genuine
asks become frequency-ranked candidates (two 'corgi' asks from different
customers → one candidate, distinct_requesters=2), the deterministic cross-check
routes an unsupported positive to needs_review, empty input holds, and the
DUCK_TEST_MODE write guard refuses a frozen prod path."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import customer_ask_scout as cas  # noqa: E402

FIXTURE = json.loads(
    (Path("/Users/philtullai/ai-agents/duck-ops/tests/fixtures/customer_ask_golden.json")).read_text()
)
TAXONOMY = cas.load_taxonomy()


def _signal_from_entry(e: dict) -> dict:
    ce = {"customer_text": e["text"], "conversation_contact": e["id"]}
    if e.get("subject"):
        ce["email_subject"] = e["subject"]
    return {
        "artifact_id": e["id"],
        "channel": e["channel"],
        "customer_event": ce,
        "source_refs": [{"subject": e.get("subject", "")}],
    }


def _fixture_fake():
    """Fake classifier keyed by exact text: returns the fixture's label. Records
    which texts were sent for classification (own-mail must never appear)."""
    by_text = {e["text"]: e for e in FIXTURE["entries"]}
    seen: list[str] = []

    def fake(text: str, subject: str) -> dict:
        seen.append(text)
        e = by_text.get(text, {})
        return {"is_ask": bool(e.get("is_ask")), "subject": e.get("expected_subject", "")}

    return fake, seen


def test_scan_excludes_own_mail_and_mints_frequency_ranked_asks():
    items = [_signal_from_entry(e) for e in FIXTURE["entries"]]
    fake, seen = _fixture_fake()
    out = cas.scan_customer_asks(items, taxonomy=TAXONOMY, classify_fn=fake)

    # Own-mail / notification signals must be excluded BEFORE classification.
    own_mail_texts = {e["text"] for e in FIXTURE["entries"] if e.get("own_mail")}
    assert own_mail_texts.isdisjoint(set(seen)), "own-mail was sent to the classifier"

    subjects = {c["subject"].lower(): c for c in out["candidates"]}
    # The two 'corgi' asks from different customers collapse to one candidate.
    assert "corgi" in subjects
    assert subjects["corgi"]["distinct_requesters"] == 2
    # Other genuine asks minted.
    for expect in ("green bay packers", "firefighter", "nurse", "dachshund"):
        assert expect in subjects, f"missing ask candidate: {expect}"
    # Corgi (2 requesters) sorts ahead of the singles.
    assert out["candidates"][0]["subject"].lower() == "corgi"
    # Variant/logistics/praise never mint.
    for banned in ("colors", "order", "any other fun ducks"):
        assert not any(banned in c["subject"].lower() for c in out["candidates"])


def test_is_own_system_mail_predicate():
    assert cas.is_own_system_mail("MJD: [newduck] Corgi | FLOW:newduck | RUN:1 | ACTION:review")
    assert cas.is_own_system_mail("You made a sale on Etsy!")
    assert cas.is_own_system_mail("Daily Etsy review summary")
    assert cas.is_own_system_mail("Re: MJD: something")
    assert not cas.is_own_system_mail("Do you make a corgi duck?")
    assert not cas.is_own_system_mail("")


def test_cross_check_routes_unsupported_positive_to_needs_review():
    # LLM confidently says is_ask but returns no subject -> must NOT mint.
    sig = {"artifact_id": "x", "channel": "mailbox_email",
           "customer_event": {"customer_text": "Do you have anything cool?"}}
    out = cas.scan_customer_asks([sig], taxonomy=TAXONOMY,
                                 classify_fn=lambda t, s: {"is_ask": True, "subject": ""})
    assert out["candidates"] == []
    assert len(out["needs_review"]) == 1
    assert out["needs_review"][0]["reason"] == "empty_subject"


def test_empty_input_holds():
    out = cas.scan_customer_asks([], taxonomy=TAXONOMY, classify_fn=lambda t, s: {"is_ask": False})
    assert out["candidates"] == []
    assert out["needs_review"] == []


def test_write_guard_refuses_frozen_prod_path(monkeypatch):
    # DUCK_TEST_MODE=1 (conftest default) + a path still resolving to the frozen
    # production path must raise, not silently write prod.
    monkeypatch.setenv("DUCK_TEST_MODE", "1")
    with pytest.raises(cas.TestModeRefusalError):
        cas._write_json(cas._FROZEN_CANDIDATES_PATH, cas._FROZEN_CANDIDATES_PATH, {"x": 1})


def test_feeder_turns_candidate_into_customer_ask_queue_item():
    import product_concept_queue as pcq
    payload = {"candidates": [
        {"subject": "corgi", "distinct_requesters": 2,
         "sample_quotes": ["Do you make a corgi duck?"],
         "source_artifact_ids": ["sig-1", "sig-2"]},
    ]}
    items = pcq._customer_ask_items(payload)
    assert len(items) == 1
    item = items[0]
    assert item["source_type"] == "customer_ask"
    assert "corgi" in item["theme"].lower()
    assert item["queue_state"] in ("ready_for_brief_review", "blocked_by_guardrail")
    assert item["concept_design_brief"]["brief_source"] == "customer_ask"
    # Frequency shows in score + evidence (2 requesters -> 0.8).
    assert item["score"] == pytest.approx(0.8)
    assert any("2 distinct" in e for e in item["evidence"])


def test_feeder_empty_payload_holds():
    import product_concept_queue as pcq
    assert pcq._customer_ask_items({"candidates": []}) == []

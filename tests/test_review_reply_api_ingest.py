"""Surface 24 Phase 1: API reviews -> quality_gate_state ingest. Fully isolated
to tmp paths so scoring real candidates never writes prod state. (The autouse
conftest + source-guard + pollution-audit land in Phase 5; this test self-
isolates so it's safe to run standalone too.)"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import review_reply_api_ingest as ing  # noqa: E402
import review_reply_executor as ex  # noqa: E402


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    monkeypatch.setattr(ex, "QUALITY_GATE_STATE_PATH", tmp_path / "quality_gate_state.json")
    monkeypatch.setattr(ex, "EXECUTION_QUEUE_STATE_PATH", tmp_path / "queue.json")
    monkeypatch.setattr(ing, "INGEST_RECEIPT_PATH", tmp_path / "receipt.json")
    monkeypatch.setattr(ing, "POSTED_TRANSACTIONS_PATH", tmp_path / "posted.json")
    self_tmp = tmp_path
    yield self_tmp


def _review(**over):
    base = {
        "shop_id": 54025273, "listing_id": 4296762041, "transaction_id": 5099739654,
        "rating": 5, "review": "Perfect gift for my older son!",
        "create_timestamp": 1781663738,
        "draft_reply": "Thank you so much! So glad it made a great gift for your son.",
    }
    base.update(over)
    return base


def _artifacts(tmp):
    return json.loads((tmp / "quality_gate_state.json").read_text())["artifacts"]


# ---- mapping --------------------------------------------------------------

def test_five_star_with_ids_becomes_scored_artifact(_isolate):
    rec = ing.ingest_api_reviews([_review()], "2026-06-17", source_path="/tmp/x.json")
    assert rec["counts"]["ingested"] == 1
    art = list(_artifacts(_isolate).values())[0]["decision"]
    assert art["flow"] == "reviews_reply_positive"
    assert art["decision"] in {"publish_ready", "needs_revision", "discard"}
    assert art["review_status"] == "pending"
    assert art["execution_state"] == "not_queued"
    assert art["review_target"]["match_quality"] == "api_exact"
    assert art["review_target"]["transaction_id"] == "5099739654"
    assert art["review_target"]["listing_id"] == "4296762041"
    assert "son" in art["approved_reply_text"]  # carries the drafted reply text
    assert art["artifact_id"] == "publish::reviews_reply_positive::2026-06-17::tx-5099739654"


def test_artifact_id_uses_review_date_not_today(_isolate):
    # create_timestamp 1781663738 -> a fixed date, independent of wall clock
    rec = ing.ingest_api_reviews([_review()], "2026-06-17")
    aid = list(_artifacts(_isolate).keys())[0]
    assert "::tx-5099739654" in aid and aid.startswith("publish::reviews_reply_positive::2026-")


# ---- fail-closed eligibility ----------------------------------------------

def test_non_five_star_skipped(_isolate):
    rec = ing.ingest_api_reviews([_review(rating=3)], "2026-06-17")
    assert rec["counts"]["ingested"] == 0 and rec["counts"]["skipped_ineligible"] == 1
    assert _artifacts(_isolate) == {}


def test_missing_transaction_or_listing_skipped(_isolate):
    rec = ing.ingest_api_reviews(
        [_review(transaction_id=""), _review(listing_id="")], "2026-06-17")
    assert rec["counts"]["ingested"] == 0 and rec["counts"]["skipped_ineligible"] == 2


def test_empty_draft_reply_skipped(_isolate):
    rec = ing.ingest_api_reviews([_review(draft_reply="")], "2026-06-17")
    assert rec["counts"]["skipped_ineligible"] == 1 and _artifacts(_isolate) == {}


# ---- dedup / already-handled ----------------------------------------------

def test_reingest_same_review_is_deduped(_isolate):
    ing.ingest_api_reviews([_review()], "2026-06-17")
    rec = ing.ingest_api_reviews([_review()], "2026-06-17")
    assert rec["counts"]["deduped"] == 1 and rec["counts"]["ingested"] == 0
    assert len(_artifacts(_isolate)) == 1  # not duplicated


def test_already_posted_transaction_skipped(_isolate):
    (_isolate / "posted.json").write_text(json.dumps({"transaction_ids": ["5099739654"]}))
    rec = ing.ingest_api_reviews([_review()], "2026-06-17")
    assert rec["counts"]["skipped_already_handled"] == 1 and _artifacts(_isolate) == {}


def test_already_in_queue_skipped(_isolate):
    aid = "publish::reviews_reply_positive::2026-06-17::tx-5099739654"
    (_isolate / "queue.json").write_text(json.dumps({"items": {aid: {"status": "posted"}}}))
    rec = ing.ingest_api_reviews([_review()], "2026-06-17")
    assert rec["counts"]["skipped_already_handled"] == 1 and _artifacts(_isolate) == {}


def test_reingest_unchanged_does_not_reset_a_queued_artifact(_isolate):
    # The real protection: an unchanged review that's already queued/approved is
    # DEDUPED (input_hash skip) -> its resolved status is never reset to pending.
    ing.ingest_api_reviews([_review()], "2026-06-17")
    state = json.loads((_isolate / "quality_gate_state.json").read_text())
    aid = list(state["artifacts"].keys())[0]
    state["artifacts"][aid]["decision"]["review_status"] = "approved"
    state["artifacts"][aid]["decision"]["execution_state"] = "queued"
    (_isolate / "quality_gate_state.json").write_text(json.dumps(state))
    rec = ing.ingest_api_reviews([_review()], "2026-06-17")  # same material
    assert rec["counts"]["deduped"] == 1 and rec["counts"]["ingested"] == 0
    after = json.loads((_isolate / "quality_gate_state.json").read_text())["artifacts"][aid]["decision"]
    assert after["review_status"] == "approved" and after["execution_state"] == "queued"


# ---- receipt --------------------------------------------------------------

def test_receipt_written_with_counts(_isolate):
    ing.ingest_api_reviews([_review()], "2026-06-17")
    receipt = json.loads((_isolate / "receipt.json").read_text())
    assert receipt["source_mode"] == "etsy_reviews_api"
    assert receipt["counts"]["ingested"] == 1
    assert receipt["run_id"] == "2026-06-17"

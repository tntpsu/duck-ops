"""Surface 24: ingest Etsy-API reviews + drafted replies into the review-reply
post-queue feed.

The post-queue was historically fed by a MANUAL browser "discovery" scan that
was never automated and went dormant after the 2026-04-24 pause. Meanwhile the
duckAgent reviews.daily flow already fetches every review via the Etsy API (with
canonical transaction_id + listing_id) and drafts an AI reply. This module
routes those drafts THROUGH the existing quality gate (evaluate_quality_gate —
never forcing publish_ready) and persists them as artifacts in
quality_gate_state.json, exactly the shape auto_enqueue_publish_ready → drain →
browser-post already consume. The browser becomes post-only.

This is duck-ops-owned (it owns quality_gate_state's schema). duckAgent calls
ingest_api_reviews() via a fail-open sibling-path bridge.

Fail-closed everywhere — this posts public replies to real customers:
  * only rating == 5 reviews enter (others stay in the private-followup lane),
  * missing transaction_id/listing_id -> skipped, never a publishable artifact,
  * a review already posted (posted-transaction ledger) or already in the
    execution queue -> skipped,
  * scoring is the existing gate's job: < threshold -> needs_revision (operator),
  * any evaluator error on a candidate -> skipped, never a half-built artifact.

HOLD-mode (operator reviews the first reconnect batch before anything posts) is
enforced downstream in auto_enqueue_publish_ready, not here — this module always
writes the honestly-scored artifact.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

RUNTIME_DIR = Path(__file__).resolve().parent
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from quality_gate_pilot import (  # noqa: E402
    EVALUATOR_VERSION,
    apply_execution_state_reconciliation,
    canonical_hash,
    carry_forward_review_resolution,
    evaluate_quality_gate,
    material_candidate_hash,
    now_iso,
    slugify,
)
from review_reply_executor import (  # noqa: E402
    STATE_DIR,
    load_quality_gate_state,
    load_queue_state,
    save_quality_gate_state,
)

# The already-posted ledger (keyed by transaction_id). Written by the executor's
# success / already_replied paths (Phase 2); read here as an ingest-time guard.
POSTED_TRANSACTIONS_PATH = STATE_DIR / "review_reply_posted_transactions.json"
INGEST_RECEIPT_PATH = STATE_DIR / "review_reply_api_ingest_receipt.json"

API_SOURCE_MODE = "etsy_reviews_api"
API_MATCH_QUALITY = "api_exact"
# Queue statuses that mean "already handled / in flight" -> do not re-ingest.
_QUEUE_DONE_STATUSES = {"queued", "running", "posted", "dismissed", "skipped"}


def load_posted_transactions(path: Path | None = None) -> set[str]:
    """Set of transaction_ids we've already posted to (or resolved as
    already-replied). Fail-soft: a missing/corrupt ledger reads as empty."""
    import json

    target = Path(path) if path else POSTED_TRANSACTIONS_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return set()
    ids = payload.get("transaction_ids") if isinstance(payload, dict) else payload
    return {str(t) for t in (ids or []) if str(t).strip()}


def _review_date(review: dict[str, Any]) -> str:
    ts = review.get("create_timestamp") or review.get("created_timestamp")
    if isinstance(ts, (int, float)) and ts > 0:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
    return datetime.now(timezone.utc).date().isoformat()


def _identifiers(review: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(review.get("transaction_id") or "").strip(),
        str(review.get("listing_id") or "").strip(),
        str(review.get("shop_id") or "").strip(),
    )


def build_candidate(review: dict[str, Any], reply_text: str, run_id: str,
                    *, source_path: str | None = None) -> dict[str, Any] | None:
    """API review + drafted reply -> a candidate in evaluate_quality_gate's
    reviews_reply_positive shape, or None if the review is ineligible
    (non-5-star, missing identifiers, or no drafted reply)."""
    if int(review.get("rating") or 0) != 5:
        return None
    transaction_id, listing_id, shop_id = _identifiers(review)
    if not transaction_id or not listing_id:
        return None
    reply_text = str(reply_text or "").strip()
    if not reply_text:
        return None
    review_date = _review_date(review)
    artifact_id = f"publish::reviews_reply_positive::{review_date}::tx-{transaction_id}"
    return {
        "artifact_id": artifact_id,
        "artifact_type": "review_reply",
        "flow": "reviews_reply_positive",
        "run_id": run_id,
        "candidate_summary": {
            "title": f"Review {transaction_id}",
            "body": reply_text,
            "customer_review": str(review.get("review") or ""),
            "review_rating": 5,
            "review_date": review_date,
            "response_kind": "public",
            "transaction_id": transaction_id,
        },
        "review_target": {
            "shop_id": shop_id or None,
            "review_key": slugify(f"{transaction_id}-{listing_id}"),
            "review_id": None,
            "transaction_id": transaction_id,
            "listing_id": listing_id,
            "review_url": None,
            "match_quality": API_MATCH_QUALITY,
        },
        "source_refs": [{"path": source_path}] if source_path else [],
        "normalization_notes": {"source_mode": API_SOURCE_MODE},
    }


def ingest_api_reviews(reviews: list[dict[str, Any]], run_id: str,
                       *, source_path: str | None = None,
                       write_receipt: bool = True) -> dict[str, Any]:
    """Score + persist API reviews into quality_gate_state. Returns a receipt
    with counts. Reuses the canonical persist pattern (input_hash skip +
    carry_forward) from quality_gate_pilot.main so a queued/posted artifact is
    never reset back to not_queued."""
    state = load_quality_gate_state()
    artifacts: dict[str, Any] = state.setdefault("artifacts", {})
    queue_items = (load_queue_state().get("items") or {})
    posted = load_posted_transactions()

    counts = {"seen": len(reviews or []), "ingested": 0, "deduped": 0,
              "skipped_ineligible": 0, "skipped_already_handled": 0, "errored": 0}
    ingested_ids: list[str] = []

    for review in reviews or []:
        if not isinstance(review, dict):
            counts["skipped_ineligible"] += 1
            continue
        candidate = build_candidate(review, _reply_for(review), run_id, source_path=source_path)
        if candidate is None:
            counts["skipped_ineligible"] += 1
            continue
        transaction_id = candidate["review_target"]["transaction_id"]
        artifact_id = candidate["artifact_id"]

        # already posted to this review, or already in flight in the queue
        if transaction_id in posted:
            counts["skipped_already_handled"] += 1
            continue
        queue_status = str((queue_items.get(artifact_id) or {}).get("status") or "")
        if queue_status in _QUEUE_DONE_STATUSES:
            counts["skipped_already_handled"] += 1
            continue

        material_hash = material_candidate_hash(candidate)
        input_hash = canonical_hash({"evaluator_version": EVALUATOR_VERSION, "material_hash": material_hash})
        previous = artifacts.get(artifact_id)
        if previous and str(previous.get("input_hash") or "") == input_hash:
            counts["deduped"] += 1
            continue

        try:
            decision = evaluate_quality_gate(candidate)
            recon = apply_execution_state_reconciliation(candidate, decision)
            carried = carry_forward_review_resolution(decision, previous, material_hash=material_hash)
        except Exception as exc:  # fail-closed: never persist a half-built artifact
            print(f"[review_api_ingest] WARNING evaluator failed for {artifact_id}: {exc}", file=sys.stderr)
            counts["errored"] += 1
            continue

        record = {
            "artifact_id": artifact_id,
            "input_hash": input_hash,
            "material_hash": material_hash,
            "evaluated_at": decision["created_at"],
            "decision": decision,
            "source_mode": API_SOURCE_MODE,
        }
        record.update(recon)
        record.update(carried)
        artifacts[artifact_id] = record
        counts["ingested"] += 1
        ingested_ids.append(artifact_id)

    save_quality_gate_state(state)

    receipt = {
        "generated_at": now_iso(),
        "run_id": run_id,
        "source_mode": API_SOURCE_MODE,
        "counts": counts,
        "ingested_artifact_ids": ingested_ids,
    }
    if write_receipt:
        _write_receipt(receipt)
    return receipt


# The drafted reply travels alongside the review. duckAgent enriches each review
# dict with the AI draft under one of these keys before calling us; we read the
# first present so the bridge stays decoupled from duckAgent's internal naming.
_REPLY_KEYS = ("draft_reply", "thank_you_message", "approved_reply_text", "reply_text")


def _reply_for(review: dict[str, Any]) -> str:
    for key in _REPLY_KEYS:
        value = str(review.get(key) or "").strip()
        if value:
            return value
    return ""


def _write_receipt(receipt: dict[str, Any]) -> None:
    import json
    import os
    import tempfile

    INGEST_RECEIPT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(INGEST_RECEIPT_PATH.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(receipt, fh, indent=2)
        os.replace(tmp, INGEST_RECEIPT_PATH)
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass

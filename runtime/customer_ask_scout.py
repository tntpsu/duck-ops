"""Surface 58 — Customer-Ask Scout.

Mines the already-normalized customer signal feed (inbox messages + review text,
`state/normalized/customer_signals.json`, written by phase1_observer) for
explicit NEW-PRODUCT requests ("do you make a corgi duck?") and writes
frequency-ranked candidates for the product-concept queue's `_customer_ask_items`
feeder to pick up (source_type="customer_ask"). The queue's Promote + brief-
approval gates are unchanged — nothing here spends image/3D credits.

Design (mirrors the build_next producer/reader split): the LLM classification
lives here and writes `state/customer_ask_candidates.json`; the pure-function
queue builder only reads that file, so it stays unit-testable with fixtures.

`scan_customer_asks` is a pure function (LLM injected) so the exclusion /
pre-filter / cross-check / frequency logic is fully testable without the API.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from llm_call_helpers import call_openai, extract_text
from workflow_control import TestModeRefusalError

ROOT = Path(__file__).resolve().parents[1]
CUSTOMER_SIGNALS_PATH = ROOT / "state" / "normalized" / "customer_signals.json"
CUSTOMER_ASK_CANDIDATES_PATH = ROOT / "state" / "customer_ask_candidates.json"
CUSTOMER_ASK_NEEDS_REVIEW_PATH = ROOT / "state" / "customer_ask_needs_review.json"
CUSTOMER_ASK_TAXONOMY_PATH = ROOT / "config" / "customer_ask_taxonomy.json"

# Comparison anchors for the DUCK_TEST_MODE write guard (architectural pattern
# per CLAUDE.md §4; mirrors build_next_engine._FROZEN_PRODUCTION_*).
_FROZEN_CANDIDATES_PATH = CUSTOMER_ASK_CANDIDATES_PATH.resolve()
_FROZEN_NEEDS_REVIEW_PATH = CUSTOMER_ASK_NEEDS_REVIEW_PATH.resolve()

DEFAULT_MODEL = "gpt-4o-mini"

# Own-system-mail / Etsy-notification subject markers. Kept identical to the
# canonical list in customer_interaction_cases.py:186-216 (the source of the
# 32-phantom-item incident, memory feedback_mailbox_detectors_exclude_own_mail);
# keep the two in sync. Reviews are always genuine customer text, so this is
# only applied to mailbox_email signals.
_OWN_MAIL_SUBJECT_MARKERS = ("mjd:", "flow:", "action:", "run:", "you have unread messages")
_NOTIFICATION_SUBJECT_TERMS = (
    "you made a sale",
    "your etsy order",
    "daily etsy review summary",
    "review summary",
    "this week in your shop",
    "order #",
    "shipment",
    "tracking",
)


def is_own_system_mail(subject: str) -> bool:
    """True when a mailbox subject is the system's OWN outbound or an Etsy
    notification, NOT a customer message. Must run BEFORE any content heuristic
    (replies quote MJD: subjects, so the body matches 'custom duck')."""
    s = str(subject or "").lower()
    if any(marker in s for marker in _OWN_MAIL_SUBJECT_MARKERS):
        return True
    if any(term in s for term in _NOTIFICATION_SUBJECT_TERMS):
        return True
    return False


def load_taxonomy(path: Path | None = None) -> dict[str, Any]:
    p = path or CUSTOMER_ASK_TAXONOMY_PATH
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"customer_ask_taxonomy_version": 0, "ask_keywords": [], "not_a_new_product": []}


def _requester_key(signal: dict[str, Any]) -> str:
    """Stable per-customer key for frequency counting. Falls back through
    contact -> thread -> order -> artifact so one chatty customer isn't counted
    as many, and distinct customers aren't collapsed into one."""
    ce = signal.get("customer_event") or {}
    biz = signal.get("business_context") or {}
    for candidate in (
        ce.get("conversation_contact"),
        ce.get("conversation_thread_key"),
        biz.get("order_id"),
        signal.get("artifact_id"),
    ):
        if candidate:
            return str(candidate)
    return "unknown"


def _signal_text(signal: dict[str, Any]) -> tuple[str, str]:
    ce = signal.get("customer_event") or {}
    text = str(ce.get("customer_text") or ce.get("raw_customer_text") or "").strip()
    subject = str(ce.get("email_subject") or "").strip()
    if not subject:
        refs = signal.get("source_refs") or []
        if refs and isinstance(refs[0], dict):
            subject = str(refs[0].get("subject") or "").strip()
    return text, subject


def scan_customer_asks(
    items: list[dict[str, Any]],
    *,
    taxonomy: dict[str, Any],
    classify_fn: Callable[[str, str], dict[str, Any]],
) -> dict[str, Any]:
    """Pure: given raw customer signals + an injected LLM classifier, return
    frequency-ranked ask candidates + needs_review items + stats. No I/O."""
    ask_keywords = [k.lower() for k in taxonomy.get("ask_keywords", [])]
    not_new = [k.lower() for k in taxonomy.get("not_a_new_product", [])]
    groups: dict[str, dict[str, Any]] = {}
    needs_review: list[dict[str, Any]] = []
    stats = {
        "scanned": 0, "excluded_own_mail": 0, "prefiltered": 0,
        "classified": 0, "asks": 0, "needs_review": 0, "llm_errors": 0,
    }

    for signal in items:
        stats["scanned"] += 1
        text, subject = _signal_text(signal)
        if not text and not subject:
            continue
        channel = signal.get("channel")
        # Own-mail exclusion (mailbox only; reviews are always customer text).
        if channel == "mailbox_email" and is_own_system_mail(subject):
            stats["excluded_own_mail"] += 1
            continue
        low = text.lower()
        # Variant/logistics guard: a question about an EXISTING product is not a
        # new-product ask — force-skip regardless of what the LLM might say.
        if any(p in low for p in not_new):
            continue
        has_keyword = any(k in low for k in ask_keywords)
        has_question = "?" in text
        # Cheap pre-filter: only pay for classification on plausible asks.
        if not (has_keyword or has_question):
            continue
        stats["prefiltered"] += 1

        result = classify_fn(text, subject) or {}
        stats["classified"] += 1
        if result.get("error"):
            stats["llm_errors"] += 1
            continue
        if not result.get("is_ask"):
            continue
        subj = str(result.get("subject") or "").strip()

        # Deterministic evidence cross-check (memory llm-stated-confidence-is-weak):
        # an is_ask=true with no clean subject, or a one-word subject unsupported
        # by any ask-keyword, is a confidently-wrong positive -> needs_review,
        # never minted.
        if not subj or (not has_keyword and len(subj.split()) <= 1 and not has_question):
            needs_review.append({
                "reason": "unsupported_positive" if subj else "empty_subject",
                "subject": subj,
                "text": text[:200],
                "artifact_id": signal.get("artifact_id"),
                "channel": channel,
            })
            stats["needs_review"] += 1
            continue

        key = subj.lower().strip()
        group = groups.setdefault(key, {
            "subject": subj, "requesters": set(), "quotes": [],
            "source_artifact_ids": [], "channels": set(),
        })
        group["requesters"].add(_requester_key(signal))
        group["channels"].add(channel)
        if len(group["quotes"]) < 3:
            group["quotes"].append(text[:180])
        if signal.get("artifact_id"):
            group["source_artifact_ids"].append(signal.get("artifact_id"))
        stats["asks"] += 1

    candidates = []
    for group in groups.values():
        n = len(group["requesters"])
        candidates.append({
            "subject": group["subject"],
            "distinct_requesters": n,
            "score": n,  # frequency-weighted: N distinct customers asking is the signal
            "channels": sorted(group["channels"]),
            "sample_quotes": group["quotes"],
            "source_artifact_ids": group["source_artifact_ids"],
        })
    candidates.sort(key=lambda c: (-c["distinct_requesters"], c["subject"]))
    return {"candidates": candidates, "needs_review": needs_review, "stats": stats}


_CLASSIFY_PROMPT = (
    "You decide whether a customer's message to a 3D-printed rubber-duck shop is an "
    "explicit request for a NEW duck product we don't already sell (a character, "
    "animal, breed, sports team, profession, or hobby the customer wants made into a "
    "duck). A question about an existing product's color/size, a shipping/refund "
    "question, praise, or vague chatter is NOT a request. Reply ONLY as JSON: "
    '{{"is_ask": true|false, "subject": "<short clean product subject, e.g. corgi or '
    'green bay packers, empty if not an ask>", "confidence": 0.0-1.0}}.\n\n'
    "Subject line: {subject}\nMessage: {text}"
)


def _llm_classify(text: str, subject: str, *, model: str = DEFAULT_MODEL) -> dict[str, Any]:
    """Real temperature-0 classifier. Fails visibly (error flag) rather than
    silently minting or dropping."""
    prompt = _CLASSIFY_PROMPT.format(subject=subject[:200], text=text[:1200])
    resp = call_openai(
        prompt, model=model, temperature=0.0, max_tokens=200,
        response_format={"type": "json_object"},
    )
    if resp is None:
        return {"error": "no_api_key", "is_ask": False}
    if not isinstance(resp, dict) or resp.get("error"):
        return {"error": str((resp or {}).get("error") or "call_failed"), "is_ask": False}
    try:
        data = json.loads(extract_text(resp))
    except (json.JSONDecodeError, TypeError):
        return {"error": "unparseable", "is_ask": False}
    return {
        "is_ask": bool(data.get("is_ask")),
        "subject": str(data.get("subject") or ""),
        "confidence": data.get("confidence"),
    }


def _refusing_test_mode_prod_write(path: Path, frozen: Path) -> bool:
    return os.environ.get("DUCK_TEST_MODE") == "1" and Path(path).resolve() == frozen


def _write_json(path: Path, frozen: Path, payload: dict[str, Any]) -> None:
    if _refusing_test_mode_prod_write(path, frozen):
        raise TestModeRefusalError(
            f"DUCK_TEST_MODE=1 but {path} still points at the frozen production path; "
            "monkeypatch it to a tmp path in the test (see tests/conftest.py)."
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def run() -> dict[str, Any]:
    """Producer entry point: read cached signals, classify, write candidates +
    needs_review side file. Never calls a live API for the input (reads the
    normalized state file)."""
    try:
        signals = json.loads(CUSTOMER_SIGNALS_PATH.read_text(encoding="utf-8"))
    except Exception:
        signals = {}
    items = signals.get("items") if isinstance(signals, dict) else (signals or [])
    taxonomy = load_taxonomy()
    result = scan_customer_asks(list(items or []), taxonomy=taxonomy, classify_fn=_llm_classify)

    generated_at = datetime.now(timezone.utc).astimezone().isoformat()
    _write_json(CUSTOMER_ASK_CANDIDATES_PATH, _FROZEN_CANDIDATES_PATH, {
        "generated_at": generated_at,
        "taxonomy_version": taxonomy.get("customer_ask_taxonomy_version"),
        "candidates": result["candidates"],
        "stats": result["stats"],
    })
    _write_json(CUSTOMER_ASK_NEEDS_REVIEW_PATH, _FROZEN_NEEDS_REVIEW_PATH, {
        "generated_at": generated_at,
        "needs_review": result["needs_review"],
    })
    return result


if __name__ == "__main__":
    out = run()
    print(json.dumps(out["stats"], indent=2))

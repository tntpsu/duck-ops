"""Periodic refresher for duck-ops/state/review_queue.json.

Closes the residual race between DuckAgent publishing a flow and the
operator next interacting with the queue. Without a periodic refresh,
``write_review_queue()`` only runs on operator commands — so an
operator can see a "pending" card on the portal for hours after
DuckAgent has already published the underlying artifact.

The viewer's queue self-correction filter (added 2026-05-25) catches
the most common case at render time, but the canonical review_queue.json
file still drifts. Some consumers (notifier, OS health surfaces) read
the file directly. Keeping it fresh on a 5-minute cadence makes the
whole stack honest.

What this runner does (delegates entirely to ``review_loop``):

  1. Load state_bundle (quality_gate, trend_ranker, customer_signals,
     etc.) and operator_state.
  2. ``write_review_queue`` reconciles against DuckAgent state files
     and writes the updated review_queue.json + review_queue.md.
  3. Append a receipt to state/review_queue_refresh_receipts.jsonl so
     "did the refresher actually run at HH:MM?" is grep-able.

Idempotent. Safe to call multiple times. Errors are logged to the
receipt log; never raise (so launchd doesn't mark the job failed and
back off — we want it to keep trying).

CLI: python runtime/refresh_review_queue.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

RUNTIME_DIR = Path(__file__).resolve().parent
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import review_loop  # type: ignore  # noqa: E402

DUCK_OPS_ROOT = RUNTIME_DIR.parent
RECEIPT_PATH = DUCK_OPS_ROOT / "state" / "review_queue_refresh_receipts.jsonl"


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _append_receipt(payload: dict[str, Any], path: Path | None = None) -> None:
    target = path or RECEIPT_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except OSError:
        # Best-effort: never let logging failures crash the runner.
        pass


def refresh(*, receipt_path: Path | None = None) -> dict[str, Any]:
    """Reload state, reconcile, and write the canonical review queue.

    Returns a receipt dict with at minimum ``at`` and ``outcome``
    fields. On success, also re-reads the freshly written
    review_queue.json so the receipt carries pending_count /
    pending_count_all / generated_at — handy for ``tail -F`` on the
    receipt log when you want to see queue depth over time."""
    receipt: dict[str, Any] = {"at": _now_iso(), "outcome": "unknown"}
    try:
        state_bundle = review_loop.load_state_bundle()
        operator_state = review_loop.load_operator_state()
        # write_review_queue returns the output paths, not the payload.
        # Read the file back to extract metrics for the receipt log.
        review_loop.write_review_queue(state_bundle, operator_state)
        queue_payload = _load_review_queue_payload()
        receipt["outcome"] = "ok"
        receipt["pending_count"] = queue_payload.get("pending_count")
        receipt["pending_count_all"] = queue_payload.get("pending_count_all")
        receipt["generated_at"] = queue_payload.get("generated_at")
    except Exception as exc:
        receipt["outcome"] = f"error:{type(exc).__name__}"
        receipt["error"] = str(exc)[:500]
    _append_receipt(receipt, path=receipt_path)
    return receipt


def _load_review_queue_payload() -> dict[str, Any]:
    """Read review_queue.json after write so we can report counts in
    the receipt. Returns {} on read failure rather than raising — the
    refresh itself already succeeded; metric-reporting shouldn't
    propagate errors."""
    queue_path = DUCK_OPS_ROOT / "state" / "review_queue.json"
    try:
        return json.loads(queue_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def main() -> int:
    receipt = refresh()
    print(json.dumps(receipt, indent=2, ensure_ascii=False))
    # Exit 0 even on error so launchd's KeepAlive/ThrottleInterval
    # doesn't enter back-off — the receipt log is the audit trail.
    return 0


if __name__ == "__main__":
    sys.exit(main())

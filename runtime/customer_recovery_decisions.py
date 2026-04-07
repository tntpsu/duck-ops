#!/usr/bin/env python3
"""
Persisted operator recovery decisions for Duck Ops customer cases.

This module intentionally stays simple in the first slice:

- decisions are stored as append-only JSONL
- matching is deterministic and conservative
- decisions are staged memory, not automatic execution
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
DECISIONS_PATH = STATE_DIR / "customer_recovery_decisions.jsonl"

RESOLUTION_ALIASES = {
    "replace": "replacement",
    "replacement": "replacement",
    "resend": "replacement",
    "refund": "refund",
    "reply": "reply_only",
    "reply_only": "reply_only",
    "wait": "wait",
    "hold": "wait",
    "escalate": "escalate",
}


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def _parse_iso(value: str | None) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        return datetime.fromtimestamp(0).astimezone()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.astimezone()
        return parsed
    except ValueError:
        return datetime.fromtimestamp(0).astimezone()


def normalize_resolution(value: str | None) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    return RESOLUTION_ALIASES.get(normalized, normalized)


def ensure_parent(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def append_decision(row: dict[str, Any]) -> dict[str, Any]:
    ensure_parent(DECISIONS_PATH)
    with DECISIONS_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row))
        handle.write("\n")
    return row


def load_customer_recovery_decisions() -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    if DECISIONS_PATH.exists():
        for raw_line in DECISIONS_PATH.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            row["resolution"] = normalize_resolution(row.get("resolution"))
            items.append(row)

    items.sort(key=lambda row: (_parse_iso(row.get("recorded_at")), str(row.get("resolution") or "")))
    by_artifact_id: dict[str, dict[str, Any]] = {}
    by_receipt_id: dict[str, dict[str, Any]] = {}
    by_transaction_id: dict[str, dict[str, Any]] = {}
    for row in items:
        for key_name, index in (
            ("artifact_id", by_artifact_id),
            ("receipt_id", by_receipt_id),
            ("transaction_id", by_transaction_id),
        ):
            key = str(row.get(key_name) or "").strip()
            if key:
                index[key] = row

    return {
        "generated_at": now_iso(),
        "counts": {
            "items": len(items),
            "by_artifact_id": len(by_artifact_id),
            "by_receipt_id": len(by_receipt_id),
            "by_transaction_id": len(by_transaction_id),
        },
        "items": items,
        "by_artifact_id": by_artifact_id,
        "by_receipt_id": by_receipt_id,
        "by_transaction_id": by_transaction_id,
    }


def _latest_match(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    rows = [row for row in candidates if row]
    if not rows:
        return None
    rows.sort(key=lambda row: _parse_iso(row.get("recorded_at")), reverse=True)
    return rows[0]


def match_decision_for_case(case: dict[str, Any], decision_state: dict[str, Any]) -> dict[str, Any] | None:
    order = case.get("order_enrichment") or {}
    candidates = [
        (decision_state.get("by_artifact_id") or {}).get(str(case.get("artifact_id") or "").strip()),
        (decision_state.get("by_receipt_id") or {}).get(str(order.get("receipt_id") or "").strip()),
        (decision_state.get("by_transaction_id") or {}).get(str(order.get("transaction_id") or "").strip()),
    ]
    return _latest_match(candidates)


def apply_customer_recovery_decisions(customer_cases: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    decision_state = load_customer_recovery_decisions()
    matched_cases = 0
    counts_by_resolution: dict[str, int] = {}
    rows: list[dict[str, Any]] = []
    for case in customer_cases:
        enriched = dict(case)
        decision = match_decision_for_case(case, decision_state)
        if decision:
            matched_cases += 1
            resolution = normalize_resolution(decision.get("resolution"))
            counts_by_resolution[resolution] = counts_by_resolution.get(resolution, 0) + 1
            enriched["approved_recovery_action"] = resolution
            enriched["operator_decision"] = {
                "resolution": resolution,
                "note": decision.get("note"),
                "recorded_at": decision.get("recorded_at"),
                "source": decision.get("source") or "customer_recovery_decisions",
            }
        rows.append(enriched)

    summary = {
        "generated_at": now_iso(),
        "counts": {
            "decisions_total": (decision_state.get("counts") or {}).get("items", 0),
            "matched_cases": matched_cases,
            **{f"matched_{key}": value for key, value in sorted(counts_by_resolution.items())},
        },
        "decisions_path": str(DECISIONS_PATH),
    }
    return rows, summary


def _build_record_from_args(args: argparse.Namespace) -> dict[str, Any]:
    resolution = normalize_resolution(args.resolution)
    if resolution not in {"replacement", "refund", "reply_only", "wait", "escalate"}:
        raise SystemExit(f"Unsupported customer recovery resolution: {args.resolution}")
    return {
        "artifact_id": args.artifact_id,
        "receipt_id": args.receipt_id,
        "transaction_id": args.transaction_id,
        "resolution": resolution,
        "note": args.note or "",
        "recorded_at": now_iso(),
        "source": args.source or "manual_cli",
        "operator": args.operator or "unknown",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Duck Ops customer recovery decision recorder")
    sub = parser.add_subparsers(dest="command", required=True)

    record_parser = sub.add_parser("record", help="Record a customer recovery decision")
    record_parser.add_argument("--artifact-id", default=None)
    record_parser.add_argument("--receipt-id", default=None)
    record_parser.add_argument("--transaction-id", default=None)
    record_parser.add_argument("--resolution", required=True)
    record_parser.add_argument("--note", default="")
    record_parser.add_argument("--source", default="manual_cli")
    record_parser.add_argument("--operator", default="unknown")

    sub.add_parser("show", help="Show the latest decision index")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "record":
        row = _build_record_from_args(args)
        if not any(str(row.get(key) or "").strip() for key in ("artifact_id", "receipt_id", "transaction_id")):
            raise SystemExit("At least one of --artifact-id, --receipt-id, or --transaction-id is required.")
        append_decision(row)
        print(json.dumps(row, indent=2))
        return
    if args.command == "show":
        print(json.dumps(load_customer_recovery_decisions(), indent=2))
        return
    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()

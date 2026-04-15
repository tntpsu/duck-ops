#!/usr/bin/env python3
"""
Shared workflow control-state and receipt helpers for Duck Ops lanes.

The control plane is intentionally artifact-first so both Duck Ops and DuckAgent
can read the same JSON state without creating tight Python package coupling.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
WORKFLOW_CONTROL_STATE_DIR = STATE_DIR / "workflow_control"
WORKFLOW_RECEIPT_STATE_DIR = STATE_DIR / "workflow_receipts"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def slugify(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized or "workflow"


def workflow_state_path(workflow_id: str, *, state_dir: Path | None = None) -> Path:
    return (state_dir or WORKFLOW_CONTROL_STATE_DIR) / f"{slugify(workflow_id)}.json"


def workflow_receipt_dir(workflow_id: str, *, receipt_root: Path | None = None) -> Path:
    return (receipt_root or WORKFLOW_RECEIPT_STATE_DIR) / slugify(workflow_id)


def load_workflow_state(workflow_id: str, *, state_dir: Path | None = None) -> dict[str, Any]:
    payload = load_json(workflow_state_path(workflow_id, state_dir=state_dir), {})
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("workflow_id", workflow_id)
    payload.setdefault("history", [])
    payload.setdefault("metadata", {})
    payload.setdefault("receipts_count", 0)
    return payload


def list_workflow_states(*, state_dir: Path | None = None) -> list[dict[str, Any]]:
    root = state_dir or WORKFLOW_CONTROL_STATE_DIR
    if not root.exists():
        return []
    items: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.json")):
        payload = load_json(path, None)
        if not isinstance(payload, dict):
            continue
        payload["_path"] = str(path)
        items.append(payload)
    return items


def write_workflow_receipt(
    workflow_id: str,
    payload: dict[str, Any],
    *,
    receipt_id: str | None = None,
    receipt_root: Path | None = None,
) -> dict[str, Any]:
    recorded_at = str(payload.get("recorded_at") or now_iso())
    if not receipt_id:
        stamp = re.sub(r"[^0-9]+", "", recorded_at) or datetime.now().strftime("%Y%m%d%H%M%S")
        kind = slugify(str(payload.get("kind") or payload.get("state") or "receipt"))
        receipt_id = f"{stamp}-{kind}"
    receipt = dict(payload)
    receipt["workflow_id"] = workflow_id
    receipt["receipt_id"] = receipt_id
    receipt["recorded_at"] = recorded_at
    path = workflow_receipt_dir(workflow_id, receipt_root=receipt_root) / f"{slugify(receipt_id)}.json"
    write_json(path, receipt)
    return {
        "receipt_id": receipt_id,
        "path": str(path),
        "recorded_at": recorded_at,
    }


def record_workflow_transition(
    *,
    workflow_id: str,
    lane: str,
    display_label: str,
    state: str,
    state_reason: str | None = None,
    entity_id: str | None = None,
    run_id: str | None = None,
    requires_confirmation: bool | None = None,
    input_freshness: dict[str, Any] | None = None,
    last_side_effect: dict[str, Any] | None = None,
    last_verification: dict[str, Any] | None = None,
    next_action: str | None = None,
    metadata: dict[str, Any] | None = None,
    receipt_kind: str | None = None,
    receipt_payload: dict[str, Any] | None = None,
    history_summary: str | None = None,
    state_dir: Path | None = None,
    receipt_root: Path | None = None,
) -> dict[str, Any]:
    current = load_workflow_state(workflow_id, state_dir=state_dir)
    updated_at = now_iso()

    merged_metadata = dict(current.get("metadata") or {})
    if metadata:
        merged_metadata.update(metadata)

    next_state: dict[str, Any] = {
        "workflow_id": workflow_id,
        "lane": lane or str(current.get("lane") or "").strip(),
        "display_label": display_label or str(current.get("display_label") or workflow_id),
        "entity_id": entity_id if entity_id is not None else current.get("entity_id"),
        "run_id": run_id if run_id is not None else current.get("run_id"),
        "state": state,
        "state_reason": state_reason,
        "requires_confirmation": (
            bool(requires_confirmation)
            if requires_confirmation is not None
            else bool(current.get("requires_confirmation"))
        ),
        "input_freshness": input_freshness if input_freshness is not None else current.get("input_freshness"),
        "last_side_effect": last_side_effect if last_side_effect is not None else current.get("last_side_effect"),
        "last_verification": last_verification if last_verification is not None else current.get("last_verification"),
        "next_action": next_action if next_action is not None else current.get("next_action"),
        "updated_at": updated_at,
        "metadata": merged_metadata,
        "history": list(current.get("history") or []),
        "latest_receipt": current.get("latest_receipt"),
        "receipts_count": int(current.get("receipts_count") or 0),
    }

    receipt_meta: dict[str, Any] | None = None
    if receipt_kind is not None or receipt_payload is not None:
        receipt = {
            "workflow_id": workflow_id,
            "lane": next_state["lane"],
            "display_label": next_state["display_label"],
            "entity_id": next_state["entity_id"],
            "run_id": next_state["run_id"],
            "state": state,
            "state_reason": state_reason,
            "kind": receipt_kind or "transition",
            "requires_confirmation": next_state["requires_confirmation"],
            "payload": receipt_payload or {},
            "recorded_at": updated_at,
        }
        receipt_meta = write_workflow_receipt(
            workflow_id,
            receipt,
            receipt_root=receipt_root,
        )
        next_state["latest_receipt"] = receipt_meta
        next_state["receipts_count"] = int(next_state.get("receipts_count") or 0) + 1

    history_entry = {
        "state": state,
        "state_reason": state_reason,
        "at": updated_at,
        "summary": history_summary or str(state_reason or state).replace("_", " "),
        "receipt_id": (receipt_meta or {}).get("receipt_id"),
    }
    next_state["history"] = [history_entry, *(next_state.get("history") or [])][:12]
    write_json(workflow_state_path(workflow_id, state_dir=state_dir), next_state)
    return next_state

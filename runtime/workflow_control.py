#!/usr/bin/env python3
"""
Shared workflow control-state and receipt helpers for Duck Ops lanes.

The control plane is intentionally artifact-first so both Duck Ops and DuckAgent
can read the same JSON state without creating tight Python package coupling.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
WORKFLOW_CONTROL_STATE_DIR = STATE_DIR / "workflow_control"
WORKFLOW_RECEIPT_STATE_DIR = STATE_DIR / "workflow_receipts"


# 2026-06-09 source-level guard against production-state pollution.
# When DUCK_TEST_MODE=1, record_workflow_transition refuses to write
# instead of silently dropping into the production state dir. Pytest
# conftest.py files (both duck-ops and duckAgent) set this env var
# automatically so individual tests don't need to remember. Tests
# that legitimately want to observe the transition write should patch
# the function with a Mock to capture calls.
#
# Defense-in-depth for the same failure mode that produced the
# stranded meme-test-run.json receipt on 2026-06-06, which kept the
# weekly_lane_approval OS card lying RED for 58h. Even if a future
# conftest.py autouse fixture is bypassed (re-import, subprocess,
# direct call from a script), this guard refuses the write.
_TEST_MODE_REFUSAL_ENV = "DUCK_TEST_MODE"
# Frozen at import — this is the FACTORY-DEFAULT production path,
# never changed by monkeypatch. The guard compares the LIVE
# WORKFLOW_CONTROL_STATE_DIR against this; if conftest patched
# the live value to tmp, they differ and the write proceeds.
_FROZEN_PRODUCTION_WORKFLOW_CONTROL_PATH = WORKFLOW_CONTROL_STATE_DIR.resolve()


class TestModeRefusalError(RuntimeError):
    """Raised when record_workflow_transition would write to the
    factory-default PRODUCTION workflow_control directory while
    DUCK_TEST_MODE=1.

    Loud per [[feedback_swallowed_errors_lie]] — a silent no-op would
    mask test failures that depend on the transition having happened.
    Tests that legitimately patch WORKFLOW_CONTROL_STATE_DIR (or pass
    state_dir=tmp_path) are NOT blocked — only writes that would
    actually land in prod get refused."""

    pass


def _resolved_state_dir(state_dir: Path | None) -> Path:
    """Return the absolute path the write WOULD land in. Reads the
    LIVE module-level WORKFLOW_CONTROL_STATE_DIR (respects monkeypatch)
    when state_dir is None, mirroring state_file_path's resolution."""
    if state_dir is not None:
        return Path(state_dir).resolve()
    # IMPORTANT: read the module constant at call time so duck-ops's
    # conftest monkeypatch (which swaps WORKFLOW_CONTROL_STATE_DIR
    # for a tmp dir per test) takes effect here.
    return WORKFLOW_CONTROL_STATE_DIR.resolve()


def _refusing_test_mode_prod_write(state_dir: Path | None) -> bool:
    """True if DUCK_TEST_MODE=1 AND the resolved write path is the
    FROZEN factory-default production path. Path-patched tests
    (duck-ops conftest pattern) sail through; tests that bypass
    isolation get caught."""
    if str(os.environ.get(_TEST_MODE_REFUSAL_ENV) or "").strip() not in {"1", "true", "TRUE", "yes"}:
        return False
    return _resolved_state_dir(state_dir) == _FROZEN_PRODUCTION_WORKFLOW_CONTROL_PATH


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
    clear_next_action: bool = False,
    metadata: dict[str, Any] | None = None,
    receipt_kind: str | None = None,
    receipt_payload: dict[str, Any] | None = None,
    history_summary: str | None = None,
    state_dir: Path | None = None,
    receipt_root: Path | None = None,
) -> dict[str, Any]:
    # Source-level test pollution guard. Refuses writes that would
    # land in the PRODUCTION workflow_control dir while
    # DUCK_TEST_MODE=1. Path-patched tests (state_dir → tmp) sail
    # through; tests that forgot to patch get caught loud.
    if _refusing_test_mode_prod_write(state_dir):
        raise TestModeRefusalError(
            f"record_workflow_transition refused: DUCK_TEST_MODE=1 and "
            f"the write would land in production workflow_control. "
            f"workflow_id={workflow_id!r}. Either patch state_dir to a "
            f"tmp path (see duck-ops conftest.py for the pattern) OR "
            f"mock record_shared_workflow_transition (see duckAgent "
            f"conftest.py). Do NOT write production state from tests."
        )
    current = load_workflow_state(workflow_id, state_dir=state_dir)
    updated_at = now_iso()

    merged_metadata = dict(current.get("metadata") or {})
    if metadata:
        merged_metadata.update(metadata)

    resolved_next_action = None if clear_next_action else (next_action if next_action is not None else current.get("next_action"))

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
        "next_action": resolved_next_action,
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

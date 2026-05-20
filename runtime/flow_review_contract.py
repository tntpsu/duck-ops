"""Shared OpenClaw flow-review contract helpers.

The contract is intentionally small and UI-friendly: each flow reviewer should
explain blockers, warnings, checks, and the recommended operator action without
requiring the portal to understand flow-specific scoring internals.
"""

from __future__ import annotations

from typing import Any


FLOW_REVIEW_SCHEMA_VERSION = "duck.openclaw.flow_review.v1"
FLOW_REVIEW_CHECK_STATUSES = {"pass", "warn", "fail"}
DEFAULT_OPERATOR_ACTIONS = ["approve", "request_revision", "skip", "discard"]


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _clean_text_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        values = [values] if values else []
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean_text(value)
        if not text or text in seen:
            continue
        cleaned.append(text)
        seen.add(text)
    return cleaned


def flow_review_check(status: str, label: str, evidence: Any = "") -> dict[str, str]:
    normalized_status = _clean_text(status).lower()
    if normalized_status not in FLOW_REVIEW_CHECK_STATUSES:
        normalized_status = "warn"
    return {
        "status": normalized_status,
        "label": _clean_text(label) or "Quality check",
        "evidence": _clean_text(evidence),
    }


def build_flow_review_contract(
    *,
    reviewer: str,
    hard_blockers: list[str] | None = None,
    warnings: list[str] | None = None,
    checks: list[dict[str, Any]] | None = None,
    operator_summary: str = "",
    approval_summary: str = "",
    recommended_action: str = "",
    operator_actions: list[str] | None = None,
) -> dict[str, Any]:
    cleaned_blockers = _clean_text_list(hard_blockers or [])
    cleaned_warnings = _clean_text_list(warnings or [])
    cleaned_checks = [
        flow_review_check(str(check.get("status") or ""), str(check.get("label") or ""), check.get("evidence") or "")
        for check in checks or []
        if isinstance(check, dict)
    ]
    actions = _clean_text_list(operator_actions or DEFAULT_OPERATOR_ACTIONS)
    if not actions:
        actions = list(DEFAULT_OPERATOR_ACTIONS)

    action = _clean_text(recommended_action)
    if not action:
        action = "request_revision" if cleaned_blockers else "approve"

    summary = _clean_text(operator_summary)
    if not summary:
        if cleaned_blockers:
            summary = "OpenClaw found blocking issues that need revision."
        elif cleaned_warnings:
            summary = "OpenClaw found no blockers, only non-blocking warnings."
        else:
            summary = "OpenClaw found no blockers."

    approval = _clean_text(approval_summary) or "Approve this item for the configured DuckAgent lane."

    return {
        "schema_version": FLOW_REVIEW_SCHEMA_VERSION,
        "reviewer": _clean_text(reviewer) or "flow_review",
        "hard_blockers": cleaned_blockers,
        "warnings": cleaned_warnings,
        "checks": cleaned_checks,
        "operator_summary": summary,
        "approval_summary": approval,
        "recommended_action": action,
        "operator_actions": actions,
    }


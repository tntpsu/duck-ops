from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from governance_review_common import (
    DUCK_OPS_ROOT,
    ENGINEERING_GOVERNANCE_DIGEST_PATH,
    OUTPUT_OPERATOR_DIR,
    REPOS,
    age_hours,
    health_alerts,
    health_payload,
    load_json,
    now_local_iso,
    repo_status,
    write_json,
    write_markdown,
)


TECH_DEBT_STATE_PATH = DUCK_OPS_ROOT / "state" / "tech_debt_triage.json"
TECH_DEBT_OUTPUT_PATH = OUTPUT_OPERATOR_DIR / "tech_debt_triage.md"


def _fix_type_for_health_item(item: dict[str, Any]) -> str:
    reason = str(item.get("last_run_state") or "")
    if reason in {"stale_input", "coordination_missing"}:
        return "scheduler/safety hardening"
    if reason in {"execution_failed", "blocked_by_upstream"}:
        return "reliability hardening"
    if reason in {"alerts_pending"}:
        return "workflow cleanup"
    return "reliability hardening"


def _owner_skill_for_health_item(item: dict[str, Any]) -> str:
    reason = str(item.get("last_run_state") or "")
    if reason in {"stale_input", "coordination_missing"}:
        return "duck-reliability-review"
    if reason in {"execution_failed", "blocked_by_upstream"}:
        return "duck-tech-debt-triage"
    return "duck-change-planner"


def build_tech_debt_triage() -> dict[str, Any]:
    generated_at = now_local_iso()
    health = health_payload()
    alerts = health_alerts(limit=8)
    repo_statuses = [repo_status(name, path) for name, path in REPOS.items()]
    governance_digest = load_json(ENGINEERING_GOVERNANCE_DIGEST_PATH, {})

    items: list[dict[str, Any]] = []
    for item in alerts:
        reason = str(item.get("last_run_state") or "unknown")
        if reason in {
            "publication_lane_ready",
            "awaiting_review",
            "draft_ready",
            "operator_push_sent",
            "reply_preview_staged",
            "eligible_candidates_ready",
        }:
            continue
        items.append(
            {
                "priority": "P1" if item.get("status") == "bad" else "P2",
                "title": f"{item.get('label')} debt review",
                "symptom": f"{item.get('label')} is reporting {item.get('status')} with state `{reason}`.",
                "root_cause": f"Health currently reflects `{reason}` and may keep operator guidance stale or noisy until the lane is hardened.",
                "affected_layer": "duck-ops" if str(item.get("flow_id") or "").startswith("weekly_") else "shared",
                "operator_impact": "Operator trust drops when health keeps surfacing stale or weak workflow guidance.",
                "recommended_fix_type": _fix_type_for_health_item(item),
                "suggested_owner_skill": _owner_skill_for_health_item(item),
            }
        )

    failures = ((health.get("failures") or {}).get("artifact_failures") or []) if isinstance(health.get("failures"), dict) else []
    for failure in failures[:3]:
        if not isinstance(failure, dict):
            continue
        items.append(
            {
                "priority": "P2",
                "title": f"Artifact failure: {failure.get('run_id')}",
                "symptom": f"{failure.get('label') or 'artifact'} is marked failed.",
                "root_cause": str(failure.get("reason") or "Saved artifact failure needs follow-through."),
                "affected_layer": "duckAgent creative runtime",
                "operator_impact": "Old failed artifact receipts keep health degraded and can mislead follow-up work.",
                "recommended_fix_type": "reliability hardening",
                "suggested_owner_skill": "duck-tech-debt-triage",
            }
        )

    for repo in repo_statuses:
        modified = int(repo.get("modified_count") or 0)
        untracked = int(repo.get("untracked_count") or 0)
        if modified <= 0 and untracked <= 0:
            continue
        items.append(
            {
                "priority": "P2",
                "title": f"{repo.get('repo')} working tree cleanup",
                "symptom": f"{modified} modified and {untracked} untracked files are present.",
                "root_cause": "In-progress local changes reduce confidence in review findings and make follow-up maintenance harder to scope.",
                "affected_layer": str(repo.get("repo") or "repo"),
                "operator_impact": "Scheduled review lanes can end up reporting transient worktree noise instead of durable product issues.",
                "recommended_fix_type": "architecture cleanup",
                "suggested_owner_skill": "duck-change-planner",
            }
        )

    findings = governance_digest.get("findings") if isinstance(governance_digest, dict) else []
    if isinstance(findings, list):
        repeated_dirty = any(isinstance(item, dict) and "Working trees are not clean" in str(item.get("title") or "") for item in findings)
        if repeated_dirty and not any("working tree cleanup" in str(item.get("title") or "").lower() for item in items):
            items.append(
                {
                    "priority": "P2",
                    "title": "Review recurring governance cleanup noise",
                    "symptom": "Governance digest is repeatedly surfacing repo dirtiness.",
                    "root_cause": "Local changes are staying in flight long enough to become recurring ops noise.",
                    "affected_layer": "shared",
                    "operator_impact": "Useful governance findings can get buried under the same cleanup warning every morning.",
                    "recommended_fix_type": "workflow cleanup",
                    "suggested_owner_skill": "duck-tech-debt-triage",
                }
            )

    priority_rank = {"P1": 0, "P2": 1, "P3": 2}
    items.sort(key=lambda item: (priority_rank.get(str(item.get("priority") or "P3"), 9), str(item.get("title") or "")))

    payload = {
        "generated_at": generated_at,
        "item_count": len(items),
        "summary": {
            "headline": "Ranked cleanup work that should be fixed before more automation or feature sprawl.",
            "top_priority_count": sum(1 for item in items if item.get("priority") == "P1"),
        },
        "items": items[:12],
    }
    write_json(TECH_DEBT_STATE_PATH, payload)
    write_markdown(TECH_DEBT_OUTPUT_PATH, render_tech_debt_triage_markdown(payload))
    return payload


def render_tech_debt_triage_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Tech Debt Triage",
        "",
        f"- Generated: `{payload.get('generated_at') or ''}`",
        f"- Item count: `{payload.get('item_count') or 0}`",
        f"- Headline: {((payload.get('summary') or {}).get('headline') or '')}",
        "",
    ]
    items = payload.get("items") or []
    if not items:
        lines.append("No tech-debt items were identified.")
        lines.append("")
        return "\n".join(lines)

    for item in items:
        lines.extend(
            [
                f"## {item.get('priority')} · {item.get('title')}",
                "",
                f"- Symptom: {item.get('symptom')}",
                f"- Root cause: {item.get('root_cause')}",
                f"- Affected layer: `{item.get('affected_layer')}`",
                f"- Operator impact: {item.get('operator_impact')}",
                f"- Recommended fix type: `{item.get('recommended_fix_type')}`",
                f"- Suggested owner skill: `{item.get('suggested_owner_skill')}`",
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the tech-debt triage report.")
    parser.parse_args()
    payload = build_tech_debt_triage()
    print({"generated_at": payload.get("generated_at"), "item_count": payload.get("item_count")})


if __name__ == "__main__":
    main()

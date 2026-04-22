from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any

from governance_review_common import (
    DUCK_AGENT_ROOT,
    DUCK_OPS_ROOT,
    OUTPUT_OPERATOR_DIR,
    STATE_DIR,
    now_local_iso,
    write_json,
    write_markdown,
)


DOCUMENTATION_GOVERNANCE_STATE_PATH = STATE_DIR / "documentation_governance_review.json"
DOCUMENTATION_GOVERNANCE_OUTPUT_PATH = OUTPUT_OPERATOR_DIR / "documentation_governance_review.md"
LAUNCH_AGENTS_DIR = Path.home() / "Library" / "LaunchAgents"

CANONICAL_DOC_SPECS = [
    {
        "review_id": "master_roadmap",
        "label": "Duck Ops master roadmap",
        "path": DUCK_OPS_ROOT / "output" / "operator" / "master_roadmap.md",
        "required_fragments": [
            ("completed major work section", "## Completed Major Work"),
            ("active operational lanes section", "## Active Operational Lanes"),
            ("highest-value open work section", "## Highest-Value Open Work"),
            ("recommended next 3 steps section", "## Recommended Next 3 Steps"),
            ("documentation-governance skill mention", "duck-documentation-governance"),
        ],
    },
    {
        "review_id": "roadmap_execution_sequence",
        "label": "Roadmap execution sequence",
        "path": DUCK_AGENT_ROOT / "docs" / "current_system" / "ROADMAP_EXECUTION_SEQUENCE.md",
        "required_fragments": [
            ("phase 2 section", "## Phase 2: Observe-Only Engineering Reviews"),
            ("nightly tech-debt cadence", "- nightly: `duck-tech-debt-triage`"),
            ("weekly reliability cadence", "- weekly: `duck-reliability-review`"),
            ("weekly data-model cadence", "- weekly: `duck-data-model-governance`"),
            ("weekly documentation cadence", "- weekly: `duck-documentation-governance`"),
        ],
    },
    {
        "review_id": "agent_governance_policy",
        "label": "Agent governance policy",
        "path": DUCK_AGENT_ROOT / "docs" / "current_system" / "AGENT_GOVERNANCE_POLICY.md",
        "required_fragments": [
            ("required guard skills section", "## Required Guard Skills"),
            ("documentation-governance skill", "- `duck-documentation-governance`"),
            ("recommended cadence section", "## Recommended Cadence"),
            ("documentation-governance cadence", "- `duck-documentation-governance`: weekly"),
        ],
    },
    {
        "review_id": "bootstrap_runbook",
        "label": "Duck Ops bootstrap runbook",
        "path": DUCK_OPS_ROOT / "BOOTSTRAP.md",
        "required_fragments": [
            ("observe/review pattern section", "Current local morning observe/review pattern:"),
            ("standard observe-review wrapper", "run_duck_ops_observe_review.sh"),
            ("local launch agents guidance", "~/Library/LaunchAgents"),
        ],
    },
]

LAUNCH_AGENT_SPECS = [
    {
        "review_id": "tech_debt_triage_nightly_schedule",
        "label": "Nightly tech debt triage schedule",
        "path": LAUNCH_AGENTS_DIR / "com.philtullai.duckops.tech-debt-triage.nightly.plist",
    },
    {
        "review_id": "reliability_review_weekly_schedule",
        "label": "Weekly reliability review schedule",
        "path": LAUNCH_AGENTS_DIR / "com.philtullai.duckops.reliability-review.weekly.plist",
    },
    {
        "review_id": "data_model_governance_weekly_schedule",
        "label": "Weekly data-model governance schedule",
        "path": LAUNCH_AGENTS_DIR / "com.philtullai.duckops.data-model-governance.weekly.plist",
    },
    {
        "review_id": "documentation_governance_weekly_schedule",
        "label": "Weekly documentation governance schedule",
        "path": LAUNCH_AGENTS_DIR / "com.philtullai.duckops.documentation-governance.weekly.plist",
    },
    {
        "review_id": "engineering_governance_digest_morning_schedule",
        "label": "Morning engineering governance digest schedule",
        "path": LAUNCH_AGENTS_DIR / "com.philtullai.duckops.engineering-governance-digest.morning.plist",
    },
]


def _modified_at(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime).astimezone().isoformat()


def _canonical_doc_review(spec: dict[str, Any]) -> dict[str, Any]:
    path = Path(spec["path"])
    issues: list[str] = []
    recommended_updates: list[str] = []
    missing_fragments: list[str] = []
    exists = path.exists()

    if exists:
        text = path.read_text(encoding="utf-8")
        for label, fragment in list(spec.get("required_fragments") or []):
            if fragment not in text:
                missing_fragments.append(str(label))
        if missing_fragments:
            issues.append(
                "Required canonical coverage is missing: " + ", ".join(missing_fragments[:4]) + "."
            )
            recommended_updates.append(
                "Update the canonical document and any dependent guidance so the roadmap/policy/runbook truth stays aligned."
            )
    else:
        issues.append("Canonical documentation file is missing.")
        recommended_updates.append(
            "Restore the canonical document before relying on downstream roadmap, policy, or runbook guidance."
        )

    if not recommended_updates:
        recommended_updates.append("No immediate documentation update is required.")

    return {
        "review_id": spec.get("review_id"),
        "review_kind": "canonical_doc",
        "label": spec.get("label"),
        "path": str(path),
        "exists": exists,
        "modified_at": _modified_at(path),
        "missing_fragments": missing_fragments,
        "issues": issues,
        "recommended_updates": recommended_updates,
    }


def _launch_agent_review(spec: dict[str, Any]) -> dict[str, Any]:
    path = Path(spec["path"])
    exists = path.exists()
    issues: list[str] = []
    recommended_updates: list[str] = []

    if not exists:
        issues.append("Expected local LaunchAgent is missing, so this observe-only review loop is incomplete.")
        recommended_updates.append(
            "Install the missing local LaunchAgent and load it with launchctl so the observe-only review cadence actually runs."
        )
    else:
        recommended_updates.append("No immediate schedule update is required.")

    return {
        "review_id": spec.get("review_id"),
        "review_kind": "local_schedule",
        "label": spec.get("label"),
        "path": str(path),
        "exists": exists,
        "modified_at": _modified_at(path),
        "missing_fragments": [],
        "issues": issues,
        "recommended_updates": recommended_updates,
    }


def build_documentation_governance_review() -> dict[str, Any]:
    reviews = [_canonical_doc_review(spec) for spec in CANONICAL_DOC_SPECS]
    reviews.extend(_launch_agent_review(spec) for spec in LAUNCH_AGENT_SPECS)
    reviews.sort(key=lambda item: (0 if item.get("issues") else 1, str(item.get("label") or "")))

    payload = {
        "generated_at": now_local_iso(),
        "review_count": len(reviews),
        "issue_count": sum(1 for item in reviews if item.get("issues")),
        "summary": {
            "headline": "Review canonical docs and local observe-only schedules so roadmap/policy truth does not drift from the live operating system.",
            "canonical_doc_count": len(CANONICAL_DOC_SPECS),
            "schedule_check_count": len(LAUNCH_AGENT_SPECS),
        },
        "reviews": reviews,
    }
    write_json(DOCUMENTATION_GOVERNANCE_STATE_PATH, payload)
    write_markdown(DOCUMENTATION_GOVERNANCE_OUTPUT_PATH, render_documentation_governance_review_markdown(payload))
    return payload


def render_documentation_governance_review_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Documentation Governance Review",
        "",
        f"- Generated: `{payload.get('generated_at') or ''}`",
        f"- Reviews: `{payload.get('review_count') or 0}`",
        f"- Reviews with issues: `{payload.get('issue_count') or 0}`",
        "",
        str((payload.get("summary") or {}).get("headline") or ""),
        "",
        "## Canonical Docs",
        "",
    ]
    reviews = [item for item in list(payload.get("reviews") or []) if str(item.get("review_kind") or "") == "canonical_doc"]
    for review in reviews:
        lines.extend(
            [
                f"### {review.get('label')}",
                "",
                f"- Path: `{review.get('path')}`",
                f"- Exists: `{bool(review.get('exists'))}`",
                f"- Modified: `{review.get('modified_at')}`",
            ]
        )
        issues = list(review.get("issues") or [])
        if issues:
            lines.append("- Issues:")
            for issue in issues:
                lines.append(f"  - {issue}")
        else:
            lines.append("- Issues: none")
        updates = list(review.get("recommended_updates") or [])
        if updates:
            lines.append("- Recommended updates:")
            for item in updates:
                lines.append(f"  - {item}")
        lines.append("")

    lines.extend(["## Local Observe/Review Schedules", ""])
    schedules = [item for item in list(payload.get("reviews") or []) if str(item.get("review_kind") or "") == "local_schedule"]
    for review in schedules:
        lines.extend(
            [
                f"### {review.get('label')}",
                "",
                f"- Path: `{review.get('path')}`",
                f"- Exists: `{bool(review.get('exists'))}`",
                f"- Modified: `{review.get('modified_at')}`",
            ]
        )
        issues = list(review.get("issues") or [])
        if issues:
            lines.append("- Issues:")
            for issue in issues:
                lines.append(f"  - {issue}")
        else:
            lines.append("- Issues: none")
        updates = list(review.get("recommended_updates") or [])
        if updates:
            lines.append("- Recommended updates:")
            for item in updates:
                lines.append(f"  - {item}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the documentation-governance review.")
    parser.parse_args()
    payload = build_documentation_governance_review()
    print({"generated_at": payload.get("generated_at"), "issue_count": payload.get("issue_count")})


if __name__ == "__main__":
    main()

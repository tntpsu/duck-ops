from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from governance_review_common import (
    DUCK_OPS_ROOT,
    OUTPUT_OPERATOR_DIR,
    STATE_DIR,
    age_hours,
    load_json,
    now_local_iso,
    parse_iso,
    write_json,
    write_markdown,
)


DATA_MODEL_STATE_PATH = STATE_DIR / "data_model_governance_review.json"
DATA_MODEL_OUTPUT_PATH = OUTPUT_OPERATOR_DIR / "data_model_governance_review.md"

SURFACES = [
    {
        "surface": "engineering_governance_digest",
        "state_json": STATE_DIR / "engineering_governance_digest.json",
        "operator_json": None,
        "markdown": OUTPUT_OPERATOR_DIR / "engineering_governance_digest.md",
    },
    {
        "surface": "tech_debt_triage",
        "state_json": STATE_DIR / "tech_debt_triage.json",
        "operator_json": None,
        "markdown": OUTPUT_OPERATOR_DIR / "tech_debt_triage.md",
    },
    {
        "surface": "reliability_review",
        "state_json": STATE_DIR / "reliability_review.json",
        "operator_json": None,
        "markdown": OUTPUT_OPERATOR_DIR / "reliability_review.md",
    },
    {
        "surface": "data_model_governance_review",
        "state_json": STATE_DIR / "data_model_governance_review.json",
        "operator_json": None,
        "markdown": OUTPUT_OPERATOR_DIR / "data_model_governance_review.md",
    },
    {
        "surface": "documentation_governance_review",
        "state_json": STATE_DIR / "documentation_governance_review.json",
        "operator_json": None,
        "markdown": OUTPUT_OPERATOR_DIR / "documentation_governance_review.md",
    },
    {
        "surface": "weekly_sale_monitor",
        "state_json": STATE_DIR / "weekly_sale_monitor.json",
        "operator_json": OUTPUT_OPERATOR_DIR / "weekly_sale_monitor.json",
        "markdown": OUTPUT_OPERATOR_DIR / "weekly_sale_monitor.md",
    },
    {
        "surface": "weekly_campaign_coordination",
        "state_json": STATE_DIR / "weekly_campaign_coordination.json",
        "operator_json": OUTPUT_OPERATOR_DIR / "weekly_campaign_coordination.json",
        "markdown": OUTPUT_OPERATOR_DIR / "weekly_campaign_coordination.md",
    },
    {
        "surface": "nightly_action_summary",
        "state_json": STATE_DIR / "nightly_action_summary.json",
        "operator_json": OUTPUT_OPERATOR_DIR / "nightly_action_summary.json",
        "markdown": OUTPUT_OPERATOR_DIR / "nightly_action_summary.md",
    },
    {
        "surface": "business_operator_desk",
        "state_json": STATE_DIR / "business_operator_desk.json",
        "operator_json": OUTPUT_OPERATOR_DIR / "business_operator_desk.json",
        "markdown": OUTPUT_OPERATOR_DIR / "business_operator_desk.md",
    },
    {
        "surface": "repo_ci_status",
        "state_json": STATE_DIR / "repo_ci_status.json",
        "operator_json": OUTPUT_OPERATOR_DIR / "repo_ci_status.json",
        "markdown": OUTPUT_OPERATOR_DIR / "repo_ci_status.md",
    },
    {
        "surface": "shopify_seo_audit",
        "state_json": STATE_DIR / "shopify_seo_audit.json",
        "operator_json": None,
        "markdown": OUTPUT_OPERATOR_DIR / "shopify_seo_audit.md",
    },
    {
        "surface": "shopify_seo_outcomes",
        "state_json": STATE_DIR / "shopify_seo_outcomes.json",
        "operator_json": OUTPUT_OPERATOR_DIR / "shopify_seo_outcomes.json",
        "markdown": OUTPUT_OPERATOR_DIR / "shopify_seo_outcomes.md",
    },
    {
        "surface": "social_insights",
        "state_json": STATE_DIR / "social_performance_rollups.json",
        "operator_json": OUTPUT_OPERATOR_DIR / "social_insights.json",
        "markdown": OUTPUT_OPERATOR_DIR / "social_insights.md",
    },
    {
        "surface": "competitor_benchmark",
        "state_json": STATE_DIR / "social_competitor_benchmark.json",
        "operator_json": OUTPUT_OPERATOR_DIR / "competitor_benchmark.json",
        "markdown": OUTPUT_OPERATOR_DIR / "competitor_benchmark.md",
    },
    {
        "surface": "current_learnings",
        "state_json": STATE_DIR / "current_learnings.json",
        "operator_json": OUTPUT_OPERATOR_DIR / "current_learnings.json",
        "markdown": OUTPUT_OPERATOR_DIR / "current_learnings.md",
    },
    {
        "surface": "competitor_social_phase1",
        "state_json": STATE_DIR / "competitor_social_phase1.json",
        "operator_json": OUTPUT_OPERATOR_DIR / "competitor_social_phase1.json",
        "markdown": OUTPUT_OPERATOR_DIR / "competitor_social_phase1.md",
    },
    {
        "surface": "competitor_social_snapshots",
        "state_json": STATE_DIR / "competitor_social_snapshots.json",
        "operator_json": OUTPUT_OPERATOR_DIR / "competitor_social_snapshots.json",
        "markdown": OUTPUT_OPERATOR_DIR / "competitor_social_snapshots.md",
    },
    {
        "surface": "competitor_social_benchmark",
        "state_json": STATE_DIR / "competitor_social_benchmark.json",
        "operator_json": OUTPUT_OPERATOR_DIR / "competitor_social_benchmark.json",
        "markdown": OUTPUT_OPERATOR_DIR / "competitor_social_benchmark.md",
    },
    {
        "surface": "weekly_strategy_recommendation_packet",
        "state_json": STATE_DIR / "weekly_strategy_recommendation_packet.json",
        "operator_json": OUTPUT_OPERATOR_DIR / "weekly_strategy_recommendation_packet.json",
        "markdown": OUTPUT_OPERATOR_DIR / "weekly_strategy_recommendation_packet.md",
    },
]


def _generated_at(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    payload = load_json(path, {})
    if not isinstance(payload, dict):
        return None
    value = payload.get("generated_at")
    return str(value) if value else None


def _surface_review(surface: dict[str, Any]) -> dict[str, Any]:
    state_json = surface.get("state_json")
    operator_json = surface.get("operator_json")
    markdown = surface.get("markdown")
    name = str(surface.get("surface") or "surface")

    missing_paths = []
    for path in [state_json, operator_json, markdown]:
        if path is not None and not Path(path).exists():
            missing_paths.append(str(path))

    state_generated_at = _generated_at(state_json)
    operator_generated_at = _generated_at(operator_json)
    state_age = age_hours(state_generated_at)
    operator_age = age_hours(operator_generated_at)
    issues: list[str] = []

    if missing_paths:
        issues.append("Required contract files are missing.")
    if state_generated_at and operator_generated_at and state_generated_at != operator_generated_at:
        issues.append("State and operator JSON are out of sync.")
    if markdown is not None and Path(markdown).exists() and state_generated_at:
        state_dt = parse_iso(state_generated_at)
        if state_dt is not None:
            state_seconds = state_dt.timestamp()
            markdown_seconds = Path(markdown).stat().st_mtime
            if markdown_seconds + 7200 < state_seconds:
                issues.append("Markdown view is older than the state payload by more than two hours.")
    if state_age is not None and state_age >= 72:
        issues.append("State payload is stale enough that downstream readers may be drifting.")

    return {
        "surface": name,
        "state_json": str(state_json) if state_json is not None else None,
        "operator_json": str(operator_json) if operator_json is not None else None,
        "markdown": str(markdown) if markdown is not None else None,
        "state_generated_at": state_generated_at,
        "operator_generated_at": operator_generated_at,
        "state_age_hours": state_age,
        "operator_age_hours": operator_age,
        "issues": issues,
    }


def build_data_model_governance_review() -> dict[str, Any]:
    reviews = [_surface_review(surface) for surface in SURFACES]
    reviews.sort(key=lambda item: (0 if item.get("issues") else 1, str(item.get("surface") or "")))
    payload = {
        "generated_at": now_local_iso(),
        "surface_count": len(reviews),
        "issue_count": sum(1 for item in reviews if item.get("issues")),
        "summary": {
            "headline": "Cross-check stable state/output contracts so operator readers do not drift from writers.",
        },
        "surfaces": reviews,
    }
    write_json(DATA_MODEL_STATE_PATH, payload)
    write_markdown(DATA_MODEL_OUTPUT_PATH, render_data_model_governance_markdown(payload))
    return payload


def render_data_model_governance_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Data Model Governance Review",
        "",
        f"- Generated: `{payload.get('generated_at') or ''}`",
        f"- Surfaces: `{payload.get('surface_count') or 0}`",
        f"- Surfaces with issues: `{payload.get('issue_count') or 0}`",
        "",
        str((payload.get("summary") or {}).get("headline") or ""),
        "",
    ]
    for surface in payload.get("surfaces") or []:
        lines.extend(
            [
                f"## {surface.get('surface')}",
                "",
                f"- State age: `{surface.get('state_age_hours')}` hour(s)",
                f"- State generated: `{surface.get('state_generated_at')}`",
            ]
        )
        if surface.get("operator_json") is not None:
            lines.append(f"- Operator JSON generated: `{surface.get('operator_generated_at')}`")
        if surface.get("issues"):
            lines.append("- Issues:")
            for issue in surface.get("issues") or []:
                lines.append(f"  - {issue}")
        else:
            lines.append("- Issues: none")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the data-model governance review.")
    parser.parse_args()
    payload = build_data_model_governance_review()
    print({"generated_at": payload.get("generated_at"), "issue_count": payload.get("issue_count")})


if __name__ == "__main__":
    main()

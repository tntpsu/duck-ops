from __future__ import annotations

import argparse
from typing import Any

from governance_review_common import DUCK_OPS_ROOT, OUTPUT_OPERATOR_DIR, health_alerts, now_local_iso, write_json, write_markdown


RELIABILITY_STATE_PATH = DUCK_OPS_ROOT / "state" / "reliability_review.json"
RELIABILITY_OUTPUT_PATH = OUTPUT_OPERATOR_DIR / "reliability_review.md"


def _gaps_for_item(item: dict[str, Any]) -> list[str]:
    status = str(item.get("status") or "")
    reason = str(item.get("last_run_state") or "")
    gaps: list[str] = []
    if reason == "stale_input":
        gaps.append("Freshness guarantees are not holding, so downstream guidance can drift from live state.")
    if reason == "blocked_by_upstream":
        gaps.append("The lane is too dependent on upstream work without enough graceful fallback.")
    if reason == "execution_failed":
        gaps.append("Happy-path execution is not enough here; the lane needs clearer failure recovery.")
    if reason == "publication_lane_ready":
        gaps.append("This is an operator-routing warning, not a failure, so it may need clearer health labeling.")
    if status == "bad" and not gaps:
        gaps.append("Health marks this lane as bad, so the operator surface is already losing trust.")
    return gaps or ["No specific reliability gap was inferred from the current state label."]


def _required_fixes_for_item(item: dict[str, Any]) -> list[str]:
    reason = str(item.get("last_run_state") or "")
    fixes: list[str] = []
    if reason == "stale_input":
        fixes.append("Define a refresh or self-heal path so stale source inputs do not linger for days.")
    if reason == "blocked_by_upstream":
        fixes.append("Make the upstream dependency explicit in health and provide a bounded retry or skip path.")
    if reason == "execution_failed":
        fixes.append("Add a clearer retry receipt or rollback instruction so operators are not guessing.")
    if reason == "publication_lane_ready":
        fixes.append("Demote this to info-only if it is not truly a degraded state.")
    if not fixes:
        fixes.append("Review lock, timeout, freshness expectations, and artifact proof before promoting this lane.")
    return fixes


def _strengths_for_item(item: dict[str, Any]) -> list[str]:
    strengths: list[str] = []
    if item.get("last_run_at"):
        strengths.append("The lane has a recent run timestamp, so there is at least some run proof.")
    if item.get("last_run_path"):
        strengths.append("A saved artifact path exists for the last observed run.")
    if item.get("success_rate_label"):
        strengths.append("Health is surfacing an operator-readable summary instead of silent failure.")
    return strengths or ["This lane is at least represented in health, which is better than silent drift."]


def build_reliability_review() -> dict[str, Any]:
    reviews: list[dict[str, Any]] = []
    for item in health_alerts(limit=6):
        status = str(item.get("status") or "")
        reason = str(item.get("last_run_state") or "")
        if reason in {"awaiting_review", "reply_preview_staged", "eligible_candidates_ready", "draft_ready", "operator_push_sent"}:
            continue
        reviews.append(
            {
                "lane": item.get("flow_id"),
                "label": item.get("label"),
                "status": status,
                "last_run_state": reason,
                "lane_summary": f"{item.get('label')} is currently `{status}` with last run state `{reason}`.",
                "reliability_strengths": _strengths_for_item(item),
                "reliability_gaps": _gaps_for_item(item),
                "required_rollout_fixes": _required_fixes_for_item(item),
                "recommended_tier": "Tier 0" if status == "bad" else "Tier 0/Tier 1",
                "go_decision": "no-go" if status == "bad" else "conditional-go",
            }
        )

    payload = {
        "generated_at": now_local_iso(),
        "review_count": len(reviews),
        "summary": {
            "headline": "Observe-only reliability review for the lanes most likely to erode operator trust.",
            "bad_count": sum(1 for item in reviews if item.get("status") == "bad"),
            "warn_count": sum(1 for item in reviews if item.get("status") == "warn"),
        },
        "reviews": reviews,
    }
    write_json(RELIABILITY_STATE_PATH, payload)
    write_markdown(RELIABILITY_OUTPUT_PATH, render_reliability_review_markdown(payload))
    return payload


def render_reliability_review_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Reliability Review",
        "",
        f"- Generated: `{payload.get('generated_at') or ''}`",
        f"- Reviews: `{payload.get('review_count') or 0}`",
        "",
        str((payload.get("summary") or {}).get("headline") or ""),
        "",
    ]
    reviews = payload.get("reviews") or []
    if not reviews:
        lines.append("No degraded lanes were identified for reliability review.")
        lines.append("")
        return "\n".join(lines)

    for review in reviews:
        lines.extend(
            [
                f"## {review.get('label')}",
                "",
                f"- Status: `{review.get('status')}`",
                f"- Last state: `{review.get('last_run_state')}`",
                f"- Go decision: `{review.get('go_decision')}`",
                f"- Recommended tier: `{review.get('recommended_tier')}`",
                f"- Summary: {review.get('lane_summary')}",
                "- Strengths:",
            ]
        )
        for item in review.get("reliability_strengths") or []:
            lines.append(f"  - {item}")
        lines.append("- Gaps:")
        for item in review.get("reliability_gaps") or []:
            lines.append(f"  - {item}")
        lines.append("- Required rollout fixes:")
        for item in review.get("required_rollout_fixes") or []:
            lines.append(f"  - {item}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the observe-only reliability review.")
    parser.parse_args()
    payload = build_reliability_review()
    print({"generated_at": payload.get("generated_at"), "review_count": payload.get("review_count")})


if __name__ == "__main__":
    main()

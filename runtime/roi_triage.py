#!/usr/bin/env python3
"""Rank the next DuckAgent/Duck Ops work by expected return on effort."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from nightly_action_summary import load_master_roadmap_focus


DUCK_OPS_ROOT = Path("/Users/philtullai/ai-agents/duck-ops")
DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"
STATE_PATH = DUCK_OPS_ROOT / "state" / "roi_triage.json"
OUTPUT_MD_PATH = DUCK_OPS_ROOT / "output" / "operator" / "roi_triage.md"
GOVERNANCE_PATH = DUCK_OPS_ROOT / "state" / "engineering_governance_digest.json"
SCHEDULER_HEALTH_PATH = DUCK_OPS_ROOT / "state" / "scheduler_health.json"
REPO_CI_PATH = DUCK_OPS_ROOT / "state" / "repo_ci_status.json"
CURRENT_LEARNINGS_PATH = DUCK_OPS_ROOT / "state" / "current_learnings.json"
TECH_DEBT_TRIAGE_PATH = DUCK_OPS_ROOT / "state" / "tech_debt_triage.json"
RELIABILITY_REVIEW_PATH = DUCK_OPS_ROOT / "state" / "reliability_review.json"
DATA_MODEL_GOVERNANCE_REVIEW_PATH = DUCK_OPS_ROOT / "state" / "data_model_governance_review.json"
DOCUMENTATION_GOVERNANCE_REVIEW_PATH = DUCK_OPS_ROOT / "state" / "documentation_governance_review.json"
CREATIVE_POLICIES_PATH = DUCK_AGENT_ROOT / "creative_agent" / "runtime" / "src" / "duck_creative_agent" / "creative_policies.py"
CREATIVE_TASKS_PATH = DUCK_AGENT_ROOT / "creative_agent" / "runtime" / "src" / "duck_creative_agent" / "tasks.py"
CREATIVE_VIEWER_DATA_PATH = DUCK_AGENT_ROOT / "creative_agent" / "runtime" / "src" / "duck_creative_agent" / "viewer_data.py"
CREATIVE_VIEWER_PATH = DUCK_AGENT_ROOT / "creative_agent" / "runtime" / "src" / "duck_creative_agent" / "viewer.py"
DESIGN_BRIEF_QUEUE_DOC_PATH = DUCK_AGENT_ROOT / "docs" / "current_system" / "DESIGN_BRIEF_QUEUE_PLAN.md"
BUSINESS_OPERATOR_DESK_PATH = DUCK_OPS_ROOT / "runtime" / "business_operator_desk.py"
README_PATH = DUCK_OPS_ROOT / "README.md"

GOVERNANCE_SOURCE_PATHS = {
    "tech_debt_triage": TECH_DEBT_TRIAGE_PATH,
    "reliability_review": RELIABILITY_REVIEW_PATH,
    "data_model_governance_review": DATA_MODEL_GOVERNANCE_REVIEW_PATH,
    "documentation_governance_review": DOCUMENTATION_GOVERNANCE_REVIEW_PATH,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed.astimezone(timezone.utc)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _payload_generated_at(path: Path) -> datetime | None:
    payload = _load_json(path)
    return _parse_iso(payload.get("generated_at"))


def _file_contains(path: Path, markers: list[str]) -> bool:
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return False
    return all(marker in text for marker in markers)


def _score(*, impact: int, urgency: int, confidence: int, effort: int) -> float:
    return round((impact * 0.40) + (urgency * 0.25) + (confidence * 0.20) + (effort * 0.15), 2)


def _candidate(
    *,
    candidate_id: str,
    title: str,
    why_now: str,
    recommended_next_slice: str,
    impact: int,
    urgency: int,
    confidence: int,
    effort: int,
    owner_skill: str,
    constraints: list[str] | None = None,
    source: str = "curated",
    lifecycle_status: str = "open",
    lifecycle_reason: str | None = None,
    evidence: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "title": title,
        "why_now": why_now,
        "recommended_next_slice": recommended_next_slice,
        "score_breakdown": {
            "impact": impact,
            "urgency": urgency,
            "confidence": confidence,
            "effort": effort,
            "roi_score": _score(impact=impact, urgency=urgency, confidence=confidence, effort=effort),
        },
        "owner_skill": owner_skill,
        "constraints": list(constraints or []),
        "source": source,
        "lifecycle_status": lifecycle_status,
        "lifecycle_reason": lifecycle_reason,
        "evidence": list(evidence or []),
    }


def _source_review_newer_than_digest(source: str, digest_generated_at: datetime | None) -> bool:
    if digest_generated_at is None:
        return False
    source_path = GOVERNANCE_SOURCE_PATHS.get(source)
    if source_path is None:
        return False
    source_generated_at = _payload_generated_at(source_path)
    return bool(source_generated_at and source_generated_at > digest_generated_at)


def _completion_evidence(candidate_id: str) -> dict[str, Any] | None:
    if candidate_id == "semantic-visual-qa" and _file_contains(
        CREATIVE_POLICIES_PATH,
        ["def run_semantic_visual_qa", "semantic_visual_review", "DUCK_SEMANTIC_VISUAL_QA"],
    ):
        return {
            "lifecycle_status": "completed",
            "lifecycle_reason": "Semantic visual QA is implemented in the creative policy gate and now surfaces a semantic_visual_review check.",
            "evidence": [str(CREATIVE_POLICIES_PATH)],
        }

    if candidate_id == "design-brief-source-hygiene" and _file_contains(
        CREATIVE_TASKS_PATH,
        ["def _filter_public_design_brief_input", "filtered_private_signals"],
    ):
        return {
            "lifecycle_status": "completed",
            "lifecycle_reason": "Design brief queue input now filters private custom-build signals before prompt/fallback construction.",
            "evidence": [str(CREATIVE_TASKS_PATH), str(DESIGN_BRIEF_QUEUE_DOC_PATH)],
        }

    if candidate_id == "maintenance-freshness-desk" and _file_contains(
        BUSINESS_OPERATOR_DESK_PATH,
        ["def _load_maintenance_freshness_surface", "maintenance_freshness_surface"],
    ):
        return {
            "lifecycle_status": "completed",
            "lifecycle_reason": "Business Desk now has a maintenance freshness surface with generated-at age, stale flags, and top actions.",
            "evidence": [str(BUSINESS_OPERATOR_DESK_PATH), str(README_PATH)],
        }

    if candidate_id == "concept-to-print-gated-workflow" and _file_contains(
        CREATIVE_VIEWER_DATA_PATH,
        ["def record_run_concept_image_reply", "concept_image_reply_receipt.json"],
    ) and _file_contains(CREATIVE_VIEWER_PATH, ["characterPrintPipeline"]):
        return {
            "lifecycle_status": "active_followup",
            "lifecycle_reason": "The approval gates exist; the remaining high-ROI slice is proving one real character concept through the gated path.",
            "evidence": [str(CREATIVE_VIEWER_DATA_PATH), str(CREATIVE_VIEWER_PATH)],
        }

    return None


def _apply_lifecycle(candidate: dict[str, Any]) -> dict[str, Any]:
    candidate_id = str(candidate.get("candidate_id") or "")
    evidence = _completion_evidence(candidate_id)
    if not evidence:
        return candidate

    updated = dict(candidate)
    updated.update(evidence)
    if evidence.get("lifecycle_status") == "active_followup" and candidate_id == "concept-to-print-gated-workflow":
        updated.update(
            {
                "title": "First character concept-to-print pilot",
                "why_now": (
                    "The viewer gates are in place; the next return comes from proving one operator-approved "
                    "character image can move cleanly toward paid 3D handoff and print review."
                ),
                "recommended_next_slice": (
                    "Run one Little Lulu-style character duck through concept-image approval, 3D handoff readiness, "
                    "paint-to-print conversion prep, and Bambu review without auto-spending credits."
                ),
                "score_breakdown": {
                    "impact": 5,
                    "urgency": 3,
                    "confidence": 4,
                    "effort": 4,
                    "roi_score": _score(impact=5, urgency=3, confidence=4, effort=4),
                },
            }
        )
    return updated


def _governance_candidates() -> list[dict[str, Any]]:
    payload = _load_json(GOVERNANCE_PATH)
    digest_generated_at = _parse_iso(payload.get("generated_at"))
    recommendations = payload.get("review_recommendations") if isinstance(payload.get("review_recommendations"), list) else []
    items: list[dict[str, Any]] = []
    priority_map = {"P1": 5, "P2": 4, "P3": 3}
    for idx, item in enumerate(recommendations[:4], start=1):
        if not isinstance(item, dict):
            continue
        priority = str(item.get("priority") or "P3").upper()
        urgency = priority_map.get(priority, 3)
        title = str(item.get("title") or "Governance recommendation").strip()
        summary = str(item.get("summary") or item.get("next_action") or "").strip()
        owner_skill = str(item.get("suggested_owner_skill") or "duck-reliability-review").strip()
        source = str(item.get("source") or "engineering_governance_digest").strip()
        lifecycle_status = "open"
        lifecycle_reason = None
        if _source_review_newer_than_digest(source, digest_generated_at):
            lifecycle_status = "stale_source_superseded"
            lifecycle_reason = (
                f"Skipped because `{source}` was regenerated after the engineering governance digest; "
                "the digest recommendation may no longer reflect the current source review."
            )
        candidate = _candidate(
            candidate_id=f"governance-{idx}",
            title=title,
            why_now=summary or f"Engineering governance marked this as {priority}.",
            recommended_next_slice=str(item.get("next_action") or "Turn this governance recommendation into a bounded implementation slice.").strip(),
            impact=4 if priority == "P1" else 3,
            urgency=urgency,
            confidence=4,
            effort=3,
            owner_skill=owner_skill,
            constraints=["Respect observe-only mode unless the recommendation explicitly calls for code changes."],
            source="engineering_governance_digest",
            lifecycle_status=lifecycle_status,
            lifecycle_reason=lifecycle_reason,
            evidence=[str(GOVERNANCE_PATH), str(GOVERNANCE_SOURCE_PATHS.get(source))] if source in GOVERNANCE_SOURCE_PATHS else [str(GOVERNANCE_PATH)],
        )
        candidate["source_review"] = source
        items.append(candidate)
    return items


def _governance_digest_refresh_candidate(suppressed_items: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not suppressed_items:
        return None
    sources = sorted({str(item.get("source_review") or item.get("source") or "source") for item in suppressed_items})
    return _candidate(
        candidate_id="engineering-governance-digest-refresh",
        title="Refresh engineering governance digest",
        why_now="ROI suppressed stale governance recommendations because their source review files are newer than the digest.",
        recommended_next_slice="Run `python3 runtime/engineering_governance_digest.py` after the observe-only reviews so ROI and Business Desk rank current findings.",
        impact=3,
        urgency=3,
        confidence=5,
        effort=5,
        owner_skill="duck-data-model-governance",
        constraints=[f"Stale source(s): {', '.join(sources)}."],
        source="roi_triage_guard",
    )


def _scheduler_candidate() -> dict[str, Any] | None:
    payload = _load_json(SCHEDULER_HEALTH_PATH)
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    attention = int(summary.get("attention_count") or summary.get("bad_count") or 0)
    if attention <= 0:
        return None
    items = [item for item in list(payload.get("items") or []) if isinstance(item, dict)]
    top = next((item for item in items if item.get("attention_needed")), items[0] if items else {})
    return _candidate(
        candidate_id="scheduler-health-hardening",
        title="Scheduler stuck-run and missed-run hardening",
        why_now=str(top.get("summary") or "Scheduler health has jobs needing attention.").strip(),
        recommended_next_slice=str(top.get("recommended_action") or "Fix the top scheduler attention item and keep stale/hung receipts visible in Business Desk.").strip(),
        impact=4,
        urgency=5,
        confidence=4,
        effort=3,
        owner_skill="duck-reliability-review",
        constraints=["Do not add new launchd jobs until the current attention item is understood."],
        source="scheduler_health",
    )


def _repo_ci_candidate() -> dict[str, Any] | None:
    payload = _load_json(REPO_CI_PATH)
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    attention = int(summary.get("attention_count") or summary.get("failing_count") or 0)
    if attention <= 0:
        return None
    return _candidate(
        candidate_id="repo-ci-recovery",
        title="Repo CI recovery",
        why_now=str(payload.get("headline") or "At least one repo CI mirror needs attention.").strip(),
        recommended_next_slice=str(payload.get("recommended_action") or "Fix the failing or stale CI mirror before stacking more automation changes.").strip(),
        impact=4,
        urgency=4,
        confidence=4,
        effort=4,
        owner_skill="github:gh-fix-ci",
        constraints=["Keep test-only credential placeholders out of CI-required paths."],
        source="repo_ci_status",
    )


def _learning_candidate() -> dict[str, Any] | None:
    payload = _load_json(CURRENT_LEARNINGS_PATH)
    notifier = payload.get("change_notifier") if isinstance(payload.get("change_notifier"), dict) else {}
    material_count = int(notifier.get("material_change_count") or 0)
    if material_count <= 0:
        return None
    return _candidate(
        candidate_id="learning-change-review",
        title="Review material learning changes",
        why_now=str(notifier.get("headline") or f"{material_count} material learning change(s) need review.").strip(),
        recommended_next_slice=str(notifier.get("recommended_action") or "Fold material learning changes into the weekly strategy packet and next design-brief queue.").strip(),
        impact=3,
        urgency=3,
        confidence=4,
        effort=4,
        owner_skill="duck-social-insights",
        constraints=["Treat competitor signals as inspiration, not copy instructions."],
        source="current_learnings",
    )


def _roadmap_candidates() -> list[dict[str, Any]]:
    focus = load_master_roadmap_focus()
    if not isinstance(focus, dict) or not focus.get("available"):
        return []
    items: list[dict[str, Any]] = []
    for idx, step in enumerate(list(focus.get("next_steps") or [])[:4], start=1):
        if not isinstance(step, dict):
            continue
        title = str(step.get("title") or "").strip()
        if not title:
            continue
        summary = str(step.get("summary") or "").strip()
        items.append(
            _candidate(
                candidate_id=f"roadmap-{idx}",
                title=title,
                why_now=summary or "Canonical roadmap has this in the next-step list.",
                recommended_next_slice=f"Turn `{title}` into the smallest implementation slice that updates code, tests, and docs.",
                impact=4,
                urgency=3,
                confidence=3,
                effort=3,
                owner_skill="duck-change-planner",
                constraints=["Use architecture/data/documentation governance if the slice crosses repo or state boundaries."],
                source="master_roadmap",
            )
        )
    return items


def _curated_candidates() -> list[dict[str, Any]]:
    return [
        _candidate(
            candidate_id="concept-to-print-gated-workflow",
            title="Character concept-to-print gated workflow",
            why_now="This connects the new character duck concept path to the paid 3D handoff, paint-to-print conversion, and Bambu review without accidentally spending credits or skipping approval.",
            recommended_next_slice="Keep the viewer gates current and make the first real concept-to-print run produce a single clear next action at each step.",
            impact=5,
            urgency=3,
            confidence=4,
            effort=3,
            owner_skill="duck-architecture-guard",
            constraints=["Paid 3D AI Studio steps require explicit operator clicks.", "Use semantic visual QA before build handoff."],
        ),
        _candidate(
            candidate_id="semantic-visual-qa",
            title="Semantic visual QA for generated ducks",
            why_now="Character prompts can drift into wrong outfits, extra props, or non-duck silhouettes, and first-pass quality needs to be trusted before approval emails get easy.",
            recommended_next_slice="Run the vision-backed checker on generated concept images and surface fail/needs-review in the viewer and approval packet.",
            impact=5,
            urgency=4,
            confidence=4,
            effort=4,
            owner_skill="duck-reliability-review",
            constraints=["If the model key is unavailable, mark manual review instead of passing silently."],
        ),
        _candidate(
            candidate_id="design-brief-source-hygiene",
            title="Design brief source hygiene",
            why_now="Private custom-order ideas should not become public product concepts by accident.",
            recommended_next_slice="Keep custom-build signals in their customer lane unless explicitly allowlisted for public concept generation.",
            impact=4,
            urgency=4,
            confidence=5,
            effort=5,
            owner_skill="duck-data-model-governance",
            constraints=["Public concept queue must not see customer names, dates, or personalization requirements."],
        ),
        _candidate(
            candidate_id="maintenance-freshness-desk",
            title="Maintenance freshness in Business Desk",
            why_now="The morning email should tell us whether OS maintenance, scheduler health, governance, CI, and documentation surfaces are fresh enough to trust.",
            recommended_next_slice="Surface generated-at age, stale flags, and top action for each maintenance surface in the desk.",
            impact=4,
            urgency=3,
            confidence=4,
            effort=4,
            owner_skill="duck-documentation-governance",
            constraints=["Freshness warnings should match designed cadence, not aspirational cadence."],
        ),
    ]


def build_roi_triage(*, write_outputs: bool = True) -> dict[str, Any]:
    generated_at = _now_iso()
    candidates: list[dict[str, Any]] = []
    for item in [
        _scheduler_candidate(),
        _repo_ci_candidate(),
        _learning_candidate(),
    ]:
        if item:
            candidates.append(item)
    candidates.extend(_governance_candidates())
    candidates.extend(_roadmap_candidates())
    candidates.extend(_curated_candidates())

    annotated_candidates = [_apply_lifecycle(candidate) for candidate in candidates]
    completed_items = [
        item for item in annotated_candidates if str(item.get("lifecycle_status") or "open") == "completed"
    ]
    suppressed_items = [
        item for item in annotated_candidates if str(item.get("lifecycle_status") or "open") == "stale_source_superseded"
    ]
    active_candidates = [
        item
        for item in annotated_candidates
        if str(item.get("lifecycle_status") or "open") not in {"completed", "stale_source_superseded"}
    ]
    refresh_candidate = _governance_digest_refresh_candidate(suppressed_items)
    if refresh_candidate:
        active_candidates.append(refresh_candidate)

    deduped: dict[str, dict[str, Any]] = {}
    for candidate in active_candidates:
        key = str(candidate.get("title") or candidate.get("candidate_id") or "").lower()
        current = deduped.get(key)
        if not current or float(candidate["score_breakdown"]["roi_score"]) > float(current["score_breakdown"]["roi_score"]):
            deduped[key] = candidate

    ranked = sorted(
        deduped.values(),
        key=lambda item: (
            float((item.get("score_breakdown") or {}).get("roi_score") or 0),
            int((item.get("score_breakdown") or {}).get("impact") or 0),
            int((item.get("score_breakdown") or {}).get("urgency") or 0),
        ),
        reverse=True,
    )
    for idx, item in enumerate(ranked, start=1):
        item["rank"] = idx

    payload = {
        "generated_at": generated_at,
        "surface_version": 2,
        "summary": {
            "candidate_count": len(ranked),
            "completed_count": len(completed_items),
            "stale_recommendation_count": len(suppressed_items),
            "top_score": (ranked[0].get("score_breakdown") or {}).get("roi_score") if ranked else 0,
            "top_title": ranked[0].get("title") if ranked else None,
            "headline": (
                f"Top ROI slice: {ranked[0].get('title')}."
                if ranked
                else "No ROI candidates are available yet."
            ),
            "recommended_action": (
                ranked[0].get("recommended_next_slice")
                if ranked
                else "Generate the roadmap and operator surfaces before relying on ROI triage."
            ),
        },
        "recommendations": ranked[:8],
        "recently_completed": completed_items[:8],
        "suppressed_recommendations": suppressed_items[:8],
    }
    if write_outputs:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_MD_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        OUTPUT_MD_PATH.write_text(render_roi_triage_markdown(payload), encoding="utf-8")
    return payload


def render_roi_triage_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    lines = [
        "# Duck ROI Triage",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Candidate count: `{summary.get('candidate_count', 0)}`",
        f"- Recently completed / filtered: `{summary.get('completed_count', 0)}`",
        f"- Stale governance signals suppressed: `{summary.get('stale_recommendation_count', 0)}`",
        f"- Top score: `{summary.get('top_score', 0)}`",
        f"- Headline: {summary.get('headline') or 'No headline.'}",
        f"- Recommended action: {summary.get('recommended_action') or 'No action available.'}",
        "",
        "## Top Recommendations",
        "",
    ]
    for item in list(payload.get("recommendations") or [])[:8]:
        score = item.get("score_breakdown") if isinstance(item.get("score_breakdown"), dict) else {}
        lines.append(
            f"{item.get('rank')}. {item.get('title')} | score `{score.get('roi_score')}` | skill `{item.get('owner_skill')}`"
        )
        lines.append(f"   - Why now: {item.get('why_now')}")
        lines.append(f"   - Next slice: {item.get('recommended_next_slice')}")
        if item.get("constraints"):
            lines.append(f"   - Constraints: {'; '.join(str(value) for value in item.get('constraints') or [])}")

    completed_items = [item for item in list(payload.get("recently_completed") or []) if isinstance(item, dict)]
    if completed_items:
        lines.extend(["", "## Recently Completed / Filtered", ""])
        for item in completed_items[:8]:
            lines.append(f"- {item.get('title')} | `{item.get('candidate_id')}`")
            if item.get("lifecycle_reason"):
                lines.append(f"  - Reason: {item.get('lifecycle_reason')}")

    suppressed_items = [item for item in list(payload.get("suppressed_recommendations") or []) if isinstance(item, dict)]
    if suppressed_items:
        lines.extend(["", "## Suppressed Stale Signals", ""])
        for item in suppressed_items[:8]:
            lines.append(f"- {item.get('title')} | source review `{item.get('source_review') or item.get('source')}`")
            if item.get("lifecycle_reason"):
                lines.append(f"  - Reason: {item.get('lifecycle_reason')}")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    payload = build_roi_triage(write_outputs=True)
    print(json.dumps({"output_path": str(STATE_PATH), "markdown_path": str(OUTPUT_MD_PATH), "top_title": (payload.get("summary") or {}).get("top_title")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

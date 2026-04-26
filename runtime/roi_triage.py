#!/usr/bin/env python3
"""Rank the next DuckAgent/Duck Ops work by expected return on effort."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from nightly_action_summary import load_master_roadmap_focus


DUCK_OPS_ROOT = Path("/Users/philtullai/ai-agents/duck-ops")
STATE_PATH = DUCK_OPS_ROOT / "state" / "roi_triage.json"
OUTPUT_MD_PATH = DUCK_OPS_ROOT / "output" / "operator" / "roi_triage.md"
GOVERNANCE_PATH = DUCK_OPS_ROOT / "state" / "engineering_governance_digest.json"
SCHEDULER_HEALTH_PATH = DUCK_OPS_ROOT / "state" / "scheduler_health.json"
REPO_CI_PATH = DUCK_OPS_ROOT / "state" / "repo_ci_status.json"
CURRENT_LEARNINGS_PATH = DUCK_OPS_ROOT / "state" / "current_learnings.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


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
    }


def _governance_candidates() -> list[dict[str, Any]]:
    payload = _load_json(GOVERNANCE_PATH)
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
        items.append(
            _candidate(
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
            )
        )
    return items


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

    deduped: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
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
        "surface_version": 1,
        "summary": {
            "candidate_count": len(ranked),
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
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    payload = build_roi_triage(write_outputs=True)
    print(json.dumps({"output_path": str(STATE_PATH), "markdown_path": str(OUTPUT_MD_PATH), "top_title": (payload.get("summary") or {}).get("top_title")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

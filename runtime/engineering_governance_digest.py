from __future__ import annotations

import argparse
import html as html_lib
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from governance_review_common import age_hours


DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"
DUCK_AGENT_VENV_PY = DUCK_AGENT_ROOT / ".venv" / "bin" / "python3"

STATE_DIR = DUCK_OPS_ROOT / "state"
DIGEST_STATE_PATH = STATE_DIR / "engineering_governance_digest.json"
DIGEST_OUTPUT_PATH = DUCK_OPS_ROOT / "output" / "operator" / "engineering_governance_digest.md"
SYSTEM_HEALTH_PATH = DUCK_AGENT_ROOT / "creative_agent" / "runtime" / "output" / "operator" / "system_health.json"
ROADMAP_PATH = DUCK_OPS_ROOT / "output" / "operator" / "master_roadmap.md"
EXECUTION_SEQUENCE_PATH = DUCK_AGENT_ROOT / "docs" / "current_system" / "ROADMAP_EXECUTION_SEQUENCE.md"
POLICY_PATH = DUCK_AGENT_ROOT / "docs" / "current_system" / "AGENT_GOVERNANCE_POLICY.md"
SKILLS_ROOT = Path("/Users/philtullai/.codex/skills")
TECH_DEBT_TRIAGE_PATH = STATE_DIR / "tech_debt_triage.json"
RELIABILITY_REVIEW_PATH = STATE_DIR / "reliability_review.json"
DATA_MODEL_GOVERNANCE_REVIEW_PATH = STATE_DIR / "data_model_governance_review.json"
COMPETITOR_SOCIAL_SNAPSHOTS_PATH = STATE_DIR / "competitor_social_snapshots.json"
BUSINESS_OPERATOR_DESK_PATH = STATE_DIR / "business_operator_desk.json"
BUSINESS_OPERATOR_DESK_MD_PATH = DUCK_OPS_ROOT / "output" / "operator" / "business_operator_desk.md"
CURRENT_LEARNINGS_PATH = STATE_DIR / "current_learnings.json"
CURRENT_LEARNINGS_MD_PATH = DUCK_OPS_ROOT / "output" / "operator" / "current_learnings.md"

REQUIRED_SKILLS = [
    "duck-change-planner",
    "duck-architecture-guard",
    "duck-reliability-review",
    "duck-data-model-governance",
    "duck-automation-safety",
    "duck-tech-debt-triage",
    "duck-social-insights",
    "duck-competitor-benchmark",
]

REPOS = {
    "duckAgent": DUCK_AGENT_ROOT,
    "duck-ops": DUCK_OPS_ROOT,
}


def _ensure_duckagent_python() -> None:
    if os.environ.get("ENGINEERING_GOVERNANCE_VENV_READY") == "1":
        return
    current_python = Path(sys.executable).resolve()
    if current_python == DUCK_AGENT_VENV_PY or not DUCK_AGENT_VENV_PY.exists():
        return
    os.environ["ENGINEERING_GOVERNANCE_VENV_READY"] = "1"
    os.execv(str(DUCK_AGENT_VENV_PY), [str(DUCK_AGENT_VENV_PY), str(Path(__file__).resolve()), *sys.argv[1:]])


def _ensure_duckagent_imports():
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        load_dotenv = None

    env_path = DUCK_AGENT_ROOT / ".env"
    if load_dotenv is not None:
        load_dotenv(env_path, override=False)

    sys.path.insert(0, str(DUCK_AGENT_ROOT))
    from helpers.email_helper import send_email  # type: ignore
    from helpers.report_email_helper import render_report_email, report_badge, report_card, report_link  # type: ignore

    return send_email, render_report_email, report_badge, report_card, report_link


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _trim_text(value: Any, limit: int = 160) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _repo_status(repo_name: str, repo_path: Path) -> dict[str, Any]:
    try:
        result = subprocess.run(
            ["git", "status", "--short"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:
        return {
            "repo": repo_name,
            "path": str(repo_path),
            "modified_count": 0,
            "untracked_count": 0,
            "status_lines": [],
            "error": str(exc),
        }

    lines = [line.rstrip() for line in (result.stdout or "").splitlines() if line.strip()]
    modified = sum(1 for line in lines if not line.startswith("??"))
    untracked = sum(1 for line in lines if line.startswith("??"))
    return {
        "repo": repo_name,
        "path": str(repo_path),
        "modified_count": modified,
        "untracked_count": untracked,
        "status_lines": lines[:20],
    }


def _skill_statuses() -> list[dict[str, Any]]:
    out = []
    for name in REQUIRED_SKILLS:
        path = SKILLS_ROOT / name
        out.append(
            {
                "name": name,
                "path": str(path),
                "present": path.exists(),
            }
        )
    return out


def _top_health_findings(limit: int = 5) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    health = _load_json(SYSTEM_HEALTH_PATH, {})
    if not isinstance(health, dict):
        return {}, []

    severity_rank = {"bad": 0, "warn": 1, "ok": 2}
    flow_health = health.get("flow_health") if isinstance(health.get("flow_health"), list) else []
    items = [item for item in flow_health if isinstance(item, dict) and item.get("status") in {"bad", "warn"}]
    items.sort(
        key=lambda item: (
            severity_rank.get(str(item.get("status") or "ok"), 9),
            str(item.get("last_run_at") or ""),
            str(item.get("label") or ""),
        )
    )
    simplified = [
        {
            "flow_id": item.get("flow_id"),
            "label": item.get("label"),
            "status": item.get("status"),
            "last_run_state": item.get("last_run_state"),
            "last_run_at": item.get("last_run_at"),
            "success_rate_label": item.get("success_rate_label"),
        }
        for item in items[:limit]
    ]
    return (
        {
            "overall_status": health.get("overall_status"),
            "overall_label": health.get("overall_label"),
            "overall_summary": health.get("overall_summary"),
        },
        simplified,
    )


def _observe_review_statuses() -> list[dict[str, Any]]:
    surfaces = [
        ("tech_debt_triage", TECH_DEBT_TRIAGE_PATH, "items"),
        ("reliability_review", RELIABILITY_REVIEW_PATH, "reviews"),
        ("data_model_governance_review", DATA_MODEL_GOVERNANCE_REVIEW_PATH, "surfaces"),
    ]
    items: list[dict[str, Any]] = []
    for name, path, key in surfaces:
        payload = _load_json(path, {})
        present = path.exists() and isinstance(payload, dict)
        data = payload.get(key) if isinstance(payload, dict) else []
        item_count = len(data) if isinstance(data, list) else 0
        top_label = None
        if isinstance(data, list) and data:
            first = data[0]
            if isinstance(first, dict):
                top_label = first.get("title") or first.get("label") or first.get("surface")
            else:
                top_label = str(first)
        items.append(
            {
                "name": name,
                "path": str(path),
                "present": present,
                "generated_at": payload.get("generated_at") if isinstance(payload, dict) else None,
                "item_count": item_count,
                "top_label": top_label,
            }
        )
    return items


def _business_desk_highlights() -> dict[str, Any]:
    payload = _load_json(BUSINESS_OPERATOR_DESK_PATH, {})
    if not isinstance(payload, dict) or not payload:
        return {
            "available": False,
            "path": str(BUSINESS_OPERATOR_DESK_MD_PATH),
            "counts": [],
            "next_actions": [],
        }

    counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
    next_actions = list(payload.get("next_actions") or [])
    count_specs = [
        ("customer_attention_items", "Customer attention"),
        ("etsy_browser_threads", "Etsy threads"),
        ("custom_build_candidates", "Custom builds"),
        ("orders_to_pack_units", "Pack tonight"),
        ("review_queue_items", "Creative reviews"),
        ("governance_top_priority_items", "Governance P1"),
        ("strategy_ready_slots", "Social slots ready"),
    ]
    count_rows = [
        {"key": key, "label": label, "count": int(counts.get(key) or 0)}
        for key, label in count_specs
        if int(counts.get(key) or 0) > 0
    ]
    if not count_rows:
        count_rows = [{"key": "all_clear", "label": "Immediate desk alerts", "count": 0}]

    highlights = []
    for item in next_actions[:3]:
        if not isinstance(item, dict):
            continue
        highlights.append(
            {
                "lane": str(item.get("lane") or "desk"),
                "title": _trim_text(item.get("title") or "Next action", 80),
                "summary": _trim_text(item.get("summary") or item.get("title") or "Follow up from the operator desk.", 140),
                "command": _trim_text(item.get("command") or "", 120),
                "secondary_command": _trim_text(item.get("secondary_command") or "", 120),
            }
        )

    return {
        "available": True,
        "path": str(BUSINESS_OPERATOR_DESK_MD_PATH),
        "generated_at": payload.get("generated_at"),
        "counts": count_rows[:5],
        "next_actions": highlights,
    }


def _learning_change_highlights() -> dict[str, Any]:
    payload = _load_json(CURRENT_LEARNINGS_PATH, {})
    if not isinstance(payload, dict) or not payload:
        return {
            "available": False,
            "path": str(CURRENT_LEARNINGS_MD_PATH),
            "items": [],
        }

    notifier = payload.get("change_notifier") if isinstance(payload.get("change_notifier"), dict) else {}
    items = []
    for item in list(notifier.get("items") or [])[:4]:
        if not isinstance(item, dict):
            continue
        items.append(
            {
                "source": str(item.get("source") or "learning"),
                "urgency": str(item.get("urgency") or "info"),
                "headline": _trim_text(item.get("headline") or "Learning change", 150),
                "detail": _trim_text(item.get("detail") or "", 180) or None,
            }
        )

    return {
        "available": True,
        "path": str(CURRENT_LEARNINGS_MD_PATH),
        "generated_at": payload.get("generated_at"),
        "headline": notifier.get("headline"),
        "change_count": int(notifier.get("change_count") or len(payload.get("changes_since_previous") or [])),
        "material_change_count": int(notifier.get("material_change_count") or 0),
        "attention_change_count": int(notifier.get("attention_change_count") or 0),
        "recommended_action": notifier.get("recommended_action"),
        "items": items,
    }


def _priority_rank(value: Any) -> int:
    return {"P1": 0, "P2": 1, "P3": 2}.get(str(value or "P3").upper(), 9)


def _review_recommendations() -> list[dict[str, Any]]:
    recommendations: list[dict[str, Any]] = []

    tech_debt_payload = _load_json(TECH_DEBT_TRIAGE_PATH, {})
    tech_debt_items = tech_debt_payload.get("items") if isinstance(tech_debt_payload, dict) else []
    if isinstance(tech_debt_items, list):
        for item in tech_debt_items:
            if not isinstance(item, dict):
                continue
            recommendations.append(
                {
                    "priority": str(item.get("priority") or "P3"),
                    "source": "tech_debt_triage",
                    "mode": "propose-only",
                    "title": str(item.get("title") or "Tech debt item"),
                    "summary": str(item.get("symptom") or item.get("root_cause") or "Tech debt follow-through is recommended.").strip(),
                    "next_action": (
                        f"{item.get('recommended_fix_type') or 'Cleanup'} via "
                        f"{item.get('suggested_owner_skill') or 'duck-tech-debt-triage'}."
                    ),
                    "recommendation_type": str(item.get("recommended_fix_type") or "cleanup"),
                    "suggested_owner_skill": str(item.get("suggested_owner_skill") or "duck-tech-debt-triage"),
                }
            )

    reliability_payload = _load_json(RELIABILITY_REVIEW_PATH, {})
    reliability_reviews = reliability_payload.get("reviews") if isinstance(reliability_payload, dict) else []
    if isinstance(reliability_reviews, list):
        for review in reliability_reviews:
            if not isinstance(review, dict):
                continue
            go_decision = str(review.get("go_decision") or "").strip().lower()
            status = str(review.get("status") or "").strip().lower()
            required_fixes = list(review.get("required_rollout_fixes") or [])
            recommendations.append(
                {
                    "priority": "P1" if go_decision == "no-go" or status == "bad" else "P2",
                    "source": "reliability_review",
                    "mode": "observe-only" if go_decision == "no-go" else "propose-only",
                    "title": f"{review.get('label') or 'Lane'} rollout guardrail",
                    "summary": str(review.get("lane_summary") or "Reliability review flagged this lane for follow-through.").strip(),
                    "next_action": str(required_fixes[0] if required_fixes else "Review lock, timeout, freshness, and artifact proof before promoting this lane."),
                    "recommendation_type": "reliability hardening",
                    "suggested_owner_skill": "duck-reliability-review",
                }
            )

    data_model_payload = _load_json(DATA_MODEL_GOVERNANCE_REVIEW_PATH, {})
    data_model_surfaces = data_model_payload.get("surfaces") if isinstance(data_model_payload, dict) else []
    if isinstance(data_model_surfaces, list):
        for surface in data_model_surfaces:
            if not isinstance(surface, dict):
                continue
            issues = [str(item).strip() for item in list(surface.get("issues") or []) if str(item).strip()]
            if not issues:
                continue
            issue_text = issues[0]
            issue_lower = " ".join(issues).lower()
            priority = "P1" if ("missing" in issue_lower or "out of sync" in issue_lower) else "P2"
            surface_name = str(surface.get("surface") or "surface")
            recommendations.append(
                {
                    "priority": priority,
                    "source": "data_model_governance_review",
                    "mode": "propose-only",
                    "title": f"{surface_name} contract drift risk",
                    "summary": issue_text,
                    "next_action": f"Refresh the canonical writer/reader contract for {surface_name} and rebuild the operator-facing artifact set.",
                    "recommendation_type": "data model cleanup",
                    "suggested_owner_skill": "duck-data-model-governance",
                }
            )

    deduped: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()
    for item in sorted(recommendations, key=lambda row: (_priority_rank(row.get("priority")), str(row.get("title") or ""))):
        key = (str(item.get("source") or ""), str(item.get("title") or ""))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(item)
    return deduped[:8]


def _competitor_social_snapshot_status() -> dict[str, Any]:
    if not COMPETITOR_SOCIAL_SNAPSHOTS_PATH.exists():
        return {
            "name": "competitor_social_snapshots",
            "path": str(COMPETITOR_SOCIAL_SNAPSHOTS_PATH),
            "present": False,
            "generated_at": None,
            "age_hours": None,
            "status_key": "hard_failing",
            "status_label": "HARD FAILING",
            "item_count": 0,
            "top_label": "Snapshot artifact is missing.",
            "summary": "No competitor snapshot state file exists yet.",
        }

    try:
        payload = _load_json(COMPETITOR_SOCIAL_SNAPSHOTS_PATH, {})
    except Exception as exc:
        return {
            "name": "competitor_social_snapshots",
            "path": str(COMPETITOR_SOCIAL_SNAPSHOTS_PATH),
            "present": False,
            "generated_at": None,
            "age_hours": None,
            "status_key": "hard_failing",
            "status_label": "HARD FAILING",
            "item_count": 0,
            "top_label": "Snapshot artifact could not be parsed.",
            "summary": f"Competitor snapshot state could not be parsed: {exc}",
        }

    if not isinstance(payload, dict):
        return {
            "name": "competitor_social_snapshots",
            "path": str(COMPETITOR_SOCIAL_SNAPSHOTS_PATH),
            "present": False,
            "generated_at": None,
            "age_hours": None,
            "status_key": "hard_failing",
            "status_label": "HARD FAILING",
            "item_count": 0,
            "top_label": "Snapshot artifact is missing or malformed.",
            "summary": "No usable competitor snapshot state was found.",
        }

    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    generated_at = payload.get("generated_at")
    live_count = int(summary.get("live_account_count") or 0) if isinstance(summary, dict) else 0
    cached_count = int(summary.get("cached_account_count") or 0) if isinstance(summary, dict) else 0
    failed_count = int(summary.get("failed_account_count") or 0) if isinstance(summary, dict) else 0
    degraded_count = int(summary.get("degraded_account_count") or 0) if isinstance(summary, dict) else 0
    scheduled_skip_count = int(summary.get("scheduled_skip_account_count") or summary.get("scheduled_skip_count") or 0) if isinstance(summary, dict) else 0
    profile_only_backoff_count = int(summary.get("profile_only_backoff_account_count") or 0) if isinstance(summary, dict) else 0
    live_canary_limited_count = int(summary.get("live_canary_limited_account_count") or 0) if isinstance(summary, dict) else 0
    live_canary_target_count = int(summary.get("live_canary_target_count") or 0) if isinstance(summary, dict) else 0
    max_live_canary_targets = int(summary.get("max_live_canary_targets") or 0) if isinstance(summary, dict) else 0
    active_refresh_target_count = int(summary.get("active_refresh_target_count") or 0) if isinstance(summary, dict) else 0
    collected_count = int(summary.get("collected_account_count") or 0) if isinstance(summary, dict) else 0
    freshness_hours = age_hours(generated_at)

    if failed_count > 0:
        status_key = "hard_failing"
        status_label = "HARD FAILING"
        top_label = f"{failed_count} hard failure(s)"
        summary_text = (
            f"{failed_count} hard failure(s) recorded; {cached_count} cached fallback account(s) and {live_count} live account(s) in the latest snapshot."
            if cached_count or live_count
            else f"{failed_count} hard failure(s) recorded and the collector could not provide a live snapshot."
        )
    elif cached_count > 0 or degraded_count > 0 or profile_only_backoff_count > 0:
        if degraded_count > 0:
            status_key = "degraded_cached_fallback"
            status_label = "DEGRADED CACHED FALLBACK"
            top_label = f"{cached_count} cached fallback account(s)"
            summary_text = (
                f"Collector is alive but using cached fallback for {cached_count} account(s) with {live_count} live account(s) and {degraded_count} degraded fetches."
            )
            if profile_only_backoff_count > 0:
                summary_text += f" `{profile_only_backoff_count}` profile-only account(s) are also on backoff."
            if live_canary_limited_count > 0:
                summary_text += f" `{live_canary_limited_count}` account(s) were also deferred by the live canary limit."
        elif profile_only_backoff_count > 0:
            status_key = "degraded_cached_fallback"
            status_label = "DEGRADED CACHED FALLBACK"
            top_label = f"{profile_only_backoff_count} profile-only backoff account(s)"
            summary_text = (
                f"Collector is stable, but {profile_only_backoff_count} profile-only account(s) are on cooldown because recent public refreshes "
                f"could not recover post timelines; {scheduled_skip_count} account(s) reused cache and {active_refresh_target_count} account(s) were targeted live."
            )
        elif live_canary_limited_count > 0:
            status_key = "healthy_staggered"
            status_label = "HEALTHY STAGGERED"
            top_label = f"{live_canary_target_count} live canary target(s)"
            summary_text = (
                f"Collector is enforcing the live canary policy: {live_canary_target_count} canary target(s) ran live while "
                f"{live_canary_limited_count} account(s) reused cache because the cap is `{max_live_canary_targets}`."
            )
        elif scheduled_skip_count > 0:
            status_key = "healthy_staggered"
            status_label = "HEALTHY STAGGERED"
            top_label = f"{scheduled_skip_count} scheduled skip account(s)"
            summary_text = (
                f"Collector is rotating refreshes intentionally: {scheduled_skip_count} account(s) reused recent cache while {active_refresh_target_count} account(s) were targeted live this run."
            )
        else:
            status_key = "degraded_cached_fallback"
            status_label = "DEGRADED CACHED FALLBACK"
            top_label = f"{cached_count} cached fallback account(s)"
            summary_text = f"Collector is alive but using cached fallback for {cached_count} account(s)."
    else:
        status_key = "healthy_live"
        status_label = "HEALTHY LIVE"
        top_label = f"{live_count} live account(s)"
        summary_text = f"Collector is live with {live_count} account(s) and no cached fallback or hard failures."

    if freshness_hours is None:
        freshness_note = "freshness unknown"
    else:
        freshness_note = f"generated {freshness_hours}h ago"

    return {
        "name": "competitor_social_snapshots",
        "path": str(COMPETITOR_SOCIAL_SNAPSHOTS_PATH),
        "present": True,
        "generated_at": generated_at,
        "age_hours": freshness_hours,
        "status_key": status_key,
        "status_label": status_label,
        "item_count": collected_count,
        "top_label": top_label,
        "summary": f"{summary_text} {freshness_note}.",
        "live_account_count": live_count,
        "cached_account_count": cached_count,
        "failed_account_count": failed_count,
        "degraded_account_count": degraded_count,
        "scheduled_skip_account_count": scheduled_skip_count,
        "profile_only_backoff_account_count": profile_only_backoff_count,
        "live_canary_limited_account_count": live_canary_limited_count,
        "live_canary_target_count": live_canary_target_count,
        "max_live_canary_targets": max_live_canary_targets,
        "active_refresh_target_count": active_refresh_target_count,
    }


def _build_findings(
    skill_statuses: list[dict[str, Any]],
    repo_statuses: list[dict[str, Any]],
    health_summary: dict[str, Any],
    health_findings: list[dict[str, Any]],
    competitor_snapshot_status: dict[str, Any],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    missing_skills = [skill["name"] for skill in skill_statuses if not skill.get("present")]
    if missing_skills:
        findings.append(
            {
                "priority": "P1",
                "kind": "propose",
                "title": "Build remaining governance and learning skills",
                "summary": f"Missing skills: {', '.join(missing_skills[:6])}" + ("..." if len(missing_skills) > 6 else ""),
                "next_action": "Create the remaining planned skills before scheduling more autonomous review work.",
            }
        )

    if str(health_summary.get("overall_status") or "") == "bad":
        findings.append(
            {
                "priority": "P1",
                "kind": "observe",
                "title": "Operator health is currently degraded",
                "summary": str(health_summary.get("overall_summary") or "Health reports the operator surface as degraded."),
                "next_action": "Review the top bad/warn flows before adding new production-facing automation.",
            }
        )

    for item in health_findings[:3]:
        findings.append(
            {
                "priority": "P1" if item.get("status") == "bad" else "P2",
                "kind": "observe",
                "title": f"{item.get('label')} is {item.get('status')}",
                "summary": f"{item.get('last_run_state') or 'unknown state'} | {item.get('success_rate_label') or 'no rate label'}",
                "next_action": "Review the lane and decide whether it needs a reliability pass, stale-input fix, or clearer operator handling.",
            }
        )

    if competitor_snapshot_status.get("status_key") == "hard_failing":
        findings.append(
            {
                "priority": "P1",
                "kind": "observe",
                "title": "Competitor snapshot collection is hard failing",
                "summary": str(competitor_snapshot_status.get("summary") or "The collector is missing live truth and has no usable fallback."),
                "next_action": "Fix the observe-only collector before treating competitor social data as current.",
            }
        )
    elif competitor_snapshot_status.get("status_key") == "degraded_cached_fallback":
        findings.append(
            {
                "priority": "P2",
                "kind": "observe",
                "title": "Competitor snapshot collection is using cached fallback",
                "summary": str(competitor_snapshot_status.get("summary") or "The collector is serving cached fallback data."),
                "next_action": "Re-run the observe-only collector to restore live competitor snapshot coverage.",
            }
        )
    elif competitor_snapshot_status.get("status_key") == "healthy_staggered":
        findings.append(
            {
                "priority": "P3",
                "kind": "observe",
                "title": "Competitor snapshot collection is on staggered cadence",
                "summary": str(competitor_snapshot_status.get("summary") or "The collector intentionally reused recent cache to reduce rate-limit pressure."),
                "next_action": "Watch whether the staggered cadence restores more live account pulls over the next few runs.",
            }
        )

    dirty_repos = [repo for repo in repo_statuses if repo.get("modified_count") or repo.get("untracked_count")]
    if dirty_repos:
        summary = ", ".join(
            f"{repo['repo']}: {repo.get('modified_count', 0)} modified / {repo.get('untracked_count', 0)} untracked"
            for repo in dirty_repos
        )
        findings.append(
            {
                "priority": "P2",
                "kind": "observe",
                "title": "Working trees are not clean",
                "summary": summary,
                "next_action": "Sort in-progress changes before allowing maintenance reviews to assume repo truth is stable.",
            }
        )

    if not findings:
        findings.append(
            {
                "priority": "P3",
                "kind": "observe",
                "title": "No major governance findings",
                "summary": "Current artifacts do not show critical governance blockers.",
                "next_action": "Continue with the next roadmap phase using the planning stack.",
            }
        )
    return findings[:8]


def build_engineering_governance_digest() -> dict[str, Any]:
    generated_at = datetime.now().astimezone().isoformat()
    skill_statuses = _skill_statuses()
    missing_skills = [skill["name"] for skill in skill_statuses if not skill.get("present")]
    repo_statuses = [_repo_status(name, path) for name, path in REPOS.items()]
    health_summary, health_findings = _top_health_findings()
    competitor_snapshot_status = _competitor_social_snapshot_status()
    business_desk_highlights = _business_desk_highlights()
    learning_change_highlights = _learning_change_highlights()
    observe_review_statuses = _observe_review_statuses()
    observe_review_statuses.append(competitor_snapshot_status)
    review_recommendations = _review_recommendations()
    findings = _build_findings(skill_statuses, repo_statuses, health_summary, health_findings, competitor_snapshot_status)
    if missing_skills:
        next_step = "Build the remaining governance/learning skills and keep scheduled skill-backed work at Tier 0/Tier 1 until the digest lane is trusted."
    else:
        next_step = "Use the planning/review skill stack to start Phase 2 observe-only reviews and the social-performance data foundation without granting unattended mutation power."

    payload = {
        "generated_at": generated_at,
        "phase_focus": "Phase 1: governance control layer (complete enough to use)",
        "roadmap_path": str(ROADMAP_PATH),
        "execution_sequence_path": str(EXECUTION_SEQUENCE_PATH),
        "policy_path": str(POLICY_PATH),
        "skill_statuses": skill_statuses,
        "repo_statuses": repo_statuses,
        "observe_review_statuses": observe_review_statuses,
        "review_recommendations": review_recommendations,
        "review_recommendation_summary": {
            "count": len(review_recommendations),
            "top_priority_count": sum(1 for item in review_recommendations if str(item.get("priority") or "").upper() == "P1"),
        },
        "business_desk_highlights": business_desk_highlights,
        "learning_change_highlights": learning_change_highlights,
        "health_summary": health_summary,
        "health_findings": health_findings,
        "findings": findings,
        "next_step": next_step,
    }
    _write_json(DIGEST_STATE_PATH, payload)
    DIGEST_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    DIGEST_OUTPUT_PATH.write_text(render_engineering_governance_markdown(payload), encoding="utf-8")
    return payload


def render_engineering_governance_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Engineering Governance Digest",
        "",
        f"- Generated: `{payload.get('generated_at') or ''}`",
        f"- Phase focus: `{payload.get('phase_focus') or ''}`",
        f"- Roadmap: {payload.get('roadmap_path')}",
        f"- Execution sequence: {payload.get('execution_sequence_path')}",
        f"- Governance policy: {payload.get('policy_path')}",
        "",
        "## Top Findings",
        "",
    ]
    for finding in payload.get("findings") or []:
        lines.append(f"### {finding.get('priority')} · {finding.get('title')}")
        lines.append(f"- Type: `{finding.get('kind')}`")
        lines.append(f"- Summary: {finding.get('summary')}")
        lines.append(f"- Next action: {finding.get('next_action')}")
        lines.append("")

    lines.extend(["## Recommended Follow-Through", ""])
    recommendations = payload.get("review_recommendations") or []
    if not recommendations:
        lines.append("No observe-only review recommendations are available yet.")
        lines.append("")
    else:
        for item in recommendations:
            lines.append(f"### {item.get('priority')} · {item.get('title')}")
            lines.append(f"- Source: `{item.get('source')}`")
            lines.append(f"- Mode: `{item.get('mode')}`")
            lines.append(f"- Type: `{item.get('recommendation_type')}`")
            lines.append(f"- Owner skill: `{item.get('suggested_owner_skill')}`")
            lines.append(f"- Summary: {item.get('summary')}")
            lines.append(f"- Next action: {item.get('next_action')}")
            lines.append("")

    lines.extend(["## Business Desk Highlights", ""])
    business_desk = payload.get("business_desk_highlights") or {}
    if not business_desk.get("available"):
        lines.append("Business desk highlights are not available yet.")
        lines.append("")
    else:
        lines.append(f"- Desk page: `{business_desk.get('path')}`")
        lines.append(f"- Desk generated: `{business_desk.get('generated_at') or 'unknown'}`")
        for item in business_desk.get("counts") or []:
            lines.append(f"- {item.get('label')}: `{item.get('count')}`")
        actions = business_desk.get("next_actions") or []
        if actions:
            lines.append("- Do next:")
            for item in actions:
                lines.append(
                    f"  - {item.get('lane') or 'desk'} | {item.get('title') or 'Next action'} | {item.get('summary') or ''}"
                )
                if item.get("command"):
                    lines.append(f"    Command: {item.get('command')}")
        lines.append("")

    lines.extend(["## Learning Change Highlights", ""])
    learning_changes = payload.get("learning_change_highlights") or {}
    if not learning_changes.get("available"):
        lines.append("Learning change highlights are not available yet.")
        lines.append("")
    else:
        lines.append(f"- Learnings page: `{learning_changes.get('path')}`")
        lines.append(f"- Learnings generated: `{learning_changes.get('generated_at') or 'unknown'}`")
        lines.append(f"- Total changes observed: `{learning_changes.get('change_count') or 0}`")
        lines.append(f"- Material changes: `{learning_changes.get('material_change_count') or 0}`")
        lines.append(f"- Attention-level changes: `{learning_changes.get('attention_change_count') or 0}`")
        if learning_changes.get("headline"):
            lines.append(f"- Notifier: {learning_changes.get('headline')}")
        if learning_changes.get("recommended_action"):
            lines.append(f"- Review command: `{learning_changes.get('recommended_action')}`")
        items = learning_changes.get("items") or []
        if items:
            lines.append("- Highlights:")
            for item in items:
                lines.append(
                    f"  - `{item.get('urgency') or 'info'}` | `{item.get('source') or 'learning'}` | {item.get('headline') or ''}"
                )
                if item.get("detail"):
                    lines.append(f"    Detail: {item.get('detail')}")
        lines.append("")

    lines.extend(["## Skill Status", ""])
    for skill in payload.get("skill_statuses") or []:
        marker = "READY" if skill.get("present") else "MISSING"
        lines.append(f"- {marker}: `{skill.get('name')}`")
    lines.append("")

    lines.extend(["## Repo Status", ""])
    for repo in payload.get("repo_statuses") or []:
        lines.append(
            f"- `{repo.get('repo')}`: `{repo.get('modified_count', 0)}` modified / `{repo.get('untracked_count', 0)}` untracked"
        )
    lines.append("")

    lines.extend(["## Observe-Only Reviews", ""])
    for review in payload.get("observe_review_statuses") or []:
        status = str(review.get("status_label") or ("READY" if review.get("present") else "MISSING"))
        detail = f"{review.get('item_count', 0)} item(s)"
        if review.get("top_label"):
            detail += f" | top: {review.get('top_label')}"
        if review.get("age_hours") is not None:
            detail += f" | freshness: {review.get('age_hours')}h"
        lines.append(f"- {status}: `{review.get('name')}` — {detail}")
    lines.append("")

    lines.extend(["## Health Snapshot", ""])
    lines.append(f"- Overall: `{payload.get('health_summary', {}).get('overall_status', 'unknown')}` — {payload.get('health_summary', {}).get('overall_summary', '')}")
    for item in payload.get("health_findings") or []:
        lines.append(f"- `{item.get('label')}`: `{item.get('status')}` / `{item.get('last_run_state')}` / {item.get('success_rate_label')}")
    lines.append("")
    lines.append(f"Next step: {payload.get('next_step')}")
    return "\n".join(lines).strip() + "\n"


def render_engineering_governance_email(payload: dict[str, Any], *, render_report_email, report_badge, report_card, report_link) -> tuple[str, str, str]:
    finding_cards = []
    for finding in (payload.get("findings") or [])[:5]:
        color = "red" if finding.get("priority") == "P1" else "amber" if finding.get("priority") == "P2" else "blue"
        finding_cards.append(
            report_card(
                str(finding.get("title") or "Governance finding"),
                (
                    f"<div style=\"margin-bottom:8px;\">{report_badge(str(finding.get('priority') or 'INFO'), color)}</div>"
                    f"<div style=\"color:#374151;margin-bottom:8px;\">{finding.get('summary') or ''}</div>"
                    f"<div style=\"color:#111827;\"><strong>Next:</strong> {finding.get('next_action') or ''}</div>"
                ),
                eyebrow=str(finding.get("kind") or "observe").title(),
            )
        )

    business_desk = payload.get("business_desk_highlights") or {}
    if business_desk.get("available"):
        count_html = "".join(
            f"<li><strong>{html_lib.escape(str(item.get('label') or 'Desk metric'))}:</strong> {int(item.get('count') or 0)}</li>"
            for item in (business_desk.get("counts") or [])[:5]
        )
        action_html = ""
        actions = business_desk.get("next_actions") or []
        if actions:
            action_html = "<div style=\"margin-top:12px;\"><strong>Do next:</strong><ul style=\"margin:8px 0 0 18px;padding:0;\">"
            for item in actions[:3]:
                lane = html_lib.escape(str(item.get("lane") or "desk"))
                title = html_lib.escape(str(item.get("title") or "Next action"))
                summary = html_lib.escape(str(item.get("summary") or ""))
                command = html_lib.escape(str(item.get("command") or ""))
                secondary = html_lib.escape(str(item.get("secondary_command") or ""))
                action_html += f"<li><strong>{lane}</strong> · {title}<br />{summary}"
                if command:
                    action_html += f"<br /><span style=\"color:#6b7280;\">Next: {command}</span>"
                if secondary:
                    action_html += f"<br /><span style=\"color:#9ca3af;\">Then: {secondary}</span>"
                action_html += "</li>"
            action_html += "</ul></div>"
        finding_cards.append(
            report_card(
                "Business Desk Highlights",
                (
                    f"<div style=\"color:#374151;\">Generated: {html_lib.escape(str(business_desk.get('generated_at') or 'unknown'))}</div>"
                    f"<ul style=\"margin:12px 0 0 18px;padding:0;\">{count_html}</ul>"
                    f"{action_html}"
                ),
                eyebrow="Operator desk",
            )
        )

    learning_changes = payload.get("learning_change_highlights") or {}
    if learning_changes.get("available"):
        change_html = "".join(
            (
                f"<li><strong>{html_lib.escape(str(item.get('urgency') or 'info'))}</strong> · "
                f"{html_lib.escape(str(item.get('source') or 'learning'))} · "
                f"{html_lib.escape(str(item.get('headline') or 'Learning change'))}"
                + (
                    f"<br /><span style=\"color:#6b7280;\">{html_lib.escape(str(item.get('detail') or ''))}</span>"
                    if item.get("detail")
                    else ""
                )
                + "</li>"
            )
            for item in (learning_changes.get("items") or [])[:4]
        )
        change_meta = (
            f"<div style=\"color:#374151;\">{html_lib.escape(str(learning_changes.get('headline') or 'No material learning change needs operator action right now.'))}</div>"
            f"<ul style=\"margin:12px 0 0 18px;padding:0;\">"
            f"<li><strong>Total changes:</strong> {int(learning_changes.get('change_count') or 0)}</li>"
            f"<li><strong>Material changes:</strong> {int(learning_changes.get('material_change_count') or 0)}</li>"
            f"<li><strong>Attention-level:</strong> {int(learning_changes.get('attention_change_count') or 0)}</li>"
            "</ul>"
        )
        if change_html:
            change_meta += f"<div style=\"margin-top:12px;\"><strong>Highlights:</strong><ul style=\"margin:8px 0 0 18px;padding:0;\">{change_html}</ul></div>"
        if learning_changes.get("recommended_action"):
            change_meta += (
                f"<div style=\"margin-top:12px;color:#6b7280;\"><strong>Next:</strong> "
                f"{html_lib.escape(str(learning_changes.get('recommended_action') or ''))}</div>"
            )
        finding_cards.append(
            report_card(
                "Learning Change Highlights",
                change_meta,
                eyebrow="Current learnings",
            )
        )

    html = render_report_email(
        label="Duck Ops Engineering",
        title="Engineering Governance Digest",
        subtitle="Observe/propose recommendations for roadmap, reliability, and repo discipline",
        body_html="".join(finding_cards),
        stats=[
            ("Findings", len(payload.get("findings") or [])),
            ("Recommendations", len(payload.get("review_recommendations") or [])),
            ("Missing skills", sum(1 for item in (payload.get("skill_statuses") or []) if not item.get("present"))),
            ("Review lanes", sum(1 for item in (payload.get("observe_review_statuses") or []) if item.get("present"))),
            ("Health alerts", len(payload.get("health_findings") or [])),
        ],
        footer_note="Duck Ops engineering governance digest",
    )
    subject = "MJD: [engineering_governance] Engineering governance digest"
    text_lines = [
        "Engineering governance digest",
        "",
        f"Phase focus: {payload.get('phase_focus')}",
        "",
    ]
    for finding in (payload.get("findings") or [])[:5]:
        text_lines.append(f"- {finding.get('priority')} {finding.get('title')}: {finding.get('summary')}")
        text_lines.append(f"  Next: {finding.get('next_action')}")
    recommendations = payload.get("review_recommendations") or []
    if recommendations:
        text_lines.extend(["", "Recommended follow-through:"])
        for item in recommendations[:3]:
            text_lines.append(f"- {item.get('priority')} {item.get('title')}: {item.get('summary')}")
            text_lines.append(f"  Next: {item.get('next_action')}")
    if business_desk.get("available"):
        text_lines.extend(["", "Business desk highlights:"])
        text_lines.append(f"- Generated: {business_desk.get('generated_at') or 'unknown'}")
        for item in (business_desk.get("counts") or [])[:5]:
            text_lines.append(f"- {item.get('label')}: {int(item.get('count') or 0)}")
        actions = business_desk.get("next_actions") or []
        if actions:
            text_lines.append("- Do next:")
            for item in actions[:3]:
                text_lines.append(
                    f"  - {item.get('lane') or 'desk'} | {item.get('title') or 'Next action'}: {item.get('summary') or ''}"
                )
                if item.get("command"):
                    text_lines.append(f"    Next: {item.get('command')}")
    if learning_changes.get("available"):
        text_lines.extend(["", "Learning change highlights:"])
        text_lines.append(f"- {learning_changes.get('headline') or 'No material learning change needs operator action right now.'}")
        text_lines.append(f"- Total changes: {int(learning_changes.get('change_count') or 0)}")
        text_lines.append(f"- Material changes: {int(learning_changes.get('material_change_count') or 0)}")
        text_lines.append(f"- Attention-level changes: {int(learning_changes.get('attention_change_count') or 0)}")
        for item in (learning_changes.get("items") or [])[:4]:
            text_lines.append(
                f"- {item.get('urgency') or 'info'} | {item.get('source') or 'learning'}: {item.get('headline') or ''}"
            )
            if item.get("detail"):
                text_lines.append(f"  Detail: {item.get('detail')}")
        if learning_changes.get("recommended_action"):
            text_lines.append(f"- Next: {learning_changes.get('recommended_action')}")
    return subject, "\n".join(text_lines).strip(), html


def send_engineering_governance_digest_email() -> dict[str, Any]:
    send_email, render_report_email, report_badge, report_card, report_link = _ensure_duckagent_imports()
    payload = build_engineering_governance_digest()
    subject, text_body, html_body = render_engineering_governance_email(
        payload,
        render_report_email=render_report_email,
        report_badge=report_badge,
        report_card=report_card,
        report_link=report_link,
    )
    send_email(subject, html_body, text_body)
    payload["email_subject"] = subject
    _write_json(DIGEST_STATE_PATH, payload)
    DIGEST_OUTPUT_PATH.write_text(render_engineering_governance_markdown(payload), encoding="utf-8")
    return payload


def main(argv: list[str] | None = None) -> int:
    _ensure_duckagent_python()
    parser = argparse.ArgumentParser(description="Build or email the engineering governance digest.")
    parser.add_argument("--send-email", action="store_true", help="Email the digest after building it.")
    args = parser.parse_args(argv)
    payload = send_engineering_governance_digest_email() if args.send_email else build_engineering_governance_digest()
    print(json.dumps({"generated_at": payload.get("generated_at"), "findings": len(payload.get("findings") or [])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

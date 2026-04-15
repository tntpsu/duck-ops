from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


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


def _build_findings(skill_statuses: list[dict[str, Any]], repo_statuses: list[dict[str, Any]], health_summary: dict[str, Any], health_findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
    observe_review_statuses = _observe_review_statuses()
    findings = _build_findings(skill_statuses, repo_statuses, health_summary, health_findings)
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
        status = "READY" if review.get("present") else "MISSING"
        detail = f"{review.get('item_count', 0)} item(s)"
        if review.get("top_label"):
            detail += f" | top: {review.get('top_label')}"
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

    html = render_report_email(
        label="Duck Ops Engineering",
        title="Engineering Governance Digest",
        subtitle="Observe/propose recommendations for roadmap, reliability, and repo discipline",
        body_html="".join(finding_cards),
        stats=[
            ("Findings", len(payload.get("findings") or [])),
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

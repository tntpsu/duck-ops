#!/usr/bin/env python3
"""Refresh Duck Ops operator maintenance surfaces in dependency order.

This is the canonical local wrapper for the morning maintenance stack. It does
not touch LaunchAgent configuration, browser automation, or marketplaces.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Callable

from customer_inbox_refresh import rebuild_customer_outputs
from dependency_health import build_dependency_health
from engineering_governance_digest import (
    build_engineering_governance_digest,
    send_engineering_governance_digest_email,
)
from repo_ci_status import build_repo_ci_status
from reliability_review import build_reliability_review
from roi_triage import build_roi_triage
from scheduler_health import build_scheduler_health
from tech_debt_triage import build_tech_debt_triage


DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = DUCK_OPS_ROOT / "state" / "operator_surface_refresh.json"
OUTPUT_MD_PATH = DUCK_OPS_ROOT / "output" / "operator" / "operator_surface_refresh.md"


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def _step_status(
    name: str,
    *,
    status: str,
    started_at: str,
    finished_at: str,
    duration_seconds: float,
    details: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": name,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": round(duration_seconds, 3),
    }
    if details:
        payload["details"] = details
    if error:
        payload["error"] = error
    return payload


def _run_step(name: str, fn: Callable[[], dict[str, Any] | None]) -> dict[str, Any]:
    started_at = _now_iso()
    monotonic_start = time.monotonic()
    try:
        details = fn() or {}
    except Exception as exc:
        finished_at = _now_iso()
        return _step_status(
            name,
            status="failed",
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=time.monotonic() - monotonic_start,
            error=str(exc),
        )
    finished_at = _now_iso()
    return _step_status(
        name,
        status="ok",
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=time.monotonic() - monotonic_start,
        details=details,
    )


def _run_repo_ci(repo_names: list[str]) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(DUCK_OPS_ROOT / "runtime" / "repo_ci_status.py"),
        "--run-checks",
    ]
    for repo in repo_names:
        cmd.extend(["--repo", repo])
    result = subprocess.run(
        cmd,
        cwd=DUCK_OPS_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    details = {
        "command": cmd,
        "returncode": result.returncode,
        "stdout_tail": (result.stdout or "").splitlines()[-20:],
        "stderr_tail": (result.stderr or "").splitlines()[-20:],
    }
    if result.returncode != 0:
        raise RuntimeError(
            f"repo_ci_status failed with exit {result.returncode}: {(result.stderr or result.stdout).strip()}"
        )
    return details


def build_operator_surface_refresh(
    *,
    send_email: bool = False,
    run_repo_ci: bool = False,
    repo_names: list[str] | None = None,
    write_outputs: bool = True,
) -> dict[str, Any]:
    """Refresh governance, ROI, and Business Desk artifacts in dependency order."""

    steps: list[dict[str, Any]] = []
    repo_names = list(repo_names or ["duckAgent", "duck-ops"])

    def append_step(step: dict[str, Any]) -> bool:
        steps.append(step)
        return step.get("status") == "ok"

    if run_repo_ci:
        if not append_step(_run_step("repo_ci_status", lambda: _run_repo_ci(repo_names))):
            return _write_refresh_payload(
                steps=steps,
                send_email=send_email,
                run_repo_ci=run_repo_ci,
                repo_names=repo_names,
                write_outputs=write_outputs,
            )
    else:
        def repo_ci_snapshot_step() -> dict[str, Any]:
            payload = build_repo_ci_status(run_checks=False, repo_names=repo_names, write_outputs=True)
            summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
            return {
                "generated_at": payload.get("generated_at"),
                "attention_count": summary.get("attention_count"),
                "dirty_count": summary.get("dirty_count"),
                "headline": payload.get("headline"),
            }

        if not append_step(_run_step("repo_ci_status", repo_ci_snapshot_step)):
            return _write_refresh_payload(
                steps=steps,
                send_email=send_email,
                run_repo_ci=run_repo_ci,
                repo_names=repo_names,
                write_outputs=write_outputs,
            )

    def scheduler_step() -> dict[str, Any]:
        payload = build_scheduler_health(write_outputs=True)
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        return {
            "generated_at": payload.get("generated_at"),
            "status": payload.get("status"),
            "attention_count": summary.get("attention_count"),
            "headline": payload.get("headline"),
        }

    if not append_step(_run_step("scheduler_health", scheduler_step)):
        return _write_refresh_payload(
            steps=steps,
            send_email=send_email,
            run_repo_ci=run_repo_ci,
            repo_names=repo_names,
            write_outputs=write_outputs,
        )

    def dependency_step() -> dict[str, Any]:
        payload = build_dependency_health(write_outputs=True)
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        return {
            "generated_at": payload.get("generated_at"),
            "status": payload.get("status"),
            "item_count": summary.get("item_count"),
            "headline": payload.get("headline"),
        }

    if not append_step(_run_step("dependency_health", dependency_step)):
        return _write_refresh_payload(
            steps=steps,
            send_email=send_email,
            run_repo_ci=run_repo_ci,
            repo_names=repo_names,
                write_outputs=write_outputs,
            )

    def tech_debt_step() -> dict[str, Any]:
        payload = build_tech_debt_triage()
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        return {
            "generated_at": payload.get("generated_at"),
            "item_count": payload.get("item_count"),
            "headline": summary.get("headline"),
        }

    if not append_step(_run_step("tech_debt_triage", tech_debt_step)):
        return _write_refresh_payload(
            steps=steps,
            send_email=send_email,
            run_repo_ci=run_repo_ci,
            repo_names=repo_names,
            write_outputs=write_outputs,
        )

    def reliability_step() -> dict[str, Any]:
        payload = build_reliability_review()
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        return {
            "generated_at": payload.get("generated_at"),
            "review_count": payload.get("review_count"),
            "headline": summary.get("headline"),
        }

    if not append_step(_run_step("reliability_review", reliability_step)):
        return _write_refresh_payload(
            steps=steps,
            send_email=send_email,
            run_repo_ci=run_repo_ci,
            repo_names=repo_names,
            write_outputs=write_outputs,
        )

    def governance_step() -> dict[str, Any]:
        payload = send_engineering_governance_digest_email() if send_email else build_engineering_governance_digest()
        return {
            "generated_at": payload.get("generated_at"),
            "findings": len(payload.get("findings") or []),
            "recommendations": len(payload.get("review_recommendations") or []),
            "email_sent": bool(send_email),
        }

    if not append_step(_run_step("engineering_governance_digest", governance_step)):
        return _write_refresh_payload(
            steps=steps,
            send_email=send_email,
            run_repo_ci=run_repo_ci,
            repo_names=repo_names,
            write_outputs=write_outputs,
        )

    def roi_step() -> dict[str, Any]:
        payload = build_roi_triage(write_outputs=True)
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        return {
            "generated_at": payload.get("generated_at"),
            "candidate_count": summary.get("candidate_count"),
            "top_title": summary.get("top_title"),
        }

    if not append_step(_run_step("roi_triage", roi_step)):
        return _write_refresh_payload(
            steps=steps,
            send_email=send_email,
            run_repo_ci=run_repo_ci,
            repo_names=repo_names,
            write_outputs=write_outputs,
        )

    def desk_step() -> dict[str, Any]:
        payloads = rebuild_customer_outputs()
        desk = payloads.get("business_operator_desk") if isinstance(payloads, dict) else {}
        counts = desk.get("counts") if isinstance(desk, dict) and isinstance(desk.get("counts"), dict) else {}
        return {
            "generated_at": desk.get("generated_at") if isinstance(desk, dict) else None,
            "customer_attention_items": counts.get("customer_attention_items"),
            "roi_triage_candidates": counts.get("roi_triage_candidates"),
            "maintenance_freshness_attention_items": counts.get("maintenance_freshness_attention_items"),
        }

    append_step(_run_step("business_operator_desk", desk_step))

    return _write_refresh_payload(
        steps=steps,
        send_email=send_email,
        run_repo_ci=run_repo_ci,
        repo_names=repo_names,
        write_outputs=write_outputs,
    )


def _write_refresh_payload(
    *,
    steps: list[dict[str, Any]],
    send_email: bool,
    run_repo_ci: bool,
    repo_names: list[str],
    write_outputs: bool,
) -> dict[str, Any]:
    status = "ok" if all(step.get("status") == "ok" for step in steps) else "failed"

    payload = {
        "generated_at": _now_iso(),
        "surface_version": 1,
        "status": status,
        "send_email": bool(send_email),
        "run_repo_ci": bool(run_repo_ci),
        "repo_names": repo_names,
        "steps": steps,
        "outputs": {
            "state": str(STATE_PATH),
            "markdown": str(OUTPUT_MD_PATH),
            "repo_ci_status": str(DUCK_OPS_ROOT / "output" / "operator" / "repo_ci_status.md"),
            "scheduler_health": str(DUCK_OPS_ROOT / "output" / "operator" / "scheduler_health.md"),
            "dependency_health": str(DUCK_OPS_ROOT / "output" / "operator" / "dependency_health.md"),
            "tech_debt_triage": str(DUCK_OPS_ROOT / "output" / "operator" / "tech_debt_triage.md"),
            "reliability_review": str(DUCK_OPS_ROOT / "output" / "operator" / "reliability_review.md"),
            "engineering_governance": str(DUCK_OPS_ROOT / "output" / "operator" / "engineering_governance_digest.md"),
            "roi_triage": str(DUCK_OPS_ROOT / "output" / "operator" / "roi_triage.md"),
            "business_operator_desk": str(DUCK_OPS_ROOT / "output" / "operator" / "business_operator_desk.md"),
        },
    }
    if write_outputs:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_MD_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        OUTPUT_MD_PATH.write_text(render_operator_surface_refresh_markdown(payload), encoding="utf-8")
    return payload


def render_operator_surface_refresh_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Operator Surface Refresh",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Status: `{payload.get('status')}`",
        f"- Email sent: `{bool(payload.get('send_email'))}`",
        f"- Repo CI run: `{bool(payload.get('run_repo_ci'))}`",
        "",
        "## Steps",
        "",
    ]
    for step in payload.get("steps") or []:
        lines.append(f"- `{step.get('name')}`: `{step.get('status')}` in `{step.get('duration_seconds')}`s")
        details = step.get("details") if isinstance(step.get("details"), dict) else {}
        if details.get("generated_at"):
            lines.append(f"  - Generated: `{details.get('generated_at')}`")
        if details.get("top_title"):
            lines.append(f"  - Top ROI: {details.get('top_title')}")
        if details.get("headline"):
            lines.append(f"  - {details.get('headline')}")
        if step.get("error"):
            lines.append(f"  - Error: {step.get('error')}")
    outputs = payload.get("outputs") if isinstance(payload.get("outputs"), dict) else {}
    if outputs:
        lines.extend(["", "## Outputs", ""])
        for label, path in outputs.items():
            lines.append(f"- {label}: `{path}`")
    return "\n".join(lines).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Refresh Duck Ops operator maintenance surfaces.")
    parser.add_argument("--send-email", action="store_true", help="Send the engineering governance digest email.")
    parser.add_argument("--run-repo-ci", action="store_true", help="Refresh the local repo CI mirror before governance.")
    parser.add_argument("--repo", action="append", dest="repos", help="Restrict repo CI refresh to one or more repo names.")
    args = parser.parse_args(argv)

    payload = build_operator_surface_refresh(
        send_email=args.send_email,
        run_repo_ci=args.run_repo_ci,
        repo_names=args.repos,
        write_outputs=True,
    )
    print(
        json.dumps(
            {
                "generated_at": payload.get("generated_at"),
                "status": payload.get("status"),
                "steps": [
                    {"name": step.get("name"), "status": step.get("status")}
                    for step in payload.get("steps") or []
                ],
                "markdown_path": str(OUTPUT_MD_PATH),
            },
            indent=2,
        )
    )
    return 0 if payload.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())

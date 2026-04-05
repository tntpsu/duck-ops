#!/usr/bin/env python3
"""
Shared decision writers for the passive OpenClaw sidecar.

These helpers keep output naming and markdown formatting centralized so
evaluators can emit auditable artifacts without duplicating path logic.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "output_naming.json"


def slugify(value: str) -> str:
    import re

    text = re.sub(r"[^a-z0-9]+", "-", (value or "").lower()).strip("-")
    return text or "unknown"


def load_output_patterns() -> dict[str, str]:
    payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    return payload.get("patterns", {})


def ensure_parent(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def render_pattern(pattern: str, replacements: dict[str, str]) -> Path:
    rendered = pattern
    for key, value in replacements.items():
        rendered = rendered.replace(f"<{key}>", value)
        rendered = rendered.replace(key, value)
    return ROOT / rendered


def decision_replacements(decision: dict[str, Any], now: datetime | None = None) -> dict[str, str]:
    current = now or datetime.now()
    artifact_slug = slugify(
        decision.get("artifact_slug")
        or decision.get("theme")
        or decision.get("artifact_id")
        or decision.get("title")
        or "artifact"
    )
    return {
        "theme": slugify(decision.get("theme") or artifact_slug),
        "date": decision.get("date") or current.strftime("%Y-%m-%d"),
        "flow": slugify(decision.get("flow") or "unknown"),
        "run_id": decision.get("run_id") or "unknown",
        "artifact_slug": artifact_slug,
        "channel": slugify(decision.get("channel") or "unknown"),
        "artifact_id": slugify(decision.get("artifact_id") or artifact_slug),
        "YYYY-MM-DD": current.strftime("%Y-%m-%d"),
        "YYYY-WW": current.strftime("%Y-%W"),
        "YYYY-MM-DDTHHMMSS": current.strftime("%Y-%m-%dT%H%M%S"),
    }


def choose_patterns(decision: dict[str, Any]) -> tuple[str, str]:
    artifact_type = decision.get("artifact_type")
    if artifact_type == "trend":
        return "trend_json", "trend_md"
    if artifact_type == "customer":
        return "customer_json", "customer_md"
    return "quality_gate_json", "quality_gate_md"


def write_markdown(path: Path, decision: dict[str, Any]) -> None:
    lines = [
        f"# Decision Review: {decision.get('artifact_id', 'unknown')}",
        "",
        f"- Artifact type: `{decision.get('artifact_type', 'unknown')}`",
        f"- Decision: `{decision.get('decision', 'pending')}`",
        f"- Score: `{decision.get('score', 'n/a')}`",
        f"- Confidence: `{decision.get('confidence', 'n/a')}`",
        f"- Priority: `{decision.get('priority', 'n/a')}`",
        f"- Review status: `{decision.get('review_status', 'pending')}`",
    ]
    if decision.get("theme"):
        lines.append(f"- Theme: `{decision['theme']}`")
    if decision.get("action_frame"):
        lines.append(f"- Action frame: `{decision['action_frame']}`")
    if decision.get("flow"):
        lines.append(f"- Flow: `{decision['flow']}`")
    if decision.get("run_id"):
        lines.append(f"- Run ID: `{decision['run_id']}`")
    human_review = decision.get("human_review") or {}
    if human_review:
        lines.append(f"- Human action: `{human_review.get('action', 'unknown')}`")
        if human_review.get("recorded_at"):
            lines.append(f"- Reviewed at: `{human_review['recorded_at']}`")
    lines.extend(["", "## Reasoning", ""])
    reasoning = decision.get("reasoning") or ["No reasoning provided."]
    lines.extend(f"- {item}" for item in reasoning)
    suggestions = decision.get("improvement_suggestions") or []
    if suggestions:
        lines.extend(["", "## Improvement Suggestions", ""])
        lines.extend(f"- {item}" for item in suggestions)
    if human_review.get("note"):
        lines.extend(["", "## Human Note", "", f"- {human_review['note']}"])
    refs = decision.get("evidence_refs") or []
    if refs:
        lines.extend(["", "## Evidence References", ""])
        lines.extend(f"- `{item}`" for item in refs)
    ensure_parent(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_decision(decision: dict[str, Any], now: datetime | None = None) -> dict[str, str]:
    patterns = load_output_patterns()
    json_key, md_key = choose_patterns(decision)
    replacements = decision_replacements(decision, now=now)
    json_path = render_pattern(patterns[json_key], replacements)
    md_path = render_pattern(patterns[md_key], replacements)

    ensure_parent(json_path).write_text(json.dumps(decision, indent=2), encoding="utf-8")
    write_markdown(md_path, decision)
    return {"json_path": str(json_path), "md_path": str(md_path)}


if __name__ == "__main__":
    raise SystemExit("Import and call write_decision() from an evaluator.")

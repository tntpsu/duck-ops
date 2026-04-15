#!/usr/bin/env python3
"""
Small coordination layer for DuckAgent's weekly theme rotation and weekly sale
lane so operator tooling can see which lane should lead this week.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from workflow_control import record_workflow_transition

ROOT = Path(__file__).resolve().parents[1]
DUCK_AGENT_ROOT = ROOT.parent / "duckAgent"
RUNS_DIR = DUCK_AGENT_ROOT / "runs"
WEEKLY_SALE_MONITOR_PATH = ROOT / "state" / "weekly_sale_monitor.json"


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _hours_since(value: str | None, now_local: datetime) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return round(max(0.0, (now_local - parsed.astimezone()).total_seconds() / 3600.0), 1)


def current_weekly_theme(now_local: datetime | None = None) -> dict[str, Any]:
    now_local = now_local or datetime.now(timezone.utc).astimezone()
    week_of_month = ((now_local.day - 1) // 7) + 1
    week_number = ((week_of_month - 1) % 4) + 1
    themes = {
        1: ("new_arrivals", "New Arrivals"),
        2: ("themed_collection", "Themed Collection"),
        3: ("promotional_offers", "Special Offers"),
        4: ("top_ducks", "Top Ducks"),
    }
    theme_key, theme_name = themes[week_number]
    return {
        "week_of_month": week_of_month,
        "rotation_week": week_number,
        "theme": theme_key,
        "theme_name": theme_name,
        "is_sale_primary_week": theme_key == "promotional_offers",
    }


def latest_weekly_state_path() -> Path | None:
    files = sorted(RUNS_DIR.glob("*/state_weekly.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def build_weekly_campaign_coordination(now_local: datetime | None = None) -> dict[str, Any]:
    now_local = now_local or datetime.now(timezone.utc).astimezone()
    theme = current_weekly_theme(now_local)
    latest_state_path = latest_weekly_state_path()
    latest_state = load_json(latest_state_path, {}) if latest_state_path else {}
    sale_monitor = load_json(WEEKLY_SALE_MONITOR_PATH, {"items": []})
    weak_sales = [item for item in list(sale_monitor.get("items") or []) if str(item.get("effectiveness") or "") == "weak"]
    strong_sales = [item for item in list(sale_monitor.get("items") or []) if str(item.get("effectiveness") or "") in {"strong", "working"}]

    sale_playbook = (latest_state.get("sale_playbook") or {}) if isinstance(latest_state, dict) else {}
    sale_theme = (sale_playbook.get("theme_of_the_week") or {}) if isinstance(sale_playbook, dict) else {}
    sale_summary = str(sale_playbook.get("approval_summary") or "").strip()
    sale_theme_name = str(sale_theme.get("name") or "").strip()
    sale_ready = bool(latest_state.get("weekly_sale_published") or latest_state.get("weekly_sale_playbook_sent"))

    if theme["is_sale_primary_week"]:
        lead_lane = "weekly_sale"
        creative_action = "sale_support_post"
        creative_tool = "Social Reference"
        publication_lane = "weekly_sale_blog"
        publication_source = "approved_sale_playbook" if latest_state.get("weekly_sale_published") else "sale_playbook_review_bundle" if sale_ready else "waiting_for_sale_playbook"
        summary = (
            "This is the sale-led week. The weekly blog lane should be built from the weekly sale playbook rather than a generic theme story, "
            "and Creative Agent should only add a support post if you want extra sale promotion."
        )
    else:
        lead_lane = "weekly_theme"
        creative_action = "weekly_theme_support_post"
        creative_tool = "Weekly Collection Creative Pack"
        publication_lane = "creative_review_post"
        publication_source = "weekly_theme_rotation"
        summary = (
            "This is a non-sale week. Let DuckAgent keep the sale lane in the background, but use Creative Agent to stage the reviewable post for this week's campaign."
        )

    return {
        "generated_at": now_local.isoformat(),
        "weekly_theme": theme,
        "latest_weekly_state_path": str(latest_state_path) if latest_state_path else None,
        "latest_weekly_state": {
            "weekly_sale_published": latest_state.get("weekly_sale_published"),
            "weekly_sale_published_at": latest_state.get("weekly_sale_published_at"),
            "weekly_sale_playbook_sent": latest_state.get("weekly_sale_playbook_sent"),
            "sale_playbook_theme": sale_theme_name,
        },
        "sale_monitor_snapshot": {
            "generated_at": sale_monitor.get("generated_at"),
            "freshness_hours": _hours_since(sale_monitor.get("generated_at"), now_local),
            "strong_or_working_titles": [item.get("product_title") for item in strong_sales[:3]],
            "weak_titles": [item.get("product_title") for item in weak_sales[:3]],
        },
        "sale_playbook_snapshot": {
            "theme_name": sale_theme_name,
            "approval_summary": sale_summary,
        },
        "coordination": {
            "lead_lane": lead_lane,
            "publication_lane": publication_lane,
            "publication_source": publication_source,
            "creative_action": creative_action,
            "creative_tool": creative_tool,
            "summary": summary,
        },
    }


def render_weekly_campaign_coordination_markdown(payload: dict[str, Any]) -> str:
    theme = payload.get("weekly_theme") or {}
    latest = payload.get("latest_weekly_state") or {}
    coord = payload.get("coordination") or {}
    sale_snapshot = payload.get("sale_monitor_snapshot") or {}
    lines = [
        "# Weekly Campaign Coordination",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        f"- Rotation week: `{theme.get('rotation_week')}`",
        f"- Theme: `{theme.get('theme_name')}`",
        f"- Sale-primary week: `{theme.get('is_sale_primary_week')}`",
        f"- Lead lane: `{coord.get('lead_lane')}`",
        f"- Publication lane: `{coord.get('publication_lane')}`",
        f"- Publication source: `{coord.get('publication_source')}`",
        f"- Creative action: `{coord.get('creative_action')}`",
        f"- Creative tool: `{coord.get('creative_tool')}`",
        "",
        "## Coordination Read",
        "",
        str(coord.get("summary") or "No summary available."),
        "",
        "## Latest Weekly Sale State",
        "",
        f"- State file: `{payload.get('latest_weekly_state_path')}`",
        f"- Sale playbook sent: `{latest.get('weekly_sale_playbook_sent')}`",
        f"- Sale published: `{latest.get('weekly_sale_published')}`",
        f"- Sale published at: `{latest.get('weekly_sale_published_at')}`",
        f"- Sale theme: `{latest.get('sale_playbook_theme')}`",
        "",
        "## Sale Monitor Snapshot",
        "",
        f"- Strong or working: `{', '.join(sale_snapshot.get('strong_or_working_titles') or [])}`",
        f"- Weak: `{', '.join(sale_snapshot.get('weak_titles') or [])}`",
        "",
        "## Playbook Snapshot",
        "",
        f"- Theme: `{(payload.get('sale_playbook_snapshot') or {}).get('theme_name')}`",
        f"- Approval summary: `{(payload.get('sale_playbook_snapshot') or {}).get('approval_summary')}`",
        "",
    ]
    return "\n".join(lines)


def sync_weekly_campaign_coordination_control(payload: dict[str, Any]) -> dict[str, Any]:
    coord = payload.get("coordination") or {}
    theme = payload.get("weekly_theme") or {}
    latest = payload.get("latest_weekly_state") or {}
    sale_monitor_snapshot = payload.get("sale_monitor_snapshot") or {}
    sale_monitor_age = sale_monitor_snapshot.get("freshness_hours")
    publication_source = str(coord.get("publication_source") or "").strip()
    lead_lane = str(coord.get("lead_lane") or "").strip()
    publication_lane = str(coord.get("publication_lane") or "").strip()

    if sale_monitor_age is not None and float(sale_monitor_age) >= 36:
        state = "blocked"
        state_reason = "stale_input"
        next_action = "Refresh the sale monitor before relying on weekly campaign coordination."
    elif not (lead_lane or publication_lane):
        state = "blocked"
        state_reason = "coordination_missing"
        next_action = "Rebuild weekly campaign coordination so the lead lane and publication lane are explicit."
    elif publication_source == "waiting_for_sale_playbook":
        state = "blocked"
        state_reason = "blocked_by_upstream"
        next_action = "Finish the weekly sale playbook before treating this sale-led week as ready to publish."
    elif theme.get("is_sale_primary_week") and not latest.get("weekly_sale_playbook_sent") and not latest.get("weekly_sale_published"):
        state = "blocked"
        state_reason = "blocked_by_upstream"
        next_action = "Generate or approve the weekly sale playbook before the sale-led publication lane can run."
    elif str(coord.get("summary") or "").strip():
        state = "verified"
        state_reason = "publication_lane_ready"
        next_action = "Use this coordination output to steer the weekly campaign and publishing lane."
    else:
        state = "observed"
        state_reason = "coordination_ready_without_summary"
        next_action = "Add or refresh the coordination summary so the operator desk has context for this week."

    control = record_workflow_transition(
        workflow_id="weekly_campaign_coordination",
        lane="weekly_campaign_coordination",
        display_label="Weekly Campaign Coordination",
        entity_id="weekly_campaign_coordination",
        state=state,
        state_reason=state_reason,
        requires_confirmation=False,
        input_freshness={
            "sale_monitor_hours": sale_monitor_age,
        },
        last_verification={
            "lead_lane": lead_lane or None,
            "publication_lane": publication_lane or None,
            "publication_source": publication_source or None,
            "sale_primary_week": bool(theme.get("is_sale_primary_week")),
            "sale_playbook_sent": latest.get("weekly_sale_playbook_sent"),
            "sale_published": latest.get("weekly_sale_published"),
        },
        next_action=next_action,
        metadata={
            "generated_at": payload.get("generated_at"),
            "theme_name": theme.get("theme_name"),
            "rotation_week": theme.get("rotation_week"),
        },
        receipt_kind="snapshot",
        receipt_payload={
            "generated_at": payload.get("generated_at"),
            "publication_lane": publication_lane,
            "publication_source": publication_source,
        },
    )
    payload["workflow_control"] = {
        "state": control.get("state"),
        "state_reason": control.get("state_reason"),
        "updated_at": control.get("updated_at"),
        "next_action": control.get("next_action"),
    }
    return payload

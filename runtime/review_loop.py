#!/usr/bin/env python3
"""
Human review queue, operator view, and command recorder for the passive OpenClaw sidecar.

This script supports:
- building a pending review queue from current evaluator outputs
- assigning short operator IDs
- maintaining a one-at-a-time current review item
- recording human decisions
- answering simple operator commands like why/agree/approve/needs changes/discard/next
"""

from __future__ import annotations

import argparse
import html
import json
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from business_operator_desk import render_business_section
from decision_writer import ensure_parent, load_output_patterns, render_pattern, write_decision
from ops_control import sync_ops_control
from trend_ranker import build_trend_concepts


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"

QUALITY_GATE_STATE_PATH = STATE_DIR / "quality_gate_state.json"
TREND_RANKER_STATE_PATH = STATE_DIR / "trend_ranker_state.json"
OVERRIDES_PATH = STATE_DIR / "overrides.jsonl"
REVIEW_QUEUE_STATE_PATH = STATE_DIR / "review_queue.json"
CUSTOMER_INTERACTION_QUEUE_PATH = STATE_DIR / "customer_interaction_queue.json"
OPERATOR_STATE_PATH = STATE_DIR / "operator_state.json"
CATALOG_ALIASES_PATH = STATE_DIR / "catalog_aliases.json"
BUSINESS_OPERATOR_DESK_PATH = STATE_DIR / "business_operator_desk.json"
PRODUCTS_CACHE_PATH = Path("/Users/philtullai/ai-agents/duckAgent/cache/products_cache.json")
DUCK_AGENT_RUNS_DIR = Path("/Users/philtullai/ai-agents/duckAgent/runs")
DUCK_AGENT_ROOT = Path("/Users/philtullai/ai-agents/duckAgent")
SYSTEM_HEALTH_PATH = DUCK_AGENT_ROOT / "creative_agent" / "runtime" / "output" / "operator" / "system_health.json"
DUCK_AGENT_PYTHON = DUCK_AGENT_ROOT / ".venv" / "bin" / "python"
DUCK_AGENT_HANDOFF_FLOWS = {
    "meme": {
        "approve": {"flow": "meme", "action": "publish"},
        "needs_changes": {"flow": "meme", "action": "revise"},
    },
    "jeepfact": {
        "approve": {"flow": "jeepfact", "action": "publish"},
        "needs_changes": {"flow": "jeepfact", "action": "revise"},
    },
    "weekly_sale": {
        "approve": {"flow": "weekly_sale", "action": "publish"},
        "needs_changes": {"flow": "weekly_sale", "action": "revise"},
    },
    "reviews_story": {
        "approve": {"flow": "reviews", "action": "publish"},
    },
}

SHORT_ID_START = 101
MAX_TREND_OPERATOR_ITEMS = 8
FRESH_REVIEW_WINDOW_DAYS = 3.0
TREND_DEDUPE_IGNORED_TOKENS = {
    "duck",
    "ducks",
    "officer",
    "bear",
}
GENERIC_TREND_TITLES = {"duck", "ducks", "rubber duck", "rubber ducks", "jeep duck", "jeep ducks"}
ACTION_ALIASES = {
    "agree": "agree",
    "approve": "approve",
    "approved": "approve",
    "approveit": "approve",
    "publish": "approve",
    "ship": "approve",
    "hold": "needs_changes",
    "revise": "needs_changes",
    "needs_revision": "needs_changes",
    "needs": "needs_changes",
    "change": "needs_changes",
    "discard": "discard",
    "drop": "discard",
    "reject": "discard",
    "ignore": "ignore",
    "skip": "ignore",
    "bad": "ignore",
    "why": "why",
    "more": "why",
    "evidence": "why",
    "build": "build",
    "promote": "promote",
    "wait": "wait",
    "suggest": "suggest_changes",
    "suggestions": "suggest_changes",
    "next": "next",
    "help": "help",
    "show": "show",
    "status": "status",
    "queue": "status",
    "remaining": "status",
    "left": "status",
    "rewrite": "rewrite",
    "same": "same_as",
    "have": "same_as",
}
CUSTOMER_SHORT_ID_PATTERN = re.compile(r"^c\d+$", re.IGNORECASE)
RECOMMENDED_ACTION_BY_DECISION = {
    "publish_ready": "approve",
    "needs_revision": "needs_changes",
    "discard": "discard",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row))
        handle.write("\n")


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def priority_rank(priority: str) -> int:
    return {"urgent": 3, "high": 2, "medium": 1, "low": 0}.get(priority, 0)


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def item_age_days(value: str | None) -> float | None:
    parsed = parse_iso_datetime(value)
    if not parsed:
        return None
    return max(0.0, (datetime.now(timezone.utc).astimezone() - parsed.astimezone()).total_seconds() / 86400.0)


def decision_age_days(decision: dict[str, Any]) -> float | None:
    run_id = str(decision.get("run_id") or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", run_id):
        parsed = parse_iso_datetime(f"{run_id}T00:00:00+00:00")
        if parsed:
            return max(0.0, (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds() / 86400.0)
    return item_age_days(decision.get("created_at"))


def item_sort_key(item: dict[str, Any]) -> tuple[str, str, int, int]:
    return (
        item.get("run_id") or "",
        item.get("created_at") or "",
        priority_rank(item.get("priority", "low")),
        int(item.get("score") or 0),
    )


def load_quality_gate_state() -> dict[str, Any]:
    return load_json(QUALITY_GATE_STATE_PATH, {"artifacts": {}, "alerts": {}, "last_digest_date": None})


def load_trend_ranker_state() -> dict[str, Any]:
    return load_json(TREND_RANKER_STATE_PATH, {"artifacts": {}})


def load_state_bundle() -> dict[str, dict[str, Any]]:
    return {
        "quality_gate": load_quality_gate_state(),
        "trend_ranker": load_trend_ranker_state(),
    }


def write_state_source(source: str, state: dict[str, Any]) -> None:
    if source == "quality_gate":
        write_json(QUALITY_GATE_STATE_PATH, state)
        return
    if source == "trend_ranker":
        write_json(TREND_RANKER_STATE_PATH, state)
        return
    raise SystemExit(f"Unknown state source: {source}")


def latest_override_index() -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in load_jsonl(OVERRIDES_PATH):
        artifact_id = str(row.get("artifact_id") or "").strip()
        if not artifact_id:
            continue
        recorded_at = str(row.get("recorded_at") or "")
        previous = latest.get(artifact_id)
        if previous is None or recorded_at >= str(previous.get("recorded_at") or ""):
            latest[artifact_id] = row
    return latest


def duckagent_publish_reconciliation(decision: dict[str, Any]) -> dict[str, Any] | None:
    run_id = str(decision.get("run_id") or "").strip()
    flow = str(decision.get("flow") or "")
    artifact_type = str(decision.get("artifact_type") or "")
    if not run_id:
        return None

    if flow == "newduck" or artifact_type == "listing":
        state_path = DUCK_AGENT_RUNS_DIR / run_id / "state_newduck.json"
        payload = load_json(state_path, {})
        if not isinstance(payload, dict):
            return None
        if payload.get("newduck_published") or payload.get("shopify_product_id") or payload.get("etsy_listing_id"):
            return {
                "recorded_at": now_iso(),
                "resolution": "approve",
                "note": "Reconciled automatically because DuckAgent already shows this listing as published.",
                "source": str(state_path),
            }
        return None

    if flow == "weekly_sale" or artifact_type == "promotion":
        state_path = DUCK_AGENT_RUNS_DIR / run_id / "state_weekly.json"
        payload = load_json(state_path, {})
        if not isinstance(payload, dict):
            return None
        if payload.get("weekly_sale_published") or payload.get("weekly_sale_published_at"):
            return {
                "recorded_at": str(payload.get("weekly_sale_published_at") or now_iso()),
                "resolution": "approve",
                "note": "Reconciled automatically because DuckAgent already shows this weekly sale as published.",
                "source": str(state_path),
            }
        return None

    return None


def apply_reconciled_review_status(
    record: dict[str, Any],
    decision: dict[str, Any],
    *,
    review_status: str,
    action: str,
    resolution: str,
    recorded_at: str,
    note: str,
    source: str,
) -> None:
    decision["review_status"] = review_status
    decision["human_review"] = {
        "action": action,
        "resolution": resolution,
        "recorded_at": recorded_at,
        "note": note,
    }
    decision["reconciled_resolution"] = {
        "action": action,
        "resolution": resolution,
        "recorded_at": recorded_at,
        "note": note,
        "source": source,
    }
    record["decision"] = decision
    record["reviewed_at"] = recorded_at
    record["reconciled_at"] = now_iso()
    record["reconciliation_reason"] = note


def reconcile_quality_gate_state(state: dict[str, Any]) -> bool:
    changed = False
    override_index = latest_override_index()
    artifacts = state.get("artifacts") or {}
    for artifact_id, record in artifacts.items():
        decision = record.get("decision") or {}
        if str(decision.get("review_status") or "") != "pending":
            continue

        override = override_index.get(artifact_id)
        if override:
            resolution = str(override.get("resolution") or "").strip().lower()
            if resolution in {"approve", "publish"}:
                apply_reconciled_review_status(
                    record,
                    decision,
                    review_status="approved",
                    action=str(override.get("action") or "override"),
                    resolution="approve",
                    recorded_at=str(override.get("recorded_at") or now_iso()),
                    note=str(override.get("note") or "Reconciled from the operator override log."),
                    source="override_log",
                )
                changed = True
                continue
            if resolution in {"discard", "ignore"}:
                apply_reconciled_review_status(
                    record,
                    decision,
                    review_status="rejected",
                    action=str(override.get("action") or "override"),
                    resolution=resolution,
                    recorded_at=str(override.get("recorded_at") or now_iso()),
                    note=str(override.get("note") or "Reconciled from the operator override log."),
                    source="override_log",
                )
                changed = True
                continue

        published_state = duckagent_publish_reconciliation(decision)
        if published_state:
            apply_reconciled_review_status(
                record,
                decision,
                review_status="approved",
                action="reconcile",
                resolution=str(published_state.get("resolution") or "approve"),
                recorded_at=str(published_state.get("recorded_at") or now_iso()),
                note=str(published_state.get("note") or "Reconciled from DuckAgent publish state."),
                source=str(published_state.get("source") or "duckagent_state"),
            )
            changed = True

    return changed


def reconcile_state_bundle(state_bundle: dict[str, dict[str, Any]]) -> bool:
    quality_gate_state = state_bundle.get("quality_gate", {})
    changed = reconcile_quality_gate_state(quality_gate_state)
    if changed:
        write_state_source("quality_gate", quality_gate_state)
    return changed


def archive_stale_quality_gate_items(state: dict[str, Any]) -> bool:
    changed = False
    artifacts = state.get("artifacts") or {}
    for record in artifacts.values():
        decision = record.get("decision") or {}
        if decision.get("review_status") != "pending":
            continue

        artifact_type = decision.get("artifact_type") or ""
        flow = decision.get("flow") or ""
        age_days = decision_age_days(decision)
        if age_days is None:
            continue

        threshold_days: float | None = None
        archive_reason: str | None = None
        if flow.startswith("reviews_") or artifact_type in {"review_reply", "social_post"}:
            threshold_days = 5.0
            archive_reason = "stale daily review item"
        elif flow == "weekly_sale" or artifact_type == "promotion":
            threshold_days = 7.0
            archive_reason = "stale weekly sale item"
        elif artifact_type == "listing" and flow == "newduck":
            threshold_days = 21.0
            archive_reason = "stale listing review item"
        elif age_days >= 21.0:
            threshold_days = 21.0
            archive_reason = "stale operator item"

        if threshold_days is None or age_days < threshold_days:
            continue

        decision["review_status"] = "archived"
        decision["archived_at"] = now_iso()
        decision["archive_reason"] = archive_reason
        record["decision"] = decision
        changed = True
    return changed


def load_operator_state() -> dict[str, Any]:
    return load_json(
        OPERATOR_STATE_PATH,
        {
            "next_short_id": SHORT_ID_START,
            "artifact_short_ids": {},
            "current_artifact_id": None,
            "last_queue_generated_at": None,
        },
    )


def write_operator_state(state: dict[str, Any]) -> None:
    write_json(OPERATOR_STATE_PATH, state)


def normalize_catalog_text(value: str) -> str:
    text = (value or "").lower()
    text = re.sub(r"\bwhite\s+tailed\b", "whitetail", text)
    text = re.sub(r"\bwhite\s+tail\b", "whitetail", text)
    text = re.sub(r"\bpolice\s+officer\b", "police", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def load_catalog_products() -> list[dict[str, Any]]:
    raw = load_json(PRODUCTS_CACHE_PATH, {})
    items = raw.get("items", {}) if isinstance(raw, dict) else {}
    products: list[dict[str, Any]] = []
    if not isinstance(items, dict):
        return products
    for pid, item in items.items():
        tags = item.get("tags") or []
        if isinstance(tags, list):
            tags_text = ", ".join(str(tag) for tag in tags)
        else:
            tags_text = str(tags)
        core_terms = item.get("core_terms") or []
        if isinstance(core_terms, list):
            core_terms_text = ", ".join(str(term) for term in core_terms)
        else:
            core_terms_text = str(core_terms)
        concept_variations = item.get("concept_variations") or []
        if isinstance(concept_variations, list):
            concept_variations_text = ", ".join(str(term) for term in concept_variations)
        else:
            concept_variations_text = str(concept_variations)
        products.append(
            {
                "product_id": str(pid),
                "title": item.get("title") or "",
                "handle": item.get("handle") or "",
                "on_sale": item.get("on_sale"),
                "tiktok_publishable": item.get("tiktok_publishable"),
                "category": item.get("category") or "",
                "ai_theme_category": item.get("ai_theme_category") or "",
                "tags": tags_text,
                "core_terms": core_terms_text,
                "concept_variations": concept_variations_text,
            }
        )
    return products


def resolve_catalog_product(reference: str) -> dict[str, Any] | None:
    query = normalize_catalog_text(reference)
    if not query:
        return None
    query_tokens = [token for token in query.split() if token]
    best: tuple[int, dict[str, Any] | None] = (-1, None)
    for product in load_catalog_products():
        haystack = normalize_catalog_text(
            " ".join(
                filter(
                    None,
                    [
                        product.get("title"),
                        product.get("handle"),
                        product.get("category"),
                        product.get("ai_theme_category"),
                        product.get("tags"),
                        product.get("core_terms"),
                        product.get("concept_variations"),
                    ],
                )
            )
        )
        score = 0
        if query == normalize_catalog_text(product.get("title") or ""):
            score = 100
        elif query == normalize_catalog_text(product.get("handle") or ""):
            score = 98
        elif query and query in haystack:
            score = 90
        else:
            present = [token for token in query_tokens if token in haystack]
            if present:
                overlap_ratio = len(present) / max(1, len(query_tokens))
                score = int(overlap_ratio * 70)
                if len(present) >= 2:
                    score += 10
        if score > best[0]:
            best = (score, product)
    return best[1] if best[0] >= 60 else None


def save_catalog_alias(theme: str, product: dict[str, Any], artifact_id: str) -> dict[str, Any]:
    payload = load_json(CATALOG_ALIASES_PATH, {"aliases": []})
    aliases = payload.get("aliases", []) if isinstance(payload, dict) else []
    aliases = [record for record in aliases if normalize_catalog_text(str(record.get("theme") or "")) != normalize_catalog_text(theme)]
    alias_record = {
        "theme": theme,
        "normalized_theme": normalize_catalog_text(theme),
        "product_id": product.get("product_id"),
        "product_title": product.get("title"),
        "product_handle": product.get("handle"),
        "recorded_at": now_iso(),
        "source_artifact_id": artifact_id,
    }
    aliases.append(alias_record)
    write_json(CATALOG_ALIASES_PATH, {"aliases": aliases})
    return alias_record


def apply_trend_alias(
    state_bundle: dict[str, dict[str, Any]],
    target_item: dict[str, Any],
    product: dict[str, Any],
) -> str:
    record = (state_bundle.get("trend_ranker", {}).get("artifacts") or {}).get(target_item["artifact_id"])
    if record is None:
        return target_item.get("action_frame") or "wait"
    decision = record.get("decision") or {}
    metadata = decision.setdefault("trend_metadata", {})
    metadata["catalog_status"] = "covered"
    metadata["matching_products"] = [
        {
            "product_id": product.get("product_id"),
            "title": product.get("title"),
            "handle": product.get("handle"),
            "on_sale": product.get("on_sale"),
            "tiktok_publishable": product.get("tiktok_publishable"),
        }
    ]
    previous_action = decision.get("action_frame") or "wait"
    updated_action = "promote" if previous_action == "build" else previous_action
    decision["action_frame"] = updated_action
    decision["review_status"] = "pending"
    decision["manual_review_requested"] = True
    reasoning = decision.setdefault("reasoning", [])
    alias_reason = f"Operator confirmed this theme matches existing product `{product.get('title')}`."
    if alias_reason not in reasoning:
        reasoning.append(alias_reason)
    suggestions = decision.setdefault("improvement_suggestions", [])
    alias_suggestion = f"Use existing product `{product.get('title')}` as the canonical match for this trend going forward."
    if alias_suggestion not in suggestions:
        suggestions.insert(0, alias_suggestion)
    if previous_action == "build":
        promote_note = f"Because you already have `{product.get('title')}`, prefer promotion over building for this theme."
        if promote_note not in suggestions:
            suggestions.insert(1, promote_note)
    elif previous_action == "wait":
        wait_note = f"You already have `{product.get('title')}`, but the trend signal is still not strong enough to move beyond wait yet."
        if wait_note not in suggestions:
            suggestions.insert(1, wait_note)
    record["decision"] = decision
    record["output_paths"] = write_decision(decision)
    return updated_action


def recommended_action(item: dict[str, Any]) -> str:
    artifact_type = item.get("artifact_type")
    if artifact_type == "trend":
        return item.get("action_frame") or "wait"
    return RECOMMENDED_ACTION_BY_DECISION.get(item.get("decision") or "", "hold")


def decision_label(decision: str) -> str:
    return {
        "publish_ready": "approve / ready",
        "needs_revision": "needs changes",
        "discard": "discard",
        "worth_acting_on": "worth acting on",
        "watch": "watch",
        "ignore": "ignore",
    }.get(decision or "", decision or "pending")


def resolution_label(resolution: str) -> str:
    return {
        "approve": "approve",
        "needs_changes": "needs changes",
        "discard": "discard",
        "ignore": "ignore",
        "build": "build",
        "promote": "promote",
        "wait": "wait",
    }.get(resolution or "", resolution or "pending")


def approval_intent_lines(item: dict[str, Any]) -> list[str]:
    flow = str(item.get("flow") or "")
    artifact_type = str(item.get("artifact_type") or "")

    if flow == "reviews_story" or artifact_type == "social_post":
        return [
            "You are approving: DuckAgent using this customer review as a social / review-story post.",
            "If approved, this is marketing content, not a reply back to the customer.",
        ]
    if flow == "reviews_reply_positive":
        return [
            "You are approving: DuckAgent posting this draft as a public Etsy reply to the customer.",
            "If approved, this reply can be posted on Etsy. This is not a social-media post.",
        ]
    if flow == "reviews_reply_private":
        return [
            "You are approving: DuckAgent sending this draft as a private customer-service recovery reply.",
            "If approved, this is customer support, not a public reply or a social-media post.",
        ]
    if flow == "newduck" or artifact_type == "listing":
        return [
            "You are approving: a new duck listing draft.",
            "If approved, DuckAgent can move this listing toward publication.",
        ]
    if flow == "weekly_sale" or artifact_type == "promotion":
        return [
            "You are approving: a weekly sale / promotion plan.",
            "If approved, DuckAgent can hand this promotion plan back to DuckAgent for direct weekly-sale publishing.",
            "If it needs work, reply `rewrite` first to see a stronger plan, then `needs changes <id> use rewrite` to send that exact feedback back into DuckAgent revise.",
        ]
    if flow == "meme":
        return [
            "You are approving: a Meme Monday social post package.",
            "If approved, DuckAgent can schedule this meme directly for social publishing. If you reply needs changes, DuckAgent can regenerate the draft.",
        ]
    if flow == "jeepfact":
        return [
            "You are approving: a Jeep Fact Wednesday social post package.",
            "If approved, DuckAgent can schedule this post directly for social publishing. If you reply needs changes, DuckAgent can regenerate the draft.",
            "If you want to steer the rewrite, reply `rewrite <id> <hint>` first, then `needs changes <id> use rewrite` to send a structured revise packet back into DuckAgent.",
        ]
    if artifact_type == "trend":
        return [
            "You are approving: OpenClaw's action recommendation for this trend.",
            "If approved, this updates the learning log for what to build, promote, or wait on.",
        ]
    return [
        "You are approving: this OpenClaw recommendation.",
    ]


def summarize_reasons(reasons: list[str], limit: int = 3) -> list[str]:
    cleaned = []
    for reason in reasons[:limit]:
        text = reason.strip()
        if text.startswith("Support score"):
            cleaned.append(text)
        elif text.startswith("Fail-closed trigger:"):
            cleaned.append(text.replace("Fail-closed trigger:", "Key blocker:", 1).strip())
        else:
            cleaned.append(text)
    return cleaned or ["No reasoning captured."]


def render_preview_lines(preview: dict[str, Any] | None) -> list[str]:
    if not preview:
        return []
    lines: list[str] = []
    context_text = (preview.get("context_text") or "").strip()
    proposed_text = (preview.get("proposed_text") or "").strip()
    asset_url = (preview.get("asset_url") or "").strip()
    asset_urls = [
        str(url).strip()
        for url in (preview.get("asset_urls") or [])
        if str(url).strip()
    ]
    if asset_url and asset_url not in asset_urls:
        asset_urls.insert(0, asset_url)
    if context_text:
        lines.extend(
            [
                "",
                f"{preview.get('context_label') or 'Context'}:",
                f"\"{context_text}\"",
            ]
        )
    if proposed_text:
        lines.extend(
            [
                "",
                f"{preview.get('proposed_label') or 'Proposed text'}:",
                f"\"{proposed_text}\"",
            ]
        )
    if asset_urls:
        lines.extend(
            [
                "",
                "Asset:",
                asset_urls[0],
            ]
        )
        if len(asset_urls) > 1:
            lines.extend(["", "Additional assets:"])
            for extra_url in asset_urls[1:5]:
                lines.append(str(extra_url))
    return lines


def load_weekly_sale_playbook(run_id: str | None) -> dict[str, Any]:
    run_id_text = str(run_id or "").strip()
    if not run_id_text:
        return {}
    state_path = DUCK_AGENT_RUNS_DIR / run_id_text / "state_weekly.json"
    state = load_json(state_path, {})
    sale_playbook = state.get("sale_playbook") if isinstance(state, dict) else {}
    return sale_playbook if isinstance(sale_playbook, dict) else {}


def summarize_sale_entries(entries: list[dict[str, Any]], limit: int = 4) -> str:
    parts: list[str] = []
    for entry in entries[:limit]:
        title = str(entry.get("product_title") or entry.get("title") or "").strip()
        discount = str(entry.get("discount") or "").strip()
        if not title:
            continue
        if discount:
            parts.append(f"{title} ({discount} off)")
        else:
            parts.append(title)
    return "; ".join(parts)


def weekly_sale_summary_lines(item: dict[str, Any]) -> list[str]:
    if str(item.get("flow") or "") != "weekly_sale":
        return []
    sale_playbook = load_weekly_sale_playbook(item.get("run_id"))
    if not sale_playbook:
        return []

    lines: list[str] = ["", "Sale targets in this plan:"]

    theme = sale_playbook.get("theme_of_the_week") or {}
    theme_name = str(theme.get("name") or "").strip()
    theme_discount = str(theme.get("discount") or "").strip()
    theme_platform = str(theme.get("platform") or "").strip()
    if theme_name:
        theme_bits = [theme_name]
        if theme_discount:
            theme_bits.append(f"{theme_discount} off")
        if theme_platform:
            theme_bits.append(theme_platform)
        lines.append("- Theme of the week: " + " | ".join(theme_bits))

    market_match = sale_playbook.get("market_match_recs") or []
    if isinstance(market_match, list) and market_match:
        summary = summarize_sale_entries(market_match, limit=3)
        if summary:
            lines.append(f"- Market match: {summary}")

    momentum = sale_playbook.get("momentum_boosters") or []
    if isinstance(momentum, list) and momentum:
        summary = summarize_sale_entries(momentum, limit=4)
        if summary:
            lines.append(f"- Momentum boosters: {summary}")

    reengagement = sale_playbook.get("re_engagement_recs") or []
    if isinstance(reengagement, list) and reengagement:
        summary = summarize_sale_entries(reengagement, limit=3)
        if summary:
            lines.append(f"- Re-engagement: {summary}")

    etsy_clearance = sale_playbook.get("etsy_clearance") or []
    if isinstance(etsy_clearance, list) and etsy_clearance:
        summary = summarize_sale_entries(etsy_clearance, limit=3)
        if summary:
            extra = ""
            if len(etsy_clearance) > 3:
                extra = f" (+{len(etsy_clearance) - 3} more)"
            lines.append(f"- Etsy clearance: {summary}{extra}")

    shopify_clearance = sale_playbook.get("shopify_clearance") or []
    if isinstance(shopify_clearance, list) and shopify_clearance:
        summary = summarize_sale_entries(shopify_clearance, limit=3)
        if summary:
            extra = ""
            if len(shopify_clearance) > 3:
                extra = f" (+{len(shopify_clearance) - 3} more)"
            lines.append(f"- Shopify clearance: {summary}{extra}")

    return lines if len(lines) > 1 else []


def weekly_sale_issue_summary_lines(item: dict[str, Any]) -> list[str]:
    if str(item.get("flow") or "") != "weekly_sale":
        return []
    metadata = item.get("quality_gate_metadata") or {}
    component_scores = metadata.get("component_scores") or {}
    fail_closed = [str(reason).strip() for reason in (metadata.get("fail_closed") or []) if str(reason).strip()]
    clarity_score = int(component_scores.get("clarity") or 0)
    conversion_score = int(component_scores.get("conversion_quality") or 0)

    lines = ["", "OpenClaw concern:"]
    if fail_closed or clarity_score <= 5 or conversion_score <= 5:
        lines.append("- This looks more incomplete than strategically wrong.")
        lines.append("- OpenClaw mostly wants the exact sale actions surfaced clearly enough to approve safely.")
        if fail_closed:
            lines.extend(f"- {reason}" for reason in fail_closed[:2])
        return lines

    lines.append("- OpenClaw thinks the current sale plan needs revision before approval.")
    return lines


def newduck_issue_summary_lines(item: dict[str, Any]) -> list[str]:
    if str(item.get("flow") or "") != "newduck":
        return []
    if str(item.get("decision") or "") == "publish_ready":
        return []

    metadata = item.get("quality_gate_metadata") or {}
    component_scores = metadata.get("component_scores") or {}
    fail_closed = [str(reason).strip() for reason in (metadata.get("fail_closed") or []) if str(reason).strip()]
    differentiation = int(component_scores.get("differentiation") or 0)
    clarity = int(component_scores.get("clarity") or 0)
    support = int(component_scores.get("support") or 0)

    lines = ["", "OpenClaw concern:"]
    if any("Existing catalog already covers this duck theme" in reason for reason in fail_closed) or differentiation <= 6:
        lines.append("- This usually means OpenClaw sees a differentiation or support problem, not necessarily that the listing itself is bad.")
        lines.append("- It wants clearer evidence that this duck is meaningfully distinct from what you already sell.")
    elif clarity <= 5:
        lines.append("- This looks more incomplete or unclear than strategically wrong.")
        lines.append("- OpenClaw wants the actual listing package surfaced more clearly before approving it.")
    else:
        lines.append("- OpenClaw wants a clearer reason to publish this listing now before approving it.")

    if support <= 13:
        lines.append("- The supporting evidence is fairly thin, so this can get noisy if old trend signals keep drifting around the same listing.")
    return lines


def weekly_sale_change_lines(item: dict[str, Any]) -> list[str]:
    if str(item.get("flow") or "") != "weekly_sale":
        return []
    sale_playbook = load_weekly_sale_playbook(item.get("run_id"))
    suggestions: list[str] = []
    metadata = item.get("quality_gate_metadata") or {}
    component_scores = metadata.get("component_scores") or {}
    clarity_score = int(component_scores.get("clarity") or 0)
    conversion_score = int(component_scores.get("conversion_quality") or 0)
    fail_closed = [str(reason).strip() for reason in (metadata.get("fail_closed") or []) if str(reason).strip()]

    if clarity_score <= 5 or fail_closed:
        suggestions.append("Keep the current targets and discounts, but surface the exact sale actions directly instead of only the strategic summary.")
    if conversion_score <= 5:
        suggestions.append("Lead with the theme-of-the-week sale first, then the top market-match and momentum items so the plan reads like concrete actions.")

    theme = sale_playbook.get("theme_of_the_week") if isinstance(sale_playbook, dict) else {}
    if isinstance(theme, dict) and theme.get("name"):
        bits = [str(theme.get("name")).strip()]
        if theme.get("discount"):
            bits.append(f"{theme.get('discount')} off")
        if theme.get("platform"):
            bits.append(str(theme.get("platform")).strip())
        suggestions.append("Make the main sale explicit: " + " | ".join(bit for bit in bits if bit))

    for explicit in item.get("improvement_suggestions") or []:
        text = str(explicit).strip()
        if text:
            suggestions.append(text)

    deduped: list[str] = []
    seen: set[str] = set()
    for suggestion in suggestions:
        if suggestion not in seen:
            deduped.append(suggestion)
            seen.add(suggestion)
    return deduped[:5]


def build_weekly_sale_rewrite_text(item: dict[str, Any], hint: str = "") -> str | None:
    if str(item.get("flow") or "") != "weekly_sale":
        return None
    sale_playbook = load_weekly_sale_playbook(item.get("run_id"))
    if not sale_playbook:
        return None

    hint_text = normalize_operator_note(hint).lower()
    shorter = "short" in hint_text

    lines: list[str] = []
    strategic_summary = str(sale_playbook.get("strategic_summary") or "").strip()
    theme = sale_playbook.get("theme_of_the_week") or {}
    theme_name = str(theme.get("name") or "").strip()
    theme_discount = str(theme.get("discount") or "").strip()
    theme_platform = str(theme.get("platform") or "").strip()
    if theme_name:
        headline_bits = [theme_name]
        if theme_discount:
            headline_bits.append(f"{theme_discount} off")
        if theme_platform:
            headline_bits.append(theme_platform)
        lines.append("Theme of the week: " + " | ".join(headline_bits))
    if strategic_summary and not shorter:
        lines.append(strategic_summary)

    sections = [
        ("Market match", sale_playbook.get("market_match_recs") or [], 2 if shorter else 3),
        ("Momentum boosters", sale_playbook.get("momentum_boosters") or [], 2 if shorter else 4),
        ("Re-engagement", sale_playbook.get("re_engagement_recs") or [], 2 if shorter else 3),
        ("Etsy clearance", sale_playbook.get("etsy_clearance") or [], 2 if shorter else 3),
        ("Shopify clearance", sale_playbook.get("shopify_clearance") or [], 2 if shorter else 3),
    ]
    for label, entries, limit in sections:
        if isinstance(entries, list) and entries:
            summary = summarize_sale_entries(entries, limit=limit)
            if summary:
                lines.append(f"{label}: {summary}")

    return "\n".join(line for line in lines if line.strip()) or None


def build_jeepfact_rewrite_text(item: dict[str, Any], hint: str = "") -> str | None:
    if str(item.get("flow") or "") != "jeepfact":
        return None

    hint_text = normalize_operator_note(hint).lower()
    selection_mode = "reroll_all"
    if "same ducks" in hint_text:
        selection_mode = "same_ducks_new_facts"
    elif "same facts" in hint_text or "new ducks" in hint_text or "different ducks" in hint_text:
        selection_mode = "new_ducks_same_facts"

    prefer_tags: list[str] = []
    avoid_tags: list[str] = []
    for tag in ("seasonal", "summer", "spring", "fall", "winter", "beach", "camping"):
        if tag in hint_text:
            prefer_tags.append("seasonal" if tag in {"summer", "spring", "fall", "winter"} else tag)
    if any(term in hint_text for term in ("avoid sports", "no sports", "without sports")):
        avoid_tags.append("sports")
    if any(term in hint_text for term in ("avoid patriotic", "no patriotic", "without patriotic")):
        avoid_tags.append("patriotic")

    hook_style = "punchy"
    if "curious" in hint_text:
        hook_style = "curious"
    elif "funny" in hint_text:
        hook_style = "funny"

    caption_tone = "standard"
    if "short" in hint_text:
        caption_tone = "shorter"
    elif "warm" in hint_text:
        caption_tone = "warmer"
    elif "educational" in hint_text:
        caption_tone = "educational"

    template_policy = "new_templates"
    if "keep template" in hint_text or "same template" in hint_text:
        template_policy = "keep_templates"

    operator_note = normalize_operator_note(hint) or "Pick a fresher duck slate, avoid recent repeats, and tighten the Jeep Fact package."
    lines = [
        "selection_mode: " + selection_mode,
        "avoid_recent_weeks: 6",
    ]
    if avoid_tags:
        lines.append("avoid_tags: " + ", ".join(dict.fromkeys(avoid_tags)))
    if prefer_tags:
        lines.append("prefer_tags: " + ", ".join(dict.fromkeys(prefer_tags)))
    lines.extend(
        [
            "hook_style: " + hook_style,
            "caption_tone: " + caption_tone,
            "template_policy: " + template_policy,
            "operator_note: " + operator_note,
        ]
    )
    return "\n".join(lines)


def _render_jeepfact_contract_card(contract_text: str) -> list[str]:
    values: dict[str, str] = {}
    for raw_line in contract_text.splitlines():
        if ":" not in raw_line:
            continue
        key, value = raw_line.split(":", 1)
        values[key.strip()] = value.strip()
    lines = [
        "Rewrite plan:",
        f"- Duck selection: {values.get('selection_mode', 'reroll_all')}",
        f"- Avoid recent weeks: {values.get('avoid_recent_weeks', '6')}",
    ]
    if values.get("avoid_tags"):
        lines.append(f"- Avoid tags: {values['avoid_tags']}")
    if values.get("prefer_tags"):
        lines.append(f"- Prefer tags: {values['prefer_tags']}")
    if values.get("prefer_categories"):
        lines.append(f"- Prefer categories: {values['prefer_categories']}")
    lines.extend(
        [
            f"- Hook style: {values.get('hook_style', 'punchy')}",
            f"- Caption tone: {values.get('caption_tone', 'standard')}",
            f"- Template policy: {values.get('template_policy', 'new_templates')}",
        ]
    )
    if values.get("operator_note"):
        lines.extend(["", "Operator note:", values["operator_note"]])
    lines.extend(["", "Raw contract:", contract_text])
    return lines


def build_quality_gate_items(state: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for artifact_id, record in state.get("artifacts", {}).items():
        decision = record.get("decision") or {}
        if decision.get("review_status") != "pending":
            continue
        items.append(
            {
                "artifact_id": artifact_id,
                "decision": decision.get("decision"),
                "score": decision.get("score"),
                "confidence": decision.get("confidence"),
                "priority": decision.get("priority"),
                "flow": decision.get("flow"),
                "run_id": decision.get("run_id"),
                "title": decision.get("title"),
                "created_at": decision.get("created_at"),
                "reasoning": decision.get("reasoning") or [],
                "improvement_suggestions": decision.get("improvement_suggestions") or [],
                "evidence_refs": decision.get("evidence_refs") or [],
                "preview": decision.get("preview") or {},
                "review_status": decision.get("review_status"),
                "artifact_type": decision.get("artifact_type") or "publish",
                "action_frame": decision.get("action_frame"),
                "state_source": "quality_gate",
                "first_reason": (decision.get("reasoning") or ["No reasoning captured."])[0],
                "output_paths": record.get("output_paths", {}),
                "input_hash": record.get("input_hash"),
                "material_hash": record.get("material_hash"),
            }
        )
    return items


def collect_trend_items(state: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    concept_records = build_trend_concepts(state.get("artifacts") or {})
    for concept in concept_records:
        if not concept.get("operator_surface"):
            continue
        item = {
            "artifact_id": concept.get("artifact_id"),
            "concept_id": concept.get("concept_id"),
            "related_artifact_ids": concept.get("related_artifact_ids") or [],
            "concept_aliases": concept.get("concept_aliases") or [],
            "decision": concept.get("decision"),
            "score": concept.get("score"),
            "confidence": concept.get("confidence"),
            "priority": concept.get("priority"),
            "flow": "trend_ranker",
            "run_id": concept.get("created_at"),
            "title": concept.get("title") or concept.get("theme") or concept.get("artifact_id"),
            "created_at": concept.get("created_at"),
            "reasoning": concept.get("reasoning") or [],
            "improvement_suggestions": concept.get("improvement_suggestions") or [],
            "evidence_refs": concept.get("evidence_refs") or [],
            "preview": {},
            "review_status": concept.get("review_status"),
            "artifact_type": "trend",
            "action_frame": concept.get("action_frame"),
            "state_source": "trend_ranker",
            "theme": concept.get("theme"),
            "trend_metadata": concept.get("trend_metadata") or {},
            "first_reason": (concept.get("reasoning") or ["No reasoning captured."])[0],
            "output_paths": concept.get("output_paths", {}),
            "human_review": concept.get("human_review"),
            "previous_human_review": concept.get("previous_human_review"),
            "manual_review_requested": concept.get("manual_review_requested"),
            "carried_review": concept.get("carried_review"),
            "carried_forward_from_artifact_id": concept.get("carried_forward_from_artifact_id"),
        }
        if not should_surface_trend_item(item):
            continue
        items.append(item)
    items.sort(key=item_sort_key, reverse=True)
    return dedupe_trend_items(items)


def build_trend_items(state: dict[str, Any]) -> list[dict[str, Any]]:
    return collect_trend_items(state)[:MAX_TREND_OPERATOR_ITEMS]


def normalize_theme_text(value: str) -> str:
    text = (value or "").lower()
    text = re.sub(r"\bpolice\s+officer\b", "police", text)
    text = re.sub(r"\bbody\s+builder\b", "bodybuilder", text)
    text = re.sub(r"\bwhite\s+tailed\b", "whitetail", text)
    text = re.sub(r"\bwhite\s+tail\b", "whitetail", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def trend_dedupe_signature(item: dict[str, Any]) -> str:
    theme = item.get("theme") or item.get("title") or item.get("artifact_id") or ""
    tokens = [token for token in normalize_theme_text(theme).split() if token not in TREND_DEDUPE_IGNORED_TOKENS]
    return " ".join(tokens) or normalize_theme_text(theme)


def trend_latest_marker(item: dict[str, Any]) -> str:
    metadata = item.get("trend_metadata") or {}
    return str(
        metadata.get("latest_observed_at")
        or metadata.get("first_seen_at")
        or item.get("created_at")
        or item.get("run_id")
        or ""
    )


def should_surface_trend_item(item: dict[str, Any]) -> bool:
    theme_value = normalize_theme_text(str(item.get("theme") or item.get("title") or ""))
    if theme_value in GENERIC_TREND_TITLES:
        return False
    action = str(item.get("action_frame") or "wait")
    if action in {"build", "promote"}:
        return True
    if action != "wait":
        return False
    return bool(
        item.get("manual_review_requested")
        or item.get("previous_human_review")
        or item.get("human_review")
    )


def trend_priority_tuple(item: dict[str, Any]) -> tuple[int, float, float, str]:
    return (
        1 if item.get("manual_review_requested") else 0,
        trend_latest_marker(item),
        int(item.get("score") or 0),
        float(item.get("confidence") or 0.0),
        float(priority_rank(item.get("priority", "low"))),
    )


def dedupe_trend_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    kept: dict[str, dict[str, Any]] = {}
    for item in items:
        signature = trend_dedupe_signature(item)
        existing = kept.get(signature)
        if existing is None or trend_priority_tuple(item) > trend_priority_tuple(existing):
            kept[signature] = item
    deduped = list(kept.values())
    deduped.sort(key=item_sort_key, reverse=True)
    return deduped


def build_review_items(state_bundle: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    items = build_quality_gate_items(state_bundle.get("quality_gate", {}))
    items.extend(build_trend_items(state_bundle.get("trend_ranker", {})))
    items.sort(
        key=item_sort_key,
        reverse=True,
    )
    return annotate_review_freshness(items)


def annotate_review_freshness(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for item in items:
        age_days = item_age_days(item.get("created_at"))
        is_fresh = age_days is not None and age_days <= FRESH_REVIEW_WINDOW_DAYS
        if age_days is None:
            freshness_label = "unknown"
        elif is_fresh:
            freshness_label = "new"
        else:
            freshness_label = "backlog"
        item["age_days"] = age_days
        item["is_fresh"] = is_fresh
        item["freshness_label"] = freshness_label
    return items


def surfaced_review_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [item for item in items if item.get("is_fresh")]


def assign_short_ids(items: list[dict[str, Any]], operator_state: dict[str, Any]) -> None:
    mapping = operator_state.setdefault("artifact_short_ids", {})
    next_short_id = int(operator_state.get("next_short_id", SHORT_ID_START))
    for item in items:
        artifact_id = item["artifact_id"]
        if artifact_id not in mapping:
            mapping[artifact_id] = next_short_id
            next_short_id += 1
        item["short_id"] = int(mapping[artifact_id])
    operator_state["next_short_id"] = next_short_id


def sync_current_item(items: list[dict[str, Any]], operator_state: dict[str, Any]) -> dict[str, Any] | None:
    surfaced_items = surfaced_review_items(items)
    pending_ids = [item["artifact_id"] for item in surfaced_items]
    current_artifact_id = operator_state.get("current_artifact_id")
    item_by_artifact = {item["artifact_id"]: item for item in surfaced_items}
    review_items = [item for item in surfaced_items if (item.get("flow") or "").startswith("reviews")]
    preferred_item = review_items[0] if review_items else (surfaced_items[0] if surfaced_items else None)
    if current_artifact_id not in pending_ids:
        current_artifact_id = preferred_item["artifact_id"] if preferred_item else None
    elif surfaced_items and current_artifact_id not in item_by_artifact:
        current_artifact_id = preferred_item["artifact_id"] if preferred_item else None
    operator_state["current_artifact_id"] = current_artifact_id
    operator_state["last_queue_generated_at"] = now_iso()
    if not current_artifact_id:
        return None
    for item in surfaced_items:
        if item["artifact_id"] == current_artifact_id:
            return item
    return None


def render_operator_card(item: dict[str, Any], include_help: bool = True) -> str:
    if item.get("artifact_type") == "trend":
        aliases = [alias for alias in (item.get("concept_aliases") or []) if alias and alias != item.get("title")]
        lines = [
            f"OpenClaw Trend {item['short_id']}",
            f"{item.get('title') or item.get('artifact_id')}",
            "",
            *approval_intent_lines(item),
            f"Recommendation: {resolution_label(item.get('action_frame'))}",
            f"Trend status: {decision_label(item.get('decision'))}",
            f"Confidence: {item.get('confidence')}",
            f"Priority: {item.get('priority')}",
        ]
        if aliases:
            lines.append("Seen as: " + ", ".join(aliases[:3]))
        lines.extend(["", "Why:"])
        lines.extend(
            f"{index}. {reason}"
            for index, reason in enumerate(summarize_reasons(item.get("reasoning") or []), start=1)
        )
        if include_help:
            lines.extend(
                [
                    "",
                    "Reply:",
                    "agree",
                    "build <short reason>",
                    "promote <short reason>",
                    "wait <short reason>",
                    "ignore <short reason>",
                    "same as <your product name>",
                    "why",
                    "next",
                ]
            )
        return "\n".join(lines)

    lines = [
        f"OpenClaw Review {item['short_id']}",
        f"{item.get('title') or item.get('artifact_id')}",
        "",
        *approval_intent_lines(item),
        f"Recommendation: {decision_label(item.get('decision'))}",
        f"Confidence: {item.get('confidence')}",
        f"Priority: {item.get('priority')}",
    ]
    lines.extend(render_preview_lines(item.get("preview")))
    lines.extend(weekly_sale_summary_lines(item))
    if str(item.get("flow") or "") == "weekly_sale" and str(item.get("decision") or "") != "publish_ready":
        lines.extend(weekly_sale_issue_summary_lines(item))
        sale_changes = weekly_sale_change_lines(item)
        if sale_changes:
            lines.extend(["", "What OpenClaw would change now:"])
            lines.extend(f"- {suggestion}" for suggestion in sale_changes[:4])
        lines.extend(
            [
                "",
                "Fast rewrite path:",
                f"rewrite {item['short_id']}",
                f"needs changes {item['short_id']} use rewrite",
            ]
        )
    if str(item.get("flow") or "") == "newduck":
        lines.extend(newduck_issue_summary_lines(item))
    lines.extend(["", "Why:"])
    lines.extend(f"{index}. {reason}" for index, reason in enumerate(summarize_reasons(item.get("reasoning") or []), start=1))
    if include_help:
        lines.extend(
            [
                "",
                "Reply:",
                "agree",
                "approve <short reason>",
                "needs changes <short reason>",
                "discard <short reason>",
                "why",
                "suggest changes",
                "rewrite",
                "next",
            ]
        )
    return "\n".join(lines)


def render_operator_detail(item: dict[str, Any]) -> str:
    if item.get("artifact_type") == "trend":
        metadata = item.get("trend_metadata") or {}
        match_titles = [m.get("title") or m.get("handle") or "existing product" for m in metadata.get("matching_products", [])[:3]]
        aliases = [alias for alias in (item.get("concept_aliases") or []) if alias and alias != item.get("title")]
        lines = [
            f"OpenClaw Detail {item['short_id']}",
            f"{item.get('title') or item.get('artifact_id')}",
            "",
            *approval_intent_lines(item),
            f"Recommendation: {resolution_label(item.get('action_frame'))}",
            f"Trend status: {decision_label(item.get('decision'))}",
            f"Score: {item.get('score')}",
            f"Confidence: {item.get('confidence')}",
            f"Priority: {item.get('priority')}",
            f"Catalog status: {metadata.get('catalog_status') or 'unknown'}",
        ]
        if aliases:
            lines.append("Seen as: " + ", ".join(aliases[:5]))
        if match_titles:
            lines.append("Matching products: " + ", ".join(match_titles))
        related_artifact_ids = item.get("related_artifact_ids") or []
        if len(related_artifact_ids) > 1:
            lines.append(f"Related trend artifacts: {len(related_artifact_ids)}")
        lines.extend(["", "Reasoning:"])
        lines.extend(f"- {reason}" for reason in (item.get("reasoning") or ["No reasoning captured."]))
        suggestions = item.get("improvement_suggestions") or []
        if suggestions:
            lines.extend(["", "What would change this:"])
            lines.extend(f"- {suggestion}" for suggestion in suggestions[:4])
        refs = item.get("evidence_refs") or []
        if refs:
            lines.extend(["", "Evidence:"])
            lines.extend(f"- {ref}" for ref in refs[:5])
        return "\n".join(lines)

    lines = [
        f"OpenClaw Detail {item['short_id']}",
        f"{item.get('title') or item.get('artifact_id')}",
        "",
        *approval_intent_lines(item),
        f"Recommendation: {decision_label(item.get('decision'))}",
        f"Score: {item.get('score')}",
        f"Confidence: {item.get('confidence')}",
        f"Priority: {item.get('priority')}",
    ]
    lines.extend(render_preview_lines(item.get("preview")))
    lines.extend(weekly_sale_summary_lines(item))
    if str(item.get("flow") or "") == "weekly_sale" and str(item.get("decision") or "") != "publish_ready":
        lines.extend(weekly_sale_issue_summary_lines(item))
        sale_changes = weekly_sale_change_lines(item)
        if sale_changes:
            lines.extend(["", "What OpenClaw would change now:"])
            lines.extend(f"- {suggestion}" for suggestion in sale_changes[:5])
    if str(item.get("flow") or "") == "newduck":
        lines.extend(newduck_issue_summary_lines(item))
    lines.extend(["", "Reasoning:"])
    lines.extend(f"- {reason}" for reason in (item.get("reasoning") or ["No reasoning captured."]))
    suggestions = item.get("improvement_suggestions") or []
    if suggestions:
        lines.extend(["", "Suggestions:"])
        lines.extend(f"- {suggestion}" for suggestion in suggestions[:4])
    refs = item.get("evidence_refs") or []
    if refs:
        lines.extend(["", "Evidence:"])
        lines.extend(f"- {ref}" for ref in refs[:5])
    return "\n".join(lines)


def render_preview_focus(item: dict[str, Any], focus: str) -> str:
    preview = item.get("preview") or {}
    title = item.get("title") or item.get("artifact_id")
    if focus == "reply":
        proposed_text = (preview.get("proposed_text") or "").strip()
        proposed_label = preview.get("proposed_label") or "Draft reply"
        if proposed_text:
            return f"{title}\n\n{proposed_label}:\n\"{proposed_text}\""
        return f"{title}\n\nNo draft reply text is attached to this item."
    if focus == "review":
        context_text = (preview.get("context_text") or "").strip()
        context_label = preview.get("context_label") or "Customer review"
        proposed_text = (preview.get("proposed_text") or "").strip()
        proposed_label = preview.get("proposed_label") or "Selected review"
        if context_text:
            return f"{title}\n\n{context_label}:\n\"{context_text}\""
        if proposed_text:
            return f"{title}\n\n{proposed_label}:\n\"{proposed_text}\""
        return f"{title}\n\nNo customer-review text is attached to this item."
    if focus == "asset":
        asset_url = (preview.get("asset_url") or "").strip()
        if asset_url:
            return f"{title}\n\nAsset:\n{asset_url}"
        return f"{title}\n\nNo asset URL is attached to this item."
    return render_operator_detail(item)


def normalize_operator_text(value: str | None) -> str:
    text = html.unescape(value or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\[[^\]]+\]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_operator_note(value: str | None) -> str:
    text = normalize_operator_text(value)
    lowered = text.lower()
    if lowered.startswith("because "):
        return text[8:].strip()
    return text


def dedupe_phrases(values: list[str]) -> list[str]:
    kept: list[str] = []
    seen: set[str] = set()
    for value in values:
        compact = value.strip()
        if not compact:
            continue
        key = compact.lower()
        if key in seen:
            continue
        seen.add(key)
        kept.append(compact)
    return kept


def _review_recipient(review_text: str) -> str:
    lowered = review_text.lower()
    return next(
        (
            term
            for term in ("friend", "daughter", "son", "wife", "husband", "mom", "mother", "dad", "father", "sister", "brother")
            if term in lowered
        ),
        "",
    )


def public_reply_detail_lines(review_text: str, draft_text: str = "") -> list[str]:
    lowered = review_text.lower()
    draft_lowered = draft_text.lower()
    details: list[str] = []
    recipient = _review_recipient(review_text)

    if "gift" in lowered and recipient:
        details.append(f"I'm so glad it made such a great gift for your {recipient}.")
    elif "gift" in lowered:
        details.append("I'm so glad it made such a great gift.")

    if any(term in lowered for term in ("fast shipping", "quick shipping", "arrived quickly", "arrived fast", "shipping was fast")):
        details.append("I'm so glad it arrived quickly.")

    if "jeep" in lowered or "dash" in lowered:
        details.append("I'm glad it looks right at home on the dash.")

    if "exactly as described" in lowered or "as described" in lowered or "no disappointments" in lowered:
        details.append("I'm so glad it was exactly what you hoped for.")

    if "recommend" in lowered:
        details.append("I really appreciate the recommendation.")

    if "love" in lowered and not any("great gift" in detail.lower() for detail in details):
        details.append("I'm so glad you love it.")

    if "quality" in lowered and any(term in lowered for term in ("cute", "adorable", "fun", "funny")):
        details.append("I'm glad the quality and the fun of it both came through.")
    elif "quality" in lowered:
        details.append("I'm glad the quality came through.")

    if recipient in {"daughter", "son"} or "kids" in lowered:
        details.append("I'm glad it got such a good reaction.")

    if "laugh" in lowered or "cracking up" in lowered or "funny" in lowered:
        details.append("I'm glad it got a laugh.")
    elif "cute" in lowered or "adorable" in lowered:
        details.append("I'm glad it gave you a smile.")

    if draft_lowered and "arrived quickly" in draft_lowered and not any("arrived quickly" in detail.lower() for detail in details):
        details.append("I'm so glad it arrived quickly.")

    if "perfect" in lowered and not any("perfect" in detail.lower() for detail in details):
        details.append("I'm so glad it was a perfect fit.")
    elif "great" in lowered and not details:
        details.append("I'm so glad it worked out so well.")

    return dedupe_phrases(details)


def private_reply_issue_line(review_text: str) -> str:
    lowered = review_text.lower()
    if "3d print" in lowered or "3d printed" in lowered or "micro plastic" in lowered:
        return "I understand why the material and listing wording felt misleading."
    if "misleading" in lowered or "not disclosed" in lowered:
        return "I understand why that felt misleading."
    if "broken" in lowered or "damaged" in lowered:
        return "I'm sorry it arrived in that condition."
    if "late" in lowered or "shipping" in lowered:
        return "I'm sorry the shipping experience fell short."
    if "cheap" in lowered or "crap" in lowered or "disappointed" in lowered:
        return "I understand why you were disappointed."
    return "I understand why this missed the mark for you."


def private_reply_remedy_line(draft_text: str) -> str:
    lowered = draft_text.lower()
    if "refund" in lowered and "replacement" in lowered:
        return "Please reply here and I can help with a refund or replacement."
    if "replacement" in lowered:
        return "Please reply here and I can help with a replacement."
    if "refund" in lowered:
        return "Please reply here and I can help with a refund."
    if "make this right" in lowered or "make things right" in lowered:
        return "Please reply here and I'll work with you to make this right."
    return "Please reply here and I'll help however I can."


def build_rewrite_suggestion_text(item: dict[str, Any], hint: str = "") -> str | None:
    if str(item.get("flow") or "") == "weekly_sale":
        return build_weekly_sale_rewrite_text(item, hint=hint)
    if str(item.get("flow") or "") == "jeepfact":
        return build_jeepfact_rewrite_text(item, hint=hint)

    if item.get("artifact_type") != "review_reply":
        return None

    preview = item.get("preview") or {}
    review_text = normalize_operator_text(preview.get("context_text"))
    draft_text = normalize_operator_text(preview.get("proposed_text"))
    hint_text = normalize_operator_note(hint).lower()
    shorter = "short" in hint_text
    warmer = "warm" in hint_text

    if item.get("flow") == "reviews_reply_private":
        opening = "Thank you for reaching out, and I'm sorry this missed the mark."
        if shorter:
            opening = "I'm sorry this missed the mark."
        if warmer and not shorter:
            opening = "Thank you for reaching out, and I'm really sorry this missed the mark."
        parts = [
            opening,
            private_reply_issue_line(review_text),
            private_reply_remedy_line(draft_text),
        ]
        return " ".join(dedupe_phrases(parts))

    opening = "Thank you so much for the kind review!"
    if shorter:
        opening = "Thanks so much for the review!"
    elif warmer:
        opening = "Thank you so much for the kind review!"

    details = public_reply_detail_lines(review_text, draft_text)
    if shorter and details:
        details = details[:1]
    elif len(details) > 2:
        details = details[:2]

    closing = "Thanks again for the kind review."
    if shorter:
        closing = "Thanks again for the review."
    if any("recommendation" in detail.lower() for detail in details):
        closing = "Thanks again for the recommendation."

    parts = [opening, *details]
    if len(parts) < 3:
        parts.append(closing)
    return " ".join(dedupe_phrases(parts))


def render_rewrite_suggestion(item: dict[str, Any], hint: str = "") -> str:
    if str(item.get("flow") or "") == "weekly_sale":
        rewrite_text = build_weekly_sale_rewrite_text(item, hint=hint)
        if not rewrite_text:
            return "I couldn't build a rewritten sale playbook for this item yet."
        hint_suffix = f" ({normalize_operator_note(hint)})" if normalize_operator_note(hint) else ""
        return "\n".join(
            [
                f"OpenClaw Rewrite {item['short_id']}{hint_suffix}",
                f"{item.get('title') or item.get('artifact_id')}",
                "",
                "Suggested revised sale plan:",
                rewrite_text,
                "",
                f"If this is the direction you want, reply `needs changes {item['short_id']} use rewrite` and DuckAgent will regenerate the sale with this feedback.",
            ]
        )
    if str(item.get("flow") or "") == "jeepfact":
        rewrite_text = build_jeepfact_rewrite_text(item, hint=hint)
        if not rewrite_text:
            return "I couldn't build a Jeep Fact rewrite contract for this item yet."
        hint_suffix = f" ({normalize_operator_note(hint)})" if normalize_operator_note(hint) else ""
        return "\n".join(
            [
                f"OpenClaw Rewrite {item['short_id']}{hint_suffix}",
                f"{item.get('title') or item.get('artifact_id')}",
                "",
                "Suggested revised Jeep Fact contract:",
                *_render_jeepfact_contract_card(rewrite_text),
                "",
                f"If this is the direction you want, reply `needs changes {item['short_id']} use rewrite` and DuckAgent will regenerate Jeep Fact with this exact contract.",
            ]
        )

    if item.get("artifact_type") != "review_reply":
        return "That command only works on review-reply and weekly-sale items. For trends, use `why` or `suggest changes`."

    rewrite_text = build_rewrite_suggestion_text(item, hint=hint)
    if not rewrite_text:
        return "I couldn't build a rewritten reply for this item yet."

    hint_suffix = f" ({normalize_operator_note(hint)})" if normalize_operator_note(hint) else ""
    return "\n".join(
        [
            f"OpenClaw Rewrite {item['short_id']}{hint_suffix}",
            f"{item.get('title') or item.get('artifact_id')}",
            "",
            "Suggested reply:",
            f"\"{rewrite_text}\"",
            "",
            f"If you want to approve this version, reply `approve {item['short_id']} because use rewrite`.",
            "For Etsy public replies, that approval will also queue the rewritten text for execution.",
        ]
    )


def note_requests_cached_rewrite(note: str | None) -> bool:
    normalized = normalize_operator_note(note).lower()
    return "use rewrite" in normalized or normalized in {"rewrite", "use rewritten reply", "use rewritten version"}


def derive_change_suggestions(item: dict[str, Any]) -> list[str]:
    explicit = [suggestion.strip() for suggestion in (item.get("improvement_suggestions") or []) if suggestion and suggestion.strip()]
    if explicit:
        return explicit[:4]

    suggestions: list[str] = []
    for reason in item.get("reasoning") or []:
        lowered = reason.lower()
        if "clarity score" in lowered:
            suggestions.append("Make the draft more specific and concrete so it is clear what would actually be posted or sent.")
        elif "differentiation score" in lowered:
            suggestions.append("Tie the response more directly to what the customer actually said so it feels less templated.")
        elif "conversion-quality score" in lowered:
            suggestions.append("Strengthen the draft so the message is more useful and less generic.")
        elif "brand-fit score" in lowered:
            suggestions.append("Tighten the tone so it sounds more like your brand and less like a generic assistant.")
        elif "support score" in lowered:
            suggestions.append("Add more evidence or context before approving so the draft is clearly backed by the source material.")
        elif "fail-closed trigger" in lowered:
            blocker = reason.split(":", 1)[-1].strip()
            suggestions.append(f"Fix the blocker first: {blocker}")
    if not suggestions:
        suggestions.append("Revise the draft to be more specific, shorter, and more obviously tied to the source material.")

    deduped: list[str] = []
    seen: set[str] = set()
    for suggestion in suggestions:
        if suggestion not in seen:
            deduped.append(suggestion)
            seen.add(suggestion)
    return deduped[:4]


def render_change_suggestions(item: dict[str, Any]) -> str:
    if item.get("artifact_type") == "trend":
        suggestions = [s.strip() for s in (item.get("improvement_suggestions") or []) if s and s.strip()]
        lines = [
            f"OpenClaw Suggestions {item['short_id']}",
            f"{item.get('title') or item.get('artifact_id')}",
            "",
            "What would change this:",
        ]
        if suggestions:
            lines.extend(f"- {suggestion}" for suggestion in suggestions[:4])
        else:
            lines.append("- Wait for stronger corroboration or a clearer catalog signal before changing this call.")
        return "\n".join(lines)

    lines = [
        f"OpenClaw Suggestions {item['short_id']}",
        f"{item.get('title') or item.get('artifact_id')}",
        "",
        "Suggested changes:",
    ]
    if str(item.get("flow") or "") == "weekly_sale":
        lines.extend(f"- {suggestion}" for suggestion in weekly_sale_change_lines(item))
        issue_lines = weekly_sale_issue_summary_lines(item)
        if issue_lines:
            lines.extend(issue_lines)
        rewrite_text = build_weekly_sale_rewrite_text(item)
        if rewrite_text:
            lines.extend(
                [
                    "",
                    "Suggested revised sale plan:",
                    rewrite_text,
                ]
            )
        return "\n".join(lines)

    lines.extend(f"- {suggestion}" for suggestion in derive_change_suggestions(item))
    preview = item.get("preview") or {}
    proposed_label = preview.get("proposed_label") or "Draft"
    proposed_text = (preview.get("proposed_text") or "").strip()
    if proposed_text:
        lines.extend(
            [
                "",
                f"Current {proposed_label.lower()}:",
                f"\"{proposed_text}\"",
            ]
        )
    return "\n".join(lines)


def write_operator_outputs(items: list[dict[str, Any]], current_item: dict[str, Any] | None) -> dict[str, str]:
    patterns = load_output_patterns()
    surfaced_items = surfaced_review_items(items)
    queue_payload = {
        "generated_at": now_iso(),
        "pending_count": len(surfaced_items),
        "pending_count_all": len(items),
        "current_short_id": current_item.get("short_id") if current_item else None,
        "current_artifact_id": current_item.get("artifact_id") if current_item else None,
        "items": items,
        "surfaced_items": surfaced_items,
    }

    queue_json = render_pattern(patterns["operator_queue_json"], {})
    queue_md = render_pattern(patterns["operator_queue_md"], {})
    write_json(queue_json, queue_payload)

    queue_lines = [
        "# Operator Queue",
        "",
        f"- Generated at: `{queue_payload['generated_at']}`",
        f"- New/material items surfaced now: `{queue_payload['pending_count']}`",
        f"- Full pending backlog: `{queue_payload['pending_count_all']}`",
        "",
    ]
    if not surfaced_items:
        if items:
            queue_lines.append("No new operator items are surfaced right now. Use `status all` to inspect older backlog.")
        else:
            queue_lines.append("No pending operator items.")
    else:
        for item in surfaced_items:
            queue_lines.append(
                f"- `{item['short_id']}` | `{decision_label(item['decision'])}` | `{item['priority']}` | `{item['title']}`"
            )
            queue_lines.append(f"  Reason: {item['first_reason']}")
    ensure_parent(queue_md).write_text("\n".join(queue_lines) + "\n", encoding="utf-8")

    current_json = render_pattern(patterns["operator_current_json"], {})
    current_md = render_pattern(patterns["operator_current_md"], {})
    current_message = (
        render_operator_card(current_item)
        if current_item
        else "No new pending reviews right now.\n\nUse `status` for the summary or `status all` to inspect older backlog."
        if items
        else "No pending reviews right now."
    )
    current_payload = {
        "generated_at": now_iso(),
        "current": current_item,
        "message": current_message,
    }
    write_json(current_json, current_payload)
    ensure_parent(current_md).write_text(current_payload["message"] + "\n", encoding="utf-8")

    detail_paths: dict[str, str] = {}
    for item in items:
        detail_json = render_pattern(patterns["operator_detail_json"], {"<short_id>": str(item["short_id"]), "short_id": str(item["short_id"])})
        detail_md = render_pattern(patterns["operator_detail_md"], {"<short_id>": str(item["short_id"]), "short_id": str(item["short_id"])})
        detail_payload = {
            "generated_at": now_iso(),
            "item": item,
            "message": render_operator_detail(item),
        }
        write_json(detail_json, detail_payload)
        ensure_parent(detail_md).write_text(detail_payload["message"] + "\n", encoding="utf-8")
        detail_paths[str(item["short_id"])] = str(detail_md)

    return {
        "queue_json": str(queue_json),
        "queue_md": str(queue_md),
        "current_json": str(current_json),
        "current_md": str(current_md),
        "detail_paths": detail_paths,
    }


def write_review_queue(state_bundle: dict[str, dict[str, Any]], operator_state: dict[str, Any] | None = None) -> dict[str, Any]:
    reconcile_state_bundle(state_bundle)
    if archive_stale_quality_gate_items(state_bundle.get("quality_gate", {})):
        write_state_source("quality_gate", state_bundle["quality_gate"])
    items = build_review_items(state_bundle)
    local_operator_state = operator_state or load_operator_state()
    assign_short_ids(items, local_operator_state)
    current_item = sync_current_item(items, local_operator_state)
    surfaced_items = surfaced_review_items(items)

    payload = {
        "generated_at": now_iso(),
        "pending_count": len(surfaced_items),
        "pending_count_all": len(items),
        "current_short_id": current_item.get("short_id") if current_item else None,
        "items": items,
        "surfaced_items": surfaced_items,
    }
    write_json(REVIEW_QUEUE_STATE_PATH, payload)
    customer_queue = load_json(CUSTOMER_INTERACTION_QUEUE_PATH, {})
    sync_ops_control(customer_queue if isinstance(customer_queue, dict) else {}, payload)

    patterns = load_output_patterns()
    current = datetime.now()
    replacements = {"YYYY-MM-DD": current.strftime("%Y-%m-%d")}
    json_path = render_pattern(patterns["review_queue_json"], replacements)
    md_path = render_pattern(patterns["review_queue_md"], replacements)
    write_json(json_path, payload)

    lines = [
        "# Review Queue",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- New/material items surfaced now: `{payload['pending_count']}`",
        f"- Full pending backlog: `{payload['pending_count_all']}`",
    ]
    if current_item:
        lines.append(f"- Current short ID: `{current_item['short_id']}`")
    lines.append("")
    if not surfaced_items:
        if items:
            lines.append("No new items are surfaced right now. Older backlog remains available via `status all`.")
        else:
            lines.append("No pending review items.")
    else:
        for item in surfaced_items:
            lines.append(
                f"- `{item['short_id']}` | `{item['priority']}` | `{item['decision']}` | score `{item['score']}` | confidence `{item['confidence']}` | `{item['title']}`"
            )
            lines.append(f"  Reason: {item['first_reason']}")
            if item.get("output_paths", {}).get("md_path"):
                lines.append(f"  Review file: `{item['output_paths']['md_path']}`")
    if items and len(items) > len(surfaced_items):
        lines.extend(["", "## Older Pending Backlog", ""])
        for item in items:
            if item.get("is_fresh"):
                continue
            lines.append(
                f"- `{item['short_id']}` | `{item['priority']}` | `{item['decision']}` | `{item['title']}`"
            )
            lines.append(f"  First reason: {item['first_reason']}")
    ensure_parent(md_path).write_text("\n".join(lines) + "\n", encoding="utf-8")

    operator_paths = write_operator_outputs(items, current_item)
    write_operator_state(local_operator_state)
    return {
        "review_queue_json": str(json_path),
        "review_queue_md": str(md_path),
        **operator_paths,
    }


def index_items(items: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[int, dict[str, Any]]]:
    by_artifact = {item["artifact_id"]: item for item in items}
    by_short_id = {int(item["short_id"]): item for item in items}
    return by_artifact, by_short_id


def next_item(items: list[dict[str, Any]], current_item: dict[str, Any] | None) -> dict[str, Any] | None:
    if not items:
        return None
    if current_item is None:
        return items[0]
    for index, item in enumerate(items):
        if item["artifact_id"] == current_item["artifact_id"]:
            if index + 1 < len(items):
                return items[index + 1]
            return items[0]
    return items[0]


def resolve_target_item(
    items: list[dict[str, Any]],
    operator_state: dict[str, Any],
    token: str | None,
) -> dict[str, Any] | None:
    _, by_short_id = index_items(items)
    if token:
        try:
            return by_short_id.get(int(token))
        except ValueError:
            return None
    current_artifact_id = operator_state.get("current_artifact_id")
    for item in items:
        if item["artifact_id"] == current_artifact_id:
            return item
    return items[0] if items else None


def parse_command(text: str) -> tuple[str, str | None, str]:
    raw = (text or "").strip()
    if not raw:
        return "help", None, ""
    lowered = raw.lower()
    if lowered == "status all" or lowered == "queue all" or lowered == "backlog":
        return "status_all", None, ""
    if lowered == "health":
        return "health", None, ""
    if lowered.startswith("health "):
        return "health", None, raw.split(" ", 1)[1].strip()
    if lowered.startswith("suggest changes"):
        parts = raw.split()
        target_token = parts[2] if len(parts) > 2 and parts[2].isdigit() else None
        return "suggest_changes", target_token, ""
    if lowered.startswith("suggest change"):
        parts = raw.split()
        target_token = parts[2] if len(parts) > 2 and parts[2].isdigit() else None
        return "suggest_changes", target_token, ""
    if lowered.startswith("suggestions"):
        parts = raw.split()
        target_token = parts[1] if len(parts) > 1 and parts[1].isdigit() else None
        return "suggest_changes", target_token, ""
    if lowered.startswith("needs changes"):
        parts = raw.split()
        target_token = parts[2] if len(parts) > 2 and parts[2].isdigit() else None
        note_start = 3 if target_token else 2
        note = " ".join(parts[note_start:]).strip()
        return "needs_changes", target_token, note
    if lowered.startswith("needs change"):
        parts = raw.split()
        target_token = parts[2] if len(parts) > 2 and parts[2].isdigit() else None
        note_start = 3 if target_token else 2
        note = " ".join(parts[note_start:]).strip()
        return "needs_changes", target_token, note
    if lowered.startswith("what evidence"):
        return "why", None, ""
    if lowered.startswith("what signal"):
        return "why", None, ""
    if lowered.startswith("what would change"):
        return "why", None, ""
    if lowered.startswith("why more"):
        return "why", None, ""
    if lowered.startswith("show reply"):
        parts = raw.split()
        target_token = parts[2] if len(parts) > 2 and parts[2].isdigit() else None
        return "show_reply", target_token, ""
    if lowered.startswith("show customer review"):
        parts = raw.split()
        target_token = parts[3] if len(parts) > 3 and parts[3].isdigit() else None
        return "show_review", target_token, ""
    if lowered.startswith("show review"):
        parts = raw.split()
        target_token = parts[2] if len(parts) > 2 and parts[2].isdigit() else None
        return "show_review", target_token, ""
    if lowered.startswith("show story asset"):
        parts = raw.split()
        target_token = parts[3] if len(parts) > 3 and parts[3].isdigit() else None
        return "show_asset", target_token, ""
    if lowered.startswith("show asset"):
        parts = raw.split()
        target_token = parts[2] if len(parts) > 2 and parts[2].isdigit() else None
        return "show_asset", target_token, ""
    if lowered.startswith("rewrite"):
        parts = raw.split()
        target_token = parts[1] if len(parts) > 1 and parts[1].isdigit() else None
        note_start = 2 if target_token else 1
        note = " ".join(parts[note_start:]).strip()
        return "rewrite", target_token, note
    if lowered.startswith("same as"):
        parts = raw.split()
        target_token = parts[2] if len(parts) > 2 and parts[2].isdigit() else None
        note_start = 3 if target_token else 2
        note = " ".join(parts[note_start:]).strip()
        return "same_as", target_token, note
    if lowered.startswith("have as"):
        parts = raw.split()
        target_token = parts[2] if len(parts) > 2 and parts[2].isdigit() else None
        note_start = 3 if target_token else 2
        note = " ".join(parts[note_start:]).strip()
        return "same_as", target_token, note
    if lowered.startswith("i have this as"):
        parts = raw.split()
        target_token = parts[4] if len(parts) > 4 and parts[4].isdigit() else None
        note_start = 5 if target_token else 4
        note = " ".join(parts[note_start:]).strip()
        return "same_as", target_token, note
    parts = raw.split()
    command = ACTION_ALIASES.get(parts[0].lower(), "help")
    target_token: str | None = None
    note_start = 1
    if len(parts) > 1 and parts[1].isdigit():
        target_token = parts[1]
        note_start = 2
    note = " ".join(parts[note_start:]).strip()
    return command, target_token, note


def should_delegate_to_customer_operator(text: str) -> bool:
    raw = (text or "").strip()
    if not raw:
        return False
    lowered = raw.lower()
    if lowered.startswith("customer "):
        return True
    if lowered == "customer":
        return True
    parts = raw.split()
    if not parts:
        return False
    first = parts[0].lower()
    if first in {"replacement", "replace", "resend", "refund", "wait", "reply", "reply_only"}:
        if len(parts) > 1 and CUSTOMER_SHORT_ID_PATTERN.match(parts[1]):
            return True
    if lowered.startswith("reply only ") and len(parts) > 2 and CUSTOMER_SHORT_ID_PATTERN.match(parts[2]):
        return True
    return False


def should_delegate_to_business_desk(text: str) -> bool:
    raw = (text or "").strip()
    if not raw:
        return False
    lowered = raw.lower()
    return lowered == "desk" or lowered.startswith("desk ") or lowered == "business" or lowered.startswith("business ")


def handle_business_desk_text(text: str) -> str:
    payload = load_json(BUSINESS_OPERATOR_DESK_PATH, {})
    if not isinstance(payload, dict) or not payload:
        return "Duck Ops business desk is not ready yet. Re-run the observer first."

    raw = (text or "").strip()
    lowered = raw.lower()
    section = "status"
    if lowered in {"desk", "desk status", "business", "business status"}:
        section = "status"
    elif lowered.startswith("desk next") or lowered.startswith("business next"):
        section = "next"
    elif lowered.startswith("desk show "):
        section = raw.split(" ", 2)[2].strip()
    elif lowered.startswith("business show "):
        section = raw.split(" ", 2)[2].strip()
    elif lowered in {"desk help", "business help"}:
        return (
            "Duck Ops desk commands:\n"
            "- desk status\n"
            "- desk next\n"
            "- desk show customer\n"
            "- desk show threads\n"
            "- desk show builds\n"
            "- desk show packing\n"
            "- desk show stock\n"
            "- desk show reviews"
        )

    body = render_business_section(payload, section)
    footer = (
        "\n\nUseful follow-ups:\n"
        "- customer status\n"
        "- customer threads\n"
        "- customer followups\n"
        "- customer open C301\n"
        "- customer drafted C301 <reply text>\n"
        "- customer waiting C301 <what we are waiting on>\n"
        "- customer resolved C301 <resolution note>\n"
        "- why 220\n"
        "- desk show stock\n"
        "- desk show builds"
    )
    return body + footer


def _health_rank(item: dict[str, Any]) -> int:
    return {"bad": 0, "warn": 1, "ok": 2}.get(str(item.get("status") or "").strip().lower(), 3)


def render_system_health_summary(filter_text: str = "") -> str:
    payload = load_json(SYSTEM_HEALTH_PATH, {})
    if not isinstance(payload, dict) or not payload:
        return "System health is not ready yet. Regenerate the health artifact first."

    flows = list(payload.get("flow_health") or [])
    filter_value = str(filter_text or "").strip().lower()
    if filter_value in {"bad", "warn", "ok"}:
        flows = [item for item in flows if str(item.get("status") or "").strip().lower() == filter_value]
    elif filter_value:
        flows = [
            item for item in flows
            if filter_value in str(item.get("flow_id") or "").lower()
            or filter_value in str(item.get("label") or "").lower()
            or filter_value in str(item.get("last_run_state") or "").lower()
        ]

    flows = sorted(flows, key=lambda item: (_health_rank(item), str(item.get("label") or item.get("flow_id") or "")))
    if not flows:
        return f"No health rows matched `{filter_text}`."

    lines = [
        f"System health: {payload.get('overall_status') or 'unknown'}",
        f"- Generated: {payload.get('generated_at') or 'unknown'}",
        f"- Flows: {len(flows)}",
        "",
    ]
    for item in flows:
        label = str(item.get("label") or item.get("flow_id") or "flow").strip()
        status = str(item.get("status") or "unknown").strip()
        last_state = str(item.get("last_run_state") or "unknown").replace("_", " ").strip()
        success = str(item.get("success_rate_label") or "").strip()
        line = f"- {label}: {status} | {last_state}"
        if success:
            line += f" | {success}"
        lines.append(line)
    return "\n".join(lines)


def record_action(
    state_bundle: dict[str, dict[str, Any]],
    artifact_id: str,
    action: str,
    note: str | None,
    resolution: str | None = None,
    approved_reply_text: str | None = None,
) -> tuple[dict[str, Any], str]:
    source_name = ""
    record: dict[str, Any] | None = None
    for candidate_source, state in state_bundle.items():
        candidate_record = (state.get("artifacts") or {}).get(artifact_id)
        if candidate_record is not None:
            source_name = candidate_source
            record = candidate_record
            break
    if record is None:
        raise SystemExit(f"Unknown artifact_id: {artifact_id}")

    review_status_map = {
        "approve": "approved",
        "reject": "rejected",
        "override": "overridden",
    }
    if action == "override" and not (note or "").strip():
        raise SystemExit("Override requires a note.")

    decision = record.get("decision") or {}
    human_review = {
        "action": action,
        "recorded_at": now_iso(),
    }
    if resolution:
        human_review["resolution"] = resolution
    if note:
        human_review["note"] = note.strip()
    if decision.get("decision"):
        human_review["recommended_resolution"] = recommended_action(
            {
                "artifact_type": decision.get("artifact_type"),
                "decision": decision.get("decision"),
                "action_frame": decision.get("action_frame"),
            }
        )
    decision["human_review"] = human_review
    operator_action = resolution or {
        "approve": "approve",
        "reject": "discard",
        "override": "needs_changes",
    }.get(action, action)
    decision["operator_resolution"] = {
        "action": operator_action,
        "note": note.strip() if note else None,
        "recorded_at": human_review["recorded_at"],
    }
    if decision.get("artifact_type") == "review_reply" or decision.get("review_target"):
        decision.setdefault("execution_mode", "manual_only")
        decision.setdefault("execution_state", "not_queued")
        decision.setdefault("execution_attempts", [])
        if approved_reply_text:
            decision["approved_reply_text"] = approved_reply_text
        elif not decision.get("approved_reply_text"):
            decision["approved_reply_text"] = (
                ((decision.get("preview") or {}).get("proposed_text")) or ""
            )
        if operator_action == "approve":
            decision["execution_mode"] = "operator_approved"
    decision.pop("manual_review_requested", None)
    decision["review_status"] = review_status_map[action]
    record["decision"] = decision
    record["reviewed_at"] = human_review["recorded_at"]
    record["output_paths"] = write_decision(decision)

    if action == "override":
        append_jsonl(
            OVERRIDES_PATH,
            {
                "artifact_id": artifact_id,
                "action": action,
                "resolution": resolution,
                "note": (note or "").strip(),
                "recorded_at": human_review["recorded_at"],
                "decision_before_override": decision.get("decision"),
                "flow": decision.get("flow"),
                "run_id": decision.get("run_id"),
                "state_source": source_name,
            },
        )
    return record, source_name


def maybe_queue_review_reply_after_operator_approval(artifact_id: str, decision: dict[str, Any]) -> dict[str, Any] | None:
    if str(decision.get("artifact_type") or "") != "review_reply":
        return None
    if str(decision.get("flow") or "") != "reviews_reply_positive":
        return None
    operator_resolution = decision.get("operator_resolution") or {}
    if str(operator_resolution.get("action") or "") != "approve":
        return None
    if str(decision.get("execution_state") or "") == "posted":
        return {
            "ok": True,
            "status": "already_posted",
            "message": "This Etsy reply was already posted, so nothing new was queued.",
        }
    try:
        from review_reply_executor import queue_review_reply

        return queue_review_reply(
            artifact_id,
            queued_by="operator_review_loop",
        )
    except SystemExit as exc:
        return {
            "ok": False,
            "status": "queue_failed",
            "message": str(exc),
        }


def duckagent_mail_subject(flow: str, run_id: str | None, title: str, action: str) -> str:
    return f"MJD: [{flow}] {title} | FLOW:{flow} | RUN:{run_id or datetime.now().strftime('%Y-%m-%d')} | ACTION:{action}"


def invoke_duckagent_mail_event(flow: str, run_id: str | None, title: str, action: str, note: str | None) -> dict[str, Any]:
    payload = {
        "subject": duckagent_mail_subject(flow, run_id, title, action),
        "body": (note or action).strip() or action,
        "text": (note or action).strip() or action,
    }
    python_bin = DUCK_AGENT_PYTHON if DUCK_AGENT_PYTHON.exists() else Path(sys.executable)
    temp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as handle:
            json.dump(payload, handle)
            temp_path = handle.name
        command = [str(python_bin), "src/main_agent.py", "--mail-file", temp_path]
        proc = subprocess.run(
            command,
            cwd=str(DUCK_AGENT_ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
    finally:
        if temp_path:
            Path(temp_path).unlink(missing_ok=True)
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": (proc.stdout or "").strip(),
        "stderr": (proc.stderr or "").strip(),
        "command": command,
    }


def maybe_handoff_duckagent_publish_after_operator_action(decision: dict[str, Any]) -> dict[str, Any] | None:
    flow = str(decision.get("flow") or "")
    mapping = DUCK_AGENT_HANDOFF_FLOWS.get(flow)
    if not mapping:
        return None

    operator_resolution = decision.get("operator_resolution") or {}
    operator_action = str(operator_resolution.get("action") or "")
    callback = mapping.get(operator_action)
    if not callback:
        return None

    execution_state = decision.get("execution_state") or {}
    if operator_action == "approve" and isinstance(execution_state, dict) and execution_state.get("already_published"):
        return {
            "ok": True,
            "status": "already_scheduled",
            "message": "DuckAgent already shows this post as scheduled or published.",
        }

    result = invoke_duckagent_mail_event(
        flow=str(callback.get("flow") or flow),
        run_id=str(decision.get("run_id") or "").strip() or None,
        title=str(decision.get("title") or flow).strip() or flow,
        action=str(callback.get("action") or "").strip() or operator_action,
        note=str(operator_resolution.get("note") or "").strip() or None,
    )
    attempts = list(decision.get("execution_attempts") or [])
    attempts.append(
        {
            "action": str(callback.get("action") or "").strip() or operator_action,
            "callback_flow": str(callback.get("flow") or flow),
            "requested_via": "openclaw_operator_review",
            "recorded_at": now_iso(),
            "ok": result["ok"],
            "returncode": result["returncode"],
            "stdout_tail": result["stdout"][-1200:],
            "stderr_tail": result["stderr"][-1200:],
        }
    )
    decision["execution_mode"] = "duckagent_mail_event"
    decision["execution_attempts"] = attempts
    decision["execution_state"] = "publish_requested" if result["ok"] and operator_action == "approve" else (
        "revise_requested" if result["ok"] else "handoff_failed"
    )
    if result["ok"]:
        message = (
            "DuckAgent publish was requested."
            if operator_action == "approve"
            else "DuckAgent revise was requested. A refreshed draft should come back through the normal review flow."
        )
        status = "running" if operator_action == "approve" else "revising"
    else:
        message = "DuckAgent handoff failed closed."
        if result["stderr"]:
            message = f"{message} {result['stderr']}"
        elif result["stdout"]:
            message = f"{message} {result['stdout']}"
        status = "handoff_failed"
    return {
        "ok": result["ok"],
        "status": status,
        "message": message,
        "updated_decision": decision,
    }


def operator_help(current_item: dict[str, Any] | None = None) -> str:
    lines = [
        "OpenClaw operator commands:",
        "agree [id]",
        "approve [id] <short reason>",
        "needs changes [id] <short reason>",
        "discard [id] <short reason>",
        "build [id] <short reason>",
        "promote [id] <short reason>",
        "wait [id] <short reason>",
        "ignore [id] <short reason>",
        "why [id]",
        "suggest changes [id]",
        "rewrite [id] [shorter|warmer]",
        "Weekly sale shortcut: rewrite [id], then needs changes [id] use rewrite",
        "Jeep Fact shortcut: rewrite [id] new ducks same facts",
        "show review [id]",
        "show reply [id]",
        "show asset [id]",
        "same as [id] <your product name>",
        "status",
        "status all",
        "health",
        "health bad",
        "next",
        "",
        "Customer lane commands:",
        "customer status",
        "customer threads",
        "customer followups",
        "customer next",
        "customer drafted C301 <reply text>",
        "customer waiting C301 <what we are waiting on>",
        "customer resolved C301 <resolution note>",
        "customer taskready C301 <brief summary>",
        "replacement C301 because ...",
        "refund C301 because ...",
        "wait C301 because ...",
        "reply only C301 because ...",
        "",
        "Business desk commands:",
        "desk status",
        "desk next",
        "desk show customer",
        "desk show builds",
        "desk show packing",
        "desk show stock",
        "desk show reviews",
        "desk show workflow",
    ]
    if current_item:
        lines.extend(["", "Current review:", "", render_operator_card(current_item)])
    return "\n".join(lines)


def render_queue_status(
    items: list[dict[str, Any]],
    current_item: dict[str, Any] | None,
    all_items: list[dict[str, Any]] | None = None,
    full_review_count: int | None = None,
    full_trend_count: int | None = None,
) -> str:
    all_items = all_items if all_items is not None else items
    surfaced_total = len(items)
    surfaced_review_count = sum(1 for item in items if item.get("artifact_type") != "trend")
    surfaced_trend_count = sum(1 for item in items if item.get("artifact_type") == "trend")
    full_review_count = sum(1 for item in all_items if item.get("artifact_type") != "trend") if full_review_count is None else full_review_count
    full_trend_count = sum(1 for item in all_items if item.get("artifact_type") == "trend") if full_trend_count is None else full_trend_count

    lines = [
        "OpenClaw queue status:",
        f"- Surfaced now: {surfaced_total}",
        f"- Surfaced reviews: {surfaced_review_count}",
        f"- Surfaced trends: {surfaced_trend_count}",
    ]

    if full_review_count != surfaced_review_count or full_trend_count != surfaced_trend_count:
        lines.extend(
            [
                f"- Full pending reviews: {full_review_count}",
                f"- Full pending trends: {full_trend_count}",
            ]
        )
        if full_trend_count > surfaced_trend_count:
            lines.append(f"- Trend backlog behind the surfaced queue: {full_trend_count - surfaced_trend_count}")
            lines.append("- Note: WhatsApp only surfaces the top 8 trends at a time, so that number can stay at 8 while you make progress.")

    if current_item:
        lines.append(f"- Current item: {current_item['short_id']} | {current_item.get('title') or current_item.get('artifact_id')}")

    upcoming = [item for item in items[:3]]
    if upcoming:
        lines.extend(["", "Next up:"])
        for item in upcoming:
            lines.append(
                f"- {item['short_id']} | {resolution_label(recommended_action(item))} | {item.get('title') or item.get('artifact_id')}"
            )
    else:
        if all_items:
            lines.append("- No new items are surfaced right now.")
            lines.append("- Older unresolved backlog exists; reply `status all` if you want to inspect it.")
        else:
            lines.append("- No pending items right now.")

    return "\n".join(lines)


def handle_operator_text(state_bundle: dict[str, dict[str, Any]], operator_state: dict[str, Any], text: str) -> str:
    reconcile_state_bundle(state_bundle)
    if should_delegate_to_business_desk(text):
        return handle_business_desk_text(text)
    if should_delegate_to_customer_operator(text):
        from customer_operator import handle_customer_text

        return handle_customer_text(text)

    all_items = build_review_items(state_bundle)
    assign_short_ids(all_items, operator_state)
    current_item = sync_current_item(all_items, operator_state)
    items = surfaced_review_items(all_items)
    command, target_token, note = parse_command(text)

    if command == "help":
        write_review_queue(state_bundle, operator_state)
        return operator_help(current_item)

    if command == "status":
        write_review_queue(state_bundle, operator_state)
        full_review_count = len(build_quality_gate_items(state_bundle.get("quality_gate", {})))
        full_trend_count = len(collect_trend_items(state_bundle.get("trend_ranker", {})))
        return render_queue_status(items, current_item, all_items=all_items, full_review_count=full_review_count, full_trend_count=full_trend_count)

    if command == "status_all":
        write_review_queue(state_bundle, operator_state)
        full_review_count = len(build_quality_gate_items(state_bundle.get("quality_gate", {})))
        full_trend_count = len(collect_trend_items(state_bundle.get("trend_ranker", {})))
        status_text = render_queue_status(items, current_item, all_items=all_items, full_review_count=full_review_count, full_trend_count=full_trend_count)
        if all_items:
            status_text += "\n\nBacklog:\n"
            for item in all_items[:8]:
                status_text += f"\n- {item['short_id']} | {resolution_label(recommended_action(item))} | {item.get('freshness_label')} | {item.get('title') or item.get('artifact_id')}"
        return status_text

    if command == "health":
        write_review_queue(state_bundle, operator_state)
        return render_system_health_summary(note)

    if command == "next":
        target = next_item(items, current_item)
        operator_state["current_artifact_id"] = target.get("artifact_id") if target else None
        write_review_queue(state_bundle, operator_state)
        if not target:
            return "No pending reviews right now."
        return render_operator_card(target)

    target_item = resolve_target_item(all_items, operator_state, target_token)
    if not target_item:
        write_review_queue(state_bundle, operator_state)
        if current_item:
            stale_note = "That review item is no longer active."
            if target_token:
                stale_note = f"Review item `{target_token}` is no longer active."
            return stale_note + "\n\nCurrent item:\n\n" + render_operator_card(current_item)
        return "I could not find that review item, and there are no pending reviews right now."

    operator_state["current_artifact_id"] = target_item["artifact_id"]

    if command == "why":
        write_review_queue(state_bundle, operator_state)
        return render_operator_detail(target_item)

    if command == "suggest_changes":
        write_review_queue(state_bundle, operator_state)
        return render_change_suggestions(target_item)

    if command == "rewrite":
        rewrite_text = build_rewrite_suggestion_text(target_item, hint=note)
        rewrite_cache = operator_state.setdefault("rewrite_suggestions", {})
        if rewrite_text and (
            target_item.get("artifact_type") == "review_reply"
            or str(target_item.get("flow") or "") == "weekly_sale"
        ):
            rewrite_cache[target_item["artifact_id"]] = {
                "text": rewrite_text,
                "generated_at": now_iso(),
            }
        write_review_queue(state_bundle, operator_state)
        return render_rewrite_suggestion(target_item, hint=note)

    if command == "show_review":
        write_review_queue(state_bundle, operator_state)
        return render_preview_focus(target_item, "review")

    if command == "show_reply":
        write_review_queue(state_bundle, operator_state)
        return render_preview_focus(target_item, "reply")

    if command == "show_asset":
        write_review_queue(state_bundle, operator_state)
        return render_preview_focus(target_item, "asset")

    if command == "same_as":
        if target_item.get("artifact_type") != "trend":
            write_review_queue(state_bundle, operator_state)
            return "That command only works on trend items. For reviews, use agree / approve / needs changes / discard."
        if not note:
            write_review_queue(state_bundle, operator_state)
            return f"Please tell me which duck you already have. Example: `same as {target_item['short_id']} White Tailed Deer / Buck Duck`"
        product = resolve_catalog_product(note)
        if not product:
            write_review_queue(state_bundle, operator_state)
            return "I couldn't confidently match that to one of your products. Try a more exact product title or handle."
        save_catalog_alias(target_item.get("theme") or target_item.get("title") or "", product, target_item["artifact_id"])
        updated_action = apply_trend_alias(state_bundle, target_item, product)
        write_state_source("trend_ranker", state_bundle["trend_ranker"])
        write_review_queue(state_bundle, operator_state)
        recommendation_line = f"Recommendation stays: {resolution_label(updated_action)}."
        if updated_action == "promote":
            recommendation_line = "Updated recommendation: promote."
        return (
            f"Saved: I’ll treat `{target_item.get('title') or target_item.get('artifact_id')}` as your existing product "
            f"`{product.get('title')}` going forward.\n\n"
            f"{recommendation_line}\n"
            "Reply `agree` if that looks right, or `next` to move on."
        )

    valid_commands = {"agree", "discard"}
    if target_item.get("artifact_type") == "trend":
        valid_commands.update({"build", "promote", "wait", "ignore"})
    else:
        valid_commands.update({"approve", "needs_changes"})

    if command not in valid_commands:
        write_review_queue(state_bundle, operator_state)
        return operator_help(target_item)

    recommended = recommended_action(target_item)
    desired_resolution = recommended if command == "agree" else command
    if desired_resolution != recommended and not note:
        write_review_queue(state_bundle, operator_state)
        return (
            f"Please add a short reason. Example: `{resolution_label(desired_resolution)} {target_item['short_id']} because ...`"
        )

    approved_reply_override: str | None = None
    note_override: str | None = None
    if desired_resolution == "approve" and note_requests_cached_rewrite(note):
        cached_rewrite = (
            (operator_state.get("rewrite_suggestions") or {}).get(target_item["artifact_id"], {}).get("text")
        )
        if cached_rewrite:
            approved_reply_override = cached_rewrite
    if desired_resolution == "needs_changes" and note_requests_cached_rewrite(note):
        cached_rewrite = (
            (operator_state.get("rewrite_suggestions") or {}).get(target_item["artifact_id"], {}).get("text")
        )
        if cached_rewrite:
            note_override = cached_rewrite

    internal_action = "approve" if desired_resolution == recommended else "override"
    _, source_name = record_action(
        state_bundle,
        target_item["artifact_id"],
        internal_action,
        note=note_override or note or None,
        resolution=desired_resolution,
        approved_reply_text=approved_reply_override,
    )
    write_state_source(source_name, state_bundle[source_name])
    recorded_decision = (
        ((state_bundle.get(source_name) or {}).get("artifacts") or {}).get(target_item["artifact_id"], {}).get("decision") or {}
    )
    execution_handoff = maybe_queue_review_reply_after_operator_approval(target_item["artifact_id"], recorded_decision)
    duckagent_handoff = maybe_handoff_duckagent_publish_after_operator_action(recorded_decision)
    if duckagent_handoff:
        artifact_record = ((state_bundle.get(source_name) or {}).get("artifacts") or {}).get(target_item["artifact_id"], {})
        artifact_record["decision"] = recorded_decision
        artifact_record["output_paths"] = write_decision(recorded_decision)
        write_state_source(source_name, state_bundle[source_name])
    write_review_queue(state_bundle, operator_state)

    remaining_items = build_review_items(state_bundle)
    assign_short_ids(remaining_items, operator_state)
    next_pending = next_item(remaining_items, None)
    ack = f"Recorded: {target_item['short_id']} -> {resolution_label(desired_resolution)}."
    if note:
        ack += f" Note: {note}"
    if execution_handoff:
        handoff_message = str(execution_handoff.get("message") or "").strip()
        if execution_handoff.get("ok"):
            if execution_handoff.get("status") in {"queued", "running"}:
                ack += "\nQueued for Etsy execution."
            elif execution_handoff.get("status") == "already_posted":
                ack += "\nAlready posted on Etsy, so nothing new was queued."
            elif handoff_message:
                ack += f"\n{handoff_message}"
        elif handoff_message:
            ack += f"\nExecution handoff failed closed: {handoff_message}"
    if duckagent_handoff:
        handoff_message = str(duckagent_handoff.get("message") or "").strip()
        if duckagent_handoff.get("ok"):
            if duckagent_handoff.get("status") == "running":
                ack += "\nDuckAgent publish was requested."
            elif duckagent_handoff.get("status") == "revising":
                ack += "\nDuckAgent revise was requested."
            elif handoff_message:
                ack += f"\n{handoff_message}"
        elif handoff_message:
            ack += f"\nDuckAgent handoff failed closed: {handoff_message}"

    if next_pending:
        operator_state["current_artifact_id"] = next_pending["artifact_id"]
        write_review_queue(state_bundle, operator_state)
        return ack + "\n\nNext review:\n\n" + render_operator_card(next_pending)

    operator_state["current_artifact_id"] = None
    write_review_queue(state_bundle, operator_state)
    return ack + "\n\nNo more queued reviews right now."


def main() -> int:
    parser = argparse.ArgumentParser(description="Build review queue, record human review, and drive operator commands.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("queue", help="Write the current pending review queue and operator outputs.")

    record_parser = sub.add_parser("record", help="Record approve/reject/override for one artifact.")
    record_parser.add_argument("--artifact-id", required=True)
    record_parser.add_argument("--action", choices=["approve", "reject", "override"], required=True)
    record_parser.add_argument("--note")
    record_parser.add_argument("--resolution")

    sub.add_parser("message", help="Print the current operator review card.")

    detail_parser = sub.add_parser("why", help="Print detailed rationale for the current or selected operator review.")
    detail_parser.add_argument("--id")

    handle_parser = sub.add_parser("handle", help="Handle a plain-language operator reply.")
    handle_parser.add_argument("--text", required=True)

    args = parser.parse_args()
    state_bundle = load_state_bundle()
    operator_state = load_operator_state()

    if args.command == "queue":
        reconcile_state_bundle(state_bundle)
        write_review_queue(state_bundle, operator_state)
        return 0

    if args.command == "record":
        reconcile_state_bundle(state_bundle)
        _, source_name = record_action(state_bundle, args.artifact_id, args.action, args.note, resolution=args.resolution)
        write_state_source(source_name, state_bundle[source_name])
        write_review_queue(state_bundle, operator_state)
        return 0

    if args.command == "message":
        reconcile_state_bundle(state_bundle)
        items = build_review_items(state_bundle)
        assign_short_ids(items, operator_state)
        current_item = sync_current_item(items, operator_state)
        write_review_queue(state_bundle, operator_state)
        if current_item:
            print(render_operator_card(current_item))
        elif items:
            print("No new pending reviews right now.\n\nUse `status` for the summary or `status all` to inspect older backlog.")
        else:
            print("No pending reviews right now.")
        return 0

    if args.command == "why":
        reconcile_state_bundle(state_bundle)
        items = build_review_items(state_bundle)
        assign_short_ids(items, operator_state)
        target_item = resolve_target_item(items, operator_state, args.id)
        write_review_queue(state_bundle, operator_state)
        print(render_operator_detail(target_item) if target_item else "No pending reviews right now.")
        return 0

    if args.command == "handle":
        response = handle_operator_text(state_bundle, operator_state, args.text)
        write_operator_state(operator_state)
        print(response)
        return 0

    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

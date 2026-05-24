#!/usr/bin/env python3
"""
Phase 2 pilot quality gate for passive OpenClaw evaluation.

This evaluator:
- reads normalized publish candidates
- scores them with conservative heuristic rules
- writes auditable JSON/Markdown decision artifacts
- appends deduped decision history
- writes a daily digest and rare urgent alert files

It intentionally does not:
- publish anything
- send email
- mutate DuckAgent
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from decision_writer import ensure_parent, load_output_patterns, render_pattern, slugify, write_decision
from flow_review_contract import build_flow_review_contract, flow_review_check
from review_reply_contract import build_review_reply_contract, contract_failure_messages, contract_improvement_suggestions
from workflow_control import record_workflow_transition


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
NORMALIZED_DIR = STATE_DIR / "normalized"
OUTPUT_DIR = ROOT / "output"

PUBLISH_CANDIDATES_PATH = NORMALIZED_DIR / "publish_candidates.json"
DECISION_HISTORY_PATH = STATE_DIR / "decision_history.jsonl"
QUALITY_GATE_STATE_PATH = STATE_DIR / "quality_gate_state.json"
EVALUATOR_VERSION = 9
QUALITY_GATE_PENDING_REVIEW_STATUSES = {"pending"}
QUALITY_GATE_REVISION_REVIEW_STATUSES = {"needs_revision"}
QUALITY_GATE_RESOLVED_REVIEW_STATUSES = {"approved", "rejected", "archived", "overridden"}
PUBLIC_REVIEW_REPLY_WARMTH_TERMS = (
    "thank",
    "appreciate",
    "thrilled",
    "wonderful to hear",
    "glad",
    "love hearing",
    "happy to hear",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row))
            handle.write("\n")


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if len(str(value).strip()) == 10:
            parsed = datetime.strptime(str(value).strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone()
    except ValueError:
        return None


def canonical_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def material_candidate_view(candidate: dict[str, Any]) -> dict[str, Any]:
    summary = candidate.get("candidate_summary") or {}
    flow = str(candidate.get("flow") or "")
    payload: dict[str, Any] = {
        "artifact_id": candidate.get("artifact_id"),
        "artifact_type": candidate.get("artifact_type"),
        "flow": flow,
        "run_id": candidate.get("run_id"),
        "review_target": candidate.get("review_target"),
        "title": summary.get("title"),
        "body": summary.get("body"),
        "images": summary.get("images") or [],
        "platform_targets": summary.get("platform_targets") or [],
        "platform_variants": summary.get("platform_variants") or {},
        "execution_state": candidate.get("execution_state") or {},
    }
    if flow in {"reviews_reply_positive", "reviews_reply_private"}:
        payload.update(
            {
                "customer_review": summary.get("customer_review"),
                "review_date": summary.get("review_date"),
                "review_rating": summary.get("review_rating"),
                "response_kind": summary.get("response_kind"),
                "transaction_id": summary.get("transaction_id"),
                "next_steps": summary.get("next_steps"),
            }
        )
    if flow == "weekly_sale":
        payload["publish_token"] = summary.get("publish_token")
    return payload


def material_candidate_hash(candidate: dict[str, Any]) -> str:
    return canonical_hash(material_candidate_view(candidate))


def carry_forward_review_resolution(
    decision: dict[str, Any],
    previous_record: dict[str, Any] | None,
    *,
    material_hash: str,
) -> dict[str, Any]:
    if not previous_record:
        return {}
    if str(previous_record.get("material_hash") or "") != str(material_hash or ""):
        return {}

    previous_decision = previous_record.get("decision") or {}
    previous_review_status = str(previous_decision.get("review_status") or "")
    if previous_review_status in {"", "pending"}:
        return {}

    decision["review_status"] = previous_review_status
    for key in (
        "human_review",
        "operator_resolution",
        "approved_reply_text",
        "execution_mode",
        "execution_state",
        "execution_attempts",
        "archive_reason",
        "archived_at",
    ):
        if key in previous_decision:
            decision[key] = previous_decision[key]

    carried: dict[str, Any] = {}
    if previous_record.get("reviewed_at"):
        carried["reviewed_at"] = previous_record["reviewed_at"]
    if previous_record.get("reconciled_at"):
        carried["reconciled_at"] = previous_record["reconciled_at"]
    if previous_record.get("reconciliation_reason"):
        carried["reconciliation_reason"] = previous_record["reconciliation_reason"]
    return carried


def apply_execution_state_reconciliation(candidate: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any]:
    execution_state = candidate.get("execution_state") or {}
    if not execution_state.get("already_published"):
        return {}

    published_channels = list(execution_state.get("published_channels") or [])
    channel_text = ", ".join(published_channels) if published_channels else "connected marketplaces"
    recorded_at = (
        str(execution_state.get("published_at") or "")
        or decision.get("created_at")
        or now_iso()
    )
    note = f"Reconciled automatically because DuckAgent already shows this item as published to {channel_text}."
    decision["review_status"] = "approved"
    decision["human_review"] = {
        "action": "reconcile",
        "resolution": "approve",
        "recorded_at": recorded_at,
        "note": note,
    }
    decision["reconciled_resolution"] = {
        "action": "reconcile",
        "resolution": "approve",
        "recorded_at": recorded_at,
        "note": note,
        "source": execution_state.get("state_source"),
    }
    reasoning = list(decision.get("reasoning") or [])
    reconciliation_reason = f"Execution-state reconciliation: DuckAgent already published this item to {channel_text}."
    if reconciliation_reason not in reasoning:
        reasoning.append(reconciliation_reason)
    decision["reasoning"] = reasoning
    return {
        "reviewed_at": recorded_at,
        "reconciled_at": now_iso(),
        "reconciliation_reason": note,
    }


def priority_rank(priority: str) -> int:
    return {"urgent": 3, "high": 2, "medium": 1, "low": 0}.get(priority, 0)


def parse_run_date(run_id: str | None) -> datetime | None:
    if not run_id:
        return None
    token = run_id[:10]
    try:
        return datetime.strptime(token, "%Y-%m-%d")
    except ValueError:
        return None


def body_has_css_noise(body: str) -> bool:
    markers = ("body {", ".container", "font-family:", "border-radius", "padding:", "background:")
    lower = (body or "").lower()
    return sum(marker in lower for marker in markers) >= 2


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def preview_text(value: str | None, limit: int = 400) -> str:
    text = " ".join((value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def media_reference_is_renderable(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    lowered = text.lower()
    if lowered in {"none", "null", "n/a", "na", "asset", "image", "preview", "placeholder"}:
        return False
    if text in {"🎯", "✅", "❌", "⚠️", "⭐"}:
        return False
    if re.match(r"^(https?|data):", text, flags=re.I):
        return True
    if re.search(r"\.(png|jpe?g|webp|gif)(\?.*)?$", text, flags=re.I):
        return True
    return False


def renderable_media_references(values: Any) -> list[str]:
    if not isinstance(values, list):
        values = [values]
    return [str(value).strip() for value in values if media_reference_is_renderable(value)]


def confidence_cap(candidate: dict[str, Any]) -> float:
    caps = [1.0]
    source_refs = candidate.get("source_refs") or []
    notes = candidate.get("normalization_notes") or {}
    if len(source_refs) <= 1 and notes.get("source_mode") != "review_summary_email":
        caps.append(0.60)
    if notes.get("completeness") == "partial_email":
        caps.append(0.70)
    if candidate.get("flow") == "newduck":
        supporting = candidate.get("supporting_context") or {}
        if not supporting.get("catalog_overlap") and not supporting.get("publication_coverage"):
            caps.append(0.70)
    # No outcome loop yet in pilot.
    caps.append(0.75)
    if notes.get("input_confidence_cap"):
        caps.append(float(notes["input_confidence_cap"]))
    return min(caps)


def default_suggestions(flow: str) -> list[str]:
    if flow == "newduck":
        return [
            "Preserve a stable draft artifact beyond the email body so future reviews can inspect richer listing data.",
            "Include at least one concrete differentiator against existing catalog coverage before asking for publish approval.",
        ]
    if flow == "reviews_story":
        return [
            "Keep the selected review quote and final story asset together in one review artifact so the gate can validate the full post package.",
            "Include a short operator note about why this review is worth posting publicly when the daily summary is generated.",
        ]
    if flow == "reviews_reply_positive":
        return [
            "Keep public replies short, warm, and explicitly tied to what the customer said instead of using a generic thank-you.",
            "Avoid overlong responses that feel templated when a customer left only a short 5-star review.",
        ]
    if flow == "reviews_reply_private":
        return [
            "Keep private recovery replies empathetic and specific about the remedy without promising more than you intend to do.",
            "Preserve the order or transaction reference so follow-up actions can be tied back to the exact customer case.",
        ]
    if flow == "weekly_sale":
        return [
            "Keep the final campaign summary concise enough that the publish recommendation is readable without parsing email scaffolding.",
            "Preserve the exact sale actions in a deterministic summary so the operator can approve the plan without opening the raw email.",
        ]
    if flow == "meme":
        return [
            "Keep the meme text, caption, and final image together in one structured artifact so approval can happen without opening the raw email.",
            "Preserve the final scheduled-platform payload so the operator can approve or revise this post directly from WhatsApp.",
        ]
    if flow == "jeepfact":
        return [
            "Keep the final Jeep Fact caption and cover image together so the operator can review the actual post package instead of a partial summary.",
            "Preserve the carousel image set and posting notes so the operator can approve or revise the scheduled post without reopening DuckAgent.",
        ]
    if flow == "thursday":
        return [
            "Keep each This-or-That option image, caption, option id, and vote labels together so approval can publish the exact option the operator chose.",
            "Preserve the option-review warnings and failures so weak pairings or source-title leakage are visible before approval.",
        ]
    return [
        "Preserve the sale playbook as a structured state artifact so the gate can inspect the actual recommended actions instead of email formatting.",
        "Keep the final campaign summary concise enough that the publish recommendation is readable without parsing email scaffolding.",
    ]


def evaluate_meme_publish_package(candidate: dict[str, Any], age_days: int | None) -> dict[str, Any]:
    """Evaluate Meme Monday as a publish package instead of a generic scored artifact."""
    summary = candidate.get("candidate_summary") or {}
    supporting = candidate.get("supporting_context") or {}
    notes = candidate.get("normalization_notes") or {}
    body = " ".join(str(summary.get("body") or "").split())
    images = renderable_media_references(summary.get("images") or [])
    platform_targets = [str(item).strip() for item in (summary.get("platform_targets") or []) if str(item).strip()]
    platform_variants = summary.get("platform_variants") if isinstance(summary.get("platform_variants"), dict) else {}
    source_refs = candidate.get("source_refs") or []
    trend_refs = supporting.get("trend_refs") or []

    hard_blockers: list[str] = []
    warnings: list[str] = []
    suggestions: list[str] = []
    checks: list[dict[str, str]] = []

    if images:
        checks.append(flow_review_check("pass", "Final meme image is attached", images[0]))
    else:
        hard_blockers.append("Final meme image is missing or is not a renderable image URL/path.")
        checks.append(flow_review_check("fail", "Final meme image is attached", "No renderable image was found."))

    if len(body) >= 80:
        checks.append(flow_review_check("pass", "Caption is present", f"{len(body)} characters"))
    else:
        hard_blockers.append("Meme caption is too short or missing for approval.")
        checks.append(flow_review_check("fail", "Caption is present", f"{len(body)} characters"))

    meme_markers = ("meme monday", "expectation:", "reality:", "pov:", "#mememonday")
    if any(marker in body.lower() for marker in meme_markers):
        checks.append(flow_review_check("pass", "Meme framing is recognizable", "Caption includes Meme Monday or meme-format cues."))
    else:
        warnings.append("Meme framing is not obvious from the caption; consider adding a clearer hook before posting.")
        checks.append(flow_review_check("warn", "Meme framing is recognizable", "No standard meme cue found in caption."))

    if platform_targets or platform_variants:
        platform_evidence = ", ".join(platform_targets) or ", ".join(sorted(platform_variants.keys()))
        checks.append(flow_review_check("pass", "Platform payload is present", platform_evidence))
    else:
        hard_blockers.append("Platform target/caption payload is missing, so approval cannot map to a publishable post.")
        checks.append(flow_review_check("fail", "Platform payload is present", "No platform targets or variants found."))

    if body_has_css_noise(body):
        hard_blockers.append("Caption appears to include email/CSS wrapper noise instead of clean social copy.")
        checks.append(flow_review_check("fail", "Caption is clean social copy", "CSS/email wrapper markers found."))
    else:
        checks.append(flow_review_check("pass", "Caption is clean social copy"))

    if age_days is None:
        warnings.append("Run date is unknown, so timing should be checked before posting.")
        timing_status = "warn"
        timing_evidence = "Unknown run date"
    elif age_days <= 3:
        timing_status = "pass"
        timing_evidence = f"{age_days} day(s) old"
    elif age_days <= 7:
        warnings.append("Meme package is several days old; verify the Monday slot still makes sense.")
        timing_status = "warn"
        timing_evidence = f"{age_days} day(s) old"
    else:
        hard_blockers.append("Meme package is stale for a Meme Monday approval; regenerate or intentionally reschedule it.")
        timing_status = "fail"
        timing_evidence = f"{age_days} day(s) old"
    checks.append(flow_review_check(timing_status, "Posting window is current", timing_evidence))

    if len(source_refs) <= 1 and not trend_refs:
        warnings.append("Trend/support evidence is thin, but the final meme package itself is complete.")
    if not platform_variants:
        warnings.append("Platform-specific captions were not preserved; approval will rely on the shared caption.")
    if len(platform_targets) == 1:
        warnings.append("Only one platform target is attached to this Meme Monday package.")

    score = 96 - (len(warnings) * 4) - (len(hard_blockers) * 35)
    score = int(clamp(score, 0, 100))
    if hard_blockers:
        decision = "needs_revision" if score >= 40 else "discard"
        priority = "high"
    else:
        decision = "publish_ready"
        priority = "medium"

    confidence = round(clamp(0.64 + min(0.08, 0.02 * len(images)) + min(0.08, 0.02 * len(platform_targets)), 0.25, confidence_cap(candidate)), 2)
    if hard_blockers:
        confidence = round(min(confidence + 0.05, confidence_cap(candidate)), 2)

    if hard_blockers:
        operator_summary = "OpenClaw found a blocking Meme Monday package issue."
        recommended_action = "request_revision"
        suggestions.extend(hard_blockers)
    elif warnings:
        operator_summary = "Meme Monday package is ready to approve with non-blocking warnings."
        recommended_action = "approve"
        suggestions.extend(warnings[:2])
    else:
        operator_summary = "Meme Monday package is ready to approve."
        recommended_action = "approve"

    reasoning = [
        operator_summary,
        f"Hard blockers: {len(hard_blockers)}.",
        f"Warnings: {len(warnings)}.",
    ]
    reasoning.extend(f"Blocker: {item}" for item in hard_blockers)
    reasoning.extend(f"Warning: {item}" for item in warnings[:3])

    component_scores = {
        "publish_package": 0 if hard_blockers else 40,
        "caption": 20 if len(body) >= 80 else 5,
        "platform_payload": 20 if platform_targets or platform_variants else 0,
        "timing_fit": 10 if age_days is not None and age_days <= 3 else 6 if age_days is None or age_days <= 7 else 0,
        "support_warning_penalty": min(10, len(warnings) * 2),
    }

    review_contract = build_flow_review_contract(
        reviewer="meme_publish_package",
        hard_blockers=hard_blockers,
        warnings=warnings,
        checks=checks,
        operator_summary=operator_summary,
        approval_summary="Approve this Meme Monday post package for the configured social publish lane.",
        recommended_action=recommended_action,
        operator_actions=["approve", "request_revision", "skip", "discard"],
    )

    return {
        "decision": decision,
        "score": score,
        "confidence": confidence,
        "priority": priority,
        "reasoning": reasoning,
        "improvement_suggestions": suggestions,
        "component_scores": component_scores,
        "fail_closed": hard_blockers,
        "review_contract": review_contract,
    }


def evaluate_jeepfact_publish_package(candidate: dict[str, Any], age_days: int | None) -> dict[str, Any]:
    """Evaluate Jeep Fact Wednesday as a reviewable carousel package."""
    summary = candidate.get("candidate_summary") or {}
    supporting = candidate.get("supporting_context") or {}
    body = " ".join(str(summary.get("body") or "").split())
    body_lower = body.lower()
    images = renderable_media_references(summary.get("images") or [])
    platform_targets = [str(item).strip() for item in (summary.get("platform_targets") or []) if str(item).strip()]
    platform_variants = summary.get("platform_variants") if isinstance(summary.get("platform_variants"), dict) else {}
    source_refs = candidate.get("source_refs") or []
    trend_refs = supporting.get("trend_refs") or []
    fact_cues = re.findall(r"\bfact\s*#?\s*\d+", body_lower)

    hard_blockers: list[str] = []
    warnings: list[str] = []
    suggestions: list[str] = []
    checks: list[dict[str, str]] = []

    if len(images) >= 2:
        checks.append(flow_review_check("pass", "Carousel slides are attached", f"{len(images)} renderable slide(s)"))
    elif images:
        hard_blockers.append("Only one Jeep Fact slide is attached; approval needs the carousel set, not just the cover.")
        checks.append(flow_review_check("fail", "Carousel slides are attached", "Only one renderable slide was found."))
    else:
        hard_blockers.append("Jeep Fact carousel images are missing or not renderable.")
        checks.append(flow_review_check("fail", "Carousel slides are attached", "No renderable slides were found."))

    if len(images) >= 5:
        checks.append(flow_review_check("pass", "Full carousel is reviewable", "Cover/facts/CTA slide set is attached."))
    elif len(images) >= 2:
        warnings.append("Jeep Fact has fewer than five slides attached; verify the cover, fact slides, and CTA are all present.")
        checks.append(flow_review_check("warn", "Full carousel is reviewable", f"{len(images)} slide(s) attached."))

    if len(body) >= 120:
        checks.append(flow_review_check("pass", "Caption is present", f"{len(body)} characters"))
    else:
        hard_blockers.append("Jeep Fact caption is too short or missing for approval.")
        checks.append(flow_review_check("fail", "Caption is present", f"{len(body)} characters"))

    if "jeep fact" in body_lower and len(fact_cues) >= 4:
        checks.append(flow_review_check("pass", "Jeep Fact framing is recognizable", f"{len(fact_cues)} fact cues found."))
    elif "jeep fact" in body_lower or fact_cues:
        warnings.append("Jeep Fact framing is partial; confirm the post clearly reads as Wednesday educational carousel content.")
        checks.append(flow_review_check("warn", "Jeep Fact framing is recognizable", f"{len(fact_cues)} fact cue(s) found."))
    else:
        hard_blockers.append("Jeep Fact framing is missing, so the operator cannot tell this is the Wednesday fact carousel.")
        checks.append(flow_review_check("fail", "Jeep Fact framing is recognizable", "No Jeep Fact/fact cues found."))

    if "myjeepduck.com" in body_lower or "#jeepfactwednesday" in body_lower:
        checks.append(flow_review_check("pass", "Brand CTA is present", "Caption includes MyJeepDuck or Jeep Fact hashtag."))
    else:
        warnings.append("Brand CTA or Jeep Fact hashtag is missing from the caption.")
        checks.append(flow_review_check("warn", "Brand CTA is present", "No MyJeepDuck URL or Jeep Fact hashtag found."))

    if platform_targets or platform_variants:
        platform_evidence = ", ".join(platform_targets) or ", ".join(sorted(platform_variants.keys()))
        checks.append(flow_review_check("pass", "Platform payload is present", platform_evidence))
    else:
        hard_blockers.append("Platform target/caption payload is missing, so approval cannot map to a publishable Jeep Fact post.")
        checks.append(flow_review_check("fail", "Platform payload is present", "No platform targets or variants found."))

    if body_has_css_noise(body):
        hard_blockers.append("Caption appears to include email/CSS wrapper noise instead of clean social copy.")
        checks.append(flow_review_check("fail", "Caption is clean social copy", "CSS/email wrapper markers found."))
    else:
        checks.append(flow_review_check("pass", "Caption is clean social copy"))

    if age_days is None:
        warnings.append("Run date is unknown, so timing should be checked before posting.")
        timing_status = "warn"
        timing_evidence = "Unknown run date"
    elif age_days <= 3:
        timing_status = "pass"
        timing_evidence = f"{age_days} day(s) old"
    elif age_days <= 7:
        warnings.append("Jeep Fact package is several days old; approve only if it still fits the next Wednesday slot.")
        timing_status = "warn"
        timing_evidence = f"{age_days} day(s) old"
    else:
        hard_blockers.append("Jeep Fact package is stale for Wednesday approval; regenerate or intentionally reschedule it.")
        timing_status = "fail"
        timing_evidence = f"{age_days} day(s) old"
    checks.append(flow_review_check(timing_status, "Posting window is current", timing_evidence))

    if len(source_refs) <= 1 and not trend_refs:
        warnings.append("Trend/support evidence is thin, but the final Jeep Fact package itself is complete.")
    if not platform_variants:
        warnings.append("Platform-specific captions were not preserved; approval will rely on the shared caption.")
    if len(platform_targets) == 1:
        warnings.append("Only one platform target is attached to this Jeep Fact package.")

    score = 97 - (len(warnings) * 4) - (len(hard_blockers) * 35)
    score = int(clamp(score, 0, 100))
    if hard_blockers:
        decision = "needs_revision" if score >= 40 else "discard"
        priority = "high"
    else:
        decision = "publish_ready"
        priority = "medium"

    confidence = round(clamp(0.64 + min(0.1, 0.015 * len(images)) + min(0.08, 0.02 * len(platform_targets)), 0.25, confidence_cap(candidate)), 2)
    if hard_blockers:
        confidence = round(min(confidence + 0.05, confidence_cap(candidate)), 2)

    if hard_blockers:
        operator_summary = "OpenClaw found a blocking Jeep Fact package issue."
        recommended_action = "request_revision"
        suggestions.extend(hard_blockers)
    elif warnings:
        operator_summary = "Jeep Fact Wednesday package is ready to approve with non-blocking warnings."
        recommended_action = "approve"
        suggestions.extend(warnings[:2])
    else:
        operator_summary = "Jeep Fact Wednesday package is ready to approve."
        recommended_action = "approve"

    reasoning = [
        operator_summary,
        f"Hard blockers: {len(hard_blockers)}.",
        f"Warnings: {len(warnings)}.",
    ]
    reasoning.extend(f"Blocker: {item}" for item in hard_blockers)
    reasoning.extend(f"Warning: {item}" for item in warnings[:3])

    component_scores = {
        "carousel_package": 0 if hard_blockers else 40,
        "caption": 20 if len(body) >= 120 else 5,
        "platform_payload": 20 if platform_targets or platform_variants else 0,
        "timing_fit": 10 if age_days is not None and age_days <= 3 else 6 if age_days is None or age_days <= 7 else 0,
        "support_warning_penalty": min(10, len(warnings) * 2),
    }

    review_contract = build_flow_review_contract(
        reviewer="jeepfact_carousel_package",
        hard_blockers=hard_blockers,
        warnings=warnings,
        checks=checks,
        operator_summary=operator_summary,
        approval_summary="Approve this Jeep Fact Wednesday carousel for the configured social publish lane.",
        recommended_action=recommended_action,
        operator_actions=["approve", "request_revision", "skip", "discard"],
    )

    return {
        "decision": decision,
        "score": score,
        "confidence": confidence,
        "priority": priority,
        "reasoning": reasoning,
        "improvement_suggestions": suggestions,
        "component_scores": component_scores,
        "fail_closed": hard_blockers,
        "review_contract": review_contract,
    }


THURSDAY_SOURCE_NOISE_PATTERNS = (
    re.compile(r"\b\d+\s*min\b", re.I),
    re.compile(r"\btonie\b", re.I),
    re.compile(r"\badd your own content\b", re.I),
    re.compile(r"\b3d\s*-?\s*printed\b", re.I),
    re.compile(r"\bcar dashboard\b", re.I),
    re.compile(r"\bcollectible figurine\b", re.I),
    re.compile(r"\bmultiple colors?\b", re.I),
)


def thursday_source_name_issues(names: list[str]) -> list[str]:
    issues: list[str] = []
    for name in names:
        clean_name = " ".join(str(name or "").split())
        if not clean_name:
            continue
        matched = [pattern.pattern for pattern in THURSDAY_SOURCE_NOISE_PATTERNS if pattern.search(clean_name)]
        if matched:
            issues.append(f"`{clean_name}` looks like it still contains source-listing wording; simplify the vote label before publishing.")
    return issues


def thursday_long_vote_label_warnings(names: list[str]) -> list[str]:
    warnings: list[str] = []
    for name in names:
        clean_name = " ".join(str(name or "").split())
        if not clean_name:
            continue
        words = [word for word in re.split(r"\s+", clean_name) if word]
        if len(words) > 6:
            warnings.append(f"`{clean_name}` is long for a vote card; prefer a short product-style duck name if the visual still reads clearly.")
    return warnings


def evaluate_thursday_publish_package(candidate: dict[str, Any], age_days: int | None) -> dict[str, Any]:
    """Evaluate This-or-That Thursday as an option-specific vote package."""
    summary = candidate.get("candidate_summary") or {}
    supporting = candidate.get("supporting_context") or {}
    body = " ".join(str(summary.get("body") or "").split())
    body_lower = body.lower()
    images = renderable_media_references(summary.get("images") or [])
    platform_targets = [str(item).strip() for item in (summary.get("platform_targets") or []) if str(item).strip()]
    platform_variants = summary.get("platform_variants") if isinstance(summary.get("platform_variants"), dict) else {}
    option_a = str(summary.get("option_a") or "").strip()
    option_b = str(summary.get("option_b") or "").strip()
    option_review = supporting.get("thursday_option_review") if isinstance(supporting.get("thursday_option_review"), dict) else {}
    source_refs = candidate.get("source_refs") or []
    trend_refs = supporting.get("trend_refs") or []

    hard_blockers: list[str] = []
    warnings: list[str] = []
    suggestions: list[str] = []
    checks: list[dict[str, str]] = []

    if images:
        checks.append(flow_review_check("pass", "Vote image is attached", images[0]))
    else:
        hard_blockers.append("This-or-That vote image is missing or is not a renderable image URL/path.")
        checks.append(flow_review_check("fail", "Vote image is attached", "No renderable vote image was found."))

    if len(body) >= 80:
        checks.append(flow_review_check("pass", "Caption is present", f"{len(body)} characters"))
    else:
        hard_blockers.append("This-or-That caption is too short or missing for approval.")
        checks.append(flow_review_check("fail", "Caption is present", f"{len(body)} characters"))

    if option_a and option_b and option_a.lower() != option_b.lower():
        checks.append(flow_review_check("pass", "Two clear vote choices are present", f"{option_a} vs {option_b}"))
    else:
        hard_blockers.append("This-or-That package must include two distinct vote choices.")
        checks.append(flow_review_check("fail", "Two clear vote choices are present", f"{option_a or 'missing'} vs {option_b or 'missing'}"))

    name_issues = thursday_source_name_issues([option_a, option_b])
    long_name_warnings = thursday_long_vote_label_warnings([option_a, option_b])
    if name_issues:
        hard_blockers.extend(name_issues)
        checks.append(flow_review_check("fail", "Vote labels are clean", "; ".join(name_issues[:2])))
    else:
        checks.append(flow_review_check("pass", "Vote labels are clean", "No source-listing residue found in the vote labels."))
    warnings.extend(long_name_warnings)

    vote_markers = ("this or that", "vote", "option a", "option b", "#thisorthatthursday")
    if any(marker in body_lower for marker in vote_markers) and option_a.lower() in body_lower and option_b.lower() in body_lower:
        checks.append(flow_review_check("pass", "Vote framing is recognizable", "Caption names both options and asks for a vote."))
    elif any(marker in body_lower for marker in vote_markers):
        warnings.append("Vote framing is partial; confirm the caption clearly names both options and asks for a vote.")
        checks.append(flow_review_check("warn", "Vote framing is recognizable", "Vote cue found, but one or both option names are not clear in the caption."))
    else:
        hard_blockers.append("This-or-That vote framing is missing from the caption.")
        checks.append(flow_review_check("fail", "Vote framing is recognizable", "No vote cue found."))

    option_failures = [str(item).strip() for item in (option_review.get("failures") or []) if str(item).strip()]
    option_warnings = [str(item).strip() for item in (option_review.get("warnings") or []) if str(item).strip()]
    if option_failures:
        hard_blockers.extend(f"Option review failure: {item}" for item in option_failures)
        checks.append(flow_review_check("fail", "Option review passed", "; ".join(option_failures[:2])))
    else:
        checks.append(flow_review_check("pass", "Option review passed", str(option_review.get("summary") or "No option-review failures.")))
    warnings.extend(f"Option review warning: {item}" for item in option_warnings)

    if platform_targets or platform_variants:
        platform_evidence = ", ".join(platform_targets) or ", ".join(sorted(platform_variants.keys()))
        checks.append(flow_review_check("pass", "Platform payload is present", platform_evidence))
    else:
        hard_blockers.append("Platform target/caption payload is missing, so approval cannot map to a publishable Thursday post.")
        checks.append(flow_review_check("fail", "Platform payload is present", "No platform targets or variants found."))

    if body_has_css_noise(body):
        hard_blockers.append("Caption appears to include email/CSS wrapper noise instead of clean social copy.")
        checks.append(flow_review_check("fail", "Caption is clean social copy", "CSS/email wrapper markers found."))
    else:
        checks.append(flow_review_check("pass", "Caption is clean social copy"))

    if age_days is None:
        warnings.append("Run date is unknown, so timing should be checked before posting.")
        timing_status = "warn"
        timing_evidence = "Unknown run date"
    elif age_days <= 3:
        timing_status = "pass"
        timing_evidence = f"{age_days} day(s) old"
    elif age_days <= 7:
        warnings.append("This-or-That package is several days old; approve only if it still fits the next Thursday slot.")
        timing_status = "warn"
        timing_evidence = f"{age_days} day(s) old"
    else:
        hard_blockers.append("This-or-That package is stale for Thursday approval; regenerate or intentionally reschedule it.")
        timing_status = "fail"
        timing_evidence = f"{age_days} day(s) old"
    checks.append(flow_review_check(timing_status, "Posting window is current", timing_evidence))

    if len(source_refs) <= 1 and not trend_refs:
        warnings.append("Trend/support evidence is thin, but the final This-or-That package itself is complete.")

    score = 97 - (len(warnings) * 4) - (len(hard_blockers) * 35)
    score = int(clamp(score, 0, 100))
    if hard_blockers:
        decision = "needs_revision" if score >= 40 else "discard"
        priority = "high"
    else:
        decision = "publish_ready"
        priority = "medium"

    confidence = round(clamp(0.64 + min(0.08, 0.03 * len(images)) + min(0.06, 0.02 * len(platform_targets)), 0.25, confidence_cap(candidate)), 2)
    if hard_blockers:
        confidence = round(min(confidence + 0.05, confidence_cap(candidate)), 2)

    if hard_blockers:
        operator_summary = "OpenClaw found a blocking This-or-That Thursday package issue."
        recommended_action = "request_revision"
        suggestions.extend(hard_blockers)
    elif warnings:
        operator_summary = "This-or-That Thursday package is ready to approve with non-blocking warnings."
        recommended_action = "approve"
        suggestions.extend(warnings[:2])
    else:
        operator_summary = "This-or-That Thursday package is ready to approve."
        recommended_action = "approve"

    reasoning = [
        operator_summary,
        f"Hard blockers: {len(hard_blockers)}.",
        f"Warnings: {len(warnings)}.",
    ]
    reasoning.extend(f"Blocker: {item}" for item in hard_blockers)
    reasoning.extend(f"Warning: {item}" for item in warnings[:3])

    component_scores = {
        "vote_package": 0 if hard_blockers else 40,
        "caption": 20 if len(body) >= 80 else 5,
        "platform_payload": 15 if platform_targets or platform_variants else 0,
        "label_quality": 15 if not name_issues else 0,
        "timing_fit": 10 if age_days is not None and age_days <= 3 else 6 if age_days is None or age_days <= 7 else 0,
        "warning_penalty": min(10, len(warnings) * 2),
    }

    review_contract = build_flow_review_contract(
        reviewer="thursday_vote_package",
        hard_blockers=hard_blockers,
        warnings=warnings,
        checks=checks,
        operator_summary=operator_summary,
        approval_summary="Approve this This-or-That Thursday vote package for the configured social publish lane.",
        recommended_action=recommended_action,
        operator_actions=["approve", "request_revision", "skip", "discard"],
    )

    return {
        "decision": decision,
        "score": score,
        "confidence": confidence,
        "priority": priority,
        "reasoning": reasoning,
        "improvement_suggestions": suggestions,
        "component_scores": component_scores,
        "fail_closed": hard_blockers,
        "review_contract": review_contract,
    }


def text_tokens(value: str) -> set[str]:
    import re

    return {
        token
        for token in re.findall(r"[a-z0-9']+", (value or "").lower())
        if len(token) >= 4 and token not in {"that", "this", "with", "your", "have", "from", "thank"}
    }


def lexical_overlap(a: str, b: str) -> float:
    left = text_tokens(a)
    right = text_tokens(b)
    if not left or not right:
        return 0.0
    return len(left.intersection(right)) / len(left)


def has_public_review_reply_warmth(response: str) -> bool:
    lowered = response.lower()
    return any(term in lowered for term in PUBLIC_REVIEW_REPLY_WARMTH_TERMS)


def evaluate_review_story(candidate: dict[str, Any], age_days: int | None) -> dict[str, Any]:
    summary = candidate.get("candidate_summary") or {}
    supporting = candidate.get("supporting_context") or {}
    source_refs = candidate.get("source_refs") or []
    selected_review = summary.get("selected_review") or summary.get("body") or ""
    story_score = summary.get("story_ai_score")
    stats = supporting.get("review_stats") or {}
    positive_reviews = int(stats.get("five_star_reviews") or 0)
    low_reviews = int(stats.get("low_rating_reviews") or 0)
    story_images = renderable_media_references(summary.get("images") or [])
    has_image = bool(story_images)

    reasoning: list[str] = []
    suggestions: list[str] = []
    fail_closed: list[str] = []

    support = 10 + min(5, positive_reviews) + min(5, max(0, int(story_score or 0) - 4))
    support = int(clamp(support, 0, 20))
    reasoning.append(
        f"Support score {support}/20 from {positive_reviews} five-star reviews and story AI score `{story_score or 'n/a'}`."
    )

    brand_fit = 18 if selected_review else 12
    reasoning.append("Brand-fit score %s/20 based on whether the selected review reads like strong public social proof." % brand_fit)

    clarity = 4
    if len(selected_review) >= 30:
        clarity += 5
    if has_image:
        clarity += 4
    if summary.get("template_id"):
        clarity += 1
    clarity = int(clamp(clarity, 0, 15))
    reasoning.append(
        f"Clarity score {clarity}/15 from selected review quality and whether the final story asset is attached."
    )

    differentiation = 9 + min(4, max(0, int(story_score or 0) - 5))
    differentiation = int(clamp(differentiation, 0, 15))
    reasoning.append(
        f"Differentiation score {differentiation}/15 from how strong and specific the featured review looks for a public story."
    )

    conversion = 5
    if any(term in selected_review.lower() for term in ("recommend", "love", "great", "quality", "gift")):
        conversion += 5
    if has_image:
        conversion += 3
    if len(selected_review.split()) >= 6:
        conversion += 2
    conversion = int(clamp(conversion, 0, 15))
    reasoning.append(
        f"Conversion-quality score {conversion}/15 from the review's persuasive language and whether the story has a usable asset."
    )

    timing = 10 if age_days is not None and age_days <= 1 else 7 if age_days is not None and age_days <= 3 else 4
    reasoning.append(f"Timing score {timing}/10 based on how recent the review summary is.")

    risk_penalty = 0
    if not has_image:
        risk_penalty += 2
        suggestions.append("Attach the final story image URL or local asset path before asking for publish approval.")
    if story_score is not None and int(story_score) < 7:
        risk_penalty += 2
    if low_reviews > 0:
        risk_penalty += 1
    risk_penalty = int(clamp(risk_penalty, 0, 5))
    reasoning.append(
        f"Risk penalty {risk_penalty}/5 from missing story assets, weaker AI selection score, and mixed review-day context."
    )

    score = int(clamp(support + brand_fit + clarity + differentiation + conversion + timing - risk_penalty, 0, 100))

    if not selected_review:
        fail_closed.append("No selected review text was preserved in the review summary email.")
    if not has_image:
        fail_closed.append("Story candidate does not include a final story image.")
    if story_score is not None and int(story_score) < 6:
        fail_closed.append("Story AI score is too weak for automatic public posting.")

    if fail_closed:
        decision = "discard" if score < 60 else "needs_revision"
    else:
        decision = "publish_ready" if score >= 76 else "needs_revision" if score >= 58 else "discard"

    priority = "high" if decision != "discard" else "medium"
    raw_confidence = 0.57 + min(0.10, 0.03 * len(source_refs)) + (0.05 if has_image else 0.0)
    if story_score is not None and int(story_score) >= 7:
        raw_confidence += 0.05
    confidence = round(clamp(raw_confidence, 0.30, confidence_cap(candidate)), 2)

    if fail_closed:
        reasoning.extend(f"Fail-closed trigger: {message}" for message in fail_closed)

    return {
        "decision": decision,
        "score": score,
        "confidence": confidence,
        "priority": priority,
        "reasoning": reasoning,
        "improvement_suggestions": list(dict.fromkeys(suggestions + default_suggestions("reviews_story"))),
        "component_scores": {
            "support": support,
            "brand_fit": brand_fit,
            "clarity": clarity,
            "differentiation": differentiation,
            "conversion_quality": conversion,
            "timing_fit": timing,
            "risk_penalty": risk_penalty,
        },
        "fail_closed": fail_closed,
    }


def evaluate_review_reply(candidate: dict[str, Any], age_days: int | None, private_mode: bool) -> dict[str, Any]:
    summary = candidate.get("candidate_summary") or {}
    source_refs = candidate.get("source_refs") or []
    customer_review = summary.get("customer_review") or ""
    response = summary.get("body") or ""
    overlap = lexical_overlap(customer_review, response)
    has_placeholder = "[" in response and "]" in response
    has_apology = any(term in response.lower() for term in ("sorry", "apolog"))
    has_remedy = any(term in response.lower() for term in ("refund", "replacement", "make this right", "make things right"))
    reply_contract = build_review_reply_contract(customer_review, response, private_mode=private_mode)
    contract_failures = contract_failure_messages(reply_contract)
    contract_suggestions = contract_improvement_suggestions(reply_contract)

    reasoning: list[str] = []
    suggestions: list[str] = []
    fail_closed: list[str] = []

    support = 11 + (4 if customer_review else 0) + (5 if response else 0)
    if private_mode and summary.get("review_rating") is not None:
        support += 0 if int(summary.get("review_rating") or 0) > 2 else 0
    support = int(clamp(support, 0, 20))
    reasoning.append(
        f"Support score {support}/20 from preserved customer-review context and a draft response in the daily summary."
    )

    if private_mode:
        brand_fit = 17 if has_apology and has_remedy else 12
    else:
        brand_fit = 18 if has_public_review_reply_warmth(response) else 13
    reasoning.append(
        f"Brand-fit score {brand_fit}/20 based on tone, empathy, and whether the reply matches the review type."
    )

    clarity = 4
    if 70 <= len(response) <= 700:
        clarity += 6
    if customer_review:
        clarity += 2
    if summary.get("transaction_id"):
        clarity += 1
    if summary.get("next_steps"):
        clarity += 2
    clarity = int(clamp(clarity, 0, 15))
    reasoning.append(
        f"Clarity score {clarity}/15 from response completeness, usable length, and case-specific context."
    )

    differentiation = 5 + min(8, int(round(overlap * 12)))
    if private_mode and has_remedy:
        differentiation += 1
    differentiation = int(clamp(differentiation, 0, 15))
    reasoning.append(
        f"Differentiation score {differentiation}/15 from how specifically the draft responds to what the customer actually said."
    )

    conversion = 5
    if not private_mode and (
        has_public_review_reply_warmth(response)
        or any(term in response.lower() for term in ("love", "support", "kind words"))
    ):
        conversion += 4
    if private_mode and has_apology:
        conversion += 3
    if private_mode and has_remedy:
        conversion += 4
    if summary.get("next_steps"):
        conversion += 2
    conversion = int(clamp(conversion, 0, 15))
    reasoning.append(
        f"Conversion-quality score {conversion}/15 from warmth, resolution strength, and whether the next action is clear."
    )

    timing = 10 if age_days is not None and age_days <= 1 else 8 if age_days is not None and age_days <= 3 else 5
    reasoning.append(f"Timing score {timing}/10 based on how quickly this reply can be used after the review arrived.")

    risk_penalty = 0
    if has_placeholder:
        risk_penalty += 2
        suggestions.append("Replace placeholder text like customer-name tokens before using this draft.")
    if len(response) < 60:
        risk_penalty += 1
    if len(response) > 850:
        risk_penalty += 1
    if private_mode and not has_remedy:
        risk_penalty += 2
    if not private_mode and "refund" in response.lower():
        risk_penalty += 2
    if contract_failures:
        risk_penalty += 2
        suggestions.extend(contract_suggestions)
    risk_penalty = int(clamp(risk_penalty, 0, 5))
    reasoning.append(
        f"Risk penalty {risk_penalty}/5 from placeholder text, length issues, and remedy mismatch risk."
    )

    score = int(clamp(support + brand_fit + clarity + differentiation + conversion + timing - risk_penalty, 0, 100))

    if not response:
        fail_closed.append("No reply text was preserved for review.")
    if has_placeholder:
        fail_closed.append("Reply still contains placeholder text and is not ready to send as written.")
    if private_mode and not has_apology:
        fail_closed.append("Low-rating private reply is missing an explicit apology.")
    if private_mode and not has_remedy:
        fail_closed.append("Low-rating private reply does not clearly offer a remedy or next step.")
    fail_closed.extend(f"Review reply contract failed: {message}" for message in contract_failures)

    if fail_closed:
        decision = "discard" if score < 60 else "needs_revision"
    else:
        decision = "publish_ready" if score >= 78 else "needs_revision" if score >= 60 else "discard"

    priority = "high" if private_mode else "medium"
    raw_confidence = 0.58 + min(0.08, 0.03 * len(source_refs)) + min(0.08, overlap * 0.15)
    if private_mode and has_remedy:
        raw_confidence += 0.04
    confidence = round(clamp(raw_confidence, 0.30, confidence_cap(candidate)), 2)

    if fail_closed:
        reasoning.extend(f"Fail-closed trigger: {message}" for message in fail_closed)

    flow = "reviews_reply_private" if private_mode else "reviews_reply_positive"
    # Flow-level default suggestions are generic style guidance. Including them on
    # publish_ready items confuses the operator (boilerplate appears under
    # "Why OpenClaw wants revision" labels in the Decision Inbox). Only attach
    # defaults when the item actually needs revision so per-item-specific
    # suggestions can speak for themselves on clean items.
    flow_defaults = default_suggestions(flow) if decision != "publish_ready" else []
    return {
        "decision": decision,
        "score": score,
        "confidence": confidence,
        "priority": priority,
        "reasoning": reasoning,
        "improvement_suggestions": list(dict.fromkeys(suggestions + flow_defaults)),
        "component_scores": {
            "support": support,
            "brand_fit": brand_fit,
            "clarity": clarity,
            "differentiation": differentiation,
            "conversion_quality": conversion,
            "timing_fit": timing,
            "risk_penalty": risk_penalty,
        },
        "fail_closed": fail_closed,
        "reply_contract": reply_contract,
    }


def evaluate_quality_gate(candidate: dict[str, Any]) -> dict[str, Any]:
    flow = candidate.get("flow") or "unknown"
    summary = candidate.get("candidate_summary") or {}
    supporting = candidate.get("supporting_context") or {}
    notes = candidate.get("normalization_notes") or {}
    body = summary.get("body") or ""
    title = summary.get("title") or "unknown"
    body_len = len(body)
    publish_token = str(summary.get("publish_token") or "").strip()
    source_mode = str(notes.get("source_mode") or "").strip()
    trend_refs = supporting.get("trend_refs") or []
    catalog_overlap = supporting.get("catalog_overlap") or []
    source_refs = candidate.get("source_refs") or []
    run_date = parse_run_date(candidate.get("run_id"))
    age_days = None
    if run_date is not None:
        age_days = (datetime.now() - run_date).days

    if flow == "reviews_story":
        outcome = evaluate_review_story(candidate, age_days)
        evidence_refs = [ref.get("path", "") for ref in source_refs[:5] if ref.get("path")]
        story_images = renderable_media_references(summary.get("images") or [])
        return {
            "artifact_id": candidate["artifact_id"],
            "artifact_type": candidate.get("artifact_type", "social_post"),
            "flow": flow,
            "run_id": candidate.get("run_id"),
            "artifact_slug": slugify(f"{flow}-{candidate.get('run_id', 'unknown')}"),
            "decision": outcome["decision"],
            "score": outcome["score"],
            "confidence": outcome["confidence"],
            "priority": outcome["priority"],
            "reasoning": outcome["reasoning"],
            "improvement_suggestions": outcome["improvement_suggestions"] if outcome["decision"] != "publish_ready" else outcome["improvement_suggestions"][:2],
            "evidence_refs": evidence_refs,
            "review_status": "pending",
            "created_at": now_iso(),
            "title": title,
            "preview": {
                "proposed_label": "Selected review",
                "proposed_text": preview_text(summary.get("selected_review") or summary.get("body")),
                "asset_url": story_images[0] if story_images else None,
            },
            "quality_gate_metadata": {
                "age_days": age_days,
                "source_mode": notes.get("source_mode"),
                "confidence_cap": confidence_cap(candidate),
                "component_scores": outcome["component_scores"],
                "fail_closed": outcome["fail_closed"],
            },
        }

    if flow in {"reviews_reply_positive", "reviews_reply_private"}:
        outcome = evaluate_review_reply(candidate, age_days, private_mode=(flow == "reviews_reply_private"))
        evidence_refs = [ref.get("path", "") for ref in source_refs[:5] if ref.get("path")]
        review_target = candidate.get("review_target") or {
            "shop_id": None,
            "review_key": slugify(title),
            "review_id": None,
            "transaction_id": summary.get("transaction_id"),
            "listing_id": None,
            "review_url": None,
            "match_quality": "missing",
        }
        return {
            "artifact_id": candidate["artifact_id"],
            "artifact_type": candidate.get("artifact_type", "review_reply"),
            "flow": flow,
            "run_id": candidate.get("run_id"),
            "artifact_slug": slugify(f"{flow}-{candidate.get('run_id', 'unknown')}-{title}"),
            "decision": outcome["decision"],
            "score": outcome["score"],
            "confidence": outcome["confidence"],
            "priority": outcome["priority"],
            "reasoning": outcome["reasoning"],
            "improvement_suggestions": outcome["improvement_suggestions"] if outcome["decision"] != "publish_ready" else outcome["improvement_suggestions"][:2],
            "evidence_refs": evidence_refs,
            "review_status": "pending",
            "created_at": now_iso(),
            "title": title,
            "preview": {
                "context_label": "Customer review",
                "context_text": preview_text(summary.get("customer_review")),
                "proposed_label": "Draft reply",
                "proposed_text": preview_text(summary.get("body")),
            },
            "approved_reply_text": summary.get("body") or "",
            "execution_mode": "manual_only",
            "review_target": review_target,
            "execution_state": "not_queued",
            "execution_attempts": [],
            "operator_resolution": {
                "action": "none",
                "note": None,
                "recorded_at": None,
            },
            "quality_gate_metadata": {
                "age_days": age_days,
                "source_mode": notes.get("source_mode"),
                "confidence_cap": confidence_cap(candidate),
                "component_scores": outcome["component_scores"],
                "fail_closed": outcome["fail_closed"],
                "reply_contract": outcome["reply_contract"],
                "review_target_match_quality": review_target.get("match_quality"),
            },
        }

    if flow == "meme":
        outcome = evaluate_meme_publish_package(candidate, age_days)
        evidence_refs = [ref.get("path", "") for ref in source_refs[:5] if ref.get("path")]
        meme_images = renderable_media_references(summary.get("images") or [])
        suggestions = list(dict.fromkeys(outcome["improvement_suggestions"] + default_suggestions(flow)))
        return {
            "artifact_id": candidate["artifact_id"],
            "artifact_type": candidate.get("artifact_type", "social_post"),
            "flow": flow,
            "run_id": candidate.get("run_id"),
            "artifact_slug": slugify(f"{flow}-{candidate.get('run_id', 'unknown')}"),
            "decision": outcome["decision"],
            "score": outcome["score"],
            "confidence": outcome["confidence"],
            "priority": outcome["priority"],
            "reasoning": outcome["reasoning"],
            "improvement_suggestions": suggestions if outcome["decision"] != "publish_ready" else suggestions[:2],
            "evidence_refs": evidence_refs,
            "review_status": "pending",
            "created_at": now_iso(),
            "title": title,
            "preview": {
                "proposed_label": "Draft caption",
                "proposed_text": preview_text(body),
                "asset_url": meme_images[0] if meme_images else None,
                "asset_urls": meme_images,
            },
            "quality_gate_metadata": {
                "age_days": age_days,
                "source_mode": notes.get("source_mode"),
                "confidence_cap": confidence_cap(candidate),
                "component_scores": outcome["component_scores"],
                "fail_closed": outcome["fail_closed"],
                "flow_review_contract": outcome["review_contract"],
            },
        }

    if flow == "jeepfact":
        outcome = evaluate_jeepfact_publish_package(candidate, age_days)
        evidence_refs = [ref.get("path", "") for ref in source_refs[:5] if ref.get("path")]
        jeepfact_images = renderable_media_references(summary.get("images") or [])
        suggestions = list(dict.fromkeys(outcome["improvement_suggestions"] + default_suggestions(flow)))
        return {
            "artifact_id": candidate["artifact_id"],
            "artifact_type": candidate.get("artifact_type", "social_post"),
            "flow": flow,
            "run_id": candidate.get("run_id"),
            "artifact_slug": slugify(f"{flow}-{candidate.get('run_id', 'unknown')}"),
            "decision": outcome["decision"],
            "score": outcome["score"],
            "confidence": outcome["confidence"],
            "priority": outcome["priority"],
            "reasoning": outcome["reasoning"],
            "improvement_suggestions": suggestions if outcome["decision"] != "publish_ready" else suggestions[:2],
            "evidence_refs": evidence_refs,
            "review_status": "pending",
            "created_at": now_iso(),
            "title": title,
            "preview": {
                "proposed_label": "Draft caption",
                "proposed_text": preview_text(body),
                "asset_url": jeepfact_images[0] if jeepfact_images else None,
                "asset_urls": jeepfact_images,
            },
            "quality_gate_metadata": {
                "age_days": age_days,
                "source_mode": notes.get("source_mode"),
                "confidence_cap": confidence_cap(candidate),
                "component_scores": outcome["component_scores"],
                "fail_closed": outcome["fail_closed"],
                "flow_review_contract": outcome["review_contract"],
            },
        }

    if flow == "thursday":
        outcome = evaluate_thursday_publish_package(candidate, age_days)
        evidence_refs = [ref.get("path", "") for ref in source_refs[:5] if ref.get("path")]
        thursday_images = renderable_media_references(summary.get("images") or [])
        suggestions = list(dict.fromkeys(outcome["improvement_suggestions"] + default_suggestions(flow)))
        return {
            "artifact_id": candidate["artifact_id"],
            "artifact_type": candidate.get("artifact_type", "social_post"),
            "flow": flow,
            "run_id": candidate.get("run_id"),
            "artifact_slug": slugify(f"{flow}-{candidate.get('run_id', 'unknown')}-option-{summary.get('option_id') or 'unknown'}"),
            "decision": outcome["decision"],
            "score": outcome["score"],
            "confidence": outcome["confidence"],
            "priority": outcome["priority"],
            "reasoning": outcome["reasoning"],
            "improvement_suggestions": suggestions if outcome["decision"] != "publish_ready" else suggestions[:2],
            "evidence_refs": evidence_refs,
            "review_status": "pending",
            "created_at": now_iso(),
            "title": title,
            "preview": {
                "proposed_label": "Draft caption",
                "proposed_text": preview_text(body),
                "asset_url": thursday_images[0] if thursday_images else None,
                "asset_urls": thursday_images[:1],
            },
            "quality_gate_metadata": {
                "age_days": age_days,
                "source_mode": notes.get("source_mode"),
                "confidence_cap": confidence_cap(candidate),
                "component_scores": outcome["component_scores"],
                "fail_closed": outcome["fail_closed"],
                "flow_review_contract": outcome["review_contract"],
                "thursday_option_id": summary.get("option_id") or notes.get("thursday_option_id"),
                "thursday_option_a": summary.get("option_a"),
                "thursday_option_b": summary.get("option_b"),
            },
        }

    reasoning: list[str] = []
    suggestions: list[str] = []
    fail_closed: list[str] = []

    # 1. trend or campaign support (20)
    if flow == "weekly_sale":
        support = 12 + min(6, len(trend_refs)) + min(2, max(0, len(source_refs) - 1))
        if "[PUBLISH:" in body:
            support += 1
    elif flow in {"meme", "jeepfact"}:
        support = 10 + min(4, len(source_refs)) + min(4, len(summary.get("images") or [])) + min(2, len(trend_refs))
    else:
        support = 4 + min(8, len(trend_refs) * 4)
        if source_refs:
            support += 1
    support = int(clamp(support, 0, 20))
    reasoning.append(f"Support score {support}/20 from {len(trend_refs)} related trend references and {len(source_refs)} source references.")

    # 2. brand fit (20)
    if flow == "newduck":
        brand_fit = 19 if "duck" in title.lower() else 13
    elif flow == "weekly_sale":
        brand_fit = 18
    elif flow in {"meme", "jeepfact"}:
        brand_fit = 18 if ("duck" in title.lower() or "jeep" in body.lower()) else 15
    else:
        brand_fit = 14
    reasoning.append(f"Brand-fit score {brand_fit}/20 based on flow `{flow}` and candidate framing.")

    # 3. clarity and specificity (15)
    clarity = 3
    if body_len >= 300:
        clarity += 5
    if body_len >= 900:
        clarity += 3
    if summary.get("platform_variants"):
        clarity += 2
    if flow in {"meme", "jeepfact"} and summary.get("images"):
        clarity += 3
    if "[PUBLISH:" in body:
        clarity += 1
    if flow == "weekly_sale" and source_mode == "state_file" and publish_token:
        clarity += 1
    if body_has_css_noise(body):
        clarity -= 4
        suggestions.append("Strip CSS and email wrapper noise before using this artifact as the final publish-review surface.")
    clarity = int(clamp(clarity, 0, 15))
    reasoning.append(f"Clarity score {clarity}/15 from artifact completeness and how directly the body communicates the publishable payload.")

    # 4. differentiation (15)
    if flow == "newduck":
        if catalog_overlap:
            differentiation = 3 if len(trend_refs) <= 1 else 6
            suggestions.append("Explain how this duck differs from the existing catalog entry before publishing.")
        else:
            differentiation = 12
    elif flow == "meme":
        differentiation = 9 + min(3, len(trend_refs))
    elif flow == "jeepfact":
        differentiation = 10 + min(3, max(0, len(summary.get("images") or []) - 1))
    else:
        differentiation = 10 + min(3, len(trend_refs) // 2)
    differentiation = int(clamp(differentiation, 0, 15))
    reasoning.append(f"Differentiation score {differentiation}/15 based on catalog overlap and whether the action looks distinct enough to justify execution.")

    # 5. likely conversion quality (15)
    conversion = 4
    if body_len >= 600:
        conversion += 4
    if flow == "newduck" and "gift ideas" in body.lower():
        conversion += 3
    if flow == "weekly_sale" and "strategic summary" in body.lower():
        conversion += 4
    if flow == "meme" and any(token in body.lower() for token in ("meme monday", "pov:", "tag your", "#mememonday")):
        conversion += 4
    if flow == "jeepfact":
        conversion += min(4, len(summary.get("images") or []))
    if flow == "weekly_sale":
        sale_action_markers = [
            "theme of the week:",
            "market match:",
            "momentum boosters:",
            "re-engagement:",
        ]
        marker_count = sum(marker in body.lower() for marker in sale_action_markers)
        if marker_count >= 4:
            conversion += 4
        elif marker_count >= 2:
            conversion += 2
    if summary.get("platform_variants"):
        conversion += 2
    conversion = int(clamp(conversion, 0, 15))
    reasoning.append(f"Conversion-quality score {conversion}/15 from the amount of actionable copy and campaign context present.")

    # 6. timing fit (10)
    timing = 6
    if flow == "weekly_sale":
        if age_days is None:
            timing = 4
        elif age_days <= 1:
            timing = 10
        elif age_days <= 3:
            timing = 8
        elif age_days <= 6:
            timing = 5
        else:
            timing = 2
    elif flow in {"meme", "jeepfact"}:
        if age_days is None:
            timing = 5
        elif age_days <= 2:
            timing = 10
        elif age_days <= 7:
            timing = 7
        else:
            timing = 3
    elif flow == "newduck":
        timing = 8 if age_days is not None and age_days <= 3 and trend_refs else 6
    reasoning.append(f"Timing score {timing}/10 based on candidate age and whether the supporting signals still look current.")

    # 7. risk penalties (0-5)
    risk_penalty = 0
    if notes.get("completeness") == "partial_email":
        risk_penalty += 2
    if catalog_overlap and flow == "newduck":
        risk_penalty += 2
    if flow in {"meme", "jeepfact"} and not summary.get("images"):
        risk_penalty += 2
    if body_has_css_noise(body):
        risk_penalty += 1
    risk_penalty = int(clamp(risk_penalty, 0, 5))
    reasoning.append(f"Risk penalty {risk_penalty}/5 from partial-email dependence, overlap risk, and email-wrapper noise.")

    score = support + brand_fit + clarity + differentiation + conversion + timing - risk_penalty
    score = int(clamp(score, 0, 100))

    if body_len < 180:
        fail_closed.append("Artifact is materially incomplete for a strict publish review.")
    if flow == "newduck" and catalog_overlap and len(trend_refs) <= 1:
        fail_closed.append("Existing catalog already covers this duck theme without enough new evidence to justify another publish.")
    if flow == "weekly_sale" and age_days is not None and age_days >= 7:
        fail_closed.append("Weekly sale playbook is stale for a publish decision and should not be acted on as-is.")
    if flow in {"meme", "jeepfact"} and not summary.get("images"):
        fail_closed.append("Social post candidate does not include a preview image for approval.")
    if clarity < 6:
        fail_closed.append("Artifact is too unclear to mark publish-ready.")

    if flow == "weekly_sale" and age_days is not None and age_days >= 7 and "Re-run the weekly flow" not in " ".join(suggestions):
        suggestions.append("Re-run the weekly flow so the sale playbook reflects the current week before publishing.")
    if flow == "newduck" and not summary.get("images"):
        suggestions.append("Preserve or attach the final review images so the gate can verify the full listing package next time.")
    if flow == "weekly_sale" and "[PUBLISH:" not in body and not publish_token:
        suggestions.append("Ensure the final review artifact carries a publish token so approval can be traced back to the exact run.")
    if flow == "meme" and not summary.get("images"):
        suggestions.append("Preserve the final meme image URL so the operator can approve the actual post package in WhatsApp.")
    if flow == "jeepfact" and len(summary.get("images") or []) < 2:
        suggestions.append("Preserve the Jeep Fact image set so the operator can review more than a single cover image.")

    if fail_closed:
        if flow == "weekly_sale" and age_days is not None and age_days >= 7:
            decision = "discard"
        elif flow == "newduck" and catalog_overlap:
            decision = "discard" if score < 70 else "needs_revision"
        else:
            decision = "needs_revision" if score >= 55 else "discard"
    else:
        if score >= 82:
            decision = "publish_ready"
        elif score >= 55:
            decision = "needs_revision"
        else:
            decision = "discard"

    if decision == "publish_ready":
        priority = "high" if flow == "weekly_sale" else "medium"
    elif decision == "needs_revision":
        priority = "high"
    else:
        priority = "medium" if flow == "weekly_sale" and age_days and age_days >= 7 else "high"

    raw_confidence = 0.52 + min(0.12, 0.04 * len(source_refs)) + min(0.12, 0.03 * len(trend_refs))
    if body_len >= 800:
        raw_confidence += 0.06
    if fail_closed:
        raw_confidence += 0.05
    confidence = round(clamp(raw_confidence, 0.25, confidence_cap(candidate)), 2)

    if fail_closed:
        reasoning.extend(f"Fail-closed trigger: {message}" for message in fail_closed)

    evidence_refs = []
    for ref in source_refs[:5]:
        evidence_refs.append(ref.get("path", ""))
    for ref in trend_refs[:5]:
        if ref.get("artifact_id"):
            evidence_refs.append(ref["artifact_id"])

    artifact_slug = slugify(title if flow == "newduck" else f"{flow}-{candidate.get('run_id', 'unknown')}")
    suggestions = list(dict.fromkeys(suggestions + default_suggestions(flow)))

    return {
        "artifact_id": candidate["artifact_id"],
        "artifact_type": candidate.get("artifact_type", "listing"),
        "flow": flow,
        "run_id": candidate.get("run_id"),
        "artifact_slug": artifact_slug,
        "decision": decision,
        "score": score,
        "confidence": confidence,
        "priority": priority,
        "reasoning": reasoning,
        "improvement_suggestions": suggestions if decision != "publish_ready" else suggestions[:2],
        "evidence_refs": [ref for ref in evidence_refs if ref],
        "review_status": "pending",
        "created_at": now_iso(),
        "title": title,
        "preview": {
            "proposed_label": "Draft body" if flow not in {"meme", "jeepfact"} else "Draft caption",
            "proposed_text": preview_text(body),
            "asset_url": ((summary.get("images") or [None])[0]),
            "asset_urls": summary.get("images") or [],
        },
        "quality_gate_metadata": {
            "age_days": age_days,
            "source_mode": notes.get("source_mode"),
            "confidence_cap": confidence_cap(candidate),
            "component_scores": {
                "support": support,
                "brand_fit": brand_fit,
                "clarity": clarity,
                "differentiation": differentiation,
                "conversion_quality": conversion,
                "timing_fit": timing,
                "risk_penalty": risk_penalty,
            },
            "fail_closed": fail_closed,
        },
    }


def load_state() -> dict[str, Any]:
    return load_json(
        QUALITY_GATE_STATE_PATH,
        {
            "artifacts": {},
            "alerts": {},
            "last_digest_date": None,
        },
    )


def _artifact_review_status(artifact: dict[str, Any]) -> str:
    decision = artifact.get("decision") or {}
    return str(
        decision.get("review_status")
        or artifact.get("review_status")
        or artifact.get("status")
        or ""
    ).strip().lower()


def _prune_stale_alerts(state: dict[str, Any]) -> dict[str, Any]:
    artifacts = state.get("artifacts") or {}
    alerts = state.get("alerts") or {}
    if not isinstance(alerts, dict):
        state["alerts"] = {}
        return {}

    active: dict[str, Any] = {}
    for raw_key, payload in alerts.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        artifact_id, _, input_hash = key.partition("::")
        artifact = artifacts.get(artifact_id)
        if not isinstance(artifact, dict):
            continue
        review_status = _artifact_review_status(artifact)
        if review_status not in QUALITY_GATE_PENDING_REVIEW_STATUSES:
            continue
        current_input_hash = str(artifact.get("input_hash") or "").strip()
        if input_hash and current_input_hash and input_hash != current_input_hash:
            continue
        active[key] = payload

    if active != alerts:
        state["alerts"] = active
    return active


def sync_quality_gate_control(state: dict[str, Any]) -> dict[str, Any]:
    artifacts = state.get("artifacts") or {}
    alerts = _prune_stale_alerts(state)
    reviewed_count = 0
    needs_revision_count = 0
    pending_count = 0
    unknown_count = 0
    latest_artifact_dt: datetime | None = None

    for artifact in artifacts.values():
        if not isinstance(artifact, dict):
            continue
        decision = artifact.get("decision") or {}
        review_status = _artifact_review_status(artifact)
        if review_status in QUALITY_GATE_RESOLVED_REVIEW_STATUSES:
            reviewed_count += 1
        elif review_status in QUALITY_GATE_REVISION_REVIEW_STATUSES:
            needs_revision_count += 1
        elif review_status in QUALITY_GATE_PENDING_REVIEW_STATUSES:
            pending_count += 1
        elif review_status:
            unknown_count += 1
        artifact_dt = parse_timestamp(
            artifact.get("evaluated_at")
            or artifact.get("reviewed_at")
            or decision.get("created_at")
        )
        if artifact_dt and (latest_artifact_dt is None or artifact_dt > latest_artifact_dt):
            latest_artifact_dt = artifact_dt

    digest_dt = parse_timestamp(state.get("last_digest_date"))
    reference_dt = latest_artifact_dt if latest_artifact_dt and (digest_dt is None or latest_artifact_dt >= digest_dt) else digest_dt
    age_hours = None
    if reference_dt is not None:
        age_hours = round((datetime.now(timezone.utc).astimezone() - reference_dt).total_seconds() / 3600.0, 2)

    if age_hours is not None and age_hours >= 96:
        control_state = "blocked"
        reason = "stale_input"
        next_action = "Rebuild the quality gate so approvals and alerts reflect the current operator queue."
    elif alerts:
        control_state = "observed"
        reason = "alerts_pending"
        next_action = "Review the urgent quality gate alerts and clear or archive them."
    elif pending_count:
        control_state = "observed"
        reason = "awaiting_operator_resolution"
        next_action = "Review the pending quality-gate items and approve, revise, or discard them."
    elif unknown_count:
        control_state = "observed"
        reason = "unknown_review_status"
        next_action = "Normalize the quality-gate review statuses so the operator queue reflects real pending work."
    elif needs_revision_count:
        control_state = "observed"
        reason = "revision_pressure"
        next_action = "Clear the needs-revision backlog so the gate reflects real operator progress."
    elif artifacts:
        control_state = "verified"
        reason = "gating_ready"
        next_action = "Use the quality gate output as the current review source of truth."
    else:
        control_state = "observed"
        reason = "idle"
        next_action = "No quality-gate artifacts are available yet."

    control = record_workflow_transition(
        workflow_id="quality_gate",
        lane="quality_gate",
        display_label="Quality Gate",
        entity_id="quality_gate",
        state=control_state,
        state_reason=reason,
        input_freshness={
            "source": str(QUALITY_GATE_STATE_PATH),
            "age_hours": age_hours,
        },
        next_action=next_action,
        metadata={
            "tracked_artifacts": len(artifacts),
            "alert_count": len(alerts),
            "pending_count": pending_count,
            "needs_revision_count": needs_revision_count,
            "reviewed_count": reviewed_count,
            "unknown_status_count": unknown_count,
        },
        receipt_kind="state_sync",
        receipt_payload={
            "tracked_artifacts": len(artifacts),
            "alert_count": len(alerts),
            "pending_count": pending_count,
            "needs_revision_count": needs_revision_count,
            "reviewed_count": reviewed_count,
            "unknown_status_count": unknown_count,
        },
        history_summary=reason.replace("_", " "),
    )
    state["workflow_control"] = {
        "state": control_state,
        "state_reason": reason,
        "age_hours": age_hours,
        "path": str((control or {}).get("latest_receipt", {}).get("path") or ""),
    }
    return state


def save_state(state: dict[str, Any]) -> None:
    state = sync_quality_gate_control(state)
    write_json(QUALITY_GATE_STATE_PATH, state)


def alert_key(decision: dict[str, Any], input_hash: str) -> str:
    return f"{decision['artifact_id']}::{input_hash}"


def should_emit_urgent(decision: dict[str, Any], previous_decision: dict[str, Any] | None = None) -> bool:
    if previous_decision:
        if (
            previous_decision.get("decision") == decision.get("decision")
            and previous_decision.get("priority") == decision.get("priority")
            and previous_decision.get("review_status") == decision.get("review_status")
        ):
            return False

    metadata = decision.get("quality_gate_metadata") or {}
    age_days = metadata.get("age_days")
    if (
        decision["decision"] == "discard"
        and decision["priority"] == "high"
        and decision["confidence"] >= 0.70
        and age_days is not None
        and age_days <= 1
    ):
        return True
    return False


def write_urgent_alert(decision: dict[str, Any]) -> dict[str, str]:
    patterns = load_output_patterns()
    current = datetime.now()
    replacements = {
        "YYYY-MM-DDTHHMMSS": current.strftime("%Y-%m-%dT%H%M%S"),
        "artifact_id": slugify(decision["artifact_id"]),
    }
    json_path = render_pattern(patterns["urgent_json"], replacements)
    md_path = render_pattern(patterns["urgent_md"], replacements)
    payload = {
        "generated_at": now_iso(),
        "type": "urgent_quality_gate_alert",
        "decision": decision,
    }
    ensure_parent(json_path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = [
        "# Urgent Quality Gate Alert",
        "",
        f"- Artifact: `{decision['artifact_id']}`",
        f"- Decision: `{decision['decision']}`",
        f"- Score: `{decision['score']}`",
        f"- Confidence: `{decision['confidence']}`",
        f"- Priority: `{decision['priority']}`",
        "",
        "## Why It Triggered",
        "",
    ]
    lines.extend(f"- {item}" for item in decision.get("reasoning") or [])
    ensure_parent(md_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json_path": str(json_path), "md_path": str(md_path)}


def write_daily_digest(latest_records: dict[str, dict[str, Any]], new_decisions: list[dict[str, Any]]) -> dict[str, str] | None:
    active_decisions = [record["decision"] for record in latest_records.values()]
    pending_items = [item for item in active_decisions if item.get("review_status") == "pending"]
    if not new_decisions and not pending_items:
        return None

    patterns = load_output_patterns()
    current = datetime.now()
    replacements = {"YYYY-MM-DD": current.strftime("%Y-%m-%d")}
    json_path = render_pattern(patterns["digest_json"], replacements)
    md_path = render_pattern(patterns["digest_md"], replacements)

    counts = Counter(item["decision"] for item in active_decisions)
    pending_counts = Counter(item["decision"] for item in pending_items)
    ordered = sorted(
        pending_items,
        key=lambda item: (priority_rank(item["priority"]), item["score"], item["artifact_id"]),
        reverse=True,
    )
    new_ordered = sorted(
        new_decisions,
        key=lambda item: (priority_rank(item["priority"]), item["score"], item["artifact_id"]),
        reverse=True,
    )

    payload = {
        "generated_at": now_iso(),
        "type": "quality_gate_digest",
        "new_decision_count": len(new_decisions),
        "active_counts": dict(counts),
        "pending_counts": dict(pending_counts),
        "pending_review_count": len(pending_items),
        "new_items": new_ordered,
        "pending_items": ordered,
        "items": ordered,
    }
    ensure_parent(json_path).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# Quality Gate Daily Digest",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- New decisions this run: `{len(new_decisions)}`",
        f"- Still pending review: `{len(pending_items)}`",
        f"- Publish-ready: `{counts.get('publish_ready', 0)}`",
        f"- Needs revision: `{counts.get('needs_revision', 0)}`",
        f"- Discard: `{counts.get('discard', 0)}`",
        "",
        "Reviewed or archived items are not listed below.",
        "",
    ]

    lines.append("## New Decisions This Run")
    lines.append("")
    if not new_ordered:
        lines.append("No new decisions this run.")
    else:
        for item in new_ordered:
            lines.append(
                f"- `{item['decision']}` | `{item['priority']}` | score `{item['score']}` | confidence `{item['confidence']}` | `{item['artifact_id']}`"
            )
            first_reason = (item.get("reasoning") or ["No reasoning captured."])[0]
            lines.append(f"  Reason: {first_reason}")

    lines.extend(["", "## Still Pending Review", ""])
    if not ordered:
        lines.append("No pending review items.")
    else:
        for item in ordered:
            lines.append(
                f"- `{item['decision']}` | `{item['priority']}` | score `{item['score']}` | confidence `{item['confidence']}` | `{item['artifact_id']}`"
            )
            first_reason = (item.get("reasoning") or ["No reasoning captured."])[0]
            lines.append(f"  Reason: {first_reason}")
    ensure_parent(md_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json_path": str(json_path), "md_path": str(md_path)}


def main() -> int:
    payload = load_json(PUBLISH_CANDIDATES_PATH, {"items": []})
    candidates = payload.get("items", [])
    state = load_state()
    latest_records: dict[str, dict[str, Any]] = state.setdefault("artifacts", {})
    history_rows: list[dict[str, Any]] = []
    new_decisions: list[dict[str, Any]] = []

    for candidate in candidates:
        artifact_id = candidate["artifact_id"]
        material_hash = material_candidate_hash(candidate)
        input_hash = canonical_hash({"evaluator_version": EVALUATOR_VERSION, "material_hash": material_hash})
        previous = latest_records.get(artifact_id)
        if previous and previous.get("input_hash") == input_hash:
            continue

        previous_decision = previous.get("decision") if previous else None
        decision = evaluate_quality_gate(candidate)
        execution_reconciliation_fields = apply_execution_state_reconciliation(candidate, decision)
        carried_record_fields = carry_forward_review_resolution(decision, previous, material_hash=material_hash)
        output_paths = write_decision(decision)
        record = {
            "artifact_id": artifact_id,
            "input_hash": input_hash,
            "material_hash": material_hash,
            "evaluated_at": decision["created_at"],
            "decision": decision,
            "output_paths": output_paths,
        }
        record.update(execution_reconciliation_fields)
        record.update(carried_record_fields)
        latest_records[artifact_id] = record
        new_decisions.append(decision)
        history_rows.append(
            {
                "artifact_id": artifact_id,
                "input_hash": input_hash,
                "evaluated_at": decision["created_at"],
                "decision": decision["decision"],
                "score": decision["score"],
                "confidence": decision["confidence"],
                "priority": decision["priority"],
                "output_paths": output_paths,
            }
        )

        if should_emit_urgent(decision, previous_decision):
            key = alert_key(decision, input_hash)
            if not state.setdefault("alerts", {}).get(key):
                state["alerts"][key] = {
                    "created_at": now_iso(),
                    "paths": write_urgent_alert(decision),
                }

    append_jsonl(DECISION_HISTORY_PATH, history_rows)
    digest_paths = write_daily_digest(latest_records, new_decisions)
    state["last_digest_date"] = datetime.now().strftime("%Y-%m-%d") if digest_paths else state.get("last_digest_date")
    save_state(state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

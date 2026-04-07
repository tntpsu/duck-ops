#!/usr/bin/env python3
"""
Normalized business-case builders for the Duck Ops customer interaction lane.

These helpers sit above raw observation artifacts such as `customer_signal`
and turn them into more operator-meaningful case records without executing any
customer-facing or manufacturing action.
"""

from __future__ import annotations

from datetime import datetime
import re
from typing import Any


PRIORITY_ORDER = {"high": 0, "medium": 1, "low": 2}
LANE_ORDER = {"customer_issue": 0, "custom_design": 1, "print_queue": 2}
ACTION_ORDER = {
    "refund_or_replacement_review": 0,
    "refund_review": 1,
    "replacement_review": 2,
    "escalate": 3,
    "reply_with_context": 4,
    "watch": 5,
    "reply_recommended": 6,
}


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (value or "").lower()).strip("-") or "unknown"


def _trim_text(value: str | None, limit: int = 240) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _normalize_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())).strip()


def _clean_customer_preview(value: str | None, limit: int = 240) -> str:
    original = str(value or "")
    name_match = re.match(r"^\s*([A-Za-z0-9 .'\-]+)\s+sent you a message\b", original, re.IGNORECASE)
    conversation_name = name_match.group(1).strip() if name_match else None
    text = original.replace("\r", "\n")
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"^[A-Za-z0-9 .'\-]+\s+sent you a message\b", "", text, flags=re.IGNORECASE)

    ignored_fragments = (
        "etsy",
        "on sale",
        "gift guide",
        "home living",
        "manage preferences",
        "unsubscribe",
        "view in browser",
        "text decoration none",
    )

    lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip(" -:\t")
        if not line:
            continue
        normalized = _normalize_text(line)
        if not normalized:
            continue
        if normalized.isdigit():
            continue
        if any(fragment in normalized for fragment in ignored_fragments):
            continue
        lines.append(line)

    cleaned = re.sub(r"\s+", " ", " ".join(lines)).strip(" -:")
    if not cleaned:
        cleaned = re.sub(r"\s+", " ", original).strip()
    normalized_cleaned = _normalize_text(cleaned)
    if "text decoration none" in normalized_cleaned or "sent you a message" in normalized_cleaned:
        if conversation_name:
            cleaned = f"Latest Etsy conversation from {conversation_name} needs review."
        else:
            cleaned = "Latest Etsy conversation needs review."
    return _trim_text(cleaned, limit)


def _extract_colors(value: str | None) -> list[str]:
    text = _normalize_text(value)
    colors = [
        "red",
        "orange",
        "yellow",
        "green",
        "blue",
        "purple",
        "pink",
        "black",
        "white",
        "gray",
        "grey",
        "brown",
        "tan",
        "gold",
        "silver",
    ]
    return [color for color in colors if f" {color} " in f" {text} "]


def _extract_customer_name(from_line: str | None) -> str | None:
    value = str(from_line or "").strip()
    if not value:
        return None
    match = re.match(r"^(.*?)\s*<", value)
    if match:
        name = match.group(1).strip().strip('"')
        return name or None
    if "@" not in value:
        return value
    return None


def _contains_any(text: str, terms: list[str]) -> bool:
    lowered = _normalize_text(text)
    return any(term in lowered for term in terms)


def _resolution_signals(case: dict[str, Any]) -> set[str]:
    return {str(signal).strip() for signal in ((case.get("resolution_enrichment") or {}).get("signals") or []) if str(signal).strip()}


def _looks_like_custom_design_request(subject: str, body_text: str, from_line: str) -> bool:
    combined = f"{subject}\n{body_text}"
    normalized = _normalize_text(combined)
    sender = _normalize_text(from_line)
    marketing_terms = [
        "unsubscribe",
        "manage preferences",
        "view in browser",
        "email preferences",
        "learn more here",
        "patreon",
        "mailgun",
        "newsletter",
        "promo",
        "promotion",
    ]
    if any(term in normalized for term in marketing_terms) or any(term in sender for term in ("mailgun", "patreon")):
        return False
    excluded_terms = [
        "you made a sale",
        "shipment",
        "delivery",
        "tracking",
        "daily etsy review summary",
        "this week in your shop",
        "fedex",
        "order #",
        "ship by",
        "travel specialist",
        "review summary",
        "your etsy order",
    ]
    if any(term in normalized for term in excluded_terms):
        return False
    if any(term in sender for term in ("fedex", "etsy transactions", "etsy sellers")):
        return False

    strong_terms = [
        "custom duck",
        "personalized duck",
        "can you make",
        "could you make",
        "can you do",
        "design a duck",
        "make a duck",
        "mascot duck",
        "team colors",
    ]
    if any(term in normalized for term in strong_terms):
        return True

    medium_terms = ["custom order", "custom design", "personalized", "personalised"]
    has_medium = any(term in normalized for term in medium_terms)
    has_request_language = any(term in normalized for term in ("can you", "could you", "i want", "i would like", "looking for"))
    return bool(has_medium and has_request_language)


def build_customer_cases(customer_signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for signal in customer_signals:
        artifact_id = str(signal.get("artifact_id") or "").strip()
        if not artifact_id:
            continue
        channel = str(signal.get("channel") or "unknown").strip()
        event = signal.get("customer_event") or {}
        context = signal.get("business_context") or {}
        customer_text = str(event.get("customer_text") or "").strip()
        rating = event.get("rating")
        issue_type = str(context.get("issue_type") or "unknown").strip()
        sentiment = str(event.get("sentiment") or "unknown").strip()
        priority = "low"
        recommended_action = "watch"
        recommended_recovery_action = "none"
        missing_context: list[str] = []

        if channel == "etsy_review":
            if rating is not None and rating <= 2:
                priority = "high"
                recommended_action = "refund_or_replacement_review"
                recommended_recovery_action = "refund_or_replacement"
            elif rating is not None and rating == 3:
                priority = "medium"
                recommended_action = "reply_with_context"
                recommended_recovery_action = "review_case"
            elif rating is not None and rating >= 4:
                priority = "low"
                recommended_action = "reply_recommended"
                recommended_recovery_action = "public_reply"
        else:
            lowered = _normalize_text(customer_text)
            if any(term in lowered for term in ("refund", "money back", "cancel")):
                priority = "high"
                recommended_action = "refund_review"
                recommended_recovery_action = "refund"
            elif any(term in lowered for term in ("replacement", "replace", "resend", "another one")):
                priority = "high"
                recommended_action = "replacement_review"
                recommended_recovery_action = "replacement"
            elif any(term in lowered for term in ("damaged", "broken", "chipped", "late", "delivery", "shipping")):
                priority = "high"
                recommended_action = "escalate"
                recommended_recovery_action = "human_review"
            else:
                priority = "medium"
                recommended_action = "reply_with_context"
                recommended_recovery_action = "context_first"

        if not customer_text:
            missing_context.append("customer_text")
            if recommended_action not in {"refund_review", "replacement_review", "refund_or_replacement_review"}:
                recommended_action = "reply_with_context"
        if not str(context.get("order_id") or "").strip() and recommended_action in {
            "refund_review",
            "replacement_review",
            "refund_or_replacement_review",
        }:
            missing_context.append("order_id")

        if missing_context == ["customer_text", "order_id"] or set(missing_context) == {"customer_text", "order_id"}:
            context_state = "missing_customer_and_order_context"
        elif "order_id" in missing_context:
            context_state = "missing_order_context"
        elif "customer_text" in missing_context:
            context_state = "missing_customer_text"
        elif channel == "mailbox_email":
            context_state = "conversation_thread_review"
        else:
            context_state = "enough_context"

        if recommended_action == "refund_or_replacement_review":
            response_recommendation = {
                "label": "apology_and_make_it_right",
                "draft_mode": "manual_only",
                "reason": "The customer described a low-rating damaged or shipping issue that likely needs a concrete recovery offer.",
            }
            recovery_recommendation = {
                "label": "refund_or_replacement",
                "requires_operator_confirmation": True,
                "reason": "Broken or damaged arrival usually merits a refund or resend review.",
            }
        elif recommended_action == "refund_review":
            response_recommendation = {
                "label": "apology_and_refund_review",
                "draft_mode": "manual_only",
                "reason": "The complaint language leans refund-first, but operator confirmation is still required.",
            }
            recovery_recommendation = {
                "label": "refund",
                "requires_operator_confirmation": True,
                "reason": "The detected complaint language leans refund-first.",
            }
        elif recommended_action == "replacement_review":
            response_recommendation = {
                "label": "apology_and_replacement_review",
                "draft_mode": "manual_only",
                "reason": "The complaint language leans resend or replacement rather than a public reply alone.",
            }
            recovery_recommendation = {
                "label": "replacement",
                "requires_operator_confirmation": True,
                "reason": "The detected complaint language leans replacement or resend.",
            }
        elif recommended_action == "escalate":
            response_recommendation = {
                "label": "manual_review_first",
                "draft_mode": "manual_only",
                "reason": "The issue appears risky or ambiguous enough that Duck Ops should not guess a customer-facing path yet.",
            }
            recovery_recommendation = {
                "label": "manual_review",
                "requires_operator_confirmation": True,
                "reason": "The case needs operator review before a refund, resend, or response path is chosen.",
            }
        elif recommended_action == "reply_with_context":
            response_recommendation = {
                "label": "clarify_and_reply",
                "draft_mode": "safe_guidance",
                "reason": "There is enough signal to stage a response path, but more context may still be needed.",
            }
            recovery_recommendation = {
                "label": "context_first",
                "requires_operator_confirmation": False,
                "reason": "Duck Ops should gather or confirm context before recommending a stronger recovery action.",
            }
        else:
            response_recommendation = {
                "label": "public_reply",
                "draft_mode": "safe_guidance",
                "reason": "This case reads like a low-risk public reply opportunity rather than a recovery case.",
            }
            recovery_recommendation = {
                "label": "none",
                "requires_operator_confirmation": False,
                "reason": "No refund or resend action is recommended from the current signal.",
            }

        rows.append(
            {
                "artifact_id": artifact_id.replace("customer::", "customer_case::", 1),
                "artifact_type": "customer_case",
                "source_signal_id": artifact_id,
                "channel": channel,
                "case_type": issue_type,
                "priority": priority,
                "recommended_action": recommended_action,
                "recommended_recovery_action": recommended_recovery_action,
                "customer_summary": _clean_customer_preview(customer_text or f"{channel} {issue_type} case"),
                "rating": rating,
                "sentiment": sentiment,
                "issue_type": issue_type,
                "context_state": context_state,
                "response_recommendation": response_recommendation,
                "recovery_recommendation": recovery_recommendation,
                "missing_context": missing_context,
                "source_refs": signal.get("source_refs") or [],
                "customer_event": event,
                "business_context": context,
                "notes": {
                    "normalization_stage": "customer_case_v1",
                    "safe_reply_autonomy": "manual_only",
                },
            }
        )
    return rows


def build_custom_design_cases(mailbox_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for email_item in mailbox_items:
        body_text = str(email_item.get("body_text") or "").strip()
        subject = str(email_item.get("subject") or "").strip()
        from_line = str(email_item.get("from") or "").strip()
        if not _looks_like_custom_design_request(subject, body_text, from_line):
            continue
        combined = f"{subject}\n{body_text}"
        uid = str(email_item.get("uid") or email_item.get("message_id") or "").strip()
        if not uid:
            continue
        normalized = _normalize_text(combined)
        requested_colors = _extract_colors(combined)
        ready_for_manual_design = len(body_text) >= 60 and bool(requested_colors or "duck" in normalized)
        open_questions: list[str] = []
        if not requested_colors:
            open_questions.append("What colors should the custom duck use?")
        if not _contains_any(combined, ["birthday", "wedding", "graduation", "gift", "team", "mascot", "memorial"]):
            open_questions.append("What is the occasion or theme for this custom duck?")
        if not _contains_any(combined, ["need by", "deadline", "by ", "before "]):
            open_questions.append("Is there a target date or deadline?")

        rows.append(
            {
                "artifact_id": f"custom_design::mail::{uid}",
                "artifact_type": "custom_design_case",
                "channel": "mailbox_email",
                "source_refs": [
                    {
                        "path": email_item.get("registry_key"),
                        "folder": email_item.get("folder"),
                        "uid": email_item.get("uid"),
                        "message_id": email_item.get("message_id"),
                    }
                ],
                "customer_name": _extract_customer_name(email_item.get("from")),
                "request_summary": _trim_text(subject or body_text, 180),
                "normalized_brief": {
                    "theme_or_character": _trim_text(body_text, 200),
                    "requested_colors": requested_colors,
                    "requested_deadline": None,
                    "recipient_or_occasion": None,
                    "design_constraints": [],
                },
                "open_questions": open_questions,
                "ready_for_manual_design": ready_for_manual_design and not open_questions,
                "google_task_status": "not_created",
                "notes": {
                    "normalization_stage": "custom_design_case_v1",
                    "source_mode": "mailbox_email",
                },
            }
        )
    return rows


def build_print_queue_candidates(
    weekly_insights: dict[str, Any],
    products: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    title_to_product: dict[str, dict[str, Any]] = {}
    for pid, product in products.items():
        title = str(product.get("title") or "").strip()
        if title:
            title_to_product[_normalize_text(title)] = {"product_id": pid, **product}
    for item in weekly_insights.get("inventory_alerts", []) or []:
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        recent_demand = int(item.get("recent_demand") or 0)
        lifetime_demand = int(item.get("lifetime_demand") or 0)
        priority = "high" if recent_demand >= 25 else "medium" if recent_demand >= 10 else "low"
        matched = title_to_product.get(_normalize_text(title)) or {}
        rows.append(
            {
                "artifact_id": f"print_queue::{_slugify(title)}::{datetime.now().strftime('%Y-%m-%d')}",
                "artifact_type": "print_queue_candidate",
                "product_id": matched.get("product_id"),
                "product_title": title,
                "channel_scope": ["shopify", "etsy"],
                "priority": priority,
                "recommended_next_action": "check_inventory_and_queue_print",
                "why_now": item.get("alert") or "Weekly ops flagged this as a high-demand product that may need replenishment.",
                "recent_demand": recent_demand,
                "lifetime_demand": lifetime_demand,
                "inventory_signal": "demand_alert_only",
                "confidence": 0.68 if recent_demand >= 10 else 0.52,
                "source_refs": [
                    {
                        "path": "/Users/philtullai/ai-agents/duckAgent/cache/weekly_insights.json",
                        "source_type": "weekly_insights.inventory_alerts",
                    }
                ],
                "notes": {
                    "normalization_stage": "print_queue_candidate_v1",
                    "stock_evidence": "not_yet_available",
                },
            }
        )
    return rows


def _customer_case_is_actionable(case: dict[str, Any]) -> bool:
    resolution_signals = _resolution_signals(case)
    recommended_action = str(case.get("recommended_action") or "").strip()
    approved_recovery_action = str(case.get("approved_recovery_action") or "").strip()
    if approved_recovery_action in {"replacement", "refund", "wait"}:
        return True
    if "public_review_reply_posted" in resolution_signals and recommended_action in {"reply_recommended", "reply_with_context"}:
        return False
    if "refund_detected" in resolution_signals and recommended_action in {
        "refund_review",
        "replacement_review",
        "refund_or_replacement_review",
    }:
        return False
    if str(case.get("priority") or "").strip() in {"high", "medium"}:
        return True
    return recommended_action not in {"watch", "reply_recommended"}


def _customer_case_title(case: dict[str, Any]) -> str:
    channel = str(case.get("channel") or "customer").replace("_", " ").title()
    issue_type = str(case.get("issue_type") or "issue").replace("_", " ")
    rating = case.get("rating")
    if case.get("channel") == "etsy_review" and rating is not None:
        return f"Etsy review ({rating} star) - {issue_type}"
    return f"{channel} - {issue_type}"


def _customer_case_approval_meaning(case: dict[str, Any]) -> str:
    action = str(case.get("recommended_action") or "").strip()
    if action == "refund_or_replacement_review":
        return "Approval means Duck Ops should stage a refund-or-replacement recovery recommendation for operator action. No customer action is sent automatically."
    if action == "refund_review":
        return "Approval means Duck Ops should stage a refund recommendation for operator review. No refund is issued automatically."
    if action == "replacement_review":
        return "Approval means Duck Ops should stage a resend or replacement recommendation for operator review. No replacement is sent automatically."
    if action == "escalate":
        return "Approval means Duck Ops should keep this as a high-risk manual case for operator follow-up. No customer reply is sent automatically."
    return "Approval means Duck Ops should stage a guided customer-response path. No customer reply is sent automatically."


def _extract_etsy_conversation_name(summary: str | None) -> str | None:
    match = re.search(r"^\s*([A-Za-z0-9 .'\-]+)\s+sent you a message\b", str(summary or ""), re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def _source_uid(case: dict[str, Any]) -> int:
    for ref in case.get("source_refs") or []:
        uid = ref.get("uid")
        try:
            return int(uid)
        except (TypeError, ValueError):
            continue
    return 0


def _representative_action(cases: list[dict[str, Any]]) -> str:
    actions = [str(case.get("recommended_action") or "").strip() for case in cases]
    actions = [action for action in actions if action]
    if not actions:
        return "reply_with_context"
    return sorted(actions, key=lambda action: ACTION_ORDER.get(action, 99))[0]


def _collapse_customer_case_group(cases: list[dict[str, Any]]) -> dict[str, Any]:
    cases = sorted(cases, key=_source_uid, reverse=True)
    representative = cases[0]
    name = _extract_etsy_conversation_name(representative.get("customer_summary")) or "Customer"
    priority = sorted(
        {str(case.get("priority") or "medium").strip() for case in cases},
        key=lambda level: PRIORITY_ORDER.get(level, 99),
    )[0]
    recommended_action = _representative_action(cases)
    latest_message_preview = _clean_customer_preview(representative.get("customer_summary") or "")
    summary = latest_message_preview or f"{len(cases)} Etsy conversation message(s) from {name} need review."
    if summary:
        summary = f"{summary} ({len(cases)} message{'s' if len(cases) != 1 else ''} in thread)"
    return {
        "queue_item_id": f"ops_queue::conversation::{_slugify(name)}",
        "source_artifact_id": representative["artifact_id"],
        "source_artifact_ids": [case["artifact_id"] for case in cases],
        "item_type": "customer_case",
        "lane": "customer_issue",
        "priority": priority,
        "title": f"Etsy conversation - {name}",
        "summary": summary,
        "recommended_action": recommended_action,
        "approval_meaning": _customer_case_approval_meaning({"recommended_action": recommended_action}),
        "source_refs": representative.get("source_refs") or [],
        "details": {
            "channel": "mailbox_email",
            "conversation_contact": name,
            "grouped_message_count": len(cases),
            "latest_uid": _source_uid(representative),
            "latest_message_preview": latest_message_preview,
            "context_state": representative.get("context_state"),
            "response_recommendation": representative.get("response_recommendation"),
            "recovery_recommendation": representative.get("recovery_recommendation"),
            "order_enrichment": representative.get("order_enrichment"),
            "tracking_enrichment": representative.get("tracking_enrichment"),
            "resolution_enrichment": representative.get("resolution_enrichment"),
            "operator_decision": representative.get("operator_decision"),
            "approved_recovery_action": representative.get("approved_recovery_action"),
            "missing_context": representative.get("missing_context") or [],
            "recommended_recovery_action": representative.get("recommended_recovery_action"),
        },
    }


def _custom_design_title(case: dict[str, Any]) -> str:
    customer_name = str(case.get("customer_name") or "").strip()
    if customer_name:
        return f"Custom design request - {customer_name}"
    return "Custom design request"


def _custom_design_approval_meaning(case: dict[str, Any]) -> str:
    if case.get("ready_for_manual_design"):
        return "Approval means Duck Ops should treat this as ready for manual design work and staged tasking. No Google Task or customer reply is sent automatically yet."
    return "Approval means Duck Ops should keep this as a structured design brief with follow-up questions. No customer reply is sent automatically."


def _print_queue_title(case: dict[str, Any]) -> str:
    return f"Print queue candidate - {case.get('product_title') or 'Unknown product'}"


def _print_queue_approval_meaning(_: dict[str, Any]) -> str:
    return "Approval means Duck Ops should stage this duck for print-queue review. No printer command is sent automatically."


def build_customer_interaction_queue(
    customer_cases: list[dict[str, Any]],
    custom_design_cases: list[dict[str, Any]],
    print_queue_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    grouped_customer_cases: dict[str, list[dict[str, Any]]] = {}
    hidden_counts = {
        "customer_cases_hidden": 0,
        "custom_design_cases_hidden": 0,
        "print_queue_candidates_hidden": 0,
    }

    for case in customer_cases:
        if not _customer_case_is_actionable(case):
            hidden_counts["customer_cases_hidden"] += 1
            continue
        conversation_name = None
        if case.get("channel") == "mailbox_email":
            conversation_name = _extract_etsy_conversation_name(case.get("customer_summary"))
        if conversation_name:
            grouped_customer_cases.setdefault(conversation_name.lower(), []).append(case)
            continue
        items.append(
            {
                "queue_item_id": f"ops_queue::{case['artifact_id']}",
                "source_artifact_id": case["artifact_id"],
                "item_type": "customer_case",
                "lane": "customer_issue",
                "priority": case.get("priority") or "medium",
                "title": _customer_case_title(case),
                "summary": case.get("customer_summary") or "",
                "recommended_action": case.get("recommended_action"),
                "approval_meaning": _customer_case_approval_meaning(case),
                "source_refs": case.get("source_refs") or [],
                "details": {
                    "channel": case.get("channel"),
                    "rating": case.get("rating"),
                    "issue_type": case.get("issue_type"),
                    "context_state": case.get("context_state"),
                    "response_recommendation": case.get("response_recommendation"),
                    "recovery_recommendation": case.get("recovery_recommendation"),
                    "order_enrichment": case.get("order_enrichment"),
                    "tracking_enrichment": case.get("tracking_enrichment"),
                    "resolution_enrichment": case.get("resolution_enrichment"),
                    "operator_decision": case.get("operator_decision"),
                    "approved_recovery_action": case.get("approved_recovery_action"),
                    "missing_context": case.get("missing_context") or [],
                    "recommended_recovery_action": case.get("recommended_recovery_action"),
                },
            }
        )

    for _, grouped_cases in sorted(grouped_customer_cases.items(), key=lambda entry: entry[0]):
        items.append(_collapse_customer_case_group(grouped_cases))

    for case in custom_design_cases:
        items.append(
            {
                "queue_item_id": f"ops_queue::{case['artifact_id']}",
                "source_artifact_id": case["artifact_id"],
                "item_type": "custom_design_case",
                "lane": "custom_design",
                "priority": "medium" if case.get("ready_for_manual_design") else "low",
                "title": _custom_design_title(case),
                "summary": case.get("request_summary") or "",
                "recommended_action": "manual_design_task" if case.get("ready_for_manual_design") else "clarify_brief",
                "approval_meaning": _custom_design_approval_meaning(case),
                "source_refs": case.get("source_refs") or [],
                "details": {
                    "customer_name": case.get("customer_name"),
                    "open_questions": case.get("open_questions") or [],
                    "ready_for_manual_design": bool(case.get("ready_for_manual_design")),
                },
            }
        )

    for case in print_queue_candidates:
        items.append(
            {
                "queue_item_id": f"ops_queue::{case['artifact_id']}",
                "source_artifact_id": case["artifact_id"],
                "item_type": "print_queue_candidate",
                "lane": "print_queue",
                "priority": case.get("priority") or "medium",
                "title": _print_queue_title(case),
                "summary": case.get("why_now") or "",
                "recommended_action": case.get("recommended_next_action"),
                "approval_meaning": _print_queue_approval_meaning(case),
                "source_refs": case.get("source_refs") or [],
                "details": {
                    "product_id": case.get("product_id"),
                    "recent_demand": case.get("recent_demand"),
                    "lifetime_demand": case.get("lifetime_demand"),
                    "inventory_signal": case.get("inventory_signal"),
                    "confidence": case.get("confidence"),
                },
            }
        )

    items.sort(
        key=lambda item: (
            PRIORITY_ORDER.get(str(item.get("priority") or "").strip(), 99),
            LANE_ORDER.get(str(item.get("lane") or "").strip(), 99),
            str(item.get("title") or "").lower(),
        )
    )

    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "counts": {
            "customer_cases_total": len(customer_cases),
            "custom_design_cases_total": len(custom_design_cases),
            "print_queue_candidates_total": len(print_queue_candidates),
            "operator_queue_items": len(items),
            **hidden_counts,
        },
        "items": items,
    }


def render_customer_interaction_queue_markdown(queue_payload: dict[str, Any]) -> str:
    lines = [
        "# Duck Ops Customer Interaction Queue",
        "",
        f"- Generated at: `{queue_payload.get('generated_at')}`",
        f"- Operator-facing items: `{queue_payload.get('counts', {}).get('operator_queue_items', 0)}`",
        f"- Hidden low-signal customer cases: `{queue_payload.get('counts', {}).get('customer_cases_hidden', 0)}`",
        f"- Custom design cases: `{queue_payload.get('counts', {}).get('custom_design_cases_total', 0)}`",
        f"- Print queue candidates: `{queue_payload.get('counts', {}).get('print_queue_candidates_total', 0)}`",
        "",
    ]
    items = queue_payload.get("items") or []
    if not items:
        lines.append("No operator-facing customer interaction items right now.")
        lines.append("")
        return "\n".join(lines)

    for index, item in enumerate(items, start=1):
        lines.extend(
            [
                f"## {index}. [{str(item.get('priority') or '').upper()}] {item.get('title')}",
                "",
                f"- Lane: `{item.get('lane')}`",
                f"- Recommended action: `{item.get('recommended_action')}`",
                f"- Approval meaning: {item.get('approval_meaning')}",
                f"- Summary: {item.get('summary') or '(none)' }",
            ]
        )
        details = item.get("details") or {}
        if details:
            if details.get("context_state"):
                lines.append(f"- Context state: `{details.get('context_state')}`")
            response_recommendation = details.get("response_recommendation") or {}
            recovery_recommendation = details.get("recovery_recommendation") or {}
            if response_recommendation.get("label"):
                lines.append(
                    f"- Response recommendation: `{response_recommendation.get('label')}`"
                    f" - {response_recommendation.get('reason') or ''}"
                )
            if recovery_recommendation.get("label"):
                lines.append(
                    f"- Recovery recommendation: `{recovery_recommendation.get('label')}`"
                    f" - {recovery_recommendation.get('reason') or ''}"
                )
            order_enrichment = details.get("order_enrichment") or {}
            tracking_enrichment = details.get("tracking_enrichment") or {}
            resolution_enrichment = details.get("resolution_enrichment") or {}
            operator_decision = details.get("operator_decision") or {}
            if order_enrichment.get("matched"):
                lines.append(
                    f"- Order enrichment: receipt `{order_enrichment.get('receipt_id') or 'n/a'}`"
                    f", transaction `{order_enrichment.get('transaction_id') or 'n/a'}`"
                    f", product `{order_enrichment.get('product_title') or 'n/a'}`"
                )
            if tracking_enrichment.get("status"):
                tracking_summary = f"`{tracking_enrichment.get('status')}`"
                if tracking_enrichment.get("tracking_number"):
                    tracking_summary += f" ({tracking_enrichment.get('tracking_number')})"
                lines.append(f"- Tracking enrichment: {tracking_summary}")
            if resolution_enrichment.get("signals"):
                lines.append(
                    f"- Resolution history: `{resolution_enrichment.get('status')}`"
                    f" - {resolution_enrichment.get('summary') or ''}"
                )
            if operator_decision.get("resolution"):
                lines.append(
                    f"- Operator decision: `{operator_decision.get('resolution')}`"
                    f" at `{operator_decision.get('recorded_at') or 'unknown'}`"
                    f" - {operator_decision.get('note') or 'No note provided.'}"
                )
            extra_details = {key: value for key, value in details.items() if key not in {
                "context_state",
                "response_recommendation",
                "recovery_recommendation",
                "order_enrichment",
                "tracking_enrichment",
                "resolution_enrichment",
                "operator_decision",
                "approved_recovery_action",
            }}
            if extra_details:
                lines.append(f"- Details: `{extra_details}`")
        lines.append("")
    return "\n".join(lines)

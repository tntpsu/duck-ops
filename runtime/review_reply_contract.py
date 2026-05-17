from __future__ import annotations

import re
from typing import Any


POSITIVE_REVIEW_TERMS = (
    "love",
    "loved",
    "great",
    "cute",
    "adorable",
    "perfect",
    "quality",
    "recommend",
    "happy",
    "awesome",
    "excellent",
)

GENERIC_POSITIVE_REPLY_PHRASES = (
    "kind review",
    "kind words",
    "glad you loved",
    "thrilled to hear you loved",
    "so glad you loved",
    "happy you loved",
    "thanks again for the review",
    "thanks so much for the review",
)

ISSUE_PATTERNS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    (
        "material_expectation",
        "expected material or feel did not match the customer expectation",
        (
            "thought they were plastic",
            "thought it was plastic",
            "expected plastic",
            "would have been better",
            "not what i expected",
            "not as expected",
            "material was not",
            "material wasn't",
            "material felt",
            "not plastic",
            "was not plastic",
            "wasn't plastic",
            "3d printed instead",
            "3d printed not",
            "micro plastic",
        ),
    ),
    (
        "size_expectation",
        "size did not match the customer expectation",
        (
            "smaller than expected",
            "larger than expected",
            "too small",
            "too big",
            "tiny",
        ),
    ),
    (
        "quality_concern",
        "customer raised a quality or disappointment concern",
        (
            "cheap",
            "disappointed",
            "misleading",
            "not disclosed",
            "poor quality",
            "bad quality",
        ),
    ),
    (
        "damage_or_shipping",
        "customer raised damage, delivery, or shipping concern",
        (
            "broken",
            "damaged",
            "arrived late",
            "late shipping",
            "shipping was late",
            "shipping issue",
            "shipping problem",
            "never arrived",
        ),
    ),
)


def _normalize(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _lower(value: str | None) -> str:
    return _normalize(value).lower()


def _matched_issue_terms(review_text: str) -> list[dict[str, str]]:
    lowered = _lower(review_text)
    matches: list[dict[str, str]] = []
    for issue_type, description, terms in ISSUE_PATTERNS:
        for term in terms:
            if term in lowered:
                matches.append(
                    {
                        "issue_type": issue_type,
                        "term": term,
                        "description": description,
                    }
                )
    return matches


def _primary_issue(matches: list[dict[str, str]]) -> tuple[str, str]:
    if not matches:
        return "none", ""
    first = matches[0]
    return first["issue_type"], first["description"]


def _has_positive_signal(review_text: str) -> bool:
    lowered = _lower(review_text)
    return any(term in lowered for term in POSITIVE_REVIEW_TERMS)


def _generic_positive_reply_detected(response: str) -> list[str]:
    lowered = _lower(response)
    return [phrase for phrase in GENERIC_POSITIVE_REPLY_PHRASES if phrase in lowered]


def _acknowledges_issue(response: str, issue_type: str) -> bool:
    lowered = _lower(response)
    if issue_type == "none":
        return True
    acknowledgement_terms = (
        "expected",
        "expectation",
        "material",
        "feel",
        "missed the mark",
        "understand",
        "honest feedback",
        "feedback",
        "disappointed",
        "sorry",
    )
    return any(term in lowered for term in acknowledgement_terms)


def build_review_reply_contract(review_text: str, response: str, *, private_mode: bool = False) -> dict[str, Any]:
    """Classify a review reply and return policy-readable checks.

    This is intentionally deterministic. AI can draft or repair the prose, but this
    contract is the stable guardrail that decides whether the reply matches the
    source review closely enough for operator review.
    """

    normalized_review = _normalize(review_text)
    normalized_response = _normalize(response)
    issue_matches = _matched_issue_terms(normalized_review)
    issue_type, issue_description = _primary_issue(issue_matches)
    has_issue = issue_type != "none"
    positive_signal = _has_positive_signal(normalized_review)
    generic_positive_phrases = _generic_positive_reply_detected(normalized_response)
    acknowledges_issue = _acknowledges_issue(normalized_response, issue_type)

    if private_mode:
        sentiment = "private_issue" if has_issue else "private_followup"
    elif has_issue:
        sentiment = "mixed_positive" if positive_signal else "complaint_adjacent"
    else:
        sentiment = "positive"

    checks: list[dict[str, str]] = []
    if not normalized_response:
        checks.append(
            {
                "id": "reply_text_present",
                "status": "fail",
                "evidence": "No draft reply text was preserved.",
            }
        )
    else:
        checks.append(
            {
                "id": "reply_text_present",
                "status": "pass",
                "evidence": "Draft reply text is present.",
            }
        )

    if has_issue and not private_mode and generic_positive_phrases:
        checks.append(
            {
                "id": "no_generic_positive_reply_for_mixed_review",
                "status": "fail",
                "evidence": f"Review contains {issue_description}; reply uses generic positive phrase(s): {', '.join(generic_positive_phrases)}.",
            }
        )
    else:
        checks.append(
            {
                "id": "no_generic_positive_reply_for_mixed_review",
                "status": "pass",
                "evidence": "Reply does not use generic happy-review language against a mixed or complaint-adjacent review.",
            }
        )

    if has_issue and not acknowledges_issue:
        checks.append(
            {
                "id": "acknowledge_review_issue",
                "status": "fail",
                "evidence": f"Review contains {issue_description}; reply does not clearly acknowledge that issue.",
            }
        )
    else:
        checks.append(
            {
                "id": "acknowledge_review_issue",
                "status": "pass",
                "evidence": "Reply acknowledges the review issue or no issue was detected.",
            }
        )

    failed_checks = [check for check in checks if check["status"] == "fail"]
    warning_checks = [check for check in checks if check["status"] == "warn"]

    return {
        "classification": {
            "sentiment": sentiment,
            "issue_type": issue_type,
            "issue_description": issue_description,
            "public_safe": not private_mode,
            "positive_signal": positive_signal,
            "needs_rewrite": bool(failed_checks or warning_checks),
        },
        "source_evidence": {
            "issue_terms": issue_matches,
            "generic_positive_reply_phrases": generic_positive_phrases,
        },
        "policy_checks": checks,
        "failed_checks": failed_checks,
        "warning_checks": warning_checks,
    }


def contract_failure_messages(contract: dict[str, Any]) -> list[str]:
    messages: list[str] = []
    for check in contract.get("failed_checks") or []:
        evidence = str((check or {}).get("evidence") or "").strip()
        if evidence:
            messages.append(evidence)
    return messages


def contract_improvement_suggestions(contract: dict[str, Any]) -> list[str]:
    classification = contract.get("classification") or {}
    issue_type = str(classification.get("issue_type") or "none")
    suggestions: list[str] = []
    if classification.get("needs_rewrite"):
        if issue_type in {"material_expectation", "size_expectation", "quality_concern"}:
            suggestions.append("Rewrite the reply to acknowledge the customer's expectation mismatch instead of treating it like a generic happy review.")
        else:
            suggestions.append("Rewrite the reply so it directly addresses the concern detected in the review text.")
    if issue_type == "material_expectation":
        suggestions.append("Use grounded wording such as 'I understand the material was not what you expected' and avoid implying the customer was simply delighted.")
    return suggestions


def public_issue_reply_lines(review_text: str, *, shorter: bool = False, warmer: bool = False) -> list[str]:
    contract = build_review_reply_contract(review_text, "", private_mode=False)
    issue_type = str((contract.get("classification") or {}).get("issue_type") or "none")

    opening = "Thanks for the honest feedback."
    if warmer:
        opening = "Thanks for taking the time to share honest feedback."
    if shorter:
        opening = "Thanks for the honest feedback."

    if issue_type == "material_expectation":
        issue_line = "I understand the material was not what you expected, and I appreciate you giving it a try."
    elif issue_type == "size_expectation":
        issue_line = "I understand the size was not what you expected, and I appreciate you sharing that."
    elif issue_type == "quality_concern":
        issue_line = "I understand why that felt disappointing, and I appreciate you sharing the feedback."
    elif issue_type == "damage_or_shipping":
        issue_line = "I understand that experience was frustrating, and I appreciate you taking the time to share it."
    else:
        issue_line = "I appreciate you sharing what could have been better."

    if shorter:
        return [opening, issue_line]
    return [opening, issue_line, "That helps me improve how I set expectations for future orders."]

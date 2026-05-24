"""
Sanity gate for LLM-generated review reply rewrites.

The LLM rewrite path can fail in several ways: empty output, off-topic
hallucination, prompt-injection compliance, template placeholders, etc.
Each check below is conservative — the goal is to catch obvious failures
fast and fall back to the rule-based path. The operator still has final
approval; sanity gates are a safety net, not a quality guarantee.

See REVIEW_REPLY_REWRITE_LLM_PLAN.md for the broader plan.
"""
from __future__ import annotations

import re
from typing import Any


MIN_LENGTH = 30
MAX_LENGTH = 600

PLACEHOLDER_PATTERNS = (
    re.compile(r"\{[^}]+\}"),     # {customer_name}, {placeholder}
    re.compile(r"\[[A-Z_ ]+\]"),  # [NAME], [PRODUCT], [INSERT REASON]
)

URL_PATTERN = re.compile(r"https?://\S+", re.IGNORECASE)
EMAIL_PATTERN = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)

# Emoji and common pictographs (extended unicode blocks). Conservative:
# strip a few specific ranges that account for almost all emoji.
EMOJI_PATTERN = re.compile(
    "["
    "\U0001F300-\U0001F6FF"  # symbols & pictographs / transport
    "\U0001F700-\U0001F77F"
    "\U0001F780-\U0001F7FF"
    "\U0001F800-\U0001F8FF"
    "\U0001F900-\U0001F9FF"  # supplemental symbols & pictographs
    "\U0001FA00-\U0001FA6F"
    "\U0001FA70-\U0001FAFF"
    "\U00002600-\U000027BF"  # misc symbols / dingbats
    "]"
)

REFUSAL_PATTERNS = (
    re.compile(r"^i\s+(?:can(?:no|')t|won['’]t|am\s+unable)", re.IGNORECASE),
    re.compile(r"^(?:i'?m\s+sorry|sorry,?\s+i)\s+", re.IGNORECASE),
    re.compile(r"as\s+an\s+ai", re.IGNORECASE),
    re.compile(r"i\s+can(?:not|'t)\s+(?:help|assist|comply)", re.IGNORECASE),
)


_STOPWORDS = frozenset({
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "from", "had",
    "has", "have", "he", "her", "him", "his", "i", "if", "in", "is", "it", "its",
    "me", "my", "no", "not", "of", "on", "or", "she", "so", "that", "the", "their",
    "them", "they", "this", "to", "too", "us", "was", "we", "were", "what", "when",
    "where", "which", "who", "why", "will", "with", "would", "you", "your", "yours",
    # Common reply-template words that don't count as content echo. The
    # rewriter often produces "Thanks again", "means a lot", "really
    # appreciate" — those tokens coincidentally appear in many reviews ("once
    # again, amazing job") but the rewrite isn't actually echoing review
    # content. Block them so echo_check requires a genuine content match.
    "thank", "thanks", "really", "very", "much", "just", "kind", "again",
    "lot", "means", "mean", "appreciate", "appreciated", "review", "great",
    "good", "best", "well", "exactly", "hoped", "hope", "hoping",
    "review!", "thank!", "thanks!", "review.", "thanks.",
})


def _tokenize(value: str) -> list[str]:
    return re.findall(r"[A-Za-z']+", (value or "").lower())


def _non_stopword_tokens(value: str) -> set[str]:
    return {tok for tok in _tokenize(value) if tok not in _STOPWORDS and len(tok) > 2}


def echo_check(rewrite: str, review_text: str, *, min_overlap_tokens: int = 1) -> bool:
    """At least one non-stopword token from the review must appear in the rewrite.

    Stops generic outputs like "Thanks again for the kind review" from passing
    when the review actually said something like "ducks for my nephew with his
    facial features".
    """
    review_tokens = _non_stopword_tokens(review_text)
    rewrite_tokens = _non_stopword_tokens(rewrite)
    if not review_tokens:
        return True
    return len(review_tokens & rewrite_tokens) >= min_overlap_tokens


def length_in_band(rewrite: str) -> bool:
    n = len((rewrite or "").strip())
    return MIN_LENGTH <= n <= MAX_LENGTH


def no_placeholders(rewrite: str) -> bool:
    text = rewrite or ""
    return not any(pat.search(text) for pat in PLACEHOLDER_PATTERNS)


def no_external_references(rewrite: str) -> bool:
    text = rewrite or ""
    return URL_PATTERN.search(text) is None and EMAIL_PATTERN.search(text) is None


def no_emoji(rewrite: str) -> bool:
    return EMOJI_PATTERN.search(rewrite or "") is None


def not_a_refusal(rewrite: str) -> bool:
    text = (rewrite or "").strip()
    return not any(pat.search(text) for pat in REFUSAL_PATTERNS)


SANITY_CHECKS = (
    ("length_in_band", length_in_band),
    ("no_placeholders", no_placeholders),
    ("no_external_references", no_external_references),
    ("no_emoji", no_emoji),
    ("not_a_refusal", not_a_refusal),
)


def evaluate_sanity(rewrite: str, *, review_text: str) -> dict[str, Any]:
    """Run every sanity check and return a structured report.

    Returns {'passed': bool, 'failures': list[str], 'checked': list[str]}.
    Callers fall back to the rule-based rewriter when passed=False.
    """
    failures: list[str] = []
    checked: list[str] = []
    for name, check in SANITY_CHECKS:
        checked.append(name)
        try:
            if not check(rewrite):
                failures.append(name)
        except Exception as exc:
            failures.append(f"{name}_raised:{type(exc).__name__}")
    checked.append("echo_check")
    try:
        if not echo_check(rewrite, review_text):
            failures.append("echo_check")
    except Exception as exc:
        failures.append(f"echo_check_raised:{type(exc).__name__}")
    return {
        "passed": not failures,
        "failures": failures,
        "checked": checked,
    }

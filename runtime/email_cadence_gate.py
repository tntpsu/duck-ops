"""
Shared email cadence gate for the daily-email → portal-page series.

Five operator-facing surfaces (profit, recommendations, reviews,
learnings, competitors) used to send a daily email each. After
shipping their portal pages, the daily cadence is the wrong default
— the portal is always current, and a daily digest is noise.

This module collapses the policy into one place. Each surface
declares a CadencePolicy with:

    cadence       — "daily" / "weekly_monday" / "manual"
    bypass_keys   — payload keys whose truthy value triggers an
                    immediate send regardless of cadence (e.g.
                    anomaly.triggered, low_rating_count). Dotted-path
                    keys are resolved against nested dicts.

``should_send_email(surface_name, payload, now=...)`` returns a
``CadenceDecision`` describing whether to send and why. Each email
sender wraps its ``send_email`` call:

    decision = should_send_email("reviews", payload)
    log_cadence_decision(decision)
    if not decision.should_send:
        return
    send_email(...)

Receipts get appended to ``state/email_cadence_decisions.jsonl`` so
"why didn't today's email fire?" is grep-able.

Adding a new surface = one new POLICY entry. ``require_policy`` raises
``UnknownSurfaceError`` for an unregistered name so a missing entry
becomes a noisy crash, not a silent default.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DECISION_LOG_PATH = DUCK_OPS_ROOT / "state" / "email_cadence_decisions.jsonl"

# ISO weekday: Monday = 0
_WEEKDAY_TO_INT: dict[str, int] = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


class UnknownSurfaceError(KeyError):
    """Raised when code references a surface not in POLICIES.

    Same fail-loud guarantee as duck_flows.require_flow. A typo or a
    missing registry entry surfaces as a noisy crash at the call site,
    not a silent "default to weekly" that drops emails the operator
    needed."""


@dataclass(frozen=True)
class CadencePolicy:
    """Declarative cadence rules for one operator-facing surface.

    Adding a new surface = one CadencePolicy entry in POLICIES.
    Changing how an existing surface defers = edit that entry. The
    five email-send call sites stay flow-agnostic — they just consult
    should_send_email."""

    surface_name: str
    cadence: Literal["daily", "weekly_monday", "manual"]
    # Payload paths to probe for "send today regardless of cadence".
    # Dotted notation traverses nested dicts: "anomaly.triggered" reads
    # payload["anomaly"]["triggered"]. Any truthy value triggers bypass.
    bypass_keys: tuple[str, ...] = ()
    # Human-readable note for the receipt log when the cadence
    # suppresses a send. Defaults to a generic string.
    deferred_note: str = ""


@dataclass(frozen=True)
class CadenceDecision:
    """Output of should_send_email.

    Consumers read ``should_send`` to gate the send. The other fields
    are for the receipt log and operator-facing surfaces (Desk +
    portal pages) that want to show "next email expected Monday"."""

    surface_name: str
    should_send: bool
    reason: str
    cadence: str
    next_send_iso: str | None
    bypass_active: bool = False
    bypass_keys_matched: tuple[str, ...] = ()


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _resolve_dotted_path(payload: Any, dotted: str) -> Any:
    """Walk a dotted path through nested dicts and return the value at
    the leaf, or None when any segment is missing. Used by the bypass
    check to read deeply-nested signals like ``anomaly.triggered``."""
    if payload is None:
        return None
    current: Any = payload
    for segment in dotted.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(segment)
        if current is None:
            return None
    return current


def _next_weekly_monday(now: datetime) -> str:
    """Return the ISO date of the next Monday strictly after now."""
    today = now.date()
    days_ahead = (_WEEKDAY_TO_INT["monday"] - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return (today + timedelta(days=days_ahead)).isoformat()


# ── The policy registry ─────────────────────────────────────────────

# Default for every operator-facing intel email after the daily-to-
# portal migration: weekly Monday rollup, with surface-specific
# bypass conditions for the cases an operator actually needs to act
# on the same day (anomalies, low reviews, attention items).
POLICIES: dict[str, CadencePolicy] = {
    "profit": CadencePolicy(
        surface_name="profit",
        cadence="weekly_monday",
        bypass_keys=("anomaly.triggered",),
        deferred_note="Profit data refreshed; weekly rollup scheduled for Monday. "
                      "Anomalies still trigger a same-day send.",
    ),
    "recommendations": CadencePolicy(
        surface_name="recommendations",
        cadence="weekly_monday",
        bypass_keys=(
            "summary.watchout_count",
            "change_notifier.attention_change_count",
        ),
        deferred_note="Strategy packet refreshed; weekly rollup scheduled for Monday. "
                      "Watchouts or attention changes trigger a same-day send.",
    ),
    "reviews": CadencePolicy(
        surface_name="reviews",
        cadence="weekly_monday",
        bypass_keys=("low_rating_count",),
        deferred_note="Daily reviews snapshot saved to the portal; weekly rollup "
                      "scheduled for Monday. ≤2★ reviews trigger a same-day send.",
    ),
    "learnings": CadencePolicy(
        surface_name="learnings",
        cadence="weekly_monday",
        bypass_keys=("change_notifier.attention_change_count",),
        deferred_note="Current learnings refreshed; weekly rollup scheduled for "
                      "Monday. Attention-level changes trigger a same-day send.",
    ),
    "competitors": CadencePolicy(
        surface_name="competitors",
        cadence="weekly_monday",
        bypass_keys=("ducks_to_build_count",),
        deferred_note="Daily competitor snapshot saved to the portal; weekly "
                      "rollup scheduled for Monday. New build candidates trigger "
                      "a same-day send.",
    ),
    "business_intelligence": CadencePolicy(
        surface_name="business_intelligence",
        cadence="weekly_monday",
        bypass_keys=("action_items_count",),
        deferred_note="Weekly insights refreshed in the portal; email "
                      "scheduled for Monday. Action items trigger a same-day send.",
    ),
    "engineering_governance": CadencePolicy(
        surface_name="engineering_governance",
        cadence="weekly_monday",
        bypass_keys=("high_severity_finding_count",),
        deferred_note="Engineering governance digest refreshed; weekly rollup "
                      "scheduled for Monday. High-severity findings trigger a "
                      "same-day send.",
    ),
    "shopify_seo": CadencePolicy(
        surface_name="shopify_seo",
        cadence="weekly_monday",
        bypass_keys=("high_severity_issue_count",),
        deferred_note="Shopify SEO review refreshed in the portal; email "
                      "scheduled for Monday. High-severity issues trigger a "
                      "same-day send.",
    ),
    # The single Monday rollup that the folded surfaces compose into. Not in
    # DIGEST_FOLDED_SURFACES, so it sends on Monday regardless of digest mode.
    "business_digest": CadencePolicy(
        surface_name="business_digest",
        cadence="weekly_monday",
        bypass_keys=(),
        deferred_note="Monday business digest scheduled for Monday rollup day.",
    ),
}

# Surfaces that fold into the Monday business_digest when DUCK_EMAIL_DIGEST_MODE=1.
DIGEST_FOLDED_SURFACES: frozenset[str] = frozenset({
    "profit", "recommendations", "reviews", "learnings", "competitors",
    "business_intelligence", "engineering_governance", "shopify_seo",
})


def known_surfaces() -> tuple[str, ...]:
    return tuple(sorted(POLICIES.keys()))


def get_policy(surface_name: str) -> CadencePolicy | None:
    return POLICIES.get(str(surface_name or "").strip().lower())


def require_policy(surface_name: str) -> CadencePolicy:
    policy = get_policy(surface_name)
    if policy is None:
        raise UnknownSurfaceError(
            f"Unknown surface {surface_name!r}. Add a CadencePolicy entry in "
            "email_cadence_gate.POLICIES."
        )
    return policy


def _bypass_check(policy: CadencePolicy, payload: dict[str, Any]) -> tuple[bool, tuple[str, ...]]:
    """Return (bypass_active, keys_matched). Truthy = an explicit
    surface signal asked for a same-day send."""
    matched: list[str] = []
    for key in policy.bypass_keys:
        value = _resolve_dotted_path(payload, key)
        if value:
            matched.append(key)
    return (bool(matched), tuple(matched))


def should_send_email(
    surface_name: str,
    payload: dict[str, Any] | None,
    *,
    now: datetime | None = None,
) -> CadenceDecision:
    """Decide whether ``surface_name`` should send its email right now.

    ``payload`` is the surface's state-file dict (or any dict that
    holds the bypass-key paths the policy declares). The decision is
    pure data; no I/O happens here. Callers can also pass ``now=``
    to test specific weekdays.
    """
    policy = require_policy(surface_name)
    now = now or datetime.now().astimezone()
    next_monday = _next_weekly_monday(now)
    bypass_active, bypass_matched = _bypass_check(policy, payload or {})

    # 2026-06-12 (Surface 15.5): digest mode. When DUCK_EMAIL_DIGEST_MODE=1,
    # the routine weekly info-surfaces fold into a single Monday
    # business_digest email instead of sending ~8 separate Monday emails.
    # The anomaly BYPASS still fires (a ≤2★ review / profit anomaly / new
    # build candidate still breaks through same-day) — only the routine
    # Monday send is suppressed.
    if (os.getenv("DUCK_EMAIL_DIGEST_MODE") == "1"
            and surface_name in DIGEST_FOLDED_SURFACES
            and not bypass_active):
        return CadenceDecision(
            surface_name=policy.surface_name,
            should_send=False,
            reason="folded_into_monday_business_digest",
            cadence=policy.cadence,
            next_send_iso=next_monday,
        )

    if policy.cadence == "daily":
        return CadenceDecision(
            surface_name=policy.surface_name,
            should_send=True,
            reason="cadence=daily",
            cadence=policy.cadence,
            next_send_iso=now.date().isoformat(),
        )

    if policy.cadence == "manual":
        return CadenceDecision(
            surface_name=policy.surface_name,
            should_send=False,
            reason="cadence=manual; explicit operator send required",
            cadence=policy.cadence,
            next_send_iso=None,
        )

    # weekly_monday
    is_monday = now.weekday() == _WEEKDAY_TO_INT["monday"]
    if is_monday:
        return CadenceDecision(
            surface_name=policy.surface_name,
            should_send=True,
            reason="cadence=weekly_monday; today is Monday rollup day",
            cadence=policy.cadence,
            next_send_iso=next_monday,
            bypass_active=bypass_active,
            bypass_keys_matched=bypass_matched,
        )
    if bypass_active:
        return CadenceDecision(
            surface_name=policy.surface_name,
            should_send=True,
            reason=f"cadence=weekly_monday; bypass triggered by {', '.join(bypass_matched)}",
            cadence=policy.cadence,
            next_send_iso=next_monday,
            bypass_active=True,
            bypass_keys_matched=bypass_matched,
        )
    return CadenceDecision(
        surface_name=policy.surface_name,
        should_send=False,
        reason=policy.deferred_note or "cadence=weekly_monday; deferred until next Monday",
        cadence=policy.cadence,
        next_send_iso=next_monday,
        bypass_active=False,
        bypass_keys_matched=(),
    )


def log_cadence_decision(
    decision: CadenceDecision,
    *,
    extra: dict[str, Any] | None = None,
    log_path: Path | None = None,
) -> None:
    """Append a decision row to ``state/email_cadence_decisions.jsonl``.

    Best-effort; never raises (so a flaky filesystem can't suppress an
    email that the policy said to send). ``extra`` lets the caller
    attach surface-specific debugging context."""
    target = log_path or DECISION_LOG_PATH
    record: dict[str, Any] = {
        "at": _now_iso(),
        "surface": decision.surface_name,
        "should_send": decision.should_send,
        "reason": decision.reason,
        "cadence": decision.cadence,
        "next_send_iso": decision.next_send_iso,
        "bypass_active": decision.bypass_active,
        "bypass_keys_matched": list(decision.bypass_keys_matched),
    }
    if extra:
        record.update(extra)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError:
        pass

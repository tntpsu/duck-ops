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
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DECISION_LOG_PATH = DUCK_OPS_ROOT / "state" / "email_cadence_decisions.jsonl"

# 2026-06-17 (Surface 23): operator-writable cadence overrides. Each surface's
# hardcoded POLICIES cadence is the default; an override here wins at runtime
# (no code commit). Operator vocabulary is off/weekly/daily; "off" stops the
# routine send but still honors a genuine anomaly bypass (operator choice).
EMAIL_CADENCE_OVERRIDES_PATH = DUCK_OPS_ROOT / "state" / "email_cadence_overrides.json"
_FROZEN_PRODUCTION_OVERRIDES_PATH = EMAIL_CADENCE_OVERRIDES_PATH.resolve()
_TEST_MODE_REFUSAL_ENV = "DUCK_TEST_MODE"
# operator label -> internal effective cadence ("off" is its own effective mode)
_OPERATOR_CADENCES: dict[str, str] = {"off": "off", "weekly": "weekly_monday", "daily": "daily"}
_OVERRIDE_HISTORY_CAP = 50

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


class TestModeRefusalError(RuntimeError):
    """Raised when set_override would write to the factory-default PRODUCTION
    overrides file while DUCK_TEST_MODE=1. Path-patched tests (conftest →
    tmp path) sail through; a test that bypasses isolation gets caught."""


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
        # 2026-06-16: turned OFF (operator preference). It was weekly_monday but
        # bypass_keys=("action_items_count",) fired EVERY day because the weekly
        # insights always produce action items — so a "weekly" email arrived
        # daily. cadence=manual + no bypass = never auto-sends; the daily data
        # sync still runs and insights stay on the portal (+ weekly Monday digest).
        cadence="manual",
        bypass_keys=(),
        deferred_note="Business intelligence email is off (operator preference); "
                      "insights live on the portal and the weekly Monday digest.",
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


def load_overrides() -> dict[str, str]:
    """Operator cadence overrides ({surface: "off"|"weekly"|"daily"}).

    Fail-soft: a missing or corrupt file returns {} so the gate always falls
    back to the hardcoded POLICIES defaults and NEVER crashes a send path."""
    try:
        raw = json.loads(EMAIL_CADENCE_OVERRIDES_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    overrides = raw.get("overrides") if isinstance(raw.get("overrides"), dict) else raw
    out: dict[str, str] = {}
    for surface, label in (overrides or {}).items():
        label = str(label or "").strip().lower()
        if label in _OPERATOR_CADENCES and str(surface or "").strip().lower() in POLICIES:
            out[str(surface).strip().lower()] = label
    return out


def _effective_cadence(surface_name: str, policy: CadencePolicy) -> str:
    """The cadence in force for this surface: an operator override (mapped to
    the internal vocabulary, with "off" as its own mode) if present and valid,
    else the hardcoded policy default."""
    label = load_overrides().get(str(surface_name or "").strip().lower())
    if label in _OPERATOR_CADENCES:
        return _OPERATOR_CADENCES[label]
    return policy.cadence


def set_override(surface_name: str, cadence: str) -> dict[str, str]:
    """Set (or clear) an operator cadence override. `cadence` is off/weekly/
    daily, or "default"/"" to remove the override. Validates the surface +
    value, refuses prod writes under DUCK_TEST_MODE, writes atomically, and
    records a capped history. Returns the updated overrides map."""
    surface = str(surface_name or "").strip().lower()
    if surface not in POLICIES:
        raise UnknownSurfaceError(
            f"Unknown surface {surface_name!r}; cannot set an email cadence override."
        )
    label = str(cadence or "").strip().lower()
    clearing = label in {"", "default", "none"}
    if not clearing and label not in _OPERATOR_CADENCES:
        raise ValueError(f"Invalid cadence {cadence!r}; expected off / weekly / daily (or 'default' to clear).")
    if (str(os.environ.get(_TEST_MODE_REFUSAL_ENV) or "").strip() in {"1", "true", "TRUE", "yes"}
            and EMAIL_CADENCE_OVERRIDES_PATH.resolve() == _FROZEN_PRODUCTION_OVERRIDES_PATH):
        raise TestModeRefusalError(
            "Refusing to write the production email_cadence_overrides.json under "
            "DUCK_TEST_MODE=1; the test isn't path-isolated (monkeypatch "
            "EMAIL_CADENCE_OVERRIDES_PATH to a tmp file)."
        )

    # read current (raw, to preserve history), apply, write atomically
    try:
        current = json.loads(EMAIL_CADENCE_OVERRIDES_PATH.read_text(encoding="utf-8"))
        if not isinstance(current, dict):
            current = {}
    except (OSError, json.JSONDecodeError, ValueError):
        current = {}
    overrides = dict(current.get("overrides") or {}) if isinstance(current.get("overrides"), dict) else {}
    history = list(current.get("override_history") or []) if isinstance(current.get("override_history"), list) else []
    prev = overrides.get(surface)
    if clearing:
        overrides.pop(surface, None)
    else:
        overrides[surface] = label
    history.append({"at": _now_iso(), "surface": surface, "from": prev, "to": (None if clearing else label)})
    payload = {"overrides": overrides, "override_history": history[-_OVERRIDE_HISTORY_CAP:]}

    EMAIL_CADENCE_OVERRIDES_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(EMAIL_CADENCE_OVERRIDES_PATH.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        os.replace(tmp, EMAIL_CADENCE_OVERRIDES_PATH)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return overrides


def list_effective_cadences() -> list[dict[str, Any]]:
    """Portal-facing: every surface with its effective cadence, its source
    (default vs operator override), and whether it folds into the Monday
    digest. Read-only."""
    overrides = load_overrides()
    out: list[dict[str, Any]] = []
    label_for = {v: k for k, v in _OPERATOR_CADENCES.items()}
    for surface in known_surfaces():
        policy = POLICIES[surface]
        ov = overrides.get(surface)
        effective = _OPERATOR_CADENCES[ov] if ov else policy.cadence
        out.append({
            "surface": surface,
            "effective_cadence": label_for.get(effective, effective),
            "default_cadence": label_for.get(policy.cadence, policy.cadence),
            "source": "override" if ov else "default",
            "folds_into_monday_digest": surface in DIGEST_FOLDED_SURFACES,
            "has_anomaly_bypass": bool(policy.bypass_keys),
        })
    return out


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
    effective = _effective_cadence(surface_name, policy)

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

    # Operator "off" override: stop the routine send, but a genuine anomaly
    # bypass still breaks through (2026-06-17 operator choice). A surface with
    # no bypass_keys (e.g. business_intelligence) is therefore fully silent.
    if effective == "off":
        if bypass_active:
            return CadenceDecision(
                surface_name=policy.surface_name,
                should_send=True,
                reason=f"operator_off; anomaly bypass triggered by {', '.join(bypass_matched)}",
                cadence="off",
                next_send_iso=now.date().isoformat(),
                bypass_active=True,
                bypass_keys_matched=bypass_matched,
            )
        return CadenceDecision(
            surface_name=policy.surface_name,
            should_send=False,
            reason="operator_off; email muted by operator (still on the portal / Monday digest)",
            cadence="off",
            next_send_iso=None,
        )

    if effective == "daily":
        return CadenceDecision(
            surface_name=policy.surface_name,
            should_send=True,
            reason="cadence=daily",
            cadence=effective,
            next_send_iso=now.date().isoformat(),
        )

    if effective == "manual":
        return CadenceDecision(
            surface_name=policy.surface_name,
            should_send=False,
            reason="cadence=manual; explicit operator send required",
            cadence=effective,
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

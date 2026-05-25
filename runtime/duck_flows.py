"""
Single source of truth for per-flow definitions across the duck stack.

Every place in the codebase that does ``if flow == "meme"`` (or branches
on a per-flow constant) should consult ``FLOWS`` instead. Today those
branches live in at least six different files across two repos:

    1. duck-ops/runtime/review_loop.py
         DUCKAGENT_PUBLISH_RECONCILIATION_SPECS
         DUCK_AGENT_HANDOFF_FLOWS
         DUCKAGENT_ARTIFACT_FLOW_ALIASES / DUCKAGENT_GATEWAY_FLOW_ALIASES
    2. duckAgent/creative_agent/.../viewer_data.py
         _DUCKAGENT_PUBLISH_STATE_SPECS (added 2026-05-25)
         _operator_review_resolved_by_workflow_state
    3. duckAgent/helpers/email_reply_action_helper.py
         default_reply_actions
    4. duckAgent/creative_agent/.../viewer.py
         portal Decisions button rows (JS)
    5. duckAgent/src/main_agent.py
         handle_mail_event flow branches

These six places are six descriptions of the same flow, written for
different audiences. When they drift the operator sees lingering
cards, lost decisions, or buttons that don't dispatch. Two bugs today
(flamingo and gym-girl) traced directly to a missing per-flow entry
in one of those six files.

This module is the migration target. Adding a new flow becomes one
``FlowSpec(...)`` entry in ``FLOWS`` and the consumers all pick it up.
``require_flow(name)`` raises ``UnknownFlowError`` on lookup miss so
forgetting to register a flow becomes a noisy crash, not a silent
filter skip.

Architecture: this file lives in duck-ops but is intentionally
dependency-light so duckAgent can import it via sys.path injection
without pulling in the rest of duck-ops. No imports beyond stdlib.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# Status values DuckAgent writes into state file ``*_publish_status``
# keys that mean "the platform accepted this". Treated as terminal
# success for the reconciliation and queue-self-correction paths.
PUBLISH_SUCCESS_STATUSES: frozenset[str] = frozenset(
    {"success", "partial", "scheduled", "published_now", "published"}
)


class UnknownFlowError(KeyError):
    """Raised when code references a flow name that isn't registered.

    This is the fail-loud guarantee. Today, an unrecognized flow
    silently falls through whatever branch is being evaluated — the
    operator sees a stale card or a dropped action. Going forward,
    consult ``FLOWS`` through ``require_flow()`` and this exception
    will surface the missing registry entry at the first place that
    needs it."""


@dataclass(frozen=True)
class ReplyAction:
    """One button on a Duck Ops approval email — the operator clicks
    it, Gmail composes a mailto reply with ``text`` as the body, and
    the inbox watcher routes it back through handle_mail_event."""

    label: str
    text: str
    action: str  # canonical action verb: publish / apply / approve / revise


@dataclass(frozen=True)
class FlowSpec:
    """Everything the duck stack needs to know about one workflow lane.

    Adding a new flow means writing one ``FlowSpec(...)`` and adding it
    to ``FLOWS`` below. Every consumer module reads from here.

    The fields are union of all per-flow concerns I found across the
    six scattered files at the time of this writing. New concerns go
    here, not into a parallel dict somewhere."""

    # Identity
    name: str

    # ── DuckAgent publish-state probe ────────────────────────────────
    # ``runs/<run_id>/<state_file>`` is the JSON DuckAgent writes when
    # the flow's publish pipeline completes. The three probe fields
    # are read with "any-of" semantics — any one truthy field counts as
    # "this artifact has reached the platform".
    state_file: str = ""
    publish_status_key: str = ""
    publish_truthy_keys: tuple[str, ...] = ()
    scheduled_at_keys: tuple[str, ...] = ()
    reconciliation_note: str = ""

    # ── Email approval buttons ───────────────────────────────────────
    # The mailto templates that render on each approval email. Order
    # matters — the first action is the primary button.
    reply_actions: tuple[ReplyAction, ...] = ()

    # ── Decision-gateway handoff mapping ─────────────────────────────
    # When an operator records action X (e.g. "approve"), how should
    # duck-ops route it to DuckAgent? {"approve": "publish"} means an
    # approve from the operator triggers DuckAgent's publish path.
    handoff_actions: dict[str, str] = field(default_factory=dict)

    # ── Portal display ───────────────────────────────────────────────
    # Hint for the operator-facing language. Today this is informal;
    # consumers may render it as the "what approval means" copy block.
    approval_meaning: str = ""

    def publish_succeeded(self, payload: dict[str, Any]) -> bool:
        """Return True when the DuckAgent state file ``payload``
        indicates this flow's publish reached the platform.

        Used by queue self-correction (drop the operator card) and by
        reconciliation (mark the quality-gate decision approved). Any
        of three signals counts as success: a status key matching the
        success set, any truthy key from ``publish_truthy_keys``, or
        any populated key from ``scheduled_at_keys``. The any-of
        semantics matches DuckAgent's own writes — different flows
        write different fields on success."""
        if not isinstance(payload, dict):
            return False
        if self.publish_status_key:
            status = str(payload.get(self.publish_status_key) or "").strip().lower()
            if status in PUBLISH_SUCCESS_STATUSES:
                return True
        if any(payload.get(key) for key in self.publish_truthy_keys):
            return True
        if any(payload.get(key) for key in self.scheduled_at_keys):
            return True
        return False

    def has_publish_state(self) -> bool:
        """True when this flow writes a DuckAgent state file we can
        probe. False for flows like ``design_brief_queue`` where the
        operator decision lands before any platform publish."""
        return bool(self.state_file) and (
            bool(self.publish_status_key)
            or bool(self.publish_truthy_keys)
            or bool(self.scheduled_at_keys)
        )


# ── The registry ────────────────────────────────────────────────────
#
# One entry per flow. Add new flows here and the consumers pick them
# up automatically (as they migrate to FLOWS lookups).
#
# Keep entries alphabetized by name so the diff stays readable.

FLOWS: dict[str, FlowSpec] = {
    "design_brief_queue": FlowSpec(
        name="design_brief_queue",
        # Pre-image-gen approval. No DuckAgent publish state yet.
        reply_actions=(ReplyAction("Reply Approve", "approve", "approve"),),
        handoff_actions={"approve": "approve"},
        approval_meaning="Approves a proposed concept brief so DuckAgent can generate the concept image.",
    ),
    "gtdf": FlowSpec(
        name="gtdf",
        state_file="state_gtdf.json",
        scheduled_at_keys=("gtdf_scheduled_at",),
        reconciliation_note="Reconciled automatically because DuckAgent already shows this GTDF post as scheduled.",
        handoff_actions={"approve": "publish", "needs_changes": "revise"},
    ),
    "jeepfact": FlowSpec(
        name="jeepfact",
        state_file="state_jeepfact.json",
        publish_status_key="jeepfact_publish_status",
        scheduled_at_keys=("jeepfact_scheduled_at", "jeepfact_published_at"),
        reconciliation_note="Reconciled automatically because DuckAgent already shows this Jeep Fact post as scheduled or published.",
        reply_actions=(ReplyAction("Reply Jeep Fact Publish", "jeepfact publish", "publish"),),
        handoff_actions={"approve": "publish", "needs_changes": "revise"},
    ),
    "meme": FlowSpec(
        name="meme",
        state_file="state_meme.json",
        publish_status_key="meme_publish_status",
        publish_truthy_keys=("meme_fb_id",),
        scheduled_at_keys=("meme_scheduled_at", "meme_published_at"),
        reconciliation_note="Reconciled automatically because DuckAgent already shows this meme as scheduled or published.",
        reply_actions=(ReplyAction("Reply Meme Publish", "meme publish", "publish"),),
        handoff_actions={"approve": "publish", "needs_changes": "revise"},
        approval_meaning="Approving schedules this meme on Facebook + Instagram.",
    ),
    "newduck": FlowSpec(
        name="newduck",
        state_file="state_newduck.json",
        publish_truthy_keys=("newduck_published", "shopify_product_id", "etsy_listing_id"),
        scheduled_at_keys=("newduck_published_at", "published_at"),
        reconciliation_note="Reconciled automatically because DuckAgent already shows this listing as published.",
    ),
    "reviews_story": FlowSpec(
        name="reviews_story",
        state_file="state_reviews.json",
        publish_status_key="reviews_story_publish_status",
        publish_truthy_keys=("reviews_story_published",),
        scheduled_at_keys=("reviews_story_published_at",),
        reconciliation_note="Reconciled automatically because DuckAgent already shows this review story as sent.",
        handoff_actions={"approve": "publish"},
    ),
    "shopify_draft_activation": FlowSpec(
        name="shopify_draft_activation",
        # Mutates Shopify directly; no per-run state file.
        reply_actions=(
            ReplyAction("Reply Publish", "publish", "publish"),
            ReplyAction("Reply Apply", "apply", "apply"),
        ),
    ),
    "shopify_seo": FlowSpec(
        name="shopify_seo",
        # Mutates Shopify directly; no per-run state file.
        reply_actions=(ReplyAction("Reply Apply", "apply", "apply"),),
    ),
    "thursday": FlowSpec(
        name="thursday",
        state_file="state_thursday.json",
        publish_truthy_keys=("thursday_published",),
        scheduled_at_keys=("thursday_publish_time",),
        reconciliation_note="Reconciled automatically because DuckAgent already shows this Thursday post as published.",
        handoff_actions={"approve": "publish", "needs_changes": "revise"},
    ),
    "weekly_sale": FlowSpec(
        name="weekly_sale",
        state_file="state_weekly.json",
        publish_truthy_keys=("weekly_sale_published",),
        scheduled_at_keys=("weekly_sale_published_at",),
        reconciliation_note="Reconciled automatically because DuckAgent already shows this weekly sale as published.",
        handoff_actions={"approve": "publish", "needs_changes": "revise"},
    ),
}


# Aliases that map non-canonical names to FLOWS keys. Today these
# come from review_loop's DUCKAGENT_ARTIFACT_FLOW_ALIASES and
# DUCKAGENT_GATEWAY_FLOW_ALIASES. Keeping them in this module so the
# resolver lives next to the registry.
FLOW_ALIASES: dict[str, str] = {
    # artifact_type → flow name
    "listing": "newduck",
    "promotion": "weekly_sale",
    "social_post": "meme",
    # gateway label → flow name
    "reviews": "reviews_story",
}


def resolve_flow(name: str | None) -> str:
    """Normalize ``name`` (or an artifact_type alias) to a canonical
    flow name. Returns the input lowercased+stripped if it's not an
    alias; the caller can then check FLOWS membership."""
    candidate = str(name or "").strip().lower()
    if not candidate:
        return ""
    return FLOW_ALIASES.get(candidate, candidate)


def get_flow(name: str | None) -> FlowSpec | None:
    """Return the FlowSpec for ``name`` (after alias resolution), or
    None if unknown. Use this when missing flows are expected (e.g.
    code paths that handle arbitrary user input)."""
    resolved = resolve_flow(name)
    return FLOWS.get(resolved)


def require_flow(name: str | None) -> FlowSpec:
    """Return the FlowSpec for ``name`` (after alias resolution), or
    raise UnknownFlowError if unknown.

    Use this in code paths where the flow MUST be registered — for
    example, the handle_mail_event dispatcher. A missing entry there
    is a real bug, not a permissive skip. The exception names the
    flow and points at this module so the fix is obvious."""
    resolved = resolve_flow(name)
    if resolved not in FLOWS:
        raise UnknownFlowError(
            f"Unknown flow {name!r} (resolved to {resolved!r}). "
            "Add a FlowSpec entry in duck_flows.py::FLOWS."
        )
    return FLOWS[resolved]


def known_flow_names() -> tuple[str, ...]:
    """Sorted tuple of every registered flow name."""
    return tuple(sorted(FLOWS.keys()))

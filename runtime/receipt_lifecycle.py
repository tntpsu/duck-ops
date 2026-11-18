"""Which workflow_control receipts are STUCK, and which are just parked.

A `state` of anything-but-verified is not evidence of stuck work, and that is
why nothing had ever graded these. Counting naively on 2026-11-18 gave "499
non-terminal receipts", which is useless: 143 of them were competitor
snapshots at 0 days old, doing exactly what they are supposed to. An earlier
estimate of "34" was equally wrong in the other direction. Neither number
told the operator anything, so neither got acted on.

The distinction is not derivable from the receipt. `observed` is TERMINAL for
an observation lane (a snapshot was taken; there is no next step) and
NON-TERMINAL for a production lane (a draft was prepared and is waiting for a
human). One state, two meanings -- the shape
[[feedback_status_fields_one_meaning]] is about. So it is DECLARED here, as
data, per (lane, state_reason) pair.

THE UNDECLARED CASE IS LOUD, NOT GUESSED. A pair absent from POLICY is
reported as `unclassified` rather than quietly graded either way. Every
allowlist that bit this stack in 2026-11 bit it by failing silently on the
value nobody had thought of -- `state_reason == "awaiting_review"` saw 1 stuck
draft of 3, the `.env` suffix predicate tracked live keys for nine months.
An unclassified pair is a question for the operator, which is strictly better
than a confident wrong answer in either direction.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

#: States that are terminal no matter which lane they appear in. `blocked` is
#: deliberately NOT here -- a blocked receipt needs action -- but it is not
#: graded against a window either: it gets its own kind and is handed to
#: `build_workflow_followthrough_items(include_all_blocked=True)`, which
#: already owns that story with a root cause and a fix hint. Grading it here
#: too would give the operator two cards for one problem. All 101 of the live
#: blocked receipts are that lane (22 etsy_token_auth, 21 meme
#: execution_failed, ...); they were the entire "unclassified" bucket on the
#: first pass, which is how a kind that means "I do not know" turns into
#: noise and gets ignored.
UNIVERSALLY_TERMINAL = frozenset({
    "verified",      # the work completed and was checked
    "dismissed",     # the operator retired it
    "resolved",      # the lane closed it out (replied, already-replied, stale)
    "cancelled",
    "published",
    "completed",
    "skipped_off",   # the flow was skipped because the operator paused it
})

#: PARKED: this (lane, state_reason) is where the lane legitimately leaves a
#: receipt forever. An observation or notification record with no next step.
PARKED = "parked"

#: PENDING: work that was supposed to progress, with the number of days after
#: which silence is a problem. The window is the lane's own cadence doubled,
#: the house rule for freshness thresholds -- a weekly lane gets 14, a
#: same-day action gets 3.
PENDING = "pending"

#: BLOCKED: `state == "blocked"`. Reported, never windowed -- owned elsewhere.
BLOCKED = "blocked"

#: UNCLASSIFIED: no POLICY entry. A question, not a verdict.
UNCLASSIFIED = "unclassified"

#: How many receipts of one (lane, state_reason) pair are real at once.
#:
#: PER_ENTITY -- each receipt is its own stuck thing: 14 `newduck/draft_ready`
#: receipts are 14 distinct listing drafts, and grading only the newest would
#: hide 13 of them.
#:
#: PER_RUN -- the pair is one recurring event and the receipts are its log:
#: `profit/weekly_operator_email_deferred` has 148 of them because the Monday
#: digest defers one every day. Only the NEWEST is graded; if that one is
#: fresh the handoff is alive and the backlog behind it is just receipts
#: nobody retires. Getting this wrong is what made the first pass report 185
#: overdue items of which 127 were a single healthy lane's log -- a count that
#: large reads as noise and gets switched off
#: ([[feedback_a_number_needs_its_denominator]]).
PER_ENTITY = "per_entity"
PER_RUN = "per_run"


@dataclass(frozen=True)
class Policy:
    kind: str                  # PARKED or PENDING
    window_days: int | None    # PENDING only
    basis: str                 # why this window, in one line
    cardinality: str = PER_ENTITY   # PENDING only


def _parked(basis: str) -> Policy:
    return Policy(PARKED, None, basis)


def _pending(window_days: int, basis: str, *, cardinality: str = PER_ENTITY) -> Policy:
    return Policy(PENDING, window_days, basis, cardinality)


#: Declared per `(lane, state_reason)`. Measured from the live receipts on
#: 2026-11-18; counts and ages in the `basis` strings are that measurement,
#: not a guess.
POLICY: dict[tuple[str, str], Policy] = {
    # --- observation / notification lanes: `observed` is their terminal ----
    ("competitor", "snapshot_collected"): _parked(
        "143 receipts, newest 0d: the daily competitor snapshot's own record"),
    ("competitor", "data_collected"): _parked("same lane, older spelling"),
    ("competitor", "report_ready"): _parked("a report was produced; no next step"),
    ("profit", "report_ready"): _parked("a report was produced; no next step"),
    ("notifier", "pending_delivery"): _parked(
        "the notifier's own send record; the queue drains it, not an operator"),
    ("ops", "backlog_present"): _parked("an observation about a backlog, not a task"),
    ("trend_ranker", "pending_review"): _parked(
        "trend_ranker writes this every run; the review is the weekly digest"),
    ("weekly_sale_monitor", "weak_items_present"): _parked(
        "a monitor observation; the weekly sale step consumes it"),

    # --- deferred operator emails -----------------------------------------
    # A deferral naming a destination is a HANDOFF CONTRACT, so these are
    # PENDING on the deferral, not parked: the SEO weak_title batch vanished
    # twice in 2026 because a fold had no rendering consumer and no age alarm
    # ([[feedback_fold_defer_is_a_handoff_contract]]). profit's newest is 0d
    # (the Monday digest is carrying them); competitor's newest is 167d, which
    # is the alarm this entry exists to raise.
    ("profit", "weekly_operator_email_deferred"): _pending(
        21, "folded into the Monday business digest; weekly cadence x3",
        cardinality=PER_RUN),
    ("competitor", "weekly_operator_email_deferred"): _pending(
        21, "folded into the Monday business digest; weekly cadence x3",
        cardinality=PER_RUN),

    # --- production lanes: a prepared draft waiting on a human ------------
    ("newduck", "draft_ready"): _pending(
        30, "a prepared listing draft; 14 of these sit at 197-213d"),
    ("newduck", "awaiting_review"): _pending(14, "operator review of one listing"),
    ("newduck", "shopify_activation_pending"): _pending(
        14, "a Shopify draft waiting to be activated; 11 sit at 41-208d"),
    ("meme", "draft_ready"): _pending(14, "weekly Monday lane; cadence x2"),
    ("meme", "copy_prepared"): _pending(14, "weekly Monday lane; cadence x2"),
    ("blog", "draft_article_ready"): _pending(
        30, "blog cadence is irregular; 8 sit at 150-199d"),
    ("weekly", "draft_article_ready"): _pending(30, "same shape as blog"),
    ("thursday", "batch_ready"): _pending(14, "weekly Thursday lane; cadence x2"),
    ("thursday", "candidate_pool_ready"): _pending(14, "weekly Thursday lane"),
    ("gtdf", "batch_ready"): _pending(14, "weekly lane; cadence x2"),

    # --- approval / execution waits ---------------------------------------
    ("review_execution", "reply_preview_staged"): _pending(
        7, "a drafted review reply; the drain paces 2/session x 3 windows/day"),
    ("customer_reply", "reply_preview_staged"): _pending(
        3, "a customer is waiting on a reply"),
    ("customer_reply", "needs_thread_review"): _pending(
        3, "a customer is waiting on a reply"),
    ("customer_reply", "thread_observed"): _pending(
        3, "a customer thread seen but not triaged"),
    ("review_execution", "submit_confirmed"): _pending(
        2, "submitted to Etsy; verification should follow the same day"),
    ("occasion_tags", "awaiting_operator_approval"): _pending(
        21, "the tag scheduler proposes ahead of the occasion"),
    ("duckvideo", "review_email_sent"): _pending(
        14, "an approval email went out; newest is 0d so the lane is live",
        cardinality=PER_RUN),
    ("review_carousel", "approval_reminder_sent"): _pending(
        14, "weekly Tuesday lane; cadence x2"),
}


@dataclass(frozen=True)
class ReceiptVerdict:
    lane: str
    state: str
    state_reason: str
    age_days: float | None
    kind: str          # PARKED, PENDING or "unclassified"
    overdue: bool
    window_days: int | None
    paused: bool
    cardinality: str
    workflow_id: str
    path: str


def _age_days(updated_at: Any, *, now: datetime | None = None) -> float | None:
    text = str(updated_at or "").strip()
    if not text:
        return None
    try:
        when = datetime.fromisoformat(text)
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    reference = now or datetime.now(timezone.utc)
    return (reference - when).total_seconds() / 86400.0


def classify_receipt(
    receipt: Any,
    *,
    now: datetime | None = None,
    paused_lanes: frozenset[str] | set[str] | None = None,
    path: str = "",
) -> ReceiptVerdict | None:
    """One receipt's verdict, or None when the state is universally terminal.

    A paused lane's receipt is never overdue: nothing is going to progress it
    and the operator said so. That check routes through the same
    `lane_pause` reader every other surface uses rather than being a seventh
    inline copy -- the caller supplies the paused set.
    """
    if not isinstance(receipt, dict):
        return None
    state = str(receipt.get("state") or "").strip().lower()
    if state in UNIVERSALLY_TERMINAL:
        return None

    lane = str(receipt.get("lane") or "").strip()
    reason = str(receipt.get("state_reason") or "").strip()
    age = _age_days(receipt.get("updated_at"), now=now)
    policy = POLICY.get((lane, reason))
    paused = lane in (paused_lanes or frozenset())

    cardinality = PER_ENTITY
    if state == BLOCKED:
        kind, window, overdue = BLOCKED, None, False
    elif policy is None:
        kind, window, overdue = UNCLASSIFIED, None, False
    elif policy.kind == PARKED:
        kind, window, overdue = PARKED, None, False
    else:
        kind, window, cardinality = PENDING, policy.window_days, policy.cardinality
        overdue = (not paused and age is not None and window is not None
                   and age > window)

    return ReceiptVerdict(
        lane=lane, state=state, state_reason=reason, age_days=age,
        kind=kind, overdue=overdue, window_days=window, paused=paused,
        cardinality=cardinality,
        workflow_id=str(receipt.get("workflow_id") or ""), path=path,
    )


def overdue_items(verdicts) -> list[ReceiptVerdict]:
    """The overdue receipts, with every PER_RUN pair collapsed to its newest.

    Collapsing happens HERE rather than in `classify_receipt`, because one
    receipt cannot know whether a newer sibling exists. The newest receipt of
    a per-run pair is the one that says whether the handoff is alive; if it is
    inside its window the whole pair is, and the older ones are a log.
    """
    newest_per_run: dict[tuple[str, str], ReceiptVerdict] = {}
    per_entity: list[ReceiptVerdict] = []
    for v in verdicts:
        if v.kind != PENDING:
            continue
        if v.cardinality == PER_RUN:
            key = (v.lane, v.state_reason)
            current = newest_per_run.get(key)
            if current is None or (v.age_days or 0) < (current.age_days or 0):
                newest_per_run[key] = v
        elif v.overdue:
            per_entity.append(v)
    collapsed = [v for v in newest_per_run.values() if v.overdue]
    return sorted(per_entity + collapsed,
                  key=lambda v: -(v.age_days or 0))

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

#: SUPERSEDED: a live producer is tracking this same entity under a NEWER
#: identity, so the receipt is a frozen snapshot rather than open work.
#:
#: Found 2026-11-18 while working the five oldest customer_reply receipts.
#: All five were April/May snapshots; the `etsy_conversation_browser_sync`
#: producer regenerates DAILY and tracked every one of the same threads,
#: three of them under a different `linked_customer_short_id` (C301->C392,
#: C394->C426, C395->C396) because the thread had been re-keyed to a newer
#: case. Two stores, one of them frozen -- the shape
#: [[feedback_retire_artifacts_in_authoritative_store]] is about, and the
#: reason an age window alone cannot tell stuck work from dead bookkeeping.
#:
#: Detecting it beats relying on someone reading five receipts by hand: the
#: next three will go stale the same way and nobody will be looking.
SUPERSEDED = "superseded"

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

    # --- Etsy browser batch -------------------------------------------------
    ("etsy_browser_batch", "browser_batch_failed"): _pending(
        1, "first seen 2026-11-22 (two slots timed out); three slots a day, "
           "so a healthy lane overwrites this within hours -- still failed a "
           "day later means the batch itself is broken",
        cardinality=PER_RUN),

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
    # PARKED, after reading the mailbox AS DELIVERED rather than trusting the
    # receipts ([[feedback_fold_defer_is_a_handoff_contract]]).
    #
    # These 7 receipts were first classified PENDING, which made the card
    # report "competitor's weekly email stopped 167 days ago" as a dead
    # handoff. That was WRONG. The operator said so -- "I thought I got 3
    # every week" -- and the mailbox agrees: `Competitor Analysis Report`
    # arrived 2026-11-15, 11-08, 11-01, 10-18, 09-13 and 09-06, from:me
    # to:me, most recently three days before this was written.
    #
    # The two are different paths. The deferral receipt is written by
    # profit_intel.py's cadence gate; the report is sent by
    # duckAgent/flows/competitor/competitor_reports.py, which has no
    # deferral step. So these receipts are the last traces of a path
    # ABANDONED in favour of a direct weekly send, not evidence of a
    # handoff that died -- the newest is 167d because that is when the
    # competitor report stopped going through the gate.
    #
    # The lesson is about the method, not the entry: a receipt said the
    # handoff was dead and the delivered mail said it was alive. The mail
    # wins, every time.
    ("competitor", "weekly_operator_email_deferred"): _parked(
        "retired path: the report now sends directly (verified in the "
        "mailbox 2026-11-15, -08, -01); profit_intel's gate no longer "
        "carries it"),

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
    # Appeared in the live store 2026-11-19T09:21 (today's This-or-That email)
    # and was caught by test_no_live_receipt_falls_into_an_undeclared_pair.
    # PENDING on the same 14d as its sibling states: the post slot is same-day,
    # but grading at one day would fire every week the operator simply has not
    # answered yet, and this lane has FIVE receipts already dismissed with
    # `slot_passed` -- an unanswered choice going stale is the real failure, so
    # it needs a window, just not a hair trigger.
    ("thursday", "awaiting_review"): _pending(
        14, "operator A/B choice on one Thursday post; weekly cadence x2"),
    ("gtdf", "batch_ready"): _pending(14, "weekly lane; cadence x2"),

    # Appeared in the live store 2026-11-19T00:23 and was caught by
    # test_no_live_receipt_falls_into_an_undeclared_pair, not by anyone reading
    # the code -- which is what that guard is for.
    #
    # PARKED, and deliberately NOT _pending: `quality-gate.json` is one
    # recurring log that quality_gate_pilot rewrites on every sidecar run (948
    # receipts, 12-entry history, updated_at advancing every ~6h), flipping
    # between verified/gating_ready and this state as `pending_count` moves off
    # zero. Its age therefore CANNOT exceed any window, so declaring a window
    # here would be coverage that can never fire -- worse than none, because it
    # reads as graded.
    #
    # The work itself is owned: the 6 pending artifacts behind this state are
    # graded by the decision_gateway_sync card (pending_count 6,
    # stale_pending_with_proof_count 0 at the time of writing), which is the
    # surface that can actually tell a fresh pending item from a stalled one.
    ("quality_gate", "awaiting_operator_resolution"): _parked(
        "the pending-review backlog is graded by decision_gateway_sync; this "
        "receipt is a per-run log whose age is meaningless"),

    # --- approval / execution waits ---------------------------------------
    ("review_execution", "reply_preview_staged"): _pending(
        7, "a drafted review reply; the drain paces 2/session x 3 windows/day"),
    ("customer_reply", "reply_preview_staged"): _pending(
        3, "a customer is waiting on a reply"),
    ("customer_reply", "needs_thread_review"): _pending(
        3, "a customer is waiting on a reply"),
    ("customer_reply", "thread_observed"): _pending(
        3, "a customer thread seen but not triaged"),
    # The step BETWEEN staged and submitted: approved and waiting on the
    # drain. `review_reply_executor` treats this receipt as the durable source
    # of truth and rehydrates the queue file from it, so these are genuinely
    # in flight rather than a per-run log. Same 7 days as the staged state it
    # follows, because it is the same work one step later and the drain is
    # DESIGNED to clear a backlog over days (2/session x 3 windows/day) -- a
    # tighter window would alarm on the pacing rather than on a stall
    # ([[feedback_rolling_window_metrics_lag_fixes]]). Measured 2026-11-20:
    # 3 receipts, newest 0d.
    ("review_execution", "queued_for_execution"): _pending(
        7, "approved and queued; the drain paces 2/session x 3 windows/day"),
    ("review_execution", "submit_confirmed"): _pending(
        2, "submitted to Etsy; verification should follow the same day"),
    ("occasion_tags", "awaiting_operator_approval"): _pending(
        21, "the tag scheduler proposes ahead of the occasion"),
    ("duckvideo", "review_email_sent"): _pending(
        14, "an approval email went out; newest is 0d so the lane is live",
        cardinality=PER_RUN),
    ("review_carousel", "approval_reminder_sent"): _pending(
        14, "weekly Tuesday lane; cadence x2"),
    # Appeared in the live store 2026-11-22T20:15, the first express drop to
    # reach its approval email. 7d matches the digest's last reminder
    # milestone (AGE_MILESTONES = 1, 3, 7): past that nobody is nudging it.
    ("express", "approval_email_sent"): _pending(
        7, "one agent drop waiting on the operator; digest reminds at 1/3/7d"),
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
    #: Set when kind is SUPERSEDED: what is tracking this entity now.
    superseded_by: str | None = None


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


#: Per-lane: which receipt metadata field names the entity, and which live
#: identity field to compare against. A receipt is SUPERSEDED when the live
#: producer tracks the same entity key under a different identity.
SUPERSESSION_KEYS: dict[str, str] = {
    "customer_reply": "conversation_thread_key",
}

#: Per-lane: the metadata field that names the PRODUCT a receipt is about,
#: for lanes where a later publish of that product supersedes its drafts.
#:
#: Every newduck prepare iteration leaves its own `draft_ready` receipt
#: (`..._hardened2` through `_hardened9` for one Monster Truck Duck), and none
#: is ever retired when the duck publishes -- so on 2026-11-21, 15 of the 42
#: receipts reddening `receipt_followthrough` were drafts of ducks already live
#: on Shopify. Matched on the EXACT name (casefolded, stripped): "Bronco Buddy"
#: must not resolve "Bronco Buddy Duck", because a false supersession hides the
#: stall the card exists to catch.
PUBLISH_SUPERSESSION_KEYS: dict[str, str] = {
    "newduck": "duck_name",
}

#: Receipt (state, state_reason) pairs that PROVE the product went live.
#: `state_reason=None` means any reason in that state.
PUBLISH_EVIDENCE: dict[str, frozenset[tuple[str, str | None]]] = {
    "newduck": frozenset({
        ("verified", None),                                # shopify_activated / published
        ("resolved", "verified_active_on_shopify"),        # read back from Shopify
    }),
}


def _parse_when(value: Any) -> datetime | None:
    try:
        when = datetime.fromisoformat(str(value or "").strip())
    except ValueError:
        return None
    return when if when.tzinfo else when.replace(tzinfo=timezone.utc)


def _is_publish_evidence(receipt: dict) -> bool:
    lane = str(receipt.get("lane") or "").strip()
    state = str(receipt.get("state") or "").strip().lower()
    reason = str(receipt.get("state_reason") or "").strip()
    accepted = PUBLISH_EVIDENCE.get(lane) or frozenset()
    return (state, None) in accepted or (state, reason) in accepted


def _product_name(receipt: dict) -> str:
    key = PUBLISH_SUPERSESSION_KEYS.get(str(receipt.get("lane") or "").strip())
    metadata = receipt.get("metadata")
    if not key or not isinstance(metadata, dict):
        return ""
    return str(metadata.get(key) or "").strip().casefold()


def build_publish_index(receipts: Any) -> dict[tuple[str, str], tuple[str, datetime]]:
    """(lane, product name) -> (workflow_id, time) of its LATEST publish."""
    index: dict[tuple[str, str], tuple[str, datetime]] = {}
    for receipt in receipts or []:
        if not isinstance(receipt, dict) or not _is_publish_evidence(receipt):
            continue
        name = _product_name(receipt)
        when = _parse_when(receipt.get("updated_at"))
        if not name or when is None:
            continue
        key = (str(receipt.get("lane")).strip(), name)
        if key not in index or when > index[key][1]:
            index[key] = (str(receipt.get("workflow_id") or ""), when)
    return index


#: Lanes whose off-switch is NOT a `<lane>_execution.json` FlowSpec config.
#:
#: `paused_lanes` is built by globbing those FlowSpec configs, so a lane gated
#: anywhere else can never be found paused -- and a receipt lane nobody runs
#: keeps aging until the card goes red about work the operator deliberately
#: stopped. That is the same false-red this module exists to prevent, arriving
#: through the back door.
#:
#: Declared here as DATA, next to the policy it modifies, so the next such
#: lane is one entry rather than another branch in whichever card notices
#: first. Value: (config filename under duck-ops/config, key path to the
#: boolean). The caller does the file IO; this module does the reading.
#: Value: (config filename under duck-ops/config, one or more key paths to a
#: boolean). The lane is off when ANY named switch is off -- a lane needs every
#: one of its switches to do work, so one of them being false is enough.
EXTERNAL_LANE_ENABLED_SWITCHES: dict[str, tuple[str, tuple[tuple[str, ...], ...]]] = {
    # 2026-11-18: the operator answers Etsy messages with their own Grok bot.
    # TWO switches, because they are genuinely separate decisions: reads could
    # come back for context without re-enabling replies. Either one being off
    # means nothing will progress a customer_reply receipt.
    "customer_reply": (
        "etsy_browser_batch.json",
        (("customer_message_reply", "enabled"), ("customer_read", "enabled")),
    ),
}


def external_switch_is_off(lane: str, config_payload: Any) -> bool:
    """True only when the lane's own config says, in a real boolean, that its
    producer is switched off.

    Fails toward GRADING: an absent file, a missing key, or a string "false"
    returns False (not paused). A missing config must never be able to park a
    real backlog silently -- that reading would be indistinguishable from the
    allowlists that went quiet on this stack all November
    ([[feedback_a_clean_scan_is_only_as_wide_as_its_selection]]).
    """
    entry = EXTERNAL_LANE_ENABLED_SWITCHES.get(lane)
    if entry is None:
        return False
    _, key_paths = entry
    for key_path in key_paths:
        node: Any = config_payload
        reachable = True
        for key in key_path:
            if not isinstance(node, dict) or key not in node:
                reachable = False
                break
            node = node[key]
        if reachable and isinstance(node, bool) and node is False:
            return True
    return False


def classify_receipt(
    receipt: Any,
    *,
    now: datetime | None = None,
    paused_lanes: frozenset[str] | set[str] | None = None,
    live_identity: dict[str, str] | None = None,
    published: dict[tuple[str, str], tuple[str, datetime]] | None = None,
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

    # Supersession is checked BEFORE the window, because an age window cannot
    # tell a frozen snapshot from open work -- that is exactly how five
    # customer_reply receipts read as 220-day-old unanswered customers when
    # four of them were already handled or re-keyed.
    superseded_by = None
    entity_key = SUPERSESSION_KEYS.get(lane)
    if entity_key and live_identity:
        metadata = receipt.get("metadata")
        thread = str((metadata or {}).get(entity_key) or "").strip() \
            if isinstance(metadata, dict) else ""
        current = live_identity.get(thread) if thread else None
        mine = str(receipt.get("entity_id") or "").strip()
        if current and mine and current != mine:
            superseded_by = current

    # A later publish of the same product supersedes its drafts -- but only a
    # LATER one. A draft made after the duck went live is a real listing edit
    # still waiting on someone, and must stay visible.
    if superseded_by is None and published and lane in PUBLISH_SUPERSESSION_KEYS:
        name = _product_name(receipt)
        hit = published.get((lane, name)) if name else None
        drafted = _parse_when(receipt.get("updated_at"))
        if (hit and drafted is not None and hit[1] >= drafted
                and hit[0] != str(receipt.get("workflow_id") or "")):
            superseded_by = hit[0]

    cardinality = PER_ENTITY
    if superseded_by:
        return ReceiptVerdict(
            lane=lane, state=state, state_reason=reason, age_days=age,
            kind=SUPERSEDED, overdue=False, window_days=None, paused=paused,
            cardinality=PER_ENTITY,
            workflow_id=str(receipt.get("workflow_id") or ""), path=path,
            superseded_by=superseded_by,
        )
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

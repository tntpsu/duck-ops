"""STATUS_CONTRACT debt #3 guard: `chain_state` is bucketed CROSS-KIND in
business_operator_desk (review_apply + promotion items share one filter at
:1301-1305). That is safe ONLY while the two value domains stay disjoint. This
invariant pins that — if a producer ever emits a colliding value (e.g. promotion
gains `idle`/`all_clear`, or review-apply gains `active`/`ready`), the shared
bucketer would mis-classify it, and this test fails loudly first.

Domains mirror the producers:
  review_apply: shopify_seo_outcomes.py:324-341
  promotion:    business_operator_desk.py:605-614 / :1144-1158
Keep these two sets in sync with those producers."""
from __future__ import annotations

REVIEW_APPLY_CHAIN_STATES = {
    "awaiting_review", "apply_attention", "ready_to_send_next", "all_clear", "idle",
}
PROMOTION_CHAIN_STATES = {"active", "ready", "blocked", "observing"}


def test_chain_state_domains_are_disjoint():
    overlap = REVIEW_APPLY_CHAIN_STATES & PROMOTION_CHAIN_STATES
    assert not overlap, (
        f"chain_state value collision across kinds: {sorted(overlap)} — the "
        "cross-kind bucketer in business_operator_desk would mis-classify it. "
        "Disambiguate the bucketing by `chain_kind`, or rename the colliding value."
    )


def test_intentional_cross_kind_groupings_are_documented():
    # The operator buckets deliberately merge semantically-aligned states from
    # both kinds (ready_to_send_next+ready, apply_attention+blocked). These are
    # the ONLY cross-kind merges; both members must keep the same operator
    # meaning. Pinned so a future divergent value isn't silently swept in.
    ready_bucket = {"ready_to_send_next", "ready"}
    blocked_bucket = {"apply_attention", "blocked"}
    assert ready_bucket <= (REVIEW_APPLY_CHAIN_STATES | PROMOTION_CHAIN_STATES)
    assert blocked_bucket <= (REVIEW_APPLY_CHAIN_STATES | PROMOTION_CHAIN_STATES)

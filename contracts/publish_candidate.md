# Contract: `publish_candidate`

## Purpose

Represent one pre-publish artifact DuckAgent already generated so OpenClaw can decide whether it is ready, weak, or should be discarded.

## Pilot sources

- `newduck` approval emails
- `weekly sale playbook` review emails
- `meme` social publish packages
- `jeepfact` social carousel packages
- `thursday` option-specific vote packages

## Required fields

- `artifact_id`
- `artifact_type`
- `flow`
- `run_id`
- `source_refs`
- `candidate_summary`
- `supporting_context`

## Decision labels

- `publish_ready`
- `needs_revision`
- `discard`

## Flow-specific review contract

OpenClaw may attach `quality_gate_metadata.flow_review_contract` when a flow needs a production reviewer rather than a generic weighted score. The contract is intentionally structured so DuckAgent and the Decision Inbox can show exactly why an item is ready, blocked, or ready with warnings.

Canonical builder: `runtime/flow_review_contract.py`. New flow reviewers should use `build_flow_review_contract(...)` and `flow_review_check(...)` instead of hand-building this dictionary.

Required shape:

- `schema_version`
- `reviewer`
- `hard_blockers`
- `warnings`
- `checks`
- `operator_summary`
- `approval_summary`
- `recommended_action`
- `operator_actions`

For `meme`, reviewer `meme_publish_package` treats missing image, missing caption, missing platform payload, stale schedule, or email/CSS wrapper noise as blockers. Thin trend/source support is a non-blocking warning when the final meme image, caption, platform payload, and timing checks pass.

For `jeepfact`, reviewer `jeepfact_carousel_package` treats missing carousel slides, missing caption, missing platform payload, stale schedule, missing Jeep Fact framing, or email/CSS wrapper noise as blockers. Thin trend/source support, a shorter-than-expected slide set, partial platform captions, or a near-stale Wednesday slot are non-blocking warnings when the carousel package itself is reviewable.

For `thursday`, reviewer `thursday_vote_package` treats missing vote image, missing caption, missing platform payload, stale schedule, missing vote framing, option-review failures, dirty vote labels, or email/CSS wrapper noise as blockers. Option-review warnings and thin trend/source support are non-blocking warnings when the actual vote image, caption, option id, and labels are reviewable.

## Key normalization rules

- parse email artifacts into structured summaries
- keep original subject or message reference as evidence
- attach trend and catalog context whenever available
- preserve multiple platform variants inside one candidate when needed

## Fail-closed rule

If the artifact is materially incomplete, weakly supported, unclear, or unjustifiably duplicative, it must not be marked `publish_ready`.

# OpenClaw Phase 2 Implementation Checklist

## Purpose

This checklist turns the roadmap into an actionable implementation sequence.

Use it as the build order for the passive OpenClaw intelligence layer that sits beside DuckAgent.

Primary rule:

- DuckAgent remains the execution system.
- OpenClaw becomes the evaluation, prioritization, and control system.

## Locked Decisions

These decisions are already made and should not be reopened unless something breaks badly in implementation.

- [x] OpenClaw is passive and read-only in Phase 2.
- [x] DuckAgent cron flows remain unchanged.
- [x] No required DuckAgent code changes for initial implementation.
- [x] OpenClaw does not publish, reply, or execute actions in Phase 2.
- [x] Mailbox access is read-only.
- [x] Notifications use a standalone notifier, not DuckAgent's mail helper.
- [x] Notification model is daily digest plus one-off urgent alerts.
- [x] Human overrides require a note.
- [x] Pilot starts with `quality_gate`.
- [x] Pilot scope starts with `newduck`, `weekly sale playbook`, and the daily `reviews` quality layer.

## Global Guardrails

- [ ] Do not add OpenClaw callouts inside DuckAgent runtime flows.
- [ ] Do not require DuckAgent to stage files for OpenClaw.
- [ ] Do not add Shopify, Etsy, or Gmail send credentials to OpenClaw for Phase 2.
- [ ] Do not let OpenClaw create new trends from web browsing alone.
- [ ] Do not let OpenClaw reply to approval emails or customer emails in Phase 2.
- [ ] Do not expand to multi-channel operations during the pilot.

## Phase 0 Checklist: Design Freeze

- [x] Confirm the roadmap file is the source of truth:
  - `/Users/philtullai/ai-agents/duck-ops/OPENCLAW_PHASE2_ROADMAP.md`
- [x] Confirm this checklist file is the implementation source of truth:
  - `/Users/philtullai/ai-agents/duck-ops/OPENCLAW_PHASE2_IMPLEMENTATION_CHECKLIST.md`
- [x] Freeze the three artifact contracts:
  - `trend_candidate`
  - `publish_candidate`
  - `customer_signal`
- [x] Freeze the three evaluator rulesets:
  - `trend_ranker`
  - `quality_gate`
  - `customer_intelligence`
- [x] Freeze the notification policy:
  - daily digest
  - urgent alerts
  - weekly phase-readiness report
- [x] Freeze the trust policy:
  - confidence caps
  - fail-closed defaults
  - mandatory human review cases

### Phase 0 acceptance

- [x] All core design choices are documented in the roadmap.
- [x] No unresolved architecture questions remain for the pilot.

## Phase 1 Checklist: Observation Foundation

### 1. Workspace and state layout

- [x] Create or confirm OpenClaw workspace folders:
  - `duck-ops/config/`
  - `duck-ops/state/`
  - `duck-ops/output/trend_rankings/`
  - `duck-ops/output/quality_gates/`
  - `duck-ops/output/customer_intelligence/`
  - `duck-ops/output/digests/`
  - `duck-ops/contracts/`
- [x] Create initial OpenClaw-owned state files:
  - `artifact_registry.jsonl`
  - `decision_history.jsonl`
  - `outcome_history.jsonl`
  - `entity_memory.json`
  - `watchlists.json`
  - `overrides.jsonl`
  - `calibration.json`

### 2. Source observer configuration

- [x] Create a source configuration file for:
  - DuckAgent `cache/`
  - DuckAgent `runs/`
  - DuckAgent `logs/`
  - read-only mailbox source
- [x] Define polling intervals for:
  - file sources
  - mailbox source
  - weekly readiness generation
- [x] Define stable content hashing rules.
- [x] Record `first_seen_at`, `last_seen_at`, and `content_hash` per artifact.

### 3. Mailbox observation

- [x] Configure read-only IMAP access to the existing mailbox.
- [ ] If read-only IMAP is not acceptable, switch to mirrored Maildir or message export.
- [x] Verify OpenClaw can observe:
  - DuckAgent approval emails
  - review-related emails
  - inbound customer or platform emails already arriving in the mailbox

### 4. Artifact detection

- [x] Detect new files in DuckAgent `runs/`.
- [x] Detect changed files in DuckAgent `cache/`.
- [x] Detect new relevant emails.
- [x] Prevent duplicate evaluation of unchanged artifacts.

### 5. Normalization foundation

- [x] Implement `trend_candidate` normalization.
- [x] Implement `publish_candidate` normalization.
- [x] Implement `customer_signal` normalization.
- [x] Preserve source references in every normalized artifact.
- [x] Cap confidence when input is partial or weak.

### 6. Base decision writer

- [x] Implement JSON decision writing.
- [x] Implement Markdown decision-summary writing.
- [x] Ensure filenames follow the roadmap naming scheme.

### Phase 1 acceptance

- [x] OpenClaw can observe new DuckAgent artifacts without changing DuckAgent.
- [x] Duplicate processing is controlled.
- [x] Normalized artifacts are written consistently.
- [x] Decision files are readable and auditable.
- [x] Digest generation can work even before email sending is enabled.

## Phase 2 Checklist: Quality Gate Pilot

### 1. Pilot scope selection

- [x] Confirm `newduck` as the first pilot flow.
- [x] Confirm `weekly sale playbook` as the second pilot flow.
- [x] Add the daily `reviews` summary as the first high-frequency pilot surface.

### 2. Publish candidate ingestion

- [x] Normalize `newduck` review emails into `publish_candidate`.
- [x] Normalize `weekly sale playbook` review emails into `publish_candidate`.
- [x] Normalize daily Etsy review summary emails into `publish_candidate` records for:
  - review-story publish checks
  - 5-star public reply drafts
  - low-rating private recovery drafts
- [x] Attach supporting trend and catalog context from:
  - `weekly_insights.json`
  - `product_recommendations.json`
  - `products_cache.json`
  - `publication_cache.json`
  - latest `state_competitor.json` when useful

### 3. Quality gate evaluator

- [x] Score each pilot artifact using the `quality_gate` ruleset.
- [x] Emit:
  - `publish_ready`
  - `needs_revision`
  - `discard`
- [x] Include:
  - score
  - confidence
  - priority
  - evidence bullets
  - exact revision suggestions when relevant
- [x] Apply fail-closed rules before any final decision.

### 4. Digest and urgent alert generation

- [x] Generate one daily digest when there are new decisions or unresolved blocked items.
- [x] Generate urgent alerts only when threshold rules are met.
- [x] Keep urgent alert volume intentionally low.

### 5. Override and review loop

- [x] Record human decisions as:
  - `approve`
  - `reject`
  - `override`
- [x] Require a note for every override.
- [x] Store override notes in `overrides.jsonl`.

### 6. Weekly phase-readiness reporting

- [ ] Generate weekly:
  - `phase_readiness__YYYY-WW.json`
  - `phase_readiness__YYYY-WW.md`
- [ ] Emit:
  - `ready_to_advance`
  - `stay_in_current_phase`
  - `blocked`
- [ ] Trigger a one-off alert if readiness becomes `ready_to_advance`.

### 7. Runtime scheduling

- [x] Create a standalone sidecar runner that executes:
  - `phase1_observer.py`
  - `quality_gate_pilot.py`
  - `review_loop.py queue`
  - `notifier.py`
- [x] Add a simple lock so overlapping sidecar runs do not pile up.
- [x] Install a macOS `launchd` job for recurring sidecar execution.
- [x] Write scheduler logs outside DuckAgent and outside OpenClaw core files.
- [x] Harden the WhatsApp operator bridge against transcript replay:
  - stop relying on a 500-message processed-id ring alone
  - track the last processed transcript message id as a durable cursor
  - persist progress after each handled message so old commands cannot be replayed after a backlog or crash

### Phase 2 pilot acceptance

- [ ] The quality gate catches weak candidates that matter to you.
- [ ] `needs_revision` and `discard` outputs feel directionally right.
- [ ] Daily digests are useful and not noisy.
- [ ] Urgent alerts remain rare and justified.
- [ ] Overrides show repeatable patterns, not random disagreement.
- [ ] DuckAgent remains unchanged and operational.
- [ ] At least 2 to 4 weeks of pilot history are collected before expansion.

## Phase 3 Checklist: Trend Ranking

### 1. Trend candidate ingestion

- [x] Normalize trend inputs from:
  - latest `state_competitor.json`
  - `weekly_insights.json`
  - `product_recommendations.json`
  - `reddit_signal_history.json` when useful
- [x] Join catalog context from:
  - `products_cache.json`
  - `publication_cache.json`

### 2. Trend evaluator

- [x] Score each `trend_candidate` with the `trend_ranker` ruleset.
- [x] Emit:
  - `worth_acting_on`
  - `watch`
  - `ignore`
- [x] Add action framing:
  - `promote`
  - `build`
  - `wait`
  - `ignore`
- [ ] Use web corroboration only for existing DuckAgent trend candidates.

### 3. Trend outputs

- [x] Write trend decision files under:
  - `output/trend_rankings/`
- [x] Include:
  - whether catalog already covers the trend
  - whether action should be promotion or build
  - urgency and confidence
- [x] Write a daily trend digest under:
  - `output/digests/trend_digest__YYYY-MM-DD.md`
  - `output/digests/trend_digest__YYYY-MM-DD.json`
- [x] Carry forward reviewed `wait / build / promote / ignore` decisions across semantically-close sibling trend variants when the catalog match and recommendation stay effectively the same.
- [x] Keep `watch + wait` trends out of the operator queue by default unless they were explicitly touched by the operator or materially changed after a previous review.
- [x] Treat silent `watch + wait` trends as background monitoring, not pending operator work.
- [x] Build a concept layer over raw trend artifacts so the operator queue and digest use one canonical trend concept instead of one queue item per daily artifact.
- [x] Keep the digest aligned with the operator queue:
  - operator-facing `build / promote` trends and revisit-worthy `wait` items count as pending review
  - silent background watches are counted separately
- [x] Add the trend evaluator to the scheduled sidecar loop.

### Phase 3 acceptance

- [ ] Trend ranking adds signal beyond DuckAgent’s raw recommendations.
- [ ] `promote` versus `build` decisions feel more disciplined.
- [ ] Trend operator noise stays low because background `wait` items remain silent unless they change.
- [ ] 7-day and 30-day attribution windows can be applied consistently.

## Optional Phase 3.5 Checklist: Operator Channel And Social Review

### 1. Operator channel

- [ ] Add only one operator channel if email becomes limiting.
- [ ] Restrict channel use to:
  - urgent alerts
  - overrides
  - readiness alerts

### 2. Browser-based social review

- [ ] Add browser-based observation only for post-performance enrichment.
- [ ] Review visible:
  - engagement context
  - comments
  - post presentation
- [ ] Do not use browser review to create new trends by itself.

### Optional Phase 3.5 acceptance

- [ ] The additional channel is genuinely more useful than email alone.
- [ ] Browser-based social review improves post feedback quality.
- [ ] Maintenance overhead stays low.

## Optional Phase 3.6 Checklist: Review Reply Execution

Current status on 2026-04-05:

- [x] One-time browser-path approval is complete.
- [x] Manual dry-run fill and manual live submit are validated.
- [x] Session-batched success confirmation is live.
- [x] Auto-queue and scheduled queue draining are implemented.
- [x] The executor now targets Etsy's paginated `/shop/.../reviews?page=N` surface and can recover the exact review row when the current page is wrong.
- [x] A queued `publish_ready` reply has been auto-recovered after auth was restored and posted successfully on the correct Etsy review row.
- [x] Exact-row-not-found failures are now treated as retryable when the seller session is still signed in.
- [x] Retryable row misses now probe a wider review-page window before giving up.
- [x] Auth/session loss is now handled as a queue-level pause with one rate-limited auth-required alert instead of cascading per-item failures.
- [x] Etsy seller-session auth state is now saved to `duck-ops` and restored on session reopen or signed-out recovery before manual sign-in is required.

Residual risk on 2026-04-05:

- Etsy can still expire the saved seller session itself, and seller-surface DOM drift can still break execution.
- In those cases the executor should still fail closed, pause the queue, and send one auth-required alert instead of posting incorrectly.

Soak gate before moving to the next step:

- [ ] Soak the executor through 48 hours of hourly sidecar runs, ending no earlier than `2026-04-07`.
- [ ] `Review Execution` shows `Auth = healthy` and `Saved Auth = available` throughout the soak except during a real Etsy expiry.
- [ ] No repeated auth-alert spam is sent for the same sign-out event.
- [ ] At least one browser reopen or signed-out recovery succeeds from saved auth without a manual Etsy sign-in.
- [ ] If a real `publish_ready` reply appears during the soak, the sidecar auto-queues it and drains it without manual help.
- [ ] Any failure still fails closed:
  - no wrong review row
  - no duplicate post
  - queue pauses cleanly
  - one clear alert

### 1. Scope freeze

- [x] Restrict first execution scope to Etsy 5-star public review replies only.
- [x] Keep low-rating private recovery replies out of execution scope.
- [x] Keep review-story posting out of execution scope.
- [x] Confirm the executor will only act on replies DuckAgent already drafted.

### 2. Review execution contract

- [x] Extend review reply decision artifacts with:
  - `execution_mode`
  - `approved_reply_text`
  - `review_target`
  - `execution_state`
  - `operator_resolution`
- [x] Ensure every executable review reply carries a stable target identifier set.
- [x] Ensure execution packets carry the exact reply text that was approved.
- [x] Ensure execution packets can be audited without reading the mailbox again.

### 3. Operator policy

- [x] If decision is `publish_ready`, allow `auto` or `manual_only` execution mode per policy.
- [x] The WhatsApp operator bridge ignores its own outbound replies by remembered outbound-message hash instead of brittle per-command text matching.
- [x] The WhatsApp operator bridge reads a fixed transcript snapshot per pass so one run cannot chase its own freshly appended self-echo lines.
- [x] Operator cards now explicitly say what approving the item will do, so social-post approvals are clearly distinct from customer-reply approvals.
- [x] Weekly sale operator cards now summarize the actual sale targets and discounts from DuckAgent's structured sale playbook.
- [x] If decision is `needs_revision` or `discard`, send the recommendation to the operator channel first.
- [x] If the operator responds with `agree`, record agreement with the recommendation.
- [x] If the operator responds with `approve`, enqueue the exact approved reply text for execution.
- [x] Keep operator notes on exceptions and overrides.
- [x] The operator lane can return a concrete `rewrite` suggestion for weak review replies.
- [x] Weekly sale playbook cards now say when OpenClaw mainly thinks the artifact is incomplete / too vague rather than strategically wrong.
- [x] Weekly sale `suggest changes` and `rewrite` now return a tightened sale-plan version instead of only generic feedback.
- [x] DuckAgent weekly sale playbooks now generate a deterministic `approval_summary`, normalize discount/platform formatting, and resolve Shopify product IDs before review.
- [x] OpenClaw now prefers DuckAgent's structured weekly-sale `approval_summary` from `state_weekly.json` over the one-line strategic summary when building publish candidates.

### 4. Deterministic Etsy executor

- [x] Use a fixed signed-in Etsy browser session or profile owned by the operator.
- [x] Persist the Etsy seller-session auth state so unattended drains can restore it when the browser session is reopened.
- [x] Open the exact review target using the strongest available identifier.
- [x] Verify review text or transaction context before entering any reply.
- [x] Paste the exact approved reply text without regenerating it.
- [x] Submit only after target verification passes.
- [x] Detect and record a successful post.
- [ ] Detect and record:
- [x] Detect and record:
  - already replied
  - target not found
  - selector drift
  - auth/session failure

### 5. Browser discovery packet and approval gate

- [x] Build a read-only discovery mode before any live submit mode exists.
- [x] Capture a browser discovery packet with:
  - Etsy page URL or path
  - step-by-step screenshots
  - matched review text and contextual identifiers
  - selector or page-anchor plan
  - reply-box proof
  - proof that submit was not clicked
- [x] Strengthen discovery verification so a browser match is only treated as exact when the matched review block also exposes the expected Etsy listing ID.
- [x] Prefer Etsy transaction-ID targeting (`data-review-region` / `data-transaction-id`) over text-only review matching when locating the live review block.
- [x] Provide a reusable Etsy seller auth-browser handoff for the exact automation session discovery uses.
- [x] Add a dry-run interaction mode that can verify the review and optionally paste text without submitting.
- [x] Expose a browser review surface that shows the latest discovery packet, one-time approval state, and current public review-reply items.
- [x] Require explicit operator review of the discovery packet before enabling live submit.
- [x] Fail the phase if discovery cannot prove a safe, repeatable targeting path.

### 6. Execution queue and state

- [x] Add an execution queue for approved review replies.
- [x] Auto-queue `publish_ready` Etsy public review replies when browser-path approval and policy allow it.
- [x] Drain queued review replies from the hourly sidecar and batch success confirmation into one session summary email.
- [x] Track:
  - `queued`
  - `running`
  - `posted`
  - `failed`
  - `skipped`
- [x] Record every execution attempt with timestamp and outcome.
- [x] Prevent duplicate posting attempts for the same review unless explicitly retried.

### 7. Notifications and confirmation

- [x] Send operator confirmation after a reply is posted successfully.
- [x] Send operator failure alerts when execution fails and needs human intervention.
- [x] Include enough context in failure alerts to retry manually if needed.

### Optional Phase 3.6 acceptance

- [x] Discovery mode proves the correct review target without submitting anything.
- [x] The operator has approved the discovery packet before live submit is enabled.
- [x] One controlled live submit has targeted the correct Etsy review and posted successfully.
- [x] `publish_ready` public replies can reuse the approved browser path without a fresh approval on every new review.
- [x] A formerly failed signed-in row-miss has been re-run successfully with the widened probe and dry-run fill path.
- [ ] The executor always targets the correct Etsy review.
- [ ] Auto-posted `publish_ready` replies feel safe enough not to re-check each time.
- [ ] Operator-approved exceptions are logged cleanly.
- [x] Failure cases are visible and recoverable.
- [ ] Maintenance overhead stays low enough to keep the executor enabled.

## Phase 4 Checklist: Customer Intelligence

### 1. Customer signal ingestion

- [ ] Normalize customer signals from:
  - `state_reviews.json`
  - DuckAgent review emails
  - inbound customer or platform emails already visible in the mailbox

### 2. Customer evaluator

- [ ] Score each `customer_signal` with the `customer_intelligence` ruleset.
- [ ] Emit:
  - `reply_now`
  - `watch`
  - `escalate`
  - `needs_human_context`
- [ ] Include:
  - risk level
  - suggested business action
  - draft reply when safe
  - escalation reason when not safe

### 3. Draft handoff

- [ ] Write reply drafts as:
  - Markdown packet
  - JSON packet
- [ ] Include draft replies in digest or owner-facing alerting as appropriate.
- [ ] Keep final sending manual or DuckAgent-mediated.

### Phase 4 acceptance

- [ ] Drafts reduce response effort.
- [ ] Escalations are clear and safe.
- [ ] No unsafe autonomous replies exist.
- [ ] Customer recommendations align with policy.

## Phase 5 Checklist: Feedback And Calibration

### 1. Outcome tracking

- [ ] Record whether advice was followed:
  - `yes`
  - `no`
  - `partial`
- [ ] Record outcome quality:
  - `positive`
  - `neutral`
  - `negative`
- [ ] Record baseline comparison:
  - `better`
  - `flat`
  - `worse`

### 2. Attribution windows

- [ ] Apply 72-hour and 7-day windows for social content.
- [ ] Apply 7-day and 30-day windows for promotion decisions.
- [ ] Apply 14-day and 30-day windows for new ducks and listings.
- [ ] Apply 7-day and 30-day windows for trend validation.
- [ ] Apply 48-hour and 14-day windows for customer issues.

### 3. Calibration

- [ ] Update confidence and threshold behavior based on:
  - false positives
  - false negatives
  - override history
  - repeated outcome patterns
- [ ] Keep calibration lightweight.
- [ ] Do not introduce model fine-tuning at this stage.

### Phase 5 acceptance

- [ ] OpenClaw decisions show measurable improvement over time.
- [ ] Thresholds are being tuned from evidence, not opinion.
- [ ] The maintenance cost remains worth the benefit.

## Notification Checklist

### Standalone notifier

- [x] Build notifier separate from DuckAgent and separate from OpenClaw core logic.
- [x] Feed notifier from OpenClaw output files only.
- [x] Support:
  - daily digest email
  - urgent alert email
  - weekly phase-readiness alert

### Daily digest rules

- [x] Send a digest only if there is new information or unresolved blocked items.
- [x] Keep digest concise and decision-focused.
- [x] Daily digests now separate `new this run` from `still pending review` and stop listing already-reviewed items as if they were fresh decisions.
- [ ] Include unresolved clarification requests.

### Urgent alert rules

- [ ] Alert on:
  - urgent customer escalation
  - severe quality-gate failure on near-term publish candidate
  - high-confidence short-window trend opportunity
  - urgent blocked decision
  - `ready_to_advance`
- [ ] Review alert rate regularly and tighten thresholds if alerts become common.

## Phase Advancement Checklist

### Move from Phase 1 to Phase 2 when

- [ ] observation is stable
- [ ] duplicate processing is controlled
- [ ] decision artifacts are consistently written

### Move from Phase 2 pilot to Phase 3 when

- [ ] the quality gate is trusted enough to keep running
- [ ] digest and urgent alerts feel operationally useful
- [ ] override notes show patterns rather than randomness

### Move from Phase 3 to optional Phase 3.5 when

- [ ] email-only operation feels limiting
- [ ] browser-visible post review would materially help
- [ ] one operator channel would reduce friction

### Move from optional Phase 3.5 to optional Phase 3.6 when

- [ ] the operator channel is already useful for reviews
- [ ] review-reply recommendations feel trustworthy enough to consider execution
- [ ] approved Etsy public replies would save meaningful time
- [ ] the first execution scope will stay narrow

### Move from Phase 3 to Phase 4 when

- [ ] trend ranking adds value beyond DuckAgent’s native outputs
- [ ] attribution can judge trend decisions directionally

### Move from optional Phase 3.5 to Phase 4 when

- [ ] the optional layer proved useful
- [ ] maintenance stayed reasonable
- [ ] browser or channel use improved outcomes or operator speed

### Move from optional Phase 3.6 to Phase 4 when

- [ ] reply execution is reliable
- [ ] execution outcomes add useful calibration signal
- [ ] browser maintenance remains low enough to keep the executor enabled

### Move from Phase 4 to Phase 5 when

- [ ] customer outputs are useful and safe
- [ ] escalation handling is clear
- [ ] drafts reduce effort without raising risk

### Move beyond Phase 5 only when

- [ ] OpenClaw materially improves decision quality
- [ ] noise stays low
- [ ] maintenance burden remains acceptable

## Future Breadcrumb Checklist

These are explicitly later and should not block the pilot.

- [ ] Add one operator channel if email becomes limiting.
- [ ] Add browser-based social review if post feedback needs richer evidence.
- [ ] Add Shopify or Etsy support-thread exports if customer intelligence proves valuable.
- [ ] Add a decision portfolio view if artifact volume grows.
- [ ] Consider tightly constrained structured handoffs back to DuckAgent only after strong scorecard evidence.

## Do Not Do These During Initial Implementation

- [ ] Do not change DuckAgent cron jobs.
- [ ] Do not make OpenClaw a second recommendation engine that duplicates DuckAgent.
- [ ] Do not let OpenClaw send approval replies.
- [ ] Do not let OpenClaw send customer replies.
- [ ] Do not add direct Shopify or Etsy action execution.
- [ ] Do not broaden into multi-agent orchestration.

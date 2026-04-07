# OpenClaw Passive Intelligence Layer Plan For DuckAgent

## Objective

Use OpenClaw as a passive, read-only intelligence layer beside DuckAgent.

DuckAgent remains the system of execution.

OpenClaw becomes the system of:

- evaluation
- prioritization
- filtering
- decision support
- feedback and calibration

OpenClaw should improve decision quality without becoming another operational system to maintain.

## Non-Negotiable Boundaries

These rules should drive every design choice:

- Do not modify DuckAgent's current cron-driven flow.
- Do not make DuckAgent depend on OpenClaw to complete a run.
- Do not push new runtime data into OpenClaw as a required pipeline step.
- Do not duplicate DuckAgent's collectors, platform integrations, or publish logic.
- Do not let OpenClaw execute customer-facing or public-facing actions in Phase 2.
- Do not overbuild a multi-agent system for what is fundamentally a read, score, and decide problem.

## System Role Split

### DuckAgent responsibilities

DuckAgent continues to own:

- Shopify, Etsy, Reddit, and report collection
- cron jobs and scheduling
- per-run state in `runs/`
- durable summaries in `cache/`
- email generation and delivery
- publish execution
- existing AI generation workflows

### OpenClaw responsibilities

OpenClaw should own:

- reading existing DuckAgent artifacts in read-only mode
- normalizing structured and unstructured artifacts into comparable records
- scoring and ranking trends DuckAgent already found
- acting as a strict quality gate for listings, posts, and recommendations
- analyzing customer reviews and inbound messages for risk and response guidance
- maintaining decision history and outcome calibration over time

## Passive Observation Model

OpenClaw should behave like a continuous observer, not a pipeline stage.

That means:

- It scans existing file locations and mailbox sources on a schedule.
- It detects new or changed artifacts using file path, timestamp, and content hash.
- It keeps its own state in OpenClaw-owned files only.
- It never writes back into DuckAgent folders.
- It never becomes a required upstream dependency for DuckAgent.

Preferred runtime model:

- DuckAgent directories are mounted or exposed to OpenClaw in read-only form.
- Email is accessed through a read-only mailbox view or mirrored Maildir export.
- OpenClaw writes only to its own workspace under `duck-ops/`.

## Source Inventory

OpenClaw should start by reading the DuckAgent artifacts already known to exist.

### Trend and recommendation sources

- `/Users/philtullai/ai-agents/duckAgent/cache/weekly_insights.json`
- `/Users/philtullai/ai-agents/duckAgent/cache/product_recommendations.json`
- `/Users/philtullai/ai-agents/duckAgent/cache/products_cache.json`
- `/Users/philtullai/ai-agents/duckAgent/cache/publication_cache.json`
- `/Users/philtullai/ai-agents/duckAgent/cache/reddit_signal_history.json`
- latest `/Users/philtullai/ai-agents/duckAgent/runs/<run_id>/state_competitor.json`

### Publish candidate sources

For publish gating, the first passive source should be the approval email stream DuckAgent already creates.

Use these before trying to add new collectors:

- `newduck` review emails described in `/Users/philtullai/ai-agents/duckAgent/flows/newduck/steps.py`
- weekly sale or content review emails already sent by DuckAgent
- `gtdf`, `thursday`, `meme`, and related approval emails when present
- run-state files like `/Users/philtullai/ai-agents/duckAgent/runs/<run_id>/state_newduck.json` if they exist
- image and asset metadata already written under `/Users/philtullai/ai-agents/duckAgent/runs/`

Important design choice:

- Use email as the primary pre-publish observation surface when no stable state file exists.
- Use state files as a higher-quality source whenever DuckAgent already emits them.

### Customer intelligence sources

Use what already exists first:

- `/Users/philtullai/ai-agents/duckAgent/runs/<run_id>/state_reviews.json`
- Etsy review collection and response generation logic in `/Users/philtullai/ai-agents/duckAgent/flows/reviews/steps.py`
- Etsy review helper and low-rating follow-up generation in `/Users/philtullai/ai-agents/duckAgent/flows/reviews/etsy_review_helper.py`
- inbound customer or notification emails already landing in the DuckAgent mailbox

Current known limitation:

- Direct Shopify support threads are not yet available as a normalized artifact in DuckAgent.
- Etsy conversation threads are also not yet exposed as a normalized artifact.

That means customer intelligence should start with:

- Etsy reviews
- Etsy low-rating follow-up cases
- Shopify or Etsy customer emails only if they already reach the monitored mailbox

## OpenClaw Components To Add

These changes belong on the OpenClaw side, not in DuckAgent.

### 1. Source observer configuration

Create a source map that tells OpenClaw where to read from:

- DuckAgent cache directory
- DuckAgent runs directory
- DuckAgent logs directory
- read-only mailbox source
- optional web corroboration targets

This should be declarative and easy to replace.

### 2. Artifact normalizers

OpenClaw needs lightweight parsers that convert source artifacts into a small set of internal record types:

- `trend_candidate`
- `publish_candidate`
- `customer_signal`
- `outcome_signal`

This is the key modular boundary.

DuckAgent artifacts can vary over time.
OpenClaw should absorb that variation at the normalizer layer so the evaluators stay stable.

### 3. Evaluators

OpenClaw should run three evaluators:

- `trend_ranker`
- `quality_gate`
- `customer_intelligence`

Each evaluator should produce structured outputs with:

- decision
- confidence
- score
- priority
- reasoning
- improvement suggestions when relevant

### 4. State and memory

OpenClaw should keep its own local files for:

- observed artifact registry
- decision history
- entity memory
- watchlists
- outcome history
- human overrides
- evaluator calibration

### 5. Output writers

OpenClaw should write machine-readable files first and human-readable summaries second.

Primary outputs:

- JSON decision artifacts
- Markdown review summaries
- daily and weekly digest files

## Minimal Change Strategy For DuckAgent

The design should assume zero required DuckAgent changes for initial Phase 2.

### Required DuckAgent changes for Phase 2 start

- None

### Strongly preferred but optional later

- Preserve stable draft artifacts for workflows that currently only exist in email form.
- Standardize email subjects to always include flow, run ID, and artifact type.
- Save final pre-publish candidate payloads to a stable run-state file when convenient.
- Expose support-thread exports only if customer reply drafting later proves valuable enough.

### What should stay untouched

- DuckAgent cron schedule
- DuckAgent collector logic
- DuckAgent email sending flow
- DuckAgent publish logic
- DuckAgent credentials and platform integrations

The default assumption should be:

- If OpenClaw needs a DuckAgent change to exist at all, that change is probably too invasive for Phase 2.

## Decision Outputs

Every evaluator output should follow the same base contract.

```json
{
  "artifact_id": "stable-id",
  "artifact_type": "trend|listing|post|customer",
  "decision": "worth_acting_on|watch|ignore|publish_ready|needs_revision|discard|reply_now|escalate",
  "score": 0,
  "confidence": 0.0,
  "priority": "low|medium|high|urgent",
  "reasoning": [],
  "improvement_suggestions": [],
  "evidence_refs": [],
  "review_status": "pending|approved|rejected|overridden",
  "created_at": "timestamp"
}
```

The exact decision label can vary by evaluator, but the shape should not.

## Capability Design

### A. Trend Ranking

OpenClaw should not discover trends from scratch.
It should evaluate DuckAgent's trend findings and decide whether they are worth acting on.

Inputs:

- DuckAgent trend and recommendation outputs
- catalog state
- publication state
- optional web corroboration
- historical outcomes of similar trends

Scoring dimensions:

- signal strength from DuckAgent
- persistence across time windows
- cross-source corroboration
- catalog overlap or gap
- execution feasibility
- historical hit rate of similar ideas

Required outputs:

- `worth_acting_on`
- `watch`
- `ignore`

Required reasoning:

- why this is real or weak
- whether the catalog already covers it
- whether action should be promote, build, or wait

### B. Output Quality Gate

This is the highest-priority evaluator.

OpenClaw should judge whether generated outputs are fit to use before a human acts on them.

Targets:

- listing drafts
- post drafts
- new duck ideas
- weekly sale or promotion recommendations

Scoring dimensions:

- trend support
- brand fit
- clarity and specificity
- differentiation versus existing catalog
- likely conversion quality
- timing
- risk or quality concerns

Required decisions:

- `publish_ready`
- `needs_revision`
- `discard`

Required reasoning:

- what is strong
- what is weak
- what exact revision would improve it
- why it should not be used if discarded

Strict behavior requirement:

- This evaluator should fail closed.
- Borderline artifacts should not be treated as good enough.

### C. Customer Intelligence

OpenClaw should not start as a customer-service bot.
It should start as an analyst and draft assistant.

Targets:

- Etsy review follow-ups
- low-rating issue detection
- customer confusion signals from inbound emails
- refund, replacement, or escalation opportunities

Scoring dimensions:

- dissatisfaction risk
- urgency
- business impact
- recoverability
- policy fit

Required outputs:

- risk level
- suggested action
- draft reply
- escalation flag
- reason for the recommendation

Important boundary:

- No autonomous sending in Phase 2.

## Evaluator Rulesets

This section turns the evaluator concepts into concrete scoring behavior.

The goal is not mathematical perfection.
The goal is consistency, conservatism, and auditability.

### Rule format

Each evaluator should produce:

- a numeric score from 0 to 100
- a confidence from 0.00 to 1.00
- a priority tier
- a decision label
- concise evidence bullets

The numeric score is for comparability across similar artifacts.
The decision label is what the operator should act on.

### 1. `trend_ranker` ruleset

#### Score components

Use these weighted components:

- commercial signal strength: 30
- persistence over time: 20
- cross-source corroboration: 15
- catalog gap or coverage clarity: 15
- execution feasibility: 10
- historical hit rate of similar themes: 10

#### Scoring guidance

Commercial signal strength should reward:

- sold counts
- quantity drops
- repeated appearance in competitor outputs

Commercial signal strength should penalize:

- engagement-only spikes with no demand evidence

Persistence should reward:

- recurrence across multiple days
- recurrence across 7-day and 30-day windows

Cross-source corroboration should reward:

- competitor plus weekly insights agreement
- competitor plus Reddit agreement
- optional web corroboration only when DuckAgent already surfaced the theme

Catalog gap or coverage clarity should reward:

- clear gap with strong fit
- partial coverage where promotion or a variant could work

Execution feasibility should reward:

- themes you can make quickly
- themes that fit current product families

Historical hit rate should reward:

- similar themes that previously converted well

#### Decision thresholds

- `worth_acting_on`: score 75+
- `watch`: score 45-74
- `ignore`: score below 45

#### Priority thresholds

- `urgent`: score 85+ with short-window opportunity
- `high`: score 75-84
- `medium`: score 55-74
- `low`: score below 55

#### Fail-closed rules

Do not emit `worth_acting_on` if:

- there is no meaningful commercial signal
- the theme appears only once with no corroboration
- catalog matching is unknown and the signal is weak

#### Web corroboration rule

Web corroboration is allowed only to strengthen or weaken an existing DuckAgent trend candidate.

It should not create a new trend candidate by itself in Phase 2.

### 2. `quality_gate` ruleset

This evaluator should be the harshest of the three.

#### Score components

Use these weighted components:

- trend or campaign support: 20
- brand fit: 20
- clarity and specificity: 15
- differentiation versus current catalog: 15
- likely conversion quality: 15
- timing fit: 10
- risk penalties: 5

#### Scoring guidance

Trend or campaign support should reward:

- clear strategic reason for the artifact to exist
- evidence from current trend or campaign context

Brand fit should reward:

- consistency with your duck business style and audience

Clarity and specificity should reward:

- clear value proposition
- clear visual and thematic coherence

Differentiation should reward:

- enough novelty to avoid creating redundant ducks or repetitive posts

Likely conversion quality should reward:

- a strong product or content hook
- obvious consumer appeal

Timing fit should reward:

- seasonal fit
- trend timing
- campaign timing

Risk penalties should apply for:

- sloppy copy
- vague idea framing
- duplicate feeling
- weak supporting evidence
- brand awkwardness

#### Decision thresholds

- `publish_ready`: score 82+
- `needs_revision`: score 55-81
- `discard`: score below 55

#### Priority thresholds

- `urgent`: only for severe negative findings on something likely to be acted on soon
- `high`: strong candidate requiring attention today
- `medium`: useful but not time-critical
- `low`: informational only

#### Fail-closed rules

Do not emit `publish_ready` if any of the following are true:

- the artifact is materially incomplete
- the idea is weakly supported
- the artifact feels duplicative without a clear reason
- the copy or framing is unclear
- timing is wrong enough to undermine likely performance

Borderline cases should become `needs_revision`, not `publish_ready`.

#### Competitor evaluation relevance

Competitor inputs should matter here only as context.

The quality gate should not approve a weak artifact just because a competitor signal exists.

#### Social-post feedback loop

For social posts, quality-gate review should later be compared against:

- 72-hour engagement
- 7-day engagement
- comment quality
- downstream product activity when visible

That post-performance feedback should tune future scoring but should not alter the original stored decision.

### 3. `customer_intelligence` ruleset

This evaluator should prioritize safety over speed.

#### Score components

Use these weighted components:

- dissatisfaction severity: 30
- urgency: 20
- business impact: 15
- recoverability: 15
- policy clarity: 10
- context completeness: 10

#### Scoring guidance

Dissatisfaction severity should reward higher risk for:

- low ratings
- angry language
- damage or shipping complaints
- refund language

Urgency should reward:

- recent negative messages
- unresolved issues
- time-sensitive shipping or event problems

Business impact should reward:

- visible refund or replacement exposure
- public-review risk
- likely repeat complaint risk

Recoverability should reward:

- situations that can be improved with a clear human reply

Policy clarity should reward:

- known allowed remedies
- known product or order context

Context completeness should reward:

- enough information to draft safely

#### Decision thresholds

- `reply_now`: score 70+ and safe to draft
- `watch`: score 40-69 with incomplete urgency or lower risk
- `escalate`: score 70+ with policy ambiguity, high risk, or remedy implications
- `needs_human_context`: whenever key context is missing

#### Priority thresholds

- `urgent`: likely refund, damage, angry tone, or public-review risk
- `high`: clear issue that should be answered soon
- `medium`: useful follow-up case
- `low`: informational only

#### Fail-closed rules

Do not emit a strong reply recommendation if:

- customer text is missing
- order or issue context is too thin
- policy ambiguity is high

Use `needs_human_context` or `escalate` instead.

#### Drafting rule

Draft replies should optimize for:

- clarity
- empathy
- policy safety
- recoverability

They should not optimize for cleverness or persuasion.

## Feedback And Learning System

This is what turns OpenClaw from a static reviewer into a useful decision system.

OpenClaw should maintain a lightweight outcome loop:

- record what it evaluated
- record what decision it gave
- record whether the human followed the advice
- observe later business outcomes
- update future confidence and thresholds

### Correlation model

Correlate across these chains when possible:

- trend -> product -> publish artifact -> sales outcome
- recommendation -> action taken -> 7-day result
- quality-gate decision -> publish choice -> engagement or conversion result
- customer issue -> response path -> resolution outcome

### Outcome sources

Use DuckAgent outputs already available:

- catalog changes in `products_cache.json`
- publication changes in `publication_cache.json`
- profit and business summaries already generated by DuckAgent
- later reviews or customer follow-ups
- post metadata and comment-related artifacts when available

### Correlation keys

Use entity-based joins first and time windows second.

Preferred correlation keys:

- normalized trend theme
- product ID
- product handle
- listing ID
- post ID
- flow
- run ID
- decision date

Use the strongest available key first.
Only fall back to fuzzy theme matching when stable IDs are unavailable.

### Time windows by decision type

Use different attribution windows for different decision families.

#### Social post or content decision

- primary window: 72 hours
- secondary window: 7 days

Measures:

- engagement lift
- comments
- saves
- click-through if available
- downstream product activity if visible

#### Promote existing product

- primary window: 7 days
- secondary window: 30 days

Measures:

- sales lift versus previous 7-day baseline
- inventory movement
- publication or visibility changes
- related engagement if available

#### Build or publish new duck or listing

- primary window: 14 days
- secondary window: 30 days

Measures:

- first sales
- inventory movement
- conversion proxy
- later review or issue rate

#### Trend ranking

- validate at 7 days
- confirm at 30 days

Measures:

- whether the ranked theme led to action
- whether the action produced sales or engagement
- whether competitor momentum persisted

#### Customer reply or support recommendation

- primary window: 48 hours
- secondary window: 14 days

Measures:

- whether a response was sent
- whether the case escalated
- whether refund or replacement occurred
- whether the customer issue repeated
- later sentiment if visible

### Outcome labels

Every attributed outcome should include:

- `advice_followed`: `yes | no | partial`
- `outcome_quality`: `positive | neutral | negative`
- `baseline_comparison`: `better | flat | worse`
- `confidence_at_decision_time`

Use relative lift versus baseline whenever possible, not raw numbers alone.

### Learning approach

Keep the learning lightweight at first:

- adjust thresholds
- adjust confidence
- keep hit-rate summaries by pattern
- keep false-positive and false-negative summaries

Do not start with model fine-tuning.

## Operating Model

This section defines how OpenClaw behaves day to day without becoming a required part of DuckAgent's execution path.

## Run Detection Model

OpenClaw should not rely on DuckAgent to announce that a run happened.

It should infer activity by observation.

Primary signals that DuckAgent has produced something new:

- a new file appears under `runs/`
- an existing file in `runs/` changes hash or modification time
- a cache file such as `weekly_insights.json` or `products_cache.json` changes
- a new approval email appears in the monitored mailbox
- a new customer or review-related email appears in the monitored mailbox
- a log file rolls forward in `logs/`

Recommended implementation model:

- poll sources on a fixed interval
- compute a stable content hash for each observed artifact
- store `first_seen_at`, `last_seen_at`, and `content_hash` in OpenClaw state
- only evaluate an artifact when it is new or materially changed

This keeps OpenClaw passive and avoids any required DuckAgent hooks.

## Reporting Model

OpenClaw should report in two layers:

### Layer 1. Structured machine output

For every decision, OpenClaw writes:

- one JSON decision artifact
- one Markdown summary artifact when human review is useful

These should live under:

- `duck-ops/output/trend_rankings/`
- `duck-ops/output/quality_gates/`
- `duck-ops/output/customer_intelligence/`
- `duck-ops/output/digests/`

### Layer 2. Human-facing notification

OpenClaw should not be the first notification engine.

Preferred Phase 2 path:

- OpenClaw writes output files
- a separate notifier reads them
- the notifier sends you a summary email or other alert

Recommended notifier order:

1. DuckAgent's existing email helper if you later choose to reuse it
2. a tiny standalone notifier outside DuckAgent if you want to keep the systems fully separated

The design should not require email delivery to prove value.
The files themselves are the primary source of truth.

### Recommended Phase 2 notification choice

Recommended default:

- OpenClaw writes files
- a tiny standalone notifier sends digest and urgent alerts

Why this is the recommended default:

- it keeps DuckAgent unchanged
- it keeps email ownership separate from evaluation
- it avoids making OpenClaw itself a mail-sending system

### Notification policy

Use two notification classes:

#### Daily digest

Send one digest email per day summarizing:

- new high-signal trend decisions
- new publish-quality decisions
- new customer-risk decisions
- unresolved clarification requests
- overrides recorded that day

The digest should prioritize:

- changes since the previous digest
- items requiring a human decision
- short evidence bullets, not full logs

#### One-off urgent alerts

Use one-off alerts only for high-threshold cases.

Recommended urgent triggers:

- high-confidence trend with clear short-window opportunity and catalog gap
- severe customer-risk signal involving refund, damage, or angry tone
- severe quality-gate failure on an artifact likely to be acted on soon
- unresolved blocked decision with urgent timing

Urgent alerts should be rare.
If they become common, the thresholds are too low.

### Phase-readiness notifications

OpenClaw should also emit a periodic phase-readiness assessment.

Recommended cadence:

- weekly

Recommended outputs:

- `phase_readiness.json`
- `phase_readiness.md`

Recommended decisions:

- `ready_to_advance`
- `stay_in_current_phase`
- `blocked`

The weekly digest should include the current readiness state.

If OpenClaw reaches `ready_to_advance`, that should also trigger a one-off owner alert.

## Clarification Model

OpenClaw should be able to ask for clarification, but not by interrupting DuckAgent runs.

Instead, it should emit a structured blocked-decision record.

Recommended blocked-decision fields:

- `decision`: `needs_human_context`
- `blocking_reason`
- `missing_fields`
- `questions`
- `safe_default`
- `deadline_or_urgency`

Example use cases:

- missing order context for a customer issue
- unclear product mapping for a trend
- draft email that does not include enough listing detail to quality-check safely

Clarification requests should appear in:

- the per-artifact JSON decision
- the Markdown review summary
- the daily digest if unresolved

## Decision Scope

OpenClaw should make more decisions than just publish decisions.

Phase 2 decision families should include:

- trend decisions:
  - `worth_acting_on`
  - `watch`
  - `ignore`
- action framing:
  - `promote`
  - `build`
  - `wait`
  - `ignore`
- quality-gate decisions:
  - `publish_ready`
  - `needs_revision`
  - `discard`
- customer-intelligence decisions:
  - `reply_now`
  - `watch`
  - `escalate`
  - `needs_human_context`
- business-action suggestions:
  - `refund_candidate`
  - `replacement_candidate`
  - `manual_review_required`

That means OpenClaw is not only a publish judge.
It is an evaluator for timing, quality, customer risk, and action priority.

## Execution Boundary

OpenClaw should not execute actions in Phase 2.

That means:

- it does not publish posts
- it does not publish listings
- it does not reply to customers
- it does not send approval commands back into DuckAgent

When OpenClaw decides a social post or listing is `publish_ready`, the result should be:

- a decision artifact is written
- a summary is surfaced to you
- you continue using DuckAgent's existing approval path if you agree

For example:

- DuckAgent sends its normal review email
- OpenClaw independently evaluates that candidate
- you read OpenClaw's decision
- if you agree, you still send the normal `publish` reply through DuckAgent's existing workflow

This keeps authority and execution with DuckAgent while letting OpenClaw act as a quality filter.

## Read-Only Email Model

Read-only email access means OpenClaw can observe mailbox contents but cannot create or send mail inside that mailbox.

That has one important consequence:

- OpenClaw should not be expected to create Gmail drafts or send Gmail replies in Phase 2

Instead, customer-reply drafting should work like this:

1. OpenClaw reads a review email, customer email, or DuckAgent review summary email in read-only mode.
2. It normalizes the message into a `customer_signal`.
3. It writes:
   - a JSON decision artifact
   - a Markdown draft packet containing:
     - recommended action
     - draft reply text
     - escalation note if needed
4. A notifier can optionally email that draft packet to you, or you can review it in the output folder.
5. You or DuckAgent send the actual reply through the existing system.

So the first usable version is:

- OpenClaw reads email
- OpenClaw drafts advice
- human or DuckAgent sends

Not:

- OpenClaw reads email
- OpenClaw writes back into Gmail

### Draft delivery options

There are three reasonable handoff options for draft replies.

#### Option 1. File-only handoff

- OpenClaw writes `reply_draft_<id>.md` and `reply_draft_<id>.json`
- you review and copy from the artifact

Pros:

- simplest
- fully passive

Cons:

- more manual

#### Option 2. Email-to-owner handoff

- OpenClaw writes the draft artifacts
- a notifier emails you the recommended reply and action summary
- you paste or forward the reply manually

Pros:

- fits your current email-first operating model
- no mailbox write access needed for OpenClaw

Cons:

- still manual for final sending

#### Option 3. DuckAgent-assisted handoff later

- OpenClaw writes structured reply artifacts
- DuckAgent or a tiny helper converts them into the same kind of owner-facing email you already use
- you approve and send through the normal channel

Pros:

- closest to your current workflow
- keeps OpenClaw passive

Cons:

- requires an optional later bridge

Recommended Phase 2 choice:

- Option 2 for human visibility
- Option 3 only after the evaluator is clearly useful

## Approval Transport Model

OpenClaw should not answer approval emails directly in Phase 2.

Instead:

- DuckAgent continues sending the review email
- OpenClaw observes the same artifact or email
- OpenClaw writes its decision
- you remain the one who replies `publish` or does not

If you later want a tighter loop, the safest future approach is:

- OpenClaw writes an approval artifact
- a bridge translates that artifact into a recommended owner action
- the final execution still happens through DuckAgent

That is safer than giving OpenClaw direct approval-email reply power.

## Configuration Requirements

This section captures what OpenClaw needs in Phase 2 and what it should not need.

### Required for Phase 2

- local model provider configuration, already present in `/Users/philtullai/ai-agents/openclaw/config/openclaw.json`
- access to OpenClaw's own workspace and state directories
- read-only access to DuckAgent artifact locations
- one mailbox observation path:
  - read-only IMAP access to the existing mailbox, or
  - mirrored Maildir or message export
- notifier configuration only if digest and urgent emails should be sent automatically

### Not required for Phase 2

- Shopify API keys inside OpenClaw
- Etsy API keys inside OpenClaw
- direct Gmail send permissions for OpenClaw itself
- publish credentials
- any new DuckAgent cron hook

### Recommended Phase 2 mailbox choice

Recommended default:

- read-only IMAP access to the same mailbox DuckAgent already uses, if that can be done safely

Fallback:

- mirrored Maildir or email export if you want harder separation

### Recommended Phase 2 notifier choice

Recommended default:

- a small standalone notifier using SMTP configuration separate from OpenClaw core logic

Phase 2 decision:

- use the standalone notifier, not DuckAgent's mail helper

Why:

- it keeps DuckAgent unchanged
- it preserves the clean separation between execution and evaluation
- it avoids coupling OpenClaw reporting to DuckAgent internals
- it is easier to remove later if the review layer is retired

Fallback:

- reuse DuckAgent's existing email helper later if you decide tighter integration is worth it

## Human In The Loop

Human review should be supported but not required for every internal evaluation.

Recommended review handling:

- all publish and customer-facing recommendations start as `pending`
- human can `approve`, `reject`, or `override`
- every override should require a short note
- overrides are stored as separate records
- override history feeds the calibration layer

This gives you accountability without slowing down internal scoring work.

## Trust And Failure Policy

OpenClaw should be conservative.

When context is thin, it should become less certain, not more opinionated.

### Confidence caps

Apply these caps before any final confidence score is emitted:

- max `0.60` if the decision is based on only one source
- max `0.70` if catalog match is unknown
- max `0.70` if the artifact was parsed only from partial email content
- max `0.75` if there is no relevant outcome history for similar prior cases

The caps are cumulative in spirit.
If multiple caps apply, use the lowest applicable cap.

### Fail-closed defaults

When OpenClaw is uncertain, default to:

- `watch`
- `needs_revision`
- `needs_human_context`
- `hold` in the action frame

Do not allow weak evidence to become a strong approval.

### Mandatory human review cases

These should always require human approval in Phase 2:

- publishing listings
- publishing social posts
- discount or sale changes
- refunds
- replacements
- angry-customer or damage-related responses
- anything involving money or public brand exposure

### DuckAgent disagreement policy

If OpenClaw disagrees with DuckAgent, it should:

- flag the disagreement explicitly
- explain the evidence behind the disagreement
- avoid attempting to override execution

The disagreement itself is valuable signal for later scorecarding.

### Noise-control rule

If urgent alerts or blocked decisions become frequent, the evaluator is too noisy.

Noise should be treated as a system defect and trigger threshold tightening.

## Proposed OpenClaw Workspace Layout

```text
duck-ops/
├── config/
│   ├── sources.md
│   ├── evaluator_rules.md
│   ├── customer_reply_policy.md
│   └── brand_guardrails.md
├── state/
│   ├── artifact_registry.jsonl
│   ├── decision_history.jsonl
│   ├── outcome_history.jsonl
│   ├── entity_memory.json
│   ├── watchlists.json
│   ├── overrides.jsonl
│   └── calibration.json
├── output/
│   ├── trend_rankings/
│   ├── quality_gates/
│   ├── customer_intelligence/
│   └── digests/
└── contracts/
    ├── trend_candidate.md
    ├── publish_candidate.md
    └── customer_signal.md
```

Note:

- This layout stores normalized records and decisions, not raw copies of DuckAgent data.
- Raw source artifacts should continue to live with DuckAgent or in the mailbox.

## Detailed Change Inventory

### OpenClaw changes we do need

- Add read-only access to DuckAgent artifact locations.
- Add read-only access to the relevant mailbox or message export.
- Add source configuration.
- Add normalizers for trends, publish candidates, customer signals, and outcomes.
- Add evaluator prompts or rules for trend ranking, quality gating, and customer intelligence.
- Add OpenClaw-owned state files.
- Add digest and decision writers.
- Add calibration logic.

### DuckAgent changes we do not need initially

- No cron changes
- No publish-flow changes
- No report-generation changes
- No email-flow changes
- No new mandatory exports
- No OpenClaw callouts inside DuckAgent runs

### DuckAgent changes we may choose later if the value is proven

- more stable draft artifact persistence
- cleaner metadata in approval emails
- support-thread export for Shopify or Etsy conversations
- optional outcome summary file if correlation becomes too noisy

## Phased Roadmap

### Phase 0. Design freeze

Goals:

- freeze the passive-observer architecture
- freeze the decision contract shape
- freeze the minimal-change rule for DuckAgent

Exit criteria:

- agreed source inventory
- agreed output contract
- agreed first evaluator to build
- agreed notification policy
- agreed trust policy

### Phase 1. Observation foundation

Goals:

- establish read-only file and mailbox observation
- create artifact registry
- normalize the first three record types

Exit criteria:

- OpenClaw can see new artifacts without any DuckAgent flow changes
- identical artifacts are not double-processed
- decision files are written consistently
- digest generation works even if no email notifier is attached

### Phase 2. Quality gate first

Goals:

- evaluate `newduck`, post drafts, and promotion recommendations
- return strict `publish_ready`, `needs_revision`, or `discard` decisions

Why first:

- highest leverage
- least duplication of DuckAgent intelligence
- easiest to evaluate manually

Exit criteria:

- stable publish review outputs
- useful feedback on weak content
- low false-confidence rate
- daily digest is useful and readable
- urgent alerts remain rare and high-signal
- human overrides are captured with notes

### Phase 2 Pilot Definition

The initial pilot should be intentionally narrow.

Pilot scope:

- evaluator: `quality_gate`
- artifacts: `newduck`, `weekly sale playbook`, and the daily `reviews` summary layer
- reporting: daily digest plus one-off urgent alerts
- review mode: human approval with override note

Pilot success metrics:

- weak candidates are correctly pushed into `needs_revision` or `discard`
- the quality gate catches issues you consider meaningful
- digest and urgent alerts are useful without being noisy
- OpenClaw does not require any DuckAgent flow changes to stay operational

Pilot exit criteria for moving forward:

- at least 2 to 4 weeks of pilot decisions collected
- override notes show the evaluator is directionally useful
- urgent alerts remain low volume and justified
- you trust the quality gate enough to keep it running continuously

### Phase 3. Trend ranking

Goals:

- score trends DuckAgent already found
- rank them by action-worthiness and urgency

Operator-facing rule on 2026-04-05:

- treat raw trends as upstream signals, not queue items by default
- collapse related raw trend artifacts into one canonical operator-facing concept before deciding whether anything should surface
- only surface a trend in WhatsApp/browser when:
  - the recommended action is `build` or `promote`
  - or a previously reviewed trend materially changed and needs a fresh `wait / build / promote / ignore` call
  - or the operator explicitly touched the trend and it still needs a final resolution
- keep first-pass `watch + wait` trends in silent background monitoring instead of asking for operator review immediately
- keep the daily digest aligned with that queue:
  - operator-facing items are listed as pending review
  - silent watches are counted separately so the email does not imply there is queue work when there is only background monitoring

Noise-reduction implementation sequence:

1. Separate trend storage from operator surfacing.
   Keep all evaluated trend artifacts in state, but stop treating every pending trend as queue work.
   Add a concept layer on top of those artifacts so review, digest, and current-item logic all talk about one canonical concept instead of one daily artifact.
2. Only surface operator-worthy trend decisions.
   `build` and `promote` stay operator-facing; first-pass `wait` stays silent unless it changed after a previous review or the operator explicitly touched it.
3. Keep digest wording aligned with the queue.
   The digest should report operator-facing pending items separately from silent background watches.
4. Rebuild the live queue after every rule change.
   Do not leave stale `wait` trends sitting in the queue just because they were pending under an older rule set.

Exit criteria:

- reliable separation of real trend, watch, and ignore
- better promote-versus-build decisions
- attribution can connect trend decisions to later outcomes using the defined windows

### Phase 3.5. Operator Channel And Social Review

This is an optional expansion phase, not a required one.

Only add it if the earlier phases are already useful and you want faster operator visibility or better post-performance review.

Goals:

- add one operator-facing alert channel in addition to email
- review live social-post performance using browser-based observation
- compare pre-publish quality-gate decisions with actual visible post outcomes

Candidate capabilities:

- urgent alerts to a single chat channel
- operator approve/reject/override messages routed outside email
- browser-based inspection of post pages, visible comments, and engagement context
- richer post-performance evidence for the feedback loop

Boundaries:

- do not make a broad multi-channel support system
- do not make social browsing a required dependency for the core plan
- do not let browser review create brand-new trend candidates in this phase

Exit criteria:

- operator alerts outside email are actually more useful, not just redundant
- browser-based post review provides signal beyond the existing artifact-based feedback
- the added operational burden remains small

### Phase 3.6. Review Reply Execution

This is also an optional expansion phase.

Current status on 2026-04-05:

- the one-time Etsy browser-path approval gate is complete
- manual dry-run fill and first controlled live submit are validated
- one session-summary email can now cover all replies posted in the same execution session
- the executor now treats "exact review row not found" as retryable when the Etsy seller session is still signed in
- the paginated Etsy review probe has been widened so transient row misses can recover without being treated as final failures
- policy-driven auto-queue plus hourly queue draining are implemented for the safest public replies
- the executor now prefers Etsy's paginated `/shop/.../reviews?page=N` surface and can probe back to the correct review page when needed
- one queued `publish_ready` public reply has now been recovered after auth restoration and posted successfully through that executor path
- auth/session loss now pauses the whole queue with a rate-limited sign-in alert instead of turning into noisy per-item failures
- the executor now saves Etsy seller-session auth state into `duck-ops`, restores it when the automation browser session is reopened, and retries that restore before asking for a manual sign-in
- the remaining reliability risk is now Etsy expiring the saved auth state itself or changing the seller reviews surface in a way that forces a fresh discovery/auth pass

Current soak gate:

- let this run through 48 hours of hourly queue-drain windows before moving to the next Phase 3.6 slice
- treat the soak as successful if auth stays healthy, saved auth remains available, no repeated auth-alert spam appears, and at least one restore-based recovery succeeds without a manual sign-in
- if a real `publish_ready` reply appears during that window, it should auto-queue and drain without manual help
- if no real reply appears, one intentional recovery test plus a healthy unattended auth state for the full soak window is still enough to move on

Only add it after the review-quality lane and operator channel are already useful enough that execution would remove real friction instead of just adding risk.

Goals:

- auto-execute the safest Etsy public review replies when OpenClaw already trusts them
- route weaker review replies to the operator channel with a clear recommendation
- execute the exact approved reply after human agreement when OpenClaw does not trust the draft enough to auto-post
- capture execution outcomes so judgment quality and execution reliability can be measured separately

First-pass scope:

- Etsy 5-star public review replies only
- only replies that DuckAgent already drafted in the daily reviews flow
- one storefront and one review surface only

Out of scope for the first pass:

- low-rating private recovery replies
- story posting
- generalized customer support
- open-ended browser autonomy

Execution model:

1. DuckAgent generates the daily review drafts as it already does.
2. OpenClaw scores each review reply as `publish_ready`, `needs_revision`, or `discard`.
3. If a review reply is `publish_ready` and review auto-execution is enabled, OpenClaw enqueues it for execution immediately.
4. If a review reply is `needs_revision` or `discard`, OpenClaw sends the recommendation to the operator channel with:
   - the customer review
   - the draft reply
   - the reasons
   - the exact recommendation
5. If the operator agrees or explicitly approves anyway, OpenClaw enqueues the exact approved reply text for execution.
6. A deterministic executor posts the reply to Etsy and returns success or failure.
7. OpenClaw records the outcome and notifies the operator.

Current implementation note:

- `publish_ready` Etsy public replies can now inherit a previously approved browser path instead of forcing a fresh manual browser approval for each new review target
- the hourly sidecar is allowed to auto-queue those replies and drain the queue with one batched session-summary email after successful posts
- the executor now persists Etsy auth state into a canonical `duck-ops/state/review_reply_execution_auth_storage/` file and can mirror that file through the Playwright session's allowed root when an older browser session was opened from a different cwd
- `needs_revision` and `discard` replies still remain outside the auto-post path, but an explicit operator `approve` now queues the exact approved text for deterministic execution without needing the browser review page
- the operator channel now supports `rewrite`, which returns a concrete replacement reply for weak review drafts; `approve because use rewrite` approves that rewritten text for execution
- the WhatsApp operator bridge now suppresses its own reflected outbound replies by remembering the exact sent message hashes, so adding new commands does not require another echo-prefix patch
- the WhatsApp operator bridge now reads a fixed transcript snapshot per polling pass so it cannot recursively react to self-echo lines appended during the same run
- the WhatsApp operator bridge now tracks a durable `last_processed_message_id` cursor instead of relying only on a fixed 500-id memory window, which prevents old operator commands from replaying when the session transcript grows
- operator cards now explicitly state what the approval means, so a review-story/social approval is clearly distinguished from a customer-reply approval
- weekly sale cards now pull the concrete sale targets and discounts from DuckAgent's structured `sale_playbook`, so the approval message shows what discounts and products the human is actually agreeing to
- weekly sale cards now explicitly say when OpenClaw's objection is mainly "too incomplete / too vague to approve safely" instead of "the sale strategy is wrong"
- weekly sale `suggest changes` and `rewrite` now return a tightened sale-plan version so the operator can see the concrete changes OpenClaw wants without guessing
- DuckAgent weekly sale generation now finishes with a deterministic `approval_summary`, normalized platforms/discounts, and resolved Shopify product IDs so the playbook is closer to operator-ready before OpenClaw touches it
- OpenClaw now reads that richer weekly-sale `approval_summary` from `state_weekly.json` instead of collapsing the artifact down to the one-line strategic summary
- trend review carry-forward now suppresses semantically-close sibling variants like `flamingo duck` versus `pink flamingo duck` when the prior reviewed resolution still applies
- daily digests now separate `new decisions this run` from `still pending review`, and no longer present already-reviewed items as if they were new
- bridge progress is now written after each handled message so a long backlog or interrupted run cannot replay the same stale commands for minutes

Critical design rule:

- The browser executor should be deterministic and task-specific.
- Do not let the model browse Etsy freely and decide what to click at execution time.

Recommended review-execution artifact contract:

```json
{
  "artifact_id": "publish::reviews_reply_positive::<run_id>::<review_key>",
  "surface": "etsy_public_review_reply",
  "platform": "etsy",
  "decision": "publish_ready|needs_revision|discard",
  "execution_mode": "auto|operator_approved|manual_only",
  "approved_reply_text": "exact text to post",
  "review_target": {
    "shop_id": "string",
    "review_key": "stable internal key",
    "review_id": "string or null",
    "transaction_id": "string or null",
    "listing_id": "string or null",
    "review_url": "string or null"
  },
  "execution_state": "not_queued|queued|running|posted|failed|skipped",
  "execution_attempts": [],
  "operator_resolution": {
    "action": "agree|approve|needs_changes|discard|none",
    "note": "string or null",
    "recorded_at": "timestamp or null"
  }
}
```

Why this contract matters:

- the executor must know exactly which review to target
- the executor must post the exact approved text, not regenerate it
- execution state must be auditable independently from the quality decision

Deterministic browser executor requirements:

- use a fixed signed-in Etsy browser profile or session owned by the operator
- navigate to the review target using the strongest available identifier
- verify the visible review text or transaction context before posting
- paste the exact approved reply text
- submit only after pre-submit verification passes
- detect and record:
  - success
  - already replied
  - target not found
  - selector drift
  - auth/session failure

Browser discovery and approval gate:

- before any live executor is enabled, run a read-only browser discovery pass first
- discovery mode must not click submit
- discovery should capture a review packet that includes:
  - the exact Etsy page URL or path used
  - step-by-step screenshots
  - the visible review text and contextual identifiers it matched
  - the selector or page-anchor strategy it plans to use
  - proof of the reply box it would use
  - proof that submit was not clicked
- after read-only discovery, allow a dry-run interaction mode:
  - navigate to the review target
  - verify the review
  - optionally paste the reply text
  - stop before submit
  - capture another screenshot packet
- current validated dry-run path:
  - reuse the existing signed-in Etsy seller session instead of forcing a fresh protected-page open
  - start from the seller dashboard
  - navigate in-session to the shop `#reviews` surface
  - locate the exact target review by Etsy transaction ID from the live DOM (`data-review-region` / `data-transaction-id`)
  - verify the matched Etsy listing link against the expected listing ID from DuckAgent
  - use review text as contextual confirmation rather than the primary locator
  - confirm the reply textarea and nearby controls are visible
  - stop before any typing or submit action
- operator browser surface:
  - show the latest discovery packet and screenshot in the shared Duck browser
  - track whether the current packet has already been approved
  - keep this as a one-time or rare review gate, not a daily packet-inspection burden
  - present the decision as three operator checks instead of a raw packet dump:
    - transaction ID match
    - listing ID match
    - reply box present on the same matched review row
  - show the current public review-reply items and their execution readiness separately from the packet itself
- do not implement the live submit path until the operator reviews and approves the discovery packet
- if discovery cannot prove the correct review target or safe page path, stop the executor project rather than weakening verification

Safety boundaries:

- fail closed if the exact review target cannot be verified
- fail closed if the draft text in the execution packet does not match the approved text
- do not regenerate or “improve” the reply during execution
- do not auto-execute private or low-rating replies in the first phase
- keep a manual fallback path if execution fails

Operator experience:

- for `publish_ready` public replies, the operator should receive a confirmation after posting
  - current implementation: one batched session-summary email can cover all replies posted in the same operator session
- for `needs_revision` or `discard`, the operator should see the recommendation first
- if the operator says `agree`, OpenClaw should treat that as agreement with the recommendation
- if the operator says `approve`, OpenClaw should post the approved reply text even if the model had recommended against it

Recommended rollout:

1. manual enqueue only:
   - OpenClaw never auto-posts
   - operator approval creates the execution job
   - current implemented slice:
     - browser path approval is required first
     - the Review Execution browser page can queue the current public reply
     - the deterministic executor can fill the exact approved reply text into the correct Etsy review row
     - the executor stops before submit and records an auditable attempt packet plus queue state
     - the Review Execution browser page now exposes an explicit `Submit Current Reply` action that only enables after a successful dry-run fill and asks for confirmation before posting
     - first controlled live-submit validation completed on 2026-04-04 for `publish::reviews_reply_positive::2026-04-04::review-2`
     - the executor posted the exact approved reply text to Etsy transaction `4991165258` after re-verifying listing `4337360106`
     - the attempt packet recorded `submit_performed = true` and post-submit row text containing `Philip responded on Apr 4, 2026`
     - the Review Execution browser page now exposes `Send Session Summary Email`, which sends one operator email for the current batch of posted replies
     - live validation on 2026-04-04 also covered the first session-summary email for that posted reply
     - live validation on 2026-04-05 confirmed that a signed-in "exact review row not found" failure for `publish::reviews_reply_positive::2026-04-04::review-1` was not an auth issue; after widening the Etsy page probe and retry logic, the same review was found and dry-run filled successfully
2. mixed mode:
   - `publish_ready` public replies auto-post
   - weaker replies still require operator action
3. later expansion:
   - consider story posting only after public reply execution is stable
   - keep private recovery replies out until a separate risk review is done

Exit criteria:

- execution targets the correct review every time
- auto-posted replies are directionally strong enough that you do not feel the need to re-check each one
- operator-approved exceptions are captured cleanly and improve calibration
- maintenance burden of the executor stays low
- failure cases are visible and recoverable

### Phase 4. Customer intelligence

Companion planning doc:

- `CUSTOMER_INTERACTION_AGENT_PLAN.md`

Goals:

- detect risk from reviews and inbound customer emails
- recommend action and draft responses

Current foundation implemented on April 6, 2026:

- `customer_case`, `custom_design_case`, and `print_queue_candidate` contracts exist
- current DuckAgent review, mailbox, and weekly-insight signals are mapped into those contracts
- a staged operator-facing queue is written to:
  - `state/customer_interaction_queue.json`
  - `output/operator/customer_interaction_queue.md`
- low-signal customer cases are hidden from the staged queue
- Etsy conversation emails are collapsed into one queue item per customer thread
- customer cases now carry explicit `context_state`, `response_recommendation`, and `recovery_recommendation`
- first-pass Etsy order and tracking enrichment is staged:
  - Etsy order-email parsing
  - cached Etsy receipt snapshot
  - product / receipt / shipment / tracking context on matched customer cases
- first-pass customer-resolution history is now staged:
  - refund history from Etsy receipt status
  - public review-reply history from Duck Ops execution sessions
  - possible prior resend history from multiple Etsy shipments
- staged customer action packets now exist for the operator lane:
  - reply
  - refund
  - replacement
- stale refund/reply packets are now suppressed when Duck Ops can tell the action already happened
- possible prior resend history is downgraded into a confirm/watch state instead of a fresh label-buy action
- explicit customer recovery decisions are now persisted under:
  - `state/customer_recovery_decisions.jsonl`
  - `runtime/customer_recovery_decisions.py`
- customer cases, action packets, and nightly summaries now honor approved operator choices such as:
  - replacement
  - refund
  - wait
  - reply_only
- a lightweight customer operator lane now exists:
  - `output/operator/current_customer_action.md`
  - `output/operator/customer_queue.md`
  - `runtime/customer_operator.py`
- `review_loop.py handle` now routes `customer status`, `customer next`, and decisions like `replacement C301 because ...` into that lane
- fail-closed USPS and Google Tasks bridges now exist:
  - `runtime/usps_tracking.py`
  - `runtime/google_tasks_bridge.py`
  - both currently report `credentials_missing` until real auth/config is added
- staged `custom_build_task_candidate` outputs now exist from live paid custom-order lines:
  - `state/custom_build_task_candidates.json`
  - `output/operator/custom_build_task_candidates.md`
- staged `etsy_conversation_thread` browser-review records now exist:
  - `state/etsy_conversation_browser_sync.json`
  - `output/operator/etsy_conversation_browser_sync.md`
- the customer operator lane now supports browser-open commands for staged Etsy thread work when a usable URL exists:
  - `customer open C301`
- a unified staged business desk now exists:
  - `state/business_operator_desk.json`
  - `output/operator/business_operator_desk.json`
  - `output/operator/business_operator_desk.md`
  - it combines customer packets, Etsy browser-review work, custom build candidates, packing work, print-soon candidates, and review-queue counts in one operator surface
  - `review_loop.py handle` now supports:
    - `desk status`
    - `desk next`
    - `desk show customer|threads|builds|packing|stock|reviews`
- nightly operations snapshots now exist for:
  - Etsy open orders
  - Shopify open orders
  - aggregated orders to pack tonight
- a nightly action summary preview is now written with sections for:
  - customer issues needing reply
  - buy replacement labels now
  - orders to pack
  - custom / novel ducks to make
  - watch list
- the nightly `orders to pack` section is now sorted by ship urgency first and rendered as a shopping-list-style table instead of a raw order dump
- staged custom build candidates now show the actual Etsy personalization / build detail in the nightly summary so the operator can see what needs to be made

Immediate next slice:

- upgrade staged Etsy conversation thread records into real browser-reviewed thread captures with latest message text and read/unread state
- add `wait_for_tracking` packets once USPS read-only status checks exist
- add real USPS credentials / endpoint config so live carrier lookups can run
- add real Google Tasks credentials / task-list config so ready custom-design cases and custom build candidates can create tasks
- surface customer packets and the staged business desk more proactively in the operator push / WhatsApp flow

Exit criteria:

- useful drafts
- better escalation clarity
- no unsafe autonomy
- reply recommendations align with policy and do not create new customer risk

### Phase 5. Feedback and calibration

Goals:

- connect decisions to later outcomes
- improve evaluator confidence and thresholds

Exit criteria:

- measurable signal that OpenClaw improves decisions
- evidence for which evaluators should stay, grow, or be removed
- thresholds have been adjusted based on actual false positives and false negatives

## Stage-Gate Rules

Do not advance to the next stage just because the prior stage "works."

Advance only when the acceptance criteria are met and the evaluator is useful in practice.

### Move from Phase 1 to Phase 2 when:

- observation is stable
- duplicate processing is controlled
- decision artifacts are consistently written

### Move from Phase 2 pilot to Phase 3 when:

- the quality gate is trusted enough to keep on
- digest and urgent alerting feel operationally useful
- override notes show repeatable patterns rather than random disagreement

### Move from Phase 3 to optional Phase 3.5 when:

- email-only operation feels limiting
- browser-visible post performance would materially improve learning
- you want one additional operator channel for urgent decisions

### Move from optional Phase 3.5 to optional Phase 3.6 when:

- the operator channel is already useful for reviews
- review-reply recommendations feel trustworthy enough to consider execution
- posting approved Etsy public replies would save meaningful time
- you are willing to keep the first execution scope narrow

### Move from Phase 3 to Phase 4 when:

- trend ranking produces signal beyond what DuckAgent reports already provide
- attribution is good enough to evaluate whether trend decisions were directionally right

### Move from optional Phase 3.5 to Phase 4 when:

- the added channel or browser layer proves useful without adding too much maintenance
- social-post observation is improving post evaluation or feedback quality
- the optional layer remains clearly additive rather than distracting

### Move from optional Phase 3.6 to Phase 4 when:

- reply execution is reliable and low-drama
- execution outcomes provide useful calibration signal
- the executor remains deterministic rather than turning into general browser maintenance

### Move from Phase 4 to Phase 5 when:

- customer-intelligence outputs are useful and safe
- escalation handling is clear
- drafts reduce response effort without increasing business risk

### Move beyond Phase 5 only when:

- scorecards show OpenClaw materially improves decision quality
- noise stays low
- the maintenance burden remains acceptable

## Future Breadcrumbs

This section is not part of the required Phase 2 implementation.
It exists so future growth has a direction without forcing premature scope.

### Breadcrumb 1. Operator channel

If email becomes too slow or noisy, add one operator channel only.

Recommended constraints:

- one channel
- urgent alerts and overrides only
- no attempt to mirror every digest or every decision

### Breadcrumb 2. Browser-based social review

If post-performance learning needs better evidence, add browser-based observation for:

- visible engagement
- visible comments
- public post context

Recommended constraints:

- use it to enrich review and feedback
- do not turn it into a general-purpose social automation layer

### Breadcrumb 3. Stronger customer context

If customer-intelligence drafts are useful enough, later add:

- Shopify support-thread exports
- Etsy conversation exports
- joined order or fulfillment context

Recommended constraint:

- add exports first, not direct action execution

### Breadcrumb 4. Decision portfolio view

If the system becomes busy across many artifact types, later add a portfolio-style prioritization layer:

- top items to act on this week
- highest-risk customer cases
- most important publish decisions pending

Recommended constraint:

- derive it from existing decision artifacts rather than creating a new recommendation engine

### Breadcrumb 5. Selective closed loop

Only after strong scorecard evidence, consider tightly constrained action loops such as:

- low-risk owner notifications with pre-filled approval suggestions
- structured handoff files that DuckAgent can optionally consume
- tightly scoped review-reply execution for approved Etsy public replies

Recommended constraint:

- keep the final execution boundary with DuckAgent unless the measured value is overwhelming

## Phase-Readiness Report Contract

OpenClaw should maintain a single structured readiness artifact for the current stage.

Recommended shape:

```json
{
  "current_phase": "phase_2_pilot",
  "readiness_decision": "ready_to_advance|stay_in_current_phase|blocked",
  "confidence": 0.0,
  "evidence": [],
  "blockers": [],
  "recommended_next_phase": "phase_3",
  "generated_at": "timestamp"
}
```

This report should be updated weekly and referenced in the weekly digest.

If `readiness_decision` becomes `ready_to_advance`, the notifier should send you a dedicated alert.

## Implementation Mapping

This section translates the architecture into the first concrete operating map without changing DuckAgent.

## First-wave source-to-contract mappings

These are the first source mappings the system should support.

### Mapping A. Trend ranking inputs

Primary sources:

- latest `/Users/philtullai/ai-agents/duckAgent/runs/<run_id>/state_competitor.json`
- `/Users/philtullai/ai-agents/duckAgent/cache/weekly_insights.json`
- `/Users/philtullai/ai-agents/duckAgent/cache/product_recommendations.json`
- `/Users/philtullai/ai-agents/duckAgent/cache/products_cache.json`
- `/Users/philtullai/ai-agents/duckAgent/cache/publication_cache.json`
- optional `/Users/philtullai/ai-agents/duckAgent/cache/reddit_signal_history.json`

Normalized output:

- one `trend_candidate` per normalized trend theme

Primary join keys:

- normalized theme
- run ID
- product handle or title match

### Mapping B. Publish quality inputs

Primary sources:

- DuckAgent review and approval emails for `newduck`
- DuckAgent review and approval emails for `weekly sale playbook`
- stable state files such as `state_newduck.json` or `state_weekly.json` if present
- supporting assets under `runs/<run_id>/`
- trend and catalog context from the cache files above

Normalized output:

- one `publish_candidate` per reviewable artifact

Primary join keys:

- flow
- run ID
- artifact slug derived from subject or candidate title

### Mapping C. Customer intelligence inputs

Primary sources:

- `/Users/philtullai/ai-agents/duckAgent/runs/<run_id>/state_reviews.json`
- DuckAgent review summary emails
- inbound mailbox messages that already represent customer issues or platform notifications

Normalized output:

- one `customer_signal` per review, issue, or reply opportunity

Primary join keys:

- channel
- native review or message ID if present
- message hash fallback

## First output file plan

OpenClaw should emit stable, predictable filenames.

### Trend outputs

- `duck-ops/output/trend_rankings/trend__<theme>__<date>.json`
- `duck-ops/output/trend_rankings/trend__<theme>__<date>.md`

### Quality-gate outputs

- `duck-ops/output/quality_gates/publish__<flow>__<run_id>__<artifact_slug>.json`
- `duck-ops/output/quality_gates/publish__<flow>__<run_id>__<artifact_slug>.md`

### Customer-intelligence outputs

- `duck-ops/output/customer_intelligence/customer__<channel>__<artifact_id>.json`
- `duck-ops/output/customer_intelligence/customer__<channel>__<artifact_id>.md`

### Digest and alert outputs

- `duck-ops/output/digests/digest__YYYY-MM-DD.json`
- `duck-ops/output/digests/digest__YYYY-MM-DD.md`
- `duck-ops/output/digests/urgent__YYYY-MM-DDTHHMMSS__<artifact_id>.json`
- `duck-ops/output/digests/urgent__YYYY-MM-DDTHHMMSS__<artifact_id>.md`
- `duck-ops/output/digests/phase_readiness__YYYY-WW.json`
- `duck-ops/output/digests/phase_readiness__YYYY-WW.md`

## Notifier trigger rules

These rules define when the standalone notifier should send email.

### Daily digest trigger

Send one digest email per day when either of these is true:

- at least one new decision artifact was created since the last digest
- at least one unresolved blocked decision remains open

If there is no new information, no digest is required.

### Urgent alert trigger

Send an urgent email immediately when any of these is true:

- a decision artifact has `priority = urgent`
- a customer signal is marked `escalate` with urgent priority
- a quality-gate artifact has a severe negative finding on an item likely to be acted on soon
- a blocked decision includes urgent timing
- the weekly readiness decision becomes `ready_to_advance`

### Weekly phase-readiness trigger

Generate the phase-readiness report once per week using:

- the previous 7 days of decisions
- the previous 7 days of overrides
- any observed urgent alerts
- available attributed outcomes

If the result is:

- `ready_to_advance`: include it in the weekly digest and send a one-off alert
- `stay_in_current_phase`: include it in the weekly digest only
- `blocked`: include it in the weekly digest and send an alert only if the blocker is urgent or long-running

## Pilot implementation mapping

The pilot should map only the smallest useful set.

### Pilot inputs

- `newduck` approval emails
- `weekly sale playbook` review emails
- cache context needed for quality-gate scoring

### Pilot outputs

- quality-gate decision files
- one daily digest
- urgent alerts only for severe failures
- one weekly phase-readiness report

### Pilot review loop

1. DuckAgent emits its normal artifact or approval email.
2. OpenClaw observes it passively.
3. OpenClaw writes a quality-gate decision artifact.
4. The standalone notifier includes it in the digest or sends an urgent alert if thresholded.
5. You decide whether to follow DuckAgent's normal `publish` path.

This gives you a complete passive loop without changing DuckAgent.

## Artifact Contracts

This section defines the minimum viable contracts for the first three OpenClaw evaluators.

These contracts are intentionally narrow.
They exist to prevent OpenClaw from needing ad hoc DuckAgent changes every time a new workflow is observed.

## Contract 1. `trend_candidate`

### Purpose

Represent one trend-like signal DuckAgent has already identified so OpenClaw can rank it, compare it to the catalog, and decide whether it is worth acting on.

### Source precedence

Use sources in this order when building a normalized trend candidate:

1. latest competitor report in `runs/<run_id>/state_competitor.json`
2. `cache/weekly_insights.json`
3. `cache/product_recommendations.json`
4. `cache/reddit_signal_history.json`
5. catalog context in `cache/products_cache.json`
6. publication context in `cache/publication_cache.json`
7. optional web corroboration only after the candidate already exists

### Stable identifier

The stable identifier should be deterministic and human-auditable.

Recommended format:

- `trend::<normalized_theme>::<first_seen_date>`

If no clean theme exists, fall back to:

- `trend::<source_type>::<source_id>`

### Required fields

```json
{
  "artifact_id": "trend::<theme>::<date>",
  "artifact_type": "trend",
  "theme": "dachshund duck",
  "source_refs": [
    {
      "path": "/abs/path/to/source.json",
      "source_type": "state_competitor",
      "run_id": "2026-03-18"
    }
  ],
  "observed_at": "2026-03-18T10:00:00-04:00",
  "first_seen_at": "2026-03-16T10:00:00-04:00",
  "signal_summary": {
    "sold_last_7d": 31,
    "sold_last_30d": 44,
    "engagement_delta_7d": 1154,
    "views_delta_7d": 850,
    "favorites_delta_7d": 76,
    "quantity": 17,
    "previous_quantity": 48,
    "delta_source": "inventory_drop"
  },
  "catalog_match": {
    "status": "covered|partial|gap|unknown",
    "matching_products": [],
    "publication_coverage": []
  }
}
```

### Optional fields

- `price_band`
- `competitor_examples`
- `recommended_action_hint`
- `reddit_evidence`
- `web_corroboration`
- `seasonality_hint`
- `historical_pattern_key`

### Normalization rules

- Normalize themes to lowercase business labels where possible.
- Merge source records that clearly refer to the same duck idea or theme.
- Prefer inventory-drop and sold-count evidence over engagement-only signals.
- Preserve raw source references for auditability.
- Match to catalog by title, tags, handle, and category metadata, not title only.

### Missing-data rules

- If no sales or inventory fields exist, the candidate can still be created, but maximum confidence should be capped.
- If catalog matching fails, set `catalog_match.status` to `unknown`, not `gap`.
- If the same theme appears with conflicting metrics, preserve both source references and mark the candidate as mixed internally.

### Decision labels

- `worth_acting_on`
- `watch`
- `ignore`

### Minimum evidence rules

Minimum evidence for `worth_acting_on`:

- at least 2 supporting signals from DuckAgent sources
- at least 1 strong commercial signal such as sold count, inventory drop, or repeated appearance over time

Maximum-confidence `worth_acting_on` should require:

- strong commercial signal
- repeated appearance across days or reports
- catalog fit known

`watch` should be used when:

- the theme is promising but not yet sales-backed
- the source is mostly engagement-based
- catalog coverage is unclear

`ignore` should be used when:

- the signal is isolated
- it lacks corroboration
- it is already fully covered and not materially rising

### Evaluator output requirements

Trend ranking output must include:

- clear action frame: `promote`, `build`, `wait`, or `ignore`
- explanation of whether the catalog already covers the signal
- confidence and urgency

### Minimal DuckAgent change impact

Required DuckAgent changes:

- none

Optional later improvements:

- more stable naming of trend themes inside reports
- explicit `first_seen` timestamps in future reports

## Contract 2. `publish_candidate`

### Purpose

Represent any pre-publish or pre-action output DuckAgent has already generated so OpenClaw can decide whether it is ready, weak, or should be discarded.

### Scope

This contract should handle:

- listing drafts
- social post drafts
- `newduck` concepts
- sale playbook recommendations
- campaign ideas that may drive public action

### Source precedence

Use sources in this order:

1. DuckAgent approval emails already generated for the artifact
2. stable run-state files such as `state_newduck.json` or `state_weekly.json` when present
3. attached assets or metadata in `runs/<run_id>/`
4. supporting trend and catalog context from DuckAgent caches

This order matters because email currently reflects the real human review surface in DuckAgent.

### Stable identifier

Recommended format:

- `publish::<flow>::<run_id>::<artifact_slug>`

Examples:

- `publish::newduck::2026-03-22::trail-ranger`
- `publish::weekly::2026-03-22::sale-playbook`

### Required fields

```json
{
  "artifact_id": "publish::newduck::2026-03-22::trail-ranger",
  "artifact_type": "listing",
  "flow": "newduck",
  "run_id": "2026-03-22",
  "source_refs": [
    {
      "source_type": "email",
      "message_id": "<mail-id>",
      "subject": "MJD: [newduck] Trail Ranger | FLOW:newduck | RUN:2026-03-22 | ACTION:review"
    }
  ],
  "candidate_summary": {
    "title": "Trail Ranger Duck",
    "body": "draft body text",
    "tags": ["duck", "jeep", "trail"],
    "images": ["newduck_1.png", "newduck_2.png"],
    "platform_targets": ["shopify", "etsy"]
  },
  "supporting_context": {
    "trend_refs": [],
    "catalog_overlap": [],
    "brand_family": "jeep",
    "timing_context": "spring"
  }
}
```

### Optional fields

- `price`
- `handle`
- `etsy_title`
- `shopify_title`
- `post_copy`
- `campaign_type`
- `sale_window`
- `raw_email_excerpt`
- `asset_preview_paths`

### Normalization rules

- If the source is email, extract the artifact fields into a structured form and keep the original subject as evidence.
- If multiple platform-specific versions exist, preserve them as separate subfields inside one publish candidate.
- Normalize image references to file names or absolute paths, not transient URLs when avoidable.
- Always attach trend and catalog context when available, even if the original artifact does not mention them.

### Missing-data rules

- If body copy is missing but title and assets exist, create the candidate and mark content completeness as low.
- If supporting trend context is absent, do not block evaluation, but cap maximum confidence.
- If an artifact is only visible in email and cannot be fully parsed, still create the record with partial parse quality.

### Decision labels

- `publish_ready`
- `needs_revision`
- `discard`

### Minimum evidence rules

Minimum evidence for `publish_ready`:

- the artifact is materially complete
- there is no obvious brand or quality defect
- the idea has some supporting context from trend, catalog, or campaign strategy

Maximum-confidence `publish_ready` should require:

- strong trend or campaign fit
- clear differentiation
- no major clarity or quality issues

`needs_revision` should be used when:

- the idea is good but execution is weak
- copy is vague
- timing is questionable
- duplication risk exists but is fixable

`discard` should be used when:

- the artifact is low quality
- the idea is poorly supported
- it conflicts with brand or catalog strategy
- the likely downside is higher than the likely upside

### Evaluator output requirements

Quality-gate output must include:

- exact weak points
- exact suggested fixes
- an explicit reason if the candidate should be discarded rather than revised

### Minimal DuckAgent change impact

Required DuckAgent changes:

- none

Optional later improvements:

- consistent email subject schema across all publish-review flows
- stable saved candidate payloads for flows that currently only exist in email

## Contract 3. `customer_signal`

### Purpose

Represent one customer-facing issue, sentiment event, or reply opportunity so OpenClaw can assess risk and recommend the right response path.

### Scope

This contract should cover:

- Etsy reviews
- low-rating review follow-up opportunities
- inbound customer-support emails already visible in the mailbox
- later Shopify or Etsy thread exports if they become available

### Source precedence

Use sources in this order:

1. `runs/<run_id>/state_reviews.json`
2. review-related emails already generated by DuckAgent
3. inbound customer or platform notification emails already landing in the mailbox
4. future exported support-thread JSON if it exists later

### Stable identifier

Recommended format:

- `customer::<channel>::<native_id>`

Fallback format:

- `customer::<channel>::<message_hash>`

### Required fields

```json
{
  "artifact_id": "customer::etsy_review::123456",
  "artifact_type": "customer",
  "channel": "etsy_review",
  "source_refs": [
    {
      "path": "/abs/path/to/state_reviews.json",
      "run_id": "2026-03-22"
    }
  ],
  "customer_event": {
    "event_type": "review|email|support_message",
    "rating": 2,
    "sentiment": "negative",
    "customer_text": "The paint chipped and it arrived late.",
    "event_time": "2026-03-22T08:15:00-04:00"
  },
  "business_context": {
    "order_id": "optional",
    "product_title": "Trail Ranger Duck",
    "issue_type": "quality|shipping|confusion|refund_request|unknown",
    "allowed_remedies": ["refund", "replacement", "apology"]
  }
}
```

### Optional fields

- `customer_name`
- `transaction_id`
- `prior_messages`
- `fulfillment_status`
- `tracking_status`
- `policy_notes`
- `suggested_reply_from_duckagent`
- `resolution_history`

### Normalization rules

- Convert reviews, emails, and future support threads into one common event structure.
- Separate customer text from business context.
- Infer `issue_type` conservatively; if unclear, use `unknown`.
- Preserve low-rating follow-up suggestions DuckAgent already generated as evidence, not as the final answer.

### Missing-data rules

- If there is no order context, the record can still be evaluated, but escalation confidence should be capped.
- If customer text is missing, do not generate a draft reply; emit `needs_human_context`.
- If the source is only a platform notification email, create the signal with partial context and mark it as incomplete.

### Decision labels

- `reply_now`
- `watch`
- `escalate`
- `needs_human_context`

### Minimum evidence rules

Minimum evidence for `reply_now`:

- clear customer text or review text
- enough context to produce a safe draft

Maximum-confidence `reply_now` should require:

- known issue type
- allowed remedy known
- no signs of legal, payment, fraud, or severe refund risk

`escalate` should be used when:

- the customer is clearly upset
- a refund or replacement is likely
- policy interpretation is needed
- fulfillment or damage issues are ambiguous

`needs_human_context` should be used when:

- the message lacks enough context to draft safely
- only metadata exists
- the customer intent is unclear

### Evaluator output requirements

Customer-intelligence output must include:

- risk level
- suggested business action
- draft reply when safe
- escalation reason when not safe

### Minimal DuckAgent change impact

Required DuckAgent changes:

- none for Etsy review intelligence

Optional later improvements:

- exported Shopify support threads
- exported Etsy conversation threads
- richer order or fulfillment context joins

## Contract Design Principles

These rules apply to all three contracts:

- Prefer incomplete but auditable records over fabricated completeness.
- Always preserve source references.
- Cap confidence aggressively when source context is thin.
- Never let missing data silently turn into a strong decision.
- Keep contracts stable even if the underlying DuckAgent artifact shapes drift.

## Revised Next Planning Step

The next planning step should be implementation mapping, not more abstract architecture.

That means defining:

- source-to-contract mappings for the artifacts you will observe first
- the first output file names and locations
- the notifier trigger rules
- the weekly phase-readiness generation rule

Why this is next:

- the architecture is now specific enough
- the remaining risk is operational ambiguity, not conceptual ambiguity
- this step can stay planning-only while getting you very close to implementation

## What Not To Do

- Do not move DuckAgent logic into OpenClaw.
- Do not make OpenClaw another recommendation engine that repeats DuckAgent reports.
- Do not require DuckAgent to stage files for OpenClaw in Phase 2.
- Do not start with direct Shopify or Etsy action execution.
- Do not build a complex agent swarm.
- Do not treat optional future integrations as required architecture now.

## Success Criteria

The design is successful only if it produces these outcomes:

- fewer low-quality ducks created
- better timing on trends
- higher conversion on listings and content
- fewer customer issues and better recovery
- measurable improvement in decision quality over time

If those outcomes are not measurable, the OpenClaw layer should be simplified or removed.

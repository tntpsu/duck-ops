# Duck Ops + DuckAgent Master Roadmap

Last updated: 2026-06-27

## Document Ownership

This is the canonical cross-repo roadmap for Duck Ops, DuckAgent, and related product work.

This document owns:
- completed major capabilities
- active operational lanes
- highest-value open work
- near-term roadmap recommendations
- legacy-plan archival notes

This document does not own:
- phase-by-phase rollout criteria
- detailed implementation design for a lane
- current code ownership boundaries
- governance power-tier policy

Companion docs:
- [ROADMAP_EXECUTION_SEQUENCE.md](/Users/philtullai/ai-agents/duckAgent/docs/current_system/ROADMAP_EXECUTION_SEQUENCE.md)
- [MASTER_IMPLEMENTATION_PLAN.md](/Users/philtullai/ai-agents/duckAgent/docs/current_system/MASTER_IMPLEMENTATION_PLAN.md)
- [AGENT_GOVERNANCE_POLICY.md](/Users/philtullai/ai-agents/duckAgent/docs/current_system/AGENT_GOVERNANCE_POLICY.md)
- [SOCIAL_PERFORMANCE_EXECUTION_PLAN.md](/Users/philtullai/ai-agents/duckAgent/docs/current_system/SOCIAL_PERFORMANCE_EXECUTION_PLAN.md)
- [CREATIVE_QUALITY_LOOP_V2_PLAN.md](/Users/philtullai/ai-agents/duckAgent/docs/current_system/CREATIVE_QUALITY_LOOP_V2_PLAN.md)
- [PROMPT_CONTRACT_AUDIT_PLAN.md](/Users/philtullai/ai-agents/duckAgent/docs/current_system/PROMPT_CONTRACT_AUDIT_PLAN.md)
- [PRODUCT_CONCEPT_BRIEF_CONTRACT_PLAN.md](/Users/philtullai/ai-agents/duckAgent/docs/current_system/PRODUCT_CONCEPT_BRIEF_CONTRACT_PLAN.md)

## Completed Major Work

### 1. Workflow Control Plane
- Shared workflow state/receipt model is live across key lanes.
- Health now prefers explicit blocker reasons over weak inference.
- Added clearer states like blocked, awaiting review, running, verified, and resolved.
- Added operator-facing follow-through summaries with next actions and commands.

### 2. Etsy Customer Workflow Safety
- Safer thread opening and verification for Etsy messages.
- Preview -> confirm -> verify reply workflow is live.
- Trusted direct Etsy thread URLs are persisted when safely verified.
- Etsy inbox refresh lane is implemented in OpenClaw and installed in launchd.
- Spam folder is now observed for Etsy conversation emails.
- Customer nightly reporting is less noisy and more action-focused.
- Shared Etsy browser guard now blocks browser-heavy Etsy automation during cooldowns or suspicious behavior.

### 3. Nightly Ops Email
- Pack list is now a single shopping-list section instead of split aging/open buckets.
- Order option details are richer for ducks with variants.
- Top customer actions are ranked higher and less noisy.
- Workflow follow-through now includes root-cause style Why/Fix guidance.
- Human-readable timestamps were added in key workflow areas.
- Quality gate and customer action sections are more operator-first.
- Daily profit email is now cadence-gated: `DUCK_PROFIT_EMAIL_CADENCE=weekly` + `DUCK_PROFIT_EMAIL_WEEKDAY=monday` defer the normal email to Monday while the 23:58 cron still refreshes workflow_control state and receipts. Operator inbox quiets without losing agent-readable data. Pattern mirrors the competitor cadence shipped earlier.
- Anomaly-bypass overrides the cadence on bad days: `net_profit < 0`, `revenue` below the 30-day floor, `orders == 0`, or total Etsy data loss each trigger an immediate email. Cold-start (< 14 days history) suppresses metric-based bypasses but still emails on auth/credential failures. Receipt records both `last_side_effect` (kind=email, reason=anomaly_bypass) and `last_verification` (kind=operator_email_cadence, status=anomaly_bypass) for downstream readers.
- Sanity floor prevents the operator from being emailed garbage: impossible metrics (revenue out of range, margin > 100%, negative orders, net > revenue) route to `state="blocked", state_reason="profit_metrics_impossible"` instead of sending. Bad numbers go to Scheduler Health, not the inbox.
- Duck Ops now has a profit-intel data layer (`runtime/profit_intel.py`) that reads workflow_control receipts and computes yesterday's headline + 7-day trend + anomaly readout. The desk panel and full page surfaces consume this data layer (Slice C/D, pending). DuckAgent is the only evaluator of anomaly triggers — Duck Ops reads the result from receipt metadata, never recomputes. Cross-repo contract anchored in `operator_interface_contracts.py::PROFIT_ANOMALY_METADATA_CONTRACT`.

### 4. Review Carousel
- Daily review stories now feed the carousel pool.
- Historical review-story assets were backfilled into the carousel pool.
- Carousel queue is healthy and can build real bundles.
- Email approval -> reply publish -> Instagram scheduling loop is working.
- Duplicate duck selection in the same carousel is blocked.
- Non-official / inconsistent review visuals are filtered out.
- Tuesday launchd automation is installed for carousel approval generation.

### 5. Shopify SEO
- Shopify SEO audit exists and scans products, collections, pages, and articles.
- Email review -> reply apply loop works.
- Missing-only bulk SEO backfill is working and already applied successfully.
- New category-batch workflow is now live for monthly cleanup beyond one top-10 review.
- Morning Shopify SEO kickoff scheduling is now installed locally so the next category email can keep advancing without manual batching.
- Newduck now writes SEO into Shopify instead of only generating it.
- Blog and newduck flows now have stronger SEO validation rules.
- Shopify MCP connectivity groundwork and SEO audit flow are in place.
- Shopify SEO audit now flags low-value SEO copy in addition to generic/duplicate/title-overlap issues.
- Newduck listing policy now enforces internal/external Shopify links and structured Etsy titles.
- Blog, newduck activation, and Shopify SEO apply now record writeback-verification receipts so failed SEO mutations stop looking like clean success.
- Shopify SEO outcomes now summarize verification truth and category-level follow-through so the business desk can show what is reopening, what still needs audit refresh, and what is just aging through the monitoring window.
- Duplicate-title fallback logic now special-cases the privacy-choices page so the chain stops proposing the same privacy title twice.
- The business desk now shows Shopify SEO review-chain status inside a generic approval-chain surface instead of hiding that state inside the raw SEO report only.

### 6. GTDF / Weekly / Review / Creative Health
- GTDF winner now reports upstream blockers honestly instead of fake failures.
- Review execution and trend/health reporting were cleaned up to reduce false bad states.
- Weekly/workflow health is more root-cause-aware than before.
- Blog and ops health now treat healthy idle/backlog states as operator truth instead of fake warnings.
- Etsy review auto-execution cooldown now degrades into a paused lane instead of crashing the sidecar.
- Scheduler health now classifies upstream PhotoRoom quota failures as dependency-blocked warnings instead of scheduler failures.
- Quality-gate control now prunes stale alerts and treats archived/overridden review items as resolved instead of counting them as still pending.
- Business desk creative-review counts now separate currently surfaced items from older backlog so the queue reads more honestly.
- Inventory truth now separates demand-only stock-watch leads from confirmed low-stock evidence so demand cannot silently become a print command.

### 7. Shopify Draft Activation Controls
- Newduck is now a two-step Shopify flow:
  - first reply creates drafts
  - second reply audits Shopify completeness and activates Shopify only
- Weekly Shopify draft activation review now exists with email approval -> reply apply/publish.
- Blocking listing issues are separated from advisory quality suggestions so activation stays operator-friendly.
- Weekly launchd scheduling is installed for the Shopify draft review pass.

### 8. Agent Governance Foundation
- `duck-architecture-guard` skill is now created and validated.
- `duck-change-planner`, `duck-reliability-review`, `duck-data-model-governance`, and `duck-automation-safety` are now created and validated.
- `duck-tech-debt-triage`, `duck-social-insights`, and `duck-competitor-benchmark` are now created and validated.
- `duck-documentation-governance` now exists so canonical roadmap, governance, and current-system docs can be reviewed for drift and cleanup.
- DuckAgent and Duck Ops now have root README front doors plus canonical `AGENTS.md` files so human and AI contributors start from the same boundaries, commands, and safety rules.
- Agent/skill governance policy now exists to define power tiers, recommendation flow, and review/push expectations.
- Engineering governance digest lane now exists and is scheduled for the morning.
- The observe-only engineering review loop is now complete:
  - nightly tech-debt triage
  - weekly reliability review
  - weekly data-model governance review
  - weekly documentation-governance review
- Governance digest email delivery is smoke-tested, so the observe/propose recommendation channel is live.
- Business Desk promotion watch now covers the current approval-policy lanes and uses an explicit autonomy-readiness contract across weekly sale, Meme Monday, Tuesday review carousel, Jeep Fact Wednesday, and Etsy review execution.
- Promotion readiness email and Business Desk sections now repeat owner, current mode, target mode, side effect, allowed tier, approval boundary, and no-self-promotion constraints before any lane can be promoted.
- Promotion readiness notifications now include state-change deltas when a lane moves between observing, blocked, ready, or active.
- Review Inbox now provides a browser fallback for the same review-reply approval loop used by WhatsApp/email, including recent decision receipts and a clearer separation between approving reply quality and supervised Etsy browser posting.
- Business Desk promotion watch now includes an explicit autonomy gate summary that identifies the next candidate, whether the operator must promote it, and confirms that clean streaks cannot self-promote a lane.

### 9. Social Strategy Intelligence Layer
- Own-post social performance collection is live from DuckAgent post receipts.
- Normalized social performance state and rollups are live in Duck Ops.
- Current learnings, competitor benchmarking, and weekly strategy packet outputs are live.
- The business desk now surfaces a weekly social plan with lane-fit reasoning and ready-to-run slot guidance.
- Weekly slots now track recommended lane, alternate lane, actual observed lane, and simple performance follow-through.
- Current learnings now turns weekly slot execution into per-slot feedback so missed, fallback, different-lane, and clean-win outcomes show up as concrete planning guidance.
- Material learning changes now feed a dedicated learnings-change digest and business-desk follow-through action.
- Weekly strategy packet now carries a `What Changed` section so learning shifts are folded back into the weekly plan.

### 10. Product Concept Queue
- Duck Ops now has a Product Concept Queue contract for trend, competitor-learning, and strategy signals.
- The queue writes a DuckAgent-compatible `DesignBriefQueueInput` handoff artifact while keeping image generation, model creation, listing work, and publishing out of Duck Ops.
- Business Desk now surfaces product concepts as ready/watch/blocked and reserves room for blocked guardrail examples instead of hiding risky signals behind ready candidates.
- IP/team/organization-adjacent themes are blocked for manual abstraction before any design brief generation.
- Product concept and OpenClaw review queues now apply a naming-quality gate so raw search-language themes must be reframed before they can become build/design-brief candidates.
- DuckAgent concept runs now support a local-only concept-to-print pilot proof, so one selected run can show its current gate and next cost boundary before external AI calls, Bambu review, or marketplace work.
- Product concept handoff now has an implemented first-slice `trend_quality_gate` + `concept_design_brief` contract so approved trend ideas can carry semantic identity, visual cues, printability guardrails, IP risks, evidence, and style-reference policy before DuckAgent spends image or 3D credits.
- Product concept feedback memory now prevents discarded, skipped, revised, abandoned, or already-approved concepts from resurfacing as fresh weekly design brief options; the weekly design brief job now reads the curated Duck Ops Product Concept Queue handoff by default instead of scraping broad strategy notes.

### 11. Creative Console Productization
- Creative Agent and Decision Inbox now have a canonical UI flow audit that separates operator use cases from internal workflow plumbing.
- Creative Agent starts from outcome cards, with provider/model, dry-run, input-file, and command-preview controls hidden under advanced sections.
- Decision Inbox is now the selected-run front door for finished creative work instead of a raw run viewer.
- A derived `decision_detail` contract now lets the browser show the artifact, decision question, recommendation, blocked reasons, receipts, and safe action before raw logs or pipeline controls.
- Decision Inbox groups pending work into Needs My Decision, Ready To Continue, Blocked, and Recently Completed lanes.
- HMI smoke coverage now checks the local console pages, action registry, decision inbox wiring, upload handling, and latest-run deep links before manual operator testing.

### 12. Cadence Gate Unification & Browser Guard Hardening
- Shared `runtime/email_cadence_gate.py` registry now covers all 8 operator-facing intel surfaces (profit, recommendations, reviews, learnings, competitors, business_intelligence, engineering_governance, shopify_seo). Each surface declares `cadence + bypass_keys + deferred_note`; per-flow env-var configs were retired and a `legacy_env_warning` helper flags stale `.env` lines.
- All 5 original daily intel emails now have matching `/portal/intel/<surface>` pages (profit, recommendations, reviews, learnings, competitors). Each page reads its source state file, uses the shared `_render_portal_shell`, and surfaces the cadence decision (cadence, next_email_at, bypass_active) so the operator can see what was deferred and why.
- Cadence decisions are now appended to `state/email_cadence_decisions.jsonl` — "why didn't today's email fire?" is grep-able.
- Cross-repo `helpers/cadence_gate_loader.py` carries the sibling-checkout import + fail-open pattern for the three duckAgent flows (reviews, profit, competitor) that gate through it.
- New `runtime/workflow_cooldown_sweeper.py` auto-clears stale cooldown-style `workflow_control` failures (>4h old on a whitelisted state_reason). Wired into the sidecar as the first step. Prevents the April 24 → May 26 stuck-state pattern from recurring silently.
- Etsy browser guard `runtime/etsy_browser_guard.py` tuned: local-only Playwright ops (snapshot, state-load, state-save, open, close) no longer count toward `MAX_COMMANDS_PER_WINDOW` (Etsy can't see them); `.click(` inside an eval no longer auto-flagged mutating — the specific marker `submit.click(` is now the trigger. Submission detection still fires correctly; routine inbox sync no longer self-trips.
- New Agent OS card `etsy_browser_guard_health` distinguishes Etsy-imposed blocks (BLOCK_PHRASES → red, manual fix) from self-imposed cooldowns (rate_limit_preemptive_cooldown → yellow, auto-clears).
- Review reply rewriter prompt hardened (commit `e4df8d7`): CRITICAL RULE leads, REJECTED EXAMPLE shows the exact echo_check failure mode from the live log, two-step chain-of-thought forces specific-detail identification before reply generation. Shared `call_openai` now retries 429/5xx with exponential backoff (3 attempts, max ~7s).
- Review-reply auto-drain enabled (`auto_execution_enabled=true`, `auto_drain_max_submits_per_run=2`). Sidecar drain step fires once per day in the afternoon window (13–19) with 0–30 min random jitter and a 20h marker-file cooldown — natural shop-owner rhythm rather than every-6h bot pattern. Operator one-time browser-path approval remains the gating prerequisite.
- New `duck-os-triage` skill (`~/.claude/skills/duck-os-triage/` + `duck-ops/runtime/agent_os_triage.py`) turns any "Repair now" finding on the Agent OS portal into a structured root-cause brief — failure modes by category (prompt / code / data / provider), sample rejected outputs from the call log, and a fix-category recommendation.
- Prompt Contract Audit Phase 0 inventory complete: 7 LLM prompts catalogued across both repos with risk tier and output discipline. See [PROMPT_CONTRACT_AUDIT_PHASE_0_INVENTORY.md](/Users/philtullai/ai-agents/duckAgent/docs/current_system/PROMPT_CONTRACT_AUDIT_PHASE_0_INVENTORY.md). 1 HIGH-risk (review reply rewriter), 5 MEDIUM, 1 LOW. 4 of 7 already use JSON-mode + schema validation; the 3 free-text-with-regex prompts are the Phase 1 targets in order.

## Active Operational Lanes

### 1. Shopify SEO Category Workflow
- Category emails can now be sent in sequence.
- After a successful category apply, DuckAgent can auto-send the next remaining SEO category email.
- Missing-title and missing-description category batches have already applied successfully.
- Duplicate-title is the current open category awaiting review/apply, with the next morning kickoff now installed locally once the chain is ready to advance.

### 2. Etsy Inbox Truth Sync
- Launchd-installed daytime refresh is live.
- Still needs a few cycles of observation to prove long-term stability with manual Etsy replies.
- Customer inbox refresh cooldown/pacing failures now surface as supervised-window retries with no overnight command, so health does not accidentally tell us to reopen Etsy during a cooldown.

### 3. Review Carousel Publishing
- Approval/publish loop is working.
- Tuesday approval scheduling is installed.

### 4. Shopify Draft Activation Review
- Monday review email is installed.
- Reply apply/publish activates only ready Shopify drafts and leaves blocked drafts alone.
- Quality suggestions are surfaced separately from blocking issues.

## Highest-Value Open Work

### First-Party Demand Intelligence (GSC + GA4) — core shipped 2026-06-27, follow-ups queued

Real first-party demand now flows into the system: **GSC** (Google search queries → Build-Next) and **GA4** (per-listing Fix/Promote/Watch, split Etsy vs Shopify). Shipped this initiative (Surfaces 38–42, all pushed): Build-Next 7-day momentum re-rank (16.4); GSC search-demand producer + factor (38); GA4 listing-performance producer (39); SEO generation fed by real demand + golden eval (40); gap→collection planner Stage A (41A); GA4 weekly-sale steering (42, drops PROMOTE winners / floats FIX leaks, behind the sale-policy gate).

Queued follow-ups (none block the shipped logic; most are "make it live on cadence"):
1. **Schedule the producers on launchd (Tier-3 install) + add the deferred two-card OS brackets.** `gsc_search_demand.py`, `ga4_listing_performance.py`, and `sale_steering.py` currently run only on manual trigger; they ship with a 21-day staleness guard that fail-soft DISABLES the consumers if data goes stale. So until they're scheduled weekly, the SEO/Build-Next/sale-steering benefits expire after 21 days. Highest-priority follow-up. Each needs its feed-freshness + throughput bracket (convention #3) added with the schedule.
2. **`/portal/intel/listing-performance` page + Business Desk tiles** — render the GA4 Fix/Promote/Watch lanes + the GSC gap/rising queries so it replaces the GA4 email (email-to-portal inversion). Deferred from Surfaces 38/39.
3. **gap → Build-Next candidate minting** — today GSC gaps only BOOST existing competitor candidates; they never MINT new ones, so "custom ducks for jeeps" demand goes nowhere. Feed high-impression gap queries in as first-class Build-Next candidates (the real "make this next" signal).
4. **SEO L4 outcome measurement** — extend `shopify_seo_outcomes.py` (which today admits it "does not measure organic search lift yet") to compare GSC CTR/position before vs after each rewrite → the system learns which copy lifts clicks (the self-tuning loop).
5. **GA4 engagement→theme feedback loop** — re-weight Thursday/Build-Next theme selection by which themes actually engage (Phase 2 strategic loop; needs a few weeks of history).
6. **gap→collection Stage B** — the gated Shopify `collectionCreate` apply path, deferred until the planner actually surfaces candidates (0 on current data; fires when a category query with ≥3 matching products trends).

### Priority 0: Creative Quality Loop Phase 4.5 — Engagement Write-Back (high-ROI #1, in flight 2026-06-06)
Phase 4 of CREATIVE_QUALITY_LOOP_V2_PLAN.md shipped the canonical receipt directory and wired meme + jeepfact + thursday through `rank_creative_candidates()`. Phase 4.5 closes the loop by feeding real-world engagement back to the receipt so the system measurably gets better at picking creatives over time.

Why this is high value:
- The system already RANKS variants per flow but has NO IDEA which ranked variant actually performed. Every published post is currently a throwaway data point.
- The Learning Inspector (`/portal/intel/learnings`) now surfaces a `signal_gap` saying "4 experiments queued, 0 executed receipts" — that gap stays open until Phase 4.5 lands.
- Plan agent on 2026-06-06 discovered the foundational fix needed first: queued IG posts NEVER get `save_social_post_receipt` called because publish.id is None at queue-time. Fix that → existing engagement collector starts working for the full publish pipeline.

What's planned (full details in CREATIVE_QUALITY_LOOP_V2_PLAN.md "Phase 5: Outcome Write-Back"):
1. Close the IG-queue receipt gap (sidecar writes the receipt after `mark_posted`).
2. Add additive `outcome` block to creative_quality_receipt schema (24h + 7d snapshots).
3. Stamp post_id → run_id link on every published post in meme/jeepfact/thursday.
4. Extend existing `social_performance_collector.py` to write outcomes back to receipts.
5. Surface `executed_experiments_last_14d` in current_learnings + Inspector (currently hardcoded to 0).
6. Tests-first per `/coverage-matrix` — add TESTS.md "Surface 9" row before code.

Effort: ~1.5-2 days. Visible win (signal_gap shrinking on Inspector) ~24h post-deploy after the first publish lands an outcome.

Explicit scope cuts: no ranker retraining yet (Phase 6 will do deterministic re-weighting after 30+ posts of clean data); no Etsy outcomes; no retro backfill of the 9 pre-fix posts.

### Priority 0.5: LLM Cost Ceiling + Per-Flow Spend Dashboard (high-ROI #2, queued)
Wrap every LLM call through a budget tracker. Daily cap, per-flow cap, automatic stop. Adds `/portal/intel/cost` surface showing spend by flow.

Why this is high value:
- Cheap to build (~4-6 hours).
- Eliminates asymmetric downside: one misbehaving Creative Quality Loop run could quietly spend $100.
- Produces the data needed to evaluate Phase 4.5 ROI: "is the loop worth its cost?"

Pairs naturally with Priority 0 (Phase 4.5) — engagement data + cost data together answer "which flows are worth running."

### Priority 0.6: Per-Product Profit Drill-Down (high-ROI #3, queued)
Audit `/portal/intel/profit` to confirm it answers "which ducks make money, which lose money." If not, add per-SKU profit drill-down following the inspector-page recipe.

Why this is high value:
- Directly drives retire/promote/restock decisions.
- ~4-6 hours if the underlying COGS data already exists; longer if joining Etsy + Shopify per-SKU history needs new collectors.

### Priority 0.7: Repeat-Buyer Automation (high-ROI #4, queued)
First-purchase thank-you + 30-day repurchase nudge email/DM. Known D2C revenue lever.

Why this is high value:
- Higher revenue upside than the other three but more moving parts (template design, deliverability, unsubscribe handling).
- ~1-2 days. Recommended only after Priority 0.5 (cost ceiling) is live so the AI-generated nudge copy can't run away.

### Priority 1: Agent OS Promotion Readiness Operationalization
The highest-ROI Agent OS work is now using the Business Desk promotion readiness gate as the explicit bridge between supervised approval lanes and controlled auto-action.

The promotion surface now shows:
1. which lane is being considered
2. who owns the executor
3. what side effect the promoted mode would allow
4. what tier and approval boundary applies
5. whether the lane is observing, blocked, ready, or already active
6. which evidence and config path support the recommendation

Why this is high value:
- Weekly sale, Meme Monday, Tuesday review carousel, and Jeep Fact Wednesday are all converging on the same approval-policy pattern.
- We need one place to answer “is this safe to automate?” instead of per-lane folklore.
- Clean gated runs should notify the operator, not silently become permission to mutate production systems.

Next slices:
1. use the readiness surface to decide whether weekly sale can move from manual email approval to auto-apply after operator approval
2. add the same promotion contract automatically whenever a new approval-policy lane is created
3. keep Email, WhatsApp, Review Inbox, and Business Desk aligned to one canonical approval contract so operator decisions do not fork

### Priority 2: Outcome Learning Layer Expansion
The social learning foundation is now live. The next high-value work is extending that same discipline into the remaining business outcomes:
1. Feed weekly slot execution feedback into current learnings and change detection so the system shows what actually changed week over week.
2. SEO outcome monitoring so we can see whether metadata changes move traffic, clicks, or ranking surfaces.
3. Relist/renew outcome monitoring so we learn what renewal actually pays off.
4. Customer-reply conversion insights so we learn what reply styles lead to orders.
5. Stronger competitor-strategy separation between stable patterns, experiments, and do-not-copy motifs over time.
6. Concept-to-print pilot outcomes so printable, rejected, and needs-revision product ideas become learning evidence instead of isolated experiments.

Why this is high value:
- We now have better workflow truth and safer execution.
- That makes it finally worth learning from outcomes instead of just automating actions.
- It also gives us a disciplined way to borrow strong ideas from competitors instead of guessing when to shift content strategy.
- Business Desk now has an outcome-learning expansion surface that points at the highest-value missing evidence, including SEO traffic proof, own-post outcome coverage, concept-to-print pilots, relist lift, and material learning changes.

### Priority 3: Duck Product Studio / Concept-To-Print Pipeline
The product creation path is becoming a real strategic lane, not just one-off experiments.

Target workflow:
1. Trends, competitor signals, and manual ideas feed a product-concept queue.
2. Duck Ops writes a structured concept design brief with semantic meaning, visual cues, risks, and source evidence.
3. The operator reviews concept framing before image generation.
4. Approved concept images move through semantic/IP policy and product-listing policy.
5. Strong concepts can be handed to 3D AI Studio or the local `paint-to-print-3d` toolchain.
6. Repaired colored model outputs are opened in Bambu Studio for final human print review.
7. Proven winners feed back into Shopify/Etsy listing creation, social launch planning, and outcome learning.

Why this is high value:
- It connects trend discovery to actual sellable inventory instead of stopping at content ideas.
- It creates a reusable path from "interesting signal" to "printable duck" with human approval checkpoints.
- It gives the AI system a safer way to propose new products without silently copying IP-heavy competitor motifs.

Next slices:
1. connect Duck Ops Product Concept Queue review to DuckAgent `design_brief_queue` email generation
2. continue Product Concept Brief Contract Phase 4: add an observe-only regeneration planner for fixable semantic QA failures before allowing any automatic image-credit retry
3. run one local-only concept-to-print pilot proof against a real approved concept run
4. add approval receipts and promotion-readiness history for recurring product concept queue runs
5. add model-quality checks for flat bottom, smoothness, color intent, and Bambu import readiness
6. when ready, run the DuckAgent [Local Image-To-3D Provider Evaluation Plan](/Users/philtullai/ai-agents/duckAgent/docs/current_system/LOCAL_IMAGE_TO_3D_PROVIDER_PLAN.md) to benchmark self-hosted providers before reducing 3D AI Studio dependency

### Priority 4: Social Strategy Hardening
- Improve cross-channel post coverage so Instagram and Facebook outcomes stay comparable when both publish.
- Continue feeding weekly strategy execution truth into current learnings, governance digest, and change-notifier surfaces, with the new per-slot feedback as the operator-facing primitive.
- Turn manual experiments into first-class lanes only after repeated execution and outcome evidence justify it.
- Build Creative Quality Loop v2 as the next creative hardening layer: start with This-or-That Thursday retry-on-warn, comparative ranking, hidden weak-option receipts, and Phil feedback memory, then reuse the loop across character-to-duck, new duck concepts, Meme Monday, Jeep Fact, GTDF, and review-story assets.

### Priority 5: Prompt Contract Audit And Policy Alignment
The next cross-cutting quality improvement is to treat prompts like testable contracts instead of isolated prose instructions.

Target model:
1. each important AI-assisted lane has a master instruction, lane-specific objective, grounded inputs, structured output schema, deterministic policy checks, operator receipt, and regression tests
2. weak or unstructured prompts are inventoried and upgraded by business risk
3. policy failures explain the exact evidence instead of silently producing vague "needs revision" states
4. structured output becomes the bridge between AI reasoning and deterministic workflow control

Why this is high value:
- Review rewrites, listing copy, tag generation, creative concepts, and Business Desk recommendations all depend on AI output quality.
- We now have enough policy and HMI infrastructure to validate outputs instead of relying on one-shot prompt quality.
- The current OpenClaw rewrite failure shows the risk: a mixed five-star review with expectation-mismatch language was treated like a generic positive review.

Next slices:
1. run the [Prompt Contract Audit And Improvement Plan](/Users/philtullai/ai-agents/duckAgent/docs/current_system/PROMPT_CONTRACT_AUDIT_PLAN.md) Phase 0 inventory across DuckAgent and Duck Ops prompts
2. implement Phase 1 for OpenClaw review rewrite with mixed-positive / expectation-mismatch classification and structured rewrite output
3. add regression tests for the known "plastic would have been better" review case
4. move New Duck listing and commerce tag generation to the same structured prompt-and-policy pattern next

### Priority 6: Expand SEO Audit Intelligence
Current audit checks:
- missing SEO title
- missing SEO description
- short SEO title
- long SEO title
- long SEO description
- duplicate SEO title

Best next SEO heuristics to add:
- weak/generic SEO titles
- weak/generic SEO descriptions
- near-duplicate SEO titles
- SEO titles too close to raw product titles
- low-value page/article SEO copy

### Priority 7: Etsy Conversation Closure Truth
- We are much better at discovery and direct links now.
- But manual Etsy replies still depend on the next inbox refresh to be fully recognized as waiting-on-customer or resolved.
- Best next step here is a lightweight recapture/closure reconciliation pass for active customer threads.

### Priority 8: Expired Etsy Relist Lane
- Logic exists for safe relisting rules:
  - max 3 renewals per day
  - only listings with at least one prior sale
- This still needs careful rollout around Etsy bot-sensitivity and browser pacing.

### Priority 9: Operationalize Product Engineering Skills
The reusable skill layer now exists. The next job is to use it consistently instead of letting it sit as documentation.

Key uses next:
1. `duck-social-insights`
   - govern the social performance collector and weekly recommendation packet
2. `duck-competitor-benchmark`
   - govern competitor snapshots and benchmark reporting
3. `duck-tech-debt-triage`
   - feed ranked cleanup work into the morning governance digest
4. `duck-architecture-guard`
   - review cross-repo changes before rollout and before commit
5. `duck-data-model-governance`
   - review new state/output/schema changes before they spread
6. `duck-automation-safety`
   - gate browser-heavy or approval-boundary-sensitive automation changes
7. `duck-reliability-review`
   - review scheduled lanes and degraded health before promotion
8. `duck-change-planner`
   - remain the entry point for major roadmap work
9. `duck-documentation-governance`
   - keep canonical roadmap, governance, and current-system docs clean, current, and non-duplicative as the system evolves

Why this matters:
- DuckAgent and Duck Ops are getting more capable and more complex.
- Skills now give Codex/agents a stable operating manual for recurring work.
- The value now comes from enforcing them in real workflows, not from creating more skill files.

### Priority 10: Operator Visibility For Cadence-Gated Reports — substantially complete

The cadence-gating shipped for competitor and profit emails closed the daily-inbox-noise problem but opened a visibility gap. As of 2026-05-26 the gap is mostly closed; see Section 12 of Completed Major Work for details.

Plan: [PROFIT_INTEL_PANEL_PLAN.md](/Users/philtullai/ai-agents/duckAgent/docs/current_system/PROFIT_INTEL_PANEL_PLAN.md).

Slice status:
1. **Slice B — Weekly roll-up email content.** ✅ Shipped. Profit Monday email now produces a 7-day rollup (`flows/profit/weekly_rollup.py`); anomaly-bypass days preserve single-day content.
2. **Slice C — Business Desk panel + override endpoint.** ✅ Shipped. The 5 portal intel pages (profit, recommendations, reviews, learnings, competitors) and the new Agent OS card pattern surface the cadence decision + next-email-at + freshness on the operator surface. Per-page `metricCard` summary feeds the Desk; per-area `_status_reason` + `_attention_subtype` feeds Agent OS.
3. **Slice D — Full pages at `/portal/intel/<surface>`.** ✅ Shipped. All 5 surfaces have full drill-down pages reading their source state files.
4. **Slice E — Cross-repo doc updates.** ✅ This update (2026-05-26). Plan + roadmap now reflect shipped state.

The pattern has been extended past the original 5 surfaces — business_intelligence, engineering_governance, and shopify_seo were added to the cadence registry in the same shape. Any future report-style email follows the same recipe: portal page + cadence policy + OS card + tests.

## Recommended Next 3 Steps

### 1. Promote Weekly Sale Into The Autonomy Gate (Priority 1 next slice)
- Foundations from the 2026-05-26 work now make this safe: cadence gate covers 8 surfaces, browser guard no longer self-trips on routine sync, workflow_cooldown_sweeper auto-recovers stuck lanes, and the Agent OS now has a guard-source card that distinguishes self-imposed from Etsy-imposed blocks.
- Concrete first move: use the Promotion Readiness gate on Business Desk to advance weekly sale from manual email approval → auto-apply after operator approval, with rollback receipts.
- Pattern model for the other policy/watch/promote lanes (Meme Monday, Tuesday review carousel, Jeep Fact Wednesday) once weekly sale proves the path.

### 2. Phase 1 Of The Prompt Contract Audit (Priority 5)
- Phase 0 inventory complete (2026-05-26) — see [PROMPT_CONTRACT_AUDIT_PHASE_0_INVENTORY.md](/Users/philtullai/ai-agents/duckAgent/docs/current_system/PROMPT_CONTRACT_AUDIT_PHASE_0_INVENTORY.md).
- Phase 1 first pick: refactor `review_reply_rewriter_llm.py` to JSON-mode with `specific_detail_echoed` as a load-bearing schema field instead of a post-hoc `echo_check` regex. This is the only HIGH-risk prompt and the only one writing publicly to Etsy.
- Second and third picks (review reply scorer, jeepfact hint parser) follow the same JSON-mode + schema-validation pattern.

### 3. Turn Learnings Into Stronger Weekly Execution Guidance (carried forward)
- Use the steadier competitor signal plus the new learnings notifier to sharpen the weekly experiment list and promotion-readiness calls.
- Keep the recommendations explicit: what stayed stable, what is worth testing once, and what should not be copied.

## Lower-Priority / Nice-to-Have
- Continue backfilling more exact Etsy `/messages/<id>` URLs.
- Add post-publish verification receipts for blog/newduck SEO fields.
- Improve long-tail product SEO copy quality further for very odd or novelty duck names.

## Legacy Plans Archived

These older plan documents have been superseded by this master roadmap and the now-extracted live flow code:
- competitor refactor plan
- reviews refactor plan
- profit refactor plan
- blog/weekly extraction plan
- newduck extraction plan
- weekly sale rotation plan

Why archive instead of treat them as current:
- the related flow modules already exist and are live
- the roadmap above now carries the current priorities
- keeping all of those older plans in active planning folders would create multiple competing sources of truth

What was preserved conceptually:
- weekly sale strategy ideas were folded into the active weekly/sale lane direction
- extraction/refactor plans are preserved as implementation history, not current roadmap items

## Summary

The system has moved from:
- disconnected scripts
- stale/inferred health
- unsafe message execution

toward:
- explicit workflow control
- safer human approval loops
- clearer operator reporting
- category-based SEO maintenance
- functioning review-carousel publishing

The biggest remaining leap is not another individual workflow fix. It is making the system learn what actually works.

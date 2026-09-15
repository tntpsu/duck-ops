# Duck Ops + DuckAgent Master Roadmap

Last updated: 2026-09-13

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

### 0. Duck Video Promotion Lane (Surface 64, shipped 2026-08-08)
- First video lane: weekly `duckvideo` flow renders a 7s vertical Reel per duck — Blender turntable of the real 3D model (textured Studio GLB preferred) or ken-burns over product photos — with AI backdrop compositing (Jeep dashboard / trail rock / workbench + generated studio card), contact shadow, hook overlay, and a rotating royalty-free music bed (`duckAgent_runtime/duckvideo_audio|_backdrops`).
- Publishes as an IG Reel through the shared social publish queue + sidecar (`media_type=reel`); approval-gated BY CONSTRUCTION (email reply only — the scheduled runner cannot publish).
- New producer: `duck-ops/runtime/product_model_index.py` (daily) joins paint-to-print/Studio models to catalog products deterministically with a needs_review escape.
- Two-card bracket (`duckvideo_input_sanity` + `duckvideo_throughput`), workflow-control transitions, FlowSpec registry entry, TESTS.md Surface 64.
- First live Reel posted 2026-08-14 (Knitted Duck, ken-burns); lane fully autonomous through the email→sidecar path.
- **Track B SHIPPED 2026-08-18 (Surface 64c):** third mode `ai_motion` — Veo 3.1 Fast image-to-video from the real product photo + a curated motion-concept library (`duckAgent/config/duckvideo_motion_concepts.json`, versioned, concept×product dedup). ISO-week mod-3 rotation keeps all three tracks active; failures fall back to ken-burns with a recorded reason; spot frames attach to the review email. Gated eval passed live same day (~$1/clip, `GEMINI_KEY`). First live ai_motion Reel: next rotation week, supervised.
- Queued next (video track): concept-library iteration from reel engagement data; TikTok Content Posting API as phase 2.

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

> **2026-07-11 audit correction** (verified against code + TESTS.md + git; truth order code > TESTS.md > roadmap). This section below lagged reality. Corrections:
> - **Priority 0.6 Per-Product Profit Drill-Down → ✅ SHIPPED.** `runtime/profit_per_product.py` does per-SKU COGS join across Etsy+Shopify → `/portal/intel/profit` (Surface 11, tested). Do not rebuild.
> - **Surface 47 demand-rank Build-Next → ✅ SHIPPED** (`build_next_engine.py` reads competitor `demand_7d`; commit `c8556bf`).
> - **Priority 0.5 LLM Cost Ceiling → ⚠️ PARTIAL.** Per-flow *logging* + `/portal/intel/cost` shipped (Surface 10); the **hard ceiling / auto-stop is NOT built** (deliberate observability-first call 2026-06-06). Open item = enforcement only. **Spec'd 2026-07-11 as TESTS.md Surface 59.**
> - **Priority 2 outcome loops → mostly ❌ NOT BUILT.** SEO outcome monitor verifies the fix *stayed* but has **no traffic tie-back** (`traffic_signal_available_count:0`); **relist lift, reply→conversion, GA4-engagement→theme-selection, and Creative Phase 6 ranker-retrain are all unbuilt** (outcomes are collected, never fed back into selection/ranking).
> - **Priority 0.7 Repeat-Buyer Automation → ❌ NOT BUILT** (no repeat-buyer/repurchase code at all).
> - **NEW GAP found — Customer-Ask Scout → ❌ NOT BUILT.** The product-concept pipeline explicitly excludes reviews + inbox from its 4 scouts, so "what customers literally ask us to make" (168 inbox threads + every review) is unmined demand signal with a fully-built downstream home (concept queue → Studio → Promote gate). **Highest-leverage open work; spec'd 2026-07-11 as TESTS.md Surface 58.**
> - Review-text → per-SKU QA is ⚠️ PARTIAL (shop-wide extraction exists, not per-SKU-routed).

### First-Party Demand Intelligence (GSC + GA4) — core shipped 2026-06-27, follow-ups queued

Real first-party demand now flows into the system: **GSC** (Google search queries → Build-Next) and **GA4** (per-listing Fix/Promote/Watch, split Etsy vs Shopify). Shipped this initiative (Surfaces 38–42, all pushed): Build-Next 7-day momentum re-rank (16.4); GSC search-demand producer + factor (38); GA4 listing-performance producer (39); SEO generation fed by real demand + golden eval (40); gap→collection planner Stage A (41A); GA4 weekly-sale steering (42, drops PROMOTE winners / floats FIX leaks, behind the sale-policy gate).

**Extended 2026-06-28 (Surfaces 45–46, pushed):** competitor leaderboard now ranks on Etsy's EXACT sold-count delta (45, replacing the 4-6× biased quantity-drop proxy); the two competitor comparison buckets ("they sell it you don't" / "same duck they outsell you") re-ranked on a 7d/30d **demand score** (favorites-velocity + sales-proxy × exact-sales credibility) instead of lifetime engagement (46). The demand-ranked gap ducks are the new MINT source for Build-Next (Surface 47, next — see below).

Queued follow-ups (none block the shipped logic; most are "make it live on cadence"):
1. ✅ **DONE 2026-06-28 — Producers scheduled on launchd.** `gsc_search_demand.py` (07:05), `ga4_listing_performance.py` (07:08), `sale_steering.py` (07:13) installed via Tier-3 plists + kickstart-verified through `run_duck_ops_observe_review.sh`. The `first_party_demand_feeds` OS card (duckAgent viewer.py) covers feed-freshness; `scheduler_health` covers did-it-run. (The 21-day staleness guard no longer silently expires the consumers.)
2. **`/portal/intel/listing-performance` page + Business Desk tiles** — render the GA4 Fix/Promote/Watch lanes + the GSC gap/rising queries so it replaces the GA4 email (email-to-portal inversion). Deferred from Surfaces 38/39.
3. **→ NEXT (Surface 47, spec'd 2026-06-28): demand-rank Build-Next on the competitor demand signal.** Plan-agent finding reshaped this: competitor gap ducks ALREADY mint into Build-Next (`assemble_candidates` unions `ducks_to_build`) and the cross-repo read ALREADY exists — the real defect is a single silent drop: gap ducks carry `demand_7d`/`demand_30d` but not `trending_score`, so `_demand_basis` falls through and caps them in the all-time pool, never reading demand. Fix = read `demand_7d` as a third normalization class + a 21-day staleness guard. ~half a day, read-only (no new isolation). The *GSC*-gap → first-class-candidate minting (the original framing here) remains a smaller separate follow-up.
4. **SEO L4 outcome measurement** — extend `shopify_seo_outcomes.py` (which today admits it "does not measure organic search lift yet") to compare GSC CTR/position before vs after each rewrite → the system learns which copy lifts clicks (the self-tuning loop).
5. **GA4 engagement→theme feedback loop** — re-weight Thursday/Build-Next theme selection by which themes actually engage (Phase 2 strategic loop; needs a few weeks of history).
6. **gap→collection Stage B** — the gated Shopify `collectionCreate` apply path, deferred until the planner actually surfaces candidates (0 on current data; fires when a category query with ≥3 matching products trends).

### Data-model integrity — status-field overload (from the 2026-06-27 Surface 43 audit)

Canonical field meanings now live in `duck-ops/STATUS_CONTRACT.md` (owned under `duck-data-model-governance`). The audit found two HIGH-risk debts beyond the shipped Surface 43 display fix:
1. **Persist `handling: auto|manual`** — Surface 43's structural root. Auto-enqueue keys off the overloaded `review_status==pending`; the shipped fix is display-only until `handling` is a real field the auto-enqueue keys off instead. The auto/manual concept currently lives in 3 unsynced places.
2. **~~Cross-repo `decision` mismatch~~ → RESOLVED (false alarm, verified live 2026-06-27).** Every flow's quality_gate `decision` is the canonical `publish_ready`/`needs_revision`/`discard`; the flagged `auto_schedule_allowed`/… values were a separate jeepfact-local scheduling variable, not the gate decision. Downgraded to a LOW naming-hygiene item (rename the jeepfact local `decision`). So the only HIGH data-model debt is persisting `handling`.
Lower: `chain_state` needs `chain_kind` to disambiguate; `available` conflates transient-absence with permanent capability-gap. Rule going forward: a new automation property gets one persisted field every consumer reads — never overload a lifecycle field.

### ~~Priority 0: Creative Quality Loop Phase 4.5 — Engagement Write-Back~~ → SHIPPED (2026-06-06; verified live 2026-06-28)
**All 7 Phase 5 steps shipped** (the roadmap previously mislabeled this "in flight"). Verified 2026-06-28: real receipts finalizing 24h+7d outcomes (`meme_2026-06-15`, `jeepfact_2026-06-10` at `final_7d`), the IG-queue receipt gap closed in the sidecar, all three lanes stamp post_id→run_id, the collector writes outcomes back, `current_learnings.executed_experiments_last_14d` reads real receipts (no longer hardcoded 0), and a Surface 9 OS watchdog card guards the "published-but-0-outcomes" case. The only follow-up — **the cross-repo prod-write three-layer isolation it shipped without — was completed 2026-06-28** (conftest redirects both repos + `_guard_receipt_write` source guard + pollution-audit tests; see TESTS.md Surface 9). **Phase 6 (ranker retraining on 30+ posts of outcome data) is the real next creative-quality step, and is now unblocked** — outcomes are accumulating.

Historical scoping (kept for context):
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

### Priority 0.7: Customer Email Layer — subscriber campaigns + repeat-buyer automation (high-ROI #4, queued; expanded 2026-09-13)
Operator 2026-09-13: "we should be able to do email campaigns in Shopify. A lot of people have subscribed." Slices: (1) read-only subscriber count + what sends today; (2) draft-campaign lane (system drafts subject/copy/featured ducks from demand + profit + occasion calendar, operator approves, send through Shopify Email or an ESP API, never unsupervised); (3) first-purchase thank-you + 30-day repurchase nudge on the same lane. Known D2C revenue lever.

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

**Status 2026-09-13 — the image stage is done (Surfaces 68/68b/68c):** concept images render on GPT Image only (`gpt-image-2`, edits on `gpt-image-1.5`) with the operator's own ChatGPT recipe: `"<subject> as a rubber duck"` + ChatGPT's account of the operator's duck constraints + one rendering sentence (recipe v7, `duckAgent/creative_agent/tools/src/duck_tools/concept_recipe.json`). No reference photos, no Gemini fallback (an outage fails closed), no forced beak; a judge (gpt-4o, literal fields) looks at turn 1 and a second edit runs only for a named logo or thin part (`concept_image_gate.py`). Wired into the concept lane, Studio regenerate, Studio text-only concepts, and Thursday (no style photos attached any more). Eval 2026-09-11/13: gated recipe 19/20 on the golden set, six operator ducks 6/6, two hardest cases 2/2 on v7; the operator's bare words alone scored 2/5 (logos, scene backgrounds). Eight operator-approved GPT Image ducks recorded as `operator_pick` style memory. Eval page: https://claude.ai/code/artifact/d13f0587-3d0b-4377-8308-2f04354a9d23

**Next slices (ordered 2026-09-13 with the operator, "so we do not forget"):**
1. **The printable bundle** (the big one): approved image → 3D AI Studio model (provider `tripo`, works, unused since May) → `paint-to-print-3d` color-region split → sized to 57 mm (2.25 in) → each region mapped to a filament slot in the operator's AMS palette → one folder per duck: the 3MF, a three-angle render of the colored model, and a receipt. Two operator approvals: the image (exists) and the colored-model render before anything prints (new). **Diagnosis done 2026-09-13** (`duckAgent/scripts/print_bundle_pilot.py`, runs under `creative_agent/runtime/runs/studio_assets/3d_lab/print_bundle_pilot_*`): May's Highland Cow GLB reconverts identically to May (6 colors, 122 components, all gates pass, 12 s), and the v7 Highland Cow image went image → 3D AI Studio (tripo p1, 124 s, one credit) → handoff in under three minutes with every gate green (8 colors, 76 components). **What the gates miss:** (a) the colored preview is a mottled brown camouflage, not the 4–5 flat regions of the concept image, because the converter quantizes the model's shaded texture rather than snapping to the concept's palette (May's `_sourceprep_posterize_4` variant was the only "close match"); (b) the 3MF is ~108 "mm" tall by accident of voxel units, the print is 57 mm; (c) no filament/AMS slot mapping; (d) 3D Lab runs never reach `concept_to_print_status.json` (task filter). **2026-09-15 mesh-engine test changed the route:** Tripo P1 caps faces at 20k (the coarse triangles); Tripo 3.1 with `generate_parts` returns the duck as 11 separate meshes (body, head, hair, beak, eyes, horns, wings, ear) in under 3 minutes. One flat color per part from the concept palette replaces texture quantization entirely. **Build order for this slice (revised):** 0) parts route: label parts (geometry heuristics + judge over the part-colored render vs the concept image), assign concept-palette colors per part, per-part repair to closed solids, multi-object 3MF; then 1) palette-guided regions only as the fallback for models without parts: take the 4–6 flat colors from the approved PNG and snap the texture to them, plus a region-cleanliness gate that fails mottled output; 2) `target_height_mm` (57) applied before export and recorded in the manifest; 3) AMS slot mapping from a config of the operator's filament colors, materials named by slot; 4) three-angle colored render for the operator's approval; 5) pilot runs registered in the concept-to-print ledger. Then the Power Ranger from the 2026-09-11 run as the second article. Golden regression on the Highland Cow GLB rides on step 1.
2. **Studio "import concept image" entry point:** the operator makes ducks in ChatGPT daily; a dropped-in PNG should enter the pipeline at the 3D step.
3. **Instrumentation:** revive `concept_to_print_status.json` (frozen since 2026-06-13) and add two OS cards: printable bundles per 30 days, and provider outages (quota/429 over the tail of `llm_call_log.jsonl`; nothing went red when OpenAI credits ran out on 2026-09-09).
4. **Turnaround sheet on GPT Image** (`generate_duck_character_sheet`): consistent front/side/rear views are what the 3D step consumes; ChatGPT's list already states the view rules.
5. **Thursday onto the recipe:** the first GPT Image Thursday batch lands the week of 2026-09-14 for the operator to review; Thursday still uses its own contract prompt, migrate it to the recipe after that review.
6. Small: golden case `headless_horseman` needs "yellow duck body showing under the coat" as a keep-trait; a unit test that drives `_run_concept_bundle` through the gate with a fake registry.
7. Carried forward from before: approval receipts and promotion-readiness history for recurring product concept queue runs; model-quality checks for flat bottom, smoothness, color intent, and Bambu import readiness; when ready, the [Local Image-To-3D Provider Evaluation Plan](/Users/philtullai/ai-agents/duckAgent/docs/current_system/LOCAL_IMAGE_TO_3D_PROVIDER_PLAN.md) before reducing 3D AI Studio dependency.

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

## Recommended Next 3 Steps (refreshed 2026-09-13)

### 1. The printable bundle (Priority 3, slice 1) — start with the Highland Cow
- Approved image → 3D AI Studio → `paint-to-print-3d` color split → 57 mm → AMS filament-slot mapping → one folder (3MF + three-angle colored render + receipt), with the operator approving the colored render before any print.
- The Highland Cow is the test article (last proven duck, 2026-05-22); then the Power Ranger from the 2026-09-11 run. Golden regression on the Highland Cow GLB.

### 1b. Studio "import concept image" + the two instrumentation cards (Priority 3, slices 2–3)
- A ChatGPT PNG enters the pipeline at the 3D step; `concept_to_print_status.json` revived; cards for printable bundles per 30 days and provider outages.

> ~~Demand-rank Build-Next on the competitor demand signal~~ — **SHIPPED 2026-06-28 (Surface 47):** `score_demand` reads the competitor `demand_7d` score first; kept here only so the old step is not rebuilt.

### 2. Measure the September title refresh (due ~2026-10-09) — the first outcome-ledger entry
- 61 Etsy titles/tag sets were rewritten 2026-09-07/09 (state in `duck-ops/state/etsy_title_refresh/`, baseline `baseline_2026-09-07.json`, ledger `etsy_title_refresh_ledger.jsonl`). Thirty days on, compare views, favorites, and sales per listing against the baseline and write the result where the weekly strategy packet and learnings can read it. #20 Tennessee Volunteers is still on its old title (sold out at apply time; `apply_titles.py --apply --ranks 20` once restocked).

### 3. Customer email layer (Priority 0.7, expanded 2026-09-13): subscriber campaigns + repeat-buyer automation
- Operator 2026-09-13: "we should be able to do email campaigns in Shopify. A lot of people have subscribed." First slice is read-only: count subscribed customers through the Admin API and see what sends today (Shopify Email vs an ESP). **Blocked 2026-09-13: the custom app's token lacks the `read_customers` scope** (`customersCount` → ACCESS_DENIED); the operator adds the scope in Shopify admin (Settings → Apps and sales channels → Develop apps → the app → Configuration → Admin API scopes → `read_customers`), then the count and segments work with the same token. Then a draft-campaign lane: the system writes the campaign (subject, copy, featured ducks picked from demand + profit + the occasion calendar), the operator approves, the send goes through Shopify Email or an ESP API; never an unsupervised send. Repeat-buyer automation (first-purchase thank-you, 30-day nudge) rides on the same lane.
- Prerequisite: the spending guardrails below, so AI-written customer copy cannot run away.

### 3b. Meme Monday "replace" — SHIPPED 2026-09-14 (Surface 70)
- Operator: "I don't like the duck we chose… the email can send 5 other duck options and I can reply to replace." The Monday email now lists the chooser's five runners-up; replying `meme replace N` (before 5:30 pm) deletes the scheduled Facebook post, quarantines the Instagram queue entry, regenerates with that duck, and the auto-schedule path reschedules it for 6 pm. CLI twin: `duckAgent/scripts/meme_replace.py`. First live use 2026-09-14 (Alabama → Bigfoot). Follow-up: a product-photo quality filter on the meme pool (the Alabama photo was a rough print).

### 4. Spending guardrails: cost ceiling (Surface 59, spec'd, unbuilt) + provider-outage card
- Two failure modes lived through in September: nothing went red when OpenAI credits ran out (2026-09-09, 72 failed calls), and nothing would stop a runaway lane. The outage card is Priority 3 slice 3; the ceiling is the stop, not just the log.

### 5. Per-listing conversion + public-reply quality (carried from the 2026-09-07 priorities and Priority 5)
- Which listings get views but no sales, so listing-clarity work goes where it pays; then the review-reply rewriter to structured output (the one prompt that writes publicly to Etsy, still mishandling mixed reviews).

> Verified shipped, do not rebuild: per-product profit drill-down (Surface 11), demand-ranked Build-Next (Surface 47), customer-ask scout (Surface 58).

> ~~Promote Weekly Sale Into The Autonomy Gate~~ — **largely moot (2026-06-22):** the operator turned OFF the sale *posts* and the recurring "pick the final weekly sale post" decision; the weekly Shopify SALE already auto-applies and is never gated. The decision this step wanted to automate no longer exists. Independent `sale_posts` toggle lives on `/portal/workflows-status`.

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

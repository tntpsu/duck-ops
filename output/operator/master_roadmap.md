# Duck Ops + DuckAgent Master Roadmap

Last updated: 2026-04-27

Detailed execution sequence:
- [ROADMAP_EXECUTION_SEQUENCE.md](/Users/philtullai/ai-agents/duckAgent/docs/current_system/ROADMAP_EXECUTION_SEQUENCE.md)
- [SOCIAL_PERFORMANCE_EXECUTION_PLAN.md](/Users/philtullai/ai-agents/duckAgent/docs/current_system/SOCIAL_PERFORMANCE_EXECUTION_PLAN.md)

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
- Quality-gate control now prunes stale alerts and treats archived/overridden review items as resolved instead of counting them as still pending.
- Business desk creative-review counts now separate currently surfaced items from older backlog so the queue reads more honestly.

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
- Agent/skill governance policy now exists to define power tiers, recommendation flow, and review/push expectations.
- Engineering governance digest lane now exists and is scheduled for the morning.
- The observe-only engineering review loop is now complete:
  - nightly tech-debt triage
  - weekly reliability review
  - weekly data-model governance review
  - weekly documentation-governance review
- Governance digest email delivery is smoke-tested, so the observe/propose recommendation channel is live.
- Business Desk promotion watch now covers the current approval-policy lanes and is being hardened into the explicit autonomy-readiness gate before any lane graduates toward auto-action.

### 9. Social Strategy Intelligence Layer
- Own-post social performance collection is live from DuckAgent post receipts.
- Normalized social performance state and rollups are live in Duck Ops.
- Current learnings, competitor benchmarking, and weekly strategy packet outputs are live.
- The business desk now surfaces a weekly social plan with lane-fit reasoning and ready-to-run slot guidance.
- Weekly slots now track recommended lane, alternate lane, actual observed lane, and simple performance follow-through.
- Material learning changes now feed a dedicated learnings-change digest and business-desk follow-through action.
- Weekly strategy packet now carries a `What Changed` section so learning shifts are folded back into the weekly plan.

## Active Operational Lanes

### 1. Shopify SEO Category Workflow
- Category emails can now be sent in sequence.
- After a successful category apply, DuckAgent can auto-send the next remaining SEO category email.
- Missing-title and missing-description category batches have already applied successfully.
- Duplicate-title is the current open category awaiting review/apply, with the next morning kickoff now installed locally once the chain is ready to advance.

### 2. Etsy Inbox Truth Sync
- Launchd-installed daytime refresh is live.
- Still needs a few cycles of observation to prove long-term stability with manual Etsy replies.

### 3. Review Carousel Publishing
- Approval/publish loop is working.
- Tuesday approval scheduling is installed.

### 4. Shopify Draft Activation Review
- Monday review email is installed.
- Reply apply/publish activates only ready Shopify drafts and leaves blocked drafts alone.
- Quality suggestions are surfaced separately from blocking issues.

## Highest-Value Open Work

### Priority 1: Agent OS Promotion Readiness Gate
The highest-ROI Agent OS work is making Business Desk promotion readiness the explicit gate between supervised approval lanes and controlled auto-action.

The promotion surface should show:
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

### Priority 2: Outcome Learning Layer Expansion
The social learning foundation is now live. The next high-value work is extending that same discipline into the remaining business outcomes:
1. Feed weekly slot execution feedback into current learnings and change detection so the system shows what actually changed week over week.
2. SEO outcome monitoring so we can see whether metadata changes move traffic, clicks, or ranking surfaces.
3. Relist/renew outcome monitoring so we learn what renewal actually pays off.
4. Customer-reply conversion insights so we learn what reply styles lead to orders.
5. Stronger competitor-strategy separation between stable patterns, experiments, and do-not-copy motifs over time.

Why this is high value:
- We now have better workflow truth and safer execution.
- That makes it finally worth learning from outcomes instead of just automating actions.
- It also gives us a disciplined way to borrow strong ideas from competitors instead of guessing when to shift content strategy.

### Priority 3: Social Strategy Hardening
- Improve cross-channel post coverage so Instagram and Facebook outcomes stay comparable when both publish.
- Feed weekly strategy execution truth into current learnings, governance digest, and change-notifier surfaces.
- Turn manual experiments into first-class lanes only after repeated execution and outcome evidence justify it.

### Priority 4: Expand SEO Audit Intelligence
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

### Priority 5: Etsy Conversation Closure Truth
- We are much better at discovery and direct links now.
- But manual Etsy replies still depend on the next inbox refresh to be fully recognized as waiting-on-customer or resolved.
- Best next step here is a lightweight recapture/closure reconciliation pass for active customer threads.

### Priority 6: Expired Etsy Relist Lane
- Logic exists for safe relisting rules:
  - max 3 renewals per day
  - only listings with at least one prior sale
- This still needs careful rollout around Etsy bot-sensitivity and browser pacing.

### Priority 7: Operationalize Product Engineering Skills
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

## Recommended Next 3 Steps

### 1. Resume Review / Reliability Hardening In Safe Windows
- When Etsy access is safe again, return to `Review Execution` stabilization and stronger failure breadcrumbs.
- Keep this in observe-first mode so the operator surface stays trustworthy.

### 2. Turn Learnings Into Stronger Weekly Execution Guidance
- Use the steadier competitor signal plus the new learnings notifier to sharpen the weekly experiment list and promotion-readiness calls.
- Keep the recommendations explicit: what stayed stable, what is worth testing once, and what should not be copied.

### 3. Harden Promotion Watch Into The Autonomy Gate
- Weekly sale, Meme Monday, Tuesday review carousel, and Jeep Fact Wednesday now share the policy/watch/promote pattern.
- The next step is to ensure every promotion candidate names the owner, side effect, allowed tier, approval boundary, current mode, target mode, and source config before the operator is asked to promote anything.
- Keep the same model: supervised first, evidence in workflow control, promotion surfaced in the business desk, and default-off until the operator explicitly approves the mode change.

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

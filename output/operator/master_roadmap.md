# Duck Ops + DuckAgent Master Roadmap

Last updated: 2026-04-15

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
- Newduck now writes SEO into Shopify instead of only generating it.
- Blog and newduck flows now have stronger SEO validation rules.
- Shopify MCP connectivity groundwork and SEO audit flow are in place.

### 6. GTDF / Weekly / Review / Creative Health
- GTDF winner now reports upstream blockers honestly instead of fake failures.
- Review execution and trend/health reporting were cleaned up to reduce false bad states.
- Weekly/workflow health is more root-cause-aware than before.

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
- Agent/skill governance policy now exists to define power tiers, recommendation flow, and review/push expectations.
- Engineering governance digest lane now exists and is scheduled for the morning.
- Governance digest email delivery is smoke-tested, so the observe/propose recommendation channel is live.

## Active Operational Lanes

### 1. Shopify SEO Category Workflow
- Category emails can now be sent in sequence.
- After a successful category apply, DuckAgent can auto-send the next remaining SEO category email.
- First category batch sent: Missing SEO titles.

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

### Priority 1: Performance Learning Layer
These are the six items we discussed that should become the next major learning system:
1. Post-performance collector for Facebook and Instagram.
2. Normalized performance state/warehouse for social results.
3. Weekly operator summary for best post times, best content types, and best duck categories.
4. SEO outcome monitoring so we can see whether metadata changes move traffic or clicks.
5. Relist/renew outcome monitoring so we learn what renewal actually pays off.
6. Customer-reply conversion insights so we learn what reply styles lead to orders.
7. Competitor-post benchmarking so we can compare our post cadence, formats, hooks, and engagement patterns against similar shops/accounts.

Why this is high value:
- We now have better workflow truth and safer execution.
- That makes it finally worth learning from outcomes instead of just automating actions.
- It also gives us a disciplined way to borrow strong ideas from competitors instead of guessing when to shift content strategy.

### Priority 2: Social Performance Observability
- Facebook page/token wiring is now fixed enough for scheduled-post smoke testing.
- The remaining gap is not auth anymore; it is collecting post outcomes reliably from both Instagram and Facebook after publish.
- Best next step is to turn cross-channel posting truth into a normalized performance warehouse instead of waiting on manual inspection.

### Priority 3: Expand SEO Audit Intelligence
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

### Priority 4: Etsy Conversation Closure Truth
- We are much better at discovery and direct links now.
- But manual Etsy replies still depend on the next inbox refresh to be fully recognized as waiting-on-customer or resolved.
- Best next step here is a lightweight recapture/closure reconciliation pass for active customer threads.

### Priority 5: Expired Etsy Relist Lane
- Logic exists for safe relisting rules:
  - max 3 renewals per day
  - only listings with at least one prior sale
- This still needs careful rollout around Etsy bot-sensitivity and browser pacing.

### Priority 6: Operationalize Product Engineering Skills
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

Why this matters:
- DuckAgent and Duck Ops are getting more capable and more complex.
- Skills now give Codex/agents a stable operating manual for recurring work.
- The value now comes from enforcing them in real workflows, not from creating more skill files.

## Recommended Next 3 Steps

### 1. Finish Phase 2A Health Stabilization
- Clean up or honestly relabel the biggest degraded lanes first:
  - `Review Execution`
  - `Weekly Coordination`
  - `Gtdf`
  - `Gtdf Winner`
- This keeps the operator surface trustworthy before we add a new insights layer.

### 2. Build the Own-Post Social Performance Foundation
- Reuse existing DuckAgent post receipts in `runs/*/*_posts.json`.
- Pull performance back from Instagram/Facebook after posts go live.
- Normalize by post type, duck family, caption, hashtags, and publish time.
- The concrete phase plan now lives in [SOCIAL_PERFORMANCE_EXECUTION_PLAN.md](/Users/philtullai/ai-agents/duckAgent/docs/current_system/SOCIAL_PERFORMANCE_EXECUTION_PLAN.md).

### 3. Build Weekly Social And Competitor Reports
- Turn the new `duck-social-insights` and `duck-competitor-benchmark` skills into real weekly output lanes.
- Keep them observe/propose only at first and route findings into the governance/operator surfaces.

### 4. Add Smarter SEO Heuristics
- Expand beyond missing/length/duplicate checks.
- Keep the same category email + apply workflow.

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

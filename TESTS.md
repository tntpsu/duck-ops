# TESTS — Coverage Matrix (Duck Ops + DuckAgent)

Last updated: 2026-07-08 (Surface 55 — review-reply ingestion repair, SPEC-first)

Built by running `/coverage-matrix` against the 2026-05-25 → 2026-05-26 shipped work after the operator surfaced that some integration-boundary tests had been skipped. This matrix lives next to [master_roadmap.md](output/operator/master_roadmap.md) per the skill's convention.

Each row is a use case shipped this cycle. Each cell is one of:
- ✅ `<test_file>` — covered by an automated test
- ⚠️  `manual:<reason>` — covered only by manual operator action
- 🔴 `MISSING` — no test, no manual coverage, should be added
- — `n/a:<reason>` — doesn't apply (e.g., n/a:config-only)

---

## Surface 1 — Carousel-to-portal integration (commits 125a694, 26553ed, 8fa77ee)

| Use case | Happy path | `run_id` parser bug (returns "outputs") | SMTP creds missing | Concurrent observer + email-reply race | Operator clicks Approve twice | Operator clicks Reject after carousel scheduled | META not configured |
|---|---|---|---|---|---|---|---|
| Observer discovers carousel in publish_candidates | ✅ test_phase1_observer_review_carousel.py | ✅ same file (RunIdParserTests×2) | n/a | ⚠️  manual:cron-cadence-race | n/a | n/a | n/a |
| Portal Approve → Instagram schedule | ✅ test_review_carousel_publish_contract.py | ✅ widget_api_carousel_reject (subject pin) | ✅ widget_api_carousel_reject | ✅ contract test idempotency (race_detected flag) | ✅ contract test idempotency | ✅ test_review_carousel_reset.py::test_reject_after_schedule_is_no_op | ✅ contract test policy-block path |
| Portal Reject → emit needs_changes email | ✅ test_widget_api_carousel_reject.py | ✅ same file (RUN: assertion) | ✅ same file (fail-soft) | ✅ idempotency below | n/a:reject-idempotent | ✅ reset_no_op test | n/a |
| Email-reply `publish` → publish helper | ✅ test_main_agent_carousel_dispatch.py | n/a:already-tested-at-observer | n/a | ✅ publish helper race_detected | ✅ publish helper idempotent | ✅ reset_no_op test | n/a |
| Email-reply `needs_changes` / `reject` → reset helper | ✅ test_main_agent_carousel_dispatch.py | n/a | n/a | ✅ reset blocks gracefully if already scheduled | ⚠️  manual:reset-after-reset-is-safe-but-untested | ✅ reset_no_op test | n/a |
| Reset helper clears pending + rebuilds queue | ✅ test_review_carousel_reset.py | n/a | n/a | ✅ blocks if already scheduled | ✅ rebuild is idempotent | ✅ same | n/a |
| Carousel state reads correctly into Agent OS | ⚠️  manual:portal-render-check | n/a:run_id-not-rendered | n/a | n/a | n/a | n/a | n/a |

**Gap summary (updated 2026-05-26 after closing two product calls):**

- ✅ **CLOSED: Concurrent observer + email-reply race.** Product decision 2A: when a duplicate publish request hits an already-scheduled run, return `race_detected: true` with explicit "race detected" messaging so the operator sees their second click was ignored. Updated `publish_review_carousel_run`'s already-scheduled summary; updated two existing contract assertions to pin the new wording.

- ✅ **CLOSED: Reject after schedule.** Product decision 1A: reject is a no-op once the carousel is already scheduled. `reset_review_carousel_run` now reads `publish_result.json` upfront; if status is scheduled/published_now/published, returns `{status: "blocked", summary: "already scheduled — reject is a no-op", scheduled_for}` WITHOUT firing a workflow_control transition (so the stale-approvals card doesn't re-surface it). New test `test_reject_after_schedule_is_no_op`.

Remaining `manual:cron-cadence-race` cells (observer discovery only): the observer running while the helper writes publish_result.json mid-cycle is genuinely a low-impact case — observer just re-reads and gets the newer state next tick. Verdict: stays manual.

---

## Surface 2 — Cadence gate (8 surfaces, commits 83bce66 → 8afde1c)

| Surface | Cadence policy | Bypass keys | Per-surface email send wired | Decision log |
|---|---|---|---|---|
| profit | ✅ test_email_cadence_gate.py | ✅ same | ✅ test_profit_email_cadence.py (10) | ✅ |
| recommendations | ✅ | ✅ | ✅ test_workflow_freshness_controls.py (via ops/steps.py wiring) | ✅ |
| reviews | ✅ | ✅ | ✅ test_reviews_email_cadence_gate.py (4) | ✅ |
| learnings | ✅ | ✅ | ✅ test_notifier_cadence_gate.py (4) | ✅ |
| competitors | ✅ | ✅ | ✅ test_competitor_email_cadence.py (4) | ✅ |
| business_intelligence | ✅ | ✅ | ⚠️  manual:ops-flow-wired-but-no-direct-test | ✅ |
| engineering_governance | ✅ | ✅ | ⚠️  manual:wired-via-shipped-2844c0a | ✅ |
| shopify_seo | ✅ | ✅ | ⚠️  manual:wired-via-shipped-2844c0a | ✅ |

**Coverage:** 5 of 8 surfaces have direct send-site tests. The other 3 (business_intelligence, engineering_governance, shopify_seo) have policy tests + wire-up but no test that exercises the per-flow send-with-bypass. **Verdict:** acceptable — the policy registry test pins the cadence logic, and each send site is a small `if not should_send(...): return` guard. Risk of regression is low. Mark in /retro for follow-up if a stale email surfaces.

---

## Surface 3 — Browser guard hardening (commit 8bbe1a0, proposal A+B)

| Behavior | Happy | Local-only ops excluded | `.click(` no longer mutating | `submit.click(` still mutating | Real burst trips |
|---|---|---|---|---|---|
| `_is_etsy_visible` filter | ✅ test_etsy_browser_guard.py | ✅ same | n/a | n/a | n/a |
| `_is_mutating_command` heuristic | ✅ same | n/a | ✅ same | ✅ same | n/a |
| `before_command` burst check | ✅ same | ✅ same | n/a | n/a | ✅ same (test_burst_still_trips_on_visible_commands) |

**Coverage:** 11 tests in test_etsy_browser_guard.py cover every documented behavior path. Solid.

---

## Surface 4 — Workflow cooldown sweeper (commit a1b3d1e)

| Behavior | Whitelist | Stale threshold | Writeback shape | Resilience |
|---|---|---|---|---|
| Whitelisted `refresh_failed` swept | ✅ test_workflow_cooldown_sweeper.py | ✅ | ✅ | n/a |
| Whitelisted `browser_batch_failed` swept | ✅ | n/a | ✅ | n/a |
| `auth_blocked` NEVER swept (requires operator) | ✅ | n/a | n/a | n/a |
| `execution_failed` NEVER swept | ✅ | n/a | n/a | n/a |
| `manual_intervention_required` NEVER swept | ✅ | n/a | n/a | n/a |
| `blocked_by_upstream` NEVER swept | ✅ | n/a | n/a | n/a |
| Recent failure not swept (under threshold) | n/a | ✅ | n/a | n/a |
| Threshold boundary (4h+ε) | n/a | ✅ | n/a | n/a |
| History appended (not replaced) | n/a | n/a | ✅ | n/a |
| State transitions to observed/cooldown_expired | n/a | n/a | ✅ | n/a |
| Dry-run doesn't write | n/a | n/a | ✅ | n/a |
| Malformed JSON skipped | n/a | n/a | n/a | ✅ |
| Missing updated_at skipped | n/a | n/a | n/a | ✅ |
| Missing dir returns empty | n/a | n/a | n/a | ✅ |

**Coverage:** 14 tests. Tight.

---

## Surface 5 — OS cards (commits 7b01e56, 806ab3f)

| Card | Status path: red | Status path: yellow | Status path: green | Filter excludes wrong kind | Card surfaces structured fields |
|---|---|---|---|---|---|
| etsy_browser_guard_health | ✅ test_etsy_browser_guard_health.py (etsy_block) | ✅ same (self_guard) | ✅ same (expired + activity) | ✅ visible-event filter | ✅ same |
| stale_approvals_health | ✅ test_stale_approvals_health.py (count>=10 OR oldest>30d) | ✅ same (small/recent) | ✅ same (empty) | ✅ awaiting_customer excluded | ✅ category grouping + top-5-oldest |

**Coverage:** 16 tests (6+10). Both cards have every status path pinned and the load-bearing filter rules (awaiting_customer must NOT count toward operator backlog) are explicit asserts.

---

## Surface 6 — Review Reply Rewriter fix (commit e4df8d7)

| Behavior | Happy | First-try success | Transient retry | Exhausted retries | Non-retryable (400) | Prompt has CRITICAL RULE |
|---|---|---|---|---|---|---|
| `call_openai` retry | n/a | ✅ test_llm_call_helpers_retry.py (no-retry) | ✅ same (500→success) | ✅ same (3×500) | ✅ same (400 immediate) | n/a |
| 429 retried | n/a | n/a | ✅ same | n/a | n/a | n/a |
| Connection exception retried | n/a | n/a | ✅ same | n/a | n/a | n/a |
| Rewriter prompt strengthened | ✅ test_review_reply_rewriter_llm.py | n/a | n/a | n/a | n/a | ✅ same (asserts on "MUST reference") |

**Coverage:** Retry layer is fully covered. Prompt hardening is pinned by one assertion. Operator-feedback regression coverage (real-world rejected outputs) is **MISSING** — would need a fixture-based test reading the actual rejected_output_text samples from the call log. Verdict: **acceptable for now** — the prompt patch IS live and the next 7 days of call-log data will reveal whether the failure rate dropped. If it doesn't, add the fixture test.

---

## Surface 7 — agent_os_triage skill (commit eb6f08d)

| Behavior | Area selection | JSONL tally | Classifier | Rendered brief |
|---|---|---|---|---|
| Area-specific lookup | ✅ test_agent_os_triage.py | n/a | n/a | n/a |
| All-red filter | ✅ same | n/a | n/a | n/a |
| Include-warn | ✅ same | n/a | n/a | n/a |
| Unknown area raises | ✅ same | n/a | n/a | n/a |
| Counts by outcome | n/a | ✅ same | n/a | n/a |
| Sample cap per mode | n/a | ✅ same | n/a | n/a |
| Missing log handled | n/a | ✅ same | n/a | n/a |
| Malformed lines skipped | n/a | ✅ same | n/a | n/a |
| http→provider | n/a | n/a | ✅ same | n/a |
| echo→prompt | n/a | n/a | ✅ same | n/a |
| unparseable→code | n/a | n/a | ✅ same | n/a |
| Rendered brief shape | n/a | n/a | n/a | ✅ same |

**Coverage:** 18 tests covering every documented analyzer + classifier branch.

---

## Cross-dimension summary (today's work)

| Dimension | Coverage | Notes |
|---|---|---|
| **Static** | ✅ | pytest discovery passes; all imports resolve; no Python syntax errors |
| **Unit** | ✅ 90+ new tests today across both repos | duck-ops: 527 → ~533 passing; duckAgent: 338 → ~347 passing |
| **Integration** | ✅ today's backfill (widget_api ↔ email, main_agent ↔ helper, observer ↔ extractor) | The three I missed initially are now covered |
| **End-to-end** | ⚠️  manual:portal-UI | No automated browser-driven test that walks `/portal/decisions` → click Approve → workflow_control transition. Would need Playwright harness. |
| **Hardware** | n/a | This stack is Python automation; no glasses/devices |
| **Performance** | ⚠️  manual:observed | Sidecar cycle ~14min for phase1_observer (Etsy browser ops). No regression budget set. |
| **Stress** | 🔴 MISSING | No soak test for prolonged sidecar runs or accumulating publish_candidates. |
| **Security** | ✅ `scripts/secret_scan.py --all` | Tracked-file secret-leak scanner. Zero install (pure stdlib + git). Last clean scan 2026-05-26: 795 files, 0 findings across duck-ops + duckAgent + paint-to-print-3d. Run manually before commits or as part of `/retro`. Upgrade path documented in script docstring (swap to gitleaks if pattern coverage needs expansion). |
| **Privacy** | ⚠️  manual | Customer message capture is staged-for-approval only; no automated check that ensures no PII leaks to logs. |
| **Compatibility** | n/a | Single-machine deployment |
| **Migration** | ✅ workflow_cooldown_sweeper auto-clears legacy stuck states | Today's bulk-dismiss of 46 stuck items is a one-time data migration |
| **Network failure modes** | ✅ retry layer (today) + cooldown sweeper | Provider 5xx, rate limits, request exceptions all retried |
| **Build / release** | ✅ git commits + pushes confirmed across 4 repos | All branches sync'd to remote |
| **Documentation** | ✅ doc sync committed (baa4b1b) + plan persisted (436b97c) | Roadmap now reflects today's shipped state |
| **Regression (each closed bug → test)** | ✅ 3 backfilled regressions for the test discipline gap; cooldown sweeper IS a regression test for the April 24→May 26 stuck state | Today's process failure (skipped integration tests) is itself memorialized as a memory rule |

---

## Empty cells / known gaps

**Realistic remediation queue, in order of leverage:**

1. **🔴 Portal UI end-to-end** — a Playwright test that opens `/portal/decisions`, clicks Approve on a fixture carousel row, asserts workflow_control transitions correctly. Highest-value missing test; would catch the entire portal→email→helper chain breaking. Estimate: 4-6 hours including Playwright harness setup.
2. ✅ **CLOSED 2026-05-26: Reject-after-schedule** — product decision 1A + `test_reject_after_schedule_is_no_op` pins it.
3. ✅ **CLOSED 2026-05-26: Concurrent observer + email-reply race** — product decision 2A + race_detected flag on `publish_review_carousel_run`.
4. **🔴 Stress / soak** — sidecar runs every 6h × multiple weeks. publish_candidates.json grows. Run a synthetic week-of-data soak test once.
5. ✅ **CLOSED 2026-05-26: Secret-leak automation** — `scripts/secret_scan.py --all`. Zero install, scans tracked files via curated regex patterns. Clean across duck-ops/duckAgent/paint-to-print-3d (795 files). Manual invocation before commits; gitleaks upgrade path documented if patterns need expanding.

---

## Surface 5 — OS observability two-card bracket (commits f4b3ec9, c60d472, in duckAgent)

The 2026-05-31 audit found a 7-week silent outage on Etsy review-reply posting that NO existing OS card had caught. Two new cards shipped to bracket the failure modes — input filter sanity + output throughput. See [feedback-two-card-observability-bracket](file:///Users/philtullai/.claude/projects/-Users-philtullai-ai-agents/memory/feedback_two_card_observability_bracket.md).

| Card | Happy (green) | Specific failure shape (red) | Stale data outside window | Cold start (idle) | Fail-soft on missing dependency |
|---|---|---|---|---|---|
| `review_reply_throughput` | ✅ `test_review_reply_throughput_green_when_posts_keep_pace` | ✅ `test_review_reply_throughput_red_when_approvals_with_zero_posts` (pins exact 2026-05-31 shape: 12 approved / 0 posted → RED) | ✅ `test_review_reply_throughput_excludes_receipts_outside_window` | ✅ `test_review_reply_throughput_green_when_idle` | ✅ `available=False` path returns yellow with "throughput can't be evaluated" |
| `filter_sanity` — drain split-brain check | ✅ `test_filter_sanity_split_brain_green_when_queue_mirrors_workflow_control` | ✅ `test_filter_sanity_split_brain_red_when_workflow_control_has_more_than_queue` (5 wc receipts / 0 queue items → RED) | n/a — instantaneous check, no window | n/a — covered by aggregate green-when-all-checks-green | ✅ each check wrapped in `try/except` so card degrades but doesn't crash OS surface |
| `filter_sanity` — test pollution check | ✅ `test_filter_sanity_green_with_clean_log` | ✅ `test_filter_sanity_test_pollution_detected_in_llm_log` (3+ entries with `test::` aid or body=`"boom"` → RED, names `conftest.py:_redirect_llm_call_log` as inspection target) | n/a | n/a | ✅ missing log file = check absent, doesn't trip |
| `filter_sanity` — meme filter check | ⚠️  manual:live-only (needs runs/ dir with state_meme.json + duckAgent on sys.path) — live-verified 2026-06-02 returns 1 product id for gym-girl-duck | ✅ pinned at unit level in `duckAgent/tests/test_meme_recently_used_filter.py::test_reads_per_flow_state_meme_json` (and 4 sister tests; the filter_sanity card uses the same function) | n/a | n/a | ✅ Import failure routes to yellow |

**Coverage:** 5 throughput tests + 4 filter_sanity tests + 5 meme-filter tests = 14 pinned today. Both cards live-verified post-deploy: throughput reads RED ("35 approved + 0 posted") with the exact diagnostic; filter_sanity reads GREEN (all 3 checks pass, proving yesterday's three fixes are stable). If the meme-filter regression test breaks, the OS card flips RED within ~6h.

**Gap:** the meme-filter check's "broken" path doesn't have a live OS-card test (only the unit-level test in the duckAgent file). If `_get_recently_used_products` regresses, the unit test fails before the OS card test would. Verdict: acceptable — the unit test pins the filter; the OS card pins the cross-system wiring.

---

## Surface 6 — Workflows card + Tier-3 off switch (2026-06-03)

The Workflows card on the operator desk + portal shows all 7 lane/manual flows (weekly_sale, meme, review_carousel, jeepfact, thursday, gtdf, blog) with their current mode (auto / approval_gated / manual / off), streak progress toward auto-promotion (for approval-gated lanes), and a Tier 3 off switch. The off switch is a production-mutation per AGENT_GOVERNANCE_POLICY.md — operator must type a reason + tick a confirm checkbox to flip.

| Use case | Card surface output | Off-switch flip | Off-mode short-circuit in flow runner | Boundary rejection on missing confirm | Boundary rejection on missing reason | Atomic write safety |
|---|---|---|---|---|---|---|
| 4 promotion lanes (weekly_sale, meme, review_carousel, jeepfact) | ✅ `test_card_lists_all_seven_flows`, `test_gated_lane_with_clean_streak_shows_progress_toward_promotion`, `test_auto_lane_shows_auto_label_and_green_dot` | ✅ `test_off_flip_with_valid_confirm_persists_to_disk` | ✅ `test_run_all_short_circuits_on_off_mode`, `test_run_only_short_circuits_on_off_mode`, `test_run_from_short_circuits_on_off_mode` (parametrized across all 7 + gtdf_winner) | ✅ `test_missing_operator_confirm_is_rejected` | ✅ `test_empty_reason_is_rejected` | ✅ `test_atomic_write_does_not_leave_corrupted_state` |
| 3 manual flows (thursday, gtdf, blog) | ✅ `test_manual_flow_shows_no_streak_progress` (pins no "clean gated" copy on manual rows) | ✅ same helper | ✅ same guard | ✅ same | ✅ same | ✅ same |
| Off-mode display | ✅ `test_off_mode_shows_red_dot_and_off_label` | n/a | n/a | n/a | n/a | n/a |
| Counts / aggregate | ✅ `test_counts_match_flow_states` (mixed off/auto/gated/manual) | n/a | n/a | n/a | n/a | n/a |
| Mutation endpoint per flow | ✅ `test_mutation_endpoint_url_pattern_is_per_flow` | ✅ via portal SPA POST | n/a | n/a | n/a | n/a |
| Audit trail (operator_mode_history) | n/a | ✅ `test_off_flip_with_valid_confirm_persists_to_disk` asserts `from`/`to`/`reason` | n/a | n/a | n/a | ✅ `test_audit_history_capped_at_32_entries` |
| Illegal mode for flow kind | n/a | ✅ `test_illegal_mode_for_manual_flow_is_rejected` (thursday cannot flip to auto_apply_shopify) | n/a | n/a | n/a | n/a |
| Unknown flow | n/a | ✅ `test_unknown_flow_is_rejected` | ✅ `test_unknown_flow_does_not_short_circuit` (no config = treat as on) | n/a | n/a | n/a |
| LanePolicyConfig registry coverage | ✅ `test_lane_policy_registry_contains_all_known_lanes` (extended to include all 7) | n/a | n/a | n/a | n/a | n/a |
| Contract: every config has required fields | ✅ `test_every_registered_lane_has_required_contract_fields` (no_auto_progression branch) | n/a | n/a | n/a | n/a | n/a |
| Portal SPA renders dot + label + switch | ⚠️ manual:portal-render-check (verify in browser after launchctl kickstart -k) | ⚠️ manual:portal-confirm-modal | n/a | n/a | n/a | n/a |
| Markdown ⇔ portal-SPA parity (same dict) | ✅ both consume `build_workflows_card_surface()` directly; no parallel path can drift | n/a | n/a | n/a | n/a | n/a |
| OS observability bracket | ⚠️ gap — no "flows stuck off > 7 days" watchdog yet; ⚠️ gap — no "flows skipped_off count last 24h" throughput | n/a | n/a | n/a | n/a | n/a |

**Coverage:** 9 card-surface tests + 9 mutation-helper tests + 5 main_agent off-guard tests + 6 contract tests = 29 pinned. Cross-repo: tests live in both `duck-ops/tests/test_workflows_card_surface.py` and `duckAgent/tests/test_main_agent_off_mode_guard.py` + `duckAgent/creative_agent/runtime/tests/test_workflows_card_mutation.py`.

**Known gaps (acceptable for ship):**
- Portal UI render is `manual:portal-render-check`. The Tier 3 confirm modal logic is exercised at the helper level (operator_confirm boundary) but the modal HTML itself is not Playwright-tested.
- No OS observability cards yet for "flows stuck off too long" or "skipped_off throughput." Per the two-card-bracket memory, both should be added before any flow is left off for > 1 week.

---

## Surface 7 — IG-side our-own scheduling queue (2026-06-04)

**Background:** verified 2026-06-03 that Meta's IG Graph API silently ignores the `scheduled_publish_time` field for most app permission tiers. The jeepfact post scheduled for 6pm went live at 10:19am (the moment of the API call). All five social lanes (jeepfact, meme, gtdf, gtdf_winner, blog, review_carousel) are affected. See `duckAgent/KNOWN_QUIRKS.md` for the dated incident record.

**Fix shape:** a per-flow local queue (`duckAgent/states/social_publish_queue/`) + a launchd sidecar (`scripts/social_publish_due.py`, every 10 min). Lane publish steps pass `lane`+`run_id` to the IG schedule helpers; if scheduled_unix > now+60s, the helpers enqueue instead of calling Meta. Sidecar wakes, finds due entries, and publishes via `bypass_queue=True`.

| Use case | Enqueue contract | Due-entry filter | Lifecycle (post / fail / quarantine) | Helper routing (queue vs Meta) | OS observability (overdue lag) | Malformed entry tolerance |
|---|---|---|---|---|---|---|
| jeepfact, meme, gtdf, gtdf_winner, blog (helpers/meta_helper.py) | ✅ `EnqueueIgPostContractTests` (5 tests: valid enqueue, empty image URLs, empty caption, malformed scheduled_at, grep-friendly id) | ✅ `DueEntryFilterTests::test_past_entries_are_due`, `test_future_entries_are_not_due`, `test_due_entries_sorted_earliest_first` | ✅ `LifecycleTests` (4 tests: mark_posted, attempt retry budget, retry-cap → failed, quarantine) | ✅ `MetaHelperRoutingTests` (4 tests: future + lane → queue; no lane → Meta; bypass_queue → Meta; imminent < 60s → Meta) | ✅ `QueueSummaryTests::test_overdue_lag_observable_for_os_card` (pins ~30 min overdue → `max_overdue_minutes ≈ 30`) | ✅ `test_malformed_entry_file_does_not_crash_due_scan` |
| review_carousel (creative_agent runtime publish_bridge) | ✅ same enqueue contract (shared module) | ✅ same filter | ✅ same | ✅ `PublishBridgeRoutingTests` (3 tests: carousel future + lane → queue; bypass_queue → Meta; single image route) | ✅ same | ✅ same |
| Sidecar heartbeat + late warnings | ⚠️ manual:smoke — `scripts/social_publish_due.py --dry-run --print-json` verified writes heartbeat + reports actions | n/a | n/a | n/a | n/a | n/a |
| launchd install on Mac mini | ⚠️ manual:operator-install — `duckAgent_runtime/com.philtullai.duckagent.social_publish_due.plist` drafted, operator runs `launchctl bootstrap gui/$UID ~/Library/LaunchAgents/...` | n/a | n/a | n/a | n/a | n/a |

**Coverage:** 23 unit tests live in `duckAgent/tests/test_social_publish_queue.py`. All 5 social lanes wired to pass `lane`+`run_id` (5 production files modified).

**Known gaps (acceptable for ship):**
- No OS observability card yet for "sidecar stopped firing" or "queue has overdue entries." Per the two-card-bracket memory, both should be added before relying on the sidecar long-term. Followup #99 in the task list.
- Sidecar's actual Meta publish path is not mocked end-to-end; the `bypass_queue=True` branch is covered by the existing helper tests for jeepfact/etc., but there's no test asserting "queue entry → sidecar runs → Meta called with bypass=True → mark_posted fires." Followup integration test.
- review_carousel runs Tuesdays — first live verification will be 2026-06-09.

---

## Surface 8 — Learning inspector page + /api/learning (2026-06-06)

**Background:** operator question "what is the system learning + what is it trying to learn — without reading the Tuesday email." Existing `/portal/intel/learnings` page covered current beliefs / top posts / windows but didn't surface *which competitor accounts are watched* or *which experiments are queued*. Now extended with three new sections sourced from a single assembler that joins five state files (current_learnings, weekly_strategy_packet, competitor_social_snapshots, competitor_social_benchmark, social_performance_posts).

| Use case | Happy path | Missing state | Malformed JSON | Empty inspector | Join correctness | Signal-gap derivation |
|---|---|---|---|---|---|---|
| `build_learning_inspector_payload` assembler | ✅ `HappyPathAssemblyTests::test_counts_match_underlying_state`, `test_current_beliefs_passed_through`, `test_queued_experiments_carry_priority_and_watch_account` | ✅ `MissingAndMalformedStateTests::test_no_state_dir_returns_unavailable_but_safe_shape` | ✅ `test_malformed_json_does_not_crash`, `test_non_dict_payloads_treated_as_empty` | ✅ same (returns `available=False`) | ✅ `test_tracking_targets_join_benchmark_scores` (profiles ⨝ benchmark on account_handle, sorted by avg_engagement_score) | ✅ `test_signal_gaps_fire_for_small_sample`, `test_signal_gaps_fire_for_n_equals_one_window`, `test_signal_gaps_fire_for_low_packet_confidence`, `test_signal_gaps_fire_for_queued_without_executed` |
| `/portal/intel/learnings` page renders 3 new sections | ✅ `InspectorSectionsTests::test_tracking_targets_section_renders`, `test_queued_experiments_section_renders`, `test_signal_gaps_section_renders` | ✅ `test_inspector_sections_absent_when_empty` (graceful degrade) | n/a (loader returns `{}` on read error) | ✅ same | n/a | n/a |
| Desk "Learnings" tile copy includes counts | ⚠️ manual:js-not-unit-tested — the JS reads `health.learnings_intel.inspector_counts` which the server-side `_load_learnings_intel` test would need a JS test runner to assert end-to-end; the server enrichment is covered by `test_counts_match_underlying_state` | ⚠️ same | ⚠️ same | ⚠️ same | n/a | n/a |
| `/api/learning` HTTP endpoint | ⚠️ manual:smoke-curl — `curl /api/learning` verified returns `available=True` + non-empty `tracking_targets` + `queued_experiments` against live state on 2026-06-06 | n/a (assembler tested) | n/a (assembler tested) | n/a | n/a | n/a |

**Coverage:** 11 assembler tests (`test_learning_inspector_payload.py`) + 4 page-section tests (`test_learnings_intel_page.py::InspectorSectionsTests`) = 15 new tests covering the load-bearing logic. Tile JS + HTTP endpoint are smoke-verified, not unit-tested — same gap pattern as the rest of the intel-page surfaces (`profit`, `competitors`, `reviews`).

**Known gaps (acceptable for ship):**
- ~~`executed_experiments_last_14d` is hard-coded to 0~~ **STALE NOTE (write-back shipped 2026-06-06, Surface 9).** It now reads the real value from `current_learnings._executed_experiments_from_receipts(window_days=14)` (line ~1091); the viewer hardcode is gone. The signal_gap shrinks as outcome receipts land (meme/jeepfact already finalizing 24h+7d outcomes on real data 2026-06-28).
- Tracking-targets section sorts by `avg_engagement_score` desc with nulls last. If a profile is observed but the benchmark hasn't scored it yet (edge case during first-run), it sorts to the bottom — acceptable.
- No test for the cache-invalidation path: `_load_learnings_intel` now calls `build_learning_inspector_payload` per system-health refresh (every 5 min via launchd). The compute is ~5 file reads + dict assembly (~10ms measured) so the additional cost is negligible.

---

## Surface 9 — Creative Quality Loop Phase 5: outcome write-back (2026-06-06 shipped; 2026-06-13 silent-pipeline fix)

**Scoping authored BEFORE implementation per `/coverage-matrix` discipline.** Cells will be filled as commits land; matrix should not show ✅ until the test exists.

**2026-06-13 — the loop was code-complete + green in tests but produced ZERO outcomes in prod.** Receipt audit: every receipt `pending`, no outcome ever written, despite the collector fetching working metrics for 9 posts (`creative_quality_writeback_counts={'skipped':21}`). Root cause: `_outcome_window_for_age` used narrow bands `[20,30)`h and `[144,192)`h. The collector runs once daily at 06:35; an 18:00 publish is observed at ~12.5h then ~36.5h — **missing the 24h band every single day** — and any post first seen after day 8 missed the 7d band too. Fix: monotonic CATCH-UP thresholds (`<20h`→None, `20–144h`→"24h", `>=144h`→"7d") leaning on the existing per-window idempotency. Also: a stray test stamped placeholder `post_id=17912345` into `meme_2026-06-08` (test-pollution-of-prod-state again) — added a placeholder-id guard in `stamp_publish_link` + cleaned the receipt. Also added the **Creative Loop Outcome Write-Back** OS watchdog card (red when posts are published but 0 outcomes land) — a passing test suite never caught the silence because nothing asserted on real output (`[[alive-status-is-not-progress]]`).

| Use case | No-skip catch-up | Placeholder guard | Watchdog card |
|---|---|---|---|
| 2026-06-13 silent-pipeline fix | ✅ `test_social_performance_collector_writeback.py::WindowSelectionTests` (catch-up 24h spans 20–144h, 7d unbounded) + `::test_day3_post_still_writes_24h_outcome` + `::test_evening_publish_daily_cadence_lands_on_day2` | ✅ `test_creative_quality_outcome_schema.py::StampPublishLinkTests::test_rejects_known_placeholder_post_ids` | ✅ `test_creative_outcome_watchdog.py` (reader: no-receipts green / published-but-0-outcomes RED / recent green / stale yellow→red; registration incl. empty payload) |

**Background:** Phase 4 wired three flows through `rank_creative_candidates()` but engagement after publish is never measured. Phase 5 closes the loop: queued IG posts get receipts written by the sidecar, every published variant stamps post_id → run_id, the existing `social_performance_collector` writes outcomes back to `data/creative_quality_receipts/<flow>_<run_id>.json` at 24h + 7d windows, and `current_learnings` surfaces `executed_experiments_last_14d` so the Inspector's "0 executed" signal_gap shrinks.

**2026-06-28 — three-layer isolation completed (the one thing Phase 5 shipped without).** The collector WRITES outcomes into duckAgent's `data/creative_quality_receipts/` cross-repo — a new prod-write path that lacked all three isolation layers (architectural convention #4, the one that bit 3× in 10 days). Closed: (1) conftest redirects in BOTH repos — duckAgent patches `_creative_quality_receipts_dir`, duck-ops patches `social_performance_collector`/`current_learnings.CREATIVE_QUALITY_RECEIPTS_DIR`; (2) source guard `_guard_receipt_write` in `creative_quality_loop.py` raises `CreativeReceiptStateError` under `DUCK_TEST_MODE=1` if a write targets the frozen prod dir (one chokepoint protects both repos since all writes funnel through it); (3) `test_no_test_pollution_in_creative_quality_receipts.py` audit in BOTH repos. Verified: live receipts already finalizing real outcomes (meme/jeepfact 24h+7d) and zero pollution markers. Latent exposure (the full-run collector path + full-flow publish tests) was real but had not yet fired.

| isolation layer (2026-06-28) | covered |
|---|---|
| Layer 1 conftest redirect (both repos) | ✅ duckAgent `_isolate_creative_quality_receipts`; duck-ops `CREATIVE_QUALITY_RECEIPTS_DIR` redirect |
| Layer 2 source `DUCK_TEST_MODE` guard | ✅ `test_no_test_pollution_in_creative_quality_receipts.py` (duckAgent) guard unit tests: `_guard_receipt_write` / `_atomic_replace_receipt` / `write_creative_quality_receipt` all refuse prod under test mode; allow tmp; no-op without test mode |
| Layer 3 post-suite pollution audit (both repos) | ✅ same file (duckAgent) + duck-ops sibling — scan prod receipts for placeholder post_ids / test-slug filenames / test-sourced outcomes |

| Use case | Happy | Receipt missing | API down | Replayed event | Deleted post | Idempotency boundary |
|---|---|---|---|---|---|---|
| Sidecar writes `*_posts.json` after `mark_posted` (Step 1) | ✅ `test_social_publish_queue_receipt.py::test_sidecar_writes_post_receipt_after_mark_posted` (+ carousel/caption/enriched-meta — 7 tests) | ✅ `::test_no_metadata_still_writes_receipt` + `ReceiptWriteFailureModeTests` (2 tests for missing-lane/missing-run_id) | n/a | ✅ `::test_replayed_sidecar_does_not_duplicate_receipt`, `::test_different_post_id_appends_new_entry` | n/a | n/a |
| `stamp_publish_link` + `record_engagement_outcome` + `mark_outcome_final` helpers (Step 2) | ✅ `test_creative_quality_outcome_schema.py::StampPublishLinkTests` (5), `RecordEngagementOutcomeTests` (5), `MarkOutcomeFinalTests` (3), `EndToEndFlowTests::test_full_lifecycle_pending_to_final` | ✅ `::test_returns_none_when_receipt_missing` (×3) | n/a | ✅ `::test_record_outcome_idempotent_on_same_window`, `::test_idempotent_on_already_final` | n/a | ✅ `::test_mismatched_post_id_overwrites_with_warning` |
| Flow steps + sidecar call `stamp_publish_link` after `save_social_post_receipt` (Step 3) | ✅ `test_publish_link_stamp_wiring.py::SidecarStampsPublishLinkTests::test_sidecar_stamps_publish_link_on_existing_receipt` + 3 flow-import smoke tests (catches NameError / wire-up bugs) | ✅ `::test_sidecar_does_not_create_receipt_when_phase4_didnt` (graceful degrade when ranker never ran) | n/a | ✅ `::test_sidecar_idempotent_on_replay`, `::test_sidecar_stamp_failure_does_not_block_posted_action` | n/a | ⚠️ manual:publish-real-meme-wait-24h covers the live e2e path |
| Collector writeback hook (Step 4) | ✅ `test_social_performance_collector_writeback.py::test_writes_24h_outcome_when_post_is_24h_old`, `::test_writes_7d_outcome_and_marks_final`, `WindowSelectionTests` (catch-up boundaries) | ✅ `::test_skips_when_receipt_missing` | ✅ `::test_skips_on_fetch_failed_metric_status`, `::test_skips_on_scheduled_future_status` | ✅ `::test_idempotent_on_same_window_replay` | ⚠️ deferred:cannot-distinguish-deleted-from-transient — perma-deleted posts leave receipt at `pending` (acceptable: Inspector's "executed" count just doesn't increment) | ✅ `::test_skips_too_early`, `::test_day3_post_still_writes_24h_outcome` (catch-up regression — was `test_skips_between_windows`, which encoded the bug) |
| `current_learnings` consumes outcomes (Step 5) | ✅ `test_current_learnings_executed_experiments.py::test_counts_only_outcome_status_partial_or_final`, `::test_filters_by_published_at_window`, `::test_custom_window_days`, `::test_outcomes_passed_through_for_inspector_use` | ✅ `::test_empty_dir_returns_zero_count`, `::test_missing_dir_returns_zero_count`, `::test_skips_receipts_without_publish_block` | ✅ `::test_malformed_receipts_are_skipped` | n/a | n/a | n/a |
| Inspector page: queued→executed promotion (Step 5) | ✅ `test_learning_inspector_payload.py::ExecutedExperimentsConsumerTests` (8 tests): executed_count replaces hardcoded 0, queued→executed promotion on fuzzy match, engagement_score prefers 7d, signal_gap quiets/transforms as receipts land, executed_receipts field present for page render | ✅ `::test_executed_count_falls_back_to_zero_when_summary_missing` (backward compat) | n/a | n/a | n/a | ✅ `::test_signal_gap_zero_executed_keeps_original_wording`, `::test_signal_gap_transforms_when_some_executed`, `::test_signal_gap_quiet_when_execution_keeps_pace` |
| End-to-end (Step 6) | ⚠️ manual:publish-real-meme-wait-24h — confirms 24h outcome lands in `meme_<run_id>.json` after live publish | n/a | n/a | n/a | n/a | n/a |

**Coverage target:** ~12 unit tests + 1 e2e manual. Cells flip from 🔴 to ✅ commit-by-commit.

**Known gaps acceptable at ship:** no ranker retraining (Phase 6); no Etsy outcomes (different lane); no backfill of pre-fix posts (forward-looking only). All three are explicit Phase 5 scope cuts in `CREATIVE_QUALITY_LOOP_V2_PLAN.md`.

### 9.1 — "Did it produce?" producer watchdogs (2026-06-13)

The creative-loop incident exposed a class gap: `scheduler_health` answers "did the job RUN" comprehensively (auto-discovers all launchd plists, flags missed_run/timeout) but several producers had no "did it PRODUCE output" card — so a job could run green for weeks while emitting nothing. Audited all 41 scheduled jobs; added OS watchdog cards for the producers that fed downstream surfaces with no output check. Each card: red on empty/zero output or staleness, yellow on near-stale, green on fresh+non-empty; reads the producer's state file directly (cheap reader, no new prod-state path to isolate).

| Producer (job) | Empty/zero output → RED | Stale → RED | Fresh+non-empty → green | Registered (incl. empty payload) |
|---|---|---|---|---|
| competitor-social snapshot (daily input) | ✅ `test_competitor_social_watchdog.py::SnapshotSanityReaderTests::test_empty_scrape_is_red` / `::test_profiles_but_no_posts_is_red` | ✅ `::test_stale_is_red` | ✅ `::test_fresh_nonempty_is_green` | ✅ `CardRegistrationTests` (both bracket cards, incl. empty) |
| competitor-social benchmark (daily output) | ✅ `BenchmarkFreshnessReaderTests::test_zero_posts_is_red` | ✅ `::test_stale_is_red` | ✅ `::test_fresh_is_green` | ✅ same |
| profit-per-product (daily) | ✅ `test_producer_freshness_watchdogs.py::ProfitPerProductReaderTests::test_zero_products_or_orders_is_red` | ✅ `::test_stale_is_red` | ✅ `::test_fresh_nonempty_is_green` | ✅ `CardRegistrationTests` (incl. empty) |
| review carousel (weekly Tuesday) | ✅ `ReviewCarouselReaderTests::test_dismissed_runs_do_not_count_as_published` (no real publish = not fresh) | ✅ `::test_stalled_lane_is_red` (>17d) | ✅ `::test_recent_scheduled_is_green`, `::test_picks_newest_scheduled_across_many` | ✅ same |

**Live verification (2026-06-13):** all 5 new cards render in `/api/system-health` (36 total). creative_quality_outcome=red (correct — 0 outcomes until the next collector run lands jeepfact's), the 4 freshness cards green on real data. Note: `/api/system-health` serves a producer/reader cache (`system_health_refresh.py`, ~5 min) — a new card lags one refresh cycle until the producer regenerates.

**Second pass (2026-06-13) — listing + content lanes.** Carded the remaining producers. The Shopify SEO card immediately went RED on a real 37d-stale audit/outcomes surface (the daily kickoff had silently stopped refreshing it) — exactly the find this exercise was for.

| Producer (job) | Empty/stale → RED | Fresh → green | Registered (incl. empty payload) |
|---|---|---|---|
| Shopify SEO lane (parked-approval + staleness) | ✅ `test_listing_content_watchdogs.py::ShopifySeoReaderTests::test_parked_review_is_red_with_actionable_reason`, `::test_37d_stale_is_red` | ✅ `::test_fresh_is_green`, `::test_recent_pending_review_is_yellow` | ✅ `CardRegistrationTests` (3 cards, incl. empty) |
| Shopify draft-activation (weekly) | ✅ `DraftActivationReaderTests::test_stale_is_red` | ✅ `::test_fresh_is_green` | ✅ same |
| Content publish cadence — meme/jeepfact/GTDF | ✅ `ContentPublishReaderTests::test_one_stale_lane_is_red` (>17d), `::test_missing_lane_is_yellow` | ✅ `::test_all_fresh_is_green` (per-lane from posts.json + GTDF receipt dates) | ✅ same |

**Root cause of the SEO red (diagnosed 2026-06-13):** NOT a broken job. `shopify_seo_kickoff.py` runs green daily but has a guard (`if latest_status == "awaiting_review": return skipped_open_review`) — it won't send a new category review while one is pending. A **May 8 "near-duplicate SEO titles" review (2 items) has sat `awaiting_review` for 37 days**, parking the whole lane. The SEO card now reads `shopify_seo_review/latest.json` status+age and gives an actionable "reply to unblock" message for a parked approval, falling back to audit/outcomes staleness only when none is pending. **Operator action:** action/dismiss that May 8 SEO email to resume the lane.

**Live (2026-06-13):** 39 OS cards total. `shopify_seo_freshness`=RED (parked May-8 review, 37d), `shopify_draft_activation_freshness`=green (5.4d), `content_publish_cadence`=green (meme 5d / jeepfact 3d / GTDF 2.8d).

**Now uncarded (accepted):** only fine-grained per-flow content QA beyond publish-cadence + outcomes; covered enough by the existing Thursday-funnel, meme-recently-used, and creative-outcome cards.

### 9.2 — Email-gated approvals strand silently (2026-06-13 operator finding)

The operator deletes most email; deleting an approval email does NOT change the workflow state, so any lane whose only resolution is an email reply parks forever. Confirmed via the Shopify SEO incident (May-8 review deleted → lane parked 37d) and a full audit of approval lanes. **HIGH-risk (email-only) lanes:** Shopify SEO, Shopify draft-activation, GTDF/Thursday, New Duck activation; occasion tags are CLI-only (also portal-invisible). **LOW-risk (already portal-actionable on `/portal/decisions`):** meme, jeepfact, weekly_sale, reviews, review replies, operator/trend reviews, Build-Next promote (the gateway flows). See memory `[[email-to-portal-inversion]]`.

**Shipped (Slice 1 — visibility):** `viewer_data._parked_approval_decision_items` surfaces stale (>10d) pending Shopify SEO + draft-activation reviews in the Decision Inbox as read-only items, so a stranded approval is no longer invisible. Tests: `test_parked_approval_inbox.py` (stale surfaces / fresh doesn't / resolved doesn't / missing-file no-raise / inbox integration). **Shipped (Slice 2a — portal Dismiss button):** parked items carry a "Dismiss & unblock lane" button that POSTs `/api/parked-approval/action` → `_handle_parked_approval_action` (avoids the creative-run detail flow). Dismiss flips the pending review → `dismissed` on latest.json + run file (atomic, pending-only guard); fires NO Shopify/Etsy mutation. Live round-trip verified (re-park → POST dismiss → status flipped). Handler tests: `test_parked_approval_inbox.py::ParkedApprovalDismissActionTests` (dismiss / noop-when-resolved / apply-rejected / unknown-lane). The JS button itself isn't browser-unit-tested — needs one manual tap-to-confirm on the page. **Deferred (Slice 2b — Apply, Tier 3):** Apply-from-portal is explicitly rejected (400, clear message) until the email executors are wired behind it with browser verification.

---

## Surface 10 — LLM spend observability page + soft alert (2026-06-06, in flight)

**Scoped BEFORE implementation per `/coverage-matrix` discipline.** Cells will turn ✅ as commits land.

**Background:** operator asked "what is LLM spend doing?" — answer today is "guess, then check the OpenAI / Anthropic billing dashboard." Scope A (observability only — no hard ceiling, no auto-stop) reads the existing `state/llm_call_log.jsonl` (4 runtime modules write to it via `log_llm_call`) and surfaces it on `/portal/intel/cost`. Scope B (instrument duckAgent flows so logs are comprehensive) is a separate followup — Scope A's page will display "% of total spend instrumented" so the operator knows the gap.

**Architecture:** producer-on-schedule + cheap-reader pattern (matches Surface 8 learning inspector, Surface 7 IG queue, system_health, current_learnings). A `runtime/llm_cost_summary.py` script aggregates daily/weekly/per-flow spend from the raw log → writes `state/llm_cost_summary.json` → the page reads the cached JSON. Soft alert: when today's spend > threshold (configurable), write an OS-card signal file.

| Use case | Happy | Empty log | Malformed line | Unknown model | Old log entries | Cost arithmetic |
|---|---|---|---|---|---|---|
| `llm_cost_summary` aggregator (producer) | ✅ `test_llm_cost_summary.py::AggregateHappyPathTests` (4 tests: by_day+flow rollups, cost arithmetic, unknown model handling, image per-call pricing) | ✅ `AggregateEmptyAndMalformedTests::test_empty_log_returns_zero_totals`, `::test_missing_log_returns_zero_totals` | ✅ `::test_skips_malformed_jsonl_lines`, `::test_entries_without_at_are_counted_in_data_quality` | ✅ `::test_unknown_model_skips_cost_but_counts_call` | ✅ `WindowFilterTests::test_filters_window_days` | ✅ `::test_cost_matches_pricing_table_for_gpt_4o_mini`, `::test_image_call_uses_per_call_pricing` |
| Flow context derivation from `artifact_id` | ✅ `ArtifactIdParseTests::test_publish_lane_parses`, `::test_score_lane_parses`, `::test_uppercase_normalized` | ✅ `::test_unparseable_falls_through_to_unknown` | n/a | n/a | n/a | n/a |
| `/portal/intel/cost` page | ✅ `test_cost_intel_page.py::CostIntelPageFullTests::test_full_summary_renders_all_sections` (all sections + coverage caveat), `::test_alert_banner_renders_when_signal_present`, `FmtHelpersTests` (4 fmt_usd edge cases) | ✅ `CostIntelPageEmptyTests::test_empty_state_when_no_summary`, `::test_chrome_scaffolding_present` | n/a | n/a | n/a | n/a |
| `/api/cost-intel` endpoint | ✅ live-verified 2026-06-06: `curl /api/cost-intel` returns `totals.cost_usd=0.0028 / call_count=31` against real log | n/a | n/a | n/a | n/a | n/a |
| Desk tile reads cost summary | ✅ live-verified 2026-06-06: system_health.cost_intel populated; Desk JS has `costIntelSummary` + `metricCard("Cost", ...)` wired | ⚠️ manual:js-not-unit-tested (matches Surfaces 4, 8 same gap) | n/a | n/a | n/a | n/a |
| Soft alert signal file | ✅ `AlertEvaluationTests::test_writes_alert_when_today_over_threshold`, `::test_no_alert_when_under_threshold`, `::test_threshold_boundary_exactly_at_threshold_does_not_alert`, `::test_red_severity_when_double_threshold` | n/a | n/a | n/a | n/a | n/a |
| Coverage transparency (% instrumented vs total) | ⚠️ manual:billing-dashboard-comparison — operator reconciles vs OpenAI dashboard once/week (page renders the instrumentation_note with reconciliation reminder) | n/a | n/a | n/a | n/a | n/a |

**Coverage target:** ~10 unit tests + 2 manual smoke. Page banner explicitly says "instrumented spend — duckAgent creative flows not yet logged" so the operator never mistakes partial coverage for total coverage.

**Pricing-gap regression (2026-06-14):** operator flagged the Cost card "looked wrong" ($1.05/30d). Two findings: (1) **real bug** — the prod log uses model `gpt-image-2` but `PER_CALL_IMAGE_COST_USD` only listed `gpt-image-1`, so 6 real image calls fell through to `unknown_model` = $0 (the `unknown_model_count: 6`); added `gpt-image-2` to the table → total $1.05 → $1.29, uncosted 6 → 0. Regression test `test_llm_cost_summary.py::AggregateHappyPathTests::test_gpt_image_2_is_priced_not_uncosted`. (2) **misleading headline** — the OS Desk tile (`costIntelSummary` in duckAgent viewer.py) read like a full provider bill; reworded to "Partial floor — duck-ops text + logged images only, not your full provider bill" and now surfaces the uncosted-call count when `unknown_model_count`+`no_pricing_count` > 0.

**Known scope cuts (Scope A, intentional):**
- No hard ceiling / auto-stop. Operator chose observability-first (`continue` on 2026-06-06 after the un-bundling question).
- No spend-by-customer / spend-by-product breakdown. Spend is by flow/model only.
- No multi-month historical archive — the log itself is the archive, summary aggregates last N days.

**Scope B (2026-06-06, shipped same day):** duckAgent's `helpers/openai_helper.py` entry points now log to the same `state/llm_call_log.jsonl`. `openai_chat` (covers `openai_json` + `openai_json_with_debug` automatically since they call it internally), `openai_dalle_generate_image`, and `openai_edit_image` all emit one log line per call with prompt_tokens / completion_tokens (text) or image_count (image). New `helpers/llm_log.py` module is the shared writer; never raises. Optional `artifact_id` + `flow` kwargs let callers attribute spend to specific flows; default falls back to `call::unknown::<timestamp>` which the producer rolls up under `unknown` flow (still useful while individual callers migrate).

| Use case | Happy | Caller-supplied artifact_id | Caller without kwargs | Logging never raises | Wrapper imports cleanly |
|---|---|---|---|---|---|
| `helpers/llm_log.log_llm_call` | ✅ `test_llm_log.py::LogLlmCallTests::test_happy_path_writes_one_jsonl_line` + 5 supporting tests (timestamp, source_repo, multi-line append, dir-creation) | n/a | n/a | ✅ `::test_never_raises_on_unwritable_path`, `::test_never_raises_on_unserializable_payload` | n/a |
| `derive_artifact_id_from_payload` | ✅ `DeriveArtifactIdTests` (3 tests: with flow, without flow, without extra) | n/a | n/a | n/a | n/a |
| `openai_chat` wrapper | ✅ `test_openai_helper_logging.py::OpenAIChatLoggingTests::test_openai_chat_success_logs_ok_outcome_with_tokens` | ✅ `::test_openai_chat_explicit_artifact_id_passes_through` | ✅ `::test_no_attribution_kwargs_still_logs` | ✅ http_error path logged too: `::test_openai_chat_http_error_logs_outcome` | ✅ `OpenAIHelperImportSmokeTests` (2 tests) |
| `openai_json` + `openai_json_with_debug` (call openai_chat internally) | ✅ `OpenAIChatPropagatesThroughJsonHelpersTests::test_openai_json_logs_via_openai_chat`, `::test_openai_json_with_debug_logs_via_openai_chat` | n/a | n/a | n/a | n/a |
| `openai_dalle_generate_image` + `openai_edit_image` | ⚠️ manual:cannot-mock-openai-sdk-client-easily — signature + wrapper structure verified by `test_wrapper_functions_have_artifact_id_kwarg` ; behavior validated by first live run | n/a | n/a | n/a | ✅ wrapper-function-signature smoke test |

**Coverage transparency now:** Scope B is rollout-complete on the shared entry points. Direct-import-and-call sites that bypass these wrappers (e.g., a file that instantiates its own `openai.OpenAI()` client) still wouldn't be logged — `helpers/twilio_helper.py`, some `flows/competitor` paths. Those are a smaller followup pass (scope B.5).

---

## Surface 11 — Per-product profit drill-down on /portal/intel/profit (2026-06-06)

**Background:** operator question "which ducks make money, which lose money?" — the existing `/portal/intel/profit` page surfaced aggregate trend + channel mix + email status but never broke down by product. Operator couldn't act on "retire this duck" / "promote this duck" decisions from the portal.

**Architecture:** producer-on-schedule + cheap-reader (matches Surfaces 8/10 + system_health/current_learnings/weekly_strategy). `runtime/profit_per_product.py` scans `duckAgent/cache/profit/orders/*.json` (line-item-level raw data with `revenue_ex_tax` + `cogs_unit` + `net_profit` already computed by the order collector), groups by `product_title` so SKU variants roll up under one duck, and writes `state/profit_per_product.json`. The page reads the cache; never reopens the 200+ raw order files per request.

**Honest framing:** the gross margin in this view excludes labor + packaging + ads + shipping. Section header says so. The aggregate `/portal/intel/profit` numbers from `profit_intel.json` are the full-P&L truth; this drill-down is product-level signal at gross-margin resolution. A row tagged `is_confident_margin=False` (< 3 units sold) gets a "low-n" pill so the operator doesn't retire a duck on n=1 noise.

| Use case | Happy | Empty cache | Missing cache dir | Malformed file | Window filter | Low-n confidence | SKU variant rollup |
|---|---|---|---|---|---|---|---|
| `aggregate_per_product` producer | ✅ `test_profit_per_product.py::HappyPathAggregationTests::test_two_orders_same_product_aggregate` (units, revenue, COGS, net, margin arithmetic), `::test_sort_by_net_profit_descending`, `::test_loss_makers_isolated` | ✅ `WindowAndMalformedTests::test_empty_cache_returns_zero_totals` | ✅ `::test_missing_cache_dir_returns_zero_totals` | ✅ `::test_malformed_file_counted_not_crashed`, `::test_unparseable_filename_skipped` | ✅ `::test_window_excludes_old_files` | ✅ `HappyPathAggregationTests::test_low_n_products_flagged_not_confident`, `::test_low_margin_excludes_low_n` | ✅ `::test_different_skus_same_title_roll_up` |
| Cache file format tolerance | ✅ `::test_orders_dict_wrapper_format` (handles `[...]` or `{"orders": [...]}`), `::test_fallback_label_when_no_product_title` (handle → sku → unknown) | n/a | n/a | n/a | n/a | n/a | n/a |
| Data quality flags | ✅ `WindowAndMalformedTests::test_data_quality_flags_missing_revenue_and_cogs` | n/a | n/a | n/a | n/a | n/a | n/a |
| `/portal/intel/profit` per-product section render | ✅ `test_profit_per_product_section.py::RenderPerProductSectionTests` (6 tests: three sub-tables, placeholder when empty, low-n pill, clean-when-no-losers, confidence note surfaced) | ✅ `::test_returns_empty_when_payload_missing`, `::test_placeholder_when_no_orders_in_window` | n/a | n/a | n/a | n/a | n/a |
| Full profit page with per-product section | ✅ `RenderFullPageWithPerProductTests::test_full_page_includes_per_product_when_both_files_present` | n/a | n/a | n/a | n/a | n/a | n/a |
| Backward compat (per-product file missing) | ✅ `::test_full_page_works_without_per_product_file` | n/a | n/a | n/a | n/a | n/a | n/a |
| `_fmt_money` formatting | ✅ `FmtMoneyTests` (4 tests: positive, negative-with-minus-prefix, comma-thousands, invalid-input em-dash) | n/a | n/a | n/a | n/a | n/a | n/a |

**Coverage:** 14 producer tests + 12 page-section tests = 26 new tests. Producer ran against live 30-day cache: 174 distinct products, 418 units, $3,974.86 revenue, $3,661.36 net (92.1% gross margin — material-only), 6 loss-makers, 0 confident-sample low-margin entries.

**Known scope cuts (acceptable):**
- No labor / packaging / ads / shipping in the per-product COGS — gross margin only. Page section header says so explicitly. Aggregate `profit_intel.json` view is the full-P&L source of truth.
- No launchd plist for `profit_per_product.py` yet. Operator runs it manually for now; a daily plist alongside `profit_intel` would be a small Tier 3 followup.
- No per-channel breakdown within a product (e.g., "Michigan Wolverines sells better on Shopify than Etsy"). Surface 12 if useful.
- No customer-segment slicing.

---

## Surface 12 — Versioned multi-label theme classifier (2026-06-11)

**Background:** occasion-engine input audit found `ai_theme_category` ~50% accurate (awareness ducks filed under "Animals & Pets"/"Holiday & Christmas", patriotic ducks under "Military & Tactical", meme ducks under "Cozy Cabin Vibes"). Root causes: single forced label, taxonomy gaps (no Awareness/Patriotic/Pop-Culture buckets), prompt referenced categories absent from its own list, off-taxonomy error fallback stored silently, cache never invalidated. Rebuilt as `helpers/theme_classifier.py` + `config/theme_taxonomy.json` (v2, versioned) emitting primary + secondaries + occasions + recipients + confidence, with needs_review instead of silent fallback.

**Quality gates (promotion criteria for any prompt/taxonomy/model change):** primary accuracy ≥90%, occasion recall ≥95% on the golden set. Current: **100% / 100%** (50 hand-labeled ducks, gpt-4o-mini).

| Use case | Happy | Off-taxonomy LLM reply | Low/garbage confidence | Confidently-wrong primary | Taxonomy bump | Input (title/tags) change | Real-model accuracy drift |
|---|---|---|---|---|---|---|---|
| Classify one product (multi-label) | ✅ `duckAgent/tests/test_theme_classifier.py::TestClassifyHappyPath` | ✅ `TestOffTaxonomyRejection` (retry w/ correction; None+needs_review after 2 fails; no silent fallback label) | ✅ `TestNeedsReview`, `TestValidation::test_garbage_confidence_clamped` | ✅ `TestKeywordEvidence` (deterministic keyword-evidence cross-check flags `keyword_evidence_favors:`; pins the historical Breast-Cancer→Christmas misfile) | ✅ `TestStaleness::test_old_taxonomy_version_is_stale` | ✅ `TestStaleness::test_changed_input_is_stale` | ✅ `scripts/eval_theme_classifier.py` golden-set gate (manual trigger, real API; exits 1 below gates) |
| Vocab filtering (occasions/recipients/secondaries) | ✅ `TestValidation` (invalid ids dropped, secondary==primary dropped, capped at 2) | n/a | n/a | n/a | n/a | n/a | n/a |
| Sweep functions (`categorize_new_ducks` / `categorize_all_existing_ducks`) | ⚠️ manual:CLI-run-observed (per-product failures logged + skipped, checkpoint save every batch_size) | n/a:handled-in-helper | n/a | n/a | ✅ staleness check drives re-sweep | ✅ same | n/a |
| Coverage card (input) | ✅ `creative_agent/runtime/tests/test_theme_classification_cards.py::CoverageCardTests` (missing state, green, taxonomy-bump yellow, stale yellow/red, ancient red) | n/a | n/a | n/a | ✅ `test_taxonomy_bump_without_sweep_yellow` | n/a | n/a |
| needs_review backlog card (output) | ✅ `BacklogCardTests` (0 green / 1-9 yellow w/ titles / 10+ red) | n/a | n/a | n/a | n/a | n/a | n/a |
| catalog_index passthrough (duck-ops) | ⚠️ manual:verify-after-next-phase1_observer-run (`theme_classification` field added at writer; per passthrough-chain-audit memory) | n/a | n/a | n/a | n/a | n/a | n/a |

**Regression rule:** every field misclassification gets appended to `duckAgent/tests/fixtures/theme_classifier_golden.json` before the fix (same discipline as regression tests).

---

## Surface 13 — Occasion Engine Phase 1 (2026-06-11, matrix written BEFORE code per /coverage-matrix)

**Background:** Father's Day (2026-06-21) push, built as data-driven occasion infrastructure (NOT hardcoded "dad"). Occasions are config (`config/occasion_calendar.json` with recurrence rules + lead windows + messaging phases). Daily producer `runtime/occasion_engine.py` resolves active occasions, selects products from `catalog_index` using Surface 12's `theme_classification.occasions/recipients` + occasion keywords, writes `state/occasion_intel.json`. duckAgent consumes via `helpers/occasion_context.py`: occasion line into meme/jeepfact prompts, occasion tag candidates into `_build_candidates`.

| Use case | Happy | Bad/missing config | Recurrence edge (year rollover, nth-weekday) | No products match | catalog_index missing/stale | State file missing (reader) | Occasion active but selection empty |
|---|---|---|---|---|---|---|---|
| Resolve next occurrence per rule | ✅ planned: test_occasion_engine.py (fathers_day=2026-06-21, july_4 fixed, christmas, mothers_day rolls to 2027) | ✅ planned: malformed rule skipped + counted, never crashes producer | ✅ planned: peak passed → next year; nth=3 sunday June | n/a | n/a | n/a | n/a |
| Active-window + messaging phase | ✅ planned: inside lead_days active w/ correct phase; outside inactive | ✅ planned | ✅ planned: peak day itself active, day after inactive | n/a | n/a | n/a | n/a |
| Product selector scoring | ✅ planned: classifier-occasion hit > recipient hit > keyword hit ordering; top-N cap; reasons attached | n/a | n/a | ✅ planned: returns [] (never invents) | ✅ planned: empty intel + flag, no crash | n/a | covered by output card |
| Atomic state write | ✅ planned: tempfile+os.replace | n/a | n/a | n/a | n/a | n/a | n/a |
| duckAgent reader (occasion_context) | ✅ planned: returns active occasions | n/a | n/a | n/a | n/a | ✅ planned: fail-soft {} — flows never crash | n/a |
| Prompt injection (meme/jeepfact) | ✅ planned: occasion line present when active, absent when not | n/a | n/a | n/a | n/a | ✅ planned: no line, no crash | n/a |
| Etsy tag candidates | ✅ planned: candidates valid per 6-20 char tag rules, category="occasion" | n/a | n/a | ✅ planned: no candidates injected | n/a | ✅ same | n/a |
| Input OS card (producer freshness/config sanity) | ✅ planned: card tests | ✅ planned: red on unparseable calendar | n/a | n/a | n/a | ✅ planned: yellow w/ bootstrap action | n/a |
| Output OS card (selection sanity) | ✅ planned | n/a | n/a | ✅ planned: RED when occasion active & 0 products (broken-selector catch) | n/a | n/a | ✅ planned |
| 07:25 launchd producer cadence | ⚠️ manual:launchd-install-is-Tier-3-operator-approved; freshness card watches it | n/a | n/a | n/a | n/a | n/a | n/a |

**Scope cuts (deliberate, Phase 1):** no review-mining tier (Surface-12 audit showed review corpus too thin — ~5/day, zero recipient mentions); no auto tag-apply with expiry (Phase 2); no weekly-packet nominations (Phase 2); no IG auto-publish changes — occasion context only angles EXISTING approved lanes.

### Surface 13 Phase 2 (2026-06-12, matrix before code)

Occasion Etsy tag apply with expiry-revert (approval-gated, NEVER silent auto-action), newduck publish hook, weekly-packet nominations. Listing mapping is the known scope risk: catalog_index has no Etsy listing_id — a mapper joins live Etsy listings by normalized title + newduck receipts as exact overrides; low-confidence matches excluded (fail closed).

| Use case | Happy | 13-tag/6-20-char invariant | Unknown/uncertain approval state | Apply fails mid-batch | Revert at window close | Mapping low-confidence | Re-run idempotency | Test-mode prod-write |
|---|---|---|---|---|---|---|---|---|
| Listing map builder | ✅ planned: title joins + receipt overrides | n/a | n/a | n/a | n/a | ✅ planned: excluded below threshold, counted | ✅ planned | ✅ planned (3-layer) |
| Tag swap plan | ✅ planned: occasion tags in, weakest out, originals preserved | ✅ planned: exactly 13 post-swap, all 6-20, deduped | n/a | n/a | n/a | n/a | ✅ planned: same plan twice | n/a |
| propose | ✅ planned: proposals + `occasion-tag-proposed` transitions | ✅ same | n/a | n/a | n/a | ✅ skipped + reason | ✅ no duplicate proposals | ✅ planned |
| approve (operator CLI — the Tier 3 gate) | ⚠️ manual:operator-runs-it | n/a | ✅ planned: refuses unknown workflow_id | n/a | n/a | n/a | ✅ approve twice = no-op | n/a |
| apply | ✅ planned: snapshot BEFORE patch, receipt per attempt | ✅ payload validated pre-PATCH | ✅ planned: refuses non-approved state (fail closed) | ✅ planned: failure receipt, batch continues, item → failed | n/a | n/a | ✅ applied item not re-applied | ✅ planned |
| revert | ✅ planned: exact originals restored + receipt | n/a | n/a | ✅ revert_pending resurfaces | ✅ planned: fires only when window inactive | n/a | ✅ reverted item not re-reverted | ✅ planned |
| newduck publish hook | ✅ planned: producer kicked on success path only | n/a | n/a | ✅ subprocess failure never fails publish; bounded timeout | n/a | n/a | n/a | n/a |
| weekly packet nominations | ✅ planned: section from occasion_intel | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| recommendations page render | ✅ planned: section renderer added (markdown≠portal-HTML) | n/a | n/a | n/a | ✅ omitted when no active occasion | n/a | n/a | n/a |

### Surface 13 Phase 3 — meme/social occasion bias (2026-06-15)

**Background:** operator asked "should I see Father's Day in today's meme?" — no. Phase 1 only injected the occasion line when the meme's product was a *curated pick*, but `choose_product_for_meme` is occasion-blind, so memes rode the holiday only by luck (today landed on a non-pick Alabama duck → generic copy). Two fixes: (1) **copy + tags always nod** — `occasion_meme_prompt_line` gives a non-pick duck a soft "still a great <occasion> gift" nod (strong "lean in" angle only for curated picks), and `occasion_meme_hashtags` merges holiday tags (e.g. `FathersDayGift`) ahead of the 12-tag cap; (2) **picker steering** — an occasion-engine bonus (40 normal / 70 last-call ≤7d) biases `choose_product_for_meme` toward curated picks without overriding variety/randomness.

| Use case | Happy | Non-pick product | No occasion active | Stale intel | Last-call (≤7d) weighting | List-position isolation |
|---|---|---|---|---|---|---|
| `occasion_meme_prompt_line` | ✅ `test_occasion_context.py::TestMemePromptLine::test_curated_pick_gets_strong_lean_in` | ✅ `::test_non_pick_product_still_gets_soft_nod` (the core ask) | ✅ `::test_no_occasion_returns_empty` | ✅ `::test_stale_intel_returns_empty` | n/a | n/a |
| `occasion_meme_hashtags` | ✅ `TestMemeHashtags::test_active_occasion_yields_camelcase_tags` | n/a (product-agnostic by design) | ✅ `::test_no_occasion_yields_empty` | covered by reader staleness | ✅ `::test_respects_limit` | n/a |
| Picker occasion bonus | ✅ `test_meme_occasion_bias.py::test_active_occasion_pick_wins_when_scores_otherwise_equal` (deterministic max-score) | n/a | ✅ `::test_no_active_occasion_no_bias` (pick placed 2nd → does NOT win) | n/a | ⚠️ bonus constant covered by code; last-call vs normal not separately asserted | ✅ negative control isolates bonus from list order |
| Live-state spot check | ✅ manual 2026-06-15: Alabama (non-pick) → soft nod + `FathersDayGift/GiftForDad/DadGiftIdea`; Grill Master (pick) → strong lean-in | n/a | n/a | n/a | n/a | n/a |

**Scope cut:** last-call (70) vs normal (40) bonus differentiation is not separately unit-tested (both proven to fire; the threshold is a tuning knob, not a correctness boundary).

### Surface 13 Phase 4 — jeepfact caption/tags occasion nod (2026-06-17)

**Background:** operator asked "why didn't jeepfact Wednesday pick up any Father's Day stuff?" Phase 3's note "Jeepfacts already nodded (no-product path) — unchanged" was WRONG in a load-bearing way: the occasion line only reached the *facts* prompt (`generate_jeep_facts`), where factual Jeep trivia + an "only if natural" instruction means the model correctly never shoehorns a holiday into a fact. The **caption and hashtags** — where the nod belongs, per the same "text + tags must nod" policy as meme — are built from 100% static templates in `generate_jeep_fact_post_content` with zero occasion awareness. Fix: new `occasion_jeepfact_caption_line()` (finished customer copy, not a model instruction) injected into full + short caption, and `occasion_meme_hashtags()` merged just after the brand tag so the short caption's first-5 slice keeps the holiday tag. Same "wire it on every surface" lesson as [[markdown_vs_portal_html]] / [[passthrough_chain_audit]].

| Use case | Happy | Phrasing edges | No occasion active | Stale intel | Short-caption survives slice |
|---|---|---|---|---|---|
| `occasion_jeepfact_caption_line` | ✅ `test_occasion_context.py::TestJeepfactCaptionLine::test_active_occasion_yields_ready_copy` (name + "10 days away", not a model instruction) | ✅ `::test_one_day_and_today_phrasing` (tomorrow / is here) | ✅ `::test_no_occasion_yields_empty` | ✅ `::test_stale_intel_yields_empty` | n/a |
| Caption + tags wiring | ✅ `TestJeepfactPostContentOccasion::test_caption_and_tags_nod_when_occasion_active` (full+short caption + `#FathersDayGift` in tags) | n/a | ✅ `::test_no_nod_when_intel_missing` (no nod, base 10 tags unchanged) | covered by reader staleness | ✅ same happy test asserts `#FathersDayGift` in short_caption |
| Live-state spot check | ✅ manual 2026-06-17: live render shows "🎁 Father's Day is just 4 days away…" in both captions + holiday tags ahead of the brand set | n/a | n/a | n/a | n/a |

**Not changed:** the cover hook (`generate_cover_hook`) and the facts prompt stay as-is — the hook is the trivia click-driver and the facts path already had its (correctly soft) line. The nod lives in the caption + tags, matching the meme decision.

### Surface 13.5 — Consumer-side occasion-nod coverage card (2026-06-17)

**Background:** the jeepfact Father's Day miss exposed a deeper gap — the two `occasion_engine_*` OS cards are PRODUCER-side ("is intel fresh? did selection pick products?") and both stayed green while the consumer silently dropped the nod. Nothing watched whether the surfaces that RAN actually tailored to the active occasion, so the operator only found out by reading the post. New card `occasion_nod_coverage` (in `viewer.py`, a cheap reader — no launchd producer): joins `get_active_occasions()` with the most recent meme + jeepfact runs in `runs/` (≤10d), and goes **red** if a surface ran during an active occasion but emitted no nod (occasion name/keyword in caption OR an occasion hashtag in tags — deterministic, `#`-asymmetry-normalized). Scope is meme + jeepfact only (their helpers force an always-nod); listing tags are relevance-gated and excluded to avoid guaranteed false reds. Closes the [[two_card_observability_bracket]] / [[alive_status_is_not_progress]] gap.

| Use case | Ran + nod (happy) | Ran + NO nod (the bug) | No occasion active | Stale intel | No run in window | Detection paths | Card registration |
|---|---|---|---|---|---|---|---|
| `occasion_nod_present` matcher | ✅ `test_occasion_nod_coverage.py::TestOccasionNodPresent::test_caption_name_match` | ✅ `::test_no_nod` | n/a | n/a | n/a | ✅ `::test_caption_keyword_word_boundary` (no 'dad' in 'additional') + `::test_hashtag_match_with_and_without_pound` (# asymmetry) | n/a |
| `read_surface_caption_hashtags` | ✅ `TestReadSurfaceCaptionHashtags::test_meme_keys` / `test_jeepfact_keys` | n/a | n/a | n/a | n/a | ✅ `::test_unknown_surface` → ("", []) | n/a |
| Card loader verdict | ✅ `TestNodCoverageCard::test_ran_with_nod_is_green` | ✅ `::test_ran_without_nod_is_red` (repair_now, names surface+occasion) + `::test_one_surface_misses_one_nods_is_red` | ✅ `::test_no_occasion_active_is_green` (available False) | ✅ `::test_stale_intel_is_green_not_red` (no double-alarm) | ✅ `::test_active_but_no_run_in_window_is_yellow` + `::test_run_outside_lookback_window_ignored` | ✅ `::test_meme_hashtag_only_and_jeepfact_caption_only` | ✅ `test_os_card_registration.py::test_occasion_nod_coverage_card_registered` + `::test_occasion_nod_coverage_registered_on_empty_payload` |
| Live spot check | ✅ 2026-06-17: card green — "All surfaces that ran (meme, jeepfact) nodded to Father's Day" | n/a | n/a | n/a | n/a | n/a | n/a |

**Why no producer/launchd:** the inputs (`occasion_intel.json` 07:25 producer + the meme/jeepfact run states) are already produced; this card is a pure cheap reader joining them, like `_load_newduck_quality_gate_health`. No new cadence → no Tier-3 plist ask.

---

## Surface 14 — This-or-That Thursday funnel fixes (2026-06-12)

**Background:** operator reported "same pairs every week, none usable, ducks standing on legs." Root causes: (1) top-6 slice of 1,048 trending candidates = already-cloned bestsellers, rejected by the qualifier, NO backfill from the remaining pool → 6-item static fallback (Cowgirl/Chef) won weekly; (2) history written only on publish, so unapproved weeks left no anti-repeat record; (3) limb-wear words ("boots") in image phrases + style contract's "unless explicitly requested" escape → leg-standing ducks; (4) semantic QA emits boolean 0/1 score, min(100,1)=1 reported as "pass".

| Use case | Happy | All-wave rejection (bestseller dupes) | Cross-wave duplicate | True exhaustion | Unapproved week | Limb-wear in phrase | Boolean QA score | Funnel starved |
|---|---|---|---|---|---|---|---|---|
| Deep wave qualification | ✅ test_thursday_funnel_fixes.py::TestDeepQualification | ✅ same (descends to wave 2) | ✅ dedupe test | ✅ fallback only after exhaustion | n/a | n/a | n/a | n/a |
| [:24] pool + deep call wiring | ✅ source-guard test | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| History on batch email | ✅ source-guard + concepts-filter test | n/a | n/a | n/a | ✅ every emailed name recorded incl. fallback | n/a | n/a | n/a |
| Image phrase scrub | ✅ TestLimbWearScrub (7 words + clean-phrase untouched) | n/a | n/a | n/a | n/a | ✅ boots removed; fallback pool clean; contract escape clause removed | n/a | n/a |
| combine_style_qa | ✅ TestQaScoreNormalization | n/a | n/a | n/a | n/a | n/a | ✅ 0/1 scaled; pass+floor-score → warn | n/a |
| Warning dedupe | ✅ TestWarningDedup (source guard) | n/a | n/a | n/a | n/a | n/a | n/a | n/a |
| Funnel input-sanity card | ✅ test_thursday_funnel_card.py + registration test | n/a | n/a | n/a | n/a | n/a | n/a | ✅ red fallback-only / yellow 1-real / green |

**Field verification pending:** next Thursday run (2026-06-18) is the real test — expect 6 novel competitor concepts instead of Cowgirl/Chef, no leg-standing ducks.

---

## Surface 15 — Workflow efficiency program (2026-06-12, matrix before code)

Five fixes from a system-wide overlap/inefficiency audit. Sequence 2→1→5→3→4 (monitored rails first).

### 15.1 duck-ops producer receipts + scheduler_health coverage
Root cause: run_duck_ops_observe_review.sh writes no receipts; scheduler_health.py only discovers com.philtullai.duckagent.* plists → 17 duck-ops jobs unmonitored.

| Use case | Happy | Missing receipt dir | Malformed receipt | Job has schedule but stale receipt | Both wrapper names |
|---|---|---|---|---|---|
| Wrapper writes receipt+history | ✅ planned: test via direct invocation | ✅ planned: mkdir -p | n/a | n/a | n/a |
| scheduler_health parses duck-ops receipt | ✅ planned: test_scheduler_health_duckops_receipt | n/a | ✅ planned: skipped not crash | ✅ planned: missed-run fires | ✅ planned: matcher accepts duckops.*+wrapper |

### 15.2 Revive Etsy tracker / remove Reddit / kill google_trends
Root cause (logged): etsy_token_manager sends `x-api-key: <key>` not `<key>:<secret>` → 403 all searches → empty intel, swallowed, job exits 0.

| Use case | Happy | 403/empty fetch | Reddit removed | Dead module removed | Stale intel |
|---|---|---|---|---|---|
| Etsy header format | ✅ planned: header regression (`:` present) | ✅ planned: empty→_status=empty not bare {} | n/a | n/a | n/a |
| Reddit removal | n/a | n/a | ✅ planned: grep-guard no reddit_weekly_tracker import/merge | n/a | n/a |
| google_trends removal | n/a | n/a | n/a | ✅ planned: grep-guard no GoogleTrends export/import | n/a |
| trend_intel_freshness card | ✅ planned: green populated | ✅ planned: red >2 empty runs | n/a | n/a | ✅ planned + registration test |

### 15.3 Competitor cadence split (daily snapshot / weekly analysis)
Coupling: velocity/temporal deltas need daily dated files with listing_snapshots (_load_previous_snapshot skips files lacking them).

| Use case | Happy | Snapshot-only shape | Velocity across mixed files | Trending unchanged | Scheduler timeout entry |
|---|---|---|---|---|---|
| save_snapshot_only | ✅ planned: passes _load_previous_snapshot filter | ✅ planned: has shop+listing_snapshots | ✅ planned: mixed snapshot/full | ✅ planned: same snapshots→same trending | ✅ planned: new job timeouts added |

### 15.4 Single theme vocabulary
ops+weekly each define get_static_theme_categories; taxonomy is a 3rd. Migrate match_keywords + ai_background into taxonomy (presentation fields, NO version bump).

| Use case | Happy | Legacy shape preserved | Classifier keywords untouched | All call sites repointed | Static copies gone |
|---|---|---|---|---|---|
| theme_vocab helper | ✅ planned: 20 cats legacy shape | ✅ planned: name/keywords/ai_background present | ✅ planned: classifier `keywords` pinned unchanged (Surface 12 safe) | ✅ planned: ops+weekly 5 sites | ✅ planned: grep-guard no get_static_theme_categories |

### 15.5 Monday business digest + email consolidation (REFRAMED)
Premise corrected on inspection: profit/reviews/etc. were ALREADY weekly_monday-gated (not daily) — the daily flood was solved in a prior session. Real opportunity: ~8 separate Monday info-emails → ONE Monday digest. Fold via DUCK_EMAIL_DIGEST_MODE=1; the anomaly bypass still fires same-day.

| Use case | Happy | Partial/missing source | Folded surface defers | Anomaly bypass still fires | Digest surface itself sends | Digest mode off = normal |
|---|---|---|---|---|---|---|
| Fold-mode cadence gate | ✅ test_business_monday_digest::TestFoldMode | n/a | ✅ folded_into_monday_business_digest | ✅ low_rating breaks through | ✅ business_digest never folded | ✅ off → normal Monday send |
| Digest builder | ✅ TestDigestBuilder (profit section from state) | ✅ fail-soft per section, status surfaced | n/a | n/a | n/a | n/a |
| Registry | ✅ test_email_cadence_gate (9 surfaces incl business_digest) | n/a | n/a | n/a | n/a | n/a |

Runtime: DUCK_EMAIL_DIGEST_MODE=1 in duckAgent/.env (Tier 3); com.philtullai.duckops.business-digest.monday plist (Mon 08:00, Tier 3 installed).

---

## Surface 0 (fix) — theme_classification strip regression (2026-06-12)

Root cause: `flows/ops/steps.py` 04:00 sync rebuilt products_cache from Shopify and preserved only `theme_tags`+`ai_theme_category`, STRIPPING `theme_classification` every run (258 nulls in catalog_index). Compounded by post-categorize saves (`:1610`, `:1894`) overwriting freshly-written classifications with stale in-memory `products_out`. Effect: occasion selector silently ran keywords-only (classifier signal weight 3.0 dead) AND a full 258-product LLM re-classification fired daily (266 calls today, 1213 on Jun 11), discarded immediately.

| Use case | Field preserved across rebuild | Falsy/null not copied | Merge-back after categorize | Non-dict tolerated | Tripwire surfaces |
|---|---|---|---|---|---|
| Sync preservation | ✅ test_sync_preserves_classification (theme_classification in PRESERVED list) | ✅ falsy existing not copied | ✅ _merge_cache_enrichment_into pulls disk→memory | ✅ None existing tolerated | n/a |
| occasion_intel coverage | n/a | n/a | n/a | n/a | ✅ classifier_coverage counts classified; 0 on stripped catalog |
| occasion selection card (duckAgent) | n/a | n/a | n/a | n/a | ✅ coverage==0+active→yellow "keywords-only"; absent key not flagged |
| Cost attribution | n/a | n/a | n/a | n/a | ✅ classifier calls log flow=theme_classifier (was "unknown") |

Manual (Tier 2, offered): one-time backfill `python src/main_agent.py --recategorize` (~$0.09, 258 calls) to repopulate the cache the strip emptied; fix makes it persist thereafter.

## Surface 16 — Build-Next ranked queue (2026-06-12, matrix before code)

One weekly answer to "what duck should we build next?": deterministic score = demand × margin × catalog-gap × occasion-fit over EXISTING state (competitor report, profit_per_product, catalog_index, occasion_intel). No new collection, no LLM in scorer. Promote routes into product_concept_queue; credits spent only on operator brief approval. Reuse-first (no duplication): competitor `ducks_to_build`/`trending_products`, profit title-join, occasion_intel direct read, product_concept feedback suppression contract, brief/quality-gate machinery, /inspector-page recipe, weekly-packet nomination slot.

### 16.1 Scoring producer (build_next_engine.py)

| Factor / path | Happy | Missing input degrades | Empty → [] not invented | Suppression | Isolation |
|---|---|---|---|---|---|
| Candidate assembly | ✅ union dedupe by listing_id | n/a | ✅ empty report → [] | n/a | n/a |
| Demand (momentum) | ✅ trending_score (sold_7d) preferred over all-time; separate momentum/all-time pools | ✅ views+favorites fallback / zero pool | n/a | ✅ all-time-only capped at FALLBACK_DEMAND_CEILING; new_listing trending_score (lifetime proxy) NOT treated as momentum | n/a |
| Margin | ✅ confident title match→real % | ✅ no match→flagged median est / no data→neutral flagged | n/a | n/a | n/a |
| Catalog gap | ✅ no overlap=full gap | n/a | n/a | ✅ high overlap→already-made suppressed | n/a |
| Occasion fit | ✅ active kw hit→1.0 | ✅ no match→neutral evergreen | n/a | n/a | n/a |
| build (all) | ✅ ranks, factors, reasons | ✅ missing profit+occasion degrade not crash | ✅ empty report→empty queue | ✅ already-made + operator-rejected | n/a |
| Write | ✅ atomic | n/a | n/a | n/a | ✅ conftest redirect + DUCK_TEST_MODE guard + pollution audit |

### 16.2 Portal page + loader (build_next_intel_page.py, _load_build_next_intel)

| Use case | Happy | Missing state | Empty payload (registration) | Coverage-0 warning |
|---|---|---|---|---|
| Loader | ✅ surfaces top+coverage | ✅ available:false not crash | ✅ empty written file→queue_count 0 | n/a |
| Page render | ✅ ranked queue+suppressed+Promote | ✅ "hasn't written" empty state | n/a | ✅ "classifier coverage 0" banner |

### 16.3 Promote action (governance)

| Use case | Records intent | Never spends credits | Idempotent | Off-policy fails closed | Requires title |
|---|---|---|---|---|---|
| _record_build_next_promotion | ✅ appends brief_source=build_next, status=pending | ✅ file-write only, no builder call (test) | ✅ dedupe per concept_key | n/a | ✅ ValueError on blank |
| product_concept_queue ingest | ✅ promoted→ready_for_brief_review | ✅ approval still gates _run_duck_concept_builder | n/a | ✅ Tennessee Vols→blocked_by_guardrail | n/a |

**16.4 — Demand re-ranked on 7-day momentum (2026-06-26).** Operator wanted Build-Next "more trend focused." The demand factor scored on all-time `engagement_score` (views + favorites×2), which favors cooled-off past hits — Duckpool ranked #1 (engagement 12,323) despite 0 recent sales, while the real #1 mover (Breast Cancer Duck, 25 sold/7d) sat lower. Fix: demand now prefers `trending_score` (the competitor engine's sales-weighted metric, sold_7d-primary), normalized in a pool SEPARATE from all-time so the two scales don't mix; all-time-only rows capped at `FALLBACK_DEMAND_CEILING` (0.5) so a cooled-off hit can't outrank a confirmed mover. **Branch caught in live verify (per [[feedback_incomplete_fix_enumerate_all_branches]]):** a `new_listing` row's `trending_score` is `favs×3 + views×0.5` — a lifetime proxy wearing a momentum label (its `views_delta_7d` is the full lifetime count). `_has_snapshot_momentum` gates on `delta_source` so only snapshot-confirmed movement counts as momentum; new_listing routes to the capped all-time pool. Live result: Breast Cancer 25/7d → #1, Highland Cow 13 → #2, Pitbull 12 → #3, Duckpool → demoted + flagged "no recent-momentum (capped)". Golden regressions: `test_cooled_off_past_hit_loses_to_current_mover`, `test_new_listing_trending_score_is_not_treated_as_momentum`.

### 16.4 Two-card OS bracket (duckAgent viewer)

| Card | Green | Yellow | Red | Registered (incl empty payload) |
|---|---|---|---|---|
| build_next_producer (input) | ✅ fresh+coverage ok | ✅ no state / coverage 0 / >9d | ✅ >16d | ✅ test_build_next_cards_registered + empty |
| build_next_promotion_throughput (output) | ✅ converting | ✅ 5+ pending unconverted | n/a | ✅ same |

### 16.5 Weekly packet nomination (Phase E)

| Use case | Top-3 ranked | Empty queue | Malformed entries |
|---|---|---|---|
| _build_build_next_nominations | ✅ top 3 in rank order + why | ✅ {} / None / empty → [] | ✅ non-dict skipped |

Runtime (Tier 3, NOT auto-installed — operator approval each): com.philtullai.duckops.build-next.weekly plist (Sun 07:00, after competitor weekly 06:30); product_concept_queue scheduled run ingests promotions.

## Surface 17 — Portal readability + ducks_to_build dedupe hardening (2026-06-13)

Operator-reported from phone screenshots: (1) `/portal/intel/reviews` table shattered one char per line; (2) Desk was a flat 15-tile wall with no action/info separation; (3) all `table.grid` intel pages share the same mobile-overflow bug; (4) a competitor "Couple Ducks - Car Dashboard Decor - Cruise Accessory - 3D Printed..." leaked into the Competitors page's ducks_to_build despite an active "Couples Duck" in catalog.

Root cause (4) + sweep finding: dedupe DID exist (`helpers/fuzzy_matching_helper.is_duck_already_made`) but is broadly weak — over the 2026-06-12 `ducks_to_build` (15 items) it suppressed **0**, while we demonstrably already sell several (Couples, Police ×2, Monkey). The keyword-stuffed Etsy titles dilute the holistic fuzz ratio below 0.80, and the catalog's auto-generated `concept_variations`/`core_terms` are noisy theme-phrases, not subject tokens.

A token-overlap "concept identity" patch was tried and **REVERTED**: it caught couples but produced ~9 FALSE POSITIVES (Highland Cow→Bride via noise fragment "adventures -", Police→Blue Jay via "buddy and", Female Golfer→Golf Cart, Tuxedo Cat→Golf Cart via "jeep ducking"). False positives are worse than misses — they stop us building good products. Lesson: lexical/token methods are the wrong tool against keyword-stuffed titles + noisy variation data.

**Shipped fix: semantic embeddings** (`helpers/semantic_dedupe.py`, `config/semantic_dedupe.json` v1, OpenAI text-embedding-3-small). Critical empirical insight: embed the **cleaned subject** (strip "3D Printed / Car Dashboard / Cruise Accessory" boilerplate), NOT the raw title — raw full titles share so much boilerplate that an unrelated duck scored cosine 0.66 vs ours while a true dupe scored 0.62; after cleaning, true dupes land ~0.84 and unrelated ~0.15. Three bands: ≥0.72 hard-suppress (moved to report `ducks_already_made`, transparent), ≥0.43 soft-flag (KEPT + `possible_dupe` badge on Competitors page — never a silent drop), else distinct. Catalog vectors cached (`cache/catalog_subject_embeddings.json`), only new/changed subjects hit the API; weekly run cost ~fractions of a cent. Fails OPEN: embedding outage → lexical fallback + `dedupe_degraded` flag shown on the page. Takes effect next competitor weekly analysis (cached snapshots, no new scraping).

| Use case | already_made (hard) | possible_dupe (soft, kept) | distinct (no false suppress) | Cache | Degrade |
|---|---|---|---|---|---|
| semantic dedupe | ✅ test_semantic_dedupe partitions; eval 2/2 couples+police @0.84 | ✅ monkey 0.44 / golfer 0.54 flagged not dropped | ✅ eval 0 false suppressions; Highland Cow/Vols/cat distinct | ✅ only-new-subjects-hit-API + version-bump invalidation | ✅ fail-open lexical + dedupe_degraded surfaced |

**Gated eval** `scripts/eval_dedupe.py` (real API, manual, NOT in pytest) vs `tests/fixtures/dedupe_golden.json`: SAFETY gate = 0 distinct→already_made; already_made recall ≥80%. Current: **0 false suppressions, 100% already-made recall, 100% dupe recall (≥soft)**. Unit tests `tests/test_semantic_dedupe.py` (mocked embeddings, deterministic). Lexical regression guard retained in `tests/test_fuzzy_matching_dedupe.py` (the reverted over-match must not return). Dead `competitor_semantic_matcher.py` (unused, uncached) deleted in consolidation.

| Surface | Reviews readable | Desk action/info split | All intel tables no-shatter | UI verification |
|---|---|---|---|---|
| Portal readability | ✅ stacked-card layout ≤640px, listing demoted to Etsy link (reviews_intel_page) | ✅ renderDeskGroup two sections, severity-sorted (test_viewer) | ✅ shared portal_shell rule: break-word + horizontal-scroll ≤640px; per-page anywhere→break-word | manual: rendered via live viewer :8765, HTTP 200 all pages; **pixel check on real phone pending** |

---

## Surface 18 — Etsy review-reply auth detection card + one-button re-auth (2026-06-14)

Operator-recurring: the review-reply Playwright login (`esd` storage-state) expires / gets rate-limit-blocked periodically (memory `etsy-review-reply-reauth`). The dead-login signal used to live only in an email alert the operator deletes, so the lane silently stalled (the 7-week Etsy review-reply outage was this, not the selectors). Card `etsy_review_auth` (duckAgent `viewer.py::_load_etsy_review_auth_health`) reads `state/review_reply_execution_auth.json` and goes RED on `auth_status` blocked/errored or `last_error`, YELLOW when the saved login is >21d old, GREEN otherwise. When non-green it carries an `action_button` descriptor; the `/portal/os` page renders a **Re-authenticate Etsy** button (`wireCardActionButtons`) that POSTs `/api/etsy-review-auth/reauth`. The handler (`_handle_etsy_review_reauth`) spawns `duck-ops/runtime/reauth_etsy_review.py` detached: it opens the headed `esd` window at the public shop reviews URL, polls ≤5min for login, then `state-save`s `esd.json` + flips the auth metadata healthy. Read-only w.r.t. reviews (opens a login window only; never clicks reply/submit). Tier-3 browser action whose authorization IS the operator's per-press click.

| Use case | RED detect | YELLOW nudge | GREEN | Button present | Launch handler | Metadata flip |
|---|---|---|---|---|---|---|
| review-reply auth | ✅ test_blocked_is_red / test_last_error_is_red | ✅ test_stale_login_is_yellow (>21d) + test_missing_is_yellow | ✅ test_fresh_healthy_is_green | ✅ test_red_card_carries_reauth_button (endpoint `/api/etsy-review-auth/reauth`) / test_green_card_has_no_button | ✅ test_launch_spawns_and_seeds_status (Popen mocked — no browser in CI), test_missing_script_raises | ✅ mark_auth_healthy smoke: blocked+last_error → healthy/None, storage saved |

Card registration: test_card_registered_red + test_card_registered_on_empty_payload (empty-payload registration per memory `loader-and-registration-both-need-tests`). All in duckAgent `tests/test_etsy_review_auth_card.py` (11 tests). **Not browser-tested:** the red-state button's live visual (forcing red would pollute prod state / burn an Etsy window) — covered by the unit path instead.

---

## Surface 19 — OS red/yellow sweep fixes (2026-06-14)

Operator asked "anything still red in OS?" — five fixes from the sweep:

1. **llm_cost_dashboard pricing (duckAgent)** — the viewer-side `_LLM_MODEL_PRICING_USD_PER_1M` had no image models, so 12 image calls (gpt-image-2/dall-e-3) were unpriced → card yellow. Added `_LLM_IMAGE_COST_USD` + `_llm_entry_cost_usd` per-call image handling (parallel to the duck-ops `PER_CALL_IMAGE_COST_USD` fix in Surface 10). Tests: `test_llm_cost_dashboard_pricing.py` (gpt-image-2 / dall-e-3 priced; text still token-priced; genuinely-unknown model still None).
2. **scheduler_health false alarms** — 4 weekly governance jobs (competitor_benchmark, data_model/documentation governance, reliability_review) + business_digest_monday showed "missed" because they last ran before the 2026-06-12 receipt mechanism (or hadn't hit their day). Generated first receipts via the wrapper → 0 bad. business_digest_monday: runs=0 was just "hasn't hit a Monday since install" (next fire Mon), not broken.
3. **load_json crash on write-race (duck-ops)** — `governance_review_common.load_json` defaulted on a missing file but crashed (`JSONDecodeError`) on an empty/partial sibling file observed mid-write by another producer; `data_model_governance_weekly` died this way. Now treats unreadable/malformed as absent. Tests: `test_governance_review_common.py` (empty/partial/valid).
4. **Etsy trend failure cause (duckAgent)** — `trend_intel_freshness` only GUESSED "credential failure" on 0 signals. The tracker now records classified `api_errors` (auth/rate_limit/timeout/api_error) and exposes `api_error_summary()`; `flows/ops/steps.py` writes the real `_status` (auth_error names a key to renew vs a clean `empty`); the card flags any non-ok status red. Tests in `test_trend_intel_card.py::FailureCauseTests`.
5. **Thursday/weekly choose-final on /portal/decisions (duckAgent)** — stranded `workflow_control` awaiting_review records (choose-final emailed, email deleted) now surface as `workflow_decision` Decision-Inbox items with a Dismiss action (dismiss → duck-ops `record_workflow_transition(state="dismissed")` un-parks the lane; no Etsy/Shopify mutation). email-to-portal inversion. Tests: `test_workflow_decision_inbox.py` (item builder stale/fresh/resolved/lane filters; dismiss handler calls transition with dismissed state; rejects bad action/lane). Browser-verified: item + Dismiss button render on /portal/decisions @390px.

| Use case | Detect/Surface | Action | Negative/guard | UI verified |
|---|---|---|---|---|
| cost image pricing | ✅ gpt-image-2/dall-e-3 priced | n/a | ✅ unknown model still None | n/a |
| governance load_json | ✅ empty/partial → default | n/a | ✅ valid JSON still loads | n/a |
| etsy trend cause | ✅ auth_error/rate_limited red + named | n/a | ✅ clean empty ≠ auth red | n/a |
| thursday strand | ✅ workflow_decision item built | ✅ dismiss → transition dismissed | ✅ fresh/resolved/lane excluded; bad action/lane rejected | ✅ item+button @390px |

**SEO audit cadence (2026-06-14, follow-up):** operator asked "when does SEO run?" — the daily 07:35 kickoff reused a CACHED audit and only rebuilt on `--force-audit`, so the snapshot drifted to 37d (the real reason for the persistent SEO-freshness yellow). `shopify_seo_kickoff.kickoff_shopify_seo_review` now rebuilds the audit when it's older than `SEO_AUDIT_MAX_AGE_DAYS` (7d), at the top of the function so even a long-parked review can't let it drift. Card-messaging cards (sale-monitor threshold, SEO action text) were the symptom; this is the root cause. One-off `build_shopify_seo_audit()` run refreshed it now (107 actionable resources → card green). Tests: `test_shopify_seo_kickoff.py::AuditFreshnessGateTests` (stale rebuilds even with review open; fresh doesn't; 7d boundary). Review-flow tests patched to default the gate off.

---

## Surface 20 — Theme Review Feedback Loop (2026-06-15, matrix before code)

**Background:** the theme classifier flags products `needs_review` when its LLM pick disagrees with a deterministic keyword cross-check, but those flags only lived in `duckAgent/state/theme_classification_health.json` with no review surface (the OS card pointed at the raw JSON). Operators couldn't review/correct from the portal, corrections didn't persist (a taxonomy-version bump re-flagged them), and the keyword check over-fires on persona ducks (a "safari guide" flagged toward Animals & Pets because of the word "safari"; a "dad at the grill" toward Food & Beverages because of "spatula"). This loop adds: Decision-Inbox review (Approve / Keep current / Change to…), a persistent operator-override store the classifier respects, false-keyword-flag logging to tune the over-firing rules, and golden-fixture append for the gated eval. Built on two existing rails — the trend `mark_duplicate → catalog_aliases` decide-and-persist loop and the theme classifier's own LLM-surface recipe.

Decision-type semantics: **approve** = adopt the keyword-suggested category; **keep_current** = reject the flag, keep existing (and log a false keyword flag); **change_to** = operator picks any taxonomy category (server-validated).

| Use case | Happy | Bad input | Re-flag / drift | Isolation | Gated |
|---|---|---|---|---|---|
| Override store (duck-ops `theme_review_decisions.py`) | ✅ planned: upsert-by-handle, atomic, idempotent overwrite (`test_theme_review_decisions`) | n/a | n/a | ✅ planned: 3-layer — conftest both repos + `TestModeRefusalError` frozen-path guard + `test_no_test_pollution_in_theme_review_decisions` | n/a |
| Classifier respects override | ✅ planned: override pins primary + clears needs_review (`test_override_suppresses_reflag`) | n/a | ✅ planned: survives taxonomy bump (`test_override_survives_taxonomy_bump`); input-hash drift → `override_stale_inputs` surfaced not silently pinned | ✅ planned: override loader mocked | n/a |
| Inbox item source (`_theme_review_decision_items`) | ✅ planned: parses suggested from `keyword_evidence_favors:X`, includes `change_options` | ✅ planned: low_confidence/off_taxonomy reasons render Keep+Change only (no Approve) | ✅ planned: items already overridden are skipped | n/a | n/a |
| Apply decision (handler + `apply_theme_review_decision`) | ✅ planned: approve/keep/change_to write override + products_cache (primary, needs_review, ai_theme_category) + re-run health | ✅ planned: `change_to` off-taxonomy rejected server-side (not just JS) | ✅ planned: never touches Shopify `category` field; drift alarm in health producer | ✅ planned: cache + store patched to tmp | n/a |
| Keyword false-flag feedback | ✅ planned: keep_current on keyword flag logs matched over-firing keyword (`theme_keyword_false_flags.json`) | ✅ planned: non-keyword reasons don't log | n/a | ✅ planned: same store isolation | ⚠️ taxonomy `keywords` edit is gated (config change → eval gate), only logging is automatic |
| Classifier learning | ✅ planned: correction appends to EXISTING `theme_classifier_golden.json` (idempotent by handle) | ✅ planned: keep_current does NOT append (no correction) | n/a | n/a | ⚠️ accuracy improvement rides existing `eval_theme_classifier.py` (≥90% gate) — append automatic, eval+model/prompt change manual |

**No Tier-3 in the core loop** — all writes are internal fields (`theme_classification`/`ai_theme_category`); Shopify product taxonomy is deliberately untouched (pushing the corrected category to Shopify would be a separate Tier-3 enhancement, out of scope). Propagation to duck-ops `catalog_index` (→ occasion targeting) is free via the next `phase1_observer` run.

---

## Surface 21 — Build-Next algorithm fixes (2026-06-15, matrix before code)

**Background:** after fixing the cadence-split regression that emptied Build-Next, the populated output exposed three algorithm flaws (operator: "is our algorithm good?"). (1) **Suppression false-positives:** the token-overlap "already made" check (`overlap = shared/smaller_set`, threshold 0.6) stripped duck-niche stopwords but not car-accessory boilerplate (car/decor/decoration/vehicle/accessory), so our "Owl Duck" `{owl,car,decor}` matched ANY competitor with car+decor at 67% — 18 of 20 suppressions were this false "Owl Duck" match, throwing away buildable products (Couple Ducks, Panda Bear, Beaver…). Same bug class fixed elsewhere with semantic embeddings. (2) **Margin dead:** every candidate got the flat global-median fallback (0.92) because competitor titles never token-matched our confident-margin products → margin contributed zero ranking signal. (3) **Off-brand leakage:** `trending_products` (unfiltered) put "Mini Pizza Fidget Toy" at rank #1.

**Fixes:** (1) semantic suppression via the existing `semantic_dedupe` embeddings (band `already_made` ≥0.72), with a **fixed-stopword token method as graceful fallback** when embeddings are unavailable (no API in env) — the stopword list now strips the car-accessory boilerplate too, so even the fallback kills the Owl Duck match. (2) margin via the semantic catalog-match joined to `profit_per_product` margin (catalog_title→margin from profit `title_variants`), median only when no match. (3) on-brand gate: a `trending_products`-only candidate must contain duck/dashboard signal or it's suppressed as off-brand.

| Use case | Happy | False-positive guard | Degrade (no API) | Off-brand |
|---|---|---|---|---|
| Suppression (already-made) | ✅ planned: semantic band already_made suppresses a true dup | ✅ planned: Owl Duck no longer matches Couple Ducks/Panda (semantic AND fixed-stopword token) | ✅ planned: empty semantic_map → fixed token method, still correct | n/a |
| Margin factor | ✅ planned: semantic catalog-match → that product's margin (varies per candidate) | n/a | ✅ planned: no match → median fallback (documented proxy) | n/a |
| Trending relevance | n/a | n/a | n/a | ✅ planned: trending-only off-brand (Pizza Fidget Toy) suppressed; ducks_to_build candidates always kept |
| Stopword boilerplate | ✅ planned: car/decor/decoration/vehicle/accessory/cruise/holder/buddy stripped | ✅ planned: regression test pins Owl-Duck-style non-match | n/a | n/a |

**Tier:** none (read-only producer). Semantic adds one cached embedding call per run; graceful-degrades to deterministic token method, so the producer never hard-depends on the API. Tests inject a fake semantic_map / embed_fn — no live API in pytest.

---

## Surface 22 — Review-reply drain starvation + failed-recovery (2026-06-15, matrix before code)

**Background:** auth was restored 2026-06-14 but the drain still posted 0 replies. Two root causes. (1) **Sync starves drain:** `etsy_browser_batch.run_slot` ran the customer-read sync, then `auto_enqueue_publish_ready`, then `drain_queue` in one process sharing the `etsy_browser_guard` budget (`MAX_COMMANDS_PER_WINDOW`=18 visible cmds / 5 min). Reads exhausted the budget, so the drain hit `rate_limit_preemptive_cooldown` and posted nothing. (2) **`failed` items stranded:** `auto_enqueue_publish_ready` only picks `not_queued`; `drain_queue` only posts `queued`; nothing retries `execution_state=failed`. The freshest stranded replies (June 8 review, drafted June 9, ~6d old, inside the 14d window) had failed with the COOLDOWN error itself — recoverable once (1) is fixed.

**Fixes:** (1) **drain-first reorder** (post before sync; drain reads only the queue + quality-gate state, never sync output → dependency-safe). (2) **budget reservation** (`RESERVED_FOR_MUTATING`=4): read-only commands soft-deferred via `PacingReservationError` once they'd eat the reserve — does NOT persist a cooldown, so posts stay free; the sync treats it as `paced_out`, not a slot failure. (3) **failed-recovery sweep** `requeue_recoverable_failed` runs in `drain_queue` after auto-dismiss: re-queues `failed`→`queued` only when within `auto_dismiss_after_days` (14d, fail-closed on unparseable date), under `failed_requeue_max_attempts` (2, independent of historical attempts so the outage backlog gets a fresh chance), and `failure_is_retryable` (conservative allowlist: cooldown/pacing, `auth_required`, `review_row_not_found`; EXCLUDES `*_mismatch` drift + generic `unexpected_executor_failure`).

| Use case | Happy | Cooldown still trips (regression) | Stale / drift excluded | Cap / disabled | Fail-closed |
|---|---|---|---|---|---|
| Drain-first ordering | ✅ `test_etsy_browser_batch.py::test_run_slot_drains_replies_before_customer_read` | n/a | n/a | n/a | ✅ `test_customer_read_pacing_refusal_is_not_a_slot_failure` (paced_out, not failed) |
| Budget reservation | ✅ `test_etsy_browser_guard.py::ReservationTests::test_post_allowed_when_reads_filled_reserve` + `test_read_allowed_below_reserve` | ✅ `::test_hard_cooldown_still_trips_at_max` (post at 18 → persistent cooldown) | n/a | n/a | ✅ `::test_read_deferred_when_reserve_reached` (no blocked_until persisted) |
| `failure_is_retryable` allowlist | ✅ `test_review_reply_failed_recovery.py::test_retryable_cooldown` + `test_retryable_auth_and_row_not_found` | n/a | ✅ `test_not_retryable_drift_or_generic` (mismatch/unexpected/None → False) | n/a | ✅ same |
| `requeue_recoverable_failed` | ✅ `test_recovers_fresh_cooldown_failed` + `test_recovers_fresh_auth_failed` | n/a | ✅ `test_skips_stale_failed` (45d), `test_skips_transaction_mismatch_even_if_fresh` | ✅ `test_respects_recovery_cap` (count≥2), `test_disabled_by_policy` | ✅ `test_unparseable_date_not_recovered`, `test_ignores_non_failed_and_other_flows` |
| Test isolation | ✅ `conftest.py` now monkeypatches `QUALITY_GATE_STATE_PATH` (the sweep writes it) alongside the queue path | n/a | n/a | n/a | n/a |

**Observability:** the existing `review_reply_throughput` OS card (Surface 5, duckAgent) already goes RED on "N approved / 0 posted" over 14d and a cooldown-blocked drain writes no per-item receipt → the stall stays visible. No card schema change; verify it still fires post-fix. **Tier:** code is Tier-2 (queue logic + ordering); the actual drain/post run is Tier-3 (operator-triggered, unchanged). One-shot backlog recovery of the ~3 fresh June-9 `failed` items happens organically on the next drain under the same gates.

**Live-run follow-up (2026-06-15 15:27 afternoon window).** All three fixes fired in prod: no cooldown, drain-first, and `requeue_recoverable_failed` recovered 3 in-window items (June-2 + two June-8). The drain then **attempted a post** (first real attempt since the outage) — it failed with `review_row_transaction_mismatch` (genuine Etsy transaction_id drift on the 13-day item), correctly excluded from re-recovery so no loop. One side effect surfaced: the budget reservation deferred the customer-inbox reads, but `run_refresh` swallows the `PacingReservationError` per-thread (marks threads `failed`), so the slot went RED on an *intentional* throttle. Fix: `_run_customer_read_batch` now reclassifies a result whose failures are ALL pacing/cooldown throttles to `status="paced_out"` (non-failing in `_overall_status`); also catches a propagating cooldown `RuntimeError`.

| Use case | Happy | Throttle (deferral) | Throttle (cooldown) | Genuine failure |
|---|---|---|---|---|
| Customer-read pacing reclassification | ✅ `test_etsy_browser_batch.py::test_overall_status_treats_paced_out_as_non_failing` | ✅ `::test_per_thread_pacing_deferrals_reclassified_as_paced_out` + `::test_customer_read_pacing_refusal_is_not_a_slot_failure` (propagating) | ✅ `::test_hard_cooldown_runtimeerror_is_paced_out_not_raised` | ✅ `::test_genuine_read_failure_stays_failed` (non-pacing reason still RED) |

Open question pending the 20:11 evening window: whether the two 7-day June-8 items post or also drift. If they drift, tighten the recovery freshness window (~7d) since old-review recovery is futile against Etsy ID drift — the durable win is prompt posting of *new* reviews (now unblocked).

---

## Surface 23 — Operator-toggleable email cadences (2026-06-17, matrix before code)

**Background:** the 9 email surfaces' cadences are hardcoded `CadencePolicy` constants in `email_cadence_gate.py`; changing one (e.g. muting `business_intelligence`) needed a code commit. This adds an operator-writable override store (`state/email_cadence_overrides.json`, `{surface: "off"|"weekly"|"daily"}`) that `should_send_email` overlays on the default, plus a portal "Email cadence" section (Phase 2). Operator decisions (2026-06-17): **off keeps urgent anomaly alerts** (off stops only the routine send; a `bypass_keys` anomaly still fires same-day) and **off stops the standalone email but the surface stays in the Monday digest** (don't touch `business_monday_digest.py`).

| Use case | Happy | Override absent / bad file | "off" behavior | Unknown / digest |
|---|---|---|---|---|
| Override overlays default | ✅ `test_email_cadence_overrides.py::test_override_weekly_beats_daily_default` (+ daily-beats-weekly) | ✅ `::test_missing_file_uses_hardcoded_default`, `::test_corrupt_file_falls_back_to_default` (fail-soft, never crashes the send path) | n/a | n/a |
| "off" suppresses routine | n/a | n/a | ✅ `::test_off_suppresses_routine_send` (no send Mon or weekday) | n/a |
| "off" keeps anomaly bypass | n/a | n/a | ✅ `::test_off_still_fires_on_anomaly_bypass` (surface with a real bypass key still breaks through); `::test_off_with_no_bypass_keys_fully_silent` (business_intelligence) | n/a |
| set_override validation | ✅ `::test_set_override_persists_and_reads_back` | n/a | n/a | ✅ `::test_set_override_unknown_surface_raises`, `::test_set_override_bad_cadence_raises` |
| Test-mode write guard | n/a | n/a | n/a | ✅ `test_no_test_pollution_in_email_cadence_overrides.py` (audit) + `::test_set_override_refuses_frozen_prod_path_under_test_mode` |
| Digest interaction | ✅ `::test_off_surface_still_eligible_for_monday_digest` (off doesn't remove it from DIGEST_FOLDED) | n/a | n/a | n/a |
| Cross-repo read (Phase 2) | ✅ `duckAgent/tests/test_cadence_override_bridge.py::test_cross_repo_write_visible_through_gate` (bridge writes, the duck-ops gate instance sees it) + `::test_set_then_list_reflects_override` | ✅ `::test_list_includes_all_surfaces_default` (gate unavailable → [] / structured error, never crashes the page) | n/a | ✅ `::test_set_unknown_surface_returns_structured_error`, `::test_set_bad_cadence_returns_structured_error` |
| Portal action (Phase 2) | ✅ `duckAgent/.../test_viewer.py::test_workflows_status_page_exposes_email_cadence_section` (Workflows page wires `/api/email-cadence` GET+POST, off/weekly/daily selector); GET lists effective cadence + source + folded flag; POST persists via the bridge; no Tier-3 gate | n/a | n/a | n/a |

**Isolation:** `EMAIL_CADENCE_OVERRIDES_PATH` is a new module-level prod-path constant → 3-layer isolation in the same commit (autouse conftest monkeypatch in BOTH `duck-ops/tests/conftest.py` and `duckAgent/tests/conftest.py` since duckAgent tests import the gate via the loader; source-level `TestModeRefusalError` guard in `set_override`; post-suite pollution-audit test). **Tier:** Phase 1 is read/config (Tier-2); the email toggle is a low-risk notification preference (NO Tier-3 reason+confirm, unlike the Workflows off-switch).

---

## Surface 24 — Restore review-reply post-queue feed via structured handoff (2026-06-19/20)

**Background:** the review-reply post-queue's only feed was `phase1_observer` parsing the daily reviews EMAIL (`recent_review_summary_emails` → `parse_positive_review_replies` → `build_review_reply_candidate_from_email`). The Surface-15.5 email fold (reviews → Monday digest) meant the email stopped sending most days, so the observer harvested 0 `reviews_reply_positive` candidates and the queue silently starved (zero new replies posted since ~2026-05-27). Classic "a notification and a data pipeline must never be the same artifact."

**Approach (operator chose ROBUST 2026-06-19):** decouple the data handoff from the foldable email. The duckAgent reviews flow (which already fetches reviews via the Etsy API with canonical `transaction_id`/`listing_id` and drafts replies) persists a structured `reviews_reply_handoff` into its run state; the duck-ops observer reads THAT directly and reuses the existing candidate builder + quality gate + `auto_enqueue` + drain. No new parallel ingest. *(An earlier parallel-ingest build — old Surface 24, commits 90ad453/0aeea25 — was reverted (aac00c8) after the operator caught it duplicating `build_review_reply_candidate_from_email` + `quality_gate_pilot.main`.)*

Because the handoff carries Etsy's own ids, `resolve_review_target` short-circuits to `match_quality="api_exact"` — no fuzzy text-match, which also removes the `review_row_transaction_mismatch` failure class that stranded the prior backlog.

| Use case | Happy | Missing identifiers | No draft / wrong kind | Staleness / non-date dirs | Email-fold regression |
|---|---|---|---|---|---|
| Handoff persisted (Phase A, duckAgent) | ✅ `duckAgent/tests/test_reviews_reply_handoff.py::test_handoff_pairs_ids_with_reply` + `::test_summary_thank_you_messages_carry_canonical_ids` (enrichment) | ✅ `::test_handoff_blank_ids_become_none` | n/a | n/a | n/a |
| Deterministic targeting (Phase B) | ✅ `test_review_reply_handoff_observer.py::test_resolve_target_short_circuits_on_canonical_ids` (api_exact) | ✅ `::test_resolve_target_without_ids_does_not_claim_api_exact` | n/a | n/a | n/a |
| Observer reads handoff → candidate | ✅ `::test_handoff_builds_candidate_with_api_exact_target` (artifact_id ::tx-, api_exact target, reuses build_review_reply_candidate_from_email) | ✅ `::test_reply_without_ids_is_skipped` (fail-closed, never guess) | ✅ `::test_reply_without_draft_is_skipped`, `::test_non_public_kind_is_skipped` | ✅ `::test_old_and_nondate_dirs_ignored` (10-day window; TEST-RUN skipped) | n/a |
| Handoff enumeration | ✅ `::test_recent_handoffs_found` | n/a | n/a | ✅ same staleness test | n/a |
| Feed-freshness SLO card (Phase C) | ✅ `duckAgent/.../test_review_reply_feed_freshness.py::test_handoff_fed_is_green` + `::test_stale_unfed_past_grace_is_red` (the starvation signal) + `::test_recent_unfed_within_grace_is_yellow` + `test_os_card_registration.py::test_review_reply_feed_freshness_registered_on_empty_payload` | ✅ `::test_reply_without_ids_not_counted_as_arrived` | n/a | ✅ `::test_no_handoffs_is_green` (nothing to feed) | ✅ a stale un-fed handoff goes RED instead of silent — exactly the 3-week outage signal |

**Why robust, not minimal:** the minimal option (always emit the parseable email even when the operator notification folds) keeps data riding on an email + regex round-trip. The structured handoff removes that coupling permanently and reuses every downstream stage. The email-parse path stays as a fallback. **Tier:** the handoff write + observer read are Tier-2; actual posting stays gated by the existing approval + drain chain.

---

## Surface 25 — Etsy image upload verify + repair (2026-06-20, field bug)

**Background:** operator: "why did the newduck flow today only upload a couple photos?" Today's run uploaded **10 images to Etsy, all returned HTTP 201 with distinct `listing_image_id`s, zero failures** — yet the live listing kept only **2** (confirmed via read-only API GET). Etsy's image endpoint is **accept-synchronously, process-asynchronously**: the 201 means "queued," not "stuck," and Etsy silently dropped images 3–10 in post-upload processing (most likely near-duplicate rejection of similar product shots). The upload response is structurally incapable of reporting this, and we discarded the returned ids + never read back, so the loss was invisible (buried in the run log; no state, no card). Same "success ≠ verified result / alive ≠ progress" pattern as Surfaces 22/24. Operator chose **auto-retry-once-then-flag** + **add pacing now**.

| Use case | Happy (all stick) | Throttle (partial then recover) | Dedup (persistent shortfall) | Upload error | Edge |
|---|---|---|---|---|---|
| `etsy_upload_images_verified` (Phase 1) | ✅ `duckAgent/tests/test_etsy_image_verify.py::test_all_stick_first_pass_no_retry` | ✅ `::test_throttle_first_pass_partial_then_retry_recovers` (re-upload recovers, complete) | ✅ `::test_dedup_persistent_shortfall_retries_once_then_stops` (retries the 8 EXACTLY once, then STOPS — never loops) | ✅ `::test_upload_exception_counts_as_missing_not_crash` | ✅ `::test_caps_at_ten_images`, `::test_pacing_sleep_called_between_uploads`, `::test_empty_images_is_not_complete` |
| Publish records result + email surface (Phase 2) | ✅ `test_newduck_etsy_image_check.py::test_complete_upload_is_ok_check` | n/a | ✅ `::test_shortfall_is_not_ok_and_names_missing` | n/a | ✅ `::test_absent_upload_yields_no_check`, `::test_shortfall_check_is_not_in_blocking_set` (warns, never blocks Shopify activation) |
| `etsy_image_completeness` OS card (Phase 3) | ✅ `test_etsy_image_completeness_card.py::test_complete_upload_is_green` | n/a | ✅ `::test_shortfall_is_red` (names listing + counts) + `test_os_card_registration.py::test_etsy_image_completeness_registered_on_empty_payload` | n/a | ✅ `::test_job_without_recorded_result_is_ignored`, `::test_sent_zero_is_ignored`, `::test_old_job_outside_window_ignored`, `::test_no_runs_dir_is_yellow` |

**Critical design guard — never loop:** a paced single retry fixes the *throttle* cause; if it's *still* short, that's near-duplicate rejection (retrying is futile + spammy), so it STOPS and flags (state + email row + red OS card). Pacing (`ETSY_IMAGE_UPLOAD_PACING_SECONDS`, default 1.5s) between uploads is the cheap throttle mitigation on the very next run. **Tier:** the verify read-back is read-only; the single re-upload is within the publish operation's existing Tier-3 scope (no NEW approval). Etsy stays a fixable draft — a shortfall never blocks activation. Memory: [[etsy-image-upload-accept-then-async-drop]].

---

## Surface 26 — Competitor weekly-sales: fix inflation + my-shop row (2026-06-21, field bug)

**Background:** operator read the competitor email, saw top shops with "~2,000 / ~500 sales this past week" and asked "is that real?" + "add my shop so I can compare." **It's NOT real.** Etsy's API exposes no per-shop weekly sales — only LIFETIME `transaction_sold_count`. The report infers weekly units from listing quantity drops, and the disappeared-listing path (`competitor_engine.py`) counted a listing's ENTIRE previous quantity as sold (a delisted made-to-order item, qty ~999, = +999 phantom sales → the ~2,000). Lives in duckAgent `flows/competitor/`.

| Slice | Happy | Cap / edge | Building / fail-soft | Render |
|---|---|---|---|---|
| **1 — cap the estimate** (`_capped_sold`) | ✅ `tests/test_competitor_sold_units.py::test_normal_drop_within_cap_counts_fully` | ✅ `::test_huge_drop_is_clamped_to_cap` (999→5 ⇒ 25), `::test_restock_is_zero_never_negative`, `::test_none_quantities_are_zero` | n/a | ✅ `::test_report_labels_sold_columns_as_estimates` ("(est.)" + caption) |
| cap config | ✅ `::test_config_default_is_25` | ✅ `::test_env_override_changes_cap`, `::test_config_env_override` (`COMPETITOR_MAX_WEEKLY_SOLD_PER_LISTING`) | n/a | n/a |
| **2 — my actual 7d** (`_get_my_actual_sold_7d`, Etsy transactions sum-of-quantity) | ✅ `test_competitor_my_shop_row.py::test_estimate_from_prior_snapshot` (actual carried) | n/a | ✅ `::test_actual_none_is_carried_through`, `::test_html_row_actual_na` | ✅ `::test_html_row_estimate` (`9 actual`) |
| **3 — my apples-to-apples estimate** (dedicated snapshot, same capped method; my shop NEVER enters competitor analysis) | ✅ `::test_estimate_from_prior_snapshot` (10+2 ⇒ 12) | ✅ `::test_estimate_is_capped` (999-drop ⇒ 25) | ✅ `::test_building_state_when_no_prior_snapshot` (status "building", not a fabricated 0) | ✅ `::test_html_row_building`, `::test_html_row_estimate`, `::test_text_report_has_my_shop_line` |

**Design choices:** (1) cap default **25**/listing/week — a 3D-print listing selling >25/wk is exceptional (shop's own best ~6/wk), so 25 never clips a real seller but kills the phantom. (2) My shop is tracked in a DEDICATED snapshot file (`cache/competitor/snapshots/my_shop_*.json`), never mixed into competitor snapshots — so it can't pollute ducks-to-build / gaps / pricing (avoids the error-prone "exclude my_shop at 6 analyzer sites" approach). (3) My row shows BOTH a capped estimate (ranks fairly vs competitors) AND the real actual (truth) — the est-vs-actual gap also calibrates how off the competitor estimates are. **Anti-mislead:** the cap (Slice 1) ships first/with the actual number so a real `12 actual` is never compared against an uncapped `2,000`. **Tier:** read-only (report generation), no mutations.

**Surface 26 fix (2026-06-21, operator "evaluate it" follow-up):** the my-shop row rendered `— · actual n/a` every time. Root cause: `my_shop_velocity` was a runtime attribute on `CompetitorAnalysisReport`, **absent from `to_dict()`/`from_dict()` and from `steps.py`'s manual rebuild** — and the emailed HTML is rendered off the report REBUILT from the saved JSON, so the renderer always saw `None`. The existing tests covered rendering but never the serialization round-trip — the gap that let it ship. Fix: add the field to the dataclass + `to_dict`/`from_dict` + the `steps.py` rebuild. Now renders `~14 est. · 9 actual` (and `building… · 9 actual` until ~7 days of daily snapshots accrue; `actual_sold_7d` works immediately). New regression tests: `test_competitor_my_shop_row.py::test_my_shop_velocity_survives_to_dict_from_dict`, `::test_rebuilt_report_renders_real_numbers_not_na`. (Snapshot dir was empty only because the daily `save_snapshot_only` pass hadn't run since the feature landed; `ETSY_SHOP_ID` is set.)

**Second Surface 26 bug (2026-06-21, live probe of the actual number):** computing the real number returned **`actual_sold_7d: 9348` from 7,748 transactions for a 7-day window** — i.e. LIFETIME, not 7 days. `etsy_get_all_shop_transactions` passed `min_created`/`max_created`, but the Etsy v3 shop-transactions endpoint **ignores them** (response `count` stays 7,748 regardless) and returns the whole history. Same "is that real?" inflation class the operator already flagged for competitors. Fix: filter client-side on each transaction's `created_timestamp` within `[start, end]`, with an early-stop (endpoint returns newest-first) so a 7-day query reads ~1 page not ~78. Real number after fix: **64 units / 53 orders** in 7d (2.1s vs all-pages). Regression: `duckAgent/tests/test_etsy_transactions_window.py` (window filter, inclusive boundaries, early-stop doesn't page the old tail, missing-timestamp skipped). Only caller was the my-shop actual, so blast radius was contained.

---

## Surface 28 — Build-Next "possible dupe" flag + one-click confirm (2026-06-21, operator question)

**Background:** operator asked "how do we confirm a duplicate so we don't show those? Or should we still show?" Investigation found the dedupe has three bands (`config/semantic_dedupe.json`: ≥0.72 `already_made` hard-suppress, 0.43–0.72 `possible_dupe` soft-flag, <0.43 `distinct`) but the **Build-Next queue producer dropped the soft-flag entirely** — only `already_made` was honored, so a near-dup like "Dachshund Duck" (0.71 vs the existing "Dachshund Duck - Doxie") ranked **#1 with no warning**. The threshold is too fuzzy to auto-hide (a Labrador was hidden at 0.721 vs a *Golden* Retriever). Decision: **keep showing the soft-flag band, but make it visible + give a one-click confirm** so the operator teaches the system instead of the threshold guessing.

| Slice | Happy | Edge / guard | Confirm loop | Surface |
|---|---|---|---|---|
| **1 — carry the flag** (`build_next_engine` Pass B) | ✅ `test_build_next_engine.py::TestDupeFlag::test_possible_dupe_kept_and_flagged` (kept + `possible_dupe`/`dupe_score`) | ✅ `::test_distinct_band_not_flagged` | n/a | duck-ops producer |
| **2 — dupe-decision store** (`record_dupe_decision`, 3-layer isolation) | ✅ `::test_record_round_trips_by_concept_key` | ✅ `::test_record_rejects_bad_decision`, `::test_dupe_write_guard_refuses_prod_path_in_test_mode`, `test_no_test_pollution_in_build_next_dupe_decisions.py` | n/a | duck-ops state |
| **3 — engine honors rulings** | n/a | n/a | ✅ `::test_operator_confirmed_duplicate_is_suppressed`, `::test_operator_distinct_clears_the_flag` | duck-ops producer |
| **4 — portal flag + buttons** | ✅ `test_build_next_page.py::test_improve_existing_section_renders_buttons_and_links` (Confirm/Not-a-dup in the Improve-Existing section; renamed in Surface 48) | ✅ `::test_build_new_entry_has_promote_no_improve_section` | ✅ `::TestDupeDecisionEndpoint` (bad decision / empty title → ValueError) | duckAgent `build_next_intel_page` |
| **5 — cross-repo write** (`/api/build-next/dupe-decision` → `build_next_dupe_loader` → duck-ops `record_dupe_decision`) | manual: live POST returned `{ok:true}`, wrote `state/build_next_dupe_decisions.json` keyed by the producer's `_concept_feedback_key` (test entry removed); bad decision → HTTP 400 | n/a | n/a | duckAgent viewer route |

**Design choices:** (1) the 0.43–0.72 band is **shown + flagged, never auto-hidden** — the Labrador→Golden false-positive proves hiding drops distinct ducks. (2) One store (`build_next_dupe_decisions.json`) for both rulings: `duplicate` → suppress next build, `distinct` → stop flagging. Separate from the concept-feedback store so a candidate can be ruled on BEFORE promotion. (3) The portal writes through the producer's `record_dupe_decision` via a bridge loader (mirrors `cadence_gate_loader`) so the concept-key normalization + DUCK_TEST_MODE write guard are a single source of truth — NOT replicated (the promote handler's inline key normalization differs and must not be reused here). (4) Verified end-to-end in headless Chromium: 8/12 real queue rows flagged after a producer re-run, Dachshund #1 shows "⚠ possible dupe of Dachshund Duck - Doxie (71%)". **Tier:** records a ranking preference; never spends credits.

---

## Surface 27 — Shared portal async-state UI (2026-06-21, field bug)

**Background:** operator: "the library on our duck page isn't loading… shouldn't we have something dynamic when it is loading and a failure and how to fix it otherwise?" Live server probe found the backend **fully healthy** — `/portal/library` 200, `/api/studio-assets` 200 (65 assets, valid JSON), `/api/studio-templates` 200, `/files` thumbnail route 200. **That diagnosis was incomplete.** The operator pushed back ("can't you load the page yourself?"); driving headless Chromium (Playwright) at the live page revealed the REAL cause: **two pre-existing JS syntax errors in non-raw Python triple-quoted page HTML** — `PORTAL_LIBRARY_HTML` had `lastIndexOf("\\\\")` which Python collapsed to JS `lastIndexOf("\\")` (backslash escapes the quote ⇒ broken), and `PORTAL_DECISIONS_HTML` had `.join("\\n")` which Python turned into a real newline inside the string literal. Each is a `SyntaxError: Invalid or unexpected token` that kills the ENTIRE inline `<script>`, so `loadAssets`/the launcher never ran and the page rendered nothing (stuck on the static "Loading records." placeholder). **Server-side route/API checks return 200 regardless — only parsing the emitted client JS catches this.** Lesson reinforced: [[verify_external_behavior_before_root_cause]] — a 200 from every endpoint is not "the page works"; load it in a browser. Fixes: `"\\\\"`→`"\\\\\\\\"` and `"\\n"`→`"\\\\n"` in the Python source (emit valid JS).

Separately, the gap the operator named — no dynamic loading/failure feedback — is addressed by the shared `duckAsyncSection` helper (defined in `portal_shell.py` as `PORTAL_ASYNC_CSS` + `PORTAL_ASYNC_JS`): real spinner, hard 12s timeout, failure card with the actual error + concrete remediation (viewer-down → `launchctl kickstart` command) + Retry button, empty-vs-failed distinction, plus `?asyncdelay=N` / `?asyncfail=1` affordances so both states are visible on demand. Auto-appended by `_render_portal_shell` (Training, Workflows-status, and all server-rendered `*_intel_page.py` pages get it free); spliced into the `<head>` of the 10 standalone `PORTAL_*_HTML` constants by `viewer._inject_async_helper` (must be in `<head>` — define-before-use — or Library's immediate launcher call ReferenceErrors). Library is the reference wrap. Same "alive ≠ progress / make the failure visible" lesson as [[two_card_observability_bracket]].

| Slice | Happy | Edge / guard | Failure / timeout | Wiring |
|---|---|---|---|---|
| **helper defined** (`PORTAL_ASYNC_CSS`/`JS`) | ✅ `test_portal_async_helper.py::test_async_css_has_spinner_and_fail_markers`, `::test_async_js_exposes_public_api_and_defaults` (spinner, fail-card, retry, 12000 default) | n/a | n/a | n/a |
| **pure logic** (node-gated, skips if node absent) | ✅ `::test_with_timeout_resolves_when_fast`, `::test_format_error_uses_caller_remediation_for_generic_error` | ✅ `::test_format_error_network_remediation_mentions_viewer` (Failed to fetch ⇒ "reach the portal server" + `127.0.0.1:8765`) | ✅ `::test_with_timeout_rejects_when_slow` (isTimeout=true), `::test_format_error_timeout` ("too long") | n/a |
| **splice into standalone pages** (`_inject_async_helper`) | ✅ `::test_standalone_page_has_helper_css_and_js` (parametrized × 10 pages: CSS + JS + exactly one defn) | ✅ `::test_inject_is_idempotent`, `::test_inject_noop_without_anchors` | n/a | ✅ `::test_shell_built_pages_inherit_helper` (shell + intel pages get it free) |
| **Library reference wrap** | ✅ `::test_library_load_runs_through_helper` (`duckAsyncSection({container:"libraryGroups", load: loadAssets})`; old silent `Could not load Library` launcher removed) | n/a | manual: live viewer @:8765 served the helper after bounce (def+CSS+wire all present, HTTP 200) | ✅ same test |
| **define-before-call ordering** (regression) | ✅ `::test_library_launcher_runs_after_helper_is_defined` | ✅ `::test_helper_defined_in_head_before_any_body_call` (parametrized × 10) | n/a | n/a |
| **test affordances** (`?asyncfail=1`, `?asyncdelay=N`) | ✅ `::test_no_test_params_loads_normally` | n/a | ✅ `::test_asyncfail_param_forces_failure_card` (forces the fail card so the operator can SEE it on a fast machine) | n/a |
| **emitted-JS validity** (THE root-cause regression) | ✅ `::test_page_inline_scripts_are_valid_js` (node `--check` every `<script>` of all 10 standalone pages; would have caught both the Library `lastIndexOf` and Decisions `join` syntax errors) | n/a | n/a | manual: headless Chromium (Playwright) loaded `/portal/library` + `/portal/decisions` post-fix → 0 page errors, 50 records / 28 groups rendered, spinner shows under `?asyncdelay`, fail card under `?asyncfail` |

**Ordering bug (operator-caught, 2026-06-21):** first cut spliced the helper `<script>` before `</body>` — i.e. AFTER each page's own inline script. Library's launcher calls `duckAsyncSection()` immediately at the end of its inline script, so it ran **before the helper was defined** → `ReferenceError` → nothing loaded (operator: "the library page isn't wrapping… I don't see anything when it's trying to load"). Fix: `_inject_async_helper` now splices the helper JS into `<head>` (before `</head>`), guaranteeing define-before-use; locked by the regression tests above. The "I don't see the spinner" half was a fast local load (spinner flashes <1 frame) — addressed with `?asyncdelay=N` (hold the spinner N ms) and `?asyncfail=1` (force the fail card) test params so both states are visible on demand without breaking the backend.

**Wired pages (all verified in headless Chromium — normal load = 0 page errors + content rendered; `?asyncfail=1` = fail card + Retry):** Library (`libraryGroups`), Agent OS (`osAreas`), Decisions (`decisionQueue`), Desk (`deskMetrics`), Workflows (`businessWorkflows`), Workflows-status (`wfList`), and Reviews (`portalReviewWorkspace`, `showSpinner:false`). Each load function throws on failure (internal swallow removed or a thin throwing loader) so the helper owns the spinner/timeout/fail-card; page-specific side-effects (e.g. Reviews disabling approve/post buttons, Desk focus-panel recovery text) move to `onError`. Container per page is one the render fully rebuilds, else the spinner would wipe the render's targets. Test: `test_page_initial_load_wired_through_helper` (parametrized, pins each page→container). **Container gotcha (caught only by loading each page):** the Workflows wiring first targeted `wfList`, which exists on `/portal/workflows-status`, NOT `/portal/workflows` (that page uses `businessWorkflows`) — reinforces [[verify_external_behavior_before_root_cause]].

**Deferred:** Studio×3 + legacy `HTML` (multiple interleaved loaders — build queue, assets, templates, job polling — no single container; opportunistic per-loader migration later). They still get the helper CSS/JS available. Server-rendered intel pages need no wrap (no client data load). **Tier:** read-only UI; viewer bounce to load the new code is a local service restart.

---

## Surface 29 — Weekly sale-POSTS toggle: keep the sale, drop the posts (2026-06-22)

**Background:** operator: "I don't do posts for sales. I just want to do the sales… can we make this toggleable on our workflow page? Just the part with the sales post?" The weekly flow auto-applies a Shopify sale AND generates promo posts (blog/social/SEO) + an approval email that parks a recurring "pick the final weekly sale post" decision. The existing whole-flow `mode:off` switch was too blunt — `step_weekly_sale_playbook` (the sale auto-apply) is bundled IN the scheduled flow (steps.py:3171 `auto_apply_allowed` branch), so turning the flow off would also stop the operator's sales. Needed a finer toggle. Lives in duckAgent.

| Slice | Happy | Guard / edge | Safety invariant | Surface |
|---|---|---|---|---|
| **gate reader** (`weekly_sale_posts_enabled`) | ✅ `tests/test_weekly_sale_posts_toggle.py::test_posts_enabled_by_default`, `::test_posts_enabled_when_on` | ✅ `::test_posts_disabled_values` (off/false/0/disabled/no), `::test_reader_fails_open_on_missing_config` (fail-open = post) | n/a | `flows/weekly/steps.py` |
| **step gate** (`_gate_weekly_post_step`) | ✅ `::test_gate_runs_underlying_when_on` | ✅ `::test_gate_skips_underlying_when_off` (post step NOT executed) | ✅ **`::test_sale_and_prepare_steps_are_never_gated`** (identity check: `weekly_sale_playbook`/`weekly_prepare` are the raw fns, never wrapped) + `::test_campaign_post_steps_are_gated` | flow list |
| **portal writer** (`set_weekly_sale_posts`, Tier-3) | ✅ `creative_agent/runtime/tests/test_weekly_sale_posts_card.py::test_set_off_keeps_sale_mode_untouched` (mode stays `auto_apply_shopify`), `::test_set_on_round_trips` | ✅ `::test_requires_confirm`, `::test_requires_reason`, `::test_get_defaults_on_when_key_absent` | ✅ same (mode untouched) | `workflows_card.py` |
| **portal toggle UI + routes** | manual: headless Chromium — weekly row shows "Turn sale posts off/back on" next to "Turn off"; GET `/api/workflows/weekly/sale-posts` `{enabled}`; POST round-trips on↔off with `mode` unchanged; no-confirm → 400 | n/a | n/a | `viewer.py` GET+POST + `/portal/workflows-status` JS |

**Design + a fixed regression:** the sale (`mode`/auto-apply) and the promo POSTS are now independent controls — flipping posts off keeps the discount running and stops the recurring decision. `weekly_prepare` + `weekly_sale_playbook` are never wrapped; everything from `weekly_blog` onward is gated. ALSO fixed a latent bug found while wiring: the workflows-status confirm dialog called a dangling `load()` (removed when the page was wrapped for async-state in Surface 27) — the existing **mode** toggle would have posted then errored on refresh; re-added `load()`. Operator preference memorialized: [[operator_runs_sales_not_sale_posts]]. **Tier:** the toggle write is Tier-3 (operator confirm + reason); it changes a ranking/posting preference, never the sale.

---

## Surface 30 — Two systemic regression guards (2026-06-22, operator: "are we missing tests?")

**Background:** after several regressions surfaced in one session, the operator asked if coverage was thin. It wasn't *volume* (1,392 tests) — it was two specific SHAPES of test missing, and nearly every regression fell into one:

| Guard | Catches | Found / would-have-caught | Where |
|---|---|---|---|
| **F821 undefined-name lint** (ruff `--select F821`) | NameError class — a name used in a branch (publish success path, except/fallback) that no test executes | the meme `successful_platforms` crash (false "publish failed"); **8 more latent bugs on arrival** incl. an outright SyntaxError in `competitor_image_analyzer.py`, missing `re`/`Path` imports, and the duck-ops `_load_json` ROI-triage fallback typo | CI (both repos) before tests + `duckAgent/scripts/check_undefined_names.sh` |
| **Portal headless smoke** (`test_portal_smoke.py` + `portal_smoke_driver.mjs`) | runtime render crashes — a render fn that throws on real data, a wrong container leaving the page blank, a dangling ref hit at load | the dead Library/Decisions pages, the wfList-vs-businessWorkflows mixup, the dangling `load()` | local (loads all 17 portal pages in headless Chromium, asserts 0 pageerror + real content); **skips in CI** (no viewer/browser) where node-check + F821 cover it |

These join the existing `test_page_inline_scripts_are_valid_js` (node `--check` every page's inline JS — the syntax-error layer). Together: **syntax (node-check) + undefined-names (F821) + runtime-render (browser smoke)** — the three classes that bit this session. The F821 gate runs first/fast in CI; the smoke is the dev-machine belt that no-ops in CI. Lesson: the suite was strong at the unit level but blind at the execution/integration boundary — see [[or_fallback_branches_hide_bitrot]] (the failing branch never runs in a test) and [[verify_external_behavior_before_root_cause]] (load it in a browser).

---

## Surface 31 — Daily snapshot clobbered the weekly competitor analysis (2026-06-22, operator: "how is the report doing?")

**Background:** operator asked how the competitor report + the new my-shop row were doing. Found the report had read **empty analysis for ~10 days**. Root cause: two crons write the SAME `{date}_competitor_report.json` — the **weekly analysis** (Sun 06:30, `--from collect_data`, full LLM analysis incl. trending/recommendations/my-shop row) and the **daily snapshot** (07:00, `--only competitor_snapshot`, empty `_snapshot_only`). The daily ran 30 min AFTER the weekly and overwrote it. Confirmed on 06-21: the 06:30 run logged "403 trending, 6 rising, 78 opportunities" but the saved file (mtime 07:00) was snapshot-only. Downstream (portal Competitors page, Build-Next) ran on the last un-clobbered report (June-12). Fix in duckAgent `competitor_engine.save_snapshot_only`: skip the report-file write when a full analysis already exists for that date (`_existing_report_is_full_analysis`); the full report already carries `listing_snapshots` so velocity lookback is unaffected.

| Slice | Happy | Guard | Verify |
|---|---|---|---|
| no-clobber | ✅ `test_competitor_snapshot_split.py::test_snapshot_does_not_clobber_full_analysis` | ✅ `::test_existing_report_is_full_analysis_classifier` (full vs snapshot vs missing) | manual: regen wrote full analysis (50 trending / 7 rising / 20 ducks_to_build / my_shop_velocity `building · 65 actual`); Build-Next rebuilt off 06-22, all top-10 recs TRENDING |
| still writes normally | ✅ `::test_snapshot_writes_when_no_report_exists` (Mon–Sat snapshot) | ✅ `::test_snapshot_refreshes_an_existing_snapshot` | n/a |

**Tier:** read-only (report generation). Same "a 2-minute cron timing collision silently emptied a daily-read surface" family as the producer/reader cadence bugs; the daily snapshot's job is velocity lookback (snapshots dir), not owning the analysis report.

---

## Surface 32 — Review replies kept "failing": pacing self-throttle + stale failed receipts (2026-06-22, operator: "why do we continue to see failures on review replies?")

**Background:** the live queue showed **11 failed** review-reply receipts. Live drain-log probe split them into two unrelated root causes (verify-external-behavior-before-root-cause):

- **7 pacing self-throttle (ages 1–3d).** The soft reservation in `etsy_browser_guard.before_command` defers reads at `14/18` visible commands to keep 4 slots for review-reply POSTS. But a review-reply POST does its OWN setup/verify reads (navigate to row, auth probe, post-submit verify). Once the window hit 14/18 those reads were deferred too, leaving replies **filled-but-not-submitted** — the reservation meant to PROTECT posts was blocking the post's own reads. Fix: thread-local `review_reply_post_window()` context manager exempts reads issued during a post from the SOFT reservation only; the hard 18-ceiling + mutating cap still apply. `review_reply_executor` wraps both `run_live_submit` call sites.
- **4 stale transaction_mismatch (ages 14/14/20/71d).** `auto_dismiss_stale_queued` only cleared `queued` receipts, so a FAILED `review_row_transaction_mismatch` older than the 14d freshness window (Etsy review-row metadata drift → will never post) sat in the queue forever as a permanent false "failure" — the recurring noise the operator saw. Fix: the status filter now includes `"failed"`. Ran against the live queue: dismissed all 4, failed dropped **11 → 7** (the 7 pacing ones stay; the pacing fix lets them post next drain).

| Slice | Happy | Guard | Verify |
|---|---|---|---|
| pacing exemption | ✅ `test_review_reply_pacing_and_stale_dismiss.py::test_read_exempt_inside_review_reply_post_window` | ✅ `::test_read_deferred_at_reservation_threshold_outside_post_window` (still deferred outside the window), `::test_hard_ceiling_still_applies_inside_post_window` (exemption is soft-only) | live drain-log: `Etsy read command deferred: 14/18 … reserving the last 4 for review-reply posts` → CleanupFailure |
| stale failed dismiss | ✅ `::test_dismisses_stale_failed_receipt`, `::test_dismisses_stale_queued_receipt` | ✅ `::test_keeps_fresh_failed_receipt` (recovery sweep owns fresh failures), `::test_ignores_terminal_statuses` (posted/dismissed/skipped never re-dismissed) | live `auto_dismiss_stale_queued` run dismissed 4, failed 11→7 |

**Tier:** the guard/executor edits are local code (Tier 2). Running `auto_dismiss_stale_queued` against the live queue mutated production review-reply state (4 dismissals, each with a workflow_control transition receipt) — done via the real function, not a hand-edit, so every dismissal is greppable. Pairs with the two-card observability bracket already on this lane ([[feedback_two_card_observability_bracket]]); these were "alive but stuck" failures the throughput card eventually catches ([[feedback_alive_status_is_not_progress]]).

---

## Surface 33 — Occasion-nod card false RED across boundary + Thursday snapshot-only starvation (2026-06-22, operator: "feels like a non-robust design" / "investigate Thursday root cause")

Two RED OS cards diagnosed by parallel read-only agents; both were detection/source bugs, not failures of the working flow. (duckAgent commit `3e2d262`.)

**33a — occasion_nod_coverage false positive.** The card compared the newest run in a 10-day window against the occasion active *now*. Jeepfact is weekly, the window is 10 days, so at every occasion rollover the card grades a pre-rollover post against the post-rollover occasion. Concretely: a 06-17 jeepfact that correctly nodded to **Father's Day** was flagged RED once the occasion rolled to **Independence Day**. Structural, recurs every transition. Fix (correct-by-construction): each surface stamps `occasion_at_runtime` (the occasions active when it ran) into its run state at post time (`meme_helper`, `jeepfact_helper` via new `occasion_context.active_occasion_stamp`); the card grades each run against its OWN stamp, never against now. A run with no stamp predates the fix and is **skipped** (yellow "grading resumes on next run"), never graded vs now. Live card went from false RED → honest yellow.

**33b — Thursday funnel snapshot-only starvation (the 06-18 RED).** `flows/thursday/steps.py::_load_latest_competitor_report` took newest-by-mtime and accepted a daily `_snapshot_only` stub (empty trending). On 06-18 the competitor engine was mid snapshot-only window (full analysis ran 06-12, recovered 06-22), so Thursday's only working candidate source (tier-2 competitor_trending) read 0, fell through to own-catalog bestsellers, and all 5 were correctly rejected (already-made / IP-risk) → fallback-only RED. The qualifier was fine; the **source was structurally empty**. Fix: walk newest-first, skip `_snapshot_only`/empty-trending, return the latest report with real `trending_products`. Cousin of Surface 31's clobber bug (same snapshot-vs-full confusion, different consumer). Tier-1 source (`etsy_intelligence.trending_products`) is separately dead-every-week — noted for follow-up, not fixed here.

| Slice | Happy | Guard | Verify |
|---|---|---|---|
| nod graded vs run-time occasion | ✅ `test_occasion_nod_coverage.py::TestCrossOccasionBoundary::test_post_graded_against_runtime_occasion_not_active_now` | ✅ `::test_post_that_missed_its_runtime_occasion_is_still_red` (stamp doesn't whitewash a real miss), `::test_legacy_unstamped_run_is_yellow_not_red` (never grade vs now), `::test_stamped_empty_no_occasion_when_ran_is_green` | live card: false RED → yellow "predate stamping" |
| stamp helpers | ✅ `::TestOccasionStampHelpers::test_active_occasion_stamp_carries_matcher_fields` | ✅ `::test_read_surface_occasion_stamp_none_for_legacy` (None≠[]), `::test_active_occasion_stamp_empty_when_no_occasion` | n/a |
| Thursday skips snapshot-only | ✅ `test_thursday_competitor_report_loader.py::test_skips_snapshot_only_returns_full` | ✅ `::test_prefers_newest_full_when_several`, `::test_returns_empty_when_only_snapshots`, `::test_full_report_with_empty_trending_is_skipped` | 06-18 run proven: tier-2 read 0, fell to top_performer_7d, 0 real qualified |

**Tier:** all local code (Tier 2). Existing 12 card tests updated to the new stamped-run contract (fixtures now write `occasion_at_runtime`); 215 related duckAgent tests pass. Same family as [[feedback_plausible_fallbacks_mask_failure]] (a green producer beside a red consumer) and the producer/reader cadence bugs. The two open follow-ups it named are now resolved in Surface 34.

---

## Surface 34 — Thursday sourcing robustness + email-approval re-arm (2026-06-23, operator: "root cause the Thursday sources" / "we refactored email approval multiple times")

Two deep root-causes from parallel read-only agents, then both fixed.

**34a — Thursday ran on ONE real source.** The 3-tier candidate cascade was structurally one source: **tier-1** (`weekly_insights.etsy_intelligence.trending_products`) is a **dead producer key** — `etsy_jeep_duck_weekly_tracker.analyze_competitive_landscape` initializes `analysis['trending_products']=[]` and never reassigns it (the real signal lands in `search_term_insights[*]['trending_ducks']`, a different key); 0 every week across `weekly_insights_history.json`. The 06-12 "revive Etsy trend collector" commit fixed the 403 auth but never repopulated this key, so the operator believed tier-1 was alive — it has produced 0 candidates ever. **Tier-3** (own-catalog `top_performer_7d`) is a guaranteed-reject trap (gate 15 `existing_product_match` rejects 100%), silently degrading to the static fallback pool. So one competitor-engine hiccup starved Thursday (06-18, 06-04). Fix (duckAgent `607b03f`): add the **Build-Next fused queue** (`state/build_next_queue.json`) as the PREFERRED tier-0 — independent (fuses trend+competitor+occasion+profit), catalog-gap filtered, dupe-flagged (so its rows survive gate 15); skip `possible_dupe`. Make tier-3 **fail loud**: stamp `thursday_sources_exhausted` + per-tier `source_tier_counts` instead of emitting guaranteed-rejects. Tier-1's dead key left as a lower-leverage follow-up (it overlaps tier-2; Build-Next adds the independence).

**34b — email approval: handler hardened 5×, reader never scheduled.** Reconciled the operator's "we refactored this multiple times" with the prior "built but never scheduled" finding: BOTH true. The dispatch *handler* (`main_agent.py:2006 handle_mail_event`) was hardened across 5+ commits (decision-gateway routing, flow-registry migration, durable handoff) and is robust. But the *reader* — `operator_inbox_poller.py` (one commit, 05-25, zero runs since) — was never on any schedule, so email "approve"/"publish" replies AND the Pulse glasses approve-button (which round-trips through this same inbox) have been dead for ~4 weeks. Operator chose **re-arm**: wired the poller into `openclaw_runtime/run_duck_ops_sidecar.sh` as a fail-soft step right after the observer (Tier-3, local runtime file — not git). Pre-arm safety: dry-run confirmed 0 pending approval-marker replies in the inbox (only a digest + a carousel notice, both skipped), so no surprise publishes on first run; poller is UNSEEN-only + marks Seen only on successful dispatch + dedups via the decision gateway.

| Slice | Happy | Guard | Verify |
|---|---|---|---|
| Build-Next as Thursday source | ✅ `test_thursday_build_next_source.py::test_loads_queue_and_maps_fields` | ✅ `::test_skips_possible_dupe_rows`, `::test_missing_file_returns_empty`, `::test_malformed_file_returns_empty`, `::test_path_override_via_env` | live: 4 clean candidates (Nurse/Finger/Artist/Custom-Jeeper; Dachshund possible_dupe skipped) |
| Thursday source isolation in tests | ✅ existing full-step test stubs `_load_build_next_candidates` → [] | ✅ avoids reading live duck-ops queue (test-pollution guard, [[feedback_test_isolation_from_production_state]]) | 69 thursday tests pass |
| email poller armed | ✅ wired into sidecar after observer (fail-soft) | ✅ UNSEEN-only + Seen-on-success + gateway dedup (idempotent); sidecar `zsh -n` clean | dry-run: 0 actionable replies pending |

**Tier:** Thursday is Tier-2 code (`607b03f`, pushed). Arming the poller is Tier-3 (re-enables an auto-publish lane) — done in the local sidecar per operator approval; will run on the next sidecar cycle. Email-handler robustness memory: [[project_email_approval_unscheduled]] (now re-armed).

---

## Surface 35 — Verified review reply false-failed by a post-submit screenshot pacing trip (2026-06-24, operator: "reply publish failure today — I thought we made this robust")

**The same class as Surface 32, through the branch that fix did not touch.** Surface 32 exempted the etsy_browser_guard **soft** reservation from a review-reply post's own reads; it deliberately left the **hard** ceiling applying (the Surface-32 test is literally named `test_hard_ceiling_still_applies_inside_post_window`). Today the hard ceiling fired — but on the post's OWN tail-end read. Timeline for Heather's review (`tx-5096110510`, 10:40–10:42): staged → `submit_confirmed` → `inspect_reply_row_state` **confirmed the reply live** (`rowTextContainsReplySnippet=True`, excerpt "Philip responded on…") → the post-submit **screenshot** read (`review_reply_executor.py:2994`) tripped the hard ceiling and raised → generic `except` recorded `failed` + emailed a false "publish failure." **The reply was live on Etsy the whole time** — a verified post downgraded to failed by cosmetic evidence-gathering, corrupting truth (a blind retry would duplicate).

Fix: (1) `inspect_reply_row_state` is the authoritative success gate — once it confirms, NOTHING downgrades the post; the screenshot is wrapped best-effort (`screenshot_error` recorded, never fatal). (2) `classify_attempt_failure` now classifies the shared-pacing-budget string as `pacing_cooldown`/retryable instead of `unexpected_executor_failure`. Did NOT exempt the hard ceiling (a *pre*-submit ceiling hit legitimately means "didn't post" — exempting would weaken real rate-limit protection). Reconciled the one mislabeled item (queue→posted, quality→posted, workflow_control→verified, evidence-backed); read-only scan found the other 7 failed items are genuine (failed at/before submit), and they're safe to re-drive (code fix + existing AlreadyRespondedError detection prevents duplicates).

| Slice | Happy | Guard | Verify |
|---|---|---|---|
| screenshot can't fail a verified post | ✅ `test_review_reply_executor_workflow.py::test_verified_post_is_not_failed_by_screenshot_pacing_error` (inspect confirms, screenshot raises pacing → status stays `posted`, `screenshot_error` recorded, no `execution_failed` transition) | ✅ asserts `reply_posted` present, `execution_failed` absent | live workflow_control `last_verification` confirmed reply live; reconciled to verified |
| pacing classification | ✅ `::test_classify_pacing_cooldown_is_retryable_not_unexpected` | n/a | matched today's exact error string |

**Bug-hunter note + Surface-35 completion (Lens 5):** none of the deterministic duck-bug-hunt lenses would catch this class. We BUILT Lens 5 (adversarial LLM panel: 3 finders → 3-skeptic refute-vote) and pointed it at the Surface-32 fix. It surfaced — surviving the refute-vote **3–0** — a gap the Surface-35 screenshot fix ALSO missed: the **post-submit verification read `inspect_reply_row_state` (`review_reply_executor.py:2998`) runs one command before the screenshot and was itself unprotected from the hard ceiling.** So a cooldown trip on the verification read (not just the screenshot) still false-failed a live reply. Completion fix: the inspect read is now wrapped — a cooldown error AFTER `submit_performed=True` records `posted` with `post_submit_verification_deferred`, never failed (re-driving would duplicate). A genuine "not posted" reading still fails. Regression: `::test_verified_post_not_failed_when_post_submit_inspect_is_paced_out`. This is the headline proof that Lens 5 catches the incomplete-fix class the deterministic lenses can't.

---

## Surface 36 — Non-numeric transaction_id minted a `tx-l` artifact (2026-06-24, found while reconciling Surface 35)

While reconciling the Surface-35 item we found its `transaction_id` stored internally as `"l"` (the queue key + workflow_control filename carried the real `5096110510`, but the record's `transaction_id`/`entity_id` were `tx-l`). Root cause (agent trace): NOT a slicing bug in duck-ops — the slug builder `phase1_observer.py:1831` (`artifact_slug=f"tx-{transaction_id}"`) faithfully interpolates whatever it receives, and the guard at `:1820` only checked **non-empty**, so a stray `"l"` (corrupt upstream of duck-ops; exact mint point not reproducible — live state already reconciled) passed and formed a `tx-l` workflow. The DOM matcher then matched the **wrong-row-by-luck** (listing 4311902199 carried it); on a less distinctive listing this could reply to the wrong review. Fix (fail closed, defense-in-depth at both ends): `phase1_observer.py:1820` rejects a non-`.isdigit()` transaction_id (Etsy ids are all-digit) — the review stays un-replied, visibly surfaced by the feed-freshness card, rather than forming a corrupt workflow; `duckAgent/flows/reviews/steps.py:273` enforces a numeric-or-None contract at the source so a bad value never propagates as a plausible string.

| Slice | Happy | Guard | Verify |
|---|---|---|---|
| producer numeric-or-None | ✅ `test_reviews_reply_handoff.py::test_handoff_pairs_ids_with_reply` | ✅ `::test_handoff_nonnumeric_transaction_id_becomes_none` (`"l"`→None, listing kept) | n/a |
| observer fail-closed | ✅ `test_review_reply_handoff_observer.py::test_handoff_builds_candidate_with_api_exact_target` | ✅ `::test_reply_with_nonnumeric_transaction_id_is_skipped` (no `tx-<garbage>` artifact) | live: all 7 existing review tx ids confirmed numeric |

**Tier:** Tier-2 code both repos. Defense-in-depth — the upstream mint point of `"l"` is unconfirmed (state reconciled away), so the guard converts silent corruption into a visible skip rather than root-causing the origin. Same fail-closed-on-ambiguous-identity discipline as the hard safety rules.

---

## Surface 37 — "Review posts failed AGAIN" was a LYING summary email, not a failure (2026-06-24, operator: "why did our review posts fail again")

The operator got a "Session Summary (1 posted, **13 failed**)" email and read it as a new regression. It wasn't. Investigation (verdict: **stale pre-fix drain + lying alert**): the only drain today ran ~10:42, BEFORE the Surface-35/36 fixes landed (13:48/15:58), and false-failed one item that was already reconciled to verified-live at 13:47. The **executor was fine**; the alarm came from `send_session_summary_email` tallying `_session_counts(session)` over the **session's frozen per-item snapshots** — a long-lived session (since June) whose 13 "failed" were the old backlog + 5 ancient auto-dismissed mismatch items + the already-reconciled item's stale 10:42 snapshot. Classic lying-status ([[feedback_alive_status_is_not_progress]], [[feedback_portal_action_vs_card_refresh]]): the alert read data already superseded by reconciliation.

Fix: `_session_counts(session, queue_state)` + `_live_item_status` reconcile each item's count against the LIVE queue status (reconciled-to-posted/dismissed wins over the snapshot; dismissed folds into skipped, never failed). The summary's per-item lines render the live status and suppress stale error/breadcrumb text for any item no longer failed. Items aged out of the queue fall back to the snapshot (honest limit; the genuine pile is handled by reconciliation below). Also reconciled the real backlog from on-disk evidence: `review-2` → posted (workflow_control `rowTextContainsReplySnippet=true`, reply live), `review-1` → skipped (workflow_control already `resolved`); the other 5 have no live-confirmation, so stay failed (a browser read-back is Tier-3, not ad-hoc). Genuine failed count: 7 → 5.

| Slice | Happy | Guard | Verify |
|---|---|---|---|
| summary counts reflect live state | ✅ `test_review_reply_executor_workflow.py::test_session_summary_counts_reconcile_against_live_queue` (reconciled-in-queue item counted posted, dismissed→skipped) | ✅ same test: item aged out of queue falls back to snapshot; no-queue path = stale snapshot count | live: 2 mislabeled backlog items corrected, failed 7→5 |
| per-drain session rotation | ✅ `::test_close_open_session_rotates_so_sessions_dont_accumulate` (next session fresh + empty) | ✅ same test: closing with nothing open is a no-op | live: drain end now closes the session even with 0 posts |

**Tier:** Tier-2 code + Tier-3 state reconcile (2 queue items flipped from on-disk verification evidence, no Etsy calls).

**Follow-ups — all three closed (2026-06-24, operator: "follow up on those now"):**
1. **Session never rotated → FIXED.** Root cause was sharper than "never rotates": the session only closed itself when a summary email fired (`drain: posted_count>0`), so drains that posted nothing accumulated failures across days into one heap that the next successful post emailed as "N failed". `close_open_session` now rotates the session at the END of every drain regardless (`status=closed_no_post`), so the next session is fresh and each summary reflects one run, not a months-long pile. (`review_reply_executor.run_drain` + helper.)
2. **5 stuck items → RE-QUEUED.** Re-queued `tx-5100946874/-5102330927/-5102347203/-5107385834/-5112822129` to `queued` for the next SCHEDULED drain's read-back (a browser window is Tier-3, never ad-hoc). Safe to re-drive: the Surface-35 pacing fixes + the existing AlreadyRespondedError detection mean an already-live one resolves to skipped (no duplicate), and a genuine one posts under the new protection. Drain cap (2/run) paces them over a few days. Queue failed count: now **0**.
3. **`tx-id="l"` origin → CONCLUDED unpinnable, guard is complete.** `transaction_id` flows verbatim from the Etsy API `review` object (`duckAgent/flows/reviews/etsy_review_helper.py:570`) — no slice/default/single-char transform anywhere in our extraction. The `"l"` originated in the API response or injected upstream data we cannot reproduce. The Surface-36 fail-closed guard is the correct and complete resolution; there is no extraction bug to fix.

---

## Surface 38 — Google Search Console first-party search demand → Build-Next (2026-06-26, matrix before code; operator: "build tier-1 #2 / understand clicks & search terms")

**Goal.** Feed Build-Next a FIRST-PARTY demand signal — what real shoppers search on Google to reach myjeepduck.com — instead of relying only on competitor view-diffs. After verifying Shopify exposes NO search-term API (ShopifyQL = sales/sessions/customers only, [[reference_etsy_transactions_ignores_date_window]] family: confirm the API surface before designing), GSC is the clean-API path: real queries + clicks/impressions/CTR/position, reusing the existing Google OAuth stack (`google_tasks_bridge.py` refresh→access pattern). **Single repo (duck-ops):** producer + Build-Next factor + state + OS cards all live here.

**Design.** Producer `runtime/gsc_search_demand.py` queries `searchconsole.googleapis.com/.../searchAnalytics/query` (dimensions=["query"]), aggregates into `state/gsc_search_demand.json` = top_queries + **gap_queries** (meaningful impressions, NO catalog token overlap = unmet demand) + a normalized `term_scores` map. Build-Next gains a 5th SOFT factor `search_demand` (NEUTRAL_SEARCH=0.6, mirrors occasion_fit) that boosts candidates whose tokens hit hot search terms; absent GSC → uniform neutral → ranking unchanged (no-op until live). **Operational prereq (Tier-3, operator one-time):** verify GSC property for the domain + mint a `webmasters.readonly` refresh token via the bootstrap helper (existing Tasks token lacks the scope). Code + all tests land now with the API mocked; live data flows after the one-time auth.

### 38.1 Producer (gsc_search_demand.py) — API mocked in all tests

| Path | Happy | Missing/!auth degrades (fail-soft) | Empty → not invented | Isolation |
|---|---|---|---|---|
| `_gsc_config(env)` | ✅ reads CLIENT_ID/SECRET/GSC_REFRESH_TOKEN/GSC_SITE_URL | ✅ missing creds → `credentials_ready=False`, no raise | n/a | n/a |
| `fetch_gsc_access_token` | ✅ mocked 200 → token | ✅ mocked 4xx/network → (None, error dict), no raise | n/a | n/a |
| `query_search_analytics` | ✅ mocked rows parsed (query/clicks/impressions/ctr/position) | ✅ mocked error → [] | ✅ no rows → [] | n/a |
| `aggregate_search_demand` | ✅ top_queries sorted; term_scores max-normalized 0..1 | n/a | ✅ empty rows → empty maps | n/a |
| gap detection | ✅ high-impression query w/ no catalog token overlap → gap_queries | ✅ all-covered → gap_queries=[] | ✅ no catalog → all flagged gap (documented) | n/a |
| `main(--dry-run)` | ✅ prints summary, no write | ✅ no creds → `available:false` payload, exit 0 (never crash schedule) | n/a | n/a |
| multi-window (7/28/90) | ✅ `_trend` rising/steady/fading/new/flat; build_payload enriches each query with `impressions_by_window` + `trend`; primary=28 drives term_scores | ✅ secondary-window query failure omitted (counts 0), only primary failure degrades | n/a | n/a |
| Write | ✅ atomic tmp+replace | n/a | ✅ available:false still written | ✅ conftest redirect + `DUCK_TEST_MODE` FROZEN-path guard + pollution audit test (convention #4) |

### 38.2 Build-Next factor (build_next_engine.score_search_demand)

| Use case | Happy | Neutral degrade | Ranking safety |
|---|---|---|---|
| token hit on hot term | ✅ overlap→ up to 1.0, reason names matched term | n/a | n/a |
| no GSC data / no match | n/a | ✅ NEUTRAL_SEARCH (0.6), reason "no first-party search signal" | ✅ uniform neutral → order unchanged vs pre-GSC |
| score fold | ✅ 5th multiplicand: demand×margin×gap×occasion×search | ✅ absent input defaults neutral, build never crashes | ✅ existing Surface-16/16.4 tests still green |

### 38.3 Observability (two-card bracket, convention #3) — DEFERRED to activation

| Card | Catches | Status |
|---|---|---|
| feed-freshness (input) | producer stale / never ran / available:false | ⏸️ deferred-to-activation `_load_gsc_search_demand_health` + registration + empty-payload test |
| gap-throughput (output) | data present but 0 gap_queries surfaced for N weeks (signal died) | ⏸️ deferred-to-activation same loader, second card |

**Why deferred (explicit decision, not a silent gap):** the producer is fail-soft `available:false` until the operator completes the one-time OAuth + launchd install (both Tier-3). A bracket watching a deliberately-dormant feed would sit permanently yellow and train alarm-blindness. The cards get built AT activation, in the same change that schedules the producer — so the bracket goes in with a LIVE lane, exactly as convention #3 intends. Until then the feed is self-announcing (`available:false` + `error` string in the payload).

### 38.4 Live verification (post-auth, operator-gated)

| Step | Expectation |
|---|---|
| one-time bootstrap | mints `webmasters.readonly` refresh token; operator stores `GSC_REFRESH_TOKEN`/`GSC_SITE_URL` in `.env` (per [[feedback_env_in_dotenv.md]]) |
| first real run | top_queries non-empty; gap_queries reviewed for plausibility; Build-Next demand reasons start naming search terms |

**Tier:** Tier-2 code (producer + factor + tests, API mocked). The one-time OAuth + `.env` token + launchd install are **Tier-3, operator-run** (not done by me).

---

## Surface 39 — GA4 listing performance → Fix-or-Promote lane (2026-06-26, matrix before code; operator: "what do experts use this data for?")

**Goal.** Most of GA4's value is NOT "what to make" (Build-Next) but "what to FIX or PROMOTE on what we already sell" — a different, higher-ROI decision (monetize traffic we already earned). GA4 already receives per-listing behavior (the operator's GA4 emails show Etsy listing titles with views / active users / bounce). Pull it via the GA4 Data API (`runReport`, scope analytics.readonly), classify each listing, and surface a Fix-or-Promote decision lane; feed only a thin "make more like winners" slice to Build-Next.

**Design.** Producer `runtime/ga4_listing_performance.py` queries `analyticsdata.googleapis.com/v1beta/properties/{id}:runReport` (dimension pageTitle; metrics screenPageViews/activeUsers/newUsers/engagementRate/bounceRate/averageEngagementTimePerActiveUser), writes `state/listing_performance.json`. Classifier verdict per listing, RELATIVE to the catalog (terciles, with a min-views floor so low-traffic noise isn't judged):
- **FIX** = high views + low engagement (traffic that isn't converting → photos/price/copy).
- **PROMOTE** = high views + high engagement (proven winner → feature/advertise/variations; this slice loops to Build-Next).
- **WATCH** = high engagement + low views (good page, needs traffic → SEO/ads).
- **neutral** otherwise. Reuses the same Google OAuth refresh→access pattern as gsc_search_demand.py (scope differs: analytics.readonly; one token can carry both scopes). Fail-soft available:false; three-layer write isolation.

### 39.1 Producer (ga4_listing_performance.py) — API mocked in all tests

| Path | Happy | Fail-soft degrade | Empty → not invented | Isolation |
|---|---|---|---|---|
| `ga4_config(env)` | ✅ reads CLIENT_ID/SECRET/GA4_REFRESH_TOKEN/GA4_PROPERTY_ID | ✅ missing → credentials_ready=False | n/a | n/a |
| `fetch_ga4_access_token` | ✅ mocked token | ✅ mocked 4xx/network → (None, error) | n/a | n/a |
| `run_report` | ✅ mocked rows parsed (pageTitle + **hostName** + metrics) | ✅ mocked error → ([], error) | ✅ no rows → [] | n/a |
| `_channel_for` / channel split | ✅ host → etsy / shopify / web; `channels` per-domain totals (Etsy traffic lands in same GA4 property via Etsy's web-analytics tag) | n/a | n/a | n/a |
| `classify_listings` | ✅ FIX/PROMOTE/WATCH terciles **per channel** (Etsy vs Shopify baselines not comparable) + min-views floor | n/a | ✅ empty → [] | n/a |
| multi-window (7/28/90) | ✅ `_trend` per listing from short-vs-long view rate; build_payload enriches each listing with `views_by_window` + `trend`; primary=28 drives the classification | ✅ secondary-window failure omitted; only primary degrades | n/a | n/a |
| totals | ✅ top-line active/new/views from a totals row | ✅ absent totals → zeros | n/a | n/a |
| `collect` | ✅ live payload (listings + fix/promote/watch subsets) | ✅ not-ready/token/query fail → available:false + error, never raise | n/a | n/a |
| Write | ✅ atomic | n/a | ✅ available:false still written | ✅ conftest redirect + DUCK_TEST_MODE FROZEN guard + pollution audit test |

### 39.2 Classifier verdicts (relative, deterministic)

| Listing shape | Verdict | Reason names the signal |
|---|---|---|
| high views, low engagement | FIX | ✅ "high traffic, low engagement — conversion leak" |
| high views, high engagement | PROMOTE | ✅ "proven winner — promote / make variations" |
| low views, high engagement | WATCH | ✅ "engages well, starved of traffic — SEO/ads" |
| below min-views floor | neutral | ✅ "not enough traffic to judge" |

### 39.3 Consumers — DEFERRED to activation (needs live GA4 data)

| Consumer | Status |
|---|---|
| `/portal/intel/listing-performance` page + Business Desk tile (duckAgent viewer) | ⏸️ deferred-to-activation (replaces the GA4 email per email-to-portal inversion) |
| Build-Next "make more like winners" slice (PROMOTE titles → demand boost) | ⏸️ deferred-to-activation |
| two OS bracket cards (feed freshness + verdicts-produced) | ⏸️ deferred-to-activation (dormant feed until OAuth, same reason as Surface 38.3) |

### 39.4 Live verification (post-auth, operator-gated)

| Step | Expectation |
|---|---|
| one-time bootstrap | mint analytics.readonly refresh token (helper --scope); set GA4_REFRESH_TOKEN + GA4_PROPERTY_ID in .env |
| first real run | listings non-empty; FIX/PROMOTE lists match the operator's gut on known listings |

**Tier:** Tier-2 code (producer + classifier + tests, API mocked). The OAuth + .env token + GA4 property id + launchd install + portal page are Tier-3 / cross-repo, layered at activation.

---

## Surface 40 — SEO generation fed by first-party demand (2026-06-27, matrix before code; operator: "best use of this data outside direct actions → do #1")

**Goal.** The Shopify SEO generator (`shopify_seo_review._generate_proposals`) writes titles/descriptions from the product title + issue codes alone — it GUESSES keywords. Feed it the real GSC queries shoppers use for each product + the product's GA4 verdict, so generated copy is built from demand, not guesses. Autonomous (improves the generated DRAFT); the existing email-apply gate stays the human checkpoint — data never auto-publishes (Tier-3 boundary preserved). L1 of the SEO maturity model.

**Design.** New `runtime/seo_demand_context.py` reads `state/gsc_search_demand.json` + `state/listing_performance.json` (fail-soft: missing/`available:false` → empty context → generator behaves exactly as today). Provides: `relevant_queries(title, limit)` (GSC queries whose tokens overlap the product, reusing build_next `_tokens`), `listing_signal(title)` (GA4 verdict/engagement/trend by normalized-title match), and global `top_search_terms`. `_generate_proposals` enriches each `prompt_resource` with `high_intent_searches` + `engagement`, adds a global `store_top_searches` block + rules ("work the listed real queries in naturally; a FIX listing's current title isn't converting — make it clearer"). LLM-output surface → convention #5: golden eval `scripts/eval_seo_search_demand.py` (real API, gated) with a deterministic cross-check (did the title include ≥1 relevant high-intent term when one was offered?). Read-only consumer — no new prod-write path, so no isolation guard needed; tests redirect the read paths to tmp.

### 40.1 Demand context reader (seo_demand_context.py) — read-only, fail-soft

| Path | Happy | Missing/unavailable degrades | Empty → not invented |
|---|---|---|---|
| `relevant_queries(title)` | ✅ returns GSC queries overlapping the product tokens, by impressions | ✅ no file / available:false → [] | ✅ no overlap → [] |
| `listing_signal(title)` | ✅ GA4 verdict+engagement+trend for the matched listing | ✅ no file → None | ✅ no match → None |
| `top_search_terms` | ✅ global high term_scores | ✅ no file → [] | n/a |
| `load_seo_demand_context()` | ✅ assembles all three | ✅ both files absent → empty context, never raises | n/a |

### 40.2 Generator wiring (_generate_proposals)

| Use case | Happy | No-demand fallback (current behavior preserved) |
|---|---|---|
| prompt enrichment | ✅ per-resource high_intent_searches + engagement + global store_top_searches injected | ✅ empty context → prompt identical in spirit to today; existing SEO tests still pass |
| relevance match | ✅ "Jeep Wrangler Duck" → wrangler queries attached | ✅ unmatched product → no searches, generator proceeds |
| FIX listing nudge | ✅ verdict=fix adds "current title isn't converting" rule | ✅ no verdict → no nudge |

### 40.3 Golden eval (convention #5, gated, real API — NOT in default pytest)

| Check | Criterion |
|---|---|
| demand incorporated | ✅ `scripts/eval_seo_search_demand.py` + `tests/fixtures/seo_search_demand_golden.json`: ≥ gate (0.8) of offered resources produce a title sharing a token with an offered query. **Live: 5/5 = 100% on 2026-06-27.** |
| deterministic cross-check | ✅ keyword-presence detector flags a proposal that ignored every offered high-intent term (confidence is weak — [[llm-stated-confidence-is-weak]]) |
| no stuffing / length | ✅ titles stay 45–70 chars, plain text (existing `_trim_to_range` contract) |

### 40.4 Hardening (failure-mode review — duck-bug-hunt Lens 2 + 3 clean)

| Failure mode | Guard | Test |
|---|---|---|
| viral-phrase fragments pollute global terms (`help`/`accidentally`/`built`) | `_SEARCH_MODIFIER_STOPWORDS` filters `top_search_terms` | ✅ `test_modifier_words_filtered_from_top_terms` |
| stalled producer keeps driving titles off weeks-old data | `_is_stale` (>21d) → fail-soft empty; lenient on unparseable | ✅ `test_stale_demand_is_ignored` |
| lone shared token equates two different products | `listing_signal` needs 2+ shared tokens unless full coverage | ✅ `test_single_shared_token_across_long_titles_not_matched` |
| an enrichment lookup error breaks the whole SEO run | per-resource `try/except` → degrade to un-enriched, core generation unaffected | covered by empty-context + wiring tests |

**Governance:** Tier-2 code (read-only consumer + prompt change + tests, LLM mocked). Publishing stays behind the existing email-apply gate (Tier-3). Per [[feedback_plausible_fallbacks_mask_failure]] the empty-context path is tested so a broken reader degrades to today's behavior, not silent garbage.

---

## Surface 41 — Gap query → draft Shopify collection (SEO path L3) (2026-06-27, matrix before code; operator: "create a collection with the things you found")

**Goal.** Turn unmet search demand into **collections** for products we already sell but haven't grouped. The defining scope rule: a gap query matching **0** catalog products → Build-Next (make it); **1** → listing SEO (Surface 40); **≥N (start 3)** → a collection candidate (this). Gated exactly like SEO (email approve → apply → receipt); **never auto-creates**. Real target: "jeep ducks for sale" (623 impr) → a "Jeep Ducks for Sale" collection grouping every jeep duck.

**Cross-repo boundary** (Plan-confirmed): **duck-ops** owns planning/gating/state/operator surfaces; **duckAgent** owns the Shopify create + apply executor. They talk only through the run-file JSON contract (`member_product_ids` must survive the round-trip — pin it, per the 2026-05 my_shop_velocity round-trip bug). Flow id `shopify_collection`; state under `state/shopify_collection_review/`.

**The one new external primitive (load-bearing):** `shopify_helper` has collection *update* + *add-member* but **no create** — `shopify_collection_create` (GraphQL `collectionCreate` + `userErrors`, REST fallback) is the only genuinely new prod-mutation. Sandbox/dry-run before live; `duck-automation-safety` weight.

**Stage A built 2026-06-27 (read-only planner; no Shopify create yet). Two design corrections the live run forced:**
1. **Source is ALL queries, not gap_queries.** A "gap" is BY DEFINITION a query with no catalog match (GSC's gap detector), so it can never reach ≥N members — sourcing collections from gaps is self-contradictory. Collections group products we ALREADY sell, so the planner reads `top_queries` + `gap_queries` combined. (Gaps remain the Build-Next signal: 0 matches → make a product.)
2. **Full-subset token match (coverage 1.0), not 0.5.** At 0.5, "baby yoda duck" `{baby,yoda}` grabbed 9 unrelated products via the generic "baby" → a junk collection. Requiring every distinctive query token be present means a collection only groups genuine theme matches.
**Empirical result on current data: 0 candidates** — and that's a true signal, not a bug: current unmet/high-traffic demand is product-specific ("baby yoda duck"→1 product→listing SEO) or jeep-model ("custom ducks for jeeps"→0 products→Build-Next), NOT category-level groupings of existing inventory. The lane fires when a category query (e.g. "dog ducks") with ≥3 matching products appears.

### 41.1 Planner producer — `duck-ops/runtime/shopify_collection_planner.py` (✅ Stage A built + 13 tests)

| Use case | Expected | Layer |
|---|---|---|
| 0 / 1 matching product | skip (not a collection) | unit |
| ≥N matching active products | emit candidate spec (handle, title, SEO, member ids+titles, query, impressions, trend) | unit |
| match bar | ≥0.5 query-token coverage AND ≥2 shared tokens (no lone "duck" match); members active + id-verified | unit |
| dedup vs existing collections | drop if handle/title-overlap/subset hits an existing collection (custom + smart) | unit |
| stale / unavailable / missing demand | empty set, `available:false`, no email (reuse `_is_stale` 21d) | unit |
| under-threshold after filtering | fail closed — drop candidate, never empty collection | unit |
| write | atomic + DUCK_TEST_MODE FROZEN-path guard (new prod-write path, convention #4) | unit + audit |

### 41.2 LLM title/SEO generation (convention #5) (🔴 planned)

| Check | Criterion |
|---|---|
| title reflects intent | gated eval `eval_shopify_collection_titles.py` + golden fixture; deterministic cross-check (title shares ≥1 gap-query token + contains "duck" + 45–70 chars) |
| fail-soft | LLM miss → deterministic title from the gap query ("Jeep Wrangler Ducks"), never blocks |

### 41.3 Approval gate (duck-ops, mirror SEO) (🔴 planned)

| Piece | Expected |
|---|---|
| flow register | `duck_flows.FLOWS["shopify_collection"]` + `ReplyAction("Reply Create","create","create")` |
| email | cadence-gated; subject `MJD: [shopify_collection] … FLOW:… RUN:… ACTION:review`; body lists member TITLES so operator catches bad groupings |
| desk tile | `business_operator_desk` shows "N collection proposals awaiting review" from `latest.json` |
| intake | existing `operator_inbox_poller` forwards FLOW/RUN/ACTION — no change |

### 41.4 Apply path (duckAgent, mirror apply_shopify_seo_review_run) (🔴 planned)

| Step | Fail-safe |
|---|---|
| live dedup re-check before create | exists now → skip as "already exists", not failure (covers email→reply race) |
| re-verify member ids live | any id unconfirmed → **fail closed**, skip candidate (never add guessed/wrong products) |
| create + add members + verify readback + receipt | per-candidate success/partial/failed; run `applied` only at zero failures else `apply_attempted` (re-runnable, idempotent via dedup) |
| dispatch | `handle_mail_event` branch `flow==shopify_collection` → wrapped in publish-feedback email |
| entry point | apply reachable ONLY via operator reply; producer only ever writes `awaiting_review` |

### 41.5 Outcome loop (L4) + observability (🔴 planned)

| Piece | Expected |
|---|---|
| `shopify_collection_outcomes.py` | baseline at create, then track GSC impressions/CTR for the source query + `/collections/<handle>` over the window |
| two-card bracket (convention #3) | producer freshness + apply throughput — built WITH the live lane, not deferred |
| isolation | conftest redirects in BOTH repos (apply writes receipts into duck-ops state) + FROZEN guard + pollution-audit test |

**Top risks (Plan-flagged):** the new create primitive (live store mutation), the cross-repo run-file contract round-trip, dedup correctness (proposing an existing collection = trust failure), apply-time live re-validation (state drifts between email and reply), and not deferring the health bracket. **Gated build, sandbox-first on the create call.**

---

## Surface 42 — GA4-verdict weekly-sale steering (2026-06-27, matrix before code; operator: "auto-sale steering")

**Goal.** Stop discounting proven winners and prioritize the leaks: steer the weekly Shopify SALE target list by each product's GA4 verdict — **PROMOTE → drop** (don't erode margin on what already sells), **FIX → keep/prioritize** (discount to convert traffic that isn't converting), **neutral/unmatched → unchanged**. Autonomous; rides the EXISTING `evaluate_weekly_sale_policy` approval gate (not unguarded discounting).

**Hook (confirmed):** `duckAgent/flows/weekly/steps.py::_collect_weekly_shopify_sale_targets` (line 578) feeds `evaluate_weekly_sale_policy`. Steering filters/reorders the target list right there. Steering logic + tests in **duck-ops** (reuse `seo_demand_context.listing_signal` for title→verdict match); one-line wire-in in **duckAgent**. Fail-soft: no/stale GA4 → targets pass through untouched.

| Use case | Expected | Layer |
|---|---|---|
| PROMOTE-verdict target | dropped from sale (with reason) | unit |
| FIX-verdict target | kept, prioritized | unit |
| neutral / no GA4 match | kept unchanged | unit |
| no GA4 data / stale | all targets pass through (today's behavior) | unit |
| match | title→verdict via `listing_signal` (2+ shared tokens / full coverage) | unit |
| wiring | ✅ duckAgent `_collect_weekly_shopify_sale_targets` calls `_apply_sale_steering` (reads duck-ops `state/sale_steering.json`) before policy eval; 3 duckAgent tests | integration |

**RESOLVED:** sale records are product_id-only → directives keyed by product_id (duck-ops `sale_steering.build_steering_directives` maps catalog id→title→GA4 verdict, writes `state/sale_steering.json`; duckAgent reads it file-based — no cross-repo import). Live: 1 promote excluded, 4 fix prioritized. **Built 2026-06-27** (duck-ops producer + 16 tests; duckAgent reader + 3 tests). **Governance:** the live discount apply stays behind `evaluate_weekly_sale_policy`'s manual-review/operator-approved gate.

**Cadence + observability shipped 2026-06-27.** All three demand producers (gsc_search_demand, ga4_listing_performance, sale_steering) now run **daily on launchd** (07:05/07:08/07:13, via `run_duck_ops_observe_review.sh`) — installed + kickstart-verified (succeeded, exit 0, state refreshed). Two-card bracket complete: (1) **"did it run"** — free via `scheduler_health` (the wrapper writes scheduler receipts); (2) **"ran but unusable"** — new `_load_first_party_demand_health` OS card (`first_party_demand_feeds`, duckAgent viewer.py) goes RED on `available=false` (expired Google token — the case scheduler_health misses), 0 rows, or >48h stale. Loader + registration + 7 tests (incl. registration guard per [[feedback_loader_and_registration_both_need_tests]]); verified live GREEN end-to-end. Surface 42 fully live + self-monitoring.

---

## Surface 43 — Auto-approved flows must not nag the operator (2026-06-27, operator: "I got the posted email before I approved")

**Root cause (architectural):** the review `review_status == "pending"` field is OVERLOADED for two consumers — the auto-enqueue (`auto_enqueue_publish_ready`) reads `pending` as "post this," and the operator decision queue reads `pending` as "needs a human." So a positive reply (auto-approved by design) sat as `pending` in the operator queue during the window between *drafted* and *auto-enqueued*, making it look like it needed approval it never needed. Naively flipping the status to hide it would STARVE the auto-poster (it keys off `pending`) — the trap that made this subtle.

**Fix (display-only, general):** `surfaced_review_items` now excludes items flagged `auto_handled` (flow on the auto-approve path AND `publish_ready`), while `review_status` stays `pending` so auto-enqueue is unaffected. The auto-approved set is a SINGLE SOURCE OF TRUTH (`_auto_approved_flows`, driven by the live execution policy) shared by both layers — so any FUTURE flow turned auto-approve drops out of the operator queue automatically, no per-flow repeat.

| Use case | Expected | Test |
|---|---|---|
| reviews_reply_positive + publish_ready, policy on | auto_handled → excluded from operator queue | ✅ `test_review_queue_auto_handled.py` |
| same flow, needs_revision | still a human decision (not auto_handled) | ✅ |
| non-auto flow (newduck) + publish_ready | still surfaced | ✅ |
| generality: any flow in the set | excluded with zero extra code (future-proof) | ✅ |
| review_status untouched | stays `pending` → auto-enqueue still posts | ✅ (display-only filter) |
| policy off | `_auto_approved_flows` empty → nothing excluded | ✅ |

**Audit answer (would this hit other flows?):** yes — any flow given an auto-approve policy without this shared-set wiring would repeat it. Now structurally prevented. Same family as [[feedback_passthrough_chain_audit]] / [[feedback_portal_action_vs_card_refresh]] (two readers of one truth). **Tier-2, display-only; 7 tests; 984 suite green.**

---

## Surface 44 — Persist `handling` and decouple the auto-poster from `review_status` (2026-06-27, Surface 43 structural root)

**Goal.** Stop overloading `review_status=="pending"` as the auto-post trigger. Introduce `resolve_handling(decision) -> auto|manual` as the SINGLE SOURCE OF TRUTH for the automation axis (persisted value wins; else derived from flow + live execution policy). The auto-enqueue keys off `handling==auto` instead of `review_status==pending`; the operator-decision queue keeps using `review_status`. Persist `handling` (backfilled lazily) so the field is real, per STATUS_CONTRACT debt #1.

**Safety (the careful part):** dropping the `review_status==pending` gate must NOT let a rejected/archived reply post. New gate = `handling==auto AND decision==publish_ready AND execution_state==not_queued AND review_status NOT IN {rejected, archived}`. `execution_state` remains the dedup (queued once → never re-queued). Behavior is preserved: auto items still queue, manual skip, operator-kill wins; the only delta is an *approved-but-unqueued* auto item now posts (correct — it's approved).

| Use case | Expected | Test |
|---|---|---|
| auto flow, publish_ready, not_queued, pending | queued (unchanged) | 🔴 planned `test_auto_enqueue_gating` |
| manual flow (handling=manual) | skipped (humans handle it) | 🔴 |
| operator rejected/archived an auto item | NEVER auto-posts (kill wins) | 🔴 |
| already queued (execution_state!=not_queued) | not re-queued (dedup) | 🔴 |
| `resolve_handling`: persisted value wins | returns stored handling | 🔴 |
| `resolve_handling`: absent → derived from flow+policy | auto when policy on, else manual | 🔴 |
| handling persisted on reconcile | `decision["handling"]` set, backfills old artifacts | 🔴 |
| display layer (`item_is_auto_handled`) routes through `resolve_handling` | one predicate, both layers agree | 🔴 |

**Governance:** touches the live Etsy review-reply auto-poster → heavy tests + duck-bug-hunt Lens-2/contract + deploy-check before commit. Tier-2 logic (no new external mutation; same posting path, different trigger field).

**Built 2026-06-27.** `resolve_handling` (single source of truth) + auto-enqueue repointed off `review_status==pending` → `handling==auto` with operator-kill (rejected/archived) + execution_state dedup guards; `item_is_auto_handled` now honors persisted `handling` too. 10 new tests (resolve_handling + every gating case incl. operator-kill, dedup, the approved-unqueued behavior change); contract lens clean; 993 suite green. **Authority is read-time** (`resolve_handling` derives auto/manual from flow+policy when `handling` is absent, and persisted value wins) — so the field is authoritative everywhere now; a produce-time stamp that writes `handling` into the artifact at creation is a clean LOW follow-up (not behavior-affecting since read-time resolution already covers it).

---

## Surface 45 — Competitor exact weekly sales via `transaction_sold_count` delta (2026-06-28, shipped duckAgent 0983c90; operator: "are there Etsy tools with a better calc?")

**Goal.** Replace the quantity-drop *proxy* leaderboard with Etsy's own exact sold-count, diffed week-over-week — the method real Etsy-analytics tools use. The proxy over-counts big catalogs 4-6× (routine restocks/edits read as sales) and under-counts restocking shops; it ranked myJeepDuck 5th-6th when the truth (counter delta) is **3rd**. Shop-level only; per-listing stays proxy (Etsy exposes no per-listing sales to anyone).

| use case | happy | no prior counter | counter went backwards | non-dated/test report | empty input |
|---|---|---|---|---|---|
| `_load_prior_shop_sold_counts` finds nearest prior (±3d) | ✅ `test_competitor_exact_sales.py::test_loads_nearest_prior_within_tolerance` | ✅ `::test_returns_empty_when_no_prior_in_window` | n/a | ✅ `::test_skips_non_dated_test_reports` | ✅ returns `({},None)` |
| `_attach_exact_sold_7d` computes delta | ✅ `::test_exact_delta_is_computed` | ✅ `::test_no_prior_counter_falls_back` (keeps labeled proxy) | ✅ `::test_counter_going_backwards_is_rejected` (never negative) | n/a | ✅ |
| render prefers exact, no tilde | ✅ `::test_format_prefers_exact_no_tilde` | ✅ `::test_format_falls_back_to_labeled_proxy` | n/a | n/a | n/a |
| leaderboard sorts by exact over proxy | ✅ `::test_sort_key_uses_exact_over_proxy` | n/a | n/a | n/a | n/a |
| "your rank #k of M" line | ✅ `::test_rank_note_places_you_correctly` / `::test_full_render_shows_exact_and_rank` | ✅ `::test_rank_note_absent_without_data` | n/a | n/a | ✅ absent |

**Shipped.** 12 tests (`duckAgent/tests/test_competitor_exact_sales.py`), 50 green across competitor suite. 157 historical reports backfilled (no API). Fails closed: backwards counter → no number, not a negative; no prior → labeled proxy fallback. **Follow-up (LOW):** an OS card that flags when many shops go `counter_anomaly`/stale so a silent Etsy data gap doesn't quietly revert the board to proxy.

---

## Surface 46 — Demand-ranked competitor comparison (2026-06-28, **matrix BEFORE code**; operator: "same duck and they're selling more, or a different duck selling more")

**Goal.** The report already has both comparison buckets, but both rank on **lifetime engagement (views + favorites *stock*)** — so an old listing that banked 5,000 favorites two years ago "beats" a new one selling now. Re-rank both on a **demand score** (current *flow*) over operator-selectable **7d and 30d** windows, and present them as the two explicit operator buckets.

**Demand score** (per competitor listing, per window W∈{7d,30d}; pure arithmetic on snapshot ints → deterministic, no LLM):
```
demand(listing, W) = ( favorites_velocity(W) + SALES_WEIGHT * sales_proxy(W) ) * shop_credibility
  favorites_velocity(W) = max(0, favorers_now − favorers_{W ago})   # leading buy signal; 7d=prev snapshot, 30d=4-week-prior
  sales_proxy(W)        = capped quantity-drop over W (_capped_sold against the W-prior snapshot)
  shop_credibility      = clamp(exact_sold_7d / SHOP_REF, lo, hi)   # a real-selling shop weights up; view-only weights down
```
- **Bucket A "Same duck — they're outselling you":** for each MY listing, matched competitor listing (existing `_find_similar_competitor_products`) whose **demand** beats mine by margin+ratio on *flow* (not stock). Output: my_title ↔ their_title, demand breakdown, window, action=refresh/promote mine.
- **Bucket B "They sell it, you don't":** gap ducks (existing semantic dedupe) ranked by **demand** (was: raw `engagement_delta_7d≥100`). Output: title, shop, demand + its favorites-velocity/sales-proxy breakdown, window, price.

_Test files: `duckAgent/tests/test_demand_score.py` (helper, 10), `test_competitor_demand_buckets.py` (engine wiring + golden, 9)._

| use case | happy (7d) | happy (30d) | missing W-prior snapshot | missing favorites/qty field | exact shop sales missing | dedupe (embedding) outage | empty input |
|---|---|---|---|---|---|---|---|
| `demand_score(listing, W)` arithmetic | ✅ `test_demand_blends...` | ✅ same helper | ✅ `test_window_unavailable_when_no_prior` | ✅ `test_missing_components_do_not_crash` | ✅ `test_shop_credibility...neutral` | n/a | ✅ |
| **Bucket A** flags same-duck on *flow* | ✅ `test_bucket_a_flags_when_competitor_demand_high` | ✅ via `their_demand_30d` | ✅ falls to available window | ✅ component→0 | ✅ neutral cred | ✅ lexical fallback (unchanged) | ✅ `test_bucket_a_no_similar_no_flag` |
| **Bucket A** old high-stock / zero-velocity does NOT flag (the key fix) | ✅ `test_bucket_a_old_high_stock_does_not_flag` + `test_old_high_stock_listing_does_not_flag` | ✅ | n/a | n/a | n/a | n/a | n/a |
| **Bucket B** gap duck ranked by demand | ✅ `test_bucket_b_gates_and_ranks_by_demand` | ✅ 30d cols asserted | ✅ `test_bucket_b_new_listing_falls_back_to_engagement` | ✅ compute_demand | ✅ neutral cred | ✅ degraded path forced in test | ✅ empty list→[] |
| **Bucket B** already-made duck stays suppressed | ✅ existing dedupe (`test_fuzzy_matching_dedupe`) unchanged | n/a | n/a | n/a | n/a | ✅ lexical hard-skip preserved | n/a |
| shop_credibility weights real seller up / view-only down | ✅ `test_real_selling_shop_weights_up...` | n/a | n/a | n/a | ✅ neutral | n/a | n/a |
| window selector (7d vs 30d) changes ranking | ✅ `test_attach_listing_demand_7d_vs_30d` | ✅ same | ✅ `test_attach_demand_unavailable_when_no_prior` | n/a | n/a | n/a | n/a |
| render: both buckets, **7d + 30d columns side by side** (sorted by 7d), breakdown in email + payload | ✅ live-render smoke (both headers + Demand 7d/30d cols) | ✅ | ✅ `_demand_cell`→"—" | n/a | n/a | n/a | ✅ "No … this window" |
| golden fixture gates retunes | ✅ `test_golden_fixture_surface_decisions` (5 cases) | — | — | — | — | — | — |

**Window presentation (operator decision 2026-06-28):** each bucket shows **7d and 30d demand in adjacent columns, sorted by 7d** — so "hot this week" vs "steady all month" is visible at a glance. A window with no prior snapshot renders "—" in that column, never a fabricated number.

**Honest limits (label in output, don't hide):** no exact per-listing competitor sales exists (Etsy doesn't expose it) → demand is a *proxy*; favorites-velocity is a *leading* indicator not a sale; the 100-listing API page cap still bounds each shop's visible catalog (separate item). **Determinism:** all-arithmetic, temp-0 N/A (no LLM in the score) — the only LLM-adjacent step is the existing embedding dedupe, which already fails open to lexical.

**Tunables to pin in config (versioned-config recipe):** `SALES_WEIGHT`, `SHOP_REF`, credibility `lo/hi`, Bucket-A margin+ratio, Bucket-B demand floor. A golden fixture (hand-picked "should/shouldn't surface" ducks) gates any retune.

**Built 2026-06-28 (duckAgent 4100d13).** `flows/competitor/demand_score.py` (pure scorer) + `config/demand_scoring.json` (versioned tunables) + `tests/fixtures/demand_scoring_golden.json`. Both buckets re-ranked on demand; Bucket A (improvement_opportunities) wired into the email for the first time; my-shop snapshot now stores favorites for future my-side velocity. 19 new tests + golden, 308 green. Verified on live 06-28: top demand = Breast Cancer / Highland Cow / Pitbull; 30d surfaces steady sellers (Highland Cow 303 vs 7d 82). The one ⏸️ still-open follow-up: a my-side demand baseline (currently 0 until favorites history accrues) so Bucket A compares your flow vs theirs, not just "is a competitor selling this."

**Roadmap (operator: "later, roadmap what things look like over the year"):** a separate surface — seasonal/year view over the **157 backfilled reports'** time series (exact shop sales + demand by theme over 12 months) to spot recurring patterns (e.g. patriotic spikes in Jun-Jul, dog breeds steady). Not this surface; flagged so the matrix has the row when we get there.

---

## Surface 47 — Demand-rank Build-Next on the competitor demand signal (2026-06-28, **matrix BEFORE code**; operator: "turn the heads-up into what we build")

**Headline (Plan agent finding, both repos read):** the MINT path and cross-repo read **already exist** — `assemble_candidates()` already unions `ducks_to_build` into the pool (build_next_engine.py:164-178), and `COMPETITOR_REPORTS_DIR` + `latest_competitor_report()` already read duckAgent's report cache (lines 57, 138-161). The real defect is a **single silent drop**: gap-duck entries carry `demand_7d`/`demand_30d` but NOT `trending_score`; `_demand_basis()` sees `delta_source=="snapshot"` → reads `trending_score` → `None` → falls through to `engagement_score` → caps the row at `FALLBACK_DEMAND_CEILING=0.5`. So **every demand-ranked gap duck is demoted to the capped all-time pool and its demand signal is never read.** That IS the "ranks on all-time, not 7d/30d" debt, biting exactly the mint source.

**Fix:** `_demand_basis` reads `demand_7d` FIRST as a third signal class (`demand` / `momentum` / `alltime`), each normalized in its OWN pool (the `demand_7d`~tens-hundreds vs `trending_score`~hundreds-thousands magnitude gap re-creates the Duckpool inversion if mixed). Missing demand degrades to the existing capped path (mirrors `NEUTRAL_SEARCH=0.6` spirit). + a 21-day staleness guard on `latest_competitor_report` (mirror `seo_demand_context.py` `STALE_MAX_DAYS`).

**Isolation:** read-only consumption of `COMPETITOR_REPORTS_DIR` → **NO new FROZEN constant / pollution-audit test** (the 3-layer rule is for new *written* prod paths; the only write, `BUILD_NEXT_QUEUE_PATH`, is already covered). One read-only hardening: redirect `COMPETITOR_REPORTS_DIR` in `tests/conftest.py` so the staleness test is hermetic. State this in the PR to preempt a reviewer asking for dead ceremony.

| use case | happy (demand present) | demand_7d None (new-to-tracking) | report missing | report stale >21d | malformed breakdown / all-None pool |
|---|---|---|---|---|---|
| `_demand_basis` reads demand_7d as its own class | 🔴 demand pool, full [0,1], NOT capped; reason cites breakdown | 🔴 falls to capped all-time pool (**today's behavior preserved**) | n/a | 🔴 demand absent → degrade | 🔴 `demand_max=0` → no div-by-zero, 0.0 + clean reason |
| higher demand_7d outranks (the inversion regression) | 🔴 **keystone unit** | n/a | n/a | n/a | n/a |
| separate-pool invariant (trending_score row can't swamp a demand_7d row) | 🔴 each normalizes in its own pool | n/a | n/a | n/a | n/a |
| momentum (trending_products) ranking unchanged | 🔴 regression | n/a | n/a | n/a | n/a |
| staleness / fail-soft | n/a | n/a | 🔴 empty queue, warn, no crash (exists) | 🔴 `sources.competitor_report_stale=True`, rows degrade, no crash | n/a |
| catalog dedup preserved | 🔴 already-made suppressed (regression) | n/a | n/a | n/a | n/a |
| product_concept_queue dedup (Phase 3, optional) | 🔴 pending concept suppressed w/ reason | n/a | n/a | n/a | n/a |
| conftest redirect makes staleness test hermetic | 🔴 reads tmp dir not prod cache | n/a | n/a | n/a | n/a |

**Phases:** 5 (matrix) → 1 (3-class `_demand_basis` + `score_demand` N-pool + entry passthrough) → 2 (staleness guard) → 3 (optional concept-queue dedup) → 1-bonus (optional duckAgent: copy demand onto `trending_products` so both sources share one pool). All duck-ops except the optional bonus. Regression gates: existing pollution-audit + `test_weekly_packet_build_next_nominations.py`. Re-run the producer after merge to refresh prod state.

**Scope honesty:** the "MINT not just boost" framing is ~90% already done — don't bill it as new wiring. The high-value change is the one-spot silent-drop fix. Effort: ~half a day, no new write path, no LLM.

**BUILT 2026-06-28.** `_demand_basis` now returns a 3-class signal (demand/momentum/alltime) reading `demand_7d` first (+`demand_30d` 0.25 ballast); `_pool_maxes` normalizes each class in its own pool; `score_demand` gained `demand_max`/`allow_demand` (backward-compatible kwargs). `_competitor_report_is_stale` (21-day, lenient) + `allow_demand=not stale` degrade demand on a stale report; `sources.competitor_report_stale` stamped; entries pass through `demand_7d`/`demand_30d`/`demand_breakdown`. Conftest redirects `COMPETITOR_REPORTS_DIR` (read-only hermetic). **No FROZEN guard / pollution-audit added (read-only) — by design.** 19 new tests (keystone `test_higher_demand_7d_outranks`, separate-pool, stale-disables, integration) + 2 existing updated to the class API; full duck-ops suite **1010 passed**. Post-deploy producer re-run clean (53 ranked, stale=False). The demand *class* goes live once the next native competitor run writes `demand_7d` onto `ducks_to_build`.

---

## Surface 48 — Build-Next split: "Build New" vs "Improve Existing" with side-by-side listings (2026-06-29, operator: "theirs might be better, or I need better photos/price — give me the link to compare")

**Background:** the operator clicked into Build-Next and found ducks they *already make* (Donald→their Trump Duck, a dachshund, hockey sticks, breast cancer). Those are the `possible_dupe` soft-flag rows from Surface 28 — correctly *kept* (the 0.43–0.72 band is too fuzzy to auto-hide) but mixed into one "build this" list, which reads as "build a duplicate." The real question for a possible_dupe row is **not** "build it?" but "is *their* version winning on photo / price / listing copy — should I improve mine?" That needs both listings side by side, both linked.

**Fix (producer + page, no new write path):**
- **Producer** (`build_next_engine.py`): each entry gains `lane` (`"improve_existing"` when `possible_dupe`, else `"build_new"`); an improve_existing entry also gets `own_listing` = `{title, handle, url, image_src}` joined from the operator catalog by normalized `semantic_match` title (`_own_listing_index`), `url=https://www.myjeepduck.com/products/<handle>`. Output `sources`-level `build_new_count`/`improve_existing_count`. `SURFACE_VERSION`→2. **Price is not in the catalog → degrades to link + image** (stated, not silently dropped).
- **Page** (`build_next_intel_page.py`): `_render_queue` → `_render_build_new` (keeps Promote) + `_render_improve_existing` (your-duck-vs-theirs table: your thumb+Shopify link, their Etsy link `https://www.etsy.com/listing/<id>`, demand%, dupe-decision buttons; **no Promote** — already made). Back-compat: `_split_lanes` falls back to `possible_dupe` when `lane` absent (pre-v2 payload).

**Isolation:** read-only join of the existing catalog; the only write (`BUILD_NEXT_QUEUE_PATH`) is already 3-layer covered. **No new FROZEN constant — by design.**

| use case | happy | lane absent (pre-v2 payload) | join misses (title not in catalog) | empty section |
|---|---|---|---|---|
| producer tags `build_new` on genuine gap | ✅ `test_build_next_engine.py::TestBuild::test_build_new_lane_on_genuine_gap` (no `own_listing`, counted) | n/a | n/a | n/a |
| producer tags `improve_existing` + attaches own_listing | ✅ `::test_improve_existing_lane_attaches_own_listing` (url+image joined) | n/a | ✅ `::test_improve_existing_own_listing_none_when_join_misses` (None, no crash) | n/a |
| lane counts sum to queue_count | ✅ `::test_lane_counts_sum_to_queue_count` | n/a | n/a | n/a |
| page routes improve_existing → its section w/ both links + buttons | ✅ `test_build_next_page.py::test_improve_existing_section_renders_buttons_and_links` (Etsy + dupe buttons, no Promote) | ✅ `::test_lane_absent_falls_back_to_possible_dupe` | ✅ degrades to "listing not found" copy (asserted via own_listing None render path) | ✅ `_render_improve_existing` returns "" when none; `_render_build_new` empty-state copy |
| page shows own listing link + thumbnail | ✅ `::test_improve_existing_shows_own_listing_link_and_image` (Shopify url + cdn img) | n/a | n/a | n/a |
| build_new keeps Promote, no improve section | ✅ `::test_build_new_entry_has_promote_no_improve_section` | n/a | n/a | n/a |

**BUILT 2026-06-29.** Producer + page split shipped; 4 new producer tests (72 pass) + 4 new/renamed page tests (14 pass), duckAgent page+smoke+viewer **266 pass**, inline JS `node --check` clean. Live verify after producer re-run + viewer bounce: 44 ranked → **2 build_new / 10 improve_existing**; every improve row carries both links + thumbnail + dupe buttons.

### 48.1 — Confident/weak band split + card redesign + correction (2026-06-30, operator: "layout's bad + wine ducks ≠ highland cow, how do I correct blunders?")

**Two problems on the live page.** (1) The 4-col table crushed to one char per line on a phone (operator screenshot). (2) A **false dupe**: competitor "Highland Cow Duck" matched the operator's "Wine Ducks" at cosine **0.4615** — both are SEO keyword-stuffed with the same outdoorsy/vehicle vibe words ("Adventure/Off-Road/Collectible") that survive the boilerplate stripper, so the embedding scored them ~46% alike despite unrelated subjects. 0.46 is the very bottom of the soft band (0.43–0.72), but the split presented it with the *same confidence* as a 0.71 real dup, and it sorted to the TOP (high demand).

**Fix (operator chose "default weak matches to Build New").** `CONFIDENT_DUPE_FLOOR = 0.55` splits the soft band: ≥0.55 → improve_existing ("you already make this"); <0.55 → build_new with a correctable `weak_match` hint ("⚠ weak NN% match to your X — already make it? [I make this ✓]"). Operator rulings (`distinct`/`duplicate`) still override the score. **The correction already existed** (`record_dupe_decision` → `distinct` re-routes to Build New on next build) — the crushed layout just made it unreachable; the rebuild surfaces it as a one-tap button in both lists. Page rebuilt from wide tables to **stacked concept cards** (your-vs-theirs is a 2-col grid collapsing to 1-col <480px); Improve-Existing sorted strongest-match-first. Live result: 6 improve / 6 build_new; **Highland Cow (0.46) now in Build New with a weak hint**, not a false dup; the 6 confident dups (Dachshund 0.71, Breast Cancer 0.66, Pitbull 0.63, Donald→Trump 0.58, Raccoon 0.57, Sheriff→Police 0.55) carry both links + thumbnail.

| use case | confident (≥0.55) | weak (<0.55) | distinct ruling | join misses |
|---|---|---|---|---|
| lane assignment | ✅ `test_build_next_engine.py::TestBuild::test_confident_dupe_lane_attaches_own_listing` (improve_existing + own_listing) | ✅ `::test_weak_dupe_defaults_to_build_new_with_hint` (build_new + weak_match) | ✅ `::test_distinct_ruling_forces_build_new_even_when_confident` (override → build_new) | ✅ `::test_confident_dupe_own_listing_none_when_join_misses` |
| floor pinned | ✅ `::test_weak_dupe_…` asserts `CONFIDENT_DUPE_FLOOR == 0.55` | n/a | n/a | n/a |
| page: improve cards + correction | ✅ `test_build_next_page.py::test_improve_existing_section_renders_buttons_and_links` (both links, "Different product →" / "I make this ✓", no Promote) | ✅ `::test_build_new_weak_match_shows_correctable_hint` ("weak 46% match" + duplicate btn + still Promote-able) | covered by producer | ✅ `_own_listing_block` "listing not found" path |
| mobile layout | ✅ stacked cards (no wide table); `bn-compare` collapses 2→1 col <480px — manual: rendered live, no char-per-line crush | n/a | n/a | n/a |

**Honest design note:** 0.46 was never a true dup — the band correctly didn't hard-suppress it, but the split over-trusted it. The floor at 0.55 is calibrated above the wine↔cow false-positive (0.46) and below the Dachshund real near-dup (0.71); it's a module constant with a documented rationale, not a golden-evaluated threshold — if a real dup ever lands at 0.50–0.55 it shows in Build New with the "already make it?" hint, so nothing slips silently (the correction is one tap). Tier 1 (producer re-run, read-only catalog join).

---

## Surface 49 — Build-Next funnel awareness (concept-queue dedup) + card cleanup (2026-06-30, operator: "is there a different purpose to each or should it just be one? how is this deduped?")

**Architecture question, real bug underneath.** Build-Next is **scout ② of four** feeding the one `product_concept_queue` funnel (trend, build_next_promotion, competitor_motif, strategy_idea → funnel → Studio; two approval gates). Documented in [`docs/PRODUCT_CONCEPT_PIPELINE.md`](docs/PRODUCT_CONCEPT_PIPELINE.md). The funnel's only cross-scout dedup, `_merge_duplicate_themes`, keys on **exact `_slugify(theme)`** — but scouts derive `theme` differently: trend "chef" → slug `chef`; Build-Next promote of the raw competitor title "3D Chef Duck: PLA Plastic Figurine" cleans only to `3d chef pla plastic` → slug `3d-chef-pla-plastic`. **Slugs differ → merge misses → duplicate concept.** Empirically confirmed (probe of `_clean_theme`+`_slugify`). Competitor titles are always keyword-stuffed, so this is the common case.

**Fix (layer 3 — Build-Next funnel awareness).** The producer reads the funnel (`_load_active_concept_themes`, fail-soft, skips `suppressed_by_operator`) and flags any build_new candidate already in it (`_concept_queue_match`: the clean queued theme must be ≥`_CONCEPT_QUEUE_COVERAGE_MIN` 0.8 present in the candidate's tokens — **token coverage, immune to keyword-stuffing**, unlike the funnel's slug match). Flagged entries carry `in_concept_queue={theme, source_type, source_label, queue_state}`; the page badges "✅ already in your concept queue (via trend scout)" and **disables Promote** so it can't be double-created. `already_in_concept_queue_count` in output; `SURFACE_VERSION`→3. **Read-only** consumption of the funnel state file → no new FROZEN write-guard (the 3-layer rule is for new *written* prod paths). Also this surface: card **stat de-duplication** (the two redundant rows — `Demand 100% · …` scores AND a prose row repeating the same five numbers — collapsed to one per-factor list `Label NN% — reason`), title trimming (`_short_title`, full title in `title=`), and clearer button labels ("Already sell this — hide").

| use case | already queued | not queued | operator-suppressed queue item | malformed funnel file |
|---|---|---|---|---|
| producer flags in_concept_queue | ✅ `test_build_next_engine.py::TestBuild::test_candidate_already_in_concept_queue_is_flagged` (theme+label+count) | ✅ `::test_candidate_not_in_queue_has_no_flag` | ✅ `::test_operator_suppressed_queue_item_does_not_block` | ✅ `::test_load_active_concept_themes_failsoft_on_garbage` (bad json / missing file → []) |
| token match immune to keyword-stuffing | ✅ live: "3D Chef Duck: PLA Plastic Figurine" → queued "Chef"; Greyhound candidate correctly NOT matched to queued "Golden Retriever" (no shared tokens) | n/a | n/a | n/a |
| page badge + Promote disabled | ✅ `test_build_next_page.py::test_build_new_already_in_queue_badges_and_disables_promote` (badge + `disabled` + no active data-listing) | covered by other page tests | n/a | n/a |
| card stats de-duplicated | ✅ `::test_build_new_factor_lines_not_duplicated` (one per-factor list; "92%" appears once, not score+prose) | n/a | n/a | n/a |

**BUILT 2026-06-30.** 4 new producer tests (78 pass) + 2 new page tests (17 pass); duck-ops full **1021 pass**, duckAgent page+smoke **34 pass**, inline JS `node --check` clean, secret scan clean. Live: 44 ranked, **1 flagged** ("3D Chef Duck" → queued "Chef" via trend scout, Promote disabled); factor rows collapsed to one clean list. Docs: new `docs/PRODUCT_CONCEPT_PIPELINE.md` (flow + gates + 3 dedup layers), linked from the contract + doc-hub README.

**Follow-up (DONE 2026-07-01):** layer-2 slug-merge was fragile for two scouts proposing the same duck under different cleaned themes (trend "Chef" vs a Build-Next promote cleaned to "3d Chef Pla Plastic" → different slugs → merge missed → duplicate). Fixed in `_merge_duplicate_themes` by keying on `_theme_merge_key` — the theme reduced to its core-subject tokens with a manufacturing/marketing `_MERGE_BOILERPLATE` stoplist stripped (mirrors duckAgent's semantic_dedupe list), order-independent, falling back to the raw slug when nothing survives so two all-boilerplate themes stay distinct. Deliberately keeps thematic words (adventure/rustic/…) so distinct subjects don't collapse. Tests: `test_product_concept_queue.py::CrossScoutMergeTests` (strips-to-subject, keeps-distinct, all-boilerplate-fallback, collapses-cross-scout-dup, keeps-distinct-concepts). Live probe: the 8 active concepts keep 8 distinct keys (no false collapse); "Chef" now keys to `chef` so a stuffed competitor Chef folds in. Full duck-ops suite 1027.

### 49.1 — "Already sell this" splits into two intents (2026-07-01, operator: "the already-have-it button doesn't hide it, right? it just shows up where I might update the listing?")

**One button, two intents.** The weak-match "Already sell this — hide" conflated *"I sell it, stop showing me"* (suppress) with *"I sell it — a competitor's outselling me, let me compare + maybe update MY listing"* (which is the Improve Existing view). Same overload family as [[STATUS_CONTRACT]]/status-fields-one-meaning, but for a UI action. New `improve` dupe-decision value (alongside `distinct`→Build New, `duplicate`→hide): forces a weak match into **Improve Existing** regardless of score, attaching own_listing for the side-by-side. Weak-match hint now offers **two** buttons — "Yes — compare to mine" (`improve`) and "Hide" (`duplicate`); Improve-Existing's confirm button relabeled to plain "Hide". `_VALID_DUPE_DECISIONS` + the viewer endpoint both accept `improve`.

| use case | route-to-improve (`improve`) | hide (`duplicate`) | different product (`distinct`) |
|---|---|---|---|
| producer honors ruling over score | ✅ `test_build_next_engine.py::TestBuild::test_improve_ruling_forces_weak_match_into_improve_existing` (weak 0.46 → improve_existing + own_listing) | ✅ `TestDupeFlag::test_operator_confirmed_duplicate_is_suppressed` | ✅ `::test_distinct_ruling_forces_build_new_even_when_confident` |
| page offers both intents on a weak match | ✅ `test_build_next_page.py::test_build_new_weak_match_shows_correctable_hint` (`data-decision="improve"` AND `="duplicate"`) | same test | n/a |
| endpoint accepts improve | ✅ `::TestDupeDecisionEndpoint::test_record_accepts_improve_decision` (bridge recorder gets `improve`); bad decision still `::test_record_rejects_bad_decision` | n/a | n/a |

**BUILT 2026-07-01.** duck-ops full **1022 pass** (1 new producer test), duckAgent page+smoke+viewer **270 pass** (2 new page tests), JS `node --check` clean, secret scan clean. Live: 4 weak-match cards now show "Yes — compare to mine" + "Hide". Tier 1.

---

## Surface 50 — Intel directory + nav entry (2026-07-01, operator: "how do I get to these intel screens? not in the main menu")

**Discoverability gap.** The 8 `/portal/intel/*` pages (profit, competitors, reviews, learnings, recommendations, cost, custom-builds, build-next) were only reachable via clickable Desk metric tiles — no menu entry, so the operator couldn't find them. Added a **`/portal/intel` directory page** (`intel_directory_page.py`, pure links via `_render_portal_shell`, no data load so it can't fail) listing all 8 with one-line "what question does this answer" copy, and an **"Intel" nav entry** in the shared shell (`portal_shell._PORTAL_NAV_ITEMS`) plus the hardcoded SPA navs (Desk + siblings, inserted between Workflows and Training; the cockpit nav after Library).

| use case | happy | route intact |
|---|---|---|
| directory lists every intel page w/ link | ✅ `test_intel_directory.py::test_lists_every_intel_page_with_a_link` (all 8 routes+titles) | n/a |
| Build-Next is the lead card | ✅ `::test_build_next_is_present_and_first` | n/a |
| nav has Intel entry | ✅ `::test_nav_has_intel_entry` + live: Desk nav shows Intel, `/portal/intel` marks it active | ✅ `/portal/intel/build-next` still 200 (exact-match route, no shadow) |

**BUILT 2026-07-01.** duckAgent directory+smoke+viewer+page suites **273 pass** (3 new directory tests). Live: `/portal/intel` renders 8 cards, Desk nav shows "Intel", all sub-routes intact. Tier 2 (portal render + nav, no state writes).

---

## Surface 51 — Real competitor hooks + own-post coverage in Social Strategy (2026-07-01, operator: "these hints are vague — why can't the AI grab the actual hook so I don't have to go look?")

**The recommendations gave strategic nudges** ("borrow a hook from f3dprinted", "go review their posts") instead of the actual content — even though `competitor_social_snapshots.json` carries every post's real `visible_hook`, engagement, format, and URL. And "own-post signal: LOW" was a bare label with no backing.

**Fix (Design 1 + 2v1 — deterministic joins, no LLM):** the weekly_strategy producer now (a) `_top_post_for_account` joins each competitor recommendation to that account's **highest-engagement real post** (hook text + plays/likes + format + link), attached as `top_post`; and (b) `_own_post_coverage` adds `own_post_coverage` to the packet (28 posts tracked, by-workflow, top posts). The recommendations page renders a **hook block** (the actual hook + "open post ↗") on each competitor rec, and a **"Your post coverage"** card that makes "low signal" concrete ("18 memes, 1 thursday — thin lanes flagged"). The AI-drafted duck version (2v2) is a deferred, spec-first follow-up.

| use case | happy | no match / empty | future posts |
|---|---|---|---|
| producer attaches real top post | ✅ `test_weekly_strategy_recommendation_packet.py::RealHooksAndCoverageTests::test_top_post_for_account_picks_highest_engagement` (+ normalized-handle match) | ✅ `::test_top_post_matches_normalized_handle` (None on no match / empty) | n/a |
| own-post coverage by workflow | ✅ `::test_own_post_coverage_counts_by_workflow_and_excludes_future` | n/a | ✅ future posts excluded from count |
| page renders real hook + link | ✅ `test_recommendations_hooks.py::test_hook_block_renders_real_hook_and_link` (hook, "1,565,673 plays", open-post link) | ✅ `::test_hook_block_empty_when_no_hook` | n/a |
| page coverage card | ✅ `::test_coverage_card_shows_count_and_thin_lane` | ✅ `::test_coverage_empty_when_no_data` | n/a |

**BUILT 2026-07-01.** duck-ops full **1030 pass** (3 new producer tests); duckAgent recs+viewer+smoke **256 pass** (4 new page tests, full `test_viewer` run per the CI lesson). Live: the f3dprinted rec shows its real top post — *"3D Printed Octopus Shot Drink Maker," 1.5M plays, reel* + link; coverage card shows 28 posts (18 meme / 1 thursday). Tier 1 (producer re-run, read-only social state).

---

## Surface 52 — 12 blind OS cards: status-key naming drift (2026-07-02, operator: "half the OS dashboard is blank")

**12 of ~27 `*_health` cards rendered blank on the Agent OS dashboard.** Root cause: the card loaders split into two conventions — "watchdog" cards set **`_status`**/`_status_reason` (underscore); "intel/inspector" cards set plain **`status`** (no underscore); and two "bracket" cards (occasion, theme) set only sub-statuses (`freshness_status`/`selection_status`, `coverage_status`/`backlog_status`) with no rollup. The OS dashboard + `os_health` aggregate classify on `_status`, and the assembler (`get_system_health_detail`) + cache producer did **no normalization** — so ~36 `status`-only cards + 2 bracket cards were invisible. Not a crash, not stale data: a field-name contract mismatch (the infra-level [[STATUS_CONTRACT]] "one field, one meaning" + [[feedback_loader_and_registration_both_need_tests]] "passes its own tests but invisible on the page"). 10 of the 12 were computing fine (incl. a hidden **shopify_seo YELLOW** and **theme_classification YELLOW 11d-stale**).

**Fix:** `_card_rollup_status(card)` resolves every card to one canonical `_status`/`_status_reason` from whichever convention it used — plain `status`, `_status`, OR the worst of any `*_status` bracket sub-fields (with its matching `<prefix>_reason`). Applied in a loop at the single assembly point in `get_system_health_detail`, **before** `_build_os_health` so the aggregate sees corrected statuses. A card with no status of any kind → **yellow** ("unknown → notice it"), never silently green or blank. No loader edits (fix is one place); reused the existing `_normalize_card_status`.

| use case | plain `status` | `_status` | bracket sub-status | no status at all |
|---|---|---|---|---|
| card resolves to canonical _status | ✅ `test_viewer.py::test_rollup_plain_status_convention` | ✅ `::test_rollup_underscore_status_convention` | ✅ `::test_rollup_bracket_takes_worst_and_its_reason` (worst + its reason) / `::test_rollup_bracket_all_green` | ✅ `::test_rollup_no_status_defaults_yellow_not_blank` (yellow, never blind) |

**BUILT 2026-07-02.** 5 new rollup tests; **full `test_viewer` + smoke 257 pass** (ran test_viewer per the CI lesson). Live after cache regen: **0 blind cards** (was 12); 3 red / 19 green / 5 yellow; occasion green ("1 active of 10, intel 17h"), theme yellow ("11d stale — weekly sync due"), shopify_seo yellow — all previously invisible. Tier 2 (read-only viewer assembler).

**Follow-up fix 2026-07-03 (`test_rollup_ignores_domain_status_fields`):** the rollup treated ANY `*_status` field as a bracket sub-status, so `shopify_seo`'s **domain** field `review_status="applied"` normalized to yellow and overrode its real `status=green`. Gate the sub-status scan on a color-valued (`green/yellow/red`) value only — domain lifecycle fields (`review_status`, `execution_status`) are not health signals. `test_viewer` 241 pass; SEO card green after the fix.

---

## Surface 53 — theme_classification false "stale" on a stable catalog (2026-07-02, operator: "why is theme classification 11d stale?")

**The card yellowed "11d stale" though classification was fine.** Root cause: `categorize_all_existing_ducks` (the daily 4am sweep) does `if not todo: return 0` **before** the `write_classification_health(...)` call. So when nothing needs reclassifying (stable catalog — no new/changed products), the sweep returns early and never refreshes `theme_classification_health.json`. Its `generated_at` ages, and the OS card reads that as stale — even though the daily job ran healthy every day (confirmed: `sync_weekly_daily` last_start today 04:00, green). **The freshness signal was coupled to reclassification ACTIVITY instead of "the sweep ran and verified"** — a stable, healthy catalog looked broken ([[feedback_alive_status_is_not_progress]] inverted).

**Fix:** refresh the health summary on the empty-todo path too, so `generated_at` reflects "swept, all N classified, 0 stale" every run. A stable catalog now reports fresh + accurate.

| use case | nothing stale (stable catalog) | products stale |
|---|---|---|
| sweep refreshes health summary | ✅ `test_theme_health_freshness.py::test_categorize_refreshes_health_when_nothing_stale` (empty todo still calls write_classification_health) | ✅ existing per-product path (line 290) |

**BUILT 2026-07-02.** 1 new test; theme + sync suites 33 pass. Live: sweep = 0 of 259 need reclassification, health refreshed to today, card's 11d-stale cleared — now shows the HONEST signal: 3 products flagged needs_review (a real minor backlog, not a false alarm). Tier 1 (read cache + write health state).

---

## Surface 54 — Demand intel page: "improve what you already sell" (2026-07-03, operator: "what ducks people view/click/buy, refresh or sale?")

The counterpart to Build-Next (what to MAKE): a `/portal/intel/demand` page that reads each duck's **Etsy demand funnel (views → engagement → buys)** and sorts it into an action bucket — **Winner** (protect, `sale_steering.exclude`), **Put on sale** (`sale_steering.prioritize`), **Refresh** (`seo_review.refresh_request`), **Fading**, **Seasonal-dormant**, or **low-signal** (the long tail). Producer `duck-ops/runtime/demand_intel.py` fuses catalog_index (spine) + GA4 listing views (via the seo_demand_context title join, now exposing views) + profit (30d buys/margin) + Etsy transactions (**7d buys, filtered client-side on created_timestamp** per [[reference_etsy_transactions_ignores_date_window]]) + occasion overlay. Deterministic, config-driven buckets (`config/demand_buckets.json` v1) — no LLM.

**Data honesty (the load-bearing calibration).** Measurable traffic is ~100% Etsy; views are sparse (only ~50/259 ducks, median 4/7d) so **sales are the rich signal and "no sale this week" is the baseline, NOT fading** — fading requires actual view-based decline. Favorites + Etsy per-listing search are unavailable via API and render as "n/a", never fake zeros. Thresholds calibrated to the real distribution, not round numbers.

| use case | happy | no GA4 view match | buys uncovered (7d) | off-season seasonal | low traffic |
|---|---|---|---|---|---|
| Winner (buys ≥ floor) → exclude | ✅ `test_demand_intel::test_winner_on_sales_even_without_views` | ✅ buys-driven, survives view miss | ✅ buys_30d path | n/a | n/a |
| Sale (engaged, not buying) → prioritize | ✅ `::test_sale_when_engaged_but_not_buying` | n/a (needs views) | ✅ 0-buys is the trigger | n/a | n/a |
| Refresh (traffic bounces) → refresh_request | ✅ `::test_refresh_when_traffic_bounces` + `test_demand_intel_page::test_coverage_is_honest` | n/a | n/a | n/a | n/a |
| Fading vs steady long-tail | ✅ `::test_fading_requires_view_based_decline` | ✅ `::test_low_signal_not_fading_when_no_traffic` | ✅ | ✅ `::test_seasonal_dormant_preempts_fading` | ✅ low_signal |
| Coverage never faked (favs/Etsy clicks) | ✅ `::test_build_fuses_and_flags_coverage` + page `::test_coverage_is_honest` | | | | |
| Prod-state isolation | ✅ `test_no_test_pollution_in_demand_intel` + conftest redirect + write_demand_intel FROZEN guard | | | | |

**BUILT 2026-07-03 (v1, read-only page).** 8 producer + 3 page tests; duck-ops 85 / duckAgent 251 pass. Live at `/portal/intel/demand` + in the intel directory.

**CRITICAL JOIN FIX 2026-07-03 (operator: "I didn't sell any Boxers — are you sure?").** v1 joined profit/buys by TITLE token-overlap, so Boxer/Doberman inherited the Dachshund's 22 sales (all share "3D-Printed…Loyal…Dog Duck Collectible" boilerplate — the [[feedback_competitor_titles_defeat_text_dedup]] trap, again). Fixed: **buys join by exact Shopify product_id / SKU** (`test_buys_join_by_id_not_title`) — catalog.id ↔ profit.sample_product_id, 7d via tx.sku→id map. GA4 views (Etsy, no product_id, still title-joined) get a **shared-subject guard** (`test_ga4_match_requires_shared_subject`): a match counts only if ≥½ the smaller side's distinctive (non-boilerplate) tokens overlap — else the views belong to another duck and are discarded (fixed Oklahoma inheriting Michigan's 20 views). After: winners 24→**7 real top sellers** (Dachshund 22, Football 6…), Boxer/Doberman correctly low-signal. Lesson banked: **never title-join a boilerplate-titled catalog — use id/SKU; for the one source without an id (Etsy GA4), require subject overlap and fail to "no data," not wrong data.** **OPEN:** (v1.1) one-click Sale/Protect/Refresh action buttons → `demand_action_receipts` → sale_steering/seo_refresh, with `acted_at`/`metrics_at_action` already persisted for (v2) the "did it work?" outcome loop; (Tier 3) launchd cadence so the producer stays fresh (seeded once, not yet scheduled).

---

## Surface 55 — Review-reply INGESTION repair (SPEC-FIRST, 2026-07-08, operator: "there are several 4 and 5 star reviews since June 26 with no replies")

The **input half** of the review-reply bracket. The output half (drain wedged on a `pacing_cooldown` throttle that tripped `stop_after_first_failure`) shipped 2026-07-07 (`_result_is_retryable_throttle` + `review_reply_drain_stall` OS card). This surface fixes why new reviews never reach the queue at all. **Cross-repo + Plan-agent verified — two premises corrected before speccing:**

- **CORE break = 4★ are never drafted.** `duckAgent/flows/reviews/etsy_review_helper.py::generate_daily_review_summary` computes `four_star_reviews` but builds `thank_you_messages` in a loop over `five_star_reviews` **only** (line ~550). 4★ never become a reply → never enter `_build_reviews_reply_handoff` → never reach the feed. One loop. This is the operator's "4★ with no replies."
- **`review_reply_post_index.json` is NOT the feed** — it is a derived *posted*-receipts index rebuilt from `review_reply_execution_sessions.json` (`customer_case_enrichment.load_review_reply_post_index`). Its staleness is a *symptom* of the drain stall, not the ingestion break. The real feed is `state/normalized/publish_candidates.json` (`phase1_observer.normalize_publish_candidates`), and it **already carries the new 5★ tx** (5124/5130xxx confirmed). Don't build against post_index.
- **5★ reach the feed but don't post** because `auto_enqueue_publish_ready` and the (previously wedged) `drain_queue` run in ONE `etsy_browser_batch` process. **Riskiest unknown (gates Phase 3):** did the 2026-07-07 drain fix also un-wedge enqueue? Not yet resolvable — the execution queue hasn't regenerated since 07-07 17:28 (no browser window has fired since the fix). Verify at the next window before writing Phase 3.

Also: daily collect uses `get_etsy_shop_reviews(days_back=1)` — no catch-up (miss a day = permanently missed); ~36 existing 4-5★ need a backfill; the Etsy reviews API returns **no seller-reply field**, so dedup is by `transaction_id` against post_index ∪ execution_queue ∪ posted receipts, backstopped by the drain's existing read-back guard ([[Surface 253-style existing-reply detection]]).

| use case | happy | auth/401 | missed day / gap | already-replied / dup | idempotent re-run | test isolation |
|---|---|---|---|---|---|---|
| Draft reply for 4★ (CORE) | ✅ `duckAgent test_reviews_handoff_ratings::test_four_star_review_gets_a_reply_draft` | n/a | n/a | ✅ `::test_three_star_excluded…` + `::test_low_rating_routed…` | 🔴 same tx→same draft key (P2 watermark) | n/a |
| Catch-up ingest (watermark) | 🔴 `test_reviews_catchup_watermark::window_from_watermark` | 🔴 auth-fail leaves watermark unmoved | 🔴 **3-day gap → 3 days ingested** (+paginate >100) | 🔴 gap re-run skips ingested tx | 🔴 watermark monotonic | 🔴 3-layer (new `REVIEWS_INGEST_WATERMARK_PATH`) |
| Handoff → feed (`publish_candidates`) | 🔴 `duck-ops test_review_reply_handoff_4star_feed::4star_lands` | n/a | 🔴 backlog handoff all land | 🔴 existing tx not re-added | 🔴 re-run = no dup rows | 🔴 conftest both repos |
| Auto-enqueue positive → exec queue | 🔴 `test_auto_enqueue_independence::queues_standalone` | n/a | n/a | 🔴 already-queued tx skipped | 🔴 idempotent | 🔴 pollution audit |
| Backfill ~36 (one-shot) | 🔴 `test_review_reply_backfill::N_queued_0_double` | 🔴 auth-fail aborts clean | n/a | 🔴 **already-replied tx → skipped, not re-queued** | 🔴 **run twice → same queue** | 🔴 3-layer (if receipt file) |
| Input-sanity OS card (✅ shipped 2026-07-26 as `review_reply_source_canary`) | ✅ `test_review_reply_source_canary::test_unhandled_past_grace_is_red_with_txn_list` | ✅ `::test_missing_receipt_is_yellow_not_green` | ✅ `::test_window_days_below_7_is_red` (the days_back=1 canary) | ✅ ledgered txns excluded (`::test_all_ledgered_is_green`, incl. low-rating) | n/a | ✅ `test_os_card_registration` ×2 |
| Drain posts backfill (paced) | ✅ Surface (drain-stall + throttle tests, shipped) | n/a | n/a | manual: real read-back | n/a | ✅ shipped |

**Phase map** (P0 tests red first, per this skill):
- **P0** — all 🔴 above + these TESTS rows, both repos + creative_agent. 3-layer isolation for `REVIEWS_INGEST_WATERMARK_PATH` (autouse conftest BOTH repos + `DUCK_TEST_MODE` FROZEN guard modeled on `gsc_search_demand.write_search_demand` + pollution-audit test).
- **P1 — ✅ SHIPPED 2026-07-08 (duckAgent 42fbae9):** `generate_daily_review_summary` now loops `positive_reviews` (rating ≥ 4); rating-aware fallback; handoff builder carries true rating with no 5★ re-filter (verified). 5 P0 tests (red→green) + 13 regression pass. *Remaining verify:* confirm a real 4★ reaches `publish_candidates.json` on the next daily reviews run (regression canary).
- **P2** — duckAgent: watermark catch-up window across the 3 `days_back=1` call-sites + pagination.
- **P3** — duck-ops: **verify riskiest-unknown from logs first**, then decouple `auto_enqueue_publish_ready` from the pacing-wedge-prone `drain_queue` in `etsy_browser_batch._run_review_reply_batch` (enqueue result must persist even if drain defers); ensure enqueue runs before any archival sweep. **Tier-3 (launchd + browser).**
- **P4** — duck-ops: idempotent one-shot backfill (`scripts/backfill_review_replies.py`), dedup by tx, queues-not-posts (paced drain clears over ~a week).
- **P5 — ✅ SHIPPED 2026-07-26** as `review_reply_source_canary` (name keeps the `review_reply_` grouping prefix; spec name was `review_reply_ingestion_gap`). Comparison target is the replied-txn LEDGER (`state/reviews_replied_transactions.json`) — the first "handled" checkpoint — because feed-freshness/drain-stall/throughput already cover each downstream hop (one-hop blame radius per card). Data via `state/reviews_source_receipt.json` written at every fetch (per-txn `first_seen` carried across nightly rewrites so the 48h grace clock works; no live API from the viewer). RED on: unhandled txn >48h, `window_days<7` regression, receipt >50h stale, ledger missing. Completes the two-card bracket at the SOURCE. Producer: `flows/reviews/steps.py::_write_reviews_source_receipt` (3-layer isolated). Known blind spot (documented): a per-review silent drop INSIDE generate_daily_review_summary ledgers without drafting — covered by the P1 rating tests, not this card.

**By dimension:** unit (rating gate, watermark math, card count, dedup key); integration (mocked Etsy API + tmp state, collect→draft→feed→queue chain); e2e (backfill fixture, idempotent, no double-post); manual/Tier-3 (paced real posting, real read-back). **Golden fixture** `reviews_backfill_golden.json` (~10 reviews: 5★/4★/3★, one already-replied, one dup tx). **Regression canary:** "a 4★ review must appear in `publish_candidates.json` within one ingest run." **Tier-3:** P3 only (launchd/browser); backfill-queue + card are not.

**STATUS: P0+P1 SHIPPED 2026-07-08** (duckAgent `42fbae9` — 4★ now drafted); **P5 SHIPPED 2026-07-26** (source canary; the 14d-window+ledger fix of 50c24c1 also superseded P2's watermark concern). P2-P4 pending. Riskiest unknown (P3: did the 07-07 drain fix un-wedge 5★ enqueue?) unresolved pending next browser window.

---

## Surface 56 — Guard hard-ceiling counts local read-evals (drain self-throttles) (SPEC-FIRST, 2026-07-11, operator: "the drain posts 0 even though the batch runs")

**Root cause:** `etsy_browser_guard.before_command` computes `visible_count` as *every* non-`_LOCAL_ONLY` command, so **local DOM read-evals** (querySelector reads, textarea fill, the click-to-open-composer) count against `MAX_COMMANDS_PER_WINDOW=18` even though they generate **zero Etsy traffic**. One reply's ~6 setup evals × a few items hits 18 → `rate_limit_preemptive_cooldown` → the batch runs but posts 0 (observed 2026-07-11: 17 eval + 1 run-code = 18, backlog undrained). Third self-inflicted throttle on the review-reply lane, after the stale scheduler lock and the React-fill bug.

**Why the fix is low-risk:** `_is_mutating_command` **already** classifies the only evals that touch Etsy — navigation (`location.assign`/`window.location`, per `navigate_within_session`) and the submit (`submit.click(`/`.submit()`/`form.submit`) — as mutating. So the network-generating evals are already identified and already capped by `MAX_MUTATING_COMMANDS_PER_WINDOW=8`. The hard ceiling just needs to stop counting the *non*-mutating (read) evals.

**Change:** the hard burst ceiling counts **Etsy-facing** commands only = non-local commands that are either mutating (nav/submit/form-write) **or** a non-eval/run-code command; a non-mutating eval/run-code is treated as local for the ceiling. Add an explicit high **`TOTAL_COMMANDS_BACKSTOP` (60)** on *all* non-local events (incl. reads) so a runaway read-loop still trips a cooldown — replacing the runaway-backstop role the old 18-on-everything played. Re-key the soft `RESERVED_FOR_MUTATING` reservation to the backstop (reads no longer threaten the post budget, so the reservation only fires near the runaway limit). Also extend `_is_mutating_command` to inspect `run-code` content like `eval` (a `run-code` submit must still count).

**Effect:** `MAX_MUTATING=8` (posts+navs / 45-min window) becomes the true Etsy-facing governor — unchanged, so no rise in Etsy-visible volume. A page-1 reply costs 1 mutating (submit), a page-2/3 reply 2 (nav+submit) → ~4–8 replies/window instead of ~1; backlog of 14 drains in ~1–2 days across the 3 jittered windows. (If faster draining is wanted later, raising `MAX_MUTATING` is the lever — operator's call, separate change.)

## Use case × failure mode

|                                          | Happy | Read burst | Post burst | Runaway loop | Isolation |
|---|---|---|---|---|---|
| Drain does N read-evals, then a post     | 🔴 `test_guard_eval_budget::reads_do_not_block_post` (20 reads + post allowed) | 🔴 reads alone never cooldown | n/a | n/a | autouse tmp `STATE_PATH` |
| Post budget still capped                 | n/a | n/a | 🔴 `::mutating_cap_still_trips` (8 mutating → cooldown) | n/a | 〃 |
| Navigation eval counts as facing         | 🔴 `::location_assign_counts_mutating` | n/a | 🔴 navs count toward the 8 | n/a | 〃 |
| run-code submit counts                   | 🔴 `::run_code_submit_is_mutating` | n/a | 🔴 | n/a | 〃 |
| Runaway read-loop backstop               | n/a | n/a | n/a | 🔴 `::backstop_trips_at_total` (60 total → cooldown) | 〃 |

**Phase map:**
- **P0** — TESTS rows above (all 🔴 first). Guard change is pure-Python, deterministic, no browser → fully unit-testable; NOT Tier-3 (no launchd/publish; it *loosens* a self-throttle without raising Etsy-visible post volume, since `MAX_MUTATING` is untouched).
- **P1** — implement `_event_is_etsy_facing` + `TOTAL_COMMANDS_BACKSTOP` + re-keyed reservation + `run-code` mutating-classification; regression tests green.
- **P2 (optional, later)** — if the backlog must clear faster, raise `MAX_MUTATING` (operator decision; document the new Etsy-visible/day figure).

**By dimension:** unit only (guard is deterministic pure-Python over an in-memory event list); no integration/e2e/hardware. Isolation: autouse `conftest` already monkeypatches `STATE_PATH` per test (verify). Regression-class: "20 read-evals in a window must not block a post" is the permanent guard against re-introducing the read-starves-posts bug.

---

## Surface 57 — Drain resilience: one unfindable row halts the queue + lane budget contention (SPEC-FIRST, 2026-07-11, operator: "we started customer messages and ran out of actions before review-reply could run — and the budget numbers seem too low")

**Root cause (verified from the 2026-07-11 15:10 batch receipt, not inferred):** the batch runs drain-first and DID get its budget. It picked ONE queued item — `tx-5116124038`, a 2026-07-01 review — and `run_dry_run_fill` returned `ok=false` with *"Exact review row could not be found in the signed-in Etsy session. Auto-retrying later."* That is a **transient locate failure**, but `_result_is_retryable_throttle` only defers *pacing/cooldown* faults, so the drain classified it as a real failure → `failed_count += 1; if stop_after_first_failure: break` → **the whole drain halted on item 1**, never reaching the ~14 recent (07-05/06/07, page-1, easily-postable) items behind it. `0 posted, 1 failed`. THEN the customer-inbox sync ran, spent the shared 8-action mutating budget on message-thread navigations, and tripped the cooldown. Two independent defects; the review-reply one is primary (it quit before the budget mattered).

Sibling of the 11-day `pacing_cooldown` wedge (Fix A, 73f2096): that added throttle-deferral but **locate-failure is a different transient branch that wasn't enumerated** — the exact [[feedback_incomplete_fix_enumerate_all_branches]] trap.

**CORRECTION (2026-07-11, verified live — do not trust the first theory here):** a direct-DOM probe of the 3 recent queued items (tx 5110921538/5123940388/5118254996) found ALL of them on **page 1** with the **exact `respond-to-review` button present** and **not yet replied** — i.e. trivially postable (they were hand-posted successfully seconds later). Yet `run_dry_run_fill` reported "could not be found" for the same rows. So **"could not be found" is an executor locate/navigation bug on rows that ARE present**, NOT a buried-old-row problem. The original "old rows sink too deep" theory was wrong (classic [[feedback_verify_external_behavior_before_root_cause]] — theorized from the error string, refuted by the live page). This reshapes the fixes below: the PRIMARY fix is locate reliability, not ordering/retirement. Note it is INTERMITTENT — the executor located+posted two other page-1 rows earlier the same day (~13:00) and failed on these at ~15:30, so suspect navigation-timing / session-state (the session was left on `about:blank` by the prior batch's customer_read) rather than a static selector fault.

**Fixes (re-ordered after the correction above):**
0. **PRIMARY — fix the executor locate/navigation reliability.** `run_dry_run_fill`/`prepare_review_row_for_execution` report "could not be found" for rows that a direct `querySelector('li[data-review-region=tx]')` finds on page 1 with a live respond button. Investigate the executor's own navigate→locate path live (instrument `navigate_to_reviews_surface` + `locate_review_block`): verify it lands on the public reviews URL (not `about:blank`/a redirect), add a settle-wait + a "rows rendered?" assertion before declaring not-found, and retry-locate with backoff. Because it's intermittent (worked ~13:00, failed ~15:30 after the session sat on `about:blank`), first suspect: navigation didn't complete / page not settled before the locate ran. THIS is what actually blocked the drain, not ordering.
1. **Skip-and-continue past transient locate failures** (resilience, still needed). Even after #0, one genuinely-unfindable row must not halt the whole drain. Broaden the "defer, don't stop" set beyond `pacing_cooldown` to include `review_row_not_found` / "could not be surfaced" / dry-run `status=queued` auto-retry outcomes. Enumerate EVERY non-throttle, non-fatal branch of `run_dry_run_fill`/`run_live_submit` and decide break-vs-continue per class (a real auth/identity fault still stops; a "can't locate this one right now" skips to the next item). Do not blanket-continue on all failures — that would hide a genuine broken-session fault.
2. **Drain newest-first** (ordering heuristic, secondary — NOT the fix). Order `eligible` by review/submitted date descending so budget is spent on the most-likely-findable rows first. Reasonable hygiene, but the correction showed page-1 recent rows also failed, so this does not on its own unblock the lane.
3. **Retire dead rows** (hygiene). A row that returns not-found for K consecutive windows *after #0 is fixed* (or whose date is older than the page-walk reach ≈ `review_page_walk_max_pages × ~14`) is parked/dismissed, not retried forever. Same sweep catches the March `missing_target_identifiers` items still queued. (Apply only after #0 — otherwise the locate bug would wrongly retire postable rows.)
4. **Reserve the review-reply lane's budget.** Run the review-reply drain with a guaranteed mutating sub-budget the customer-inbox sync cannot consume (e.g. cap `customer_read` mutating spend, or reserve N mutating slots for review-reply when a review backlog exists). The lanes share `etsy_browser_guard`'s `MAX_MUTATING_COMMANDS_PER_WINDOW`; today the inbox sync can starve a review backlog.
5. **Budget re-tune (OPERATOR DECISION — the "numbers too low" question).** `MAX_MUTATING_COMMANDS_PER_WINDOW=8` per 45-min window (×3 windows = 24 Etsy-facing actions/day) is very conservative — a human seller replies to far more than 8 items in a sitting, and `MIN_MUTATING_GAP_SECONDS=3.5` (≥3.5s between mutating actions) is the actual anti-bot-detection control, not the raw count. Recommend raising to ~16–20/window while KEEPING the 3.5s gap + 3 jittered windows (those, plus session caps, are the real block-avoidance mechanisms). This is the genuine Etsy-risk lever, so it ships only with explicit operator sign-off and starts at the low end (16) with the liveness/throughput cards watched for any block signal.

## Use case × failure mode

|                                          | Happy | Locate-fail (transient) | Auth/identity fault | Budget contention | Isolation |
|---|---|---|---|---|---|
| Drain reaches postable items past a bad one | 🔴 `test_drain_skips_not_found_continues` (item1 not-found → item2 posts) | 🔴 not-found does NOT break the loop | 🔴 real auth fault STILL stops | n/a | autouse tmp queue |
| Ordering                                 | 🔴 `test_drain_newest_first` (07-07 before 07-01) | n/a | n/a | n/a | 〃 |
| Dead-row retirement                      | 🔴 `test_row_not_found_k_times_dismissed` | 🔴 K-th not-found → parked | n/a | n/a | 〃 |
| Missing-id rows                          | 🔴 `test_missing_identifiers_dismissed_not_retried` | n/a | n/a | n/a | 〃 |
| Lane budget reservation                  | n/a | n/a | n/a | 🔴 `test_customer_read_cannot_starve_review_backlog` (review reserve honored) | guard tmp state |
| MAX_MUTATING re-tune                      | 🔴 `test_mutating_cap_value` pins the agreed number | n/a | n/a | 🔴 gap still enforced at 3.5s | 〃 |

**Phase map:**
- **P0** — TESTS rows above (🔴 first). Fixes 1–3 are pure-Python drain-logic changes over an in-memory queue → deterministic, unit-testable, NOT Tier-3. **Fix 0 (locate reliability) needs a live browser to diagnose** (Tier-3 investigation) since it only reproduces against the real Etsy DOM/session — instrument + observe first, per [[feedback_verify_external_behavior_before_root_cause]].
- **P1 — PRIMARY: fix 0 (locate/navigation reliability).** This is what actually blocked the lane. Diagnose live (does navigate land on the reviews URL? are rows rendered before locate runs?), then add settle-wait + rendered-assertion + retry-locate. Then fix 1 (skip-and-continue) as the resilience backstop. Regression: a not-found item followed by a postable item → the postable one posts.
- **P2** — implement 2 (newest-first ordering) + 3 (dead-row retirement / dismiss missing-id + persistent-not-found, only AFTER fix 0).
- **P3** — implement 4 (lane budget reservation) in `etsy_browser_batch` + guard.
- **P4 (Tier-3, operator sign-off)** — 5: raise `MAX_MUTATING` to the agreed number; keep `MIN_MUTATING_GAP`; watch `etsy_browser_batch_liveness` + throughput for any block signal for a few windows before further raising.

**By dimension:** unit (drain break/continue per failure class, ordering, dismiss thresholds, budget reservation math); no integration/e2e (real posting stays manual/paced). Isolation: autouse conftest tmp queue + guard `STATE_PATH` (both already patterned). **Regression-class:** "a not-found item at the head of the queue must not block a postable item behind it" — the permanent guard against re-introducing the stop-after-first-failure wedge. **Observability:** the batch reported `status=failed` but the *reason* (drain quit on unfindable row, then inbox sync cooled down) was only visible by reading the receipt JSON — consider a drain-outcome breakdown on the liveness card so "tried 1, all not-found, stopped" is legible without log spelunking.

**STATUS 2026-07-11: fixes 0 + 1 SHIPPED.** Fix 0 (locate reliability) — `navigate_to_reviews_surface` now force-navigates to the canonical `SHOP_PUBLIC_REVIEWS_URL` (never trusts a reused session's page / never link-hunts on about:blank) + `wait_for_review_rows` polls until rows render (up to 8s) before locating. Fix 1 (skip-and-continue) — `_result_is_transient_locate_failure` deferrals no longer trip `stop_after_first_failure`; a genuine fault still stops. Tests: `test_review_reply_navigate_surface.py` (4) + `test_drain_locate_deferral.py` (3), full suite green (1062). Bridge DONE earlier: 5 recent 5★ replies hand-posted (tx 5123597926, 5121094714 via executor ~13:00; tx 5110921538/5123940388/5118254996 via direct-DOM, operator-approved, ~15:35). **P4 SHIPPED 2026-07-11 (operator sign-off):** `MAX_MUTATING_COMMANDS_PER_WINDOW` 8→16 (kept `MIN_MUTATING_GAP_SECONDS`=3.5 — the real anti-burst control) + `max_replies_per_session` 2→5. Pinned by `test_guard_eval_budget::test_mutating_cap_and_gap_pinned`. Confirmed working: the 19:45 evening window auto-located + posted tx-5117517288 (an old 07-01 review the pre-fix code failed on). **P2 (newest-first ordering) SHIPPED 2026-07-11:** `_review_recency_key` + `reverse=True` drains recent reviews first — they sit on page 1 (~1 mutating each) vs old rows that need deep page-walks (each pagination is a mutating `location.assign`, so an old post can cost ~4 of the budget). This is ALSO a budget-efficiency win, not just relevance. Pinned by `test_drain_orders_newest_review_first`. **P3 (lane budget reservation) DEFERRED pending observation:** the 19:45 1-post cap was largely the expensive old-review page-walk (now fixed by P2) under the old 8-budget (now 16 via P4). P2+P4 likely resolve the customer-inbox contention — so watch the next windows before building P3; only build it if a window still gets starved with real page-1 posts. **Dead-row/missing-id retirement** also deferred (low value now: skip-and-continue already prevents dead rows from blocking, and newest-first drains them last). **Verify:** next windows should post up to ~5 page-1 reviews/window and clear the backlog.

---

## Surface 58 — Customer-Ask Scout: mine inbox + review text into the product-concept queue (SPEC-FIRST, 2026-07-11, operator: "are we leveraging all the information we have?")

**Gap (verified by 2026-07-11 audit):** the product-concept pipeline has exactly 4 scouts (trend / build_next_promotion / competitor_motif / strategy_idea) and `docs/PRODUCT_CONCEPT_PIPELINE.md` *explicitly* states reviews + inbox "do not feed this queue." So the highest-intent demand signal we own — a customer literally asking us to make something we don't sell, across ~168 inbox threads + every review — is unmined, even though the entire downstream (concept queue → Studio → Promote gate → brief-approval) is already built. This is a "turn a dead asset into decisions" win with no new downstream plumbing.

**Design (reuse existing infra; add ONE new scout + source):**
- **Producer** `customer_ask_scout.py`: scans the already-collected customer inbox state + review text, extracts explicit product REQUESTS ("do you make a &lt;X&gt; duck?", "can you do &lt;team/breed/theme&gt;?") and mints `product_concept_queue` candidates with a new `source_type="customer_ask"` alongside the 4 existing scouts. Runs on a launchd cadence (producer/reader pattern).
- **LLM-output-surface recipe (all 5 pieces, per the top-level CLAUDE.md rule):** (1) versioned `config/customer_ask_taxonomy.json` (request-vs-not categories, `*_version`); (2) golden fixture `tests/fixtures/customer_ask_golden.json` (hand-labeled asks vs non-asks vs ambiguous); (3) gated eval `scripts/eval_customer_ask.py` (real API, promotion criteria, NOT in default pytest); (4) **needs_review escape** — an ambiguous ask is flagged for the operator, never silently minted; (5) deterministic keyword cross-check ("do you make/sell/have", "can you", "wish you had", "looking for") to catch confidently-wrong LLM output. **Classification runs at temperature 0.**
- **Own-mail exclusion** ([[feedback_mailbox_detectors_exclude_own_mail]]): strip the system's OWN outbound (MJD:/FLOW:/ACTION:/RUN: markers) + Etsy notification emails BEFORE classifying; pin false-positives in the golden fixture.
- **Frequency weighting:** aggregate similar asks — N distinct customers requesting the same subject is a strong signal; a one-off is weak. Rank customer-ask candidates by distinct-requester count.
- **Dedup:** reuse the pipeline's 3 dedup layers so an ask for something already in the catalog / concept queue does not mint (guard against the known slug-merge fragility for non-Build-Next scouts, [[reference_product_concept_pipeline]]).
- **Gates unchanged:** customer-ask concepts flow through the identical Promote (no-credits) + brief-approval (credits) gates — no auto image/3D spend.

## Use case × failure mode

|                                          | Happy | Ambiguous ask | Own-mail / notification | Dup of catalog | Isolation |
|---|---|---|---|---|---|
| Explicit request → concept minted        | 🔴 golden: "do you make a corgi duck" → candidate | 🔴 unsure → needs_review, NOT minted | 🔴 MJD:/Etsy-notif excluded before classify | 🔴 already-sell → suppressed | 🔴 3-layer prod-path (both repos) |
| Frequency ranking                        | 🔴 3 requesters > 1 requester | n/a | n/a | n/a | 〃 |
| Deterministic cross-check                | 🔴 LLM "yes" + no ask-keyword → flagged | n/a | n/a | n/a | 〃 |
| Two-card bracket                         | 🔴 input: scanner finds asks (not empty) | n/a | n/a | 🔴 output: asks reach queue / acted on | registration + empty-payload |

**Phase map:** **P0** TESTS rows + golden fixture (spec-first). **P1** producer + `source_type=customer_ask` + temp-0 classifier + own-mail exclusion + 3-layer isolation. **P2** frequency ranking + dedup reuse. **P3** gated eval with promotion criteria; two-card bracket (input filter-sanity + output throughput). Plan-agent finding: the input is ONE already-normalized duck-ops file (`state/normalized/customer_signals.json`, 776 items) and the LLM helper is duck-ops-local (`llm_call_helpers.call_openai`) — so this is **intra-repo**, not cross-repo. By dimension: unit (classifier mock, keyword cross-check, dedup, frequency); gated eval (real API); no e2e.

**STATUS 2026-07-11: P0 + P1-producer + P2 SHIPPED.** `config/customer_ask_taxonomy.json` (v1) + `tests/fixtures/customer_ask_golden.json` + `runtime/customer_ask_scout.py` (pure `scan_customer_asks` + I/O `run()`, temp-0 `_llm_classify`, own-mail exclusion `is_own_system_mail`, deterministic cross-check → needs_review, frequency-by-distinct-requester ranking, DUCK_TEST_MODE write guard) + `tests/test_customer_ask_scout.py` (6) + `tests/test_no_test_pollution_in_customer_ask.py` + conftest redirect. Full suite green (1072). **`_customer_ask_items()` feeder SHIPPED** — `product_concept_queue.py` now wires candidates → queue with `source_type=customer_ask`, `brief_source=customer_ask`, canonical `_clean_theme` (collapses with a trend/competitor "corgi" via `_merge_duplicate_themes`), frequency-weighted score (1→0.7, 2→0.8, 3+→0.9), fail-closed guardrail; `PRODUCT_CONCEPT_PIPELINE.md` updated to 5 scouts. End-to-end: scout → candidates file → feeder → queue → existing Promote/brief gates (no auto-spend). **Remaining:** **P3** `scripts/eval_customer_ask.py` gated real-API eval + promotion criteria + two-card bracket (input filter-sanity + output throughput); `catalog_match.find_existing_matches` for real `catalog_status` (currently hardcoded "gap"); operator launchd plist for the producer cadence (Tier-3 ask). Minor debt: `is_own_system_mail` duplicates the marker list in `customer_interaction_cases.py` — extract to a shared predicate.

## Surface 59 — LLM Cost-Ceiling Enforcement: the stop, not just the log (SPEC-FIRST, 2026-07-11)

**Gap (verified):** per-flow cost *logging* + `/portal/intel/cost` + a soft daily *alert* are shipped (Surface 10), but there is **no hard daily cap, no per-flow cap, and no auto-stop** — a deliberate "observability-first" call (2026-06-06). That call is now higher-risk: lanes run unattended, so one misbehaving loop can quietly spend $100 overnight with nothing to halt it. Open work = **enforcement only** (do NOT rebuild the dashboard).

**Design:**
- **Pre-call budget gate** in the shared LLM helper (`runtime/llm_call_helpers.py` / `call_openai` / `call_anthropic`): before each call, read today's total spend + this flow's spend from a fast running counter (seeded from the `llm_call_log` rollup); if `today >= DAILY_CAP_USD` or `flow_today >= PER_FLOW_CAP_USD`, **raise `BudgetExceededError` instead of calling.** Fail-CLOSED on the spend (don't call), fail-LOUD (OS-card signal + log line), never silent ([[feedback_swallowed_errors_lie.md]]).
- **Config as data:** `config/llm_budget.json` (`daily_cap_usd`, `per_flow_caps`, `version`) — calibrate caps to observed spend + generous headroom so a normal day never trips (a cap that fires on legit work is worse than none).
- **Explicit override, never silent:** an operator flag / env to lift the cap for a known-heavy run, recorded in a receipt.
- **Two-card bracket:** input filter-sanity (is the gate wired + reading real running spend, not a stale/zero counter?) + output (did a flow actually stop when over cap? is daily spend within cap?).

## Use case × failure mode

|                                    | Under cap | At daily cap | At per-flow cap | Counter stale/zero | Override |
|---|---|---|---|---|---|
| LLM call proceeds / stops          | 🔴 under → call proceeds | 🔴 over daily → BudgetExceededError, no call | 🔴 over flow → stop that flow only | 🔴 counter unreadable → fail-closed + loud (not fail-open) | 🔴 override lifts cap, records receipt |

**Phase map:** **P0** TESTS rows (spec-first). **P1** budget gate + `config/llm_budget.json` + `BudgetExceededError` + OS-card signal. **P2** override path + receipt. **Cross-repo** (the helper is used by both repos) → Plan agent before implementing. Calibrate caps against the shipped `llm_cost_summary` history first. By dimension: unit (mock spend levels → assert proceed/stop, fail-closed on unreadable counter); no e2e. **STATUS: SPEC-ONLY 2026-07-11.** Note: the operator chose observability-first once before — confirm the ceiling values before shipping so it protects without throttling legit work.

---

## Surface 60 — Shopify SEO near-duplicate title: no-op "fix" (2026-07-13, operator: "the recommendation looks exactly like the input, right?")

**Root cause (verified in `shopify_seo_review.py`):** the audit correctly detects `near_duplicate_seo_title` ("avoid blending into another similar search result"), but (1) the finding was **never fed into the generation prompt** — `_generate_proposals` prompted the LLM on each resource *in isolation* with no sibling context and no differentiation instruction, so for two near-identical pages (`/collection-bundles` "Mix **&** Match" vs `/collection-bundle` "Mix **and** Match") it echoed near-identical titles; and (2) **no guard checked proposed ≠ current** — `_title_needs_fallback` only checks empty/placeholder/length, so an echo of the current title (in-range length) passed straight through as "SEO title action", identical to the current title. The prior "privacy-choices special-case" was a one-page band-aid for the same general bug. ([[feedback_plausible_fallbacks_mask_failure]].)

**Fix (SHIPPED 2026-07-13):**
1. **Prompt injection** — `_generate_proposals` computes `_title_differentiation_groups` (resources sharing a normalized current title; `&`→`and`, case/space-insensitive) and adds a CRITICAL DIFFERENTIATION rule listing the colliding ids+titles, instructing each `seo_title` be distinct from its group and its own current title.
2. **Fail-closed guard** — `_flag_title_differentiation` post-pass: a near/dup-title item whose proposal echoes the current title OR still collides with a sibling's proposal is flagged `title_needs_differentiation` and `apply_seo_title=False` (never auto-applied); the email shows "NEEDS MANUAL DIFFERENTIATION" instead of the no-op echo.
3. **Consolidation flag** — two near-identical PAGES get `consolidation_candidate` + an email note ("consider merging + 301-redirecting one") since the real fix is a redirect, not a retitle.

## Use case × failure mode

|                                    | Distinct proposal | Echo of current | Sibling collision | Prompt |
|---|---|---|---|---|
| Near-dup title pair                | 🟢 `test_distinct_proposal_passes_without_flag` (no flag) | 🟢 `test_echo_proposal_flags_needs_differentiation_and_does_not_apply` (flagged, not applied, consolidation) | 🟢 covered by echo/collision guard | 🟢 `test_prompt_includes_differentiation_instruction` |

**By dimension:** unit (mocked `_generate_proposals` + captured prompt); deterministic guard (no LLM). **STATUS: SHIPPED 2026-07-13** — `runtime/shopify_seo_review.py` + `tests/test_shopify_seo_review.py` (3 tests), full suite green (1075). **Follow-up (P2, not built):** golden fixture + gated real-API eval that a near-dup pair produces *distinct* titles (LLM-differentiation accuracy), per the LLM-output-surface recipe.

---

## Surface 61 — SEO generator gets the LLM-output-surface recipe (golden fixture + gated eval + quality cross-check) (2026-07-14, operator: "can we improve design of our seo agent")

**Gap:** unlike `theme_classifier` (full 5-piece recipe), the Shopify SEO generator wrote LLM copy straight to Shopify with only length/placeholder fallback checks — no golden set, no gated eval, no promotion threshold, weak deterministic cross-check. It could silently regress and nothing would catch it.

**Fix (SHIPPED 2026-07-14):**
- **Deterministic quality cross-check** `_seo_title_quality(title, subject)` in `shopify_seo_review.py` — checks length 45-70, subject presence (core concept retained), keyword-stuffing (`duck`>3, brand repeat), multi-separator, placeholder. Stronger than the length-only `_title_needs_fallback`; catches confidently-wrong LLM output.
- **Golden fixture** `tests/fixtures/shopify_seo_golden.json` — products/pages incl. a near-dup pair, each with a `subject` constraint. Regression rule: append every real miss.
- **Gated eval** `scripts/eval_shopify_seo.py` — runs the REAL LLM on the golden set, scores with `_seo_title_quality` + a dup-differentiation check, gates at **≥90% quality pass AND 100% dup-group distinctness**, exit 0/1, NOT in default pytest.
- Unit tests pin the scorer (good/short/off-subject/stuffed/placeholder).

**By dimension:** unit (deterministic scorer, 5 cases); gated eval (real API, manual). **STATUS: SHIPPED 2026-07-14.** **Live guard SHIPPED 2026-07-16:** `_flag_low_quality_titles` runs `_seo_title_quality` on the FINAL proposed title (post-fallback) and routes clear-cut failures (`_LIVE_QUALITY_ISSUE_CODES` — length/placeholder/stuffing/brand-repeat/multi-separator; excludes the heuristic `subject_missing` to avoid false-positives) to `title_needs_review` + `apply_seo_title=False`; email shows "NEEDS REVIEW (…issues)" instead of shipping a plausible-but-bad title or a silent canned fallback. 2 build tests (stuffed→needs_review+not-applied; good→applies). Suite green (1082). **Remaining follow-up (P2):** close the OUTCOME loop — per-page GSC clicks before/after an apply (open-loop gap from the roadmap audit).

---

## Infra note — launchd StartInterval → StartCalendarInterval migration (2026-07-14)

The nightly ~01:36 wedge (see memory `reference_etsy_browser_scheduler` / `feedback_verify_the_job_is_firing_first`) killed EVERY `StartInterval` LaunchAgent, not just the review-reply checker — `system_health_refresh` (the operator health dashboard) was frozen 07-12 01:36 → 07-14, so the RED batch-liveness card never surfaced. Migrated all 7 to `StartCalendarInterval` (or `KeepAlive` for whatsapp-bridge's 20s service); backups at `~/Library/LaunchAgents/*.plist.bak-20260714b`. Verified firing (system_health.json refreshed on load; checker at runs=45/day). launchd is machine-local (not git) — this is a documented operator state, not a committed change.

---

## Surface 62 — newduck Etsy description: LLM-written, tone/occasion-aware, SEO-front-loaded, quality-gated (SPEC-FIRST, 2026-07-16, operator: "this description needs to be SEO friendly" + the operator-direction leak)

**Context:** the operator-direction *leak* is already fixed (2f16ba7 — it's steering-only now). This is the separate quality upgrade. Plan-agent correction: the newduck description is NOT purely deterministic — `newduck_generate_ingredients` (temp 0.5) + `_newduck_harden_generated_copy` (temp 0.2) already LLM-write the prose; the tone-deaf output ("Breast Cancer Fighting **fans**", raw `visual_summary` image-caption opener) comes from the **deterministic FALLBACK** (`_fallback_newduck_ingredients`) winning when a policy-rebuild triggers (`should_prefer_deterministic`). So three prongs: (a) prompt awareness, (b) fallback parity, (c) quality gate.

**Design (decisions baked in — operator may override):** LLM-write **only the body prose paragraphs** (shared by Etsy body + Shopify `body_html`); keep the SEO **meta** description, GIFT IDEAS / OCCASIONS / SPECS / disclaimer / links / tags **deterministic** (they are SEO-POSITIVE — keyword surface + trust — and must be retained). Temp stays ≤0.5. Sensitivity via the existing free-text occasion `angle` (no cross-repo calendar change). Fields: `operator_direction` (steering, never echoed) + an occasion/tone line feed the prompt; front-load the subject + core keyword in sentence 1.

**SEO standards (Etsy + Shopify — the validator enforces these length/placement bounds; a violation routes to operator review, never publishes):**
- **Etsy title:** ≤140 chars, primary keyword in the first ~40 (Etsy weights title + front words heaviest).
- **Etsy tags:** exactly 13, ≤20 chars each, multi-word phrases (Etsy's #1 search lever — use all 13).
- **Shopify SEO/meta title:** 50–60 chars (Google truncates ~60).
- **Shopify meta description:** 150–160 chars (Google truncates ~155) — stays deterministic (`build_newduck_seo_description`).
- **Description BODY prose (the LLM part):** first ~160 chars keyword-front-loaded (= the Google/Etsy snippet zone), then **~150–250 words** of prose. `too_short` (<~120 words) and `too_long` (>~300 words) are quality codes.
- **Full listing (prose + sections):** ~250–500 words, scannable; primary keyword appears **2–3× naturally** + related long-tail (occasion / audience / material) across body + the deterministic sections.
- **Front-load rule (highest-weight):** exact subject + core keyword in **sentence 1** — `keyword_not_frontloaded` blocks. Full listing shape stays: `[LLM prose ~150-250w, front-loaded, shop voice, tone-aware]` → GIFT IDEAS → PERFECT OCCASIONS → PRODUCT SPECIFICATIONS → internal collection + external reference links → 3D-print disclaimer (all deterministic).

**Voice examples (gold, operator-approved 2026-07-16):** 3 exemplar prose bodies define the shop voice for the system prompt — a persona/humor case, a plain-subject case, and the sensitive cause case (breast-cancer duck: supportive/empowering, NO "{theme} fans", no medical/charity claims). Voice = persona + one evocative beat, keyword-front-loaded, audience nod without stuffing. These become the few-shot examples; do NOT mine existing auto-generated descriptions (they carry the "{theme} fans" bug).

**Build — SHIPPED 2026-07-16 (simplified per operator "are we over complicating this?"):** the operator endorsed the SIMPLE best-practice version — one voice-driven LLM call + a thin deterministic validator routing failures to the EXISTING human email review — over the full versioned-config/gated-eval recipe. A human already reviews every newduck listing, so the golden-fixture + gated-CI-eval (original P4) was **intentionally dropped**; the mocked prompt-construction tests + validator unit tests carry the coverage.

1. ✅ **Deterministic quality gate** `helpers/seo_rules.py::newduck_description_quality(...)` + `NEWDUCK_DESCRIPTION_BLOCK_CODES` (mirrors `_seo_title_quality`/Surface 61). Block codes: `naive_fans_slot` (gated on `newduck_is_sensitive_theme` — hobby "{theme} fans" stays allowed), `raw_caption_opener`, `operator_direction_echo`. Advisory: `keyword_not_frontloaded`, `too_short/long`, `empty`. (`subject_missing`/`color_missing` already covered by the audit's existing visible-colors + listing-name-in-sentence-1 checks.) Folded into `_audit_newduck_copy_package` → `copy_review` blockers/warnings (reuses the 2-step email gate). Commit e9ecfed.
2. ✅ **Fallback parity** — `_newduck_theme_family` gains an `awareness` family (via `newduck_is_sensitive_theme`, checked first); `_fallback_newduck_ingredients` gets respectful awareness audiences/gifts/occasions/tags and drops the `f"{theme_phrase} fans"` lead for sensitive themes (kept for hobby). Commit 8ff49d4.
3. ✅ **Voice-driven prompt** — `newduck_generate_ingredients` now carries 3 operator-approved voice exemplars (persona/plain/sensitive) + explicit SEO rules (front-load duck name in sentence 1, ~150-250-word body, tags ≤20 chars) + `operator_direction` as steer-don't-echo + cause-sensitivity guardrails (no "fans", no medical/charity claims). Commit e847348.
4. **Docs** — this surface. MASTER_IMPLEMENTATION_PLAN pointer pending.

**Deliberately deferred (not blocking):** (a) golden fixture + gated real-LLM eval — dropped; the human email review + validator is the gate for a human-reviewed surface; (b) `occasion_copy_tone_line()` into the body prose — the occasion nod already flows through tags/meme/jeepfact consumers; folding it into newduck prose is an incremental enhancement, revisit if descriptions read occasion-blind.

## Use case × failure mode

|                                    | Happy (grounded, keyword-led) | Cause/sensitive theme | Operator-direction | Fallback wins |
|---|---|---|---|---|
| Body prose quality                 | ✅ mocked: front-load rule in prompt | ✅ mocked: sensitive guardrails in prompt | ✅ mocked: steers, NEVER echoed | ✅ unit: fallback not tone-deaf (parity) |
| needs-review routing               | n/a | ✅ unit: naive-fans → block | ✅ unit: echo → block | ✅ unit: naive-fans → block |
| Deterministic quality fn           | ✅ unit: each code | ✅ unit: cause case | ✅ unit: echo detection | n/a |

Tests: `duckAgent/tests/test_newduck_description_quality.py` (12: validator codes, fallback parity, prompt construction) + the corrected `test_newduck_publish_seo.py` audit tests. **STATUS: SHIPPED 2026-07-16** (e9ecfed → 8ff49d4 → e847348). Original 5-decision confirmation moot — operator picked the simple version live.

---

## Surface 63 — SEO outcome loop: per-page GSC before/after for Shopify SEO applies (SPEC-FIRST 2026-07-26)

**Background:** writeback receipts prove edits STUCK (`shopify_seo_writeback/receipts/`); `shopify_seo_outcomes.py` proves they STAYED (audit re-check) but every item carries a hardcoded `traffic_signal: available=false` stub. Nothing proves the edits *worked*. This surface fills that stub: `runtime/seo_outcome_intel.py` joins each receipt's page to GSC per-page data in 28d windows anchored to the receipt's `verified_at` (+3d settle gap, +3d GSC lag). **Shopify domain only — Etsy is invisible to GSC (GA4's job).** Verdicts are deterministic: impressions % + position drive them, clicks never do (~8 clicks/day sitewide = noise); incomplete windows are `pending` with days_remaining, never "no effect"; thin volume is `low_data`, honest. Cohort batching = ≤2 GSC calls per apply-date cohort (~33 calls/weekly run). Dead-token mode writes `available:false` + the FULL roster as `unmeasured` with live countdowns — page useful before re-auth; verdicts light up on the first authenticated run (Apr–Jun cohorts' windows are already complete).

| Use case | Happy | Token dead | Zero GSC rows | Window incomplete | Bad receipt |
|---|---|---|---|---|---|
| receipt roster (latest-per-resource, superseded, unjoinable) | ✅ unit | n/a | n/a | n/a | ✅ missing resource_url → unjoinable |
| window math anchored to verified_at (28/3/3) | ✅ frozen-today unit | n/a | n/a | ✅ pending + days_remaining, never flat | ✅ unparseable date skipped |
| cohort batching ≤2 GSC calls/cohort | ✅ call-recording mock | n/a | n/a | ✅ after-call skipped pre-window | n/a |
| join GSC page rows ↔ receipt paths | ✅ mocked per-window rows | n/a | ✅ zeros → no_data/low_data | n/a | n/a |
| verdicts (impr%/position; clicks never drive) | ✅ table-driven unit | ✅ unmeasured | ✅ low_data honesty | ✅ pending | n/a |
| collect() degrade | ✅ available:true | ✅ available:false + full roster, exit 0 | ✅ | ✅ | ✅ |
| write guard (3-layer) | ✅ tmp write | n/a | n/a | n/a | ✅ FROZEN TestModeRefusalError + pollution audit |
| /portal/intel/seo-outcome page | ✅ full fixture | ✅ re-auth-pending roster state | ✅ empty-state | ✅ pending section | ✅ malformed state |
| /api/seo-outcome-intel + Desk tile | manual: smoke curl after deploy | n/a | n/a | n/a | n/a |

**Phase 2 (gated on real verdicts existing after re-auth):** fill `shopify_seo_outcomes.py` traffic_signal stub from this intel by resource_id; splice a fail-soft track-record line into `shopify_seo_review._generate_proposals`. Cadence: weekly launchd Sun 07:20 (Tier-3 install).

## Surface 64 — duckvideo promotion lane: automated product Reels from the real 3D models (SPEC-FIRST 2026-08-07, operator: "for the video stuff, i dont want to do extra work, so can we do anything with AI?" → "you can build this and automate?")

**Background:** first video promotion lane. Turntable renders (Blender 5.2 LTS headless, installed 2026-08-07 with ffmpeg 8.1.2) of the ACTUAL production models — `paint-to-print-3d/outputs/*/selected/region_materials.obj+.mtl` (anchor: `handoff_manifest.json`) and Studio `3d_lab` GLBs — with automatic ken-burns fallback over Shopify CDN photos when no model matches. Hook line overlaid via PIL+ffmpeg; MP4 hosted on Shopify Files (`stagedUploadsCreate resource:VIDEO` → `fileCreate contentType:VIDEO` → poll `Video.sources[].url`); published as IG Reel (`media_type=REELS` + container-status poll) through the EXISTING `social_publish_queue` + `social_publish_due` sidecar. New flow `duckvideo` mirrors meme's email approval (`MJD: [duckvideo] … ACTION:review` → reply "duckvideo publish"); policy hardcoded `approval_gated` — never auto without explicit operator promotion. New prod state: `duck-ops/state/product_model_index.json` (product_id-keyed; deterministic token join via `catalog_match` primitives; `needs_review` escape, never a silent guess). Two genuinely-uncertain integrations (Shopify staged VIDEO target shape; Meta REELS container behavior) are gated-live-eval'd BEFORE the lane depends on them.

| Use case | Happy | No/ambiguous model match | Render fails | Upload/transcode fails | Meta container ERROR/timeout | Old-entry migration | Test pollution |
|---|---|---|---|---|---|---|---|
| product→model index scan+join (`duck-ops/runtime/product_model_index.py`) | unit: fixture tree of manifests+catalog | unit: `needs_review` + `unmatched_models`/`products_without_model` buckets, never silent guess | n/a | n/a | n/a | n/a | 3-layer SAME COMMIT: autouse conftest in BOTH repos + FROZEN-path guard + post-suite audit |
| turntable render (`scripts/render_turntable.py` + `duck_video_helper.render_turntable_video`) | unit: blender cmd construction; **gated eval `scripts/eval_duckvideo_render.py`**: real render of a real paint-to-print output, probe 1080x1920 h264 | n/a | unit: `status:"failed"` + stderr tail, never raises | n/a | n/a | n/a | outputs under `runs/<run_id>/assets/` (gitignored) |
| ken-burns fallback (`render_kenburns_video`) | unit: ffmpeg cmd, mocked downloads | unit: auto-selected when index has no model | unit: turntable-fail → kenburns + `fallback_reason` recorded | n/a | n/a | n/a | n/a |
| hook gen + validator (`generate_video_hook`/`validate_hook_line`) | unit: validator table (word cap, charset, no hashtags, no material claims) | n/a | unit: 1 regen then deterministic template — lane never blocks | n/a | n/a | n/a | n/a |
| Shopify video hosting (`shopify_helper` VIDEO fns) | unit: mocked graphql both target shapes (S3-params / empty-params PUT); **gated eval `scripts/eval_shopify_video_upload.py`**: real 1s mp4 → READY → CDN URL | n/a | n/a | unit: poll timeout → None → step fails loud | n/a | n/a | n/a |
| Reels publish (`meta_helper.schedule_instagram_reel_post`) | unit: mocked container→status poll→publish; queue-routing branch parity with image path | n/a | n/a | n/a | unit: `ERROR`/`EXPIRED` → raise w/ status detail; timeout → raise (sidecar marks attempt_failed) | n/a | n/a |
| queue `media_type`/`video_url` extension | unit: reel entry enqueue+validate (reel requires video_url) | n/a | n/a | n/a | n/a | unit: entry WITHOUT `media_type` reads as `"image"` (all existing entries) | existing frozen-dir guard covers |
| sidecar reel branch (`social_publish_due`) | unit: mocked-Meta publish both media types + `--dry-run` | n/a | n/a | unit: attempt cap → `failed` (existing machinery) | unit: receipt `content_type="reel"`, asset_url fallback to video_url | n/a | n/a |
| duckvideo flow steps + approval (`flows/duckvideo/steps.py`, `main_agent`, `duck_flows` FlowSpec) | unit: state-key contract, policy gating (approval_gated, auto_apply refused), email FLOW/RUN/ACTION metadata | unit: prepare picks photo-mode product when no model candidates | unit: render-fail run still reaches email (kenburns) | n/a | n/a | n/a | n/a |
| two-card bracket (viewer: input sanity + throughput) | unit: loader tests + **registration test incl. empty payload** ([[feedback_loader_and_registration_both_need_tests]]) | input card red: index stale >48h / zero candidates / blender missing | output card red: approvals without posted receipts in 14d | n/a | n/a | n/a | n/a |
| first live Reel end-to-end | **manual:supervised (Tier-3):** trigger flow → approve by reply → sidecar posts → verify IG post + `duckvideo_posts.json` receipt + both cards green, dated entry here | n/a | n/a | n/a | n/a | n/a | n/a |

**Ship gates:** the two gated evals MUST pass live before Phase 5 wires the flow (risk order: Shopify VIDEO upload first). Launchd (weekly duckvideo run + daily index producer) is Tier-3, prepared-plist pattern.

**Wiring-audit addendum (2026-08-08, operator: "did we add all areas in our duck operations site?"):** post-ship audit found 3 real gaps despite following the meme pattern — (1) NO workflow-control transitions → Business Desk blind to the lane (fixed: `_record_duckvideo_transition` at email/scheduled/published/failed + conftest caller-module entry; coverage: existing flow tests exercise the no-op-patched path), (2) viewer_data friendly-description set missing duckvideo (fixed, cosmetic), (3) roadmap not updated at ship (fixed). Social-performance collection needed NO fix — the collector globs all `runs/*/*_posts.json`. Root cause: the new-flow checklist lived only in session memory → codified as the `/new-flow` skill (wiring checklist + SLO/OS-card design standards). Music+backdrop upgrades (same day): eof_action=endall duration regression caught by the eval's duration check; contact-shadow + photo-backdrop path covered by `TestPickers` in test_duck_video_helper.py.

**Build status (2026-08-07):** P1 ✅ 4138972/8c03740 (index producer + 3-layer, 12 tests; real dry-run: 2 matches both flagged needs_review, honest buckets). P2 ✅ 213705d + 8add188 (22 tests; **render gated eval PASSED live** — real 7s 1080x1920 h264 Highland Cow turntable; eval caught 2 real Blender-5.x bugs: Action.fcurves removed, and Blender exits 0 on script exceptions without --python-exit-code). P3 ✅ ebee233 (11 tests; **Shopify VIDEO live eval NOT yet run — Tier-3, blocks first live post**). P4 ✅ 1eb5e84 (11 tests + 33 regression green). P5 ✅ 3bb31ee/45a3d92/b9a710c (13 flow tests + 20 card tests + registration; approval hole in the --all runner closed — fn(st,force=) gets awaiting_approval, only email-reply/CLI pass operator_approved=True). P6 pending Tier-3: Shopify video live eval → plist installs (duckvideo Wed 09:30, index daily 07:35; prepared_plists/) → supervised first Reel. NOTE: 7 pre-existing order-dependent full-suite failures (thursday_funnel×4, weekly_sale_history×1, workflow_freshness×2) reproduce at clean HEAD — NOT from this surface; separate follow-up.

## Surface 64b — duckvideo enhancements: multi-duck montage + local cutout-on-backdrop (SPEC-FIRST 2026-08-09, operator: "should the photo based ones have a background? ... fade in and out for other ducks")

**Design:** montage = photos-only collection reel (3-5 ducks, per-duck scrim labels, one hook/one approval; turntable renders stay the single-duck format — operator-confirmed split). Selection ladder occasion→trending→theme, strategies NEVER mix; trending source = `trend_candidates.json` `signal_summary.trending_score`/`sold_last_7d` via `catalog_match.matching_products` (`demand_intel.json` REJECTED — stale since 07-03). Mode: `DUCKVIDEO_MODE=single|montage|auto`, auto = even-ISO-week proposes montage, data disposes (≥3 ducks or single fallback with recorded reason). Cutout: reuse meme lane's local rembg (`studio_image._rembg_remove_background`, u2net, ~0.4s warm, models cached in ~/.u2net) — deterministic mask gate (alpha coverage 0.15-0.90, bbox sanity), ALL-or-nothing per video, fallback to blurred-self with `cutout_fallback_reason` ([[plausible fallbacks]]). Kill switches: `DUCKVIDEO_CUTOUT=off`, `DUCKVIDEO_MODE=single`.

| Use case | Happy | Starved/ambiguous selection | Cutout degenerate | Render fails | Mode/env override | Old-run migration | Test pollution |
|---|---|---|---|---|---|---|---|
| montage selection (`flows/duckvideo/selection.py`) | unit: fixtures → ladder picks occasion→trending→theme, scores+why recorded | unit: every strategy <3 → strategy None → single fallback + reason; strategies never mix | n/a | n/a | unit: DUCKVIDEO_MODE table + ISO-parity, deterministic | unit: `_recently_video_product_ids` reads BOTH `product.id` and `products[].id` | autouse conftest redirect for TREND_CANDIDATES/CATALOG_INDEX paths (3-layer, same commit) |
| montage render (`render_montage_video`, `_scrim_text_card`) | unit: frame math, label baked per segment, hook refactor stays green | unit: <2 segments → failed, never raises | all-or-nothing spans segments | unit: montage fail → single kenburns + `montage_failed:` reason | n/a | n/a | frames under runs/ (gitignored) |
| local cutout (`cutout_product_image`, `evaluate_cutout_mask`) | unit: mocked rembg → composite at contact-shadow line, renderer=kenburns_cutout; **gated eval: real rembg + montage probe** | n/a | unit: gate table (coverage low/high, empty bbox) → ALL blurred-self + reason | unit: rembg exception → (None, meta) → fallback, never raises | unit: DUCKVIDEO_CUTOUT=off → legacy path | n/a | rembg mocked in units; ~/.u2net machine-local |
| flow + email + contracts | unit: montage state keys (`products` + compat `product`), email lists ALL ducks+why, ONE approval, publish/FlowSpec untouched (existing 64 tests green = the proof) | unit: montage-starved → single + reason in state | n/a | n/a | n/a | n/a | n/a |
| cards/throughput | unit: montage run = one run/one receipt → cards unchanged | n/a | n/a | n/a | n/a | n/a | n/a |
| first live montage + first live cutout reel | manual:supervised (Tier-3), dated entry | n/a | n/a | n/a | n/a | n/a | n/a |

**Retro rows (2026-08-12, from the 08-08→08-12 segment audit):** (1) meme repeat-avoidance was UNENUMERATED — `tests/test_meme_repeat_avoidance.py` pins the exclusion window ≥4 weekly cycles (7-day window let Scooby repeat in 2 weeks); (2) hook fit-to-card — `TestHookFit` pins the historical truncated hook now fitting untruncated; (3) sync-weekly skip runs stamp classification health — `tests/test_weekly_sync_health_stamp.py` (source-scan pin; idle ≠ broken false RED); (4) the 07-31 social-queue pollution audit row: covered by `tests/test_no_test_pollution_in_social_publish_queue.py` + guard tests (row formally recorded here now).

**Build status (2026-08-09):** P1 ✅ e5340ea (cutout+gate+compose, 20 tests; real wrestler-duck cutout passed visually, coverage 0.417). P2 ✅ 6e6d4a4 (selection ladder, 12 tests). P3 ✅ 6346a3f (montage renderer+labels+hooks, 8 tests). P4 ✅ 9801f5d (flow wiring, 6 tests; existing Surface 64 tests untouched = contract proof). Real-generation run (trending top-5, led by Texas Longhorn 25 sold/7d) caught 2 bugs units missed — tz-naive observed_at crash + scrim truncating a valid 6-word hook — both fixed+regression-pinned in db2c97c; final montage_cutout video passed visual review (full hook, labels, dashboard cutouts). Preview run dir deleted so preview ducks stay in rotation. REMAINING: supervised live montage + cutout reels through email→sidecar (next even ISO week ≈ 2026-08-10 run window), dated entries here.

## Surface 65 — competitor social intel: viral-post surfacing + generation priors + /portal/intel/social (SPEC-FIRST 2026-08-12, operator: "post types that go more viral… we haven't gathered as much intel as we could" / "wire it in and include tier 2" / "a clean duck ops page… raw data")

**Design:** the per-post competitor data (84 posts, 7 accounts: format/hook_family/theme/caption/engagement/url) existed but only averaged into aggregates. Three additions: (T1) `competitor_social_benchmark_collector` gains `top_viral_posts` (top 8 w/ links) + median/max per format (means hid the 272× reel outlier) + markdown section; (T2) a `social_priors` block {winning_format, winning_hook_family, sample_hooks, basis_post_count} consumed FAIL-SOFT by meme + duckvideo caption/hook prompts (human-reviewed lanes → light gate per convention 5 calibration; priors are one steering line, never a hard rule); (Page) `/portal/intel/social` per the inspector recipe over the EXISTING snapshots/benchmark state (no new producer). Poster fix (same day): video poster = FIRST frame (mid-frame was 180° into rotation — duck facing away; operator caught it).

| Use case | Happy | Empty/missing data | Outlier honesty | Priors absent | Test pollution |
|---|---|---|---|---|---|
| top_viral_posts + medians (collector) | unit: fixture posts → top-N ranked w/ url/hook, by_format carries median+max | unit: zero posts → empty lists, no crash | unit: median ≠ mean asserted on skewed fixture | n/a | collector tests already isolated (existing conftest) |
| social_priors block | unit: winning format/hook derived from MEDIAN, sample_hooks non-empty | unit: <10 posts → priors omitted (too thin to steer) | n/a | n/a | n/a |
| prompt consumption (meme + duckvideo) | unit: prompt contains prior line when priors exist | unit: priors file missing → prompt identical to today (fail-soft) | n/a | unit: malformed priors → ignored | n/a |
| /portal/intel/social page | loader+page+route+tile tests per inspector recipe incl. empty-state | loader never raises; yellow unavailable | raw table sorted by engagement desc | n/a | reads only; no writes |
| poster first-frame | unit: poster written from frames[0] | n/a | n/a | n/a | n/a |

**Build status (2026-08-12): SHIPPED same day.** T1 ✅ 8745994 (collector: top_viral_posts w/ links, median+max per dimension, markdown section; 5 tests; live run verified — reel median 43.9 vs mean 807, the 12k outlier now visible instead of hidden). T2 ✅ b25c4b2 (social_priors median-ranked + fail-soft prompt line into meme + duckvideo hooks; 5 tests; priors currently statement_showcase/reel). Page ✅ 66d4232 (/portal/intel/social per inspector recipe, 10 page tests + 241 viewer regression; intel directory gains social AND the previously-missing seo-outcome entry). **Live-surface verified:** viewer bounced, real /portal/intel/social served 54KB w/ viral posts + .table-scroll containers, /api/social-intel live, operator markdown carries the viral section. Poster-first-frame fix in b25c4b2 w/ unit pin.

## Process note (this is the first matrix; previous work shipped without one)

The skill discipline is **invoke `/coverage-matrix` BEFORE the feature, not after.** Today's matrix is backfill — the three integration-boundary tests it surfaced (widget_api email, main_agent dispatch, observer end-to-end) were caught only because the operator asked "did you test your last changes?"

Memorized for future sessions: [feedback_invoke_coverage_matrix.md](file:///Users/philtullai/.claude/projects/-Users-philtullai-ai-agents/memory/feedback_invoke_coverage_matrix.md).

Acceptance criteria for next ship:
- [ ] This file is current — every new code path has a row
- [ ] No `🔴 MISSING` cells without a `manual:` or `skip:` decision and a one-line reason
- [ ] At least one new regression-class test exists per closed bug
- [ ] The "Empty cells" queue above has been reviewed in /retro

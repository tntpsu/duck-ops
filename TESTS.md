# TESTS — Coverage Matrix (Duck Ops + DuckAgent)

Last updated: 2026-05-26 (post carousel-to-portal integration)

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
| Portal Approve → Instagram schedule | ✅ test_review_carousel_publish_contract.py | ✅ widget_api_carousel_reject (subject pin) | ✅ widget_api_carousel_reject | 🔴 MISSING | ✅ contract test idempotency | 🔴 MISSING | ✅ contract test policy-block path |
| Portal Reject → emit needs_changes email | ✅ test_widget_api_carousel_reject.py | ✅ same file (RUN: assertion) | ✅ same file (fail-soft) | 🔴 MISSING | n/a:reject-idempotent | 🔴 MISSING | n/a |
| Email-reply `publish` → publish helper | ✅ test_main_agent_carousel_dispatch.py | n/a:already-tested-at-observer | n/a | 🔴 MISSING | ✅ publish helper has its own idempotency | 🔴 MISSING | n/a |
| Email-reply `needs_changes` / `reject` → reset helper | ✅ test_main_agent_carousel_dispatch.py | n/a | n/a | 🔴 MISSING | ⚠️  manual:reset-after-reset-is-safe-but-untested | 🔴 MISSING | n/a |
| Reset helper clears pending + rebuilds queue | ✅ test_review_carousel_reset.py | n/a | n/a | n/a | ✅ rebuild is idempotent | n/a | n/a |
| Carousel state reads correctly into Agent OS | ⚠️  manual:portal-render-check | n/a:run_id-not-rendered | n/a | n/a | n/a | n/a | n/a |

**Gap summary:** 4 cells of "concurrent observer + email-reply race" untested. Realistic scenario: observer fires at T=0 (writes draft row to publish_candidates), operator hits Approve in portal at T=1s, observer fires again at T=30s with the now-scheduled publish_result.json. The publish helper has idempotency guards, but the publish_candidates row state can disagree with the workflow_control state in that window. Verdict: **acceptable risk** because (a) publish helper no-ops on second call, (b) observer regen frequency is minutes, (c) operator-visible failure mode is at worst "duplicate feedback email." Mark `manual:cron-cadence-race` with note in next /retro.

**Gap summary 2:** "Reject after schedule" — what if the operator clicks Reject AFTER the publish already scheduled at 7 PM? Reset would clear pending_carousel and slides could be re-selected; the scheduled IG post would still publish. That's the failure mode the contract test `test_blocked_publish_records_policy_block_not_scheduled` is closest to but not exact. Verdict: **add test** if this scenario is realistic. For now mark `MISSING` and discuss in /retro.

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
| **Security** | ⚠️  manual:secrets-in-env | .env file convention in place; no automated secret-leak scan against commits. |
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
2. **🔴 Reject-after-schedule race** — what if operator clicks Reject after the publish_result.json is already written? Need to decide expected behavior first, then test.
3. **🔴 Concurrent observer + email-reply** — same scenario class. Document expected behavior in /retro then test.
4. **🔴 Stress / soak** — sidecar runs every 6h × multiple weeks. publish_candidates.json grows. Run a synthetic week-of-data soak test once.
5. **⚠️  Secret-leak automation** — `gitleaks detect --staged` as a pre-commit step. Currently relies on careful human review.

---

## Process note (this is the first matrix; previous work shipped without one)

The skill discipline is **invoke `/coverage-matrix` BEFORE the feature, not after.** Today's matrix is backfill — the three integration-boundary tests it surfaced (widget_api email, main_agent dispatch, observer end-to-end) were caught only because the operator asked "did you test your last changes?"

Memorized for future sessions: [feedback_invoke_coverage_matrix.md](file:///Users/philtullai/.claude/projects/-Users-philtullai-ai-agents/memory/feedback_invoke_coverage_matrix.md).

Acceptance criteria for next ship:
- [ ] This file is current — every new code path has a row
- [ ] No `🔴 MISSING` cells without a `manual:` or `skip:` decision and a one-line reason
- [ ] At least one new regression-class test exists per closed bug
- [ ] The "Empty cells" queue above has been reviewed in /retro

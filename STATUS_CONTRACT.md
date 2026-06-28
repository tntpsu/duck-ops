# Artifact Status Contract

Canonical meaning of every artifact status/lifecycle field. **One field, one
meaning.** Born from Surface 43 (2026-06-27): `review_status: "pending"` was
read as *"needs a human"* by the operator queue AND *"go post this"* by the
auto-enqueue — one field, two meanings, two readers that silently disagreed.

This doc is owned under the `duck-data-model-governance` guard. **Before adding
or changing any status field, read this, and add an invariant test.** If a value
would drive both an automation action AND an operator-facing surface with
different intent, you are about to repeat Surface 43 — split it instead.

---

## The four orthogonal axes

An artifact's state is FOUR independent questions. Never collapse two into one
field — that's the bug.

| Axis | Field | Question it answers | Legal values |
|---|---|---|---|
| 1. Quality | `decision` | What did the gate conclude about output quality? | `publish_ready` / `needs_revision` / `discard` |
| 2. Human lifecycle | `review_status` | Where is the operator's decision? | `pending` / `approved` / `rejected` / `overridden` / `archived` |
| 3. Automation policy | `handling` *(to add — see Debt #1)* | Does this post WITHOUT a human? | `auto` / `manual` |
| 4. Queue position | `execution_state` | Where is it in the posting pipeline? | `not_queued` / `queued` / `running` / `posted` / `failed` / `skipped` / `resolved` |

`execution_state` is the **reference implementation** — a clean single-purpose
axis (`review_reply_executor.py`). Model new status fields on it.

"Needs a human decision" = `decision==publish_ready AND handling==manual AND review_status==pending`.
Today `handling` isn't persisted, so that conjunction is derived on the fly by
`_auto_approved_flows()` / `item_is_auto_handled()` (`review_loop.py`). That works
for display but is the debt below.

---

## Per-field canonical meaning

### `decision` — machine quality verdict ONLY
- Means: the gate's verdict on output quality. **Must NOT** encode auto-vs-manual or human approval.
- Values: `publish_ready` / `needs_revision` / `discard`.
- ✅ **Verified canonical across flows (2026-06-27):** live `quality_gate_state.json` shows EVERY flow (meme/jeepfact/weekly/thursday/newduck/reviews_*) uses only `publish_ready`/`needs_revision`/`discard`. duck-ops `== "publish_ready"` checks DO match all flows. (An earlier audit flagged a "cross-repo mismatch" — that was a false alarm: jeepfact has a *local* variable also named `decision` holding scheduling-policy values (`auto_schedule_allowed`/`manual_publish_allowed`/…) that drives buttons/confirmation, NOT the artifact's gate `decision`. Different field, no collision. See naming-hygiene note below.)
- 🟡 **Naming-hygiene (LOW):** `flows/jeepfact/steps.py` reuses the name `decision` for a publish-scheduling-policy local var — distinct from the gate `decision`. Footgun if ever serialized into the artifact. Consider renaming the local to `schedule_policy`.

### `review_status` — operator decision lifecycle ONLY
- Means: where the human decision stands. **Must NOT** be an automation trigger.
- Values: `pending` (awaiting human) / `approved` / `rejected` / `overridden` / `archived`.
- Surface 43 fix: `surfaced_review_items` hides `auto_handled` items so auto-posting replies don't appear as human decisions — but `review_status` stays `pending` so the auto-enqueue still fires. Display-only; root fix is Debt #1.

### `handling` — automation policy (NEW, Debt #1)
- Means: `auto` (system posts without a human) vs `manual` (human gate required).
- Should be **persisted at produce time** from the live execution policy, not re-derived per reader. The auto-enqueue should key off `handling==auto AND execution_state==not_queued` — NOT `review_status==pending`.
- The auto/manual concept currently lives in THREE unsynced places: duck-ops `_auto_approved_flows()` (derived, not persisted), duckAgent's `decision=auto_*|manual_*` vocabulary, and the review-reply policy flags. Persist it once.

### `execution_state` — queue position ONLY ✅ (the model)
- Values: `not_queued` / `queued` / `running` / `posted` / `failed` / `skipped` / `resolved`. Single-purpose. Leave as-is.

### `available` — data presence (transient) ONLY
- Means: is there fresh data right now? `false` = retry later (missing/stale producer output). This is the **top-level** producer flag (gsc_search_demand, listing_performance, sale_steering, profit_intel, …) that readers like `build_next_engine`, `seo_demand_context`, `roi_triage` check.
- **Permanent "feature not wired" is a DIFFERENT field at a different path**, carrying its own marker: `status:"unavailable"` + a `note` (reference: `shopify_seo_outcomes.py` `traffic_signal.available:false, status:"unavailable"`). **Verified 2026-06-27:** no reader cross-reads these two — transient `available` and the nested capability-gap are separate paths/consumers. Convention to keep: a transient gap is a bare `available:false`; a permanent capability-gap MUST also carry `status:"unavailable"` (+ note), never a bare `available:false`.

### `chain_state` — disjoint domains, cross-kind bucketed by design
- Two state machines share this key: review_apply (`awaiting_review` / `apply_attention` / `ready_to_send_next` / `all_clear` / `idle`, from `shopify_seo_outcomes.py`) and promotion (`active` / `ready` / `blocked` / `observing`, from `business_operator_desk.py`). **Verified disjoint 2026-06-27.**
- The operator desk INTENTIONALLY buckets cross-kind (merges `ready_to_send_next`+`ready`, `apply_attention`+`blocked` into one operator view). That's safe **only while the two domains stay disjoint** — guarded by `tests/test_chain_state_domains.py` (fails if a producer ever emits a colliding value). If you add a value, keep the domains disjoint or branch the bucketer on `chain_kind`.

### `status` — SURFACE-LOCAL; never cross-read
- Each surface owns its own `status` vocabulary: OS health cards (`green/yellow/red`, `gap/observing/ready_to_test/…`), scheduler receipts (`healthy/missed_run/failed/timeout/hung/orphaned/slow/running/fixed_pending_next_run`), execution queue (`queued/running/posted/skipped/dismissed/…`), dependency_health (`ok/warn/bad`). **Never apply one surface's vocabulary to another's values.** Safe today (no cross-read path); keep it that way.

### Records, not flags (leave alone)
- `operator_resolution`, `reconciled_resolution` are structured records (action/note/channel/timestamp), not enums. Fine.

---

## Known debts (ranked)

1. **HIGH — persist `handling: auto|manual`.** Surface 43's root. Auto-enqueue keys off `review_status==pending` (overloaded). Add `handling`, set it at produce time, key auto-enqueue off it; then `review_status` is purely human-lifecycle. The shipped Surface 43 fix is display-only until this lands.
2. **~~HIGH — cross-repo `decision` mismatch~~ → RESOLVED (false alarm, verified 2026-06-27).** Live artifacts use the canonical vocabulary across all flows; the flagged values were a separate jeepfact-local scheduling var. Downgraded to the LOW naming-hygiene note above (rename the local `decision`).
3. **~~MEDIUM — `chain_state`~~ → GUARDED (2026-06-27).** Verified the two domains are disjoint; cross-kind bucketing is intentional and safe. Pinned by `tests/test_chain_state_domains.py` so a future colliding value fails loudly. (Stronger follow-up: extract the domains into shared constants the producers + bucketer both import.)
4. **~~MEDIUM-LOW — `available`~~ → VERIFIED SAFE (2026-06-27).** Transient `available` (top-level) and the permanent capability-gap (`traffic_signal.available` + `status:"unavailable"`) are different paths read by different consumers; no conflation. Convention documented above. No code change needed.

So after verification ALL FOUR audit debts are closed: #1 **fixed** (Surface 44), #2 **false alarm**, #3 **guarded** (invariant test), #4 **verified safe**. The data model is in good shape, and the status-overload class that caused Surface 43 is structurally addressed (canonical contract + the real overload removed + latent risks guarded).

## Rule of thumb
When you add an automation property (auto-approve, auto-apply, auto-schedule) to
any flow, it gets a **persisted field with one owner that every consumer reads** —
never re-derive it per surface, and never overload an existing lifecycle field to
carry it. Add an invariant test that encodes the meaning (e.g.
`test_review_queue_auto_handled.py`: an auto-handled item never surfaces as a
manual decision).

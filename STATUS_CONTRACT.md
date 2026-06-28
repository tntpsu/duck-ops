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
- ⚠️ **Cross-repo collision (Debt #2):** duckAgent flows write a DIFFERENT vocabulary into `decision` (`auto_schedule_allowed`, `manual_review_required`, `manual_publish_allowed`, `auto_apply_allowed`, `approved_for_publish`, `blocked`). duck-ops readers checking `decision == "publish_ready"` silently never match duckAgent-authored artifacts. Either unify the vocabulary or namespace by `artifact_type`.

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
- Means: is there fresh data right now? `false` = retry later (missing/stale producer output).
- **Must NOT** be overloaded for "feature not wired / never coming" — that permanent capability-gap needs a separate marker (some surfaces nest `status:"unavailable"`). Readers can't currently tell "retry later" from "never." `seo_demand_context.py` does it right (checks staleness separately).

### `chain_state` — read ONLY with `chain_kind`
- Two disjoint state machines share this key: SEO review chain (`awaiting_review` / `apply_attention` / `ready_to_send_next` / `all_clear` / `idle`) and promotion lifecycle (`active` / `observing` / …). Always branch readers on `chain_kind` ("seo" vs "promotion"); collision risk if promotion ever emits `ready`/`idle`.

### `status` — SURFACE-LOCAL; never cross-read
- Each surface owns its own `status` vocabulary: OS health cards (`green/yellow/red`, `gap/observing/ready_to_test/…`), scheduler receipts (`healthy/missed_run/failed/timeout/hung/orphaned/slow/running/fixed_pending_next_run`), execution queue (`queued/running/posted/skipped/dismissed/…`), dependency_health (`ok/warn/bad`). **Never apply one surface's vocabulary to another's values.** Safe today (no cross-read path); keep it that way.

### Records, not flags (leave alone)
- `operator_resolution`, `reconciled_resolution` are structured records (action/note/channel/timestamp), not enums. Fine.

---

## Known debts (ranked)

1. **HIGH — persist `handling: auto|manual`.** Surface 43's root. Auto-enqueue keys off `review_status==pending` (overloaded). Add `handling`, set it at produce time, key auto-enqueue off it; then `review_status` is purely human-lifecycle. The shipped Surface 43 fix is display-only until this lands.
2. **HIGH — cross-repo `decision` vocabulary mismatch.** duckAgent's `decision` values never match duck-ops `==publish_ready` checks. Verify this isn't silently mis-handling meme/jeepfact/weekly/thursday artifacts, then unify or namespace.
3. **MEDIUM — `chain_state`** depends on `chain_kind` to disambiguate; document or branch.
4. **MEDIUM-LOW — `available`** can't distinguish transient absence from permanent capability-gap.

## Rule of thumb
When you add an automation property (auto-approve, auto-apply, auto-schedule) to
any flow, it gets a **persisted field with one owner that every consumer reads** —
never re-derive it per surface, and never overload an existing lifecycle field to
carry it. Add an invariant test that encodes the meaning (e.g.
`test_review_queue_auto_handled.py`: an auto-handled item never surfaces as a
manual decision).

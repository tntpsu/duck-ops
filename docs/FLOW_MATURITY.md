# Flow Maturity Scorecard

**What this is:** a dated baseline of how "self-improving" each LLM-*judgment* flow
in the duck stack is, on a 0–4 maturity ladder. The point is to make "are we getting
smarter over time?" a **number that moves**, not a vibe. Re-measure periodically
(the scan commands are at the bottom) and append a row to the trend table.

**What this is NOT:** a scorecard for deterministic plumbing (producers, secret scan,
state writers, API wiring). Those are Level 0 *by design* and should stay boring —
see [design doctrine](../../duckAgent/CLAUDE.md) pattern #5. Only surfaces that make an
LLM *judgment* (classify / score / select / generate) can meaningfully "get smarter,"
so only those are tracked here.

---

## The maturity ladder

| Level | Name | Definition | Gets smarter when… |
|---|---|---|---|
| 0 | Deterministic | plumbing; same output forever | never (correct) |
| 1 | Observable | receipts + OS card, but no truth set | a human reads the card |
| 2 | Evaluable | golden fixture + gated eval script | a human tunes config from eval results |
| 3 | Self-correcting | operator corrections captured **and respected** | operator corrects once → it stops repeating |
| 4 | Closed outcome loop | measures its own **business** result and adjusts | automatically, within a gate |

Promotion rule (from doctrine): no surface advances a level without the evidence for
that level actually present in the repo — a golden set for L2, a respected override
store for L3, a prediction→measurement→gated-adjust loop for L4.

---

## Trend (append one row per re-measurement)

| Date | Judgment surfaces | Avg level /4 | L2+ (eval) | L3 (self-correct) | L4 (closed loop) | Notes |
|---|---|---|---|---|---|---|
| 2026-07-06 | 18 | **1.4** | 6 (33%) | 2 (11%) | 0 full / 2 partial | Baseline. Strong on eval discipline, zero closed outcome loops. |

**Headline read at baseline:** the system is mature on **observability** (OS cards,
two-card brackets, receipts everywhere) and **evaluation** (3 gated eval scripts, rich
golden corpora, 14 files with `needs_review` fail-closed). It is **immature on closed
outcome loops** — nothing yet measures "did my action produce the business result I
predicted, and adjust?" That is the frontier (see Flagship gap below).

---

## Baseline inventory — 2026-07-06

### Level 3 — self-correcting (2)
| Surface | Evidence |
|---|---|
| **theme_classifier** | `config/theme_taxonomy.json` (versioned) + `scripts/eval_theme_classifier.py` (gated) + `theme_review_decisions` override store the classifier respects (no re-flag) + false-keyword feedback log. The crown jewel. |
| **semantic dedupe** | `scripts/eval_dedupe.py` + versioned `config/semantic_dedupe.json` + `build_next_dupe_decisions` capture. |

### Level 2 — evaluable (4)
| Surface | Evidence | Missing for L3 |
|---|---|---|
| **shopify SEO** | `scripts/eval_seo_search_demand.py` + `seo_search_demand_golden.json`; `shopify_seo_outcomes.py` reaches toward L4 | operator-correction store |
| **trend_ranker** | rich dated golden regression corpus (`trend__golden-*`) | corrections not fed back |
| **thursday namer** | temp-0, fail-closed, golden set | no override capture |
| **creative_quality_outcome** | records generation outcomes (measured) | outcome not fed back into generation |

### Level 1 — observable only (~12)
Generators & scorers with a receipt/OS card but no truth set or feedback:
`review_reply_rewriter`, `review_reply_scorer` (has `review_reply_feedback.jsonl` — a
half-step toward L3, not yet respected/gated), `jeepfact_rewriter`, `meme_helper`,
`weekly_sale_rewriter`, `competitor_engine`, `build_next_engine` (versioned
`demand_scoring.json` but no eval), `review_selection_helper`, `duck_style_system`,
`duck_image_helper`, `etsy_review_helper`, occasion nod.

### Level 0 — deterministic plumbing (correctly boring, not scored)
profit / GSC / GA4 / occasion producers, secret scan, state writers, browser
guard/batch, notifiers. ~40 modules. These should never become loops.

---

## Flagship gap — the one closed outcome loop to build

No flow is Level 4. The nearest-ready candidate is the **Demand page**: the plumbing is
already stubbed (`acted_at` / `metrics_at_action` persisted for a v2 outcome loop).

Target loop: **Sale/Refresh/Protect action → record the prediction → measure the duck's
demand 14/30d later → score the prediction → feed the result into `demand_scoring.json`
weights, gated.** A flow that grades its own past decisions and adjusts. Building this
end-to-end is the showpiece "self-learning agentic loop" and proves the L4 pattern the
rest of the Level-1 surfaces can then adopt. (Spec: queued alongside next-week hook review.)

---

## How to re-measure (keep the method identical so the trend is honest)

```bash
cd /Users/philtullai/ai-agents
# gated eval scripts (L2+ surfaces)
ls duckAgent/scripts/eval_*.py duck-ops/scripts/eval_*.py 2>/dev/null
# golden fixtures (truth sets)
find duckAgent duck-ops -path '*/fixtures/*golden*' -o -name '*golden*.json' | grep -v node_modules
# versioned config-as-data
grep -rl '_version' duckAgent/config duck-ops/config
# operator-correction / feedback stores (L3 signal)
find duckAgent duck-ops \( -name '*override*' -o -name '*_decisions*' -o -name '*feedback*' \) | grep -viE 'test|\.pyc'
# outcome recording (L4 signal)
grep -rliE 'acted_at|metrics_at_action|_outcomes' duckAgent/flows duck-ops/runtime | grep -v test
# fail-closed escapes
grep -rl 'needs_review' duckAgent duck-ops | grep -v test | wc -l
```

To advance the trend: raise **Avg level**, or move a specific surface up a tier with the
evidence for that tier. The single highest-leverage move is closing the first L4 loop.

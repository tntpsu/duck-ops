#!/usr/bin/env python3
"""Surface 61 — gated eval for Shopify SEO title generation quality.

Runs the REAL LLM (_generate_proposals) on tests/fixtures/shopify_seo_golden.json
and scores each proposed title with the deterministic quality checks
(_seo_title_quality) + a differentiation check on dup_group members. NOT in the
default pytest suite — needs OPENAI_API_KEY. Promotion gate: no prompt/model
change ships below it.

Gate (2026-11-19): ZERO titles carry a code that production would fail closed
on (`_LIVE_QUALITY_ISSUE_CODES`), AND every dup_group produces DISTINCT titles.
Exit 0 = PASS, 1 = FAIL.

The gate used to be ">=90% pass ALL quality checks", which was **unsatisfiable
by construction** and had never passed:

  * `subject_missing` is deliberately NOT in `_LIVE_QUALITY_ISSUE_CODES` -- the
    live guard excludes it as an advisory heuristic that false-positives on
    legitimately reworded titles. An eval gating on a strictly LARGER set than
    production enforces blocks changes production ships happily.
  * The fixture's dup pair `d1`/`d2` share one subject ("Mix Match") and must
    produce DISTINCT titles. At most one of them can keep the subject, so
    `subject_missing` is forced on the other -- the two gate conditions
    contradicted each other.
  * With 8 resources a 90% threshold can only land on 87.5% or 100%, so one
    advisory false positive forced a FAIL carrying no signal
    ([[gate thresholds need resolution]]).

Advisory codes are still printed and counted, just not gating -- a rising
advisory rate is a real signal, it is simply not a reason to refuse a release.

    cd duck-ops && ./.venv-or-duckagent-venv/bin/python scripts/eval_shopify_seo.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

RUNTIME = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import shopify_seo_review as ssr  # noqa: E402

FIXTURE = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "shopify_seo_golden.json"


def main() -> int:
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    resources = data["resources"]
    proposals = ssr._generate_proposals(resources)
    by_id = {str(p.get("id")): p for p in proposals}
    subject_by_id = {str(r["id"]): r.get("subject", "") for r in resources}

    blocking = 0
    advisory = 0
    templated = 0
    dup_groups: dict[str, list[str]] = {}
    print(f"{'id':>6}  {'src':<9}  {'blocking':<22}  {'advisory':<18}  title")
    print("-" * 110)
    for r in resources:
        rid = str(r["id"])
        proposal = by_id.get(rid, {})
        title = str(proposal.get("seo_title", ""))
        source = str(proposal.get("title_source") or "?")
        templated += source == "template"
        issues = ssr._seo_title_quality(title, subject=subject_by_id.get(rid, ""))
        blocks = [c for c in issues if c in ssr._LIVE_QUALITY_ISSUE_CODES]
        advise = [c for c in issues if c not in ssr._LIVE_QUALITY_ISSUE_CODES]
        blocking += bool(blocks)
        advisory += bool(advise)
        print(
            f"{rid:>6}  {source:<9}  {(','.join(blocks) or '-'):<22}  "
            f"{(','.join(advise) or '-'):<18}  {title!r}"
        )
        group = r.get("dup_group")
        if group:
            dup_groups.setdefault(group, []).append(ssr._dup_title_key(title))

    total = len(resources)
    dup_ok = all(len(set(keys)) == len(keys) for keys in dup_groups.values())
    print("-" * 110)
    print(f"blocking failures  : {blocking}/{total}  (gate: 0 -- production withholds any of these)")
    print(f"advisory flags     : {advisory}/{total}  (not gating; see the module docstring)")
    print(f"template fallbacks : {templated}/{total}  (gate: 0 -- a template is not a generation)")
    print(f"dup differentiation: {'PASS' if dup_ok else 'FAIL'}  {dup_groups}")
    ok = blocking == 0 and dup_ok and templated == 0
    print("\n" + ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

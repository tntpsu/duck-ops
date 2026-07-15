#!/usr/bin/env python3
"""Surface 61 — gated eval for Shopify SEO title generation quality.

Runs the REAL LLM (_generate_proposals) on tests/fixtures/shopify_seo_golden.json
and scores each proposed title with the deterministic quality checks
(_seo_title_quality) + a differentiation check on dup_group members. NOT in the
default pytest suite — needs OPENAI_API_KEY. Promotion gate: no prompt/model
change ships below it.

Gate: >=90% of titles pass ALL quality checks AND every dup_group produces
DISTINCT titles. Exit 0 = PASS, 1 = FAIL.

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

QUALITY_GATE = 0.90
FIXTURE = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "shopify_seo_golden.json"


def main() -> int:
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    resources = data["resources"]
    proposals = ssr._generate_proposals(resources)
    by_id = {str(p.get("id")): p for p in proposals}
    subject_by_id = {str(r["id"]): r.get("subject", "") for r in resources}

    passes = 0
    dup_groups: dict[str, list[str]] = {}
    print(f"{'id':>6}  {'issues':<34}  title")
    print("-" * 90)
    for r in resources:
        rid = str(r["id"])
        title = str(by_id.get(rid, {}).get("seo_title", ""))
        issues = ssr._seo_title_quality(title, subject=subject_by_id.get(rid, ""))
        passes += not issues
        print(f"{rid:>6}  {(','.join(issues) or 'OK'):<34}  {title!r}")
        group = r.get("dup_group")
        if group:
            dup_groups.setdefault(group, []).append(ssr._dup_title_key(title))

    rate = passes / len(resources) if resources else 0.0
    dup_ok = all(len(set(keys)) == len(keys) for keys in dup_groups.values())
    print("-" * 90)
    print(f"quality pass rate : {passes}/{len(resources)} = {rate:.0%}  (gate {QUALITY_GATE:.0%})")
    print(f"dup differentiation: {'PASS' if dup_ok else 'FAIL'}  {dup_groups}")
    ok = rate >= QUALITY_GATE and dup_ok
    print("\n" + ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

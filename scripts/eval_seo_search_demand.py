"""Surface 40 gated eval (real API — NOT in default pytest): does the demand-fed
SEO generator actually work the real GSC queries into the titles it writes?

Promotion gate: >= gate.min_pass_rate of the resources that were OFFERED
high-intent searches produce a title sharing >=1 token with an offered query.
Plus a deterministic 'ignored every offered term' detector — LLM stated
confidence is weak, so cross-check the OUTPUT, never trust it. Run this before
shipping any change to the SEO prompt or the demand context.

Usage:
  cd duck-ops && PYTHONPATH=runtime \
    /Users/philtullai/ai-agents/duckAgent/.venv/bin/python scripts/eval_seo_search_demand.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "runtime"))
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(ROOT.parent / "duckAgent" / ".env")
except Exception:
    pass

import seo_demand_context as sdc  # noqa: E402
import shopify_seo_review as ssr  # noqa: E402
from build_next_engine import _tokens  # noqa: E402

FIXTURE = ROOT / "tests" / "fixtures" / "seo_search_demand_golden.json"


def main() -> int:
    fx = json.loads(FIXTURE.read_text(encoding="utf-8"))
    gsc, lp = fx["gsc"], fx["listings"]
    ctx = sdc.SeoDemandContext(
        queries=list(gsc.get("top_queries") or []) + list(gsc.get("gap_queries") or []),
        term_scores=gsc.get("term_scores") or {},
        listings=lp.get("listings") or [],
    )
    resources = fx["resources"]
    proposals = {str(p.get("id")): p for p in ssr._generate_proposals(resources, demand_context=ctx)}

    offered = passed = 0
    misses: list[tuple[str, list[str], str]] = []
    for r in resources:
        rid = str(r["id"])
        rq = ctx.relevant_queries(r["title"])
        if not rq:
            continue
        offered += 1
        offered_tokens: set[str] = set()
        for q in rq:
            offered_tokens |= _tokens(q["query"])
        title = proposals.get(rid, {}).get("seo_title", "")
        if _tokens(title) & offered_tokens:
            passed += 1
        else:
            misses.append((r["title"], [q["query"] for q in rq], title))

    rate = (passed / offered) if offered else 1.0
    gate = float(fx.get("gate", {}).get("min_pass_rate", 0.8))
    print(f"[eval_seo_search_demand] {passed}/{offered} titles incorporated a "
          f"high-intent term  rate={rate:.0%}  gate={gate:.0%}")
    for title, qs, got in misses:
        print(f"  MISS  {title} | offered {qs} | got: {got!r}")
    ok = rate >= gate
    print("PASS" if ok else "FAIL — generator ignored offered demand on too many items")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

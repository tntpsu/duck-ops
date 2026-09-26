"""Pricing coverage for the LLM cost card (2026-09-25).

The model registry moved ~12 roles onto gpt-5.5 and NEITHER pricing table knew
that model. `estimate_cost_usd` returns None for an unknown model, which the
aggregator books as $0, so the operator's cost surface would have shown spend
FALLING while it actually rose sharply. The card mentioned "N calls uncosted" in
its copy but kept status "ok", so it read GREEN throughout — a textbook
[[feedback_plausible_fallbacks_mask_failure]].

The durable fix is not the two price entries (those go stale again); it is that
an unpriced model is now NAMED and drives the card's status.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import llm_call_helpers
import llm_cost_summary


class TestPricingTable:
    @pytest.mark.parametrize("model", ["gpt-5.5", "gpt-5"])
    def test_the_upgraded_models_are_priced(self, model):
        assert llm_call_helpers.estimate_cost_usd(1000, 1000, model) is not None

    def test_estimated_prices_are_declared_as_estimates(self):
        """An unverified number must never be presented as a measurement."""
        assert "gpt-5.5" in llm_call_helpers.ESTIMATED_PRICING_MODELS

    def test_a_genuinely_unknown_model_still_returns_none(self):
        """The None path must survive; it is what the counter keys on."""
        assert llm_call_helpers.estimate_cost_usd(1000, 1000, "gpt-99-imaginary") is None

    def test_both_repos_agree_on_the_gpt5_price(self):
        """The viewer holds a documented-in-sync copy; drift means the card and
        the producer disagree about spend."""
        viewer = Path(
            "/Users/philtullai/ai-agents/duckAgent/creative_agent/runtime/src/"
            "duck_creative_agent/viewer.py"
        ).read_text(encoding="utf-8")
        for model in ("gpt-5.5", "gpt-5"):
            price = llm_call_helpers.MODEL_PRICING_USD_PER_1M_TOKENS[model]
            needle = f'"{model}": {{"prompt": {price["prompt"]:.3f}, "completion": {price["completion"]:.3f}}}'
            assert needle in viewer, f"viewer mirror missing or disagreeing for {model}"


class TestUnpricedModelsAreNamed:
    def _log(self, tmp_path, entries):
        log = tmp_path / "llm_call_log.jsonl"
        log.write_text("\n".join(json.dumps(e) for e in entries) + "\n", encoding="utf-8")
        return log

    def _entry(self, model, **kw):
        from datetime import datetime

        base = {
            "at": datetime.now().astimezone().isoformat(),
            "provider": "openai",
            "model": model,
            "prompt_tokens": 1000,
            "completion_tokens": 200,
            "artifact_id": "call:unknown:x",
        }
        base.update(kw)
        return base

    def test_an_unpriced_model_is_reported_by_name(self, tmp_path):
        """A bare count cannot be acted on; the name says what to add."""
        log = self._log(tmp_path, [self._entry("gpt-99-imaginary")])
        summary = llm_cost_summary.aggregate_llm_costs(log_path=log, window_days=30)

        coverage = summary["pricing_coverage"]
        assert coverage["unpriced_model_count"] == 1
        assert coverage["unpriced_models"][0]["model"] == "gpt-99-imaginary"
        assert coverage["unpriced_models"][0]["call_count"] == 1

    def test_unpriced_models_are_ranked_by_call_count(self, tmp_path):
        log = self._log(tmp_path, [self._entry("model-a")] * 1 + [self._entry("model-b")] * 3)
        coverage = llm_cost_summary.aggregate_llm_costs(log_path=log, window_days=30)["pricing_coverage"]
        assert [m["model"] for m in coverage["unpriced_models"]] == ["model-b", "model-a"]

    def test_estimated_pricing_calls_are_counted_separately(self, tmp_path):
        """Priced, but from a guess — a different problem from unpriced, and the
        operator needs to be able to tell them apart."""
        log = self._log(tmp_path, [self._entry("gpt-5.5")] * 2)
        coverage = llm_cost_summary.aggregate_llm_costs(log_path=log, window_days=30)["pricing_coverage"]

        assert coverage["estimated_pricing_call_count"] == 2
        assert coverage["unpriced_model_count"] == 0
        assert "gpt-5.5" in coverage["estimated_pricing_models"]

    def test_a_fully_priced_log_reports_clean_coverage(self, tmp_path):
        log = self._log(tmp_path, [self._entry("gpt-4o-mini")])
        coverage = llm_cost_summary.aggregate_llm_costs(log_path=log, window_days=30)["pricing_coverage"]
        assert coverage["unpriced_model_count"] == 0
        assert coverage["estimated_pricing_call_count"] == 0

    def test_an_empty_log_does_not_raise(self, tmp_path):
        log = tmp_path / "empty.jsonl"
        log.write_text("", encoding="utf-8")
        coverage = llm_cost_summary.aggregate_llm_costs(log_path=log, window_days=30)["pricing_coverage"]
        assert coverage["unpriced_models"] == []


class TestTheCardActuallyChangesColour:
    """The failure was not missing information — the copy already said 'uncosted'.
    It was that status stayed 'ok', so nobody scanning card colours saw it."""

    def _tile_source(self) -> str:
        viewer = Path(
            "/Users/philtullai/ai-agents/duckAgent/creative_agent/runtime/src/"
            "duck_creative_agent/viewer.py"
        ).read_text(encoding="utf-8")
        i = viewer.index("function costIntelSummary")
        return viewer[i : viewer.index("function seoOutcomeIntelSummary")]

    def test_uncosted_calls_drive_the_status_not_just_the_copy(self):
        tile = self._tile_source()
        assert "uncostedWarn" in tile
        assert "(alert || uncostedWarn)" in tile, "status must react to uncosted calls"

    def test_the_tile_names_the_unpriced_models(self):
        assert "unpriced_models" in self._tile_source()

    def test_the_tile_flags_estimated_pricing_as_unverified(self):
        assert "UNVERIFIED" in self._tile_source()

    def test_the_tile_reads_the_payload_variable_that_exists(self):
        """costIntelSummary's payload is `c`. Referencing an undefined `cost`
        throws a ReferenceError that kills the whole inline <script> and renders
        a blank page while the server still returns 200
        ([[feedback_python_triple_quote_corrupts_embedded_js]])."""
        tile = self._tile_source()
        assert "c.pricing_coverage" in tile
        import re

        assert not re.search(r"\bcost\.", tile), "stale `cost.` reference would blank the page"

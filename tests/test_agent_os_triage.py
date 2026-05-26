"""Contract tests for agent_os_triage.

Pins the four pieces that matter:
1. Area selection (--area / --all-red / --include-warn)
2. JSONL tally (outcome breakdown, sanity reasons, api errors,
   sample collection)
3. Failure-mode classification (prompt vs provider vs code vs data)
4. Markdown rendering shape

If the JSONL tally drifts (mis-counts a column, swallows a failure
mode, leaks samples cross-mode), the rewriter fix won't surface
correctly the next time something breaks — and the skill loses the
trust the worked example bought.
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import agent_os_triage as triage  # noqa: E402


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")


class AreaSelectionTests(unittest.TestCase):
    def _fixture(self) -> dict:
        return {
            "generated_at": "ignored",
            "overall_status": "ignored",
            "red_area": {"_status": "red", "_status_reason": "boom"},
            "warn_area": {"_status": "warn", "_status_reason": "watch this"},
            "ok_area": {"_status": "ok"},
            "stale_area": {"_status": "stale"},
            "no_status_block": {"some": "data"},
        }

    def test_specific_area_returned_even_when_green(self) -> None:
        out = triage.select_areas(self._fixture(), area="ok_area")
        self.assertEqual([k for k, _ in out], ["ok_area"])

    def test_all_red_only_returns_red(self) -> None:
        out = triage.select_areas(self._fixture(), all_red=True)
        self.assertEqual([k for k, _ in out], ["red_area"])

    def test_include_warn_adds_warn_and_stale(self) -> None:
        out = triage.select_areas(
            self._fixture(), all_red=True, include_warn=True
        )
        keys = {k for k, _ in out}
        self.assertEqual(keys, {"red_area", "warn_area", "stale_area"})

    def test_unknown_area_raises(self) -> None:
        with self.assertRaises(KeyError):
            triage.select_areas(self._fixture(), area="does-not-exist")

    def test_non_area_blocks_skipped(self) -> None:
        out = triage.select_areas(self._fixture(), all_red=True, include_warn=True)
        # "generated_at" is not an area; should not appear.
        self.assertNotIn("generated_at", [k for k, _ in out])


class JsonlTallyTests(unittest.TestCase):
    def test_filters_by_kind_and_counts_outcomes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "log.jsonl"
            _write_jsonl(log_path, [
                {"kind": "review_reply_rewrite", "outcome": "ok"},
                {"kind": "review_reply_rewrite", "outcome": "api_failure", "error": "http_500"},
                {"kind": "review_reply_rewrite", "outcome": "sanity_failed", "sanity_failures": ["echo_check"], "rejected_output_text": "boilerplate"},
                # Different kind — must be excluded.
                {"kind": "weekly_sale_rewrite", "outcome": "ok"},
            ])
            tally = triage._tally_jsonl(
                log_path,
                filter_kind="review_reply_rewrite",
                rejected_field="rejected_output_text",
                max_samples=2,
            )
        self.assertEqual(tally["total_calls"], 3)
        self.assertEqual(tally["outcomes"], {"ok": 1, "api_failure": 1, "sanity_failed": 1})
        self.assertEqual(tally["sanity_failures"], {"echo_check": 1})
        self.assertEqual(tally["api_errors"], {"http_500": 1})
        self.assertEqual(
            tally["samples_by_mode"]["echo_check"][0]["rejected_output"], "boilerplate"
        )

    def test_caps_samples_per_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "log.jsonl"
            records = [
                {
                    "kind": "review_reply_rewrite",
                    "outcome": "sanity_failed",
                    "sanity_failures": ["echo_check"],
                    "rejected_output_text": f"sample {i}",
                }
                for i in range(10)
            ]
            _write_jsonl(log_path, records)
            tally = triage._tally_jsonl(
                log_path,
                filter_kind="review_reply_rewrite",
                rejected_field="rejected_output_text",
                max_samples=3,
            )
        self.assertEqual(len(tally["samples_by_mode"]["echo_check"]), 3)
        self.assertEqual(tally["sanity_failures"]["echo_check"], 10)

    def test_missing_log_returns_unavailable(self) -> None:
        tally = triage._tally_jsonl(
            Path("/nonexistent/log.jsonl"),
            filter_kind=None,
            rejected_field=None,
            max_samples=3,
        )
        self.assertFalse(tally["available"])
        self.assertIn("not found", tally["reason"])

    def test_malformed_lines_skipped_without_crashing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "log.jsonl"
            log_path.write_text(
                '{"kind": "review_reply_rewrite", "outcome": "ok"}\n'
                'NOT JSON\n'
                '{"kind": "review_reply_rewrite", "outcome": "ok"}\n',
                encoding="utf-8",
            )
            tally = triage._tally_jsonl(
                log_path,
                filter_kind="review_reply_rewrite",
                rejected_field=None,
                max_samples=3,
            )
        self.assertEqual(tally["total_calls"], 2)


class ClassificationTests(unittest.TestCase):
    """The fix-category mapping is the operator's first decision —
    a wrong classification sends them to the wrong file. Pin the
    boundary cases."""

    def test_http_500_is_provider(self) -> None:
        self.assertEqual(triage._classify_failure_mode("http_500"), "provider")

    def test_echo_is_prompt(self) -> None:
        self.assertEqual(triage._classify_failure_mode("echo_check"), "prompt")

    def test_unparseable_is_code(self) -> None:
        self.assertIn("code", triage._classify_failure_mode("unparseable_or_invalid_json"))

    def test_rate_limit_is_provider(self) -> None:
        self.assertEqual(triage._classify_failure_mode("http_429_rate_limit"), "provider")

    def test_refusal_is_prompt(self) -> None:
        self.assertEqual(triage._classify_failure_mode("not_a_refusal"), "prompt")

    def test_unknown_returns_unknown(self) -> None:
        self.assertEqual(triage._classify_failure_mode("totally_made_up"), "unknown")


class TriageAndRenderTests(unittest.TestCase):
    def test_triage_area_pulls_jsonl_when_analyzer_registered(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "log.jsonl"
            _write_jsonl(log_path, [
                {"kind": "review_reply_rewrite", "outcome": "api_failure", "error": "http_500"},
            ])
            block = {
                "_status": "red",
                "_attention_subtype": "repair_now",
                "_status_reason": "api=100%",
                "call_log_path": str(log_path),
                "feedback_log_path": "/missing",
            }
            out = triage.triage_area("review_reply_rewriter_health", block)
        self.assertEqual(out["status"], "red")
        self.assertEqual(out["jsonl_tally"]["total_calls"], 1)
        self.assertIn("provider", out["fix_categories"])

    def test_triage_area_skips_jsonl_when_no_analyzer(self) -> None:
        block = {"_status": "red", "_status_reason": "no log"}
        out = triage.triage_area("unmapped_area", block)
        self.assertIsNone(out["jsonl_tally"])
        self.assertEqual(out["fix_categories"], [])

    def test_render_brief_includes_key_status_and_tally_headers(self) -> None:
        triage_dict = {
            "key": "review_reply_rewriter_health",
            "status": "red",
            "subtype": "repair_now",
            "reason": "boom",
            "structured_fields": {"total_calls": 62},
            "jsonl_tally": {
                "available": True,
                "log_path": "/tmp/x.jsonl",
                "total_calls": 3,
                "outcomes": {"ok": 1, "api_failure": 2},
                "sanity_failures": {},
                "api_errors": {"http_500": 2},
                "samples_by_mode": {},
            },
            "fix_categories": ["provider"],
        }
        out = triage.render_brief(triage_dict)
        self.assertIn("## review_reply_rewriter_health", out)
        self.assertIn("**Status:** red (repair_now)", out)
        self.assertIn("Call-log tally — 3 records", out)
        self.assertIn("`http_500` × 2", out)
        self.assertIn("**Fix categories:** provider", out)


if __name__ == "__main__":
    unittest.main()

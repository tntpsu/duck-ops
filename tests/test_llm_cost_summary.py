"""Surface 10 producer contract tests.

Pin the aggregate_llm_costs / parse_artifact_id / evaluate_alert
behaviors so the /portal/intel/cost page reads a stable shape.
"""
from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import llm_cost_summary
from llm_cost_summary import (  # noqa: E402
    aggregate_llm_costs,
    evaluate_alert,
    parse_artifact_id,
)


def _write_log(path: Path, entries: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")


def _entry(*, at: str = "2026-06-06T10:00:00-04:00",
           artifact_id: str = "publish::reviews_reply_positive::2026-06-06::r1",
           provider: str = "openai",
           model: str = "gpt-4o-mini",
           prompt_tokens: int = 1000,
           completion_tokens: int = 200,
           **extra) -> dict:
    base = {
        "at": at,
        "artifact_id": artifact_id,
        "provider": provider,
        "model": model,
        "outcome": "ok",
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
    }
    base.update(extra)
    return base


class ArtifactIdParseTests(unittest.TestCase):
    def test_publish_lane_parses(self) -> None:
        self.assertEqual(
            parse_artifact_id("publish::reviews_reply_positive::2026-05-23::review-1"),
            ("publish", "reviews_reply_positive"),
        )

    def test_score_lane_parses(self) -> None:
        self.assertEqual(
            parse_artifact_id("score::review_reply::2026-05-25::review-6"),
            ("score", "review_reply"),
        )

    def test_unparseable_falls_through_to_unknown(self) -> None:
        self.assertEqual(parse_artifact_id("garbage with spaces"), ("unknown", "unknown"))
        self.assertEqual(parse_artifact_id(""), ("unknown", "unknown"))
        self.assertEqual(parse_artifact_id(None), ("unknown", "unknown"))

    def test_uppercase_normalized(self) -> None:
        """Case-insensitive — publish::Reviews_Reply normalizes to lowercase
        so rollups don't split the same flow into two buckets."""
        self.assertEqual(
            parse_artifact_id("PUBLISH::REVIEWS_REPLY::2026-06-06::x"),
            ("publish", "reviews_reply"),
        )


class AggregateHappyPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_ctx = TemporaryDirectory()
        self.tmp = Path(self.tmp_ctx.name)
        self.log = self.tmp / "llm_call_log.jsonl"

    def tearDown(self) -> None:
        self.tmp_ctx.cleanup()

    def test_happy_path_aggregates_by_day_and_flow(self) -> None:
        _write_log(self.log, [
            _entry(at="2026-06-06T10:00:00-04:00",
                   artifact_id="publish::reviews_reply::2026-06-06::r1",
                   prompt_tokens=1000, completion_tokens=200),
            _entry(at="2026-06-06T11:00:00-04:00",
                   artifact_id="publish::reviews_reply::2026-06-06::r2",
                   prompt_tokens=500, completion_tokens=100),
            _entry(at="2026-06-05T10:00:00-04:00",
                   artifact_id="score::review_reply::2026-06-05::r3",
                   prompt_tokens=400, completion_tokens=50),
        ])
        summary = aggregate_llm_costs(
            log_path=self.log,
            now_iso="2026-06-06T18:00:00-04:00",
        )
        self.assertEqual(summary["totals"]["call_count"], 3)
        self.assertEqual(summary["totals"]["prompt_tokens"], 1900)
        self.assertEqual(summary["totals"]["completion_tokens"], 350)
        # Two days of rollup.
        self.assertEqual(len(summary["by_day"]), 2)
        # Two flows.
        flows = {row["flow"] for row in summary["by_flow"]}
        self.assertEqual(flows, {"reviews_reply", "review_reply"})
        # by_flow sorted by cost desc — reviews_reply has more tokens.
        self.assertEqual(summary["by_flow"][0]["flow"], "reviews_reply")
        # Today's bucket has 2 calls.
        self.assertEqual(summary["today"]["call_count"], 2)

    def test_cost_matches_pricing_table_for_gpt_4o_mini(self) -> None:
        """1M prompt + 1M completion tokens of gpt-4o-mini at
        $0.150/$0.600 per 1M → $0.75 total."""
        _write_log(self.log, [
            _entry(at="2026-06-06T10:00:00-04:00",
                   prompt_tokens=1_000_000, completion_tokens=1_000_000,
                   model="gpt-4o-mini"),
        ])
        summary = aggregate_llm_costs(log_path=self.log,
                                      now_iso="2026-06-06T18:00:00-04:00")
        self.assertAlmostEqual(summary["totals"]["cost_usd"], 0.75, places=4)

    def test_unknown_model_skips_cost_but_counts_call(self) -> None:
        _write_log(self.log, [
            _entry(at="2026-06-06T10:00:00-04:00", model="mystery-model-9000"),
        ])
        summary = aggregate_llm_costs(log_path=self.log,
                                      now_iso="2026-06-06T18:00:00-04:00")
        self.assertEqual(summary["totals"]["call_count"], 1)
        self.assertEqual(summary["totals"]["cost_usd"], 0.0)
        # Data quality surfaces the gap.
        self.assertEqual(summary["data_quality"]["unknown_model_count"], 1)

    def test_image_call_uses_per_call_pricing(self) -> None:
        """Image generation uses per-call pricing, not token-based."""
        _write_log(self.log, [
            {"at": "2026-06-06T10:00:00-04:00",
             "artifact_id": "publish::meme::2026-06-06::m1",
             "provider": "openai", "model": "gpt-image-1",
             "kind": "image", "image_count": 3},
        ])
        summary = aggregate_llm_costs(log_path=self.log,
                                      now_iso="2026-06-06T18:00:00-04:00")
        # 3 images × $0.04 = $0.12
        self.assertAlmostEqual(summary["totals"]["cost_usd"], 0.12, places=4)

    def test_gpt_image_2_is_priced_not_uncosted(self) -> None:
        """Regression (2026-06-14): the prod log uses model 'gpt-image-2'
        but the price table only had 'gpt-image-1', so 6 real image calls
        were counted as $0 (unknown_model). gpt-image-2 must be priced."""
        _write_log(self.log, [
            {"at": "2026-06-06T10:00:00-04:00",
             "artifact_id": "publish::meme::2026-06-06::m1",
             "provider": "openai", "model": "gpt-image-2",
             "kind": "image", "image_count": 1},
        ])
        summary = aggregate_llm_costs(log_path=self.log,
                                      now_iso="2026-06-06T18:00:00-04:00")
        self.assertAlmostEqual(summary["totals"]["cost_usd"], 0.04, places=4)
        self.assertEqual(summary["totals"]["unknown_model_count"], 0)


class AggregateEmptyAndMalformedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_ctx = TemporaryDirectory()
        self.tmp = Path(self.tmp_ctx.name)

    def tearDown(self) -> None:
        self.tmp_ctx.cleanup()

    def test_empty_log_returns_zero_totals(self) -> None:
        log = self.tmp / "llm_call_log.jsonl"
        log.write_text("")
        summary = aggregate_llm_costs(log_path=log,
                                      now_iso="2026-06-06T18:00:00-04:00")
        self.assertEqual(summary["totals"]["call_count"], 0)
        self.assertEqual(summary["totals"]["cost_usd"], 0.0)
        self.assertEqual(summary["by_day"], [])
        self.assertEqual(summary["by_flow"], [])

    def test_missing_log_returns_zero_totals(self) -> None:
        log = self.tmp / "does_not_exist.jsonl"
        summary = aggregate_llm_costs(log_path=log,
                                      now_iso="2026-06-06T18:00:00-04:00")
        self.assertEqual(summary["totals"]["call_count"], 0)
        self.assertEqual(summary["by_day"], [])

    def test_skips_malformed_jsonl_lines(self) -> None:
        log = self.tmp / "llm_call_log.jsonl"
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open("w") as fh:
            fh.write("not valid json\n")
            fh.write(json.dumps(_entry()) + "\n")
            fh.write('"a string not a dict"\n')
            fh.write("\n")  # Empty line.
        summary = aggregate_llm_costs(log_path=log,
                                      now_iso="2026-06-06T18:00:00-04:00")
        self.assertEqual(summary["totals"]["call_count"], 1)
        self.assertEqual(summary["data_quality"]["malformed_lines"], 2)

    def test_entries_without_at_are_counted_in_data_quality(self) -> None:
        log = self.tmp / "llm_call_log.jsonl"
        log.write_text(json.dumps({"model": "gpt-4o-mini"}) + "\n")
        summary = aggregate_llm_costs(log_path=log,
                                      now_iso="2026-06-06T18:00:00-04:00")
        self.assertEqual(summary["data_quality"]["entries_without_at"], 1)
        self.assertEqual(summary["totals"]["call_count"], 0)


class WindowFilterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_ctx = TemporaryDirectory()
        self.tmp = Path(self.tmp_ctx.name)
        self.log = self.tmp / "llm_call_log.jsonl"

    def tearDown(self) -> None:
        self.tmp_ctx.cleanup()

    def test_filters_window_days(self) -> None:
        """Entries older than window_days are excluded."""
        now_dt = datetime(2026, 6, 6, 18, 0, tzinfo=timezone(timedelta(hours=-4)))
        fresh = (now_dt - timedelta(days=5)).isoformat()
        old = (now_dt - timedelta(days=45)).isoformat()
        _write_log(self.log, [
            _entry(at=fresh, prompt_tokens=100, completion_tokens=50),
            _entry(at=old, prompt_tokens=1000, completion_tokens=500),
        ])
        s30 = aggregate_llm_costs(log_path=self.log, window_days=30,
                                  now_iso=now_dt.isoformat())
        s60 = aggregate_llm_costs(log_path=self.log, window_days=60,
                                  now_iso=now_dt.isoformat())
        self.assertEqual(s30["totals"]["call_count"], 1)
        self.assertEqual(s60["totals"]["call_count"], 2)


class AlertEvaluationTests(unittest.TestCase):
    def _summary(self, today_cost: float, top_flow: str = "reviews_reply") -> dict:
        return {
            "generated_at": "2026-06-06T18:00:00-04:00",
            "today": {"date": "2026-06-06", "cost_usd": today_cost, "call_count": 1},
            "by_flow": [{"flow": top_flow, "cost_usd": today_cost, "call_count": 1}],
        }

    def test_no_alert_when_under_threshold(self) -> None:
        self.assertIsNone(evaluate_alert(self._summary(2.0), threshold_usd=5.0))

    def test_writes_alert_when_today_over_threshold(self) -> None:
        alert = evaluate_alert(self._summary(7.50), threshold_usd=5.0)
        self.assertIsNotNone(alert)
        self.assertEqual(alert["today_cost_usd"], 7.50)
        self.assertEqual(alert["threshold_usd"], 5.0)
        self.assertEqual(alert["exceeded_by_usd"], 2.50)
        self.assertEqual(alert["top_flow_today"], "reviews_reply")
        # 7.50 < 2 × 5.00 = 10.00 → warn, not red.
        self.assertEqual(alert["severity"], "warn")

    def test_red_severity_when_double_threshold(self) -> None:
        alert = evaluate_alert(self._summary(15.0), threshold_usd=5.0)
        self.assertEqual(alert["severity"], "red")

    def test_threshold_boundary_exactly_at_threshold_does_not_alert(self) -> None:
        """today_cost == threshold → no alert. Alerts fire on `>` not `>=`
        so a sane threshold doesn't trip on its own boundary."""
        self.assertIsNone(evaluate_alert(self._summary(5.0), threshold_usd=5.0))


if __name__ == "__main__":
    unittest.main()

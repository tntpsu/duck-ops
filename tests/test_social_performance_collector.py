from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import social_performance_collector


class SocialPerformanceCollectorTests(unittest.TestCase):
    def test_build_social_performance_normalizes_receipts_and_rolls_up(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            receipt = root / "runs" / "run-1" / "meme_posts.json"
            receipt.parent.mkdir(parents=True, exist_ok=True)
            receipt.write_text(
                json.dumps(
                    {
                        "workflow": "meme",
                        "run_id": "run-1",
                        "posts": [
                            {
                                "platform": "instagram",
                                "post_id": "ig-1",
                                "status": "scheduled",
                                "scheduled_time": "2026-04-14T18:00:00-04:00",
                                "saved_at": "2026-04-14T12:00:00-04:00",
                                "url": "https://example.com/p/ig-1",
                                "meta_data": {
                                    "receipt_contract_version": 1,
                                    "content_type": "image",
                                    "title": "Cowgirl Duck",
                                    "theme": "cowgirl",
                                    "caption": "Meet the duck! #DuckDuckJeep #CowgirlDuck",
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            state_path = root / "state" / "social_performance_posts.json"
            rollups_path = root / "state" / "social_performance_rollups.json"
            history_path = root / "state" / "social_performance_history.json"
            operator_json_path = root / "output" / "operator" / "social_insights.json"
            output_path = root / "output" / "operator" / "social_insights.md"

            with patch.object(social_performance_collector, "STATE_PATH", state_path), patch.object(
                social_performance_collector, "ROLLUPS_PATH", rollups_path
            ), patch.object(
                social_performance_collector, "HISTORY_PATH", history_path
            ), patch.object(
                social_performance_collector, "OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                social_performance_collector, "OUTPUT_MD_PATH", output_path
            ), patch.object(
                social_performance_collector, "_receipt_paths", return_value=[receipt]
            ), patch.object(
                social_performance_collector,
                "fetch_post_metrics",
                return_value={
                    "status": "ok",
                    "metrics": {"like_count": 11, "comments_count": 2, "reach": 100, "saved": 3, "permalink": "https://example.com/p/ig-1"},
                    "errors": [],
                },
            ), patch.object(
                social_performance_collector,
                "datetime",
                wraps=social_performance_collector.datetime,
            ) as mock_datetime:
                mock_datetime.now.return_value = social_performance_collector.datetime.fromisoformat("2026-04-15T09:00:00-04:00")
                post_payload, rollup_payload = social_performance_collector.build_social_performance(window_days=30, fetch_metrics=True)

            self.assertEqual(post_payload["summary"]["normalized_post_count"], 1)
            self.assertEqual(post_payload["posts"][0]["hashtags"], ["DuckDuckJeep", "CowgirlDuck"])
            self.assertEqual(post_payload["posts"][0]["engagement_score"], 16.0)
            self.assertEqual(rollup_payload["summary"]["metrics_coverage_pct"], 100.0)
            self.assertTrue(rollup_payload["current_learnings"])
            self.assertTrue(state_path.exists())
            self.assertTrue(rollups_path.exists())
            self.assertTrue(history_path.exists())
            self.assertTrue(operator_json_path.exists())
            self.assertTrue(output_path.exists())

    def test_future_posts_are_not_fetched(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            receipt = root / "runs" / "run-2" / "jeepfact_posts.json"
            receipt.parent.mkdir(parents=True, exist_ok=True)
            receipt.write_text(
                json.dumps(
                    {
                        "workflow": "jeepfact",
                        "run_id": "run-2",
                        "posts": [
                            {
                                "platform": "instagram",
                                "post_id": "ig-future",
                                "status": "scheduled",
                                "scheduled_time": "2026-04-16T18:00:00-04:00",
                                "saved_at": "2026-04-15T12:00:00-04:00",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(social_performance_collector, "_receipt_paths", return_value=[receipt]), patch.object(
                social_performance_collector,
                "datetime",
                wraps=social_performance_collector.datetime,
            ) as mock_datetime:
                mock_datetime.now.return_value = social_performance_collector.datetime.fromisoformat("2026-04-15T09:00:00-04:00")
                post_payload, rollup_payload = social_performance_collector.build_social_performance_payload(window_days=30, fetch_metrics=True)

            self.assertEqual(post_payload["posts"][0]["metric_status"], "scheduled_future")
            self.assertEqual(post_payload["summary"]["metric_status_counts"]["scheduled_future"], 1)
            self.assertEqual(rollup_payload["summary"]["metrics_coverage_pct"], 0.0)

    def test_render_social_insights_mentions_data_quality(self) -> None:
        markdown = social_performance_collector.render_social_insights_markdown(
            {
                "summary": {
                    "metric_status_counts": {"ok": 1, "partial": 1},
                    "malformed_receipt_count": 0,
                }
            },
            {
                "generated_at": "2026-04-15T09:00:00-04:00",
                "window_days": 30,
                "summary": {
                    "post_count": 2,
                    "metrics_coverage_pct": 50.0,
                    "data_quality_note": "Receipt history is still sparse.",
                },
                "current_learnings": [
                    {
                        "headline": "Evening is the current best-performing posting window.",
                        "confidence": "low",
                        "evidence": "2 posts with average engagement score 10.",
                        "recommendation": "Keep testing evening.",
                    }
                ],
                "top_posts": [
                    {
                        "workflow": "meme",
                        "platform": "instagram",
                        "post_id": "123",
                        "title": "Cowgirl Duck",
                        "url": "https://example.com/p/123",
                        "engagement_score": 10,
                        "engagement_rate": 0.1,
                    }
                ],
                "rollups": {
                    "by_workflow": [{"label": "meme", "post_count": 2, "avg_engagement_score": 10, "avg_engagement_rate": 0.1}],
                    "by_platform": [],
                    "by_time_window": [],
                    "by_theme": [],
                },
            },
        )

        self.assertIn("Current Learnings", markdown)
        self.assertIn("Receipt history is still sparse", markdown)
        self.assertIn("Cowgirl Duck", markdown)


class FetchInstagramMetricsTests(unittest.TestCase):
    """2026-08-18: the collector requested `video_views`, which Instagram
    rejects for REELS media with a metric-enum 400 — the first Reel's view
    counts were silently lost. `views` is Meta's unified metric (live-probed:
    262 views returned where video_views errored)."""

    class _FakeResponse:
        def __init__(self, payload: dict, status_code: int = 200) -> None:
            self._payload = payload
            self.status_code = status_code
            self.text = json.dumps(payload)

        def json(self) -> dict:
            return self._payload

    def test_reels_requests_unified_views_metric_not_video_views(self) -> None:
        calls: list[tuple[str, dict]] = []
        fake_response = self._FakeResponse

        class FakeTokenManager:
            def make_request(self, method, url, *, params=None, **kwargs):
                calls.append((url, dict(params or {})))
                if url.endswith("/insights"):
                    metric = (params or {}).get("metric")
                    return fake_response(
                        {"data": [{"name": metric, "values": [{"value": 7}]}]}
                    )
                return fake_response({
                    "id": "123", "media_type": "VIDEO",
                    "media_product_type": "REELS", "permalink": "p",
                    "timestamp": "2026-08-14T21:00:38+0000",
                    "comments_count": 1, "like_count": 4,
                })

        result = social_performance_collector._fetch_instagram_metrics(
            {"post_id": "123"}, FakeTokenManager()
        )
        requested = [p.get("metric") for url, p in calls if url.endswith("/insights")]
        self.assertIn("views", requested)
        self.assertNotIn("video_views", requested)
        self.assertEqual(result["metrics"].get("views"), 7)
        self.assertEqual(result["errors"], [])


def _post(workflow, *, platform="instagram", status="ok", like=0, reach=None, post_id="p"):
    """A normalized post as build_social_performance_payload leaves it."""
    metrics = {}
    if status in {"ok", "partial"}:
        metrics = {"like_count": like, "comments_count": 0, "saved": 0}
        if reach is not None:
            metrics["reach"] = reach
    return {
        "workflow": workflow,
        "platform": platform,
        "post_id": post_id,
        "theme": "t",
        "time_window": "evening",
        "duck_family": "d",
        "metric_status": status,
        "metrics": metrics,
    }


class UnmeasuredPostsAreNotZeroTests(unittest.TestCase):
    """2026-09-24 field bug: 9 Facebook posts whose metrics fetch failed were
    scored 0.0 and averaged into the meme lane, which reported 1.60 instead of
    its true 4.00 and ranked last instead of tied second. The collector already
    recorded metric_status=fetch_failed per post and counted it in the summary;
    _engagement_score simply never read it. The writeback path had had the same
    guard since June (test_skips_on_fetch_failed_metric_status) -- the rollups
    never got it."""

    def test_failed_fetches_do_not_drag_the_lane_mean(self) -> None:
        posts = [
            _post("meme", like=6, post_id="m1"),
            _post("meme", platform="facebook", status="fetch_failed", post_id="m2"),
            _post("meme", platform="facebook", status="fetch_failed", post_id="m3"),
        ]
        row = social_performance_collector._rollup_rows(posts, "workflow")[0]
        self.assertEqual(row["avg_engagement_score"], 6.0)
        self.assertEqual(row["post_count"], 3)
        self.assertEqual(row["measured_post_count"], 1)

    def test_a_measured_zero_still_counts(self) -> None:
        """Only UNMEASURED posts are excluded; a real zero is real data."""
        posts = [_post("meme", like=6, post_id="m1"), _post("meme", like=0, post_id="m2")]
        row = social_performance_collector._rollup_rows(posts, "workflow")[0]
        self.assertEqual(row["avg_engagement_score"], 3.0)
        self.assertEqual(row["measured_post_count"], 2)

    def test_lane_with_nothing_measured_reports_none_not_zero(self) -> None:
        posts = [_post("meme", status="fetch_failed", post_id="m1")]
        row = social_performance_collector._rollup_rows(posts, "workflow")[0]
        self.assertIsNone(row["avg_engagement_score"])
        self.assertEqual(row["measured_post_count"], 0)

    def test_unmeasured_lanes_never_outrank_measured_ones(self) -> None:
        posts = [
            _post("meme", like=6, post_id="m1"),
            _post("jeepfact", status="fetch_failed", post_id="j1"),
        ]
        rows = social_performance_collector._rollup_rows(posts, "workflow")
        self.assertEqual(rows[0]["label"], "meme")

    def test_per_post_score_is_none_when_never_measured(self) -> None:
        """0.0 is a measurement; absence must not look like one."""
        self.assertIsNone(
            social_performance_collector._engagement_score_or_none(_post("meme", status="fetch_failed"))
        )
        self.assertEqual(
            social_performance_collector._engagement_score_or_none(_post("meme", like=4)), 4.0
        )

    def test_top_workflow_belief_ignores_unmeasured_posts(self) -> None:
        """The belief that actually shipped wrong."""
        posts = [
            _post("duckvideo", like=4, post_id="d1"),
            _post("meme", like=7, post_id="m1"),
            _post("meme", like=5, post_id="m2"),
        ] + [
            _post("meme", platform="facebook", status="fetch_failed", post_id=f"m{i}")
            for i in range(3, 12)
        ]
        rollups = {"by_workflow": social_performance_collector._rollup_rows(posts, "workflow")}
        learnings = social_performance_collector._derive_learnings(posts, rollups)
        top = next(item for item in learnings if item.key == "top_workflow")
        self.assertIn("meme", top.headline)
        self.assertIn("2 of 11 posts measured", top.evidence)


class ReachRollupTests(unittest.TestCase):
    """Reach is the only metric here with real dynamic range (0 comments and 2
    saves across 30 live posts), but it is Instagram-only, so ranking on it has
    to stay inside one platform or it just compares platform mix."""

    def test_reach_rows_are_grouped_within_platform(self) -> None:
        posts = [
            _post("duckvideo", reach=180, post_id="d1"),
            _post("review_carousel", reach=26, post_id="r1"),
            _post("meme", platform="facebook", status="ok", like=1, post_id="f1"),
        ]
        rows = social_performance_collector._reach_rollup_rows(posts, "workflow")
        self.assertTrue(all(row["platform"] == "instagram" for row in rows))
        self.assertEqual(rows[0]["label"], "duckvideo")
        self.assertEqual(rows[0]["avg_reach"], 180.0)

    def test_posts_without_reach_are_skipped_not_zeroed(self) -> None:
        posts = [
            _post("meme", reach=100, post_id="m1"),
            _post("meme", like=3, post_id="m2"),
        ]
        rows = social_performance_collector._reach_rollup_rows(posts, "workflow")
        self.assertEqual(rows[0]["avg_reach"], 100.0)
        self.assertEqual(rows[0]["measured_post_count"], 1)


class FacebookFetchRecoveryTests(unittest.TestCase):
    """Both live Graph failures, from state/social_performance_posts.json:
    6 posts -> (#100) Tried accessing nonexisting field (permalink_url)
    3 posts -> Unsupported get request. Object with ID '1222...' does not exist
    The first threw away reaction/comment counts the object would have returned;
    the second needs the {page_id}_{post_id} form the working posts already use."""

    def _manager(self, responses):
        calls = []

        class _Resp:
            def __init__(self, status_code, payload, text=""):
                self.status_code = status_code
                self._payload = payload
                self.text = text

            def json(self):
                return self._payload

        class _Manager:
            def get_facebook_page_token(self):
                return "tok"

            def make_request(self, method, url, params=None, token_override=None):
                calls.append({"url": url, "params": params or {}})
                return responses(url, params or {}, _Resp)

        return _Manager(), calls

    def test_retries_without_permalink_url_on_field_error(self) -> None:
        field_error = {"error": {"message": "(#100) Tried accessing nonexisting field (permalink_url)", "code": 100}}

        def responses(url, params, Resp):
            if url.endswith("/insights"):
                return Resp(400, {}, "no insights")
            if "permalink_url" in (params.get("fields") or ""):
                return Resp(400, field_error, json.dumps(field_error))
            return Resp(200, {"id": "1", "reactions": {"summary": {"total_count": 5}}, "comments": {"summary": {"total_count": 1}}})

        manager, calls = self._manager(responses)
        result = social_performance_collector._fetch_facebook_metrics({"post_id": "554_1222"}, manager)

        self.assertEqual(result["metrics"]["reaction_count"], 5)
        self.assertEqual(result["metrics"]["comment_count"], 1)
        self.assertNotEqual(result["status"], "fetch_failed")
        self.assertTrue(any("permalink_url" not in (c["params"].get("fields") or "") for c in calls))

    def test_retries_with_page_qualified_id_when_bare_id_is_unknown(self) -> None:
        missing = {"error": {"message": "Unsupported get request. Object with ID '122253963512757684' does not exist", "code": 100}}

        def responses(url, params, Resp):
            if url.endswith("/insights"):
                return Resp(400, {}, "no insights")
            if "/554937081033679_122253963512757684" in url:
                return Resp(200, {"id": "x", "reactions": {"summary": {"total_count": 3}}})
            return Resp(400, missing, json.dumps(missing))

        manager, calls = self._manager(responses)
        with patch.dict("os.environ", {"META_PAGE_ID": "554937081033679"}):
            result = social_performance_collector._fetch_facebook_metrics(
                {"post_id": "122253963512757684"}, manager
            )

        self.assertEqual(result["metrics"]["reaction_count"], 3)
        self.assertNotEqual(result["status"], "fetch_failed")

    def test_gives_up_cleanly_when_every_candidate_fails(self) -> None:
        err = {"error": {"message": "nope", "code": 100}}

        def responses(url, params, Resp):
            return Resp(400, err, json.dumps(err))

        manager, _ = self._manager(responses)
        with patch.dict("os.environ", {"META_PAGE_ID": "554937081033679"}):
            result = social_performance_collector._fetch_facebook_metrics({"post_id": "1222"}, manager)

        self.assertEqual(result["status"], "fetch_failed")
        self.assertEqual(result["metrics"], {})
        self.assertTrue(result["errors"])


if __name__ == "__main__":
    unittest.main()

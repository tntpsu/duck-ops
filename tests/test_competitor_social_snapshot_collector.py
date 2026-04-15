from __future__ import annotations

import json
import sys
import urllib.error
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import competitor_social_snapshot_collector


class CompetitorSocialSnapshotCollectorTests(unittest.TestCase):
    def test_request_json_with_retries_recovers_after_retryable_http_error(self) -> None:
        payload = {"ok": True}
        calls = {"count": 0}

        def flaky_request(url: str, *, referer_handle: str | None = None) -> dict:
            calls["count"] += 1
            if calls["count"] == 1:
                raise urllib.error.HTTPError(url, 401, "Unauthorized", hdrs=None, fp=None)
            return payload

        with patch.object(competitor_social_snapshot_collector, "_request_json", side_effect=flaky_request), patch.object(
            competitor_social_snapshot_collector.time, "sleep", lambda *_args, **_kwargs: None
        ):
            result, attempts = competitor_social_snapshot_collector._request_json_with_retries(
                "https://example.com/profile",
                referer_handle="example",
            )

        self.assertEqual(result, payload)
        self.assertEqual(calls["count"], 2)
        self.assertEqual(
            attempts,
            [
                {"attempt": 1, "outcome": "http_error", "code": 401},
                {"attempt": 2, "outcome": "success"},
            ],
        )

    def test_build_competitor_social_snapshots_collects_profiles_and_posts(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config" / "competitor_social_sources.json"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(
                json.dumps(
                    {
                        "collection_boundary": {
                            "max_accounts_per_run": 10,
                            "latest_posts_per_account": 2,
                        },
                        "seed_accounts": [
                            {
                                "brand_key": "wilder",
                                "display_name": "Wilderkind Studio",
                                "instagram_handle": "wilderkind.studio",
                                "verification_status": "confirmed",
                                "confidence": "high",
                                "category": "direct",
                                "reason": "Overlap",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            state_path = root / "state" / "competitor_social_snapshots.json"
            history_path = root / "state" / "competitor_social_snapshot_history.json"
            operator_json_path = root / "output" / "operator" / "competitor_social_snapshots.json"
            markdown_path = root / "output" / "operator" / "competitor_social_snapshots.md"

            profile_payload = {
                "data": {
                    "user": {
                        "full_name": "Wilderkind Studio",
                        "biography": "Duck Duck Fun collectibles",
                        "external_url": "https://example.com",
                        "edge_followed_by": {"count": 93},
                        "edge_follow": {"count": 304},
                        "edge_owner_to_timeline_media": {"count": 93},
                        "is_private": False,
                        "is_verified": False,
                        "profile_pic_url": "https://example.com/pic.jpg",
                        "eimu_id": "1784",
                    }
                }
            }
            timeline_payload = {
                "items": [
                    {
                        "pk": "1",
                        "code": "ABC123",
                        "taken_at": 1759797656,
                        "media_type": 2,
                        "product_type": "clips",
                        "like_count": 6,
                        "comment_count": 1,
                        "play_count": 158,
                        "caption": {"text": "Showgirl Duck is ready for her debut. Would you give her a spot? #showgirl #duck"},
                    }
                ]
            }

            def fake_request_json(url: str, *, referer_handle: str | None = None) -> dict:
                if "web_profile_info" in url:
                    return profile_payload
                return timeline_payload

            with patch.object(competitor_social_snapshot_collector, "CONFIG_PATH", config_path), patch.object(
                competitor_social_snapshot_collector, "STATE_PATH", state_path
            ), patch.object(
                competitor_social_snapshot_collector, "HISTORY_PATH", history_path
            ), patch.object(
                competitor_social_snapshot_collector, "OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                competitor_social_snapshot_collector, "OUTPUT_MD_PATH", markdown_path
            ), patch.object(
                competitor_social_snapshot_collector, "_request_json", side_effect=fake_request_json
            ), patch.object(
                competitor_social_snapshot_collector.time, "sleep", lambda *_args, **_kwargs: None
            ):
                payload = competitor_social_snapshot_collector.build_competitor_social_snapshots()
                self.assertTrue(state_path.exists())
                self.assertTrue(history_path.exists())
                self.assertTrue(operator_json_path.exists())
                self.assertTrue(markdown_path.exists())

        self.assertEqual(payload["summary"]["collected_account_count"], 1)
        self.assertEqual(payload["summary"]["failed_account_count"], 0)
        self.assertEqual(payload["summary"]["degraded_account_count"], 0)
        self.assertEqual(payload["summary"]["post_count"], 1)
        self.assertEqual(payload["profiles"][0]["account_handle"], "wilderkind.studio")
        self.assertEqual(payload["posts"][0]["post_format"], "reel")
        self.assertEqual(payload["posts"][0]["hook_family"], "engagement_prompt")
        self.assertEqual(payload["summary"]["html_profile_account_count"], 0)
        self.assertEqual(payload["summary"]["profile_only_account_count"], 0)

    def test_build_competitor_social_snapshots_reuses_cached_posts_on_timeline_failure(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config" / "competitor_social_sources.json"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(
                json.dumps(
                    {
                        "collection_boundary": {
                            "max_accounts_per_run": 10,
                            "latest_posts_per_account": 2,
                        },
                        "seed_accounts": [
                            {
                                "brand_key": "wilder",
                                "display_name": "Wilderkind Studio",
                                "instagram_handle": "wilderkind.studio",
                                "verification_status": "confirmed",
                                "confidence": "high",
                                "category": "direct",
                                "reason": "Overlap",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            state_path = root / "state" / "competitor_social_snapshots.json"
            history_path = root / "state" / "competitor_social_snapshot_history.json"
            operator_json_path = root / "output" / "operator" / "competitor_social_snapshots.json"
            markdown_path = root / "output" / "operator" / "competitor_social_snapshots.md"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "profiles": [
                            {
                                "account_handle": "wilderkind.studio",
                                "full_name": "Old Wilderkind Studio",
                                "snapshot_source": "live",
                                "observed_at": "2026-04-14T10:00:00-04:00",
                            }
                        ],
                        "posts": [
                            {
                                "account_handle": "wilderkind.studio",
                                "post_url": "https://www.instagram.com/p/OLDPOST/",
                                "observed_at": "2026-04-14T10:00:00-04:00",
                                "post_format": "image",
                                "hook_family": "statement_showcase",
                                "theme": "wedding",
                                "engagement_visible": {"likes": 5, "comments": 1},
                                "engagement_score": 9.0,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            profile_payload = {
                "data": {
                    "user": {
                        "full_name": "Wilderkind Studio",
                        "biography": "Duck Duck Fun collectibles",
                    }
                }
            }

            def fake_request_json(url: str, *, referer_handle: str | None = None) -> dict:
                if "web_profile_info" in url:
                    return profile_payload
                raise urllib.error.HTTPError(url, 401, "Unauthorized", hdrs=None, fp=None)

            with patch.object(competitor_social_snapshot_collector, "CONFIG_PATH", config_path), patch.object(
                competitor_social_snapshot_collector, "STATE_PATH", state_path
            ), patch.object(
                competitor_social_snapshot_collector, "HISTORY_PATH", history_path
            ), patch.object(
                competitor_social_snapshot_collector, "OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                competitor_social_snapshot_collector, "OUTPUT_MD_PATH", markdown_path
            ), patch.object(
                competitor_social_snapshot_collector, "_request_json", side_effect=fake_request_json
            ), patch.object(
                competitor_social_snapshot_collector.time, "sleep", lambda *_args, **_kwargs: None
            ):
                payload = competitor_social_snapshot_collector.build_competitor_social_snapshots()

        self.assertEqual(payload["summary"]["collected_account_count"], 1)
        self.assertEqual(payload["summary"]["failed_account_count"], 0)
        self.assertEqual(payload["summary"]["degraded_account_count"], 1)
        self.assertEqual(payload["summary"]["cached_account_count"], 1)
        self.assertEqual(payload["summary"]["post_count"], 1)
        self.assertEqual(payload["profiles"][0]["snapshot_source"], "live_profile_cached_posts")
        self.assertEqual(payload["posts"][0]["snapshot_source"], "cached")
        self.assertTrue(payload["failures"][0]["fallback_used"])
        self.assertEqual(payload["failures"][0]["failure_class"], "timeline_http_error")

    def test_build_competitor_social_snapshots_uses_html_profile_fallback_when_profile_api_is_rate_limited(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config" / "competitor_social_sources.json"
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(
                json.dumps(
                    {
                        "collection_boundary": {
                            "max_accounts_per_run": 10,
                            "latest_posts_per_account": 2,
                        },
                        "seed_accounts": [
                            {
                                "brand_key": "wilder",
                                "display_name": "Wilderkind Studio",
                                "instagram_handle": "wilderkind.studio",
                                "verification_status": "confirmed",
                                "confidence": "high",
                                "category": "direct",
                                "reason": "Overlap",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            state_path = root / "state" / "competitor_social_snapshots.json"
            history_path = root / "state" / "competitor_social_snapshot_history.json"
            operator_json_path = root / "output" / "operator" / "competitor_social_snapshots.json"
            markdown_path = root / "output" / "operator" / "competitor_social_snapshots.md"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "profiles": [
                            {
                                "account_handle": "wilderkind.studio",
                                "full_name": "Old Wilderkind Studio",
                                "snapshot_source": "live",
                                "observed_at": "2026-04-14T10:00:00-04:00",
                            }
                        ],
                        "posts": [
                            {
                                "account_handle": "wilderkind.studio",
                                "post_url": "https://www.instagram.com/p/OLDPOST/",
                                "observed_at": "2026-04-14T10:00:00-04:00",
                                "post_format": "image",
                                "hook_family": "statement_showcase",
                                "theme": "wedding",
                                "engagement_visible": {"likes": 5, "comments": 1},
                                "engagement_score": 9.0,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            html_text = """
            <html><head>
            <meta content="93 Followers, 304 Following, 93 Posts - Wilderkind Studio (@wilderkind.studio) on Instagram: &quot;Modern keepsakes for Wild Hearts&quot;" name="description" />
            <meta property="og:title" content="Wilderkind Studio (@wilderkind.studio) • Instagram photos and videos" />
            </head></html>
            """

            def fake_request_json(url: str, *, referer_handle: str | None = None) -> dict:
                raise urllib.error.HTTPError(url, 401, "Unauthorized", hdrs=None, fp=None)

            with patch.object(competitor_social_snapshot_collector, "CONFIG_PATH", config_path), patch.object(
                competitor_social_snapshot_collector, "STATE_PATH", state_path
            ), patch.object(
                competitor_social_snapshot_collector, "HISTORY_PATH", history_path
            ), patch.object(
                competitor_social_snapshot_collector, "OPERATOR_JSON_PATH", operator_json_path
            ), patch.object(
                competitor_social_snapshot_collector, "OUTPUT_MD_PATH", markdown_path
            ), patch.object(
                competitor_social_snapshot_collector, "_request_json", side_effect=fake_request_json
            ), patch.object(
                competitor_social_snapshot_collector, "_request_text_with_retries", return_value=(html_text, [{"attempt": 1, "outcome": "success"}])
            ), patch.object(
                competitor_social_snapshot_collector.time, "sleep", lambda *_args, **_kwargs: None
            ):
                payload = competitor_social_snapshot_collector.build_competitor_social_snapshots()

        self.assertEqual(payload["summary"]["collected_account_count"], 1)
        self.assertEqual(payload["summary"]["failed_account_count"], 0)
        self.assertEqual(payload["summary"]["degraded_account_count"], 1)
        self.assertEqual(payload["summary"]["cached_account_count"], 1)
        self.assertEqual(payload["summary"]["html_profile_account_count"], 1)
        self.assertEqual(payload["summary"]["profile_only_account_count"], 0)
        self.assertEqual(payload["profiles"][0]["snapshot_source"], "html_profile_cached_posts")
        self.assertEqual(payload["profiles"][0]["follower_count"], 93)
        self.assertEqual(payload["posts"][0]["snapshot_source"], "cached")
        self.assertTrue(payload["failures"][0]["fallback_used"])
        self.assertEqual(payload["failures"][0]["failure_class"], "profile_api_rate_limited_html_profile_cached_posts")


if __name__ == "__main__":
    unittest.main()

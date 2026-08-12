"""Surface 65 tier 1: viral-post surfacing + median honesty in the
competitor social benchmark collector."""
from __future__ import annotations

import competitor_social_benchmark_collector as col


def _posts(scores, fmt="reel"):
    return [{
        "post_format": fmt, "hook_family": "statement_showcase",
        "account_handle": f"acct{i}", "visible_hook": f"Hook {i}",
        "caption_excerpt": f"Caption {i}", "theme": "ducks",
        "post_url": f"https://instagram.com/p/{i}/", "post_date": "2026-08-10",
        "engagement_score": s,
    } for i, s in enumerate(scores)]


class TestMedianHonesty:
    def test_median_and_max_alongside_mean(self):
        # One 12k outlier among modest scores — the mean lies, the median doesn't.
        rows = col._top_dimensions(_posts([10, 20, 30, 40, 12000]), "post_format")
        row = rows[0]
        assert row["median_engagement_score"] == 30
        assert row["max_engagement_score"] == 12000
        assert row["avg_engagement_score"] > 2000  # mean dragged by the outlier
        assert row["median_engagement_score"] != row["avg_engagement_score"]


class TestTopViralPosts:
    def test_ranked_with_links(self):
        viral = col._top_viral_posts(_posts([5, 500, 50]))
        assert [v["engagement_score"] for v in viral] == [500, 50, 5]
        assert viral[0]["post_url"].startswith("https://instagram.com/")
        assert viral[0]["visible_hook"]

    def test_empty_posts_no_crash(self):
        assert col._top_viral_posts([]) == []


class TestSocialPriors:
    def test_priors_derived_from_median_not_mean(self):
        # reels: median 30 w/ one huge outlier; images: median 100 —
        # median-ranked priors must pick IMAGE despite the reel outlier mean.
        posts = _posts([10, 20, 30, 40, 12000], fmt="reel") + _posts(
            [90, 100, 110, 120, 130], fmt="image")
        by_format = col._top_dimensions(posts, "post_format")
        by_hook = col._top_dimensions(posts, "hook_family")
        viral = col._top_viral_posts(posts)
        priors = col._social_priors(posts, by_format, by_hook, viral)
        assert priors["winning_format"] == "image"
        assert priors["winning_hook_family"] == "statement_showcase"
        assert priors["sample_hooks"]

    def test_thin_sample_omits_priors(self):
        posts = _posts([10, 20, 30])  # < 10 posts
        assert col._social_priors(posts, [], [], []) is None

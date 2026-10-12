"""Engagement is split into passive and active, and never averaged across platforms.

Both halves come from the same 2026-10-12 reading of our own data:

* Every measured meme scored on likes alone -- comments and saves were exactly
  zero, every week -- while jeepfact carousels earned both nearly every week.
  Summed into one number that reads as "memes are a bit weaker". Split apart it
  reads as "memes have never earned the two signals that matter".
* Every Facebook post returned an engagement rate of 0.000. Averaging Facebook
  in with Instagram ranked a lane partly on how often it cross-posts, which has
  nothing to do with its creative.
"""
from __future__ import annotations

from runtime.social_performance_collector import (
    _active_engagement,
    _derive_learnings,
    _engagement_score,
    _passive_engagement,
    _rollup_rows,
)


def _post(workflow, platform, *, likes=0, comments=0, saved=0, shares=0, reactions=0, theme="t"):
    metrics = (
        {"reaction_count": reactions, "comment_count": comments, "share_count": shares, "reach": 50}
        if platform == "facebook"
        else {"like_count": likes, "comments_count": comments, "saved": saved, "reach": 50}
    )
    return {
        "workflow": workflow, "platform": platform, "theme": theme,
        "metric_status": "ok", "metrics": metrics, "post_id": f"{workflow}-{platform}-{likes}",
        "time_window": "evening", "duck_family": workflow, "title": workflow,
    }


def test_likes_are_passive_and_comments_and_saves_are_active():
    post = _post("meme", "instagram", likes=7, comments=2, saved=1)
    assert _passive_engagement(post) == 7
    assert _active_engagement(post) == 3
    # The combined score keeps its old meaning so nothing downstream shifts.
    assert _engagement_score(post) == 10


def test_facebook_counts_reactions_and_shares_on_the_right_sides():
    post = _post("meme", "facebook", reactions=4, comments=1, shares=2)
    assert _passive_engagement(post) == 4
    assert _active_engagement(post) == 3


def test_a_lane_with_only_likes_reports_zero_active():
    """The exact shape of every meme we had measured."""
    posts = [_post("meme", "instagram", likes=n) for n in (3, 5, 5, 7)]
    row = _rollup_rows(posts, "workflow")[0]
    assert row["label"] == "instagram / meme"
    assert row["avg_passive_engagement"] == 5.0
    assert row["avg_active_engagement"] == 0.0
    assert row["active_post_count"] == 0


def test_rollups_do_not_average_across_platforms():
    """A lane that cross-posts must not be penalised for the platform that
    returns nothing -- that was scoring distribution, not creative."""
    posts = [_post("meme", "instagram", likes=10) for _ in range(3)]
    posts += [_post("meme", "facebook", reactions=0) for _ in range(3)]
    labels = {row["label"]: row for row in _rollup_rows(posts, "workflow")}
    assert set(labels) == {"instagram / meme", "facebook / meme"}
    assert labels["instagram / meme"]["avg_engagement_score"] == 10.0
    assert labels["facebook / meme"]["avg_engagement_score"] == 0.0


def test_the_platform_rollup_itself_is_not_double_grouped():
    """Grouping the by-platform rows BY platform would give 'instagram /
    instagram'."""
    posts = [_post("meme", "instagram", likes=1) for _ in range(3)]
    assert _rollup_rows(posts, "platform")[0]["label"] == "instagram"


def test_a_learning_names_the_platform_gap():
    posts = [_post("meme", "instagram", likes=10) for _ in range(4)]
    posts += [_post("meme", "facebook", reactions=0) for _ in range(4)]
    rollups = {"by_workflow": _rollup_rows(posts, "workflow")}
    keys = {learning.key for learning in _derive_learnings(posts, rollups)}
    assert "platform_gap" in keys


def test_a_learning_names_the_lane_that_never_earns_a_comment_or_save():
    posts = [_post("meme", "instagram", likes=5) for _ in range(4)]
    learnings = {l.key: l for l in _derive_learnings(posts, {"by_workflow": _rollup_rows(posts, "workflow")})}
    assert "no_active_engagement_instagram" in learnings
    assert "meme" in learnings["no_active_engagement_instagram"].headline


def test_a_lane_that_earns_saves_is_not_flagged():
    """The card must stay quiet for a lane that is working, or it is noise."""
    posts = [_post("jeepfact", "instagram", likes=6, saved=1) for _ in range(4)]
    keys = {l.key for l in _derive_learnings(posts, {"by_workflow": _rollup_rows(posts, "workflow")})}
    assert "no_active_engagement_instagram" not in keys


def test_a_platform_with_too_few_posts_is_not_judged():
    """Two posts is not evidence that a platform is dead."""
    posts = [_post("meme", "instagram", likes=10) for _ in range(4)]
    posts += [_post("meme", "facebook", reactions=0) for _ in range(2)]
    keys = {l.key for l in _derive_learnings(posts, {"by_workflow": _rollup_rows(posts, "workflow")})}
    assert "platform_gap" not in keys

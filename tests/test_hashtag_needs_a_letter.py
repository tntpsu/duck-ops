""""rule #1" in a caption is prose, not a hashtag (2026-11-23 meme receipt)."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runtime"))

import competitor_social_snapshot_collector as competitor  # noqa: E402
import social_performance_collector as ours  # noqa: E402

CAPTION = "Thanksgiving road trip rule #1: priority #MyJeepDuck #2026 #1st"


def test_our_collector_skips_numbers_after_a_hash():
    assert ours._extract_hashtags(CAPTION) == ["MyJeepDuck", "1st"]


def test_competitor_collector_skips_numbers_after_a_hash():
    assert competitor._extract_hashtags(CAPTION) == ["MyJeepDuck", "1st"]

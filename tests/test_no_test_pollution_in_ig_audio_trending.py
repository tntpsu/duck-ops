"""Belt-and-suspenders pollution audit for the Instagram audio snapshot.

Scans the PRODUCTION duck-ops/state/ig_audio_trending.json (and its history)
for content only a test run could have written. Fails loud if any is found.

Three layers of defence, matching ai-agents/CLAUDE.md section 4:

  1. conftest autouse fixtures in BOTH repos redirect the snapshot paths to
     tmp — duckAgent via `duckagent_test_ig_audio_guard`, imported by all three
     of its test roots, and duck-ops by patching the same two constants on
     `helpers.ig_audio_helper`.
  2. `scripts/ig_audio_snapshot._write_atomic` raises `TestModeRefusalError`
     when DUCK_TEST_MODE=1 and the resolved target is the frozen production
     path; `ig_audio_helper._require_not_test_mode` refuses the outbound API
     call itself, AHEAD of the credentials check, because `.env` loads a live
     META_ACCESS_TOKEN under pytest.
  3. THIS test — audits the real files at suite time, catching anything that
     slipped past both (a subprocess, a re-import, a direct write).

The marker is STRUCTURAL rather than a list of fixture names, so it does not
need maintaining as fixtures change: a real Instagram `audio_id` is a long
numeric string (measured 2026-11-11: 15-17 digits across 697 live rows), while
every plausible fixture id is short or non-numeric. Asserting the RULE rather
than today's values ([[feedback_pin_the_rule_not_the_reading]]).

If this fails, find the polluting test and fix ITS isolation. Do not widen the
rule to excuse it, and do not delete this test.
"""
from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

_STATE = Path(__file__).resolve().parents[1] / "state"
PRODUCTION_SNAPSHOT = _STATE / "ig_audio_trending.json"
PRODUCTION_HISTORY = _STATE / "ig_audio_trending_history.json"

#: Shortest real id observed in the live catalog is 15 digits. 10 is a floor
#: with room to spare that no hand-written fixture id reaches.
_MIN_REAL_AUDIO_ID_DIGITS = 10


def _bad_audio_id(audio_id: object) -> str | None:
    text = str(audio_id)
    if not text.isdigit():
        return f"audio_id {text!r} is not numeric"
    if len(text) < _MIN_REAL_AUDIO_ID_DIGITS:
        return (f"audio_id {text!r} has {len(text)} digits, "
                f"fewer than the {_MIN_REAL_AUDIO_ID_DIGITS} a real one has")
    return None


def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with open(path) as handle:
            loaded = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise AssertionError(f"{path} is not readable JSON: {exc}") from exc
    return loaded if isinstance(loaded, dict) else {}


class TestNoTestPollutionInIgAudioTrending(unittest.TestCase):
    def test_every_snapshot_audio_id_looks_real(self) -> None:
        snapshot = _load(PRODUCTION_SNAPSHOT)
        if not snapshot:
            self.skipTest("no production snapshot yet — producer has not run")
        findings = []
        for audio_type, payload in (snapshot.get("by_type") or {}).items():
            for track in (payload or {}).get("tracks") or []:
                reason = _bad_audio_id((track or {}).get("audio_id"))
                if reason:
                    findings.append(f"{audio_type}: {reason}")
        self.assertEqual(findings[:10], [], f"{len(findings)} suspicious ids")

    def test_every_history_rank_key_looks_real(self) -> None:
        history = _load(PRODUCTION_HISTORY)
        snapshots = history.get("snapshots") or []
        if not snapshots:
            self.skipTest("no production history yet — producer has not run")
        findings = []
        for entry in snapshots:
            for audio_type, ranks in ((entry or {}).get("ranks") or {}).items():
                for audio_id in list(ranks or {})[:200]:
                    reason = _bad_audio_id(audio_id)
                    if reason:
                        findings.append(f"{entry.get('captured_at')} {audio_type}: {reason}")
        self.assertEqual(findings[:10], [], f"{len(findings)} suspicious ids")

    def test_the_snapshot_is_not_stamped_in_the_future(self) -> None:
        """A frozen or hand-built clock is a test write, not a producer run."""
        snapshot = _load(PRODUCTION_SNAPSHOT)
        if not snapshot:
            self.skipTest("no production snapshot yet — producer has not run")
        stamped = snapshot.get("captured_at")
        self.assertTrue(stamped, "production snapshot has no captured_at")
        when = datetime.fromisoformat(str(stamped).replace("Z", "+00:00"))
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        # One hour of slack for clock skew; a fixture clock is usually years off.
        self.assertLess(
            when, datetime.now(timezone.utc) + timedelta(hours=1),
            f"captured_at {stamped} is in the future — a test wrote this file",
        )


if __name__ == "__main__":
    unittest.main()

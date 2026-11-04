"""A daily free check that the model ids we ship still exist.

2026-10-22 Google shut down every `veo-3.1-*` id. duckvideo kept running — it
caught the 404, logged `ai_motion_failed`, rendered a ken-burns clip and
emailed it as normal — so nothing alarmed and the operator found out six weeks
later by watching a bad reel. The gated eval's preflight would have caught it
on the day, but it is manual-trigger because it also spends money generating a
clip. The preflight half costs nothing: one ListModels call.

The grader's job is to be useful in three different bad states and tell them
apart: a REQUIRED id gone (a shipped surface is broken), an optional one gone
(the fallback ladder is thinning), and a probe that could not run at all —
which is never green ([[feedback_a_failed_probe_is_a_fact_about_the_probe]]).
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from runtime import google_model_availability as gma  # noqa: E402


def _entry(model_id, *, required=True, **extra):
    row = {"id": model_id, "purpose": "test", "source": "test", "required": required}
    row.update(extra)
    return row


def _listing(*names):
    return {"names": list(names)}


class TestTheGrade:
    def test_everything_present_is_green(self):
        payload = gma.grade([_entry("gemini-omni-1.1-flash")],
                            _listing("models/gemini-omni-1.1-flash"))
        assert payload["status"] == "green"
        assert payload["missing_required"] == []

    def test_a_missing_required_id_is_red(self):
        payload = gma.grade([_entry("veo-3.1-fast-generate-preview")],
                            _listing("models/gemini-omni-1.1-flash"))
        assert payload["status"] == "red"
        assert payload["missing_required"] == ["veo-3.1-fast-generate-preview"]

    def test_the_red_reason_names_the_dead_id(self):
        """"A model is missing" sends nobody anywhere."""
        payload = gma.grade([_entry("veo-3.1-fast-generate-preview")], _listing("x"))
        assert "veo-3.1-fast-generate-preview" in payload["status_reason"]

    def test_the_red_action_points_at_the_override(self):
        payload = gma.grade([_entry("veo-3.1")], _listing("x"))
        assert "DUCKVIDEO_VIDEO_MODEL" in payload["recommended_action"]

    def test_a_missing_optional_id_is_only_yellow(self):
        """The fallback ladder thinning is worth saying, not worth going red
        over — the primary still works."""
        payload = gma.grade(
            [_entry("models/gemini-2.5-flash-image"),
             _entry("models/imagen-4.0-generate-001", required=False)],
            _listing("models/gemini-2.5-flash-image"))
        assert payload["status"] == "yellow"
        assert payload["missing_optional"] == ["models/imagen-4.0-generate-001"]
        assert payload["missing_required"] == []

    def test_required_beats_optional_in_the_verdict(self):
        payload = gma.grade(
            [_entry("gone-primary"), _entry("gone-fallback", required=False)],
            _listing("something-else"))
        assert payload["status"] == "red"

    def test_an_id_that_could_not_be_collected_is_yellow_not_green(self):
        """An import that failed means we checked FEWER things, which must not
        look like checking everything and liking it."""
        payload = gma.grade([_entry(None, error="ImportError: boom")],
                            _listing("models/x"))
        assert payload["status"] == "yellow"
        assert payload["uncollected"]

    def test_every_model_carries_its_own_verdict(self):
        payload = gma.grade(
            [_entry("alive"), _entry("dead", required=False)], _listing("models/alive"))
        verdicts = {m["id"]: m["verdict"] for m in payload["models"]}
        assert verdicts == {"alive": "ok", "dead": "missing"}

    def test_the_source_constant_rides_along(self):
        """So the operator knows WHICH constant to edit, not just which id."""
        payload = gma.grade(
            [_entry("dead", source="helpers.duck_video_motion_helper.DEFAULT_VIDEO_MODEL")],
            _listing("alive"))
        assert payload["models"][0]["source"].endswith("DEFAULT_VIDEO_MODEL")


class TestAFailedProbeIsNeverGreen:
    @pytest.mark.parametrize("error", ["no_gemini_key", "empty_model_list",
                                       "ClientError: 503"])
    def test_a_listing_error_grades_unknown(self, error):
        payload = gma.grade([_entry("anything")], {"error": error})
        assert payload["status"] == "unknown"
        assert payload["listing_error"] == error

    def test_nothing_is_marked_missing_when_we_could_not_look(self):
        """Reporting a dead model on the strength of a failed probe is the
        "VIEWER DOWN" mistake — a confident story from a broken measurement."""
        payload = gma.grade([_entry("anything")], {"error": "no_gemini_key"})
        assert payload["missing_required"] == []
        assert payload["models"][0]["verdict"] == "unchecked"
        assert payload["models"][0]["resolves"] is None

    def test_the_unknown_reason_says_what_could_not_be_done(self):
        payload = gma.grade([_entry("anything")], {"error": "no_gemini_key"})
        assert "no_gemini_key" in payload["status_reason"]

    def test_no_configured_ids_at_all_is_not_green_by_default(self):
        """An empty inventory means the collector broke, not that everything
        is fine."""
        payload = gma.grade([_entry(None, error="collector failed")], _listing("x"))
        assert payload["status"] != "green"


class TestIdSpelling:
    """ListModels returns `models/x`; call sites configure both spellings."""

    def test_a_bare_id_matches_a_prefixed_listing(self):
        assert gma._resolves("gemini-omni-1.1-flash", ["models/gemini-omni-1.1-flash"])

    def test_a_prefixed_id_matches_a_prefixed_listing(self):
        assert gma._resolves("models/gemini-2.5-flash-image",
                             ["models/gemini-2.5-flash-image"])

    def test_a_different_model_does_not_match(self):
        assert not gma._resolves("gemini-2.5-flash", ["models/gemini-2.5-flash-image"])

    def test_a_substring_is_not_a_match(self):
        """`veo-3.1` must not be satisfied by `veo-3.1-fast-generate-preview`
        — they are different models and one of them is dead."""
        assert not gma._resolves("veo-3.1", ["models/veo-3.1-fast-generate-preview"])


class TestTheProbeNeverRaises:
    def test_a_missing_key_is_a_result_not_an_exception(self, monkeypatch):
        monkeypatch.delenv("GEMINI_KEY", raising=False)
        assert gma.live_model_names() == {"error": "no_gemini_key"}

    def test_an_sdk_explosion_is_a_result(self, monkeypatch):
        import google.genai

        monkeypatch.setattr(google.genai, "Client",
                            lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")))
        out = gma.live_model_names(api_key="k")
        assert "RuntimeError" in out["error"]

    def test_an_empty_list_is_an_error_not_a_catalog(self, monkeypatch):
        """Zero models means the call did not do what we think, and grading
        every configured id "missing" off that would be a false alarm."""
        import google.genai

        class _Models:
            @staticmethod
            def list():
                return []

        monkeypatch.setattr(google.genai, "Client",
                            lambda **kw: type("C", (), {"models": _Models})())
        assert gma.live_model_names(api_key="k") == {"error": "empty_model_list"}

    def test_a_broken_inventory_import_still_returns_rows(self, monkeypatch):
        monkeypatch.setattr(gma, "DUCK_AGENT_ROOT", Path("/nonexistent"))
        monkeypatch.setitem(sys.modules, "helpers.google_model_inventory", None)
        rows = gma.configured_ids()
        assert rows and rows[0].get("required") is True


class TestWritesStayOutOfProduction:
    def test_it_writes_atomically_to_a_tmp_path(self, tmp_path):
        target = tmp_path / "google_model_availability.json"
        gma.write_state({"status": "green"}, target)
        assert json.loads(target.read_text())["status"] == "green"
        assert not list(tmp_path.glob("*.tmp"))

    def test_test_mode_refuses_the_frozen_production_path(self, monkeypatch):
        """Layer 2 of the 3-layer convention: the source itself refuses, so a
        test that slips past conftest cannot paint the OS card with fixtures.

        Asserted against _FROZEN_STATE_PATH, not STATE_PATH — layer 1 (the
        autouse conftest redirect) has already pointed STATE_PATH at a tmp
        file by the time this runs, which is the whole idea. The guard's
        contract is about the factory default, so that is what it is tested
        against."""
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        with pytest.raises(gma.GoogleModelAvailabilityTestModeRefusal):
            gma.write_state({"status": "red"}, gma._FROZEN_STATE_PATH)

    def test_layer_one_has_already_redirected_the_live_constant(self):
        """If this ever fails, the conftest redirect was dropped and every
        producer test in this file is one bug away from the OS card."""
        assert Path(gma.STATE_PATH).resolve() != gma._FROZEN_STATE_PATH

    def test_test_mode_allows_a_tmp_path(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        gma.write_state({"status": "green"}, tmp_path / "s.json")
        assert (tmp_path / "s.json").exists()

    def test_the_frozen_path_is_resolved_not_a_string_compare(self):
        """A symlinked or ../-containing path must not slip past the guard."""
        assert gma._FROZEN_STATE_PATH.is_absolute()
        assert ".." not in str(gma._FROZEN_STATE_PATH)


class TestNoTestPollutionInProductionState:
    """Layer 3: scan the live state file for fixture markers."""

    def test_the_production_state_holds_no_fixture_ids(self):
        path = gma._FROZEN_STATE_PATH
        if not path.exists():
            pytest.skip("producer has not run yet")
        blob = path.read_text(encoding="utf-8")
        for marker in ("gone-primary", "gone-fallback", "alive", "dead",
                       "anything", "collector failed"):
            assert f'"{marker}"' not in blob, f"test fixture {marker!r} in production state"

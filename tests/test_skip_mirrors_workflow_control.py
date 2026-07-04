"""Gap 1b regression (2026-07-03): skipping a publish-lane decision in the inbox
must mirror a 'dismissed' transition into workflow_control so the OS card clears.
The two stores were unsynced. Match on structured lane+run_id; trend/customer
items with no workflow_control run must no-op."""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "runtime"))
import review_loop as rl  # noqa: E402
import workflow_control as wc  # noqa: E402


def test_skip_dismisses_matching_run(monkeypatch):
    monkeypatch.setattr(wc, "list_workflow_states", lambda: [
        {"lane": "jeepfact", "run_id": "2026-07-01", "state": "proposed",
         "workflow_id": "jeepfact::2026-07-01", "display_label": "Jeep Fact"},
    ])
    calls = {}
    monkeypatch.setattr(wc, "record_workflow_transition", lambda **k: calls.update(k))
    rl._mirror_skip_to_workflow_control({"flow": "jeepfact", "run_id": "2026-07-01"})
    assert calls.get("state") == "dismissed"
    assert calls.get("workflow_id") == "jeepfact::2026-07-01"
    assert calls.get("state_reason") == "operator_skipped_in_inbox"


def test_skip_noops_when_no_matching_run(monkeypatch):
    monkeypatch.setattr(wc, "list_workflow_states", lambda: [
        {"lane": "jeepfact", "run_id": "2026-07-01", "state": "proposed", "workflow_id": "x"},
    ])
    calls = []
    monkeypatch.setattr(wc, "record_workflow_transition", lambda **k: calls.append(k))
    rl._mirror_skip_to_workflow_control({"flow": "thursday", "run_id": "2026-07-02"})  # no match
    rl._mirror_skip_to_workflow_control({"flow": "trend", "run_id": ""})               # trend, no run
    assert calls == []


def test_skip_skips_already_terminal(monkeypatch):
    monkeypatch.setattr(wc, "list_workflow_states", lambda: [
        {"lane": "jeepfact", "run_id": "2026-07-01", "state": "dismissed", "workflow_id": "x"},
    ])
    calls = []
    monkeypatch.setattr(wc, "record_workflow_transition", lambda **k: calls.append(k))
    rl._mirror_skip_to_workflow_control({"flow": "jeepfact", "run_id": "2026-07-01"})
    assert calls == []  # already dismissed → no redundant write

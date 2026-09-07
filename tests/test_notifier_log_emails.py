"""Surface 66.5 — the three OpenClaw machine-log emails (quality-gate digest,
trend digest, phase readiness) route through the cadence gate (default OFF,
operator-flippable on /portal/workflows); phase readiness counts only the
quality-gate population and labels its subject with the artifact's ISO week."""
from __future__ import annotations

import json
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import notifier  # noqa: E402
import email_cadence_gate  # noqa: E402


class LogEmailGatingTests(unittest.TestCase):
    def test_digest_kinds_route_to_off_policies(self) -> None:
        for kind, surface in (("digest", "quality_gate_digest"), ("trend_digest", "trend_digest"), ("phase_readiness", "phase_readiness")):
            artifact = {"kind": kind, "payload": {}, "json_path": Path("/tmp/x.json")}
            with mock.patch.object(notifier, "log_cadence_decision"):
                decision = notifier.cadence_gate_decision_for_artifact(artifact)
            self.assertIsNotNone(decision, kind)
            self.assertEqual(decision.surface_name, surface)
            self.assertFalse(decision.should_send, kind)
            self.assertEqual(email_cadence_gate.POLICIES[surface].cadence, "off")

    def test_truly_unmapped_kind_returns_none(self) -> None:
        artifact = {"kind": "urgent", "payload": {}, "json_path": Path("/tmp/u.json")}
        self.assertIsNone(notifier.cadence_gate_decision_for_artifact(artifact))

    def test_operator_can_turn_a_log_email_back_on(self) -> None:
        with TemporaryDirectory() as tmp:
            override_path = Path(tmp) / "overrides.json"
            override_path.write_text(json.dumps({"trend_digest": "daily"}))
            with mock.patch.object(email_cadence_gate, "EMAIL_CADENCE_OVERRIDES_PATH", override_path):
                decision = email_cadence_gate.should_send_email("trend_digest", {})
        self.assertTrue(decision.should_send)


class PhaseReadinessTests(unittest.TestCase):
    def test_counts_only_quality_gate_rows(self) -> None:
        now = datetime.now().astimezone()
        recent = (now - timedelta(days=1)).isoformat()
        rows = [
            {"evaluator": "trend_ranker", "decision": "watch", "evaluated_at": recent},
            {"evaluator": "trend_ranker", "decision": "worth_acting_on", "evaluated_at": recent},
            {"decision": "publish_ready", "evaluated_at": recent},
            {"decision": "needs_revision", "evaluated_at": recent},
            {"decision": "publish_ready"},  # no timestamp → NOT in window (was: counted forever)
        ]
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "state").mkdir()
            (root / "state" / "decision_history.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
            (root / "state" / "overrides.jsonl").write_text("")
            (root / "digests").mkdir()
            with mock.patch.object(notifier, "ROOT", root), \
                 mock.patch.object(notifier, "OUTPUT_DIGESTS", root / "digests"), \
                 mock.patch.object(notifier, "QUALITY_GATE_STATE_PATH", root / "state" / "missing.json"):
                summary = notifier.summarize_phase_readiness(now)
        self.assertIn("Collected 2 quality-gate decisions", summary["evidence"][0])
        self.assertIn("1 publish-ready, 1 needs-revision, 0 discard", summary["evidence"][-1])

    def test_subject_week_matches_artifact_week(self) -> None:
        with TemporaryDirectory() as tmp:
            json_path = Path(tmp) / "phase_readiness__2026-36.json"
            json_path.write_text("{}")
            md_path = Path(tmp) / "phase_readiness__2026-36.md"
            md_path.write_text("# readiness")
            settings = {"subjects": {"phase_readiness": "[OpenClaw Phase Readiness] <week>"}, "user": "a@b.c", "to": "a@b.c"}
            artifact = {"kind": "phase_readiness", "payload": {}, "json_path": json_path, "md_path": md_path}
            with mock.patch.object(notifier, "render_notifier_html", return_value="<p>x</p>"):
                msg = notifier.build_message(settings, artifact)
        self.assertEqual(msg["Subject"], "[OpenClaw Phase Readiness] 2026-36")


if __name__ == "__main__":
    unittest.main()

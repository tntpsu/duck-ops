"""Belt-and-suspenders pollution audit for the shared LLM cost ledger.

Scans the PRODUCTION duck-ops/state/llm_call_log.jsonl for rows that only a
test run could have written. Fails loud if any are found.

Three layers of defence now exist for this file, matching the policy in
ai-agents/CLAUDE.md section 4:

  1. conftest autouse fixtures in BOTH repos redirect the log path to tmp —
     duck-ops patches `llm_call_helpers.LLM_CALL_LOG_PATH`, and (since
     2026-11-10) both conftests also patch `helpers.llm_log._LLM_CALL_LOG_PATH`,
     duckAgent's separate writer to the same file.
  2. `helpers.llm_log.log_llm_call` raises TestModeRefusalError when
     DUCK_TEST_MODE=1 and the write would land on the frozen production path.
  3. THIS test — audits the real file at suite time, and catches anything that
     slipped past both (a subprocess, a re-import, a direct file write).

Why it exists: on 2026-05-31 ninety-six percent of this ledger turned out to be
test fixtures, and the Review Reply Rewriter card ran RED for a week on them.
duck-ops was fixed; duckAgent's writer to the same file was not, because no
test could reach a caller. Instrumenting the duckvideo video generation call on
2026-11-10 made one reachable and put 26 fixture rows in the ledger on the
first run. The rows were removed and the guards added in the same commit.

If this test fails, the right response is:
  a. Find the polluting test (grep tests/ for the marker it reports)
  b. Fix that test's isolation — do not widen the marker list to excuse it
  c. Then remove the polluted rows

Do NOT just delete this test if it starts failing.
"""
from __future__ import annotations

import json
import unittest
from pathlib import Path

PRODUCTION_LOG = Path(__file__).resolve().parents[1] / "state" / "llm_call_log.jsonl"

#: Model ids that exist only in test fixtures. A real provider never answers to
#: these, so a row naming one is proof of a test write rather than a heuristic.
_FIXTURE_MODELS = {
    "old-name-model",
    "new-name-model",
    "gpt-99-imaginary",
    "model-a",
    "model-b",
}

#: Synthetic artifact ids built from single-letter fixture concept ids. The
#: duckvideo instrumentation stamps `call::duckvideo::<concept_id>`, and real
#: concept ids are slugs like `snowy_dashboard_santa_grab_v1`.
_FIXTURE_ARTIFACT_IDS = {
    "call::duckvideo::c",
    "call::duckvideo::unknown",
    "call::unknown:x",
}


def _pollution_reason(row: dict) -> str | None:
    model = str(row.get("model") or "")
    if model in _FIXTURE_MODELS:
        return f"fixture model {model!r}"
    artifact_id = str(row.get("artifact_id") or "")
    if artifact_id in _FIXTURE_ARTIFACT_IDS:
        return f"fixture artifact_id {artifact_id!r}"
    return None


class TestTheProductionLedgerHoldsNoFixtures(unittest.TestCase):
    def test_no_fixture_rows_in_the_production_llm_call_log(self):
        if not PRODUCTION_LOG.exists():
            self.skipTest(f"no production ledger at {PRODUCTION_LOG}")
        offenders: list[str] = []
        with PRODUCTION_LOG.open("r", encoding="utf-8") as fh:
            for number, raw in enumerate(fh, start=1):
                line = raw.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    # Malformed lines are a separate concern, already counted
                    # by the producer's data_quality block.
                    continue
                if not isinstance(row, dict):
                    continue
                reason = _pollution_reason(row)
                if reason:
                    offenders.append(f"line {number}: {reason} at {row.get('at')}")
        self.assertEqual(
            offenders, [],
            "test fixtures reached the production cost ledger:\n  "
            + "\n  ".join(offenders[:20]),
        )

    def test_the_audit_recognises_the_rows_it_is_meant_to_catch(self):
        """An audit nobody has proved can fail is not evidence
        ([[feedback_a_failed_probe_is_a_fact_about_the_probe]])."""
        self.assertIsNotNone(_pollution_reason({"model": "new-name-model"}))
        self.assertIsNotNone(
            _pollution_reason({"model": "gemini-omni-1.1-flash",
                               "artifact_id": "call::duckvideo::c"}))
        self.assertIsNone(
            _pollution_reason({"model": "gemini-omni-1.1-flash",
                               "artifact_id": "call::duckvideo::snowy_dashboard_santa_grab_v1"}))


if __name__ == "__main__":
    unittest.main()

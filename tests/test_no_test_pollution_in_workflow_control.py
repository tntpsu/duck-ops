"""Belt-and-suspenders pollution audit.

Scans the PRODUCTION duck-ops/state/workflow_control/ directory for
any receipt with TEST-RUN markers. Fails loud if pollution is found.

Three layers of defense now exist:
  1. conftest.py autouse fixtures (duckAgent + duck-ops) patch the
     bridge function to a no-op stub. Default for every test.
  2. record_workflow_transition raises TestModeRefusalError when
     DUCK_TEST_MODE=1 AND the write would land in prod. Belt for
     when tests bypass the autouse fixture.
  3. THIS test — runs at suite end and audits the actual state dir.
     Catches anything that slips past both fixtures above.

If this test fails, the right response is:
  a. Find the polluting test (grep for the markers in tests/)
  b. Patch its autouse-bypass path (subprocess? re-import? direct
     file write?)
  c. Then archive the polluting receipt with a .bak suffix matching
     the operator's existing pattern.

Do NOT just delete this test if it starts failing.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

PRODUCTION_WC_DIR = Path("/Users/philtullai/ai-agents/duck-ops/state/workflow_control")


# Markers that identify test pollution. Mirrors viewer.py's
# _is_test_pollution_receipt — keep in sync.
def _is_test_pollution_receipt(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    run_id = str(payload.get("run_id") or "").strip()
    entity_id = str(payload.get("entity_id") or "").strip()
    if run_id == "TEST-RUN" or entity_id == "TEST-RUN":
        return True
    if run_id.startswith("TEST-RUN-"):
        return True
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    if str(metadata.get("template_reason") or "").strip().lower() == "test":
        return True
    return False


class NoTestPollutionInWorkflowControlTests(unittest.TestCase):
    def test_no_polluting_receipts_in_production_state(self) -> None:
        """Scan every *.json in production workflow_control. None
        should match the test-pollution markers.

        This test should be the LAST line of defense — by the time
        it fails, both conftest fixtures and the source-level
        TestModeRefusalError guard have already been bypassed.
        """
        if not PRODUCTION_WC_DIR.exists():
            # Repo hasn't run yet (CI from scratch). Nothing to audit.
            self.skipTest("Production workflow_control dir does not exist")

        offenders: list[tuple[str, dict]] = []
        for path in PRODUCTION_WC_DIR.glob("*.json"):
            # Skip operator-archived backups (.bak suffix is the
            # convention for "this used to be a real receipt but
            # we put it aside").
            if ".bak" in path.name:
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if _is_test_pollution_receipt(payload):
                # Surface enough detail to triage which test wrote it.
                offenders.append((
                    path.name,
                    {
                        "run_id": payload.get("run_id"),
                        "entity_id": payload.get("entity_id"),
                        "template_reason": (payload.get("metadata") or {}).get("template_reason"),
                        "updated_at": payload.get("updated_at"),
                    },
                ))

        if offenders:
            detail = "\n".join(f"  - {name}: {markers}" for name, markers in offenders)
            self.fail(
                f"Production workflow_control has {len(offenders)} polluting "
                f"receipt(s). Both conftest autouse fixtures and the source-"
                f"level DUCK_TEST_MODE guard were bypassed. Triage:\n"
                f"  1. Find the polluting test: grep -rn 'TEST-RUN' "
                f"     duckAgent/tests/ duck-ops/tests/\n"
                f"  2. Either patch the bypass path in tests, or extend the "
                f"     autouse fixtures in conftest.py.\n"
                f"  3. Archive each offending file with .bak.YYYYMMDD-reason.\n"
                f"Polluting receipts found:\n{detail}"
            )


if __name__ == "__main__":
    unittest.main()

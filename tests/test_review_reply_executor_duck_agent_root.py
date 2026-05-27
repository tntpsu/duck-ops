"""Regression test for DUCK_AGENT_ROOT path resolution.

Born from the 2026-05-26 silent drain failure: DUCK_AGENT_ROOT was
computed as `ROOT.parents[2] / "duckAgent"` which resolved to
`/Users/duckAgent` (one level too high). The bug slept until the
operator approved the carousel publish on 2026-05-26, which caused
the sidecar's daily review-reply drain to attempt to send an
auth-required alert email, which tried to load `helpers/email_helper.py`
from the wrong path, which crashed. The sidecar's bash glue had
`|| true` so the failure was logged but reported exit=0.

Two surfaces this pin protects:
1. The path itself must resolve to the real duckAgent dir
2. helpers/email_helper.py must exist there so _load_send_email works
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import review_reply_executor  # noqa: E402


class DuckAgentRootResolutionTests(unittest.TestCase):
    def test_duck_agent_root_resolves_to_sibling_directory(self) -> None:
        """DUCK_AGENT_ROOT must point at the duckAgent dir that
        actually exists on disk, not a phantom path like
        /Users/duckAgent. This trips on any future refactor that
        moves the runtime/ subdir or changes the parents[N] math."""
        root = review_reply_executor.DUCK_AGENT_ROOT
        # The expected shape: ai-agents/duckAgent
        self.assertTrue(
            root.exists(),
            f"DUCK_AGENT_ROOT must resolve to an existing dir; got {root!r}"
        )
        self.assertEqual(
            root.name, "duckAgent",
            f"DUCK_AGENT_ROOT must end in 'duckAgent', got {root.name!r}"
        )
        self.assertTrue(
            root.is_dir(),
            f"DUCK_AGENT_ROOT must be a directory; got {root!r}"
        )

    def test_email_helper_loadable_from_duck_agent_root(self) -> None:
        """The whole reason DUCK_AGENT_ROOT exists: _load_send_email
        falls back to importing helpers/email_helper.py from it when
        the package isn't on sys.path. The fallback file must exist."""
        helper_path = review_reply_executor.DUCK_AGENT_ROOT / "helpers" / "email_helper.py"
        self.assertTrue(
            helper_path.exists(),
            f"helpers/email_helper.py must exist under DUCK_AGENT_ROOT; got {helper_path!r}"
        )

    def test_duck_agent_root_is_sibling_of_duck_ops(self) -> None:
        """Explicit invariant: duckAgent and duck-ops sit side-by-side
        under ai-agents/. This is the cross-repo layout assumption
        baked into the cadence_gate_loader and several other paths."""
        duck_ops_root = Path(review_reply_executor.__file__).resolve().parents[1]
        duck_agent_root = review_reply_executor.DUCK_AGENT_ROOT
        self.assertEqual(
            duck_ops_root.parent, duck_agent_root.parent,
            f"duck-ops ({duck_ops_root}) and duckAgent ({duck_agent_root}) "
            f"must share the same parent directory"
        )


if __name__ == "__main__":
    unittest.main()

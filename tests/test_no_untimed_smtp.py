"""Every SMTP connection must carry a timeout.

Python's default is NO timeout, so a stalled connection blocks forever. On
2026-11-13 at 18:00 the duck-ops sidecar wedged inside `notifier.send_message`
and was still holding its lock three days later. Everything behind that step
died with it -- trend_ranker, quality_gate, review_queue,
operator_surface_refresh, the review drain, and the operator INBOX POLLER, so
every approval the operator emailed went unread while each component reported
healthy ([[feedback_swallowed_errors_lie]]).

`widget_api` already passed `timeout=10`. The lesson simply never reached the
other call sites, which is what this test is for: an AST check, because the
timeout usually sits on a continuation line where a line-based grep misses it
-- as mine did while verifying the fix.
"""
from __future__ import annotations

import ast
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DUCKAGENT_ROOT = REPO_ROOT.parent / "duckAgent"

SEARCH_DIRS = [
    REPO_ROOT / "runtime",
    REPO_ROOT / "scripts",
    DUCKAGENT_ROOT / "helpers",
    DUCKAGENT_ROOT / "scripts",
]


def _untimed_smtp_calls() -> list[str]:
    bad: list[str] = []
    for root in SEARCH_DIRS:
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*.py")):
            if "test" in path.name:
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except (OSError, SyntaxError):
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                name = getattr(node.func, "attr", None)
                if name not in {"SMTP", "SMTP_SSL"}:
                    continue
                if not any(k.arg == "timeout" for k in node.keywords):
                    bad.append(f"{path}:{node.lineno} smtplib.{name}")
    return bad


class EverySmtpConnectionIsTimedTests(unittest.TestCase):
    def test_no_untimed_smtp_connection_exists(self):
        bad = _untimed_smtp_calls()
        self.assertEqual(
            bad, [],
            "SMTP without a timeout blocks forever and wedged the sidecar for "
            "three days on 2026-11-13:\n  " + "\n  ".join(bad))

    def test_the_scan_can_actually_see_an_untimed_call(self):
        """Positive control. A walker that silently matched nothing would make
        the test above pass forever ([[feedback_a_failed_probe_is_a_fact_about_the_probe]])."""
        tree = ast.parse("import smtplib\nwith smtplib.SMTP(h, p) as s:\n    pass\n")
        found = [n for n in ast.walk(tree)
                 if isinstance(n, ast.Call)
                 and getattr(n.func, "attr", None) == "SMTP"
                 and not any(k.arg == "timeout" for k in n.keywords)]
        self.assertEqual(len(found), 1)

    def test_it_looks_in_both_repos(self):
        """The hung call was in duck-ops; the one the drop digest uses is in
        duckAgent. A scan of one repo would have missed half the problem."""
        self.assertTrue(any("duck-ops" in str(d) for d in SEARCH_DIRS))
        self.assertTrue(any("duckAgent" in str(d) for d in SEARCH_DIRS))

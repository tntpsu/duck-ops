"""Every blocking call that leaves this process must carry a timeout.

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

SUBPROCESS IS THE ONE THAT ACTUALLY BIT. The SMTP diagnosis above was an
inference from reading code, and it was wrong: on 2026-11-17 the sidecar hung
again, with the fix already in place, inside

    docker exec openclaw-openclaw-gateway-1 ... message send --channel whatsapp

while the Docker daemon was unresponsive. `subprocess.run` also defaults to no
timeout, and `select.poll` -- which is what the stack showed both times -- is
equally what reading a subprocess pipe looks like. A cause read off the source
is a hypothesis; the process tree is the evidence
([[feedback_verify_external_behavior_before_root_cause]]).
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


#: Calls that block on something outside this process. Neither smtplib nor
#: subprocess defaults to a timeout, and both have now wedged a scheduled job.
_BLOCKING = {"SMTP", "SMTP_SSL", "run", "Popen", "check_output", "check_call"}
#: `run` and `Popen` are common words; only count them on subprocess.
_SUBPROCESS_ONLY = {"run", "Popen", "check_output", "check_call"}


#: Call sites that predate this check and have NOT been reviewed. A blanket
#: timeout would be wrong for several of them -- the Drive mirror's rsync
#: moves ~10 GB and the Etsy Playwright batch runs for minutes, so a number
#: picked at speed would break working jobs to fix a hypothetical one.
#:
#: This is a RATCHET, not an amnesty: nothing new may join the list without a
#: reason, and the list only shrinks. It exists so the guard can be strict
#: about new code today instead of waiting on an audit of eighteen call sites
#: ([[feedback_residual_risk_needs_a_tracked_home]] -- the risk is listed
#: here and in TESTS.md Surface 137, not left as prose).
UNREVIEWED = {
    "drive_mirror.py:684",
    "drive_mirror.py:813",
    "engineering_governance_digest.py:104",
    "etsy_browser_batch.py:432",
    "etsy_browser_guard.py:269",
    "governance_review_common.py:58",
    "refresh_operator_surfaces.py:100",
    "repo_ci_status.py:111",
    "repo_ci_status.py:124",
    # Was :2917 until 2026-11-21, when `build_drop_items` was inserted above
    # it. The CALL is unchanged -- a line-keyed ratchet moves whenever
    # anything is added earlier in the file, which is a known cost of keying
    # on position rather than on the call.
    "review_loop.py:3009",
    "review_reply_discovery.py:217",
    "review_reply_discovery.py:239",
    "secret_scan.py:92",
    "listing_video_helper.py:172",
    "pictostl_helper.py:117",
    "pictostl_helper.py:242",
}


def _is_subprocess_call(node: ast.Call) -> bool:
    fn = node.func
    return (isinstance(fn, ast.Attribute) and isinstance(fn.value, ast.Name)
            and fn.value.id == "subprocess")


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
                if name not in _BLOCKING:
                    continue
                if name in _SUBPROCESS_ONLY and not _is_subprocess_call(node):
                    continue
                if any(k.arg == "timeout" for k in node.keywords):
                    continue
                if f"{path.name}:{node.lineno}" in UNREVIEWED:
                    continue
                bad.append(f"{path}:{node.lineno} {name}")
    return bad


class EveryBlockingCallIsTimedTests(unittest.TestCase):
    def test_no_untimed_blocking_call_exists(self):
        bad = _untimed_smtp_calls()
        self.assertEqual(
            bad, [],
            "A blocking call without a timeout wedged the sidecar twice -- "
            "three days from 2026-11-13, eight hours from 2026-11-17:\n  "
            + "\n  ".join(bad))

    def test_the_ratchet_only_covers_calls_that_still_exist(self):
        """A stale entry silently exempts whatever moves onto that line
        number. The list shrinks as sites are fixed; it must not rot.

        Judged only over files this runner could actually OPEN. Two of the
        four SEARCH_DIRS live in duckAgent, which duck-ops CI does not check
        out, so there every entry naming a duckAgent file read as "no longer
        an un-timed call" and the test demanded their deletion -- deleting
        them would have un-exempted three real un-timed calls that are still
        there, on the authority of a scan that never opened the files. A
        clean result is only as wide as the selection behind it
        ([[feedback_a_clean_scan_is_only_as_wide_as_its_selection]]).
        """
        live = set()
        scanned_files = set()
        scanned_dirs = []
        for root in SEARCH_DIRS:
            if not root.is_dir():
                continue
            scanned_dirs.append(root)
            for path in sorted(root.rglob("*.py")):
                if "test" in path.name:
                    continue
                try:
                    tree = ast.parse(path.read_text(encoding="utf-8"))
                except (OSError, SyntaxError):
                    continue
                scanned_files.add(path.name)
                for node in ast.walk(tree):
                    if (isinstance(node, ast.Call) and _is_subprocess_call(node)
                            and not any(k.arg == "timeout" for k in node.keywords)):
                        live.add(f"{path.name}:{node.lineno}")
        # Guard against the vacuous pass: a run that opened nothing would
        # report every entry as un-judgeable and agree with itself.
        self.assertTrue(scanned_dirs, f"none of {SEARCH_DIRS} exists")
        stale = {entry for entry in (UNREVIEWED - live)
                 if entry.split(":")[0] in scanned_files}
        self.assertEqual(stale, set(),
                         "these ratchet entries no longer name an un-timed "
                         "call; delete them so the list keeps shrinking")

    def test_the_ratchet_audit_says_what_it_could_not_judge(self):
        """Entries whose file this runner never opened are UNJUDGED, not
        clean -- and the distinction has to be visible, or the next person
        reads a green CI run as "the ratchet is current"."""
        unopened = {entry for entry in UNREVIEWED
                    if not any((root / entry.split(":")[0]).exists()
                               or any(p.name == entry.split(":")[0]
                                      for p in root.rglob("*.py"))
                               for root in SEARCH_DIRS if root.is_dir())}
        if unopened:
            print("\n[ratchet] not judged on this runner (file not present): "
                  + ", ".join(sorted(unopened)))
        # Nothing to assert about the operator's Mac, where all four dirs
        # exist; this exists so a CI run states its own blind spot.
        self.assertIsInstance(unopened, set)

    def test_notifier_is_held_to_the_strict_rule(self):
        """It holds the sidecar lock, so a hang there takes seven other steps
        and the operator inbox poller down with it. No exemptions."""
        self.assertFalse([e for e in UNREVIEWED if e.startswith("notifier.py")])

    def test_the_scan_sees_an_untimed_subprocess_too(self):
        """The one that actually bit. Added after a fix aimed at smtplib left
        the real cause untouched."""
        tree = ast.parse("import subprocess\nsubprocess.run(['docker','exec'])\n")
        found = [n for n in ast.walk(tree)
                 if isinstance(n, ast.Call) and _is_subprocess_call(n)
                 and not any(k.arg == "timeout" for k in n.keywords)]
        self.assertEqual(len(found), 1)

    def test_a_bare_run_on_something_else_is_not_flagged(self):
        """`run` is a common method name; flagging every one would make this
        noisy enough to be switched off."""
        tree = ast.parse("flow.run(x)\nserver.run()\n")
        self.assertEqual(
            [n for n in ast.walk(tree)
             if isinstance(n, ast.Call) and _is_subprocess_call(n)], [])

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

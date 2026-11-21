"""Tests for the operator-inbox poller — focused on the parsing path
(quoted-reply stripping, multipart bodies, address normalization) and
the trust-gate behavior. The IMAP/network calls are exercised
indirectly via the dispatch unit test that mocks handle_operator_text."""
from __future__ import annotations

import email.utils
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from unittest.mock import MagicMock, patch

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import operator_inbox_poller as poller


GMAIL_QUOTED_REPLY = """approve 240

On Mon, May 25, 2026 at 12:33 PM Duck Ops <ops@example.com> wrote:
> Subject: Meme Monday: Gym Girl Duck
> POV: You see the Gym Girl Duck...
"""

APPLE_QUOTED_REPLY = """agree 205

On May 25, 2026, at 11:00 AM, Duck Ops <ops@example.com> wrote:

Trend ranker proposed Flamingo Duck...
"""

OUTLOOK_QUOTED_REPLY = """discard 188

-----Original Message-----
From: ops@example.com
Sent: Monday, May 25, 2026
Subject: Trend candidate
"""


class ParsingTests(unittest.TestCase):
    def _build_message(self, body: str, *, content_type: str = "text/plain") -> EmailMessage:
        msg = EmailMessage()
        msg["From"] = "Operator <op@example.com>"
        msg["To"] = "ops@example.com"
        msg["Subject"] = "Re: DuckAgent Approval"
        if content_type == "text/plain":
            msg.set_content(body)
        else:
            msg.add_alternative(body, subtype=content_type.split("/", 1)[1])
        return msg

    def test_strips_gmail_quoted_reply(self) -> None:
        msg = self._build_message(GMAIL_QUOTED_REPLY)
        text = poller._extract_command_text(msg)
        self.assertEqual(text, "approve 240")

    def test_strips_apple_quoted_reply(self) -> None:
        msg = self._build_message(APPLE_QUOTED_REPLY)
        text = poller._extract_command_text(msg)
        self.assertEqual(text, "agree 205")

    def test_strips_outlook_original_message_block(self) -> None:
        msg = self._build_message(OUTLOOK_QUOTED_REPLY)
        text = poller._extract_command_text(msg)
        self.assertEqual(text, "discard 188")

    def test_preserves_multi_line_operator_text(self) -> None:
        body = """needs_changes 240

make the tagline less aggressive please

On Mon, May 25, 2026 Duck Ops wrote:
> [quoted]
"""
        msg = self._build_message(body)
        text = poller._extract_command_text(msg)
        self.assertIn("needs_changes 240", text)
        self.assertIn("tagline", text)

    def test_handles_html_only_body(self) -> None:
        msg = EmailMessage()
        msg["From"] = "op@example.com"
        msg["Subject"] = "Re: Approve"
        msg.set_content("plain fallback")
        msg.add_alternative("<p>approve 240</p>", subtype="html")
        text = poller._extract_command_text(msg)
        # text/plain wins when both are present.
        self.assertEqual(text, "plain fallback")

    def test_parses_bare_address_from_display_name(self) -> None:
        self.assertEqual(
            poller._parse_address('"Operator Name" <ops@example.com>'),
            "ops@example.com",
        )
        self.assertEqual(poller._parse_address("plain@example.com"), "plain@example.com")
        self.assertEqual(poller._parse_address(""), "")

    def test_strips_mobile_signature(self) -> None:
        body = """approve 240

Sent from my iPhone

On Mon, May 25 Duck Ops wrote:
> [quoted]
"""
        msg = self._build_message(body)
        text = poller._extract_command_text(msg)
        self.assertEqual(text, "approve 240")


class DispatchTests(unittest.TestCase):
    """The dispatch path subprocess-invokes DuckAgent's main_agent.py
    --mail-file. Tests mock subprocess.run so the assertion is on the
    contract: an {subject, body} JSON is written and main_agent.py is
    invoked from DuckAgent's root with its own venv."""

    def _fake_duckagent(self) -> tuple[Path, Path]:
        tmp = Path(tempfile.mkdtemp())
        venv = tmp / ".venv" / "bin" / "python3"
        venv.parent.mkdir(parents=True)
        venv.write_text("#!/usr/bin/env python3\n")
        venv.chmod(0o755)
        main = tmp / "src" / "main_agent.py"
        main.parent.mkdir(parents=True)
        main.write_text("# placeholder\n")
        return venv, main

    def test_dispatch_subprocess_calls_main_agent_mail_file(self) -> None:
        from unittest.mock import MagicMock
        venv, main = self._fake_duckagent()
        with patch.object(poller, "DUCK_AGENT_VENV_PY", venv):
            with patch.object(poller, "DUCK_AGENT_MAIN", main):
                proc = MagicMock(returncode=0, stdout="ok", stderr="")
                with patch.object(poller.subprocess, "run", return_value=proc) as mock_run:
                    result = poller._dispatch_mail_event(
                        subject="Re: MJD: [meme] Gym Girl Duck",
                        body="meme publish\n\n----\nDuckAgent approval metadata:\nFLOW:meme | RUN:2026-05-25 | ACTION:publish\n",
                    )
        self.assertTrue(result["ok"])
        self.assertEqual(result["returncode"], 0)
        called_args = mock_run.call_args.args[0]
        self.assertEqual(called_args[0], str(venv))
        self.assertEqual(called_args[1], str(main))
        self.assertEqual(called_args[2], "--mail-file")
        self.assertTrue(called_args[3].endswith(".json"))

    def test_dispatch_returns_failure_when_duckagent_unreachable(self) -> None:
        with patch.object(poller, "DUCK_AGENT_VENV_PY", Path("/nonexistent/venv/python3")):
            with patch.object(poller, "DUCK_AGENT_MAIN", Path("/nonexistent/main_agent.py")):
                result = poller._dispatch_mail_event(
                    subject="x", body="meme publish\nDuckAgent approval metadata:\nFLOW:meme",
                )
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "duckagent_unreachable")

    def test_dispatch_returns_failure_on_nonzero_returncode(self) -> None:
        from unittest.mock import MagicMock
        venv, main = self._fake_duckagent()
        with patch.object(poller, "DUCK_AGENT_VENV_PY", venv):
            with patch.object(poller, "DUCK_AGENT_MAIN", main):
                proc = MagicMock(returncode=2, stdout="", stderr="boom")
                with patch.object(poller.subprocess, "run", return_value=proc):
                    result = poller._dispatch_mail_event(
                        subject="x",
                        body="meme publish\nDuckAgent approval metadata:\nFLOW:meme",
                    )
        self.assertFalse(result["ok"])
        self.assertEqual(result["returncode"], 2)
        self.assertIn("boom", result["stderr_tail"])

    def test_empty_body_returns_failure(self) -> None:
        result = poller._dispatch_mail_event(subject="x", body="   ")
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "empty_body")


class MetadataFooterTests(unittest.TestCase):
    def test_strips_footer_with_separator(self) -> None:
        body = "meme publish\n\n----\nDuckAgent approval metadata:\nFLOW:meme | RUN:x | ACTION:publish\n"
        self.assertEqual(poller._strip_metadata_footer(body), "meme publish")

    def test_strips_footer_without_separator(self) -> None:
        body = "approve\nDuckAgent approval metadata:\nFLOW:design_brief_queue | RUN:y | ACTION:approve\n"
        self.assertEqual(poller._strip_metadata_footer(body), "approve")

    def test_passes_through_when_no_footer(self) -> None:
        body = "just text"
        self.assertEqual(poller._strip_metadata_footer(body), "just text")


class MetadataFooterSeparatorTests(unittest.TestCase):
    """The receipt log is what a human reads to ask "what did they send?".

    Gmail put the digest's `----` separator on the SAME line as the command,
    so neither of the two markers matched and the logged command text read
    `drop approve <stem> only ----`.
    """

    def test_a_same_line_separator_is_trimmed(self) -> None:
        self.assertEqual(
            poller._strip_metadata_footer(
                "drop approve alpha_v4 only ----\n"
                f"{poller.APPROVAL_METADATA_MARKER} FLOW:drops"),
            "drop approve alpha_v4 only")

    def test_the_own_line_separator_still_works(self) -> None:
        self.assertEqual(
            poller._strip_metadata_footer(
                "drop approve alpha_v4 only\n\n----\n"
                f"{poller.APPROVAL_METADATA_MARKER} FLOW:drops"),
            "drop approve alpha_v4 only")

    def test_a_command_with_no_furniture_is_untouched(self) -> None:
        self.assertEqual(
            poller._strip_metadata_footer("drop approve alpha_v4 only"),
            "drop approve alpha_v4 only")

    def test_a_numeric_command_is_untouched(self) -> None:
        self.assertEqual(poller._strip_metadata_footer("publish 240"),
                         "publish 240")


class TrustGateTests(unittest.TestCase):
    def test_explicit_override_wins(self) -> None:
        with patch.dict(os.environ, {"POLLER_OPERATOR_EMAIL": "Alice@Example.COM"}):
            self.assertEqual(poller._trusted_operator_email(), "alice@example.com")

    def test_no_trusted_sender_when_no_env(self) -> None:
        with patch.dict(os.environ, {"POLLER_OPERATOR_EMAIL": ""}, clear=False):
            # Point to a temp file that has no EMAIL_TO so the env
            # fallback also returns empty.
            with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as fh:
                fh.write("OTHER=value\n")
                path = fh.name
            with patch.dict(os.environ, {"DUCKAGENT_ENV_FILE": path}):
                self.assertEqual(poller._trusted_operator_email(), "")
            Path(path).unlink(missing_ok=True)


class ReceiptTests(unittest.TestCase):
    def test_receipt_appended_to_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp) / "operator_inbox_receipts.jsonl"
            with patch.object(poller, "RECEIPT_PATH", tmp_path):
                poller._append_receipt({"at": "2026-05-25T13:00:00", "outcome": "ok", "uid": "1"})
                poller._append_receipt({"at": "2026-05-25T13:01:00", "outcome": "ok", "uid": "2"})
            lines = tmp_path.read_text(encoding="utf-8").strip().split("\n")
            self.assertEqual(len(lines), 2)
            self.assertEqual(json.loads(lines[0])["uid"], "1")
            self.assertEqual(json.loads(lines[1])["uid"], "2")

class _FakeImap:
    """Minimal IMAP stand-in shaped like the Gmail behaviour that mattered.

    Per-folder UID lists, ascending as a real server returns them, so a test
    that expects newest-first has to prove the poller reversed it. `select`
    on an unknown folder returns NO, which is what a non-Gmail mailbox does
    for "[Gmail]/Trash".
    """

    def __init__(self, folders: dict[str, list[tuple[bytes, bytes]]]) -> None:
        self.folders = folders
        self.current: str | None = None
        self.searches: list[tuple[str, str]] = []
        self.fetched: list[tuple[str, str]] = []
        self.stored: list[tuple[str, str]] = []

    def select(self, folder, readonly=False):
        if folder not in self.folders:
            self.current = None
            return "NO", [b""]
        self.current = folder
        return "OK", [b"1"]

    def search(self, charset, query):
        self.searches.append((self.current or "", query))
        uids = b" ".join(uid for uid, _raw in self.folders[self.current])
        return "OK", [uids]

    def fetch(self, uid, spec):
        self.fetched.append((self.current or "", uid.decode()))
        for candidate, raw in self.folders[self.current or ""]:
            if candidate == uid:
                return "OK", [(b"1 (BODY[] {%d}" % len(raw), raw)]
        return "NO", None

    def store(self, uid, flags, value):
        self.stored.append((self.current or "", uid.decode()))
        return "OK", [b""]

    def close(self):
        self.current = None

    def logout(self):
        return "BYE", [b""]


def _reply_bytes(*, subject: str, age_hours: float, command: str,
                 message_id: str, sender: str = "op@example.com") -> bytes:
    """A reply shaped like the real mailto replies, dated relative to now."""
    msg = EmailMessage()
    msg["From"] = sender
    msg["Subject"] = subject
    msg["Message-ID"] = message_id
    sent = datetime.now(timezone.utc) - timedelta(hours=age_hours)
    msg["Date"] = email.utils.format_datetime(sent)
    msg.set_content(
        f"{command}\n\n----\n{poller.APPROVAL_METADATA_MARKER}\n"
        "FLOW:drops | RUN:2026-11-16 | ACTION:approve\n"
    )
    return msg.as_bytes()


class FolderSelectionTests(unittest.TestCase):
    """The predicate, not the parser.

    A reply the poller never OPENS is invisible no matter how well the
    body parsing works, which is how an approval sat unread in Trash for
    nine hours on 2026-11-17 while the digest re-sent the same four drops
    ([[feedback_a_clean_scan_is_only_as_wide_as_its_selection]]).
    """

    def setUp(self) -> None:
        self._ctx = [
            patch.object(poller, "_trusted_operator_email", return_value="op@example.com"),
            patch.object(poller, "_load_config", return_value={}),
            patch.object(poller, "load_mailbox_settings", return_value={"enabled": True}),
            patch.object(poller, "RECEIPT_PATH", Path(tempfile.mkdtemp()) / "r.jsonl"),
        ]
        for c in self._ctx:
            c.start()
        self.addCleanup(lambda: [c.stop() for c in self._ctx])

    def _run(self, folders, *, dry_run=False, limit=25):
        fake = _FakeImap(folders)
        with patch.object(poller, "connect_imap", return_value=(fake, None, None)), \
             patch.object(poller, "_dispatch_mail_event",
                          return_value={"ok": True, "response": "done"}) as dispatch:
            receipts = poller.poll_inbox(dry_run=dry_run, limit=limit)
        return fake, receipts, dispatch

    def test_trash_is_polled(self) -> None:
        """The 2026-11-17 regression: the approval was in Trash."""
        reply = _reply_bytes(subject="Re: drops", age_hours=1.0,
                             command="drop approve duck_v4 only", message_id="<trash-1>")
        fake, receipts, dispatch = self._run({
            "INBOX": [],
            '"[Gmail]/All Mail"': [],
            '"[Gmail]/Trash"': [(b"7", reply)],
        })
        ok = [r for r in receipts if r["outcome"] == "ok"]
        self.assertEqual(len(ok), 1, receipts)
        self.assertEqual(ok[0]["folder"], '"[Gmail]/Trash"')
        self.assertEqual(ok[0]["command_text"], "drop approve duck_v4 only")
        dispatch.assert_called_once()

    def test_every_configured_folder_is_searched(self) -> None:
        fake, _receipts, _d = self._run({f: [] for f in poller.POLL_FOLDERS})
        self.assertEqual([f for f, _q in fake.searches], list(poller.POLL_FOLDERS))

    def test_trash_is_in_the_configured_folders(self) -> None:
        """Pins the FOLDER NAME, because All Mail does not cover Trash.

        Gmail's "[Gmail]/All Mail" excludes Trash and Spam: measured
        2026-11-17, it returned the same 2 unseen messages as INBOX while
        Trash held 174. Dropping this entry silently restores the bug.
        """
        self.assertIn('"[Gmail]/Trash"', poller.POLL_FOLDERS)
        self.assertIn("INBOX", poller.POLL_FOLDERS)

    def test_missing_folder_is_recorded_not_fatal(self) -> None:
        reply = _reply_bytes(subject="Re: drops", age_hours=1.0,
                             command="drop approve duck_v4", message_id="<inbox-1>")
        _fake, receipts, _d = self._run({"INBOX": [(b"3", reply)]})
        self.assertEqual([r["outcome"] for r in receipts if r["outcome"] == "ok"], ["ok"])
        unavailable = {r["folder"] for r in receipts if r["outcome"] == "folder_unavailable"}
        self.assertEqual(unavailable, {'"[Gmail]/All Mail"', '"[Gmail]/Trash"'})

    def test_newest_reply_wins_the_limit(self) -> None:
        """Trash held 174 unread operator messages; oldest-first starves today's."""
        old = _reply_bytes(subject="Re: old", age_hours=10.0,
                           command="drop approve old_one", message_id="<old>")
        new = _reply_bytes(subject="Re: new", age_hours=0.5,
                           command="drop approve new_one", message_id="<new>")
        _fake, receipts, _d = self._run(
            {"INBOX": [(b"1", old), (b"2", new)]}, limit=1)
        ok = [r for r in receipts if r["outcome"] == "ok"]
        self.assertEqual(len(ok), 1)
        self.assertEqual(ok[0]["command_text"], "drop approve new_one")

    def test_search_window_is_bounded_by_since(self) -> None:
        fake, _receipts, _d = self._run({"INBOX": []})
        query = fake.searches[0][1]
        self.assertIn("SINCE", query)
        self.assertIn('FROM "op@example.com"', query)
        self.assertIn("UNSEEN", query)

    def test_same_message_in_two_folders_dispatches_once(self) -> None:
        """Gmail shows one message in several folders under different UIDs."""
        raw = _reply_bytes(subject="Re: drops", age_hours=1.0,
                           command="drop approve duck_v4", message_id="<shared-1>")
        _fake, receipts, dispatch = self._run({
            "INBOX": [(b"11", raw)],
            '"[Gmail]/All Mail"': [(b"22", raw)],
            '"[Gmail]/Trash"': [],
        })
        self.assertEqual(dispatch.call_count, 1, receipts)
        dupes = [r for r in receipts if r["outcome"] == "skipped_duplicate_folder_copy"]
        self.assertEqual(len(dupes), 1)
        self.assertEqual(dupes[0]["folder"], '"[Gmail]/All Mail"')

    def test_stale_reply_is_reported_never_dispatched(self) -> None:
        """A five-day-old THURSDAY approval was sitting unread in Trash.

        Widening the search without this guard would have published it.
        """
        stale = _reply_bytes(subject="Re: MJD: [THURSDAY] BATCH REVIEW",
                             age_hours=poller.MAX_REPLY_AGE_HOURS + 1,
                             command="publish 1188", message_id="<stale-1>")
        fake, receipts, dispatch = self._run({'"[Gmail]/Trash"': [(b"9", stale)]})
        dispatch.assert_not_called()
        skipped = [r for r in receipts if r["outcome"] == "skipped_stale_reply"]
        self.assertEqual(len(skipped), 1, receipts)
        self.assertEqual(skipped[0]["command_text"], "publish 1188")
        self.assertGreater(skipped[0]["age_hours"], poller.MAX_REPLY_AGE_HOURS)
        self.assertEqual(fake.stored, [], "a stale reply must stay UNREAD")

    def test_undatable_reply_is_treated_as_stale(self) -> None:
        msg = EmailMessage()
        msg["From"] = "op@example.com"
        msg["Subject"] = "Re: drops"
        msg["Message-ID"] = "<nodate-1>"
        msg.set_content(
            f"drop approve duck_v4\n\n----\n{poller.APPROVAL_METADATA_MARKER}\n")
        _fake, receipts, dispatch = self._run({"INBOX": [(b"4", msg.as_bytes())]})
        dispatch.assert_not_called()
        self.assertEqual(
            [r["outcome"] for r in receipts if r.get("uid") == "4"], ["skipped_stale_reply"])

    def test_fresh_reply_is_marked_seen_in_its_own_folder(self) -> None:
        reply = _reply_bytes(subject="Re: drops", age_hours=2.0,
                             command="drop approve duck_v4", message_id="<fresh-1>")
        fake, _receipts, _d = self._run({'"[Gmail]/Trash"': [(b"5", reply)]})
        self.assertEqual(fake.stored, [('"[Gmail]/Trash"', "5")])

    def test_dry_run_leaves_everything_unread(self) -> None:
        reply = _reply_bytes(subject="Re: drops", age_hours=2.0,
                             command="drop approve duck_v4", message_id="<dry-1>")
        fake, receipts, dispatch = self._run(
            {"INBOX": [(b"6", reply)]}, dry_run=True)
        dispatch.assert_not_called()
        self.assertEqual(fake.stored, [])
        self.assertTrue(any(r.get("dry_run") for r in receipts))

    def test_untrusted_sender_is_refused_even_if_imap_returns_it(self) -> None:
        reply = _reply_bytes(subject="Re: drops", age_hours=1.0,
                             command="drop approve duck_v4", message_id="<evil-1>",
                             sender="someone-else@example.com")
        _fake, receipts, dispatch = self._run({"INBOX": [(b"8", reply)]})
        dispatch.assert_not_called()
        self.assertEqual(
            [r["outcome"] for r in receipts if r.get("uid") == "8"],
            ["skipped_untrusted_sender"])

if __name__ == "__main__":
    unittest.main()


class AnUnreadMailboxIsAFailedRunTests(unittest.TestCase):
    """Surface 176 — a mailbox it never reached used to exit 0.

    Measured 2026-11-20. The 18:16 sidecar run hit Gmail's "[ALERT] Too many
    simultaneous connections", read nothing, and exited 0. The sidecar wrapper
    already greps for a non-zero `poller_rc` and prints POLLER_FAILED — it
    just never got one — so the run logged `exit=0`, scheduler_health graded
    it healthy, and the operator spent an hour asking why an approval reply
    they had sent was not picked up.

    The loud channel existed. Nothing fed it
    ([[feedback_swallowed_errors_lie]]).
    """

    def test_a_connect_failure_exits_non_zero(self):
        receipts = [{"at": "now", "outcome": "imap_connect_failed",
                     "reason": "Too many simultaneous connections"}]
        with patch.object(poller, "poll_inbox", return_value=receipts), \
             patch.object(sys, "argv", ["operator_inbox_poller.py"]):
            self.assertEqual(poller.main(), 1)

    def test_every_unreached_outcome_exits_non_zero(self):
        # Enumerated rather than spot-checked: each of these is a different
        # way of never reading the mailbox, and one left at exit 0 is one
        # silent failure mode still open
        # ([[feedback_incomplete_fix_enumerate_all_branches]]).
        for outcome in sorted(poller.MAILBOX_UNREACHED_OUTCOMES):
            with self.subTest(outcome=outcome):
                with patch.object(poller, "poll_inbox",
                                  return_value=[{"outcome": outcome}]), \
                     patch.object(sys, "argv", ["operator_inbox_poller.py"]):
                    self.assertEqual(poller.main(), 1)

    def test_a_quiet_mailbox_is_NOT_a_failure(self):
        # The normal state three runs out of four. Failing here would make a
        # quiet inbox look like a broken job, which is the mistake
        # run_drop_intake.sh already documents for refused drops.
        for outcome in ("no_new_replies", "skipped_missing_approval_marker",
                        "skipped_untrusted_sender", "skipped_stale_reply",
                        "skipped_duplicate_folder_copy", "ok"):
            with self.subTest(outcome=outcome):
                with patch.object(poller, "poll_inbox",
                                  return_value=[{"outcome": outcome}]), \
                     patch.object(sys, "argv", ["operator_inbox_poller.py"]):
                    self.assertEqual(poller.main(), 0)

    def test_a_per_folder_problem_is_not_a_failed_run(self):
        # `folder_unavailable` and `fetch_failed` are about ONE folder; the
        # mailbox was still reached and other folders were still read, so the
        # run is not a wash.
        receipts = [{"outcome": "folder_unavailable", "folder": "Trash"},
                    {"outcome": "ok", "folder": "INBOX"}]
        with patch.object(poller, "poll_inbox", return_value=receipts), \
             patch.object(sys, "argv", ["operator_inbox_poller.py"]):
            self.assertEqual(poller.main(), 0)

    def test_one_unreached_outcome_among_many_still_fails(self):
        receipts = [{"outcome": "ok"},
                    {"outcome": "imap_connect_failed", "reason": "x"}]
        self.assertEqual(poller.unreached_outcomes(receipts),
                         ["imap_connect_failed"])

    def test_the_stderr_message_says_approvals_are_still_waiting(self):
        # The operator-actionable part: the failure's consequence is that a
        # reply they already sent has not been seen, which is not obvious
        # from "imap_connect_failed".
        import contextlib, io

        buf = io.StringIO()
        with patch.object(poller, "poll_inbox",
                          return_value=[{"outcome": "imap_connect_failed"}]), \
             patch.object(sys, "argv", ["operator_inbox_poller.py"]), \
             contextlib.redirect_stderr(buf), \
             contextlib.redirect_stdout(io.StringIO()):
            poller.main()
        err = buf.getvalue()
        self.assertIn("MAILBOX NOT READ", err)
        self.assertIn("still waiting", err)

    def test_the_wrapper_checks_the_code_this_now_returns(self):
        # The fix only helps if the caller looks. The sidecar wrapper greps
        # `poller_rc` and prints POLLER_FAILED; assert that contract rather
        # than assuming it ([[feedback_loader_and_registration_both_need_tests]]
        # -- the producer and its consumer both need a test).
        # DERIVED, not absolute: tests/ -> duck-ops -> ai-agents. The wrapper
        # lives in a local-only runtime dir outside any checkout, so this is
        # skipped wherever that dir is absent rather than hardcoding one
        # machine's home (which tests/test_the_suite_runs_where_it_is_checked_out
        # correctly refused when this test first tried it).
        wrapper = (Path(__file__).resolve().parents[2]
                   / "openclaw_runtime" / "run_duck_ops_sidecar.sh")
        if not wrapper.is_file():
            self.skipTest("sidecar wrapper lives outside git; not on this runner")
        text = wrapper.read_text(encoding="utf-8")
        self.assertIn("operator_inbox_poller.py", text)
        self.assertIn("poller_rc", text)
        self.assertIn("POLLER_FAILED", text)

"""Tests for the operator-inbox poller — focused on the parsing path
(quoted-reply stripping, multipart bodies, address normalization) and
the trust-gate behavior. The IMAP/network calls are exercised
indirectly via the dispatch unit test that mocks handle_operator_text."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from email.message import EmailMessage
from pathlib import Path
from unittest.mock import MagicMock, patch

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
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


if __name__ == "__main__":
    unittest.main()

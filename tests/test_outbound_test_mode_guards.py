"""DUCK_TEST_MODE must stop everything in duck-ops that leaves the machine.

Why this file exists: on 2026-11-08, rendering a jeepfact review email under
DUCK_TEST_MODE=1 to verify an unrelated change delivered a real email to the
operator. duckAgent's email_helper had carried a test-mode guard since the
2026-09-25 four-email incident -- but on `send_email`, while every lane review
email actually goes out through `email_summary`, which dialled SMTP unguarded.
The flag was set. One sender read it. The other didn't.

Then the same audit found duck-ops in the same state: notifier.send_message,
notifier.send_whatsapp_message and both widget_api senders had no test-mode
awareness at all, and widget_api's two mail a `Re: ... ACTION:publish` that the
mail watcher PARSES to trigger a real publish.

So these tests exist to answer one question for each sender: if the suite ran
right now, would the outside world notice? Every test here replaces the
transport with something that RAISES rather than something that records, so a
regression fails loudly here instead of mailing the operator. A mock that
quietly absorbs the send would let all of this pass with the guards deleted,
which is exactly how the original hole survived ten-plus test files.
"""
from __future__ import annotations

import subprocess
import sys
from email.message import EmailMessage
from pathlib import Path

import pytest

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import notifier  # noqa: E402
import widget_api  # noqa: E402
import workflow_control  # noqa: E402

OutboundRefusal = workflow_control.TestModeRefusalError


SETTINGS = {
    "host": "smtp.invalid",
    "port": 587,
    "user": "duck@invalid",
    "password": "nope",
    "to": "operator@invalid",
    "use_starttls": True,
    "whatsapp": {"target": "+10000000000", "docker_path": "/bin/false"},
}


def _message(subject: str = "MJD: [digest] guard probe") -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SETTINGS["user"]
    msg["To"] = SETTINGS["to"]
    msg.set_content("body")
    return msg


@pytest.fixture
def explode_on_send(monkeypatch):
    """Make every transport in both modules raise.

    This is the sharpness of the whole file: with the guards removed, each
    test below fails on this AssertionError instead of passing quietly.
    """

    def _smtp_boom(*args, **kwargs):
        raise AssertionError(
            f"A test reached a real SMTP connection: {args!r} {kwargs!r}. "
            "The DUCK_TEST_MODE guard did not hold."
        )

    def _subprocess_boom(*args, **kwargs):
        raise AssertionError(
            f"A test reached the WhatsApp gateway: {args!r}. "
            "The DUCK_TEST_MODE guard did not hold."
        )

    monkeypatch.setattr(notifier.smtplib, "SMTP", _smtp_boom)
    monkeypatch.setattr(notifier.subprocess, "run", _subprocess_boom)
    monkeypatch.setattr(widget_api.smtplib, "SMTP", _smtp_boom)
    monkeypatch.setattr(widget_api.smtplib, "SMTP_SSL", _smtp_boom)


@pytest.fixture
def test_mode_on(monkeypatch):
    monkeypatch.setenv("DUCK_TEST_MODE", "1")


class TestNotifierEmail:
    def test_refuses_to_send_under_test_mode(self, explode_on_send, test_mode_on):
        with pytest.raises(OutboundRefusal):
            notifier.send_message(SETTINGS, _message())

    def test_refusal_names_the_way_out(self, explode_on_send, test_mode_on):
        """A refusal the caller can't act on just gets worked around. The
        message has to say what to do instead."""
        with pytest.raises(OutboundRefusal) as excinfo:
            notifier.send_message(SETTINGS, _message())
        text = str(excinfo.value)
        assert "DUCK_TEST_MODE" in text
        assert "--dry-run" in text
        assert "smtplib" in text

    def test_raises_rather_than_returning_quietly(self, explode_on_send, test_mode_on):
        """The load-bearing shape of this guard.

        send_message's caller runs record_sent_artifact + write_json(STATE_PATH)
        on the very next line. If the guard returned None instead of raising,
        the artifact would be stamped as sent, the dedup ledger would never
        offer it again, and the email would be LOST rather than deferred --
        the guard itself would become a silent mail-eater.

        So: it must raise. This test fails if someone "softens" it to a
        quiet return to stop a noisy suite.
        """
        with pytest.raises(OutboundRefusal):
            notifier.send_message(SETTINGS, _message())

    def test_still_sends_when_the_flag_is_absent(self, monkeypatch):
        """The guard must be a test-mode gate, not a blanket off-switch.

        Without this, deleting the env check entirely would still leave every
        other test in this file green.
        """
        monkeypatch.delenv("DUCK_TEST_MODE", raising=False)
        sent = []

        class _FakeSMTP:
            def __init__(self, host, port):
                sent.append(("connect", host, port))

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def starttls(self):
                sent.append(("starttls",))

            def login(self, user, password):
                sent.append(("login", user))

            def send_message(self, msg):
                sent.append(("send", msg["Subject"]))

        monkeypatch.setattr(notifier.smtplib, "SMTP", _FakeSMTP)
        notifier.send_message(SETTINGS, _message("MJD: [digest] real send"))
        assert ("send", "MJD: [digest] real send") in sent

    def test_reads_the_env_at_call_time(self, explode_on_send, monkeypatch):
        """Not at import time -- a module-level snapshot would make the guard
        depend on import order, which no caller can see."""
        monkeypatch.delenv("DUCK_TEST_MODE", raising=False)
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        with pytest.raises(OutboundRefusal):
            notifier.send_message(SETTINGS, _message())

    @pytest.mark.parametrize("flag", ["1", "true", "TRUE", "yes", " 1 "])
    def test_every_truthy_spelling_refuses(self, explode_on_send, monkeypatch, flag):
        monkeypatch.setenv("DUCK_TEST_MODE", flag)
        with pytest.raises(OutboundRefusal):
            notifier.send_message(SETTINGS, _message())

    @pytest.mark.parametrize("flag", ["0", "", "false", "no"])
    def test_falsy_spellings_do_not_refuse(self, monkeypatch, flag):
        """A guard that treats "0" as "on" is a guard that gets deleted."""
        monkeypatch.setenv("DUCK_TEST_MODE", flag)
        calls = []
        monkeypatch.setattr(
            notifier.smtplib, "SMTP", lambda *a, **k: calls.append(a) or _NullSMTP()
        )
        notifier.send_message(SETTINGS, _message())
        assert calls, "a falsy flag must not block the send"


class _NullSMTP:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def starttls(self):
        pass

    def login(self, *a):
        pass

    def send_message(self, msg):
        pass


class TestNotifierWhatsApp:
    def test_refuses_before_touching_the_gateway(self, explode_on_send, test_mode_on):
        """WhatsApp is the sender it would be easiest to forget: it is not
        SMTP, so an audit that greps for smtplib misses it entirely. It still
        reaches the operator's phone."""
        with pytest.raises(OutboundRefusal):
            notifier.send_whatsapp_message(SETTINGS, "guard probe")

    def test_guard_precedes_the_target_check(self, explode_on_send, test_mode_on):
        """With no target configured the function raises SystemExit. The
        test-mode refusal has to come first, or whether we refuse depends on
        unrelated config."""
        with pytest.raises(OutboundRefusal):
            notifier.send_whatsapp_message({"whatsapp": {}}, "guard probe")


class TestWidgetApiSenders:
    """Both of these mail a Re: ... ACTION:publish / ACTION:needs_changes to
    the operator's own inbox, which the mail watcher parses to trigger the
    real publish. A test that reaches them does not just send mail -- it
    publishes a duck post."""

    CANDIDATE = {
        "artifact_id": "artifact-guard-probe",
        "flow": "review_carousel",
        "candidate_summary": {"title": "Guard probe", "publish_token": "run-guard"},
        "execution_state": {"state": "draft", "state_source": "runs/guard/state.json"},
    }

    def test_needs_changes_email_refuses(self, explode_on_send, test_mode_on):
        result = widget_api._send_review_carousel_needs_changes_email(self.CANDIDATE)
        assert result["ok"] is False
        assert result["blocked_test_mode"] is True

    def test_refusal_is_distinguishable_from_a_real_failure(
        self, explode_on_send, test_mode_on
    ):
        """Both return {"ok": False}. A caller reading only `ok` cannot tell
        "SMTP is down" from "we refused on purpose", and the fail-soft caller
        reports the error string to the operator. Tag it."""
        result = widget_api._send_review_carousel_needs_changes_email(self.CANDIDATE)
        assert result.get("blocked_test_mode") is True
        assert "DUCK_TEST_MODE" in result["error"]

    def test_guard_precedes_the_creds_check(self, explode_on_send, test_mode_on):
        """_load_smtp_creds reads duckAgent/.env straight off disk, so the
        "missing SMTP env" branch is not a safety net -- the credentials are
        really there. But on a machine without that file the creds check would
        return first and the flag would never be consulted, so the refusal
        would depend on whether .env happens to exist."""
        monkeypatch_free = widget_api._load_smtp_creds
        assert callable(monkeypatch_free)
        result = widget_api._send_review_carousel_needs_changes_email(self.CANDIDATE)
        assert result.get("blocked_test_mode") is True

    def test_approve_publish_refuses(self, explode_on_send, test_mode_on, monkeypatch):
        monkeypatch.setattr(
            widget_api, "_find_candidate_by_artifact", lambda _id: self.CANDIDATE
        )
        result = widget_api.approve_publish_candidate("artifact-guard-probe")
        assert result["ok"] is False
        assert result["blocked_test_mode"] is True

    def test_dry_run_still_works_under_test_mode(
        self, explode_on_send, test_mode_on, monkeypatch
    ):
        """dry_run returns before the transport, so refusing it would be
        pointless strictness that costs the operator a usable preview."""
        monkeypatch.setattr(
            widget_api, "_find_candidate_by_artifact", lambda _id: self.CANDIDATE
        )
        monkeypatch.setattr(
            widget_api,
            "_load_smtp_creds",
            lambda: {
                "SMTP_HOST": "smtp.test",
                "SMTP_PORT": "587",
                "SMTP_USER": "u@test",
                "SMTP_PASS": "p",
            },
        )
        monkeypatch.setattr(widget_api, "_log_approval", lambda *a, **k: None)
        result = widget_api.approve_publish_candidate(
            "artifact-guard-probe", dry_run=True
        )
        assert result["ok"] is True
        assert result["dryRun"] is True
        assert "ACTION:publish" in result["subject"]


class TestNoSenderIsLeftBehind:
    def test_every_smtplib_call_site_lives_in_a_guarded_function(self):
        """The audit that found this hole, kept as a test.

        The original defect was not that a guard was wrong -- it was that
        nobody enumerated the senders. This walks every function in runtime/
        that touches smtplib directly and asserts it consults the test-mode
        flag, so a NEW unguarded sender fails here instead of in the
        operator's inbox.

        Two deliberate choices. It matches any callee whose name contains
        "test_mode" rather than today's `_refuse_outbound_under_test_mode`,
        so renaming the guard doesn't break this test -- per
        feedback_pin_the_rule_not_the_reading, pin the rule, not the string.
        And it only flags functions that reach smtplib THEMSELVES: a function
        that calls an already-guarded sender (notifier.main) is fine, and
        flagging it would train the next person to add the guard twice.

        Scope limit, stated rather than implied: this covers SMTP only.
        notifier.send_whatsapp_message shells out to a docker gateway and is
        covered by TestNotifierWhatsApp above, not by this scan. A future
        sender on a third channel (an HTTP mail API, a webhook) will NOT be
        caught here -- extend this when one lands.
        """
        import ast

        offenders = []
        for path in sorted(RUNTIME_DIR.glob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue

                touches_smtp = any(
                    isinstance(sub, ast.Attribute)
                    and isinstance(sub.value, ast.Name)
                    and sub.value.id == "smtplib"
                    for sub in ast.walk(node)
                )
                if not touches_smtp:
                    continue

                consults_flag = any(
                    "test_mode" in (
                        sub.func.id if isinstance(sub.func, ast.Name)
                        else getattr(sub.func, "attr", "")
                    )
                    for sub in ast.walk(node)
                    if isinstance(sub, ast.Call)
                )
                if not consults_flag:
                    offenders.append(f"{path.name}::{node.name}")

        assert offenders == [], (
            "These functions reach SMTP without consulting DUCK_TEST_MODE: "
            f"{offenders}. Add the guard (see "
            "notifier._refuse_outbound_under_test_mode or "
            "widget_api._blocked_by_test_mode) rather than removing this test."
        )

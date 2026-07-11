"""fill_reply_text_without_submit must stage the reply through React's controlled
-input path, or the value evaporates on the next re-render and every submit fails
closed at the valueMatches gate.

Regression for 2026-07-11: the fill assigned `textarea.value = replyText`
directly. Etsy's composer is a React controlled input whose _valueTracker was
never updated, so React reverted the box to empty on re-render. The same-tick
`textarea.value === replyText` check passed, the tool reported ok, but the
separate-command inspect in run_live_submit then saw an empty box
(valueMatches=false) and raised "The textarea no longer matches the exact
approved reply text." — silently dropping every reply for weeks. The fix sets
the value via the native prototype setter and rewinds _valueTracker so React's
onChange commits the text to component state and it survives re-render."""
from __future__ import annotations

import sys
from pathlib import Path

RUNTIME = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import review_reply_executor as rre  # noqa: E402


def _capture_fill_js(monkeypatch) -> str:
    """Run the fill and return the JS eval string it hands the browser."""
    captured: dict[str, str] = {}

    def _fake_run_pw_command(session_name, verb, script):
        assert verb == "eval"
        captured["js"] = script
        # Mimic a healthy fill result so the function returns normally.
        return '{"ok": true, "valueLength": 12, "submitVisible": true, ' \
               '"submitDisabled": false, "submitPerformed": false}'

    monkeypatch.setattr(rre, "run_pw_command", _fake_run_pw_command)
    result = rre.fill_reply_text_without_submit("esd", "TX123", "Thank you!!")
    assert result.get("ok") is True
    return captured["js"]


def test_fill_uses_react_native_setter(monkeypatch):
    js = _capture_fill_js(monkeypatch)
    # The React-safe staging path must be present.
    assert "getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value')" in js
    assert "nativeSetter.call(textarea, replyText)" in js
    assert "_valueTracker" in js
    # And it must still dispatch input so React's onChange fires.
    assert "new Event('input'" in js


def test_fill_does_not_use_bare_value_assignment(monkeypatch):
    js = _capture_fill_js(monkeypatch)
    # The exact buggy pattern that React silently reverts must not return.
    assert "textarea.value = replyText" not in js

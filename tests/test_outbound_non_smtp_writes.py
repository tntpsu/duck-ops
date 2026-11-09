"""duck-ops sibling of duckAgent/tests/test_outbound_non_smtp_writes.py.

The 2026-11-08 mail work closed five SMTP senders in both repos and said what
it had not covered: *"Non-SMTP outbound (`requests.post` to a mail API, a
webhook, a gateway shell-out) is still unenumerated."* This repo's share of
that gap is small and lopsided -- almost everything outbound here is a READ
(GA4, GSC, the competitor collectors, USPS tracking) -- which is exactly why it
needed enumerating rather than asserting: "mostly reads" is not a control, and
the one write in the set creates a task in the operator's real Google Tasks
list.

The WhatsApp gateway is covered separately, by the `subprocess.run` filter in
tests/conftest.py, because it is a shell-out rather than an HTTP call. Mail is
covered by tests/test_outbound_test_mode_guards.py.
"""
from __future__ import annotations

import ast
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_SKIP = ("/.venv/", "/tests/", "__pycache__", "/node_modules/", "/test/", "/.git/")
_WRITE_VERBS = {"post", "put", "patch", "delete", "request"}
_FUNC = (ast.FunctionDef, ast.AsyncFunctionDef)


ALLOWED: dict[tuple[str, str], str] = {
    # --- Reads. These are the producers behind the intel pages. -----------
    ("runtime/competitor_social_snapshot_collector.py", "_request_json"):
        "GET: reads a competitor's public Etsy/social data",
    ("runtime/competitor_social_snapshot_collector.py", "_request_text"):
        "GET: reads a competitor's public page HTML",
    ("runtime/ga4_listing_performance.py", "_request_json"):
        "GA4 reporting API: a POST by protocol, but runReport only READS "
        "analytics -- it creates nothing and GA4 has no record of it",
    ("runtime/gsc_search_demand.py", "_request_json"):
        "Search Console searchAnalytics.query: a POST by protocol, read-only",
    ("runtime/usps_tracking.py", "_http_json"):
        "GET: reads tracking status for our own shipments",
    ("runtime/whatsapp_collage_helper.py", "_load_image"):
        "GET: downloads an image to build a collage locally",

    # --- Operator-run OAuth setup. Not reachable from the suite. ----------
    ("runtime/google_tasks_oauth_helper.py", "exchange_code_for_tokens"):
        "interactive: the operator pastes a one-time code into a terminal",
    ("scripts/mint_google_refresh_token.py", "main"):
        "interactive: hand-run once to mint a refresh token",

    # --- Paid generation. Same reasoning as the duckAgent sibling. --------
    ("runtime/llm_call_helpers.py", "call_openai"):
        "paid LLM call; spend guard tracked separately, no external artifact",
}


def _outermost_functions(tree: ast.AST) -> list[ast.AST]:
    out: list[ast.AST] = []

    def walk(node, inside_func: bool) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, _FUNC):
                if not inside_func:
                    out.append(child)
                walk(child, True)
            else:
                walk(child, inside_func)

    walk(tree, False)
    return out


def _outbound_calls(node: ast.AST) -> list[str]:
    found: set[str] = set()
    for sub in ast.walk(node):
        if not isinstance(sub, ast.Call):
            continue
        fn = sub.func
        if (isinstance(fn, ast.Attribute) and isinstance(fn.value, ast.Name)
                and fn.value.id == "requests" and fn.attr in _WRITE_VERBS):
            found.add(f"requests.{fn.attr}")
        if isinstance(fn, ast.Attribute) and fn.attr == "urlopen":
            found.add("urlopen")
    return sorted(found)


def _consults_the_flag(node: ast.AST) -> bool:
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call):
            name = (sub.func.id if isinstance(sub.func, ast.Name)
                    else getattr(sub.func, "attr", "")) or ""
            if "test_mode" in name:
                return True
    return False


def _scan() -> tuple[list[tuple[str, str, list[str], bool]], list[str]]:
    rows, unparseable = [], []
    for path in sorted(_REPO.rglob("*.py")):
        sp = str(path)
        if any(part in sp for part in _SKIP):
            continue
        rel = str(path.relative_to(_REPO))
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError) as exc:
            unparseable.append(f"{rel}: {type(exc).__name__} {exc}")
            continue
        for fn in _outermost_functions(tree):
            calls = _outbound_calls(fn)
            if calls:
                rows.append((rel, fn.name, calls, _consults_the_flag(fn)))
    return rows, unparseable


class TestEveryOutboundWriteIsGuardedOrNamed:
    def test_no_unguarded_unnamed_outbound_path(self):
        offenders = [
            f"{rel}::{name} ({', '.join(calls)})"
            for rel, name, calls, guarded in _scan()[0]
            if not guarded and (rel, name) not in ALLOWED
        ]
        assert not offenders, (
            "Non-SMTP outbound path that neither consults DUCK_TEST_MODE nor "
            "appears in ALLOWED with a reason:\n  " + "\n  ".join(offenders))

    def test_every_scanned_file_parses(self):
        rows, unparseable = _scan()
        assert not unparseable, (
            "Files this audit could not parse, so their outbound paths were "
            "invisible to it:\n  " + "\n  ".join(unparseable))
        assert len(rows) >= 10, (
            f"only {len(rows)} outbound paths found; the walk is probably broken "
            "-- there were 11 on 2026-11-09")

    def test_allowlist_has_no_stale_entries(self):
        live = {(rel, name) for rel, name, _, _ in _scan()[0]}
        stale = sorted(f"{rel}::{name}" for (rel, name) in ALLOWED if (rel, name) not in live)
        assert not stale, "ALLOWED names paths that no longer exist:\n  " + "\n  ".join(stale)

    def test_every_reason_is_specific(self):
        for (rel, name), reason in ALLOWED.items():
            assert len(reason) >= 25, f"{rel}::{name} reason too thin: {reason!r}"


class TestGoogleTasksRefusesWrites:
    """Behavioural, not just structural: the AST walk proves a guard is called,
    these prove it refuses the right things. Both, because a guard that is
    present and wrong looks identical to the walk."""

    def _bridge(self):
        import importlib
        import sys

        sys.path.insert(0, str(_REPO / "runtime"))
        return importlib.import_module("google_tasks_bridge")

    def test_a_post_is_refused(self, monkeypatch):
        import pytest

        bridge = self._bridge()
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        with pytest.raises(bridge.TestModeRefusalError) as exc:
            bridge._http_json("https://tasks.googleapis.com/tasks/v1/lists/x/tasks",
                              method="POST", payload={"title": "fixture"})
        assert "operator's real task list" in str(exc.value)

    def test_a_get_still_works(self, monkeypatch):
        """Over-refusing teaches people to switch the guard off."""
        bridge = self._bridge()
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        seen = {}

        class _Resp:
            def read(self):
                return b"{}"

            def getcode(self):
                return 200

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        def _urlopen(request, timeout=None):
            seen["url"] = request.full_url
            return _Resp()

        monkeypatch.setattr(bridge.urllib.request, "urlopen", _urlopen)
        status, payload = bridge._http_json("https://tasks.googleapis.com/x", method="GET")
        assert (status, payload, seen["url"]) == (200, {}, "https://tasks.googleapis.com/x")

    def test_minting_a_token_is_refused(self, monkeypatch):
        """A separate urlopen from _http_json, so it needs its own guard: a
        token mint spends the operator's refresh token."""
        import pytest

        bridge = self._bridge()
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        with pytest.raises(bridge.TestModeRefusalError):
            bridge.fetch_google_access_token({"client_id": "x", "refresh_token": "y"})

    def test_nothing_is_refused_without_the_flag(self, monkeypatch):
        bridge = self._bridge()
        monkeypatch.delenv("DUCK_TEST_MODE", raising=False)
        calls = []
        monkeypatch.setattr(bridge.urllib.request, "urlopen",
                            lambda *a, **k: calls.append(1) or (_ for _ in ()).throw(RuntimeError("reached")))
        try:
            bridge._http_json("https://tasks.googleapis.com/x", method="POST", payload={})
        except RuntimeError:
            pass
        assert calls == [1], "the guard must not fire when the flag is absent"

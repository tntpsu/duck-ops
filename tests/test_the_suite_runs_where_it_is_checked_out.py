"""The suite must test the checkout it is running in.

Until 2026-11-09, 125 of the 151 files here opened paths beginning
`/Users/philtullai/ai-agents/duck-ops`, including `conftest.py`, which put
that absolute `runtime/` on `sys.path`. A clone probe that day ran `pytest`
inside a fresh checkout and watched it report 1486 passing while importing
`/Users/philtullai/ai-agents/duck-ops/runtime/workflow_control.py` -- the
ORIGINAL working tree. A green run in one copy of the repo was grading
another copy's code, and nothing said so
([[feedback_a_failed_probe_is_a_fact_about_the_probe]]).

That is also why this suite had never run in CI: on a runner, those paths do
not exist at all.

This pins the rule rather than the fix. A path may still appear as DATA -- a
launchd plist fixture naming a wrapper, a recorded `state_source` string --
because those are values the code under test parses, not files it opens. What
may not come back is an absolute path used to REACH something.
"""
from __future__ import annotations

import re
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent

# Path("/abs/..."), open("/abs/..."), read_text on a literal: anything that
# turns an absolute literal into a filesystem reference.
_REACHES_OUT = re.compile(
    r'(?:Path|open|load|read_text|read_bytes|glob|listdir|chdir)\s*\(\s*'
    r'(?:f?["\'])/(?:Users|home|private|var)/')


def _offenders() -> list[str]:
    found: list[str] = []
    for path in sorted(TESTS_DIR.glob("*.py")):
        if path.name == Path(__file__).name:
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if _REACHES_OUT.search(line):
                found.append(f"{path.name}:{number}: {line.strip()[:100]}")
    return found


def test_no_test_reaches_outside_its_own_checkout():
    offenders = _offenders()
    assert not offenders, (
        "These tests open an absolute path instead of deriving one from "
        "__file__, so they grade whatever lives at that path rather than the "
        "code under test:\n  " + "\n  ".join(offenders))


def test_the_conftest_derives_the_runtime_it_puts_on_the_path():
    """The single most load-bearing one: conftest's sys.path insert decides
    which copy of runtime/ every other test imports."""
    import conftest

    assert conftest.REPO_ROOT == TESTS_DIR.parent
    assert conftest.RUNTIME_DIR == TESTS_DIR.parent / "runtime"
    import workflow_control

    assert Path(workflow_control.__file__).resolve().is_relative_to(TESTS_DIR.parent), (
        f"imported {workflow_control.__file__} from outside this checkout")

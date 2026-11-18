"""Is a sibling repo checked out beside duck-ops?

duck-ops CI checks out duck-ops ALONE. The workflow has said since
2026-11-09 that "the handful of cross-repo assertions skip BY NAME rather
than failing, so the summary says what this runner could not check instead
of implying it did" -- and that was true of the handful that existed then.
Three later features added hard dependencies without the guard, and CI had
been red on them for days:

  * `drive_mirror`'s code bundles shell out to `git` in each source repo, so
    an absent sibling reports "code bundle duckAgent: not a git repository"
    and the run records a refusal;
  * `drop_digest._reply_buttons` imports `helpers.email_reply_action_helper`
    from duckAgent and returns "" when it cannot, so every
    assertion about an approval button failed;
  * the un-timed-SMTP ratchet scans both repos' files.

A missing sibling is a fact about THIS RUNNER, never about the code
([[feedback_a_failed_probe_is_a_fact_about_the_probe]]). So these skip with
the reason named, and stay hard assertions on the Mac where the whole stack
actually runs and both checkouts exist.

Checking out the siblings in CI would be strictly better -- it would test
the cross-repo contract rather than declining to -- but it needs a
cross-repo token, which is an operator decision, not a test change.
"""
from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Rehearse the CI condition on the Mac, where both siblings DO exist:
#:     DUCK_OPS_NO_SIBLINGS=1 pytest tests -q
#: Worth running before any push that touches a cross-repo path, because the
#: alternative is learning it from a red CI run (which is how this whole file
#: came to exist).
_REHEARSE_ENV = "DUCK_OPS_NO_SIBLINGS"


def rehearsing_without_siblings() -> bool:
    return str(os.environ.get(_REHEARSE_ENV) or "").strip() in {"1", "true", "TRUE", "yes"}


def sibling_repo(name: str) -> Path | None:
    """The sibling CHECKOUT, or None when it is absent.

    Requires `.git`, not merely the directory: the code-bundle step needs a
    git repository, and a bare directory would pass a looser check and then
    fail inside the test with a worse message.
    """
    if rehearsing_without_siblings():
        return None
    path = REPO_ROOT.parent / name
    return path if (path / ".git").exists() else None


def skip_without_siblings(case, *names: str) -> None:
    """Skip `case` naming every sibling this runner does not have."""
    missing = [name for name in names if sibling_repo(name) is None]
    if missing:
        case.skipTest(
            "not checked out beside duck-ops: " + ", ".join(missing)
            + " — this runner cannot verify the cross-repo half"
        )


def duckagent_reply_helper_importable() -> bool:
    """Whether `drop_digest._reply_buttons` can actually render buttons.

    The import is what decides, not the directory: `_reply_buttons` catches
    its own ImportError and returns "", so a test asserting on a button has
    to gate on the same thing that would make one appear.
    """
    if sibling_repo("duckAgent") is None:
        return False
    import sys
    root = str(REPO_ROOT.parent / "duckAgent")
    if root not in sys.path:
        sys.path.insert(0, root)
    try:
        import helpers.email_reply_action_helper  # noqa: F401
    except Exception:
        return False
    return True


def duckagent_preview_helpers_importable() -> bool:
    """Whether `drop_digest._build_preview` can render frames.

    It imports `helpers.duck_video_helper` and `helpers.image_lane_reel`
    from duckAgent (drop_digest.py:191).
    """
    if sibling_repo("duckAgent") is None:
        return False
    import sys
    root = str(REPO_ROOT.parent / "duckAgent")
    if root not in sys.path:
        sys.path.insert(0, root)
    try:
        from helpers import duck_video_helper, image_lane_reel  # noqa: F401
    except Exception:
        return False
    return True


def skip_without_reply_buttons(case) -> None:
    if not duckagent_reply_helper_importable():
        case.skipTest(
            "duckAgent's helpers.email_reply_action_helper is not importable "
            "here, so _reply_buttons renders nothing — this runner cannot "
            "check the approval buttons")


def skip_without_preview_helpers(case) -> None:
    if not duckagent_preview_helpers_importable():
        case.skipTest(
            "duckAgent's helpers.duck_video_helper / image_lane_reel are not "
            "importable here, so no preview frames can be built")

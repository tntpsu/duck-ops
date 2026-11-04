#!/usr/bin/env python3
"""Daily: do the Google model ids we ship still exist on the API?

Origin, 2026-11-04. Every `veo-3.1-*` id was shut down on the Gemini API on
2026-10-22. The duckvideo lane kept running: it caught the 404, logged
`ai_motion_failed`, rendered a ken-burns clip instead and emailed it as normal.
A tagged fallback, so not silent — but nothing ALARMED, and the operator found
out six weeks later by watching a bad reel. The gated eval
(`scripts/eval_duckvideo_motion.py`) resolves the configured id against the
live model list and would have failed loudly on the day; it is manual-trigger
by design, because it also spends money on a real clip.

The preflight half costs nothing. One `models.list` call, no generation, so it
can run daily on its own — which is what this is.

Why it covers more than the video model: `gemini_helper._resolve_image_model`
walks a preference list, then falls back to "any model with 'image' in the
name", then to a hard-coded default. A retired id there does not raise either;
it silently promotes a different model. Same failure shape, one call away.

Grading:

    red     a REQUIRED id is absent — a surface is configured to call
            something that no longer exists
    yellow  a non-required id is absent (the fallback ladder is thinning), or
            an id could not be collected from source
    unknown the probe could not run (no key, listing failed, SDK missing)
    green   every configured id resolves

**The probe failing is never health.** A listing that errors grades `unknown`
with the error attached, never green — the point of this file is that a thing
we could not check is different from a thing we checked and liked
([[feedback_a_failed_probe_is_a_fact_about_the_probe]]).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"

STATE_PATH = DUCK_OPS_ROOT / "state" / "google_model_availability.json"
#: Frozen factory default: a DUCK_TEST_MODE run must never write here.
_FROZEN_STATE_PATH = STATE_PATH.resolve()

SCHEMA_VERSION = "duck_ops.google_model_availability.v1"


class GoogleModelAvailabilityTestModeRefusal(RuntimeError):
    """Raised when a test-mode run would write the production state file."""


def _load_env() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(DUCK_AGENT_ROOT / ".env", override=True)
    except ImportError:
        pass


def configured_ids() -> list[dict[str, Any]]:
    """The ids duckAgent is configured to call, read from its own constants."""
    if str(DUCK_AGENT_ROOT) not in sys.path:
        sys.path.insert(0, str(DUCK_AGENT_ROOT))
    try:
        from helpers.google_model_inventory import configured_google_model_ids

        return list(configured_google_model_ids())
    except Exception as exc:  # noqa: BLE001
        return [{"id": None, "purpose": "model inventory", "required": True,
                 "source": "helpers.google_model_inventory",
                 "error": f"{type(exc).__name__}: {exc}"}]


def live_model_names(api_key: str | None = None) -> dict[str, Any]:
    """One ListModels call. Returns {"names": [...]} or {"error": "..."}."""
    key = api_key if api_key is not None else os.getenv("GEMINI_KEY")
    if not key:
        return {"error": "no_gemini_key"}
    try:
        from google import genai

        client = genai.Client(api_key=key)
        names = []
        for model in client.models.list():
            name = getattr(model, "name", None)
            if not name and isinstance(model, dict):
                name = model.get("name")
            if name:
                names.append(str(name))
        if not names:
            return {"error": "empty_model_list"}
        return {"names": names}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {str(exc)[:300]}"}


def _resolves(model_id: str, names: list[str]) -> bool:
    """The API accepts both `gemini-x` and `models/gemini-x`; ListModels
    returns the prefixed form. Compare on the bare id so a configured value in
    either spelling is judged on what it IS, not how it is written."""
    bare = str(model_id or "").split("/")[-1]
    return any(str(n).split("/")[-1] == bare for n in names)


def grade(entries: list[dict[str, Any]], listing: dict[str, Any],
          *, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now().astimezone()
    names = list(listing.get("names") or [])
    listing_error = listing.get("error")

    checked: list[dict[str, Any]] = []
    missing_required: list[str] = []
    missing_optional: list[str] = []
    uncollected: list[str] = []

    for entry in entries:
        model_id = entry.get("id")
        row = {k: v for k, v in entry.items()}
        if not model_id:
            row["resolves"] = None
            row["verdict"] = "uncollected"
            uncollected.append(str(entry.get("purpose") or entry.get("source") or "?"))
        elif listing_error:
            row["resolves"] = None
            row["verdict"] = "unchecked"
        else:
            ok = _resolves(str(model_id), names)
            row["resolves"] = ok
            row["verdict"] = "ok" if ok else "missing"
            if not ok:
                (missing_required if entry.get("required") else missing_optional).append(
                    str(model_id))
        checked.append(row)

    if listing_error:
        status = "unknown"
        reason = (f"Could not list Google models ({listing_error}); "
                  f"{len(checked)} configured id(s) unchecked.")
        action = ("Check GEMINI_KEY and connectivity, then re-run "
                  "runtime/google_model_availability.py.")
    elif missing_required:
        status = "red"
        reason = ("Configured model(s) no longer on the API: "
                  + ", ".join(sorted(missing_required)) + ".")
        action = ("Pick a live replacement and update the constant named in "
                  "`source`; DUCKVIDEO_VIDEO_MODEL / GEMINI_IMAGE_MODEL "
                  "override without a deploy.")
    elif missing_optional or uncollected:
        status = "yellow"
        bits = []
        if missing_optional:
            bits.append("fallback model(s) gone: " + ", ".join(sorted(missing_optional)))
        if uncollected:
            bits.append("could not read configured ids for: " + ", ".join(sorted(uncollected)))
        reason = "; ".join(bits) + "."
        action = ("Prune the retired ids from the preference list so the "
                  "ladder reflects what exists.")
    else:
        status = "green"
        reason = f"All {len(checked)} configured Google model id(s) resolve on the API."
        action = ""

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now.isoformat(),
        "status": status,
        "status_reason": reason,
        "recommended_action": action,
        "models": checked,
        "missing_required": sorted(missing_required),
        "missing_optional": sorted(missing_optional),
        "uncollected": sorted(uncollected),
        "live_model_count": len(names),
        "listing_error": listing_error or "",
    }


def _write_refused(path: Path) -> bool:
    return (os.getenv("DUCK_TEST_MODE") == "1"
            and Path(path).resolve() == _FROZEN_STATE_PATH)


def write_state(payload: dict[str, Any], path: Path | None = None) -> None:
    target = Path(path or STATE_PATH)
    if _write_refused(target):
        raise GoogleModelAvailabilityTestModeRefusal(
            f"DUCK_TEST_MODE=1 refuses to write the production state file {target}. "
            "Point the test at a tmp path."
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, target)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="probe and print, write nothing")
    args = parser.parse_args()

    _load_env()
    payload = grade(configured_ids(), live_model_names())
    if args.dry_run:
        print(json.dumps(payload, indent=2))
        return 0
    write_state(payload)
    print(f"[google_model_availability] {payload['status']}: {payload['status_reason']}")
    # Exit code mirrors the verdict so a scheduler receipt is honest about it.
    return 1 if payload["status"] == "red" else 0


if __name__ == "__main__":
    raise SystemExit(main())

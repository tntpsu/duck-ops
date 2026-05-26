"""
Agent OS triage tool.

Translates a "Repair now" item on the Agent OS portal page into a
concrete root-cause brief: which failure modes, in what proportions,
with sample evidence pulled from the underlying state files.

The Agent OS page tells you ``review_reply_rewriter_health`` is red
with ``api=19%, sanity=19%``. This tool tells you which API errors
(http_500 vs rate_limit vs schema), which sanity rejections (echo
vs length vs refusal), shows the rejected outputs verbatim, and
maps each failure mode to a fix category (prompt / code / data /
provider).

Pattern came from the worked example on the Review Reply Rewriter
on 2026-05-26. Captured here so subsequent OS findings get the same
treatment without ad-hoc grep.

USAGE

    python -m agent_os_triage --area review_reply_rewriter_health
    python -m agent_os_triage --all-red
    python -m agent_os_triage --all-red --include-warn

The system_health.json path defaults to the runtime output (see
DEFAULT_HEALTH_PATH). Override with --health-path or DUCK_OS_HEALTH_PATH
env var.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"
DEFAULT_HEALTH_PATH = (
    DUCK_AGENT_ROOT
    / "creative_agent"
    / "runtime"
    / "output"
    / "operator"
    / "system_health.json"
)

# Areas with a log_path field whose JSONL records carry an `outcome`
# tally — the rewriter pattern. New analyzers go below this list.
_JSONL_OUTCOME_ANALYZERS: dict[str, dict[str, Any]] = {
    "review_reply_rewriter_health": {
        "log_field": "call_log_path",
        "filter_kind": "review_reply_rewrite",
        "rejected_field": "rejected_output_text",
        "max_samples": 3,
    },
}


def _load_health(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(
            f"system_health.json not found at {path}. Pass --health-path or "
            "set DUCK_OS_HEALTH_PATH to point at the right file."
        )
    return json.loads(path.read_text(encoding="utf-8"))


def _is_area(entry: Any) -> bool:
    """system_health.json mixes plain metric blocks (overall_status,
    overall_label) with structured health areas. An area always has a
    ``_status`` field stamped on it by the producer."""
    return isinstance(entry, dict) and "_status" in entry


def select_areas(
    health: dict[str, Any],
    *,
    area: str | None = None,
    all_red: bool = False,
    include_warn: bool = False,
) -> list[tuple[str, dict[str, Any]]]:
    """Pick which areas to triage.

    --area: a specific key, returned as-is even if green.
    --all-red: every area with _status in {red, bad}.
    --include-warn: also include _status in {warn, watch}.
    """
    if area:
        block = health.get(area)
        if not _is_area(block):
            raise KeyError(f"No area named {area!r} in system_health.json")
        return [(area, block)]
    red_statuses = {"red", "bad"}
    warn_statuses = {"warn", "watch", "stale"}
    selected: list[tuple[str, dict[str, Any]]] = []
    for key, block in health.items():
        if not _is_area(block):
            continue
        status = str(block.get("_status") or "").lower()
        if all_red and status in red_statuses:
            selected.append((key, block))
        elif include_warn and status in warn_statuses:
            selected.append((key, block))
    return selected


def _tally_jsonl(log_path: Path, *, filter_kind: str | None, rejected_field: str | None, max_samples: int) -> dict[str, Any]:
    """Walk an LLM call-log JSONL. Tally outcomes, sanity_failures,
    and api errors. Capture up to ``max_samples`` rejected outputs
    per failure mode — that's the "show me the bad output" the
    operator actually needs."""
    if not log_path.exists():
        return {"available": False, "reason": f"log not found at {log_path}"}
    outcomes: Counter[str] = Counter()
    sanity: Counter[str] = Counter()
    api_errors: Counter[str] = Counter()
    samples_by_mode: dict[str, list[dict[str, str]]] = {}
    total = 0
    for raw in log_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except (ValueError, json.JSONDecodeError):
            continue
        if filter_kind and rec.get("kind") != filter_kind:
            continue
        total += 1
        outcome = str(rec.get("outcome") or "unknown")
        outcomes[outcome] += 1
        if outcome == "api_failure":
            api_errors[str(rec.get("error") or "")[:80] or "unknown"] += 1
        sanity_failures = rec.get("sanity_failures") or []
        for sf in sanity_failures:
            name = sf if isinstance(sf, str) else (sf.get("reason") or sf.get("name") or "unknown")
            sanity[str(name)] += 1
            bucket = samples_by_mode.setdefault(str(name), [])
            if len(bucket) < max_samples and rejected_field:
                rejected = rec.get(rejected_field) or ""
                if rejected:
                    bucket.append({"rejected_output": rejected[:300]})
    return {
        "available": True,
        "log_path": str(log_path),
        "total_calls": total,
        "outcomes": dict(outcomes),
        "sanity_failures": dict(sanity),
        "api_errors": dict(api_errors),
        "samples_by_mode": samples_by_mode,
    }


def _classify_failure_mode(name: str) -> str:
    """Map a failure-mode name to a fix category so the brief
    suggests the right next move. Heuristics, not exhaustive."""
    n = name.lower()
    if n.startswith("http_") or "rate_limit" in n or "request_failed" in n:
        return "provider"
    if "unparseable" in n or "schema" in n or "json_decode" in n:
        return "code (output parsing / schema)"
    if "echo" in n or "length" in n or "refusal" in n or "placeholder" in n:
        return "prompt"
    if "missing" in n:
        return "data"
    return "unknown"


def triage_area(key: str, block: dict[str, Any]) -> dict[str, Any]:
    """Produce a structured triage dict for one area. The dict gets
    rendered to markdown by render_brief; keeping the structured form
    makes this callable from other tools too."""
    out: dict[str, Any] = {
        "key": key,
        "status": block.get("_status"),
        "subtype": block.get("_attention_subtype"),
        "reason": block.get("_status_reason"),
        "structured_fields": {
            k: v
            for k, v in block.items()
            if not k.startswith("_") and isinstance(v, (int, float, str, bool))
        },
        "jsonl_tally": None,
        "fix_categories": [],
    }
    analyzer = _JSONL_OUTCOME_ANALYZERS.get(key)
    if analyzer:
        log_path_str = block.get(analyzer["log_field"])
        if log_path_str:
            tally = _tally_jsonl(
                Path(log_path_str),
                filter_kind=analyzer.get("filter_kind"),
                rejected_field=analyzer.get("rejected_field"),
                max_samples=int(analyzer.get("max_samples") or 3),
            )
            out["jsonl_tally"] = tally
            if tally.get("available"):
                modes = list(tally["sanity_failures"].keys()) + list(tally["api_errors"].keys())
                cats: list[str] = []
                for mode in modes:
                    cat = _classify_failure_mode(mode)
                    if cat not in cats:
                        cats.append(cat)
                out["fix_categories"] = cats
    return out


def render_brief(triage: dict[str, Any]) -> str:
    """Markdown rendering of one triage dict. Mirrors the worked
    example from the conversation that birthed this skill."""
    lines: list[str] = []
    lines.append(f"## {triage['key']}")
    lines.append("")
    status_line = f"**Status:** {triage.get('status') or '?'}"
    if triage.get("subtype"):
        status_line += f" ({triage['subtype']})"
    lines.append(status_line)
    if triage.get("reason"):
        lines.append(f"**Reason:** {triage['reason']}")
    lines.append("")
    tally = triage.get("jsonl_tally")
    if tally and tally.get("available"):
        lines.append(f"### Call-log tally — {tally['total_calls']} records")
        lines.append(f"_Source: `{tally['log_path']}`_")
        lines.append("")
        if tally["outcomes"]:
            lines.append("**Outcomes:**")
            total = tally["total_calls"] or 1
            for name, count in sorted(tally["outcomes"].items(), key=lambda x: -x[1]):
                pct = round(100 * count / total, 1)
                lines.append(f"- `{name}`: {count} ({pct}%)")
            lines.append("")
        if tally["sanity_failures"]:
            lines.append("**Sanity rejection breakdown:**")
            for name, count in sorted(tally["sanity_failures"].items(), key=lambda x: -x[1]):
                cat = _classify_failure_mode(name)
                lines.append(f"- `{name}` × {count}  _(category: {cat})_")
            lines.append("")
        if tally["api_errors"]:
            lines.append("**API error breakdown:**")
            for name, count in sorted(tally["api_errors"].items(), key=lambda x: -x[1]):
                cat = _classify_failure_mode(name)
                lines.append(f"- `{name}` × {count}  _(category: {cat})_")
            lines.append("")
        for mode, samples in tally.get("samples_by_mode", {}).items():
            if not samples:
                continue
            lines.append(f"**Sample rejected outputs — `{mode}`:**")
            for s in samples:
                lines.append(f"- `{s['rejected_output']}`")
            lines.append("")
    elif tally:
        lines.append(f"_Evidence log: {tally.get('reason')}_")
        lines.append("")
    if triage.get("fix_categories"):
        lines.append(f"**Fix categories:** {', '.join(triage['fix_categories'])}")
        lines.append("")
    if triage.get("structured_fields"):
        lines.append("**Structured metrics (from system_health.json):**")
        for k, v in triage["structured_fields"].items():
            lines.append(f"- `{k}` = `{v}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--area", help="Specific area key (e.g. review_reply_rewriter_health)")
    parser.add_argument("--all-red", action="store_true", help="Triage every red/repair-now area")
    parser.add_argument("--include-warn", action="store_true", help="Also include warn/watch/stale areas")
    parser.add_argument(
        "--health-path",
        default=os.environ.get("DUCK_OS_HEALTH_PATH") or str(DEFAULT_HEALTH_PATH),
        help="Path to system_health.json",
    )
    args = parser.parse_args(argv)

    if not (args.area or args.all_red or args.include_warn):
        parser.error("specify --area or --all-red (optionally with --include-warn)")

    health = _load_health(Path(args.health_path))
    selected = select_areas(
        health,
        area=args.area,
        all_red=args.all_red,
        include_warn=args.include_warn,
    )
    if not selected:
        print("(no areas matched the selection — system is healthy or filter was too narrow)")
        return 0
    for key, block in selected:
        triage = triage_area(key, block)
        sys.stdout.write(render_brief(triage))
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

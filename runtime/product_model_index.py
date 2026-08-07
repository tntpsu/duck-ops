"""Product→3D-model index producer (Surface 64, duckvideo lane).

The duckvideo flow needs to answer "which local 3D model file renders THIS
Shopify product?" — no such mapping existed anywhere. This producer scans the
model source locations and joins them to the catalog deterministically:

Sources scanned (newest wins within a product, obj_mtl beats glb):
  1. paint-to-print-3d/outputs/<job>/handoff_manifest.json — production-grade
     conversions; `ready_for_duckagent_handoff: true` + `source.object_name`
     is the strongest signal (schema duckagent.paint_to_print_handoff.v1).
  2. paint-to-print-3d/outputs/<job>/selected/region_materials.obj without a
     manifest — older conversions; join text falls back to the job dir name.
  3. duckAgent .../runs/studio_assets/3d_lab/portal_studio_3d_<ts>_<slug>/ —
     Studio deliveries (textured GLBs); slug is the concept name, NOT the
     Shopify handle.

Join: deterministic token coverage against catalog title/handle/tags/aliases
(reuses catalog_match primitives — no LLM). A model whose tokens are not
fully covered by exactly one product gets `needs_review`, never a silent
guess; wrong joins are additionally caught by the human approval email that
shows the rendered video.

Output: state/product_model_index.json (atomic write), consumed by
duckAgent flows/duckvideo/steps.py as a cheap reader.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import catalog_match
from workflow_control import TestModeRefusalError

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
AI_AGENTS_ROOT = DUCK_OPS_ROOT.parent

PAINT_OUTPUTS_DIR = AI_AGENTS_ROOT / "paint-to-print-3d" / "outputs"
STUDIO_3D_LAB_DIR = (
    AI_AGENTS_ROOT / "duckAgent" / "creative_agent" / "runtime" / "runs"
    / "studio_assets" / "3d_lab"
)
PRODUCT_MODEL_INDEX_PATH = DUCK_OPS_ROOT / "state" / "product_model_index.json"

# Frozen at import: layer 2 of the 3-layer isolation policy. A test that
# reaches this path under DUCK_TEST_MODE has escaped the conftest redirect.
_FROZEN_PRODUCTION_PRODUCT_MODEL_INDEX_PATH = (
    DUCK_OPS_ROOT / "state" / "product_model_index.json"
).resolve()

# Model-source dir/object names carry conversion jargon that would
# false-positive against catalog text ("3d printed" appears in most titles).
_MODEL_NOISE_TOKENS = {
    "reference", "model", "handoff", "print", "printed", "preview",
    "conversion", "corrected", "fixed", "repaired", "generic", "single",
    "multiview", "quicklook", "oracle", "voxel", "smooth", "intent",
    "generalization", "cleanup", "transfer", "bake", "glb", "obj",
}

_STUDIO_DIR_RE = re.compile(r"^portal_studio_3d_\d{8}_\d{6}_\d+_(?P<slug>.+)$")


def _model_tokens(text: str) -> list[str]:
    tokens = catalog_match._theme_tokens(text)
    return [t for t in tokens if t not in _MODEL_NOISE_TOKENS and not t.isdigit()]


def _latest_mtime(paths: list[Path]) -> float:
    best = 0.0
    for p in paths:
        try:
            best = max(best, p.stat().st_mtime)
        except OSError:
            continue
    return best


def _scan_paint_outputs(outputs_dir: Path) -> list[dict[str, Any]]:
    models: list[dict[str, Any]] = []
    if not outputs_dir.is_dir():
        return models
    for job_dir in sorted(outputs_dir.iterdir()):
        if not job_dir.is_dir():
            continue
        manifest_path = job_dir / "handoff_manifest.json"
        manifest: dict[str, Any] = {}
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                manifest = {}
        ready = bool(manifest.get("ready_for_duckagent_handoff"))
        obj = job_dir / "selected" / "region_materials.obj"
        mtl = job_dir / "selected" / "region_materials.mtl"
        if not obj.exists():
            continue
        object_name = str(((manifest.get("source") or {}).get("object_name")) or "")
        slug_text = object_name or job_dir.name
        models.append({
            "kind": "obj_mtl",
            "slug_text": slug_text,
            "obj": str(obj),
            "mtl": str(mtl) if mtl.exists() else None,
            "glb": None,
            "dir": str(job_dir),
            "manifest": str(manifest_path) if manifest else None,
            "handoff_ready": ready,
            "mtime": _latest_mtime([obj, mtl]),
        })
    return models


def _scan_studio_3d_lab(lab_dir: Path) -> list[dict[str, Any]]:
    models: list[dict[str, Any]] = []
    if not lab_dir.is_dir():
        return models
    for run_dir in sorted(lab_dir.iterdir()):
        match = _STUDIO_DIR_RE.match(run_dir.name)
        if not run_dir.is_dir() or not match:
            continue
        glb = None
        for name in ("3dai_single_model.glb", "3dai_multiview_model.glb"):
            candidate = run_dir / name
            if candidate.exists():
                glb = candidate
                break
        if glb is None:
            continue
        models.append({
            "kind": "glb",
            "slug_text": match.group("slug").replace("-", " "),
            "obj": None,
            "mtl": None,
            "glb": str(glb),
            "dir": str(run_dir),
            "manifest": None,
            "handoff_ready": False,
            "mtime": _latest_mtime([glb]),
        })
    return models


def scan_model_sources(
    *,
    paint_outputs_dir: Path | None = None,
    studio_lab_dir: Path | None = None,
) -> list[dict[str, Any]]:
    return (
        _scan_paint_outputs(paint_outputs_dir or PAINT_OUTPUTS_DIR)
        + _scan_studio_3d_lab(studio_lab_dir or STUDIO_3D_LAB_DIR)
    )


def _match_one(
    model: dict[str, Any],
    catalog: dict[str, dict[str, Any]],
    alias_by_key: dict[str, list[str]],
) -> dict[str, Any]:
    """Score one model against every catalog product.

    Returns {product_id, matched_by, match_score, needs_review, reason}
    or {product_id: None, reason} when nothing plausible matched.
    """
    tokens = _model_tokens(model["slug_text"])
    if not tokens:
        return {"product_id": None, "reason": "no_usable_tokens"}
    full: list[tuple[str, dict[str, Any], int]] = []
    partial: list[tuple[str, dict[str, Any], int]] = []
    for pid, item in catalog.items():
        if not isinstance(item, dict):
            continue
        alias_text = (
            alias_by_key.get(str(pid), [])
            + alias_by_key.get(str(item.get("handle") or ""), [])
        )
        haystack = catalog_match._product_haystack(item, alias_text)
        matched = [t for t in tokens if t in haystack]
        if not matched:
            continue
        if len(matched) == len(tokens):
            full.append((str(pid), item, len(matched)))
        else:
            partial.append((str(pid), item, len(matched)))
    if len(full) == 1:
        pid, _, score = full[0]
        return {
            "product_id": pid,
            "matched_by": "full_token_coverage",
            "match_score": score,
            "matched_tokens": tokens,
            "needs_review": len(tokens) < 2,
            "reason": "single_full_match" if len(tokens) >= 2
            else "single_token_match_low_confidence",
        }
    if len(full) > 1:
        # Ambiguous: several products fully cover the model's tokens.
        # Prefer none silently — flag the strongest for review.
        full.sort(key=lambda r: -r[2])
        return {
            "product_id": full[0][0],
            "matched_by": "ambiguous_full_coverage",
            "match_score": full[0][2],
            "matched_tokens": tokens,
            "needs_review": True,
            "reason": f"ambiguous_across_{len(full)}_products",
        }
    if partial:
        partial.sort(key=lambda r: -r[2])
        pid, _, score = partial[0]
        if score / len(tokens) >= 0.6 and score >= 2:
            return {
                "product_id": pid,
                "matched_by": "partial_token_coverage",
                "match_score": score,
                "matched_tokens": tokens,
                "needs_review": True,
                "reason": f"partial_coverage_{score}_of_{len(tokens)}",
            }
    return {"product_id": None, "reason": "no_confident_match"}


def match_models_to_catalog(
    models: list[dict[str, Any]],
    catalog: dict[str, dict[str, Any]],
    aliases: list[dict[str, Any]],
) -> dict[str, Any]:
    alias_by_key = catalog_match._alias_text_by_product_key(aliases)
    items: dict[str, dict[str, Any]] = {}
    unmatched: list[dict[str, Any]] = []
    for model in models:
        verdict = _match_one(model, catalog, alias_by_key)
        pid = verdict.get("product_id")
        if not pid:
            unmatched.append({
                "dir": model["dir"], "kind": model["kind"],
                "slug_text": model["slug_text"], "reason": verdict["reason"],
            })
            continue
        candidate = {
            "handle": str(catalog.get(pid, {}).get("handle") or ""),
            "title": str(catalog.get(pid, {}).get("title") or ""),
            "model": {
                k: model[k]
                for k in ("kind", "obj", "mtl", "glb", "dir", "manifest",
                          "handoff_ready", "mtime", "slug_text")
            },
            "matched_by": verdict["matched_by"],
            "match_score": verdict["match_score"],
            "matched_tokens": verdict["matched_tokens"],
            "needs_review": bool(verdict["needs_review"]),
            "match_reason": verdict["reason"],
        }
        existing = items.get(pid)
        if existing is None or _model_beats(candidate["model"], existing["model"]):
            items[pid] = candidate
    without_model = sorted(
        pid for pid, item in catalog.items()
        if isinstance(item, dict)
        and str(item.get("status") or "") == "active"
        and str(pid) not in items
    )
    return {
        "items": items,
        "unmatched_models": unmatched,
        "products_without_model": without_model,
    }


def _model_beats(a: dict[str, Any], b: dict[str, Any]) -> bool:
    """obj_mtl (print-production geometry) beats glb; then handoff-ready
    beats not; then newest."""
    rank = {"obj_mtl": 1, "glb": 0}
    key_a = (rank.get(a["kind"], -1), int(bool(a.get("handoff_ready"))), a.get("mtime") or 0)
    key_b = (rank.get(b["kind"], -1), int(bool(b.get("handoff_ready"))), b.get("mtime") or 0)
    return key_a > key_b


def _guard_index_write(path: Path) -> None:
    if (
        os.environ.get("DUCK_TEST_MODE") == "1"
        and Path(path).resolve() == _FROZEN_PRODUCTION_PRODUCT_MODEL_INDEX_PATH
    ):
        raise TestModeRefusalError(
            f"product_model_index write refused: DUCK_TEST_MODE=1 and {path} "
            "is the frozen production index — monkeypatch PRODUCT_MODEL_INDEX_PATH."
        )


def write_product_model_index(payload: dict[str, Any], path: Path | None = None) -> Path:
    target = Path(path or PRODUCT_MODEL_INDEX_PATH)
    _guard_index_write(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{target.name}.", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, target)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return target


def run_once(
    *,
    dry_run: bool = False,
    paint_outputs_dir: Path | None = None,
    studio_lab_dir: Path | None = None,
    catalog_index_path: Path | None = None,
    aliases_path: Path | None = None,
    index_path: Path | None = None,
) -> dict[str, Any]:
    models = scan_model_sources(
        paint_outputs_dir=paint_outputs_dir, studio_lab_dir=studio_lab_dir,
    )
    catalog = catalog_match._load_catalog_index(catalog_index_path)
    aliases = catalog_match._load_aliases(aliases_path)
    result = match_models_to_catalog(models, catalog, aliases)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model_sources_scanned": len(models),
        **result,
    }
    if not dry_run:
        write_product_model_index(payload, path=index_path)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the product→3D-model index")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--print-json", action="store_true")
    args = parser.parse_args()
    payload = run_once(dry_run=args.dry_run)
    if args.print_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        matched = len(payload["items"])
        review = sum(1 for v in payload["items"].values() if v["needs_review"])
        print(
            f"product_model_index: {matched} matched ({review} needs_review), "
            f"{len(payload['unmatched_models'])} unmatched models, "
            f"{len(payload['products_without_model'])} active products without a model"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

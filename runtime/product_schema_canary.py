"""Watch the product structured data that earns star ratings in Google.

On 2026-10-15 the storefront emitted TWO `Product` nodes per product page and
neither was usable: Smart SEO's had the price but no rating, Judge.me's had the
rating but no price and an `@id` meant to merge into a node that carried none.
Google showed stars for nothing. The fix lives entirely inside two third-party
app UIs -- Smart SEO's review source, and Judge.me's "Add JSON-LD snippets"
checkbox, which ships CHECKED. No repo file holds it, our Shopify token cannot
read it (`appInstallations` -> ACCESS_DENIED), and nothing would announce a
regression. A re-tick during a Judge.me update silently restores the broken
state and costs search traffic for as long as nobody re-reads a product page.

So this producer re-reads them. Per page it answers three questions:

  1. Is Judge.me still suppressing its own JSON-LD?  `jdgmSettings` is embedded
     in the page HTML and `disable_json_ld` is the exact flag the operator
     flipped -- one key out of 640 changed between the broken and fixed states,
     which makes the regression detectable from a plain fetch. No browser.
  2. Does the one good node carry an `aggregateRating` matching the rating
     Shopify has stored for that product?
  3. Is that rating on a node that also carries `offers`? A rating without a
     price is not eligible for a merchant rich result, which is precisely how
     the old Judge.me stub failed.

The hard rule this module exists to honour: **"nothing is wrong" and "I could
not check" are different answers.** A producer that cannot read the storefront
reports zero problems, and zero problems rendered as green is the fake
reassurance that let the original defect live. `pages_checked` and
`pages_unreadable` are therefore separate counters, and the input-sanity card
goes RED on a blind run rather than green.
"""
from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable

DUCK_OPS_ROOT = Path(__file__).resolve().parents[1]
DUCK_AGENT_ROOT = DUCK_OPS_ROOT.parent / "duckAgent"
DUCK_AGENT_VENV_PY = DUCK_AGENT_ROOT / ".venv" / "bin" / "python3"

PRODUCT_SCHEMA_CANARY_PATH = DUCK_OPS_ROOT / "state" / "product_schema_canary.json"
# Frozen at import -- the FACTORY-DEFAULT production path, never monkeypatched.
# The guard compares the LIVE module constant against this, so a test that
# patches the constant to tmp sails through and one that forgets gets caught.
_FROZEN_PRODUCTION_PRODUCT_SCHEMA_CANARY_PATH = PRODUCT_SCHEMA_CANARY_PATH.resolve()
_TEST_MODE_ENV = "DUCK_TEST_MODE"

STOREFRONT_BASE_URL = "https://www.myjeepduck.com"
DEFAULT_SAMPLE_SIZE = 20
# The storefront 429s readily: eight parallel requests produced 23 false
# "no Product node" failures that looked like unpublished products. Sequential
# with spacing, and a 429 is recorded as unreadable rather than as a defect.
FETCH_SPACING_SECONDS = 0.8
FETCH_RETRIES_ON_RATE_LIMIT = 4
UNRATED_CONTROL_COUNT = 3
ANCHOR_COUNT = 5

_LD_JSON_RE = re.compile(
    r'<script[^>]*type\s*=\s*["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.S | re.I,
)
_JDGM_SETTINGS_RE = re.compile(r"jdgmSettings\s*=\s*\{")


class TestModeRefusalError(RuntimeError):
    """Raised when a write would land in the factory-default production state
    path while DUCK_TEST_MODE=1."""


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


# --------------------------------------------------------------------------
# page parsing
# --------------------------------------------------------------------------

def extract_ld_json_documents(html: str) -> list[Any]:
    """Every parseable ld+json document on the page. Unparseable blocks are
    skipped rather than raised: a third-party app shipping broken JSON must not
    blind the check on the blocks that are fine."""
    docs: list[Any] = []
    for raw in _LD_JSON_RE.findall(html or ""):
        try:
            docs.append(json.loads(raw.strip()))
        except (ValueError, TypeError):
            continue
    return docs


def product_nodes(document: Any) -> list[dict]:
    """All Product / ProductGroup nodes in a document, walking the containers
    Smart SEO actually uses. `@graph` holds the product beside
    MerchantReturnPolicy and OfferShippingDetails; `hasVariant` holds the
    per-variant Products of a ProductGroup. A walker that only looks at the top
    level reports zero Product nodes on every page and reads as a catastrophe."""
    found: list[dict] = []

    def walk(node: Any) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, dict):
            return
        if node.get("@type") in ("Product", "ProductGroup"):
            found.append(node)
        for key in ("@graph", "hasVariant", "mainEntity"):
            if node.get(key) is not None:
                walk(node[key])

    walk(document)
    return found


def judgeme_json_ld_disabled(html: str) -> bool | None:
    """Value of `jdgmSettings.disable_json_ld`, or None when the blob is absent
    or unparseable. True is the healthy state: Judge.me is NOT emitting its own
    Product node, so Smart SEO's is the only one.

    None is deliberately not folded into False. "Judge.me is emitting again" is
    a regression; "I could not find the setting" is a gap in this check, and
    they get different statuses."""
    match = _JDGM_SETTINGS_RE.search(html or "")
    if not match:
        return None
    start = match.end() - 1
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(html)):
        char = html[index]
        # Brace counting has to ignore braces inside string VALUES -- the real
        # blob is 640 keys of operator-authored copy and one "{...}" in a label
        # would otherwise truncate the parse and report None.
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                try:
                    settings = json.loads(html[start:index + 1])
                except (ValueError, TypeError):
                    return None
                value = settings.get("disable_json_ld")
                return bool(value) if isinstance(value, bool) else None
    return None


def _rating_value(node: dict) -> tuple[float | None, int | None]:
    rating = node.get("aggregateRating")
    if not isinstance(rating, dict):
        return None, None
    try:
        value = float(rating.get("ratingValue"))
    except (TypeError, ValueError):
        value = None
    try:
        count = int(float(rating.get("reviewCount")))
    except (TypeError, ValueError):
        count = None
    return value, count


def _offer_reachable(node: dict) -> bool:
    """Is there a price Google can attach to this rating?

    A plain `Product` must carry `offers` itself. A `ProductGroup`
    legitimately does not: per Google's variant spec the group carries
    `aggregateRating` + `hasVariant` and the offers live on the variant
    `Product` entries. Requiring offers on the group flagged two perfectly
    correct pages on this canary's first live run -- football-ducks (30
    variants) and dachshund-duck (2), both with every variant priced.

    Smart SEO emits both shapes: build-your-own-duck puts `offers` on the group
    as well. So accept either, and only call it a defect when there is no
    reachable price anywhere under the rating.
    """
    if node.get("offers") is not None:
        return True
    variants = node.get("hasVariant")
    if isinstance(variants, list):
        return any(isinstance(v, dict) and v.get("offers") is not None for v in variants)
    return False


def evaluate_page(
    html: str,
    *,
    handle: str,
    expected_rating: float | None,
    expected_count: int | None,
) -> dict:
    """One page's verdict. `expected_rating` None means Shopify has no rating
    stored, in which case the page must NOT claim one."""
    docs = extract_ld_json_documents(html)
    nodes: list[dict] = []
    for doc in docs:
        nodes.extend(product_nodes(doc))
    rated = [node for node in nodes if isinstance(node.get("aggregateRating"), dict)]
    jm_disabled = judgeme_json_ld_disabled(html)

    result: dict[str, Any] = {
        "handle": handle,
        "ld_json_blocks": len(docs),
        "product_nodes": len(nodes),
        "rating_nodes": len(rated),
        "judgeme_json_ld_disabled": jm_disabled,
        "expected_rating": expected_rating,
        "expected_review_count": expected_count,
        "page_rating": None,
        "page_review_count": None,
        "offers_on_rating_node": None,
    }

    # Judge.me emitting again is the headline regression and outranks every
    # other verdict on the page, because it is what silently broke stars once.
    if jm_disabled is False:
        result["verdict"] = "duplicate_emitter"
        return result
    if not nodes:
        result["verdict"] = "no_product_node"
        return result

    if expected_rating is None:
        result["verdict"] = "unexpected_rating" if rated else "ok_unrated"
        if rated:
            value, count = _rating_value(rated[0])
            result["page_rating"], result["page_review_count"] = value, count
        return result

    if not rated:
        result["verdict"] = "rating_missing"
        return result
    if len(rated) > 1:
        result["verdict"] = "duplicate_rating_nodes"
        return result

    node = rated[0]
    value, count = _rating_value(node)
    has_offers = _offer_reachable(node)
    result["page_rating"], result["page_review_count"] = value, count
    result["offers_on_rating_node"] = has_offers

    if value is None or abs(value - float(expected_rating)) >= 0.01:
        result["verdict"] = "rating_mismatch"
    elif expected_count is not None and count != expected_count:
        result["verdict"] = "rating_mismatch"
    elif not has_offers:
        result["verdict"] = "rating_without_offers"
    else:
        result["verdict"] = "ok"
    return result


# --------------------------------------------------------------------------
# sampling
# --------------------------------------------------------------------------

def select_sample(
    catalog: Iterable[dict],
    *,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    rotation: int = 0,
) -> list[dict]:
    """Pick which products to check today.

    Anchors (highest review counts) are checked every run because they carry
    the most search value. The rest of the rated budget rotates so coverage
    compounds across days instead of re-reading the same twenty pages forever.
    A few unrated products ride along as controls: if the markup ever starts
    claiming ratings for products with no reviews, that is a fabricated-review
    problem and worse than having no stars at all.

    Deterministic for a given `rotation` so a test can assert the selection
    without depending on the day it runs."""
    items = [dict(item) for item in catalog or []]
    rated = [item for item in items if item.get("expected_rating") is not None]
    unrated = [item for item in items if item.get("expected_rating") is None]
    if sample_size <= 0 or not items:
        return []

    rated.sort(key=lambda item: (-int(item.get("expected_review_count") or 0),
                                 str(item.get("handle") or "")))
    controls = min(UNRATED_CONTROL_COUNT, len(unrated), max(0, sample_size - 1))
    rated_budget = max(0, sample_size - controls)

    anchors = rated[:min(ANCHOR_COUNT, rated_budget)]
    pool = rated[len(anchors):]
    rotating_budget = max(0, rated_budget - len(anchors))
    rotating: list[dict] = []
    if pool and rotating_budget:
        start = (int(rotation) * rotating_budget) % len(pool)
        doubled = pool + pool
        rotating = doubled[start:start + min(rotating_budget, len(pool))]

    chosen = anchors + rotating
    if controls and unrated:
        unrated.sort(key=lambda item: str(item.get("handle") or ""))
        start = (int(rotation) * controls) % len(unrated)
        doubled = unrated + unrated
        chosen += doubled[start:start + controls]
    return chosen[:sample_size]


# --------------------------------------------------------------------------
# live data
# --------------------------------------------------------------------------

def _ensure_shopify_imports():
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        load_dotenv = None
    if load_dotenv is not None:
        load_dotenv(DUCK_AGENT_ROOT / ".env", override=False)
    sys.path.insert(0, str(DUCK_AGENT_ROOT))
    from helpers.shopify_helper import shopify_graphql  # type: ignore
    return shopify_graphql


_CATALOG_QUERY = """
query($cursor: String) {
  products(first: 100, after: $cursor, query: "status:active") {
    pageInfo { hasNextPage endCursor }
    nodes {
      handle
      rating: metafield(namespace: "reviews", key: "rating") { value }
      ratingCount: metafield(namespace: "reviews", key: "rating_count") { value }
    }
  }
}
"""


def fetch_catalog(shopify_graphql: Callable[..., Any]) -> list[dict]:
    """Active products with the rating Shopify has stored for each.

    The ratings live in Shopify's STANDARD `reviews.rating` metafields, not in
    `judgeme.*` -- those hold only widget HTML. Reading the wrong namespace
    makes every product look unrated."""
    catalog: list[dict] = []
    cursor = None
    while True:
        response = shopify_graphql(_CATALOG_QUERY, {"cursor": cursor}) or {}
        products = (response.get("data") or {}).get("products") or {}
        for node in products.get("nodes") or []:
            raw = (node.get("rating") or {}).get("value")
            rating = None
            if raw:
                try:
                    rating = float(json.loads(raw).get("value"))
                except (ValueError, TypeError, AttributeError):
                    rating = None
            try:
                count = int((node.get("ratingCount") or {}).get("value") or 0)
            except (TypeError, ValueError):
                count = 0
            catalog.append({
                "handle": node.get("handle"),
                "expected_rating": rating,
                "expected_review_count": count if rating is not None else None,
            })
        page_info = products.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
    return catalog


def _default_fetch(url: str) -> tuple[int, str]:
    import time

    import requests  # type: ignore

    session = requests.Session()
    for attempt in range(FETCH_RETRIES_ON_RATE_LIMIT):
        response = session.get(url, timeout=30)
        if response.status_code == 429:
            time.sleep(3 * (attempt + 1))
            continue
        return response.status_code, response.text
    return 429, ""


def build_product_schema_canary(
    *,
    catalog: Iterable[dict] | None = None,
    fetch: Callable[[str], tuple[int, str]] | None = None,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    rotation: int | None = None,
    base_url: str = STOREFRONT_BASE_URL,
    sleep: Callable[[float], None] | None = None,
) -> dict:
    """Read a sample of live product pages and summarise what Google would see.

    `catalog` and `fetch` are injected so the default pytest run touches no
    network. Nothing here raises on a single bad page: one unreadable product
    must not lose the signal from the other nineteen."""
    # Source-level outbound guard. Both live paths reach the outside world --
    # `fetch_catalog` hits the Shopify Admin API and `_default_fetch` hits the
    # storefront. Every test injects both, so this is unreachable today; the
    # guard is here so a future test that FORGETS gets a loud refusal instead
    # of quietly consuming our API budget and rate-limiting the real
    # storefront ([[feedback_test_runs_reach_the_outside_world]]).
    if (str(os.environ.get(_TEST_MODE_ENV) or "").strip() in {"1", "true", "TRUE", "yes"}
            and (catalog is None or fetch is None)):
        raise TestModeRefusalError(
            "build_product_schema_canary refused: DUCK_TEST_MODE=1 and "
            f"{'catalog' if catalog is None else 'fetch'} was not injected, so this "
            "call would reach the live Shopify API / storefront. Pass catalog= and "
            "fetch= in tests."
        )
    if catalog is None:
        catalog = fetch_catalog(_ensure_shopify_imports())
    if fetch is None:
        fetch = _default_fetch
    if rotation is None:
        rotation = datetime.now().timetuple().tm_yday
    if sleep is None:
        import time as _time

        sleep = _time.sleep

    sample = select_sample(catalog, sample_size=sample_size, rotation=rotation)
    pages: list[dict] = []
    for index, item in enumerate(sample):
        handle = str(item.get("handle") or "")
        if not handle:
            continue
        if index:
            sleep(FETCH_SPACING_SECONDS)
        try:
            status_code, html = fetch(f"{base_url}/products/{handle}")
        except Exception as exc:  # a dead network is "could not check", not "fine"
            pages.append({"handle": handle, "verdict": "unreadable",
                          "http_status": None, "error": type(exc).__name__})
            continue
        if status_code != 200:
            pages.append({"handle": handle, "verdict": "unreadable",
                          "http_status": status_code,
                          "rate_limited": status_code == 429})
            continue
        page = evaluate_page(
            html,
            handle=handle,
            expected_rating=item.get("expected_rating"),
            expected_count=item.get("expected_review_count"),
        )
        page["http_status"] = status_code
        pages.append(page)

    def count(*verdicts: str) -> int:
        return sum(1 for page in pages if page.get("verdict") in verdicts)

    readable = [page for page in pages if page.get("verdict") != "unreadable"]
    rated_pages = [page for page in readable if page.get("expected_rating") is not None]
    summary = {
        "sample_size": len(sample),
        "pages_checked": len(readable),
        "pages_unreadable": count("unreadable"),
        "pages_rate_limited": sum(1 for page in pages if page.get("rate_limited")),
        "rated_sampled": len(rated_pages),
        "rated_ok": sum(1 for page in rated_pages if page.get("verdict") == "ok"),
        "rated_missing": sum(1 for page in rated_pages if page.get("verdict") == "rating_missing"),
        "rated_mismatch": sum(1 for page in rated_pages if page.get("verdict") == "rating_mismatch"),
        "rated_without_offers": sum(1 for page in rated_pages if page.get("verdict") == "rating_without_offers"),
        "unrated_sampled": sum(1 for page in readable if page.get("expected_rating") is None),
        "unrated_claiming_rating": count("unexpected_rating"),
        "duplicate_emitter_pages": count("duplicate_emitter"),
        "duplicate_rating_node_pages": count("duplicate_rating_nodes"),
        "no_product_node_pages": count("no_product_node"),
        "judgeme_setting_unreadable_pages": sum(
            1 for page in readable if page.get("judgeme_json_ld_disabled") is None
        ),
    }
    return {
        "generated_at": now_iso(),
        "base_url": base_url,
        "rotation": int(rotation),
        "catalog_size": len(list(catalog)) if not isinstance(catalog, list) else len(catalog),
        "summary": summary,
        "pages": pages,
    }


def write_product_schema_canary(payload: dict, path: Any = None) -> Path:
    out = Path(path or PRODUCT_SCHEMA_CANARY_PATH)
    if (str(os.environ.get(_TEST_MODE_ENV) or "").strip() in {"1", "true", "TRUE", "yes"}
            and out.resolve() == _FROZEN_PRODUCTION_PRODUCT_SCHEMA_CANARY_PATH):
        raise TestModeRefusalError(
            "write_product_schema_canary refused: DUCK_TEST_MODE=1 and the write "
            "would land in production state/product_schema_canary.json. Monkeypatch "
            "product_schema_canary.PRODUCT_SCHEMA_CANARY_PATH to a tmp path (see "
            "duck-ops tests/conftest.py for the pattern). Do NOT write production "
            "state from tests."
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(out.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
        os.replace(tmp, out)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return out


def _ensure_duckagent_python() -> None:
    if os.environ.get("PRODUCT_SCHEMA_CANARY_VENV_READY") == "1":
        return
    try:
        current = Path(sys.executable).resolve()
    except Exception:
        current = Path(sys.executable)
    if current == DUCK_AGENT_VENV_PY or not DUCK_AGENT_VENV_PY.exists():
        return
    os.environ["PRODUCT_SCHEMA_CANARY_VENV_READY"] = "1"
    os.execv(str(DUCK_AGENT_VENV_PY),
             [str(DUCK_AGENT_VENV_PY), str(Path(__file__).resolve()), *sys.argv[1:]])


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    _ensure_duckagent_python()
    sample_size = DEFAULT_SAMPLE_SIZE
    if "--sample" in args:
        try:
            sample_size = int(args[args.index("--sample") + 1])
        except (IndexError, ValueError):
            print("--sample needs an integer", file=sys.stderr)
            return 2
    payload = build_product_schema_canary(sample_size=sample_size)
    write_product_schema_canary(payload)
    print(json.dumps(payload["summary"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

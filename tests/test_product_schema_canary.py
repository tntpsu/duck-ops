"""Surface 95 -- the product-schema canary.

The defect this guards against is not in our code at all: it is two
third-party app settings that no repo file records and no API can read. The
tests below therefore fix the SHAPES of the page HTML (taken from the real
storefront in both its broken and fixed states) and assert what the producer
concludes from them.

No test here touches the network. `fetch` and `catalog` are injected.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

RUNTIME = Path(__file__).resolve().parents[1] / "runtime"
if str(RUNTIME) not in sys.path:
    sys.path.insert(0, str(RUNTIME))

import product_schema_canary as psc  # noqa: E402


def jdgm(disable_json_ld: bool | None) -> str:
    """The Judge.me settings blob as the storefront embeds it. One key out of
    640 differs between the broken and fixed states; this is that key."""
    if disable_json_ld is None:
        return "<script>var other = 1;</script>"
    body = json.dumps({"pagination": 5, "disable_json_ld": disable_json_ld,
                       "remove_microdata_snippet": True})
    return f"<script>var jdgmSettings={body};</script>"


def ld(doc: object) -> str:
    return f'<script type="application/ld+json">{json.dumps(doc)}</script>'


def fixed_page(rating: str = "5.00", count: str = "12", *, offers: bool = True,
               jm: bool | None = True) -> str:
    """The healthy shape: Smart SEO's @graph, one Product carrying both the
    rating and the offer, Judge.me suppressed."""
    product: dict = {
        "@type": "Product", "name": "Bigfoot Duck", "sku": "bf1",
        "brand": {"@type": "Brand", "name": "myJeepDuck"},
        "aggregateRating": {"@type": "AggregateRating",
                            "description": "Judge.me Reviews",
                            "ratingValue": rating, "reviewCount": count},
    }
    if offers:
        product["offers"] = [{"@type": "Offer", "price": "8.99", "priceCurrency": "USD"}]
    graph = {"@context": "https://schema.org", "@graph": [
        {"@type": "MerchantReturnPolicy", "merchantReturnDays": 30},
        {"@type": "OfferShippingDetails"},
        product,
    ]}
    return "<html>" + ld(graph) + ld({"@type": "BreadcrumbList", "itemListElement": []}) + jdgm(jm) + "</html>"


def broken_page() -> str:
    """The state the storefront was actually in before 2026-10-15: Smart SEO's
    complete Product with NO rating, plus Judge.me's rating stub with an @id
    meant to merge into a node that carries none, so neither qualifies."""
    smart_seo = {"@context": "https://schema.org", "@graph": [{
        "@type": "Product", "name": "Pride Rainbow Duck", "sku": "erpride",
        "offers": [{"@type": "Offer", "price": "8.99"}],
    }]}
    judgeme_stub = {"@context": "https://schema.org", "@type": "Product",
                    "@id": "https://www.myjeepduck.com/products/pride-rainbow-duck#product",
                    "name": "Pride Rainbow Duck",
                    "aggregateRating": {"@type": "AggregateRating",
                                        "ratingValue": "5.00", "reviewCount": 2}}
    return "<html>" + ld(smart_seo) + ld(judgeme_stub) + jdgm(False) + "</html>"


def variant_group_page() -> str:
    """A multi-variant product. Google's spec puts the rating on the
    ProductGroup and the variants under hasVariant; a naive node counter calls
    this a duplicate."""
    group = {"@context": "https://schema.org", "@graph": [{
        "@type": "ProductGroup", "name": "Build Your Own Duck",
        "offers": [{"@type": "Offer", "price": "8.99"}],
        "aggregateRating": {"@type": "AggregateRating", "ratingValue": "5.00", "reviewCount": "29"},
        "hasVariant": [
            {"@type": "Product", "name": "variant A", "offers": [{"@type": "Offer", "price": "8.99"}]},
            {"@type": "Product", "name": "variant B", "offers": [{"@type": "Offer", "price": "9.99"}]},
        ],
    }]}
    return "<html>" + ld(group) + jdgm(True) + "</html>"


def run(pages: dict[str, str], catalog: list[dict], **kwargs) -> dict:
    """Build a payload against canned pages. Unlisted handles 404."""
    def fetch(url: str) -> tuple[int, str]:
        handle = url.rsplit("/", 1)[-1]
        if handle in pages:
            return 200, pages[handle]
        return 404, ""
    kwargs.setdefault("sample_size", 10)
    kwargs.setdefault("rotation", 0)
    return psc.build_product_schema_canary(
        catalog=catalog, fetch=fetch, sleep=lambda _s: None, **kwargs)


RATED = {"handle": "bigfoot-duck", "expected_rating": 5.0, "expected_review_count": 12}
UNRATED = {"handle": "plain-duck", "expected_rating": None, "expected_review_count": None}


class TestJudgeMeDuplicateEmitterIsCaught:
    """The headline regression. Judge.me's 'Add JSON-LD snippets' ships
    CHECKED, so an app update re-enabling it silently restores the two-node
    state that showed stars for nothing."""

    def test_the_pre_fix_page_shape_is_flagged(self):
        page = psc.evaluate_page(broken_page(), handle="pride-rainbow-duck",
                                 expected_rating=5.0, expected_count=2)
        assert page["verdict"] == "duplicate_emitter"

    def test_it_is_flagged_even_though_a_rating_is_present(self):
        # This is the trap: the page DOES carry a 5.00 rating. A check that
        # only asked "is there a rating?" would call the broken state healthy.
        page = psc.evaluate_page(broken_page(), handle="x",
                                 expected_rating=5.0, expected_count=2)
        assert page["rating_nodes"] == 1
        assert page["verdict"] != "ok"

    def test_the_setting_is_read_from_the_page_not_guessed(self):
        assert psc.judgeme_json_ld_disabled(jdgm(True)) is True
        assert psc.judgeme_json_ld_disabled(jdgm(False)) is False

    def test_a_missing_setting_is_unknown_not_healthy(self):
        # "I could not find the flag" must never collapse into "it is off".
        assert psc.judgeme_json_ld_disabled(jdgm(None)) is None

    def test_the_summary_counts_duplicate_emitter_pages(self):
        payload = run({"bigfoot-duck": broken_page()}, [RATED])
        assert payload["summary"]["duplicate_emitter_pages"] == 1
        assert payload["summary"]["rated_ok"] == 0


class TestTheRatingMustMatchWhatShopifyStores:
    def test_a_matching_rating_passes(self):
        page = psc.evaluate_page(fixed_page("5.00", "12"), handle="h",
                                 expected_rating=5.0, expected_count=12)
        assert page["verdict"] == "ok"

    def test_a_wrong_value_is_a_mismatch(self):
        page = psc.evaluate_page(fixed_page("4.00", "12"), handle="h",
                                 expected_rating=5.0, expected_count=12)
        assert page["verdict"] == "rating_mismatch"

    def test_a_wrong_review_count_is_a_mismatch(self):
        # A drifting count means the Judge.me -> metafield sync broke even
        # though the stars still render.
        page = psc.evaluate_page(fixed_page("5.00", "3"), handle="h",
                                 expected_rating=5.0, expected_count=12)
        assert page["verdict"] == "rating_mismatch"

    def test_a_missing_rating_is_its_own_verdict(self):
        page = psc.evaluate_page(fixed_page(jm=True).replace('"aggregateRating"', '"notRating"'),
                                 handle="h", expected_rating=5.0, expected_count=12)
        assert page["verdict"] == "rating_missing"

    def test_missing_and_mismatched_are_counted_separately(self):
        payload = run({"a-duck": fixed_page("4.00", "12"),
                       "b-duck": fixed_page().replace('"aggregateRating"', '"notRating"')},
                      [{"handle": "a-duck", "expected_rating": 5.0, "expected_review_count": 12},
                       {"handle": "b-duck", "expected_rating": 5.0, "expected_review_count": 12}])
        assert payload["summary"]["rated_mismatch"] == 1
        assert payload["summary"]["rated_missing"] == 1


class TestARatingWithoutAPriceIsUseless:
    """The old Judge.me stub had a rating and no offer and earned nothing.
    A rating on an offer-less node is a defect, not a pass."""

    def test_a_rating_node_without_offers_is_flagged(self):
        page = psc.evaluate_page(fixed_page(offers=False), handle="h",
                                 expected_rating=5.0, expected_count=12)
        assert page["verdict"] == "rating_without_offers"
        assert page["offers_on_rating_node"] is False

    def test_the_healthy_page_puts_both_on_one_node(self):
        page = psc.evaluate_page(fixed_page(), handle="h",
                                 expected_rating=5.0, expected_count=12)
        assert page["offers_on_rating_node"] is True
        assert page["rating_nodes"] == 1


class TestAnUnratedProductMustNotClaimStars:
    """Stars on a product with no reviews is a fabricated-review problem, which
    is worse than having no stars at all."""

    def test_an_unrated_product_with_no_rating_passes(self):
        page = psc.evaluate_page(fixed_page().replace('"aggregateRating"', '"notRating"'),
                                 handle="h", expected_rating=None, expected_count=None)
        assert page["verdict"] == "ok_unrated"

    def test_an_unrated_product_showing_a_rating_is_flagged(self):
        page = psc.evaluate_page(fixed_page(), handle="h",
                                 expected_rating=None, expected_count=None)
        assert page["verdict"] == "unexpected_rating"

    def test_the_summary_counts_it(self):
        payload = run({"plain-duck": fixed_page()}, [UNRATED])
        assert payload["summary"]["unrated_claiming_rating"] == 1


class TestAVariantGroupIsNotADuplicate:
    def test_a_product_group_with_variants_passes(self):
        page = psc.evaluate_page(variant_group_page(), handle="build-your-own-duck",
                                 expected_rating=5.0, expected_count=29)
        assert page["verdict"] == "ok"

    def test_all_three_nodes_are_seen_but_only_one_carries_the_rating(self):
        page = psc.evaluate_page(variant_group_page(), handle="h",
                                 expected_rating=5.0, expected_count=29)
        assert page["product_nodes"] == 3
        assert page["rating_nodes"] == 1

    def test_a_group_may_carry_the_rating_while_its_variants_carry_the_price(self):
        """Google's variant spec does NOT require offers on the ProductGroup.
        Demanding it there flagged football-ducks (30 priced variants) and
        dachshund-duck (2) as broken on the canary's first live run."""
        doc = {"@context": "https://schema.org", "@graph": [{
            "@type": "ProductGroup", "name": "Football Ducks",
            "aggregateRating": {"ratingValue": "5.00", "reviewCount": "11"},
            "hasVariant": [
                {"@type": "Product", "name": "a", "offers": [{"@type": "Offer", "price": "8.99"}]},
                {"@type": "Product", "name": "b", "offers": [{"@type": "Offer", "price": "8.99"}]},
            ]}]}
        page = psc.evaluate_page("<html>" + ld(doc) + jdgm(True) + "</html>",
                                 handle="football-ducks", expected_rating=5.0, expected_count=11)
        assert page["verdict"] == "ok"
        assert page["offers_on_rating_node"] is True

    def test_a_group_with_no_price_anywhere_is_still_flagged(self):
        doc = {"@context": "https://schema.org", "@graph": [{
            "@type": "ProductGroup", "name": "Priceless Duck",
            "aggregateRating": {"ratingValue": "5.00", "reviewCount": "11"},
            "hasVariant": [{"@type": "Product", "name": "a"},
                           {"@type": "Product", "name": "b"}]}]}
        page = psc.evaluate_page("<html>" + ld(doc) + jdgm(True) + "</html>",
                                 handle="h", expected_rating=5.0, expected_count=11)
        assert page["verdict"] == "rating_without_offers"

    def test_two_rating_bearing_nodes_are_a_duplicate(self):
        doc = {"@context": "https://schema.org", "@graph": [
            {"@type": "Product", "name": "a", "offers": [{"@type": "Offer"}],
             "aggregateRating": {"ratingValue": "5.0", "reviewCount": "2"}},
            {"@type": "Product", "name": "a",
             "aggregateRating": {"ratingValue": "5.0", "reviewCount": "2"}}]}
        page = psc.evaluate_page("<html>" + ld(doc) + jdgm(True) + "</html>",
                                 handle="h", expected_rating=5.0, expected_count=2)
        assert page["verdict"] == "duplicate_rating_nodes"


class TestBlindIsNotHealthy:
    """A producer that cannot read the storefront finds zero problems. Zero
    problems rendered as green is the fake reassurance that let the original
    defect live for months."""

    def test_an_unreadable_page_is_not_counted_as_checked(self):
        payload = run({}, [RATED])          # every fetch 404s
        assert payload["summary"]["pages_checked"] == 0
        assert payload["summary"]["pages_unreadable"] == 1
        assert payload["summary"]["rated_ok"] == 0

    def test_zero_defects_and_zero_checks_are_distinguishable(self):
        blind = run({}, [RATED])["summary"]
        healthy = run({"bigfoot-duck": fixed_page()}, [RATED])["summary"]
        assert blind["rated_missing"] == healthy["rated_missing"] == 0
        # The ONLY thing separating them is the checked count.
        assert blind["pages_checked"] == 0 and healthy["pages_checked"] == 1

    def test_rate_limiting_is_recorded_distinctly(self):
        def fetch(_url):
            return 429, ""
        payload = psc.build_product_schema_canary(
            catalog=[RATED], fetch=fetch, sleep=lambda _s: None,
            sample_size=5, rotation=0)
        assert payload["summary"]["pages_rate_limited"] == 1
        assert payload["summary"]["pages_unreadable"] == 1

    def test_a_raised_exception_does_not_lose_the_other_pages(self):
        def fetch(url):
            if url.endswith("boom-duck"):
                raise TimeoutError("network down")
            return 200, fixed_page()
        payload = psc.build_product_schema_canary(
            catalog=[{"handle": "boom-duck", "expected_rating": 5.0, "expected_review_count": 12},
                     RATED],
            fetch=fetch, sleep=lambda _s: None, sample_size=5, rotation=0)
        assert payload["summary"]["pages_unreadable"] == 1
        assert payload["summary"]["rated_ok"] == 1

    def test_a_page_with_no_product_node_is_not_a_pass(self):
        page = psc.evaluate_page("<html>" + jdgm(True) + "</html>", handle="h",
                                 expected_rating=5.0, expected_count=12)
        assert page["verdict"] == "no_product_node"


class TestDegenerateHtmlDoesNotCrash:
    def test_empty_html(self):
        page = psc.evaluate_page("", handle="h", expected_rating=5.0, expected_count=1)
        assert page["verdict"] in {"no_product_node", "unreadable"}

    def test_unparseable_ld_json_block_is_skipped_not_fatal(self):
        html = ('<html><script type="application/ld+json">{ not json </script>'
                + ld({"@type": "Product", "name": "x", "offers": [{}],
                      "aggregateRating": {"ratingValue": "5.0", "reviewCount": "1"}})
                + jdgm(True) + "</html>")
        page = psc.evaluate_page(html, handle="h", expected_rating=5.0, expected_count=1)
        assert page["verdict"] == "ok"

    def test_a_brace_inside_a_settings_string_does_not_truncate_the_parse(self):
        html = ('<script>var jdgmSettings={"label":"pick a { size }",'
                '"disable_json_ld":true};</script>')
        assert psc.judgeme_json_ld_disabled(html) is True

    def test_a_non_boolean_setting_is_unknown(self):
        html = '<script>var jdgmSettings={"disable_json_ld":"yes"};</script>'
        assert psc.judgeme_json_ld_disabled(html) is None


class TestTheSampleIsDeterministicAndRotates:
    @staticmethod
    def catalog(n_rated: int = 30, n_unrated: int = 10) -> list[dict]:
        rated = [{"handle": f"r{i:02d}", "expected_rating": 5.0,
                  "expected_review_count": 100 - i} for i in range(n_rated)]
        unrated = [{"handle": f"u{i:02d}", "expected_rating": None,
                    "expected_review_count": None} for i in range(n_unrated)]
        return rated + unrated

    def test_the_same_rotation_picks_the_same_products(self):
        a = psc.select_sample(self.catalog(), sample_size=20, rotation=7)
        b = psc.select_sample(self.catalog(), sample_size=20, rotation=7)
        assert [i["handle"] for i in a] == [i["handle"] for i in b]

    def test_a_different_rotation_moves_the_window(self):
        a = psc.select_sample(self.catalog(), sample_size=20, rotation=0)
        b = psc.select_sample(self.catalog(), sample_size=20, rotation=1)
        assert [i["handle"] for i in a] != [i["handle"] for i in b]

    def test_the_highest_review_counts_are_checked_every_run(self):
        # Anchors carry the most search value, so they must not rotate out.
        for rotation in (0, 1, 5, 99):
            handles = {i["handle"] for i in
                       psc.select_sample(self.catalog(), sample_size=20, rotation=rotation)}
            assert "r00" in handles, f"top-reviewed product dropped at rotation {rotation}"

    def test_unrated_controls_are_always_included(self):
        chosen = psc.select_sample(self.catalog(), sample_size=20, rotation=3)
        assert sum(1 for i in chosen if i["expected_rating"] is None) == psc.UNRATED_CONTROL_COUNT

    def test_the_sample_never_exceeds_its_budget(self):
        chosen = psc.select_sample(self.catalog(), sample_size=8, rotation=2)
        assert len(chosen) == 8

    def test_an_empty_catalog_yields_nothing_rather_than_raising(self):
        assert psc.select_sample([], sample_size=20, rotation=0) == []

    def test_a_catalog_smaller_than_the_sample_is_fine(self):
        chosen = psc.select_sample(self.catalog(2, 1), sample_size=20, rotation=0)
        assert len(chosen) == 3

    def test_rotation_eventually_covers_products_outside_the_anchors(self):
        seen: set[str] = set()
        for rotation in range(12):
            seen |= {i["handle"] for i in
                     psc.select_sample(self.catalog(), sample_size=20, rotation=rotation)}
        assert len([h for h in seen if h.startswith("r")]) > psc.ANCHOR_COUNT


class TestWritesRefuseProductionUnderTestMode:
    def test_a_tmp_path_write_round_trips(self, tmp_path, monkeypatch):
        monkeypatch.setattr(psc, "PRODUCT_SCHEMA_CANARY_PATH", tmp_path / "canary.json")
        out = psc.write_product_schema_canary({"generated_at": "now", "summary": {}})
        assert json.loads(out.read_text())["generated_at"] == "now"

    def test_the_frozen_production_path_is_refused_under_test_mode(self, monkeypatch):
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        with pytest.raises(psc.TestModeRefusalError):
            psc.write_product_schema_canary(
                {"summary": {}}, psc._FROZEN_PRODUCTION_PRODUCT_SCHEMA_CANARY_PATH)

    def test_the_guard_compares_the_frozen_path_not_the_live_constant(self, tmp_path, monkeypatch):
        # Patching the live constant must NOT disarm the guard for the real path.
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        monkeypatch.setattr(psc, "PRODUCT_SCHEMA_CANARY_PATH", tmp_path / "x.json")
        with pytest.raises(psc.TestModeRefusalError):
            psc.write_product_schema_canary(
                {"summary": {}}, psc._FROZEN_PRODUCTION_PRODUCT_SCHEMA_CANARY_PATH)

    def test_without_test_mode_the_guard_does_not_fire(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DUCK_TEST_MODE", raising=False)
        out = psc.write_product_schema_canary({"summary": {}}, tmp_path / "ok.json")
        assert out.exists()


class TestTheSuiteCannotReachTheOutsideWorld:
    """pytest once sent four real emails to the operator. The convention that
    every test injects `fetch` is not a guard; this is."""

    def test_a_missing_fetch_under_test_mode_is_refused(self, monkeypatch):
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        with pytest.raises(psc.TestModeRefusalError):
            psc.build_product_schema_canary(catalog=[RATED], sample_size=1)

    def test_a_missing_catalog_under_test_mode_is_refused(self, monkeypatch):
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        with pytest.raises(psc.TestModeRefusalError):
            psc.build_product_schema_canary(fetch=lambda _u: (200, ""), sample_size=1)

    def test_both_injected_is_allowed_under_test_mode(self, monkeypatch):
        monkeypatch.setenv("DUCK_TEST_MODE", "1")
        payload = psc.build_product_schema_canary(
            catalog=[RATED], fetch=lambda _u: (200, fixed_page()),
            sleep=lambda _s: None, sample_size=1, rotation=0)
        assert payload["summary"]["rated_ok"] == 1


class TestTheCatalogReadsTheRightMetafields:
    def test_the_standard_reviews_namespace_is_used(self):
        # judgeme.* holds only widget HTML; reading it makes every product
        # look unrated.
        assert '"reviews", key: "rating"' in psc._CATALOG_QUERY
        assert '"reviews", key: "rating_count"' in psc._CATALOG_QUERY
        assert "judgeme" not in psc._CATALOG_QUERY

    def test_a_rating_metafield_is_unwrapped_from_its_json(self):
        calls = {"n": 0}

        def fake_graphql(_query, _vars=None):
            calls["n"] += 1
            return {"data": {"products": {
                "pageInfo": {"hasNextPage": False},
                "nodes": [{"handle": "a",
                           "rating": {"value": json.dumps({"scale_min": "1.0", "scale_max": "5.0", "value": "4.5"})},
                           "ratingCount": {"value": "7"}},
                          {"handle": "b", "rating": None, "ratingCount": None}]}}}

        catalog = psc.fetch_catalog(fake_graphql)
        assert catalog[0] == {"handle": "a", "expected_rating": 4.5, "expected_review_count": 7}
        assert catalog[1] == {"handle": "b", "expected_rating": None, "expected_review_count": None}
        assert calls["n"] == 1

    def test_pagination_is_followed(self):
        pages = [
            {"data": {"products": {"pageInfo": {"hasNextPage": True, "endCursor": "c1"},
                                   "nodes": [{"handle": "a", "rating": None, "ratingCount": None}]}}},
            {"data": {"products": {"pageInfo": {"hasNextPage": False},
                                   "nodes": [{"handle": "b", "rating": None, "ratingCount": None}]}}},
        ]
        it = iter(pages)

        def fake_graphql(_q, _v=None):
            return next(it)

        assert [c["handle"] for c in psc.fetch_catalog(fake_graphql)] == ["a", "b"]

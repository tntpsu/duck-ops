"""Contract tests for the duck_flows registry.

If a flow is registered, its FlowSpec must:
  - declare a name matching the registry key
  - publish_succeeded() against a realistic state payload returns True
  - publish_succeeded() against an empty payload returns False

If a flow is unregistered, require_flow() must raise loudly with a
pointer to the registry. This is the load-bearing guarantee that
keeps "I forgot to add the new flow to one of the six places" from
being a silent operator-facing bug.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import duck_flows


# Realistic-shaped payloads keyed by flow name. Each is the minimum
# state-file content that should count as "this artifact published"
# for the flow. If any of these become outdated as DuckAgent's state
# schema evolves, the test fails and we know the registry needs an
# update.
_SUCCESS_PAYLOADS: dict[str, dict] = {
    "meme": {
        "meme_publish_status": "success",
        "meme_scheduled_at": "2026-05-25T18:00:00-04:00",
        "meme_fb_id": "122239048256757684",
    },
    "jeepfact": {
        "jeepfact_publish_status": "scheduled",
        "jeepfact_scheduled_at": "2026-05-28T09:00:00-04:00",
    },
    "thursday": {
        "thursday_published": True,
        "thursday_publish_time": "2026-05-21T20:00:00-04:00",
    },
    "gtdf": {
        "gtdf_scheduled_at": "2026-05-25T20:00:00-04:00",
    },
    "reviews_story": {
        "reviews_story_publish_status": "published",
        "reviews_story_published": True,
        "reviews_story_published_at": "2026-05-21T14:00:00-04:00",
    },
    "newduck": {
        "newduck_published": True,
        "shopify_product_id": "8086420000000",
        "newduck_published_at": "2026-05-25T15:00:00-04:00",
    },
    "weekly_sale": {
        "weekly_sale_published": True,
        "weekly_sale_published_at": "2026-05-19T10:00:00-04:00",
    },
}


class FlowSpecContractTests(unittest.TestCase):
    def test_every_registered_flow_has_matching_name(self) -> None:
        """Catch the "named entry under the wrong key" footgun — e.g.
        FLOWS["meme"] = FlowSpec(name="meeme", ...). Consumers look up
        by key but pass spec.name back through; a mismatch would loop."""
        for key, spec in duck_flows.FLOWS.items():
            self.assertEqual(spec.name, key, f"FLOWS[{key!r}].name should be {key!r}, got {spec.name!r}")

    def test_publish_succeeded_is_true_on_realistic_state(self) -> None:
        """For every flow that writes a publish state file, the
        realistic success payload must be recognized as published.
        This is the load-bearing assertion that keeps queue-self-
        correction working."""
        for name, payload in _SUCCESS_PAYLOADS.items():
            spec = duck_flows.FLOWS[name]
            self.assertTrue(
                spec.publish_succeeded(payload),
                f"{name}: success payload {payload!r} was not recognized "
                f"as published; FlowSpec probes are out of sync with "
                f"DuckAgent's state-file writes.",
            )

    def test_publish_succeeded_is_false_on_empty_state(self) -> None:
        """An empty state file means the publish has not happened
        yet. Misclassifying this drops cards the operator hasn't
        decided on — a much worse failure mode than lingering cards."""
        for name, spec in duck_flows.FLOWS.items():
            self.assertFalse(
                spec.publish_succeeded({}),
                f"{name}: empty payload was misclassified as published",
            )
            self.assertFalse(
                spec.publish_succeeded(None),  # type: ignore[arg-type]
                f"{name}: None payload was misclassified as published",
            )

    def test_has_publish_state_partitions_correctly(self) -> None:
        """Flows that mutate external services directly without a
        per-run state file (shopify_seo, shopify_draft_activation,
        design_brief_queue) must report has_publish_state == False so
        the queue self-correction path skips them rather than trying
        to read a nonexistent state file."""
        non_publish_flows = {"shopify_seo", "shopify_draft_activation", "design_brief_queue"}
        for name, spec in duck_flows.FLOWS.items():
            if name in non_publish_flows:
                self.assertFalse(
                    spec.has_publish_state(),
                    f"{name} declares a state file but shouldn't",
                )
            else:
                self.assertTrue(
                    spec.has_publish_state(),
                    f"{name} should declare a state file and probe keys",
                )


class RequireFlowTests(unittest.TestCase):
    def test_known_flow_returns_spec(self) -> None:
        spec = duck_flows.require_flow("meme")
        self.assertEqual(spec.name, "meme")

    def test_unknown_flow_raises_loudly(self) -> None:
        with self.assertRaises(duck_flows.UnknownFlowError) as ctx:
            duck_flows.require_flow("not_a_real_flow")
        msg = str(ctx.exception)
        self.assertIn("not_a_real_flow", msg)
        self.assertIn("duck_flows.py", msg, "Error should point at the registry file")

    def test_alias_resolves_to_canonical(self) -> None:
        """Artifact-type aliases (listing → newduck, etc.) must
        resolve through require_flow so consumers don't have to know
        about the alias table."""
        self.assertEqual(duck_flows.require_flow("listing").name, "newduck")
        self.assertEqual(duck_flows.require_flow("promotion").name, "weekly_sale")
        self.assertEqual(duck_flows.require_flow("social_post").name, "meme")
        self.assertEqual(duck_flows.require_flow("reviews").name, "reviews_story")

    def test_get_flow_returns_none_on_unknown(self) -> None:
        """get_flow is the soft sibling of require_flow — used when
        the caller has reason to expect arbitrary input. Returns None
        instead of raising."""
        self.assertIsNone(duck_flows.get_flow("not_a_real_flow"))
        self.assertIsNone(duck_flows.get_flow(""))
        self.assertIsNone(duck_flows.get_flow(None))

    def test_case_and_whitespace_normalized(self) -> None:
        self.assertEqual(duck_flows.require_flow("  MEME ").name, "meme")
        self.assertEqual(duck_flows.require_flow("Newduck").name, "newduck")


class ReplyActionsTests(unittest.TestCase):
    def test_flows_with_buttons_declare_them(self) -> None:
        """The flows the operator interacts with via email buttons
        must each declare at least one ReplyAction. Mirrors what
        helpers/email_reply_action_helper.py::default_reply_actions
        produced before the migration."""
        expected_with_buttons = {
            "design_brief_queue", "jeepfact", "meme",
            "shopify_seo", "shopify_draft_activation",
        }
        for name in expected_with_buttons:
            spec = duck_flows.FLOWS[name]
            self.assertGreater(
                len(spec.reply_actions), 0,
                f"{name}: expected at least one reply_action; the email "
                f"templates render buttons from this list.",
            )

    def test_meme_publish_button_matches_template(self) -> None:
        """Spot check: the canonical 'Reply Meme Publish' button must
        carry the exact reply text handle_mail_event parses to identify
        a meme publish intent."""
        spec = duck_flows.FLOWS["meme"]
        publish = next((a for a in spec.reply_actions if a.action == "publish"), None)
        self.assertIsNotNone(publish)
        assert publish is not None  # mypy
        self.assertEqual(publish.text, "meme publish")


if __name__ == "__main__":
    unittest.main()

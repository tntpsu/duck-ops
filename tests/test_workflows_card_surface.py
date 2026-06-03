"""Workflows-card surface tests.

The Workflows card is the operator's single view of all 7 flows
(weekly_sale, meme, review_carousel, jeepfact + thursday, gtdf, blog)
with their auto-vs-gated-vs-manual state and on/off mode. Both the
desk markdown renderer and the portal SPA card read the same dict
emitted by build_workflows_card_surface — these tests pin the dict's
shape so the two renderers can never drift apart.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

from business_operator_desk import (  # noqa: E402
    BLOG_LANE_CONFIG,
    GTDF_LANE_CONFIG,
    JEEPFACT_LANE_CONFIG,
    LANE_POLICY_CONFIGS,
    MEME_LANE_CONFIG,
    REVIEW_CAROUSEL_LANE_CONFIG,
    THURSDAY_LANE_CONFIG,
    WEEKLY_SALE_LANE_CONFIG,
    build_workflows_card_surface,
)


class WorkflowsCardSurfaceTests(unittest.TestCase):
    def test_card_lists_all_seven_flows(self) -> None:
        with (
            patch("business_operator_desk.list_workflow_states", return_value=[]),
            patch(
                "business_operator_desk.load_json",
                return_value={"mode": "approval_gated"},
            ),
        ):
            surface = build_workflows_card_surface()

        flows = surface.get("flows") or []
        self.assertEqual(len(flows), 7, "Card must show all 7 registered flows.")
        flow_lanes = [entry["flow"] for entry in flows]
        self.assertEqual(
            flow_lanes,
            ["weekly", "meme", "review_carousel", "jeepfact", "thursday", "gtdf", "blog"],
            "Flow order matters for the operator's mental model (Sunday→"
            "Wednesday lane flows, then manual flows).",
        )

    def test_every_flow_row_carries_card_contract_fields(self) -> None:
        with (
            patch("business_operator_desk.list_workflow_states", return_value=[]),
            patch(
                "business_operator_desk.load_json",
                return_value={"mode": "approval_gated"},
            ),
        ):
            surface = build_workflows_card_surface()

        required_keys = {
            "flow", "display_name", "mode", "off", "progression_kind",
            "status_dot", "status_label", "clean_gated_streak",
            "promotion_threshold", "last_run_at", "last_run_state",
            "config_path", "mutation_endpoint", "off_switch_tier",
            "no_auto_progression",
        }
        for entry in surface["flows"]:
            missing = required_keys - set(entry.keys())
            self.assertFalse(
                missing,
                f"Workflows-card row for {entry.get('flow')!r} missing "
                f"contract keys: {sorted(missing)}. The portal SPA + "
                "markdown renderer read these keys; missing keys render "
                "as blank.",
            )

    def test_gated_lane_with_clean_streak_shows_progress_toward_promotion(self) -> None:
        """An approval_gated lane with a clean streak short of the
        threshold must show 'N/M clean gated runs — need M to promote
        to auto', not 'AUTO'."""
        with (
            patch("business_operator_desk.list_workflow_states", return_value=[]),
            patch(
                "business_operator_desk.load_json",
                return_value={"mode": "approval_gated"},
            ),
        ):
            surface = build_workflows_card_surface(configs=(MEME_LANE_CONFIG,))

        row = surface["flows"][0]
        self.assertEqual(row["progression_kind"], "gated")
        self.assertEqual(row["status_dot"], "yellow")
        self.assertIn("clean gated runs", row["status_label"])
        self.assertIn("need", row["status_label"])
        self.assertIn("promote to auto", row["status_label"])

    def test_auto_lane_shows_auto_label_and_green_dot(self) -> None:
        with (
            patch("business_operator_desk.list_workflow_states", return_value=[]),
            patch(
                "business_operator_desk.load_json",
                return_value={"mode": WEEKLY_SALE_LANE_CONFIG.auto_mode_label},
            ),
        ):
            surface = build_workflows_card_surface(configs=(WEEKLY_SALE_LANE_CONFIG,))

        row = surface["flows"][0]
        self.assertEqual(row["progression_kind"], "auto")
        self.assertEqual(row["status_dot"], "green")
        # status_label deliberately doesn't repeat the mode (shown as
        # a chip elsewhere). It just signals the lane is running auto.
        self.assertIn("auto", row["status_label"].lower())

    def test_manual_flow_shows_no_streak_progress(self) -> None:
        """thursday/gtdf/blog must NOT show 'X/Y clean gated runs' —
        they have no streak concept."""
        with (
            patch("business_operator_desk.list_workflow_states", return_value=[]),
            patch(
                "business_operator_desk.load_json",
                return_value={"mode": "manual"},
            ),
        ):
            surface = build_workflows_card_surface(
                configs=(THURSDAY_LANE_CONFIG, GTDF_LANE_CONFIG, BLOG_LANE_CONFIG),
            )

        for row in surface["flows"]:
            self.assertEqual(row["progression_kind"], "manual", row["flow"])
            self.assertEqual(row["status_dot"], "gray", row["flow"])
            self.assertIn("Manual flow", row["status_label"], row["flow"])
            self.assertNotIn("clean gated", row["status_label"], row["flow"])
            self.assertTrue(row["no_auto_progression"], row["flow"])

    def test_off_mode_shows_red_dot_and_off_label(self) -> None:
        with (
            patch("business_operator_desk.list_workflow_states", return_value=[]),
            patch(
                "business_operator_desk.load_json",
                return_value={"mode": "off"},
            ),
        ):
            surface = build_workflows_card_surface(configs=(MEME_LANE_CONFIG,))

        row = surface["flows"][0]
        self.assertTrue(row["off"])
        self.assertEqual(row["status_dot"], "red")
        self.assertIn("OFF", row["status_label"])

    def test_counts_match_flow_states(self) -> None:
        # Mix: one off, one auto, one gated, one manual.
        def _load_json_side_effect(path, default=None):
            path_str = str(path)
            if "weekly_sale" in path_str:
                return {"mode": "off"}
            if "meme" in path_str:
                return {"mode": "auto_schedule_meta"}
            if "jeepfact" in path_str:
                return {"mode": "approval_gated"}
            if "thursday" in path_str:
                return {"mode": "manual"}
            return {"mode": "approval_gated"}

        with (
            patch("business_operator_desk.list_workflow_states", return_value=[]),
            patch(
                "business_operator_desk.load_json",
                side_effect=_load_json_side_effect,
            ),
        ):
            surface = build_workflows_card_surface(
                configs=(
                    WEEKLY_SALE_LANE_CONFIG,
                    MEME_LANE_CONFIG,
                    JEEPFACT_LANE_CONFIG,
                    THURSDAY_LANE_CONFIG,
                )
            )

        counts = surface["counts"]
        self.assertEqual(counts["total"], 4)
        self.assertEqual(counts["off"], 1)
        self.assertEqual(counts["auto"], 1)

    def test_mutation_endpoint_url_pattern_is_per_flow(self) -> None:
        with (
            patch("business_operator_desk.list_workflow_states", return_value=[]),
            patch(
                "business_operator_desk.load_json",
                return_value={"mode": "approval_gated"},
            ),
        ):
            surface = build_workflows_card_surface()

        for row in surface["flows"]:
            expected = f"/api/workflows/{row['flow']}/mode"
            self.assertEqual(
                row["mutation_endpoint"], expected,
                f"Portal SPA POSTs to mutation_endpoint to flip mode. "
                f"Row for {row['flow']!r} must have endpoint {expected!r}.",
            )

    def test_off_switch_tier_is_advertised_per_row(self) -> None:
        """Operator sees the tier label inline so they know what they're
        about to do. Drift here means the SPA could quietly demote."""
        with (
            patch("business_operator_desk.list_workflow_states", return_value=[]),
            patch(
                "business_operator_desk.load_json",
                return_value={"mode": "approval_gated"},
            ),
        ):
            surface = build_workflows_card_surface()

        for row in surface["flows"]:
            self.assertIn("Tier 3", row["off_switch_tier"], row["flow"])


if __name__ == "__main__":
    unittest.main()

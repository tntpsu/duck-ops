from __future__ import annotations

import json
import sys
import unittest
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import profit_intel
import operator_interface_contracts as contracts


def _write_receipt(state_dir: Path, run_date: str, *, state: str, state_reason: str, metadata: dict, updated_at: str | None = None, last_verification: dict | None = None, last_side_effect: dict | None = None) -> None:
    wc_dir = state_dir / "workflow_control"
    wc_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "workflow_id": f"profit::{run_date}",
        "lane": "profit",
        "display_label": f"Profit {run_date}",
        "entity_id": run_date,
        "run_id": run_date,
        "state": state,
        "state_reason": state_reason,
        "requires_confirmation": False,
        "last_side_effect": last_side_effect,
        "last_verification": last_verification,
        "next_action": None,
        "updated_at": updated_at or f"{run_date}T23:58:23.000000-04:00",
        "metadata": metadata,
        "history": [],
        "receipts_count": 1,
    }
    (wc_dir / f"profit-{run_date}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _normal_metadata(orders: int = 14, revenue: float = 214.37, net: float = 114.09, margin: float = 53.2) -> dict:
    return {
        "total_orders": orders,
        "total_revenue": revenue,
        "total_net_profit": net,
        "overall_margin": margin,
        "etsy_receipt_count": 0,
        "etsy_error": None,
        "shopify_orders": orders - 2,
        "etsy_orders": 2,
        "shopify_revenue": revenue * 0.85,
        "etsy_revenue": revenue * 0.15,
    }


def _seed_history(state_dir: Path, *, today: date, days: int = 30) -> None:
    for i in range(days):
        day = (today - timedelta(days=days - i)).isoformat()
        _write_receipt(
            state_dir,
            day,
            state="verified",
            state_reason="report_emailed",
            metadata=_normal_metadata(orders=10 + (i % 5), revenue=150.0 + i, net=70.0 + i, margin=45.0 + (i % 7)),
        )


class ProfitIntelTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.state_dir = Path(self.tmp.name) / "state"
        self.output_dir = Path(self.tmp.name) / "output" / "operator"
        self._patches = [
            patch.object(profit_intel, "STATE_DIR", self.state_dir),
            patch.object(profit_intel, "WORKFLOW_CONTROL_DIR", self.state_dir / "workflow_control"),
            patch.object(profit_intel, "OUTPUT_OPERATOR_DIR", self.output_dir),
            patch.object(profit_intel, "PROFIT_INTEL_STATE_PATH", self.state_dir / "profit_intel.json"),
            patch.object(profit_intel, "PROFIT_INTEL_MD_PATH", self.output_dir / "profit_intel.md"),
        ]
        for p in self._patches:
            p.start()
        self.addCleanup(self._stop_patches)
        self.today = date(2026, 5, 23)
        self.now = datetime(2026, 5, 23, 12, 0, tzinfo=timezone(timedelta(hours=-4)))
        self._now_patch = patch.object(profit_intel, "_now", return_value=self.now)
        self._now_patch.start()
        self.addCleanup(self._now_patch.stop)

    def _stop_patches(self) -> None:
        for p in self._patches:
            p.stop()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_contract_shape_available(self) -> None:
        _seed_history(self.state_dir, today=self.today, days=20)
        _write_receipt(
            self.state_dir,
            self.today.isoformat(),
            state="observed",
            state_reason="weekly_operator_email_deferred",
            metadata={
                **_normal_metadata(),
                "operator_email_cadence": {
                    "cadence": "weekly",
                    "weekly_email_day": "Monday",
                    "target_weekday": 0,
                },
                "anomaly": {"triggered": False, "reasons": [], "sanity_blocked": False, "confidence": "normal"},
            },
            updated_at=f"{self.today.isoformat()}T11:00:00-04:00",
            last_verification={"kind": "operator_email_cadence", "status": "deferred", "weekly_email_day": "Monday", "target_weekday": 0, "cadence": "weekly"},
        )
        payload = profit_intel.build_profit_intel(today=self.today)
        self.assertTrue(payload["available"])
        for key in ("generated_at", "data_as_of", "yesterday", "trend_7d", "anomaly", "email_status", "route"):
            self.assertIn(key, payload, msg=f"missing key: {key}")
        self.assertEqual(payload["email_status"]["today_action"], "deferred")
        self.assertEqual(payload["yesterday"]["date"], self.today.isoformat())
        self.assertEqual(payload["anomaly"]["triggered"], False)

    def test_no_history_returns_empty_state(self) -> None:
        payload = profit_intel.build_profit_intel(today=self.today)
        self.assertFalse(payload["available"])
        self.assertEqual(payload["reason"], "no_profit_history")
        self.assertNotIn("email_status", payload, "empty-state must not include email_status")

    def test_stale_data_returns_stale_empty_state(self) -> None:
        _write_receipt(
            self.state_dir,
            (self.today - timedelta(days=5)).isoformat(),
            state="verified",
            state_reason="report_emailed",
            metadata=_normal_metadata(),
            updated_at=(self.now - timedelta(hours=72)).isoformat(),
        )
        payload = profit_intel.build_profit_intel(today=self.today)
        self.assertFalse(payload["available"])
        self.assertEqual(payload["reason"], "stale_data")
        self.assertNotIn("email_status", payload)

    def test_pending_when_today_not_run_but_history_exists(self) -> None:
        _seed_history(self.state_dir, today=self.today, days=14)
        payload = profit_intel.build_profit_intel(today=self.today)
        self.assertTrue(payload["available"])
        self.assertEqual(payload["email_status"]["today_action"], "pending")
        self.assertIn("banner", payload)

    def test_anomaly_round_trip_from_receipt_metadata(self) -> None:
        _seed_history(self.state_dir, today=self.today, days=20)
        _write_receipt(
            self.state_dir,
            self.today.isoformat(),
            state="verified",
            state_reason="report_emailed_anomaly_bypass",
            metadata={
                **_normal_metadata(net=-18.40, revenue=42.10, margin=-43.7),
                "anomaly": {"triggered": True, "reasons": ["net_negative", "revenue_below_30d_floor"], "sanity_blocked": False, "confidence": "normal"},
            },
            updated_at=f"{self.today.isoformat()}T11:00:00-04:00",
            last_verification={"kind": "operator_email_cadence", "status": "anomaly_bypass", "anomaly_reasons": ["net_negative", "revenue_below_30d_floor"], "cadence": "weekly", "weekly_email_day": "Monday", "target_weekday": 0},
        )
        payload = profit_intel.build_profit_intel(today=self.today)
        self.assertTrue(payload["anomaly"]["triggered"])
        self.assertEqual(payload["anomaly"]["reasons"], ["net_negative", "revenue_below_30d_floor"])
        self.assertEqual(payload["email_status"]["today_action"], "anomaly_bypass")
        ok, errors = contracts.validate_profit_anomaly_metadata(payload["anomaly"])
        self.assertTrue(ok, msg=f"contract violations: {errors}")

    def test_cold_start_trend_is_null(self) -> None:
        _seed_history(self.state_dir, today=self.today, days=5)
        payload = profit_intel.build_profit_intel(today=self.today)
        self.assertTrue(payload["available"])
        self.assertIsNone(payload["trend_7d"]["orders_delta_pct"])
        self.assertIsNone(payload["trend_7d"]["net_delta_pct"])

    def test_channels_pass_through(self) -> None:
        _seed_history(self.state_dir, today=self.today, days=20)
        payload = profit_intel.build_profit_intel(today=self.today)
        channels = payload["yesterday"]["channels"]
        self.assertIn("shopify", channels)
        self.assertIn("etsy", channels)

    def test_unknown_state_reasons_render_cleanly(self) -> None:
        _seed_history(self.state_dir, today=self.today, days=14)
        _write_receipt(
            self.state_dir,
            self.today.isoformat(),
            state="blocked",
            state_reason="profit_metrics_impossible",
            metadata={
                **_normal_metadata(),
                "anomaly": {"triggered": False, "reasons": [], "sanity_blocked": True, "confidence": "normal"},
            },
            updated_at=f"{self.today.isoformat()}T11:00:00-04:00",
        )
        payload = profit_intel.build_profit_intel(today=self.today)
        self.assertEqual(payload["email_status"]["today_action"], "errored")
        rendered = profit_intel.render_profit_intel_markdown(payload)
        self.assertIn("Sanity floor tripped", rendered)

    def test_contract_validator_accepts_default_anomaly(self) -> None:
        ok, errors = contracts.validate_profit_anomaly_metadata({
            "triggered": False,
            "reasons": [],
            "sanity_blocked": False,
            "confidence": "normal",
        })
        self.assertTrue(ok, msg=f"contract violations: {errors}")

    def test_contract_validator_rejects_unknown_reason(self) -> None:
        ok, errors = contracts.validate_profit_anomaly_metadata({
            "triggered": True,
            "reasons": ["made_up_reason"],
            "sanity_blocked": False,
            "confidence": "normal",
        })
        self.assertFalse(ok)
        self.assertTrue(any("unknown anomaly reason" in e for e in errors))


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock


RUNTIME_DIR = Path("/Users/philtullai/ai-agents/duck-ops/runtime")
if str(RUNTIME_DIR) not in sys.path:
    sys.path.insert(0, str(RUNTIME_DIR))

import widget_api  # noqa: E402


class WidgetApiTests(unittest.TestCase):
    def test_build_widget_status_delegates_to_shared_contract_payload(self) -> None:
        expected = {"surfaceVersion": 1, "ducksToPackToday": 3}
        with mock.patch.object(widget_api, "build_widget_status_payload", return_value=expected) as payload_mock:
            result = widget_api.build_widget_status()

        self.assertEqual(result, expected)
        payload_mock.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()

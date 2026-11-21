"""Strategy package loading through the Python host. No exchange access."""

import json
import subprocess
import sys
import unittest
from decimal import Decimal
from pathlib import Path

from python.strategy_host.host import PackageError, run_strategy

ROOT = Path(__file__).resolve().parents[2]
HOST = ROOT / "python" / "strategy_host" / "host.py"
PACKAGE = ROOT / "packages" / "ema_crossover"


def run_host(request):
    completed = subprocess.run(
        [sys.executable, str(HOST)],
        input=json.dumps(request),
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )
    if completed.returncode != 0:
        raise AssertionError(completed.stderr)
    return json.loads(completed.stdout)


def bars(prices):
    rows = []
    for index, price in enumerate(prices):
        text = str(price)
        rows.append({
            "open_time_ms": index * 60_000,
            "open": text,
            "high": text,
            "low": text,
            "close": text,
            "volume": "1",
        })
    return rows


class StrategyLoadingTest(unittest.TestCase):
    def test_sample_package_publishes_long_then_flat(self):
        prices = [100] * 30 + [200] * 30 + [50] * 30
        result = run_host({
            "op": "run",
            "package_dir": str(PACKAGE),
            "parameters": {},
            "symbol": "ETHUSDT",
            "interval": "1m",
            "bars": bars(prices),
        })
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["prepared"]["symbol"], "ETHUSDT")
        quantities = [item["quantity"] for item in result["publications"]]
        self.assertIn("1", quantities)
        self.assertIn("0", quantities)

    def test_host_emits_weight_without_sizing_in_python(self):
        class WeightStrategy:
            def __init__(self, _context):
                pass

            def on_start(self):
                pass

            def on_bar(self, _bar):
                self.publish(symbol="ETHUSDT", weight=Decimal("-0.25"))

        published = run_strategy(
            WeightStrategy, {}, "ETHUSDT", "1m", bars([100])
        )
        self.assertEqual(published, [
            {"symbol": "ETHUSDT", "bar_index": 0, "weight": "-0.25"}
        ])
        self.assertNotIn("quantity", published[0])

    def test_host_rejects_invalid_weight_targets(self):
        for value in (Decimal("1.01"), Decimal("-1.01"), Decimal("NaN"), 0.5):
            class InvalidStrategy:
                def __init__(self, _context):
                    pass

                def on_start(self):
                    pass

                def on_bar(self, _bar):
                    self.publish(symbol="ETHUSDT", weight=value)

            with self.subTest(value=value):
                with self.assertRaises(PackageError):
                    run_strategy(InvalidStrategy, {}, "ETHUSDT", "1m", bars([100]))

    def test_versioned_host_protocol_round_trip(self):
        result = run_host({
            "protocol_version": 1,
            "op": "prepare",
            "package_dir": str(PACKAGE),
            "parameters": {},
            "symbol": "ETHUSDT",
            "interval": "1m",
            "bars": [],
        })
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["protocol_version"], 1)
        self.assertEqual(result["publications"], [])

    def test_host_rejects_unknown_and_boolean_protocol_versions(self):
        for version in (2, True, "1"):
            with self.subTest(version=version):
                result = run_host({
                    "protocol_version": version,
                    "op": "prepare",
                    "package_dir": str(PACKAGE),
                    "parameters": {},
                })
                self.assertFalse(result["ok"], result)
                self.assertIn("版本", result["error"])
                self.assertNotIn("protocol_version", result)

    def test_versioned_host_rejects_unknown_fields_and_ops(self):
        base = {
            "protocol_version": 1,
            "op": "prepare",
            "package_dir": str(PACKAGE),
            "parameters": {},
            "bars": [],
        }
        for override in (
            {"orders": []},
            {"op": "submit_order"},
            {"archive_path": "/tmp/other.zip"},
            {"package_dir": None},
            {"op": "prepare", "bars": [{"close": "100"}]},
        ):
            with self.subTest(override=override):
                result = run_host({**base, **override})
                self.assertFalse(result["ok"], result)
                self.assertEqual(result["protocol_version"], 1)

    def test_unknown_parameter_is_rejected(self):
        result = run_host({
            "op": "prepare",
            "package_dir": str(PACKAGE),
            "parameters": {"api_key": "should-not-load"},
            "symbol": "ETHUSDT",
            "interval": "1m",
            "bars": [],
        })
        self.assertFalse(result["ok"])
        self.assertIn("密钥", result["error"])

    def test_strategy_cannot_submit_an_order(self):
        package = ROOT / "packages" / "ema_crossover" / "strategy.py"
        self.assertIn("publish", package.read_text(encoding="utf-8"))
        self.assertNotIn("submit_order(", package.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()

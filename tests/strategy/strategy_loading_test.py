"""Strategy package loading through the Python host. No exchange access."""

import json
import subprocess
import sys
import unittest
from pathlib import Path

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

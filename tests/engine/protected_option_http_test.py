"""Authorized issued option demo end-to-end through the offline HTTP backtest."""

import base64
import json
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from engine.api.app import create_app
from engine.config import EngineConfig
from engine.strategies.package import archive_directory, parse_metadata
from engine.trading.runtime import EngineConflict, EngineRuntime
from tests.engine.option_history_test import CALL, PUT, _catalog


def _archive(root):
    root.mkdir()
    (root / "seealpha-package.json").write_text(json.dumps({
        "format": 1, "name": "demo_option_hedged_bollinger",
        "version": "1", "entry_point": "strategy:DemoOptionHedgedBollinger",
    }), encoding="utf-8")
    (root / "strategy.py").write_text(
        """from nautilus_trader.trading import Strategy

def prepare(params):
    return dict(params)

class DemoOptionHedgedBollinger(Strategy):
    pass

class BollingerModel:
    def __init__(self, period, deviation):
        self.count = 0

    def propose(self, snapshot, params):
        self.count += 1
        if self.count > 2:
            return {"action": "flat", "targets": []}
        entering = self.count == 1
        return {
            "action": "enter_long" if entering else "exit",
            "new_position": 1 if entering else 0,
            "targets": [
                {"product": "options", "symbol": params["put_symbol"],
                 "quantity": "0.100" if entering else "0"},
                {"product": "options", "symbol": params["call_symbol"],
                 "quantity": "0.100" if entering else "0"},
                {"product": "futures_um", "symbol": params["symbol"],
                 "quantity": "0.100" if entering else "0"},
            ],
        }

    def commit(self, plan):
        self.last_committed = plan["action"]
""",
        encoding="utf-8",
    )
    payload = archive_directory(root)
    return payload, parse_metadata(payload)


def _issued_payload(archive, meta):
    return {
        "account_id": "desk",
        "issuance_id": "option-1",
        "code_key": meta.name,
        "version": meta.version,
        "grant_purpose": "development",
        "parameters": {
            "symbol": "ETHUSDT",
            "put_symbol": PUT,
            "call_symbol": CALL,
            "allocation": "0.100", "risk_limit": "0.100",
            "option_contracts": "0.100", "period": 5, "deviation": "2",
            "min_dte_seconds": 3600, "max_age_seconds": 180,
            "max_total_premium": "30",
        },
        "entry_point": meta.entry_point,
        "package_hash": meta.digest,
        "archive": base64.b64encode(archive).decode(),
    }


def test_issued_native_protected_backtest_authenticated_and_persisted(tmp_path):
    _, start = _catalog(tmp_path / "data")
    archive, meta = _archive(tmp_path / "package")
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    runtime.ensure_tenant("dev", "development")
    runtime.ensure_tenant("other", "development")
    for product, symbol in (
        ("futures_um", "ETHUSDT"), ("options", PUT), ("options", CALL),
    ):
        runtime.note_subscription(
            "dev", "market-data", product, symbol,
            "intent", "configured", {"history_enabled": True},
        )
    runtime.receive_strategy("dev", _issued_payload(archive, meta))
    token = "protected-native-integration-token-" + "0" * 34
    app = create_app(
        EngineConfig(tmp_path / "state", token=token, auth_mode="token", project=tmp_path),
        runtime=runtime,
    )
    endpoint = "/tenants/dev/accounts/desk/protected-option-backtests"
    body = {
        "start": start.isoformat(),
        "end": (start + timedelta(minutes=4)).isoformat(),
        "interval": "1m",
        "starting_options_balance": "1000",
        "starting_perpetual_balance": "1000",
        "max_gross_notional": "1000",
    }
    try:
        with TestClient(app) as client:
            assert client.post(endpoint, json=body).status_code == 401
            headers = {"Authorization": "Bearer " + token}
            response = client.post(endpoint, json=body, headers=headers)
            assert response.status_code == 200, response.text
            result = response.json()
            assert result["mode"] == "native_protected_options_development_only"
            assert result["model_source"] == "issued_strategy_package"
            assert len(result["fills"]) == 6
            assert result["target_events"]
            latest = client.get(endpoint + "/latest", headers=headers)
            assert latest.status_code == 200
            assert latest.json() == result
            other = client.post(
                "/tenants/other/accounts/desk/protected-option-backtests",
                json=body, headers=headers,
            )
            assert other.status_code == 409
    finally:
        runtime.close()


def test_rejects_missing_issued_demo_and_subscriptions(tmp_path):
    _, start = _catalog(tmp_path / "data")
    archive, meta = _archive(tmp_path / "package")
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    runtime.ensure_tenant("dev", "development")
    window = (
        start.isoformat(),
        (start + timedelta(minutes=4)).isoformat(),
        "1m", "1000", "1000", "1000",
    )
    try:
        with pytest.raises(EngineConflict, match="单一已下发"):
            runtime.run_protected_option_backtest("dev", "desk", *window)
        runtime.receive_strategy("dev", _issued_payload(archive, meta))
        with pytest.raises(EngineConflict, match="订阅"):
            runtime.run_protected_option_backtest("dev", "desk", *window)
    finally:
        runtime.close()

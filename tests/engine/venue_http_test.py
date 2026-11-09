"""Venue HTTP queries remain protected by tenant identity and Engine token."""
from decimal import Decimal

from fastapi.testclient import TestClient

from engine.app import create_app
from engine.config import EngineConfig


def test_public_venue_query_tenant_identity_and_contract(tmp_path, monkeypatch):
    from engine.connectors.binance import assets, manual

    monkeypatch.setattr(
        manual, "market_tickers", _ticker,
    )
    monkeypatch.setattr(
        assets, "asset_price", lambda env, currency: {"price": Decimal("1.000000001"), "priced_at": 123},
    )
    app = create_app(EngineConfig(tmp_path / "state", "private", project=tmp_path, tenant_id="23", tenant_purpose="development"))
    with TestClient(app) as client:
        auth = {"Authorization": "Bearer private"}
        assert client.post("/tenants/23/venue/tickers", json={"environment": "production"}).status_code == 401
        assert client.post("/tenants/24/venue/tickers", headers=auth, json={"environment": "production"}).status_code == 403
        assert client.post("/tenants/23/venue/tickers", headers=auth, json={"environment": "production"}).json()["rows"]["ETHUSDT"]["last_price"] == "2000"
        assert client.post(
            "/tenants/23/venue/asset-account", headers=auth,
            json={"environment": "production", "api_key": "fake", "api_secret": "fake", "account_id": "1"},
        ).status_code == 403
        price = client.post("/tenants/23/venue/asset-price", headers=auth, json={
            "environment": "production", "currency": "USDT",
        })
        assert price.status_code == 200
        assert price.json()["price"] == "1.000000001"
        assert client.post("/tenants/23/venue/asset-price", headers=auth, json={
            "environment": "production", "currency": "../etc/passwd",
        }).status_code == 422


async def _ticker(env):
    return {"checked_at": 123, "rows": {"ETHUSDT": {"last_price": "2000"}}}


def test_production_venue_history_and_precision_are_isolated(tmp_path, monkeypatch):
    import json

    from engine.connectors.binance import assets, operations, options, options_account
    from engine.connectors.binance import income as income_module

    monkeypatch.setattr(assets, "asset_account", lambda account, *, positions=False: {
        "multi_assets": False,
        "observed_at": 123,
        "totals": {"total_wallet_balance": "100.000000000001"},
        "assets": {"USDT": {"walletBalance": Decimal("100.000000000001"),
                            "unrealizedProfit": Decimal(0),
                            "maintMargin": Decimal(0)}},
    })

    async def history(account, start_ms, end_ms):
        yield {"start_ms": start_ms, "end_ms": end_ms, "records": [{"transaction_id": "one"}]}

    async def operation_history(account, *, lookback_minutes):
        return [{"event_id": "read-only", "timestamp": 123}]

    class FakeOptionsClient:
        def __init__(self, key, secret, *, environment):
            assert (key, secret, environment) == ("key", "secret", "production")

        async def positions(self, symbol=None):
            return [{"symbol": "ETH-TEST", "quantity": "1"}]

        async def open_orders(self, symbol=None):
            return {"orders": [], "possibly_truncated": False}

        async def order_history(self, symbol, start_ms, end_ms):
            return {"orders": [], "possibly_truncated": False}

        async def history_pages(self, endpoint, start_ms, end_ms, *, symbol=None):
            yield {"rows": [{"id": "1"}], "start_ms": start_ms, "end_ms": end_ms}

    monkeypatch.setattr(income_module, "income_history", history)
    monkeypatch.setattr(operations, "operation_history", operation_history)
    monkeypatch.setattr(options, "get", lambda name: {"serverTime": 123})
    monkeypatch.setattr(options_account, "BinanceOptionsAccountClient", FakeOptionsClient)
    app = create_app(EngineConfig(
        tmp_path / "state", "private", project=tmp_path,
        tenant_id="77", tenant_purpose="production",
    ))
    app.state.runtime.ensure_tenant("77", "production")
    payload = {
        "environment": "production", "api_key": "key", "api_secret": "secret",
        "account_id": "5", "account_name": "Account", "exchange": "binance",
    }
    headers = {"Authorization": "Bearer private"}
    with TestClient(app) as client:
        assets_response = client.post("/tenants/77/venue/asset-account", headers=headers, json=payload)
        assert assets_response.status_code == 200
        assert assets_response.json()["assets"]["USDT"]["walletBalance"] == "100.000000000001"
        assert client.post("/tenants/78/venue/asset-account", headers=headers, json=payload).status_code == 403
        records = client.post(
            "/tenants/77/venue/operation-history", headers=headers,
            json={**payload, "lookback_minutes": 60},
        )
        assert records.status_code == 200
        assert records.json()["records"][0]["event_id"] == "read-only"
        pages = client.post(
            "/tenants/77/venue/income", headers=headers,
            json={**payload, "start_ms": 1, "end_ms": 2},
        )
        assert pages.status_code == 200
        assert json.loads(pages.text.splitlines()[0])["records"][0]["transaction_id"] == "one"
        options_pages = client.post(
            "/tenants/77/venue/options/pages", headers=headers,
            json={**payload, "endpoint": "bill", "start_ms": 1, "end_ms": 2},
        )
        assert options_pages.status_code == 200
        assert json.loads(options_pages.text.splitlines()[0])["rows"][0]["id"] == "1"
        assert client.post(
            "/tenants/77/venue/options/time", headers=headers,
            json={"environment": "production"},
        ).json()["serverTime"] == 123

"""Spot market data is isolated from USD-M and never opens trading endpoints."""

import asyncio

import pytest
from fastapi.testclient import TestClient

from core.market.instruments import (
    MarketConfigError,
    bar_type_for,
    instrument_id_for,
    normalize_symbol,
)
from core.market.models import DataError
from engine.app import create_app
from engine.config import EngineConfig
from engine.connectors.binance import spot


class FakeREST:
    def __init__(self, response):
        self.response = response
        self.requests = []

    async def get(self, endpoint, **params):
        self.requests.append((endpoint, params))
        return self.response


def test_spot_identity_never_collides_with_usdm():
    assert normalize_symbol("BTC/USDT", "spot") == "BTCUSDT"
    assert normalize_symbol("1INCH-USDC", "spot") == "1INCHUSDC"
    assert instrument_id_for("BTCUSDT", "spot") == "BTCUSDT.BINANCE"
    assert instrument_id_for("BTCUSDT", "futures_um") == "BTCUSDT-PERP.BINANCE"
    assert bar_type_for("BTCUSDT", "1m", "spot") == "BTCUSDT.BINANCE-1-MINUTE-LAST-EXTERNAL"
    with pytest.raises(MarketConfigError):
        normalize_symbol("../../etc", "spot")


def test_spot_catalog_filters_inactive_and_nonspot_contracts():
    data = {
        "symbols": [
            {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT",
             "status": "TRADING", "isSpotTradingAllowed": True},
            {"symbol": "ETHUSDC", "baseAsset": "ETH", "quoteAsset": "USDC",
             "status": "HALT", "isSpotTradingAllowed": True},
            {"symbol": "BTCUSDC", "baseAsset": "BTC", "quoteAsset": "USDC",
             "status": "TRADING", "permissions": ["MARGIN"]},
            {"symbol": "SOLFDUSD", "baseAsset": "SOL", "quoteAsset": "FDUSD",
             "status": "TRADING", "permissions": ["SPOT"]},
        ]
    }
    rest = FakeREST(data)
    rows = asyncio.run(spot.markets(rest=rest))
    assert rows == [
        {"symbol": "BTC/USDT", "market_type": "spot", "base": "BTC", "quote": "USDT", "expiry": None},
        {"symbol": "SOL/FDUSD", "market_type": "spot", "base": "SOL", "quote": "FDUSD", "expiry": None},
    ]
    assert rest.requests == [("exchangeInfo", {})]


def test_spot_candles_maintain_precision_closed_status_and_pagination():
    base = 1785542400000
    rest = FakeREST([
        [base, "2000.00000001", "2001.00", "1999.0", "2000.5", "5.123456789",
         base + 59999, "0", 1, "0", "0", "0"],
    ])
    value = asyncio.run(spot.candles("ETHUSDT", "1m", base, base + 120000, limit=1, rest=rest))
    assert value["product"] == "spot"
    assert value["symbol"] == "ETHUSDT"
    assert value["candles"][0]["open"] == "2000.00000001"
    assert value["candles"][0]["volume"] == "5.123456789"
    assert value["candles"][0]["is_closed"] is True
    assert value["next_start_ms"] == base + 60000
    assert rest.requests == [("klines", {
        "symbol": "ETHUSDT", "interval": "1m", "startTime": base,
        "endTime": base + 119999, "limit": 1,
    })]
    with pytest.raises(ValueError):
        asyncio.run(spot.candles("ETHUSDT", "1m", base + 1000, base, rest=rest))
    with pytest.raises(DataError):
        asyncio.run(spot.candles("ETHUSDT", "1m", base, base + 60000,
                                 rest=FakeREST([[base, "1", "0.5", "2", "1", "1", base+59999]])))


def test_recent_spot_trades_are_not_represented_as_complete_history():
    result = asyncio.run(spot.recent_trades("ETH/USDT", rest=FakeREST([
        {"id": 14, "time": 1785542400000, "price": "2000.000000001",
         "qty": "0.123456789", "isBuyerMaker": False},
    ])))
    assert result["complete_history"] is False
    assert result["trades"][0]["price"] == "2000.000000001"
    assert spot.SpotREST().source is None
    assert "order" not in spot.SpotREST.allowed_endpoints


def test_spot_venue_http_authorization_validation_and_boundaries(tmp_path, monkeypatch):
    async def fake_markets(*, environment="production"):
        return [{"symbol": "ETH/USDT", "market_type": "spot", "base": "ETH",
                 "quote": "USDT", "expiry": None}]

    async def fake_candles(symbol, interval, start_ms, end_ms, *, limit, environment="production"):
        return {"product": "spot", "symbol": symbol, "interval": interval,
                "candles": [], "next_start_ms": None, "source": "binance_spot_public_rest"}

    async def fake_trades(symbol, *, limit, environment="production"):
        return {"product": "spot", "symbol": symbol, "trades": [],
                "complete_history": False, "source": "binance_spot_public_rest"}

    monkeypatch.setattr(spot, "markets", fake_markets)
    monkeypatch.setattr(spot, "candles", fake_candles)
    monkeypatch.setattr(spot, "recent_trades", fake_trades)
    app = create_app(EngineConfig(
        tmp_path / "state", "integration-spot-test-token-" + "x" * 48, project=tmp_path, auth_mode="token", tenant_id="23",
        tenant_purpose="development",
    ))
    auth = {"Authorization": "Bearer integration-spot-test-token-" + "x" * 48}
    base = "/tenants/23/venue"
    with TestClient(app) as client:
        assert client.post(base + "/catalog", json={"product": "spot"}).status_code == 401
        assert client.post("/tenants/24/venue/catalog", headers=auth,
                           json={"product": "spot"}).status_code == 403
        listing = client.post(base + "/catalog", headers=auth,
                              json={"product": "spot", "environment": "production"})
        assert listing.status_code == 200
        assert listing.json()["items"][0]["market_type"] == "spot"
        sandbox_listing = client.post(base + "/catalog", headers=auth,
                                      json={"product": "spot", "environment": "sandbox"})
        assert sandbox_listing.status_code == 200
        assert sandbox_listing.json()["items"][0]["market_type"] == "spot"
        candles = client.post(base + "/spot/klines", headers=auth, json={
            "symbol": "ETH/USDT", "interval": "1m", "start_ms": 1, "end_ms": 60001,
        })
        assert candles.status_code == 200
        assert candles.json()["symbol"] == "ETHUSDT"
        assert client.post(base + "/spot/klines", headers=auth, json={
            "symbol": "ETHUSDT", "interval": "1m", "start_ms": 60001, "end_ms": 1,
        }).status_code == 422
        assert client.post(base + "/spot/trades", headers=auth, json={
            "symbol": "ETHUSDT", "limit": 1001,
        }).status_code == 422
        assert client.post(base + "/spot/trades", headers=auth, json={
            "symbol": "ETHUSDT", "limit": 1,
        }).json()["complete_history"] is False

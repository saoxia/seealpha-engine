"""Public option contract parsing and native model mapping, without trading I/O."""

from copy import deepcopy

import pytest

from engine.connectors.binance import options
from engine.connectors.binance.queries import QueryError


def exchange_info():
    return {"serverTime": 1793145600000,
            "optionContracts": [{"underlying": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "settleAsset": "USDT"}],
            "optionSymbols": [{"symbol": "BTC-261225-85000-C", "expiryDate": 1798185600000,
                               "underlying": "BTCUSDT", "side": "CALL", "strikePrice": "85000.000",
                               "unit": 10, "quoteAsset": "USDT", "priceScale": 3, "quantityScale": 2,
                               "minQty": "0.01", "maxQty": "200", "status": "TRADING",
                               "initialMargin": "0.15", "maintenanceMargin": "0.075",
                               "filters": [{"filterType": "PRICE_FILTER", "tickSize": "5.000"},
                                           {"filterType": "LOT_SIZE", "stepSize": "0.01"}]}]}


def definition(monkeypatch):
    monkeypatch.setattr(options, "get", lambda endpoint, **params: exchange_info())
    return options.markets()[0]


def test_option_metadata_preserves_contract_identity_units_and_put(monkeypatch):
    value = exchange_info()
    put = deepcopy(value["optionSymbols"][0])
    put.update(symbol="BTC-261225-85000-P", side="PUT")
    value["optionSymbols"].append(put)
    expired = deepcopy(put)
    expired.update(symbol="BTC-260801-85000-P", expiryDate=1785571200000)
    value["optionSymbols"].append(expired)
    monkeypatch.setattr(options, "get", lambda *args, **kwargs: value)
    items = options.markets()
    assert len(items) == 2
    call = options.instrument(items[0])
    put_model = options.instrument(items[1])
    assert str(call.id) == "BTC-261225-85000-C.BINANCE"
    assert str(put_model.option_kind) == "PUT"
    assert str(call.multiplier) == "10"
    assert call.expiration_ns == 1798185600000000000
    assert not call.info["activation_time_known"]
    assert str(call.strike_price) == "85000.000"
    assert str(call.margin_init) == "0.15"
    assert str(call.margin_maint) == "0.075"


@pytest.mark.parametrize("field,value", [("side", "PUT"), ("strikePrice", "85001"),
                                       ("expiryDate", 1798272000000), ("unit", 0),
                                       ("priceScale", 99), ("quantityScale", True)])
def test_inconsistent_metadata_is_rejected(monkeypatch, field, value):
    packet = exchange_info()
    packet["optionSymbols"][0][field] = value
    monkeypatch.setattr(options, "get", lambda *args, **kwargs: packet)
    with pytest.raises(QueryError):
        options.markets()


def test_only_fixed_public_endpoints_are_permitted():
    with pytest.raises(ValueError):
        options.get("order", symbol="BTC-261225-85000-C")


def test_public_transport_bounds_response_and_rejects_redirect(monkeypatch):
    import json

    seen = []

    class Response:
        url = options.REST_URL + "exchangeInfo"
        body = json.dumps(exchange_info()).encode()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, limit):
            assert limit == 16 * 1024 * 1024 + 1
            return self.body

    def open_request(request, timeout):
        seen.append(request)
        assert timeout == 20
        return Response()

    monkeypatch.setattr(options, "urlopen", open_request)
    monkeypatch.setattr(options.time, "sleep", lambda seconds: None)
    assert options.get("exchangeInfo")["optionSymbols"]
    assert seen[0].full_url == options.REST_URL + "exchangeInfo"
    assert "X-mbx-apikey" not in seen[0].headers
    Response.url = "https://untrusted.example/exchangeInfo"
    with pytest.raises(QueryError):
        options.get("exchangeInfo")
    Response.url = options.REST_URL + "exchangeInfo"
    Response.body = b"x" * (16 * 1024 * 1024 + 1)
    with pytest.raises(QueryError):
        options.get("exchangeInfo")


def test_rate_limit_backoff_is_bounded(monkeypatch):
    from urllib.error import HTTPError

    delays = []

    def unavailable(request, timeout):
        raise HTTPError(request.full_url, 429, "rate limit", {"Retry-After": "1000"}, None)

    monkeypatch.setattr(options, "urlopen", unavailable)
    monkeypatch.setattr(options.time, "sleep", delays.append)
    with pytest.raises(QueryError):
        options.get("trades", symbol="BTC-261225-85000-C", limit=500)
    assert max(delays) <= 30

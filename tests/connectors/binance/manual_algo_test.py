"""Offline native algo/leverage guards: no live exchange requests."""
import asyncio

import pytest

from core.trading_contracts import AlgoOrderValues, LeverageChangeValues
from engine.connectors.binance import manual_algo
from engine.connectors.binance.manual import ManualValidationError
from engine.connectors.binance.queries import QueryError


ACCOUNT = {"id": 1, "environment": "sandbox",
           "credentials": {"apiKey": "fictional-key", "secret": "fictional-secret"}}
VALID = {
    "account_revision": "f" * 64, "request_id": "f819b521-1293-4ccb-a0e2-9b196e960101",
    "symbol": "ETHUSDT-PERP", "environment": "sandbox", "side": "sell",
    "order_type": "STOP_MARKET", "quantity": "0.010", "trigger_price": "1800",
    "expected_position_mode": "oneway",
}
POSITION = {"position_mode": "oneway",
            "positions": [{"position_side": "BOTH", "side": "long", "quantity": "0.020"}],
            "instrument": {"size_increment": "0.001", "min_quantity": "0.001",
                           "price_increment": "0.01"}}


def order_row(status="NEW", client_id="SA" + "1" * 32):
    return {"algoId": 22, "clientAlgoId": client_id, "algoStatus": status,
            "orderType": "STOP_MARKET", "symbol": "ETHUSDT", "side": "SELL",
            "positionSide": "BOTH", "quantity": "0.010", "triggerPrice": "1800",
            "workingType": "MARK_PRICE", "createTime": 123}


def test_native_algo_validation_rejects_incompatible_parameters():
    assert AlgoOrderValues(**VALID).order_type == "STOP_MARKET"
    for changes in (
        {"order_type": "TRAILING_STOP_MARKET"},
        {"order_type": "TRAILING_STOP_MARKET", "trigger_price": None, "callback_rate": "0.01"},
        {"order_type": "TAKE_PROFIT", "price": None},
        {"order_type": "STOP_MARKET", "price": "1700"},
        {"quantity": "0"},
        {"position_effect": "open"},
        {"price_protect": True, "order_type": "STOP"},
    ):
        with pytest.raises(ValueError):
            AlgoOrderValues(**{**VALID, **changes})
    assert LeverageChangeValues(account_revision="f"*64,
        request_id=VALID["request_id"], symbol="ETHUSDT-PERP", environment="sandbox",
        leverage=10).leverage == 10


def test_algo_view_is_not_regular_order_and_checks_identity():
    row = manual_algo.algo_view(order_row())
    assert row["open"] and row["client_algo_id"].startswith("SA")
    assert row["trigger_price"] == "1800" and row["order_type"] == "STOP_MARKET"
    assert not manual_algo.algo_view(order_row("CANCELED"))["open"]
    for bad in ({**order_row(), "clientAlgoId": "bad id"}, {**order_row(), "algoStatus": "MYSTERY"}):
        with pytest.raises(QueryError):
            manual_algo.algo_view(bad)


def test_reduce_only_algo_dispatch_does_not_bypass_native_position_guard(monkeypatch):
    requests = []
    class FakeClient:
        def __init__(self, account, **kwargs):
            assert account == ACCOUNT
        def request(self, method, path, values=None):
            requests.append((method, path, values))
            return order_row(client_id=values["clientAlgoId"])
    monkeypatch.setattr(manual_algo, "SignedFutures", FakeClient)
    cid = "SA" + "1" * 32
    values = AlgoOrderValues(**VALID).model_dump(mode="json")
    result = manual_algo.command_sync(ACCOUNT, "algo_submit", values, cid, snapshot=POSITION)
    assert result["algo_order"]["client_algo_id"] == cid
    sent = requests[0][2]
    assert sent["algoType"] == "CONDITIONAL" and sent["reduceOnly"] == "true"
    assert sent["positionSide"] == "BOTH" and sent["triggerPrice"] == "1800"
    assert sent["workingType"] == "MARK_PRICE" and sent["side"] == "SELL"
    with pytest.raises(ManualValidationError):
        manual_algo.command_sync(ACCOUNT, "algo_submit", {**values, "quantity": "0.10"}, cid, snapshot=POSITION)
    with pytest.raises(ManualValidationError):
        manual_algo.command_sync(ACCOUNT, "algo_submit", {**values, "expected_position_mode": "hedge"}, cid, snapshot=POSITION)
    with pytest.raises(ManualValidationError):
        manual_algo.command_sync(ACCOUNT, "algo_submit", {**values, "trigger_price": "1800.001"}, cid, snapshot=POSITION)
    assert len(requests) == 1


def test_hedge_and_trailing_are_venue_hosted(monkeypatch):
    sent = []
    class FakeClient:
        def __init__(self, account, **kwargs): pass
        def request(self, method, path, values=None):
            sent.append(values)
            return {**order_row(client_id=values["clientAlgoId"]), "orderType": "TRAILING_STOP_MARKET",
                    "positionSide": "SHORT", "triggerPrice": "", "callbackRate": "0.5"}
    monkeypatch.setattr(manual_algo, "SignedFutures", FakeClient)
    snapshot = {**POSITION, "position_mode": "hedge",
                "positions": [{"position_side": "SHORT", "side": "short", "quantity": "0.020"}]}
    values = {**VALID, "order_type": "TRAILING_STOP_MARKET", "trigger_price": None,
              "callback_rate": "0.5", "activation_price": "2050", "side": "buy",
              "expected_position_mode": "hedge"}
    manual_algo.command_sync(ACCOUNT, "algo_submit", AlgoOrderValues(**values).model_dump(mode="json"),
                             "SA" + "2"*32, snapshot=snapshot)
    assert sent[0]["type"] == "TRAILING_STOP_MARKET"
    assert sent[0]["positionSide"] == "SHORT"
    assert "reduceOnly" not in sent[0]
    assert sent[0]["callbackRate"] == "0.5"
    assert "triggerPrice" not in sent[0]


def test_leverage_is_bounded_by_symbol_bracket_and_verified_after_write(monkeypatch):
    requests = []
    class FakeClient:
        def __init__(self, account, **kwargs): pass
        def request(self, method, path, values=None):
            requests.append((method, path, values))
            if path.endswith("leverageBracket"):
                return [{"symbol": "ETHUSDT", "brackets": [{"initialLeverage": 20}]}]
            if path.endswith("symbolConfig"):
                return [{"symbol": "ETHUSDT", "leverage": 10}]
            if path.endswith("leverage"):
                return {"symbol": "ETHUSDT", "leverage": values["leverage"]}
    monkeypatch.setattr(manual_algo, "SignedFutures", FakeClient)
    payload = {"symbol": "ETHUSDT-PERP", "leverage": 10}
    assert manual_algo.command_sync(ACCOUNT, "leverage", payload, None)["leverage"] == 10
    assert any(path == "/fapi/v1/leverage" for _, path, _ in requests)
    requests.clear()
    with pytest.raises(ManualValidationError):
        manual_algo.command_sync(ACCOUNT, "leverage", {**payload, "leverage": 21}, None)
    assert not any(path == "/fapi/v1/leverage" for _, path, _ in requests)


def test_known_rejection_is_not_unknown_and_unknown_write_is_never_retried(monkeypatch):
    calls = []
    class FakeClient:
        def __init__(self, account, **kwargs): pass
        def request(self, method, path, values=None):
            calls.append(path)
            raise QueryError("ambiguous")
    monkeypatch.setattr(manual_algo, "SignedFutures", FakeClient)
    with pytest.raises(QueryError):
        manual_algo.command_sync(ACCOUNT, "algo_submit", VALID, "SA" + "1"*32, snapshot=POSITION)
    assert calls == ["/fapi/v1/algoOrder"]
    async def scenario():
        result = await manual_algo.execute(ACCOUNT, "algo_submit", {**VALID, "quantity": "100"}, "SA"+"1"*32,
                                           snapshot=POSITION)
        assert "validation_error" in result
    asyncio.run(scenario())

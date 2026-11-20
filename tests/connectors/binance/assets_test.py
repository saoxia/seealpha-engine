"""Fixed asset endpoints, exact prices and private-error isolation."""

import hashlib
import hmac
import io
import json
import time
from decimal import Decimal
from urllib.parse import parse_qs, urlsplit

import pytest

from engine.connectors.binance import assets
from engine.connectors.binance.queries import QueryError


def opener(monkeypatch, rows):
    calls = []

    class FakeOpener:
        def open(self, request, timeout):
            calls.append(request)
            assert request.method == "GET" and timeout == 8
            return io.BytesIO(json.dumps(rows.pop(0)).encode())

    monkeypatch.setattr(assets, "build_opener", lambda *args: FakeOpener())
    return calls


def test_read_only_asset_evidence_keeps_mode_currency_and_signed_values(monkeypatch):
    calls = opener(monkeypatch, [{"serverTime": 12345}, {
        "multiAssetsMargin": False,
        "totalWalletBalance": "100", "totalUnrealizedProfit": "5",
        "totalMarginBalance": "105", "totalMaintMargin": "1",
        "assets": [{"asset": "USDT", "walletBalance": "100", "unrealizedProfit": "5", "maintMargin": "1"},
                   {"asset": "BNB", "walletBalance": "-0.001", "unrealizedProfit": "0", "maintMargin": "0"}],
    }])
    result = assets.asset_account({"environment": "sandbox", "credentials": {"apiKey": "key", "secret": "secret"}})
    assert result["assets"]["BNB"]["walletBalance"] == Decimal("-0.001")
    assert result["multi_assets"] is False
    assert [urlsplit(r.full_url).path for r in calls] == ["/fapi/v1/time", "/fapi/v2/account"]
    assert all(urlsplit(r.full_url).hostname == "testnet.binancefuture.com" for r in calls)
    query, signature = urlsplit(calls[1].full_url).query.rsplit("&signature=", 1)
    assert parse_qs(query)["timestamp"] == ["12345"]
    assert hmac.new(b"secret", query.encode(), hashlib.sha256).hexdigest() == signature


@pytest.mark.parametrize("changes", [{}, {"symbol": "1000BNBUSD"}, {"index": "NaN"}, {"index": "0"}, {"time": 1}])
def test_asset_index_identity_precision_and_freshness(monkeypatch, changes):
    opener(monkeypatch, [{"symbol": "BNBUSD", "index": "600.123456789", "time": int(time.time() * 1000), **changes}])
    if changes:
        with pytest.raises(QueryError, match="资产指数价格"):
            assets.asset_price("production", "BNB")
    else:
        assert assets.asset_price("production", "BNB")["price"] == Decimal("600.123456789")


def test_asset_errors_do_not_expose_signed_requests_or_credentials(monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("private-secret signature=private-signature")

    monkeypatch.setattr(assets, "_read", fail)
    with pytest.raises(QueryError) as error:
        assets.asset_account({"environment": "production", "credentials": {"apiKey": "key", "secret": "private-secret"}})
    assert "private" not in str(error.value)

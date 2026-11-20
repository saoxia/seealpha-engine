"""Read-only income signing, bounded backfill, saturation and evidence validation."""

import asyncio
import hashlib
import hmac
import io
import json
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

import pytest

from engine.connectors.binance import income
from engine.connectors.binance.queries import QueryError


def account(**values):
    return {"id": 1, "account_name": "main", "exchange": "binance",
            "environment": "production",
            "credentials": {"apiKey": "fixture-key", "secret": "fixture-secret"}, **values}


def row(stamp=1000, transaction=9007199254740993, **values):
    return {"symbol": "", "incomeType": "TRANSFER", "income": "-0.123456789012345678",
            "asset": "USDC", "info": "", "time": stamp, "tranId": transaction,
            "tradeId": "", **values}


def collect(start, end):
    async def scenario():
        return [page async for page in income.income_history(account(), start, end)]
    return asyncio.run(scenario())


def test_signed_gets_use_fixed_hosts_all_types_and_exact_identifiers(monkeypatch):
    calls = []

    class Opener:
        def open(self, request, timeout):
            calls.append(request)
            assert request.get_method() == "GET" and request.data is None and timeout == 15
            parsed = urlsplit(request.full_url)
            assert parsed.scheme == "https" and parsed.hostname == "fapi.binance.com"
            if parsed.path.endswith("/time"):
                assert not request.has_header("X-mbx-apikey")
                return io.BytesIO(json.dumps({"serverTime": 123456789}).encode())
            query, signature = parsed.query.rsplit("&signature=", 1)
            expected = hmac.new(b"fixture-secret", query.encode(), hashlib.sha256).hexdigest()
            assert hmac.compare_digest(signature, expected)
            assert request.get_header("X-mbx-apikey") == "fixture-key"
            assert parse_qs(query) == {
                "startTime": ["1000"], "endTime": ["1999"], "limit": ["1000"],
                "timestamp": ["123456789"], "recvWindow": ["5000"],
            }
            return io.BytesIO(json.dumps([row()]).encode())

    monkeypatch.setattr(income, "build_opener", lambda *args: Opener())
    monkeypatch.setattr(income, "REQUEST_INTERVAL", 0)
    page = collect(1000, 1999)[0]
    record = page["records"][0]
    assert len(calls) == 2
    assert record["transaction_id"] == "9007199254740993"
    assert record["amount"] == "-0.123456789012345678" and record["asset"] == "USDC"
    assert record["symbol"] == "" and record["event_type"] == "cashflow"


def test_full_pages_are_bisected_without_dropping_same_millisecond_records(monkeypatch):
    monkeypatch.setattr(income, "PAGE_SIZE", 3)
    source = [row(1000, 1), row(1000, 2), row(1001, 3), row(1002, 4)]
    calls = []

    def get(account, opener, first, last):
        calls.append((first, last))
        # Deliberately return in descending order: ordering is not a cursor contract.
        return [item for item in reversed(source) if first <= item["time"] <= last][:3]

    monkeypatch.setattr(income, "_income_page", get)
    pages = collect(1000, 1003)
    assert (1000, 1003) in calls
    assert {item["transaction_id"] for page in pages for item in page["records"]} == {"1", "2", "3", "4"}
    assert [(page["start_ms"], page["end_ms"]) for page in pages] == [
        (1000, 1000), (1001, 1001), (1002, 1003)
    ]


def test_saturated_millisecond_and_duplicate_identity_are_not_complete(monkeypatch):
    monkeypatch.setattr(income, "PAGE_SIZE", 2)
    monkeypatch.setattr(income, "_income_page", lambda *args: [row(1000, 1), row(1000, 2)])
    with pytest.raises(QueryError, match="同一毫秒"):
        collect(1000, 1000)
    monkeypatch.setattr(income, "_income_page", lambda *args: [row(1000, 1), row(1000, 1)])
    with pytest.raises(QueryError, match="重复身份"):
        collect(1000, 1000)


def test_empty_windows_are_explicit_and_backfill_is_bounded(monkeypatch):
    calls = []

    def get(account, opener, first, last):
        calls.append((first, last))
        return []

    monkeypatch.setattr(income, "_income_page", get)
    end = 1000 + 8 * income.DAY_MS
    pages = collect(1000, end)
    assert len(pages) == 2
    assert calls[0][1] + 1 == calls[1][0] and calls[-1][1] == end
    assert all(page["records"] == [] for page in pages)


@pytest.mark.parametrize("values", [
    {"income": "NaN"}, {"income": "Infinity"}, {"income": 1.2}, {"time": True},
    {"time": 999}, {"time": 2000}, {"tranId": 1.5}, {"tranId": True},
    {"asset": ""}, {"asset": "US DT"}, {"incomeType": ""},
])
def test_invalid_rows_fail_closed(values):
    with pytest.raises(QueryError, match="未确认覆盖"):
        income.income_observation(row(**values), account(), 1000, 1999)


def test_unknown_income_types_and_zero_values_are_preserved():
    record = income.income_observation(row(incomeType="FUTURE_NEW_TYPE", income="0"), account(), 1000, 1999)
    assert record["income_type"] == "FUTURE_NEW_TYPE" and record["amount"] == "0"


@pytest.mark.parametrize("status", [302, 401, 403, 418])
def test_http_errors_are_sanitized_and_permanent_errors_not_retried(monkeypatch, status):
    calls = []

    class Opener:
        def open(self, request, timeout):
            calls.append(request)
            raise HTTPError("https://private/?signature=fixture-secret", status,
                            "fixture-secret", {}, None)

    monkeypatch.setattr(income, "REQUEST_INTERVAL", 0)
    monkeypatch.setattr(income, "build_opener", lambda *args: Opener())
    with pytest.raises(QueryError) as error:
        collect(1000, 1999)
    assert "fixture-secret" not in str(error.value) and "signature" not in str(error.value)
    assert len(calls) == 1
    assert income._NoRedirect().redirect_request(None, None, 302, "", {}, "https://other/") is None


def test_transient_get_failure_retries_and_invalid_account_never_sends(monkeypatch):
    calls = []

    class Opener:
        def open(self, request, timeout):
            calls.append(request)
            if len(calls) == 1:
                raise HTTPError(request.full_url, 429, "limited", {"Retry-After": "0"}, None)
            return io.BytesIO(json.dumps({"serverTime": 123456789} if request.full_url.endswith("time") else []).encode())

    monkeypatch.setattr(income.time, "sleep", lambda _: None)
    monkeypatch.setattr(income, "build_opener", lambda *args: Opener())
    assert collect(1000, 1999)[0]["records"] == [] and len(calls) == 3

    async def invalid():
        async for _ in income.income_history(account(environment="untrusted"), 1000, 1999):
            pytest.fail("Unsupported account cannot yield evidence")

    with pytest.raises(ValueError):
        asyncio.run(invalid())
    assert len(calls) == 3

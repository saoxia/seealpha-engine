"""Display-only USD-M asset evidence unavailable in the native AccountState.info."""

import hashlib
import hmac
import json
import re
import time
from decimal import Decimal
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .queries import QueryError

BASES = {"production": "https://fapi.binance.com", "sandbox": "https://testnet.binancefuture.com"}


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _read(opener, base, path, headers=None):
    with opener.open(Request(base + path, headers=headers or {}, method="GET"), timeout=8) as response:
        raw = response.read(2 * 1024 * 1024 + 1)
    if len(raw) > 2 * 1024 * 1024:
        raise ValueError("Oversize asset evidence")
    return json.loads(raw)


def asset_price(env, currency):
    """Return a timestamped USD index, never a futures contract/quantity-prefix price."""
    try:
        if not re.fullmatch(r"[A-Z0-9]{1,30}", currency):
            raise ValueError("Invalid asset")
        symbol = currency + "USD"
        row = _read(build_opener(_NoRedirect()), BASES[env],
                    "/fapi/v1/assetIndex?" + urlencode({"symbol": symbol}))
        price = Decimal(row["index"])
        stamp = row["time"]
        if (row["symbol"] != symbol or not price.is_finite() or price <= 0
                or type(stamp) is not int or abs(int(time.time() * 1000) - stamp) > 120_000):
            raise ValueError("Invalid asset price")
        return {"price": price, "priced_at": stamp // 1000}
    except Exception:  # noqa: BLE001 - Do not expose upstream payloads or URLs.
        raise QueryError("资产指数价格读取失败或已过期。") from None


def asset_account(account, *, positions=False):
    """Supplement native snapshots with mode and per-asset wallet/PnL for display only.

    Fixed official GET endpoints; no orders, transfers or configuration commands.
    The native public Python snapshot drops these per-asset fields. Valuation
    may also request raw read-only positions, avoiding an execution-node session
    which would share and delete listenKeys used by existing short-lived queries.
    """
    try:
        base = BASES[account["environment"]]
        credentials = account["credentials"]
        opener = build_opener(_NoRedirect())
        server = _read(opener, base, "/fapi/v1/time")
        if type(server["serverTime"]) is not int or server["serverTime"] <= 0:
            raise ValueError("Invalid server time")
        params = urlencode({"timestamp": server["serverTime"], "recvWindow": 5000})
        signature = hmac.new(credentials["secret"].encode(), params.encode(), hashlib.sha256).hexdigest()
        row = _read(opener, base, "/fapi/v2/account?" + params + "&signature=" + signature,
                    {"X-MBX-APIKEY": credentials["apiKey"]})
        if type(row["multiAssetsMargin"]) is not bool or not isinstance(row["assets"], list):
            raise ValueError("Invalid asset account")
        assets = {}
        for item in row["assets"]:
            currency = item["asset"]
            if not re.fullmatch(r"[A-Z0-9]{1,30}", currency) or currency in assets:
                raise ValueError("Invalid asset identity")
            values = {key: Decimal(item[key]) for key in ("walletBalance", "unrealizedProfit", "maintMargin")}
            if any(not value.is_finite() for value in values.values()) or values["maintMargin"] < 0:
                raise ValueError("Invalid asset value")
            if positions or any(values.values()):
                assets[currency] = values
        totals = {target: str(row[source]) for source, target in (
            ("totalWalletBalance", "total_wallet_balance"),
            ("totalUnrealizedProfit", "total_unrealized_profit"),
            ("totalMarginBalance", "total_margin_balance"),
            ("totalMaintMargin", "total_maint_margin"),
        )}
        if any(not Decimal(value).is_finite() for value in totals.values()):
            raise ValueError("Invalid account totals")
        result = {"multi_assets": row["multiAssetsMargin"], "assets": assets,
                  "totals": totals, "observed_at": int(time.time())}
        if positions:
            result["positions"] = []
            identities = set()
            for position in row["positions"]:
                quantity = Decimal(position["positionAmt"])
                if not quantity.is_finite():
                    raise ValueError("Invalid position amount")
                if quantity == 0:
                    continue
                symbol, side = position["symbol"], position["positionSide"]
                identity = f"{symbol}:{side}"
                entry = Decimal(position["entryPrice"])
                if (not re.fullmatch(r"[A-Z0-9]+(?:USDT|USDC)", symbol)
                        or side not in {"BOTH", "LONG", "SHORT"} or identity in identities
                        or side == "LONG" and quantity < 0 or side == "SHORT" and quantity > 0
                        or not entry.is_finite() or entry <= 0):
                    raise ValueError("Invalid valuation position")
                identities.add(identity)
                result["positions"].append({"id": identity, "instrument": symbol + "-PERP.BINANCE",
                                            "quantity": str(quantity), "entry": str(entry),
                                            "currency": "USDC" if symbol.endswith("USDC") else "USDT"})
        return result
    except Exception:  # noqa: BLE001 - Signed requests and credentials stay on the connector boundary.
        raise QueryError("资产明细读取失败，请检查合约读取权限、校时和网络。") from None

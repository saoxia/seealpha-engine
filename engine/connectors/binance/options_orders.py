"""Explicit order transport; writes are sent once, unknown outcomes stay unknown."""

import asyncio
import json
import time
from urllib.error import HTTPError
from urllib.request import Request

from . import options
from .options_account import READ_PARAMS, BinanceOptionsAccountClient, signed_request
from .queries import QueryError


class OptionsOrderError(QueryError):
    def __init__(self, *, code=None, unknown=True):
        self.code, self.unknown = code, unknown
        super().__init__("Binance 期权请求结果待核实" if unknown else "Binance 期权请求被拒绝")


class BinanceOptionsOrderTransport(BinanceOptionsAccountClient):
    """Separate from the read-only client. No write retries, including on 429/5xx."""

    def __repr__(self):
        return "BinanceOptionsOrderTransport(environment='production')"

    def _request(self, method, endpoint, params):
        if method == "GET":
            if endpoint not in READ_PARAMS or params.keys() - READ_PARAMS[endpoint]:
                raise ValueError("不支持的期权查询")
        elif method == "POST":
            if endpoint != "order" or set(params) != {"symbol", "side", "type", "quantity", "price", "timeInForce",
                                                      "postOnly", "reduceOnly", "clientOrderId", "newOrderRespType"}:
                raise ValueError("不支持的期权下单参数")
        elif method != "DELETE" or endpoint != "order" or set(params) != {"symbol", "clientOrderId"}:
            raise ValueError("不支持的期权撤单请求")
        sent = False
        with self._lock:
            try:
                time.sleep(max(0, self._last_request + .3 - time.monotonic()))
                self._last_request = time.monotonic()
                before = time.monotonic()
                with self._opener.open(Request(options.REST_URL + "time"), timeout=5) as response:
                    server = json.loads(response.read(4096))
                if type(server["serverTime"]) is not int or time.monotonic() - before > 2:
                    raise ValueError("校时无效")
                sent = True
                value = signed_request(self._opener, self._key, self._secret, method, endpoint,
                                       params, server["serverTime"], timeout=5)
                if isinstance(value, dict) and "code" in value:
                    code = value["code"] if type(value["code"]) is int else None
                    raise OptionsOrderError(code=code, unknown=code not in {-1013, -1100, -1102, -1111, -2014, -2015})
                return value
            except HTTPError as exc:
                try:
                    code = json.loads(exc.read(4096)).get("code")
                    code = code if type(code) is int else None
                except (ValueError, TypeError, AttributeError):
                    code = None
                definitive = exc.code in {400, 401, 403} and code in {-1013, -1100, -1102, -1111, -2014, -2015}
                raise OptionsOrderError(code=code, unknown=sent and not definitive) from None
            except OptionsOrderError:
                raise
            except (OSError, TimeoutError, ValueError, KeyError, TypeError):
                raise OptionsOrderError(unknown=sent) from None

    def _get(self, endpoint, **params):
        return self._request("GET", endpoint, params)

    async def send(self, method, endpoint, **params):
        # A cancelled await cannot cancel an urllib worker that may already be
        # sending an order. Join it and retain its result before node disposal.
        worker = asyncio.create_task(asyncio.to_thread(self._request, method, endpoint, params))
        try:
            return await asyncio.shield(worker)
        except asyncio.CancelledError:
            return await worker

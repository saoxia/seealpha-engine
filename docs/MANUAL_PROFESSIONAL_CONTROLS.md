# Manual USD-M Algo and Leverage Controls

Engine owns signing and exchange I/O. Platform uses the authenticated Engine v2 API; frontends never obtain keys.

## Native commands

`POST /tenants/{tenant}/accounts/{account}/trading/controls` accepts body `{environment, api_key, api_secret, action, values}` with actions:

- `algo_submit`: `AlgoOrderValues`, exchange-hosted USD-M `STOP`, `STOP_MARKET`, `TAKE_PROFIT`, `TAKE_PROFIT_MARKET` or `TRAILING_STOP_MARKET`. Only a reduce-risk exit against the **actual currently open** native position is accepted. Instrument quantity and price increments are checked exactly, never rounded; hedge/oneway mode must match expected version. On Binance the commands use signed `/fapi/v1/algoOrder` rather than the ordinary order endpoint.
- `algo_cancel`: `AlgoCancelValues` with exact `client_algo_id`; query the existing open Algo order before signing DELETE.
- `leverage`: `LeverageChangeValues` for a 1–125 integer; query leverage brackets first, send signed POST to `/fapi/v1/leverage` and query `/fapi/v1/symbolConfig` to verify the result. The command does **not** change margin/position mode.

All commands retain Platform account revision, Engine durable UUID reservation, tenant/production gate and per-account file serialization. Unknown exchange sends stay `unknown` until explicitly queried through the same `/requests/{uuid}/reconcile` flow, and are never retried automatically. A definitive validation/venue rejection is saved as a completed failure, **not** an accepted order.

Futures manual state returns `algo_orders`, `algo_checked_at` and `algo_error`. Missing permissions/network must be represented as `algo_orders: null`, not empty orders. The snapshot includes bounded recent exchange history but does not promise complete historical coverage.

## Research target provenance

Development backtests now store `target_events` per issued strategy with UTC event timestamp, source issuance, strategy id, symbol and target signed quantity. These events are historical target intentions, not fills, live signals or executable orders. Platform may read them for a user-authorized display overlay.

## Offline validation

```bash
uv run ruff check .
uv run pytest -q tests/connectors/binance/manual_algo_test.py tests/connectors/binance/manual_test.py tests/engine/backtest_test.py
```

Do not place real trading keys in tests. Native exchange requests must be verified with mock local endpoints; sandbox or live integration requires separate account controls and explicit acceptance.

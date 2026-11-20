# Rust Options fills / signed settlement ledger migration (2026-11-21)

## Implemented increment

- Options `GET /eapi/v1/userTrades` page-guarded signed reader, with exchange execution ID, order ID, Decimal quantities, fee sign, symbol, asset, side and timestamp checks.
- `query_order_trades` now routes Options through this reader. The Runtime verified-fill path no longer rejects Options categorically; it still fails closed on inconsistent cumulative quantities.
- Option `order_view` now reads `quantity` (rather than only Spot/UM `origQty`) and recognizes `ACCEPTED` as nonterminal.
- New SQLite `account_settlements` with tenant/account/environment/product/kind/event ID composite key, transactional deduplication and rejection on conflicting evidence.
- `sync_registered_settlements` reads already registered in-memory credentials, fetches signed USD-M funding income or Options account exercise records, and imports them without automatic wallet posting.
- Offline unit test fixtures for signed option trade conversion and idempotent settlement evidence.

## Not production-verified

- Options trade pagination has a fixed 32-page safety bound; if history cannot be proven complete the operation returns Unconfirmed. Exchange documentation limits historical trades to the last three months. No real-key test run has occurred.
- Complete cash accounting, correction/reversal rules, Spot conditional/OCO, USD-M Algo submit/restart recovery, multi-product streaming, historical gap reconciliation and HTTP v2 routing are not completed.
- Cargo fmt/clippy/test/release build are not run in this GitHub-only editing environment. CI remains paused; do not remove Python implementations or activate live trading based on this incremental work.

Source endpoints verified against Binance developer API docs (Nov 2026): `/eapi/v1/userTrades`, `/eapi/v1/exerciseRecord`, `/fapi/v1/income`.

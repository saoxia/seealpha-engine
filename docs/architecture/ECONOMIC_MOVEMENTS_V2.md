# Economic Movements V2 — isolated migration lane

## Scope

This new model is additive. Legacy `fills`, `account_settlements`, `account_evidence_events` and their consumers are left unchanged until replacement acceptance. The V2 tables are created on opening an existing ledger; no old rows are rewritten.

- `movement_raw_events_v2`: source payload, scoped by tenant/account/environment/venue/source_type/source_id.
- `economic_movements_v2`: normalized economic event and optional `native_event_id` / `instrument_id`, with exact decimal-string effects.
- An append happens in one SQLite IMMEDIATE transaction. Identity collisions with different raw or normalized content fail atomically.
- Paginated read is scoped to tenant/account/environment.
- No float amounts, synthetic fills, inferred balance deltas, or double-entry posting.

## Trust and Nautilus ownership

**Important:** V2 ingestion is a persistence boundary, **not an exchange authentication API**. Callers cannot mark a record verified; every new movement has `verification: pending`. The V2 API must not be used as a production balance/position or verified PnL source until an authenticated exchange adapter or reconciler validates provenance and completeness.

NautilusTrader's native `OrderFilled`, instrument, position, account, execution lifecycle and reconciliation remain authoritative for runtime trading. V2 only stores an optional native event ID and immutable exchange evidence reference. It does not reconstruct or independently replay Nautilus orders/positions. A future trusted adapter should map authenticated exchange fills to their native event identities without synthesizing fills from order ACKs or unverified reports.

For compound source transactions, `effects` can express multiple assets and fee components; effects are descriptions of reported changes and do not automatically post or update account balances. Missing effect values are not inferred. Funding, trade, option exercise, transfers and deposits/withdrawals require per-source canonicalization, verified event identity and exchange-specific tests before promotion out of pending.

## Follow-on acceptance before cutover

1. Wire version-pinned Nautilus native order/fill/account event references through a tested adapter.
2. Implement authenticated exchange evidence adapters, backfills and pagination/completeness watermarks.
3. Add status promotion requiring evidence validation and explicit versioned provenance.
4. Compare V2 events against old projections and trusted exchange account snapshots over representative historical and restart windows.
5. Migrate read consumers and only then remove legacy paths.

The current PR introduces durable V2 storage and validation, **not** complete replacement or production source verification.

## Ownership update: private account events are a Pipeline process

Venue-specific extraction/normalization belongs in `engine/pipeline/account_events/` (or per-venue source adapters under that pipeline). The Binance implementation is not part of Ledger. Its current functions `binance_usdm_funding` and `binance_account_fill` normalize supplied private-account evidence into the shared `MovementInput`; they do not establish HTTP authentication.

`ledger/movements` retains only venue-neutral append/query/idempotency and immutable evidence storage. `account_events.rs` is legacy and will be retired after consumer migration; the future generic account-event feed should be derived from canonical movements rather than reconstructed independently in Ledger.

This new private pipeline must not change the separately frozen **public market-data** ingestion scope or route private trades into the public Tick/Bar catalog. Connection-layer signing and native Nautilus execution remain separate from this account-event normalization process.

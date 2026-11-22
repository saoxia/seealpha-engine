# Nautilus-native account events: capability audit and implementation boundary

Status: **initial audit / no production integration** (2026-11-22).
Target: SeeAlpha native Rust, Nautilus crates pinned to **0.68.0** (NautilusTrader 2.0.0rc9).

## Decision

Use Nautilus-native execution/account/position lifecycle and native cache persistence **before** creating an alternative SeeAlpha account event model or storage layer. The superseded experimental PR #191 was closed without merge. This branch starts from main and does not inherit its V2 schemas, Binance account-event crate or custom `MovementInput`.

## Grounded repository findings

| Capability | Present in SeeAlpha now | Implication |
| --- | --- | --- |
| Binance native market data | `engine/pipeline/sources/shared/nautilus/src/main.rs`: read-only `LiveNode`, `BinanceDataClientFactory`; `TradeSubscriber` uses public `TradeTick` | Public TradeTicks **must not** be treated as authenticated account fills |
| Binance native execution | The same file has an offline `BinanceExecutionClientFactory::create` contract test for Spot and USD-M | This validates construction and OMS type only; **no live execution client** is attached to the node |
| Native account/event model | Nautilus provides `AccountState`, native order events including `OrderFilled`, and cache account history | Use these types for runtime trading facts; **do not reconstruct native fills from order ACKs** |
| Native account-event persistence | Nautilus cache `persist_account_events` option and cache backends are documented | Verify exact pinned API, configured backing store, restart restoration, and error behavior before enabling in the engine |
| Current SeeAlpha Ledger | `engine/ledger/movements` stores verified fills, funding/option evidence and paginatable evidence references | Legacy APIs are **left untouched** until consuming services migrate and replacement is tested |
| Binance Options | `engine/connection/binance-options` normalizes private option evidence; current native market-data node is only Spot / USD-M | Do not assert native Options account integration |
| Binance private income, transfers, exercise | No native end-to-end integration has been demonstrated in this repository | Funding **rate** and account **income** are different; determine native support per record category before writing adapters |

## Proposed minimal implementation, in order

1. **Native-only offline acceptance test** under the standalone Nautilus crate for the pinned version: configure the native Binance execution factory, verify native `AccountState` and `OrderFilled` event transport and persistence interfaces without network or real keys. This is a gate; failure must not be bypassed by creating synthetic event types.
2. Configure the existing native execution node and cache in a dedicated opt-in, sandbox/offline path **only after** confirming native adapter recovery behavior. Do not silently convert the current public read-only data node to a trading client.
3. Test restart/replay and account-scoped isolation using fixtures, including failed storage and out-of-order reports. Account snapshots are not proof of individual income transfers.
4. Compare coverage of authoritative Binance account-income, deposit/withdrawal, transfer and option-exercise records. Only uncovered, authenticated **economic** records merit a small venue-specific collector. Prefer a native adapter extension or narrowly scoped pipeline; no second OMS, portfolio, or general movement store.
5. Demonstrate consumer parity and reconciliation first; then remove equivalent legacy ledger methods/tables in a separate deliberate cleanup, preserving historical evidence.

## Explicit exclusions

- No V2 movement schema, no `pipeline/binance_account_events` crate until a documented native gap requires it.
- No invented cash postings from `AccountState` diffs or public funding rates.
- No live trading, credentials, balance modifications, schema migration, production-volume deletion, or automatic removal of existing evidence.
- No claim that a documented API option is already configured or tested in SeeAlpha.

## Review evidence

Repository:
- `engine/pipeline/sources/shared/nautilus/Cargo.toml` — version pin and standalone workspace exclusion.
- `engine/pipeline/sources/shared/nautilus/src/main.rs` — current `LiveNode` composition and offline execution-factory test.
- `engine/ledger/README.md` — current ownership and legacy calls.

Nautilus references (latest docs are *capability signals*, not proof of pinned code/runtime integration):
- https://nautilustrader.io/docs/latest/concepts/cache/
- https://nautilustrader.io/docs/latest/concepts/events/
- https://nautilustrader.io/docs/latest/how_to/configure_live_trading/
- https://github.com/nautechsystems/nautilus_trader/issues/4893 — bounded account funding income coverage discussion; verify release/implementation before depending on it.

## Definition of done

Native account event receipt, durable storage, source-specific provenance, restart recovery, tenant/account isolation and reconciliation must be demonstrated with pinned-version integration tests. Any unsupported economic record type must be listed as a gap, not fabricated from an account snapshot. Existing safety gates and legacy consumers remain in effect until demonstrated cutover.

## Agreed implementation decision (2026-11-22)

Proceed **native-first and native-only for supported account events**: use pinned Nautilus Binance execution/client reporting, native `AccountState` / `OrderFilled` / order and position engines, and native cache-backed persistence/recovery. Do not add a parallel account-event collector, generic movement DTO, second database, or duplicate reconciliation engine merely for completeness of architecture.

History is limited to what the pinned native adapter and exchange APIs can genuinely retrieve; a restored account snapshot is not a reconstructed economic ledger. For any missing funding-income, deposit, withdrawal, transfer, or option-exercise history, record the verified capability gap and leave it unimplemented unless explicitly prioritized. Do not synthesize transactions from balance differences.

Acceptance remains gated on offline/restart tests with pinned 0.68.0 and confirmation of safe execution-client integration; existing legacy consumers and safety gates remain untouched until native parity is demonstrated.

## PostgreSQL Cache implementation (native Nautilus 0.68.0, opt-in)

The current PR now **wires the native Postgres Cache factory to the standalone
Binance Testnet account observer**. This is not production account migration.

### Actual native API integration

`engine/pipeline/sources/shared/nautilus/Cargo.toml` enables
`nautilus-infrastructure = "=0.68.0"` with `postgres` feature. The
`native_account_observer` binary builds a native `PostgresCacheConfig`,
sets `CacheConfig { persist_account_events: true, flush_on_start: false, .. }`,
and calls `LiveNodeBuilder::with_cache_database_factory(Box::new(postgres))`.
This reuses Nautilus's native account event, order and position Cache machinery
without a SeeAlpha-specific SQL event writer.

Credentials are read from the container's local
`SEEALPHA_POSTGRES_CACHE_PASSWORD_FILE` (never a command-line password);
the database endpoint is fixed at `127.0.0.1:5432`, database/user
`seealpha_cache`. Enabling the native backing is explicit:
`SEEALPHA_ENABLE_POSTGRES_CACHE=1`. No live account is connected by default.

### Single-container database and schema

`docker/engine.Dockerfile` now compiles and includes
`native_account_observer` and PostgreSQL 15. Its entrypoint starts a
**container-local** PostgreSQL server and the existing Engine, without Compose
or an extra DB container. PostgreSQL's data directory is
`/app/.local/state/postgresql`; the generated first-boot password is stored
alongside the data in `/app/.local/state/.postgres-password`.
The directory and credentials must be backed up together.

The image includes the four upstream `v2.0.0rc9` SQL files from
`nautechsystems/nautilus_trader/schema/sql` under
`docker/nautilus-postgres-schema`. They are imported in the official order:
`types.sql`, `functions.sql`, `partitions.sql`, `tables.sql`. They
remain NautilusTrader's upstream SQL, not a forked SeeAlpha schema
(LGPL-3.0 source project; keep provenance when upgrading). Bootstrap is only
attempted on an empty schema; partial or unknown schemas fail closed rather
than being deleted or implicitly migrated.

The existing SQLite Ledger remains completely separate and unchanged.
Persist both via:

```sh
docker build -f docker/engine.Dockerfile -t seealpha-engine:dev .
docker run --name seealpha-engine -v seealpha-state:/app/.local/state seealpha-engine:dev
```

### Explicit testnet-only account observation

Only after offline validation, enable the **read-only Binance Testnet** account
observer (no trading commands or strategies) with all required environment
variables:

```sh
docker run --name seealpha-account-test \
  -v seealpha-test-state:/app/.local/state \
  -e SEEALPHA_NATIVE_ACCOUNT_OBSERVER=1 \
  -e SEEALPHA_ENABLE_POSTGRES_CACHE=1 \
  -e SEEALPHA_ACCOUNT_PRODUCT=spot \
  -e SEEALPHA_ACCOUNT_ID=BINANCE-SPOT-TEST-001 \
  -e SEEALPHA_BINANCE_TESTNET_KEY=... \
  -e SEEALPHA_BINANCE_TESTNET_SECRET=... \
  seealpha-engine:dev
```

For credentials, use a secret-injection facility rather than shell command
history; the inline environment examples show names, not recommended secret
transport. The observer only accepts `spot` or `futures_um` Testnet
execution clients, with explicit account identity. The same-container
supervisor shuts down the other processes if any supervised service exits.

### Verification gates and limitations

The integration is **code-complete only at the configuration/wiring level**.
It has not yet passed Docker build, pinned-version `cargo check`, first-boot
Nautilus SQL migration, authenticated Testnet account event write, restart
restore, or fault injection. In particular:
- `AccountState` persistence does not prove that private funding income,
  withdrawals or option exercise history are captured.
- The observer's local recovery cursor is **not** Nautilus's account Cache and
  should be removed only after verified native recovery parity.
- A PostgreSQL health check is not proof of successful Nautilus SQL writes.
- PostgreSQL WAL and asynchronous Cache task processing have different
  durability semantics; test clean shutdown and crash recovery independently.
- Production should review data ownership, secret injection, backup, least
  privilege and resource limits. No real orders or account balances are changed.

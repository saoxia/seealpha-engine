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

## Deployment decision: PostgreSQL in the existing Engine container

The Docker image installs PostgreSQL 15 and its client tools instead of Redis.
`docker/entrypoint-with-postgres.sh` runs PostgreSQL and SeeAlpha Engine under one container supervisor.
PostgreSQL binds only to `127.0.0.1:5432`, with no published database port. The bootstrap creates
a non-superuser `seealpha_cache` login and matching database; the password is generated on the first
startup and kept in a mode-0600 file under the state volume, not compiled into the image.

The database files are stored at `/app/.local/state/postgresql`. Mount
`/app/.local/state` as a persistent volume so that existing SQLite and PostgreSQL survive replacement:

```bash
docker run --name seealpha-engine -v seealpha-state:/app/.local/state seealpha-engine:dev
```

The bootstrap refuses to start an existing PostgreSQL data directory without its credential file,
or to start a cluster with a mismatched major version. An interrupted initial bootstrap requires
manual recovery rather than destructive automatic reinitialization. Readiness is checked using
`pg_isready` before the Engine starts. If either process terminates the supervisor stops the other.

The `SEEALPHA_POSTGRES_URL` environment variable is only a **local endpoint hint** for the
future native Nautilus cache adapter. It does not configure the native cache, install Nautilus
into this image, or prove account events are persisted. The native account observer currently
runs as a separate standalone Rust program; it must be co-located before using this
container-loopback-only database. The underlying Nautilus 0.68.0 `postgres` feature
and actual cache schema/bootstrap path still require pinned-version compile/runtime and
restart validation.

Security and durability: PostgreSQL uses WAL, but application-level asynchronous cache writes
still require explicit flush/restart tests. Back up the PostgreSQL database and generated
credential together; normal snapshots of an actively running database must use database-aware
backup procedures. Production deployment should use a reviewed non-root supervisor and
resource/memory limits. This PR does not enable live trading or modify old SQLite ledger data.

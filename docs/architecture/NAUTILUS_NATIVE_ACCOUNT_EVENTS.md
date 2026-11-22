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

## Deployment decision: Redis inside the existing Engine container

The Engine image installs Debian's `redis-server` and `redis-tools`; `docker/entrypoint-with-redis.sh` supervises Redis and SeeAlpha Engine in the **same container**, rather than adding a Compose service. Redis binds only `127.0.0.1:6379` and is not exposed by Docker. An internal `SEEALPHA_REDIS_URL=redis://127.0.0.1:6379` is exported for future native Nautilus cache configuration; **this does not mean the native cache has been wired yet**.

Persistence: Redis `appendonly yes`, `appendfsync everysec`, periodic snapshot settings, and data directory `/app/.local/state/redis`. Mount `/app/.local/state` on a persistent host volume to retain both existing SQLite and Redis data across container replacement, e.g.:

```bash
docker run --name seealpha-engine \
  -v seealpha-state:/app/.local/state \
  seealpha-engine:dev
```

The supervisor checks for Redis PONG before starting Engine and terminates the sibling process when either stops. The Redis connection is available **only within this container**; an external native Nautilus process cannot use `127.0.0.1` to reach it. If the Nautilus native account observer remains an external, separately launched binary, it must be explicitly packaged/routed into the container before this local Redis arrangement can serve it. No Redis endpoint should be exposed on a public interface merely to work around that gap.

Caveats: AOF `everysec` is not synchronous per-event durability; power loss can lose the latest writes. Configure credentials, non-root operation and backup/restore policies before production approval. Do not treat Redis Cache contents as complete long-term economic settlement history. The current PR only provisions the service; it does **not** assert successful Nautilus Redis backend activation or restart-tested persistence.

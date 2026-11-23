# Strategy / Research Rust migration: parity matrix and integration contract

Baseline: `main@83a19d857bca0ec16faf1a731ccb9835081672ab` (2026-11-21). This change is limited to
`crates/seealpha-strategy/` and `crates/seealpha-research/`. The retained
`python/strategy_host/host.py` is intentional; it is not replaced by user-code compilation.

## Verified source comparison (implementation status, not test pass)

| Python reference | Existing Rust at baseline | This branch | Outstanding |
|---|---|---|---|
| `engine/strategies/package.py`: manifest, version, SHA-256 | SHA-256, basic manifest checks | Validate title/description/defaults, reject duplicate ZIP paths, traversal, Unix symlinks, missing entry, unsupported members, oversized decompression and removed imports | Full parameter preparation continues in isolated Python; archive-vs-directory digest format remains different by design |
| `engine/strategies/loading.py`: issued identity / tenant / account | Runtime compared some manifest fields on load | `authorize_issued` binds *trusted issuance* to tenant, account, entry, name, version and exact package hash; `run_issued_host` stages those verified bytes | AI10 must use this API with Ledger records for every issued execution. This crate cannot validate Platform ownership by itself |
| `python/strategy_host/host.py`: prepare and target publications | Spawn, 30-second wait; unbounded stdout and blocked pipes | Bounded input (1 MiB), stdout (1 MiB), max 5,000 bars, timeout with kill/reap, concurrent pipe processing, restricted interpreter environment, typed response, no extra output command fields | OS-level CPU/RAM/process count, UID, seccomp, filesystem/network isolation must be enforced by Runtime/deployment. `-I` and environment clearing are **not** a strong sandbox |
| `engine/strategies/package.py`: Decimal targets | Host output strings | Parse exact bounded fixed-point Decimal with no binary floating point; validate authorized symbol, bar index, prepare-only mode and finite quantities | Rust validation deliberately rejects precision above 28 digits; Python may accept some wider values. No execution or accounting occurs here |
| `engine/research/auth.py`: grant format/HMAC/TTL | Python compatible HMAC and operation allowlist | Reject overflowing time subtraction; `authorize_operation` binds grant to trusted tenant/task/resource | Platform still issues grants. Engine MUST verify HMAC and expiry on every request, not cache a stale `Grant` indefinitely |
| `engine/research/snapshots.py`: snapshot isolation/readiness | No Rust snapshot artifact validation | `authorize_snapshot_request` checks 31-day UTC-nanosecond windows; `authorize_snapshot` checks dataset/task/tenant, valid states and complete journal evidence; `verify_snapshot_bytes` checks ready, size, Parquet sentinels, SHA-256 | AI10 owns queue, atomic persistence/restart, Catalog/Parquet export, no-symlink file opening; parser does not assert true Catalog completeness |
| `engine/research/candidate_package.py`, `python_candidate.py`: candidate scopes and hashes | Grant has strategy list | `authorize_candidate_envelope` checks operation, dataset, account ref, candidate ID and archive SHA-256 | Restricted candidate Python AST and full Python static ZIP inspection are **not** replaced; invoke the validated parser before queue dispatch. Unrestricted Python requires explicit grant operation and strong isolation before exposure |
| `engine/research/mcp.py`, `engine/research_mcp.py` | Basic Research HTTP | No route modifications | AI9 owns HTTP/MCP mapping, status codes, protocol versions and tool discovery |
| Research backtests / snapshot task scheduling | Only limited native handlers | No task scheduler modifications | AI10 owns durable jobs, idempotency, cancellation, recovery and Runtime integration |

## Contract for AI10: issued strategies

`inspect_archive(bytes)` returns `PackageMetadata {name, version, entry_point, digest}`.
`authorize_issued(IssuedStrategy, AccountScope, &metadata)` compares the **Ledger-issued**
record (not fields supplied by the untrusted strategy) with a trusted request scope.
For host execution use:

```rust
let issued = seealpha_strategy::IssuedStrategy {
    tenant_id: load.tenant_id.as_str(),
    account_id: load.account_id.as_str(),
    issuance_id: load.issuance_id.as_str(),
    code_key: load.code_key.as_str(),
    version: load.version.as_str(),
    entry_point: load.entry_point.as_str(),
    package_hash: load.package_hash.as_str(),
};
let caller = seealpha_strategy::AccountScope {
    tenant_id: trusted_tenant,
    account_id: trusted_account,
};
// The host command points to an existing issued ZIP; no directory is accepted.
let run = seealpha_strategy::run_issued_host(&command, issued, caller)?;
```

`run_host` remains a lower-level legacy/sample interface and must not be interpreted
as authorization. It now validates and pins bytes as well. The host publishes only
`Publication {symbol, quantity, bar_index}` targets. It returns **neither** fills,
orders, trading permissions, nor market-data configuration. A target is only an
intent; pass it through Portfolio risk/netting and the Ledger order process.
Do not attach credentials or administrative API tokens to the Python process.

## Contract for AI9 and AI10: Research

1. `verify_research_grant(key, token, now)`: HMAC, strict claim schema and TTL; reject as authentication failure.
2. Construct `ResearchContext {tenant_id, task_id}` from a trusted Engine tenant and task registry. `authorize_operation(&grant, context, op, dataset, strategy)` adds task and tenant binding; map `ResearchError::Scope` to scope denial.
3. For candidate submissions, additionally invoke `authorize_candidate_envelope`, **then** the existing static restricted candidate parser or Python candidate inspector. This function alone cannot authorize arbitrary Python execution or interpret ZIP/AST.
4. Before queueing a snapshot, use `authorize_snapshot_request` with parsed UTC epoch nanoseconds, then for snapshot metadata use `authorize_snapshot`, which rejects foreign tasks, datasets and unknown states. For downloads use `verify_snapshot_bytes` on the exact opened bytes and do not follow symlinks. Never report `pending`, `running`, `failed` or `interrupted` as ready.

Research grants do not authorize account trading, configuration changes or strategy
publication. Research candidate operations are granted individually; tenant type does not grant or deny them. The
signing key stays in trusted Platform/Engine verification infrastructure, not
in Agent Shell, uploaded archives, or Python host input.

## Offline test cases added

- Strategy: ZIP path traversal, duplicate files, member limits, missing entry,
  version/format failures, exact digest and removed imports.
- Strategy: mismatched issuance tenant/account/version/hash/entry;
  direct-order operation rejection, wrong symbol/bar index, invalid Decimal and
  secret-bearing parameter rejection; oversized protocol input.
- Research: Python-signed Grant vector, tampering/duplicate/invalid operation,
  signed integer overflow and TTL boundary; cross-tenant/task scopes;
  snapshot unready, unknown status and wrong bytes; candidate scope/hash.

Run from repository root:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --locked -p seealpha-strategy -p seealpha-research
cargo test --workspace --locked
```

**Test execution status:** NOT RUN. The current execution container has no
`cargo`, `rustc` or `rustfmt`; `.github/workflows-paused/` is intentionally disabled
in the source repository. No CI result or Rust compilation success is claimed.
The workspace `Cargo.lock` and files outside the two owned crates are untouched.
Before merge, AI9/AI10 should run the commands above and Python/Rust integration
fixtures in an environment with Rust installed, record actual results, and
resolve any differences. No live keys, orders, account state, historical raw
data or production database were accessed.

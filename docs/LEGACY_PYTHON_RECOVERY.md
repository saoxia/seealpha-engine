# Python Engine recovery during Rust migration

## Why this exists

The Rust runtime switch at commit `da0a5ff` removed Python implementations
before end-to-end native parity had been demonstrated. The recovery baseline
is `f18e48e995becc0662df288058be6059c91c45b0`, the last Python-runtime
commit before that switch (the earlier Rust helper crates already existed).

This restoration brings back **all 256 tracked files missing from that
baseline**, including Python implementations, regression tests, source
manifests, and old Rust helper programs. The baseline `pyproject.toml` and
matching `uv.lock` are restored together. The current native `crates/`
workspace and default Rust Dockerfile are preserved.

Restoration is **not** evidence that the legacy and native runtimes can share
one running account, database, writer, state directory or market-data task.

## Two explicit entrypoints

| Entry point | Purpose | Command |
| --- | --- | --- |
| Rust (default) | Migration target / current default image | `cargo run --locked --release -p seealpha-engine` |
| Legacy Python (opt-in) | Restore functionality not yet verified in Rust | `uv run python -m engine` |

Legacy Python dependencies require Python 3.15 and the checked-in uv lock.
From the repository root:

```bash
uv sync --locked
mkdir -p .local/legacy-project/data .local/state/legacy-engine
ENGINE_PROJECT="$(pwd)/.local/legacy-project" \
ENGINE_STATE="$(pwd)/.local/state/legacy-engine" \
ENGINE_HOST=127.0.0.1 ENGINE_PORT=18767 \
ENGINE_AUTH_MODE=none \
uv run python -m engine
```

This local command is only for isolated development data. The old engine
assumes its own data-root layout; copy fixtures to the independent project's
`data/` directory, not to a directory used by a running Rust instance.

### Separate Docker image

```bash
docker build -f docker/engine-legacy.Dockerfile -t seealpha-engine:legacy .
export ENGINE_TOKEN="$(openssl rand -hex 32)"
docker run --rm --name seealpha-legacy \
  -p 127.0.0.1:18767:8766 \
  -e ENGINE_AUTH_MODE=token -e ENGINE_TOKEN \
  -e ENGINE_TENANT_ID=42 -e ENGINE_TENANT_PURPOSE=development \
  seealpha-engine:legacy
```

The legacy image has a separate Dockerfile and `ENGINE_STATE` default.
It is not built or deployed as the primary `seealpha-engine` image. Do
**not** bind the same writable SQLite database, Catalog, collector directory,
lock directory, or exchange execution account into both runtimes. If
restoring a real existing environment, stop the other engine first, take
a verified backup, reconcile open/unknown orders, and verify the migration
of stored state before choosing one runtime. Never automatically retry an
unknown external order.

The Python runtime includes the original signed trading connectors. The
Docker image requires explicit token authentication by default, but that is
not a substitute for a production security and account-state review.
No production switch or live trading is authorized by this restoration.

## Verification gates

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
uv sync --locked
uv run pytest -q tests/engine tests/connectors tests/feed tests/pipeline tests/core
python3 -m unittest discover -s tests/strategy -t . -p '*_test.py' -v
```

The `Legacy Python parity` CI workflow tests the old Python suite and
builds/smokes the separate legacy image. The standard Rust CI continues to
verify the native workspace and limited Python strategy host. A restored
file or a same-named Rust unit test is **not** proof that the production
behavior is equivalent.

## Rust migration rule: retain until replaced

For each old domain, record its external routes, storage contracts, callbacks,
error semantics, tenant boundaries, idempotency, failure paths and test cases.
Use the legacy behavior as a **reference**, implement the Rust equivalent,
and run direct parity/integration tests on identical fixtures.

1. Data control, durable worker supervisor, collector configuration, source
   lifecycle, historical repairs, bounded retry and read-only subscriptions.
2. Strategy-market authorization, snapshot synchronization, strategy
   packages and target/portfolio constraints.
3. NautilusTrader history/Catalog backtests, funding and options evidence,
   issued and multi-product simulations, async task control and research API.
4. Account/order/position/fill ledger, reconciliation, partial fills, unknown
   results, account snapshots, derivatives controls and real venue adapters.
5. HTTP v2 routes, authentication, error shapes, tenant boundaries and
   Platform integration under both failure and success paths.

Known examples that still need native parity: current Rust `GET /jobs`
returns a static empty list; worker settings return constants; the old
Python API included data policies, subscriptions, worker lifecycle,
tenant export/import, positions and request lookup not all mounted in
Rust. The Rust backtest path does not reproduce every NautilusTrader
Catalog/strategy scenario. The old test mapping at
`docs/TEST_MAP_F18E48E.md` contains 519 old pytest names but is **not**
a verified behavior-parity certificate.

Do not delete a legacy module merely because a new Rust function or test
has the same name. Retire it only when the complete route/behavior/storage/
failure-path matrix passes, the replacement is integrated in the active
runtime, and the owner explicitly approves the cutover.

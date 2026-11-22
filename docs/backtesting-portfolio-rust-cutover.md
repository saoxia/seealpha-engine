# Backtesting and portfolio Python directory retirement

The native Rust owners are `backtest` (`seealpha-backtest`) and
`portfolio` (`seealpha-execution`). Their integration is through
`application/runtime`, `accounting/ledger`, and the native HTTP server.

The legacy directories `engine/backtesting` (8 files) and
`engine/execution` (10 files) were removed as the module-level retirement
unit. The migration intentionally does not claim Python/NautilusTrader
event-level parity. Rust backtests are offline simulations, not evidence
of live exchange fills.

| Former Python responsibility | Rust owner |
| --- | --- |
| historical window and bar replay | `seealpha-backtest::window`, `timeline`, `matching` |
| funding carry and protected options | `seealpha-backtest::timeline`, `matching`, `option_quotes` |
| strategy publication, target netting, virtual orders | `seealpha-execution::lib`, `coordinator` |
| basket planning, phase reconciliation | `seealpha-execution::basket`, `journal`, `phase_gate` |
| execution evidence, monitoring, settlement | `seealpha-execution::order_evidence`, `risk`, `settlement` |
| persistent scoped coordinator | `seealpha-runtime::portfolio` with ledger persistence |

**Cutover boundary:** the retired Python `engine.trading.runtime` and
`engine.research.basket_research_session` source files still import retired
modules and must not be used as entrypoints; the Python `engine.__main__`
already rejects HTTP startup. The active binary is
`cargo run --locked --release -p seealpha-engine`.

Future work can improve execution precision and behavior independently in
Rust. Preserve strict evidence/CAS and fail-closed rules for real order
commands; do not silently substitute paper fills for venue confirmations.

Verification required on a Rust-capable checkout:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test -p seealpha-backtest --locked
cargo test -p seealpha-execution --locked
cargo test --workspace --locked
```

These commands have not been run in the GitHub connector environment.

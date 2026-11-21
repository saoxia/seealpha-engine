# Legacy Engine Python retirement (2026-11-21)

This cleanup removes the dormant `engine/` package and obsolete Python market helpers (`core/market/`, `core/option_records.py`, `core/paths.py`). Rust crates already own the application-level responsibilities: `market_data/market`, `market_data/catalog`, `accounting/ledger`, `application/runtime`, `alpha/strategy`, `interfaces/api/http`.

Removed Python tests under `tests/engine/`, `tests/feed/`, `tests/pipeline/`, the obsolete Nautilus parity fixture script, historical Python benchmark runner, unused HTTP smoke script, `nautilus_support.py`, and the nonfunctional `legacy-python-parity.yml` workflow. Historical sources remain in Git history. The Rust tests are not asserted to cover every deleted legacy test case.

**Not a zero-Python migration:** `python/strategy_host/` and the `packages/ema_crossover` Python sample implement active user-authored strategy execution. The shared Platform v2 `core/engine_contracts.py` and `core/trading_contracts.py` remain. The Engine Docker image already excludes the legacy modules. Future strategy-language migration requires a distinct implementation and runtime/package contract review.

No cargo, Docker, or Python test execution was possible in the editing environment. Live orders are not authorized by this cleanup. Validation before merge: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, `cargo build --workspace --locked --release`, strategy/contract unit tests, and Docker image smoke.

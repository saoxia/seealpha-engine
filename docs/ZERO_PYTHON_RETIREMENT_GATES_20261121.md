# Zero-Python repository retirement gates (2026-11-21)

Status: **not ready to remove active Python**. This document records the remaining boundary and the acceptance gates for safe cleanup. It does not claim functional parity or successful test runs.

## Already retired

- Legacy `engine/` Python runtime, old Python market/option helpers, and their obsolete Engine/feed/pipeline tests.
- Default `docker/engine.Dockerfile` builds a Rust-only runtime image, without Python interpreter or `.py` files.
- Rust binary `seealpha-engine` is the only long-lived Engine server.

## Active Python dependencies (do not delete)

| Path | Active role | Blocking replacement |
| --- | --- | --- |
| `python/strategy_host/` | One-shot subprocess for issued user Python strategies; loads entry points and emits targets | Versioned non-Python strategy package/executor protocol with migration for issued packages |
| `packages/ema_crossover/` | Python sample exercising the supported strategy host | Native Rust sample and API/CI parity, plus clear deprecation of Python sample |
| `core/engine_contracts.py` | Platform-compatible v2 header, errors, and account snapshot contract | Canonical language-neutral contract and Platform migration |
| `core/trading_contracts.py` | Pydantic order/conditional order validation shared with Platform | Schema-based contract with strict validation parity, including invalid inputs |
| `tests/core/`, `tests/strategy/` | Python contract/strategy regression tests | Equivalent Rust and Platform contract/host migration tests |
| `docker/engine-python.Dockerfile` | Explicit opt-in Python strategy runtime image | No remaining issued strategy requiring Python execution |
| `.github/workflows/engine-ci.yml` | Runs Python tests and validates optional Python image | Replace these jobs only after their responsibilities have native coverage |

## Removal sequence

1. **Inventory and freeze compatibility:** enumerate issued package manifests and `.py` entry points, Platform clients of both `core/*contracts.py` files, and v2 error/response semantics. Record a supported upgrade/rollback procedure. Do not delete already-issued strategy blobs.
2. **Choose and implement the strategy runtime contract:** version package format and publication IPC separately; implement Rust-native execution for supported strategies (or an explicit external, language-independent plugin mechanism). Preserve quantity and weight publications, prepare/run paths, policy enforcement, idempotency, and failure behavior. Reject unsupported legacy packages explicitly.
3. **Migrate contract ownership:** create canonical versioned schema (such as JSON Schema/OpenAPI), generate or validate Rust and Platform bindings, and prove field, numeric and error-case parity. Update Platform consumers before removing Python contract sources.
4. **Run parity and security gates:** test valid/invalid archives, execution limits, sandboxing, risk clipping, paper replay, backtest results, account/conditional orders and Platform HTTP compatibility. Maintain fail-closed live execution. Verify no production-issued strategy still requires the Python host.
5. **Remove optional Python runtime and samples:** only then delete `python/strategy_host/`, `packages/ema_crossover/`, `docker/engine-python.Dockerfile`, unused Python tests/contracts, and their CI steps; remove obsolete README references. Do not remove documentation of historical migration evidence.
6. **Prove zero Python dependency:** check tracked source, build stages, deployment artifacts, workflows and Runtime configuration; run Rust workspace fmt/clippy/test/release build, Docker build and smoke tests, plus Platform end-to-end contract checks.

## Acceptance criteria

- No runtime code path invokes Python, relies on `ENGINE_PYTHON`, or requires a Python strategy entry point.
- No active build, CI, or deployment path requires Python tooling (Python in the GitHub-hosted runner by itself does not imply an Engine dependency).
- Issued packages have a documented compatibility strategy; incompatible packages are migrated or explicitly rejected with validated error semantics.
- Shared Platform contract inputs, outputs and rejection cases pass bidirectional integration tests.
- Default and release containers contain no Python interpreter, host, or `.py` files.
- Compilation, test, image and end-to-end results are recorded; **do not infer test success from this document**.

## Scope warning

Removing Python files will not itself complete Rust live-trading parity. The outstanding derivatives, history, data readiness, account/order lifecycle and exchange acceptance gates remain independently required. See `docs/RUST_CUTOVER_AUDIT_20261121.md` and `docs/ARCHITECTURE.md`.

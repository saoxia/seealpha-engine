# Strategy / 策略领域

Rust crate: `seealpha-strategy`.

## Current active responsibilities

- `store.rs`: immutable native manifest / ZIP issuance storage, PostgreSQL metadata index, archive integrity and restart recovery.
- `issued.rs` and `native_manifest.rs`: account-scoped issuance identity and exact SHA-256 verification.
- `native_issuance.rs`: package inspection, issuance, trusted read-only execution dispatch.
- `native_ema.rs`: **temporary** Rust EMA crossover implementation used by `/accounts/{account_id}/strategy-preview`. NautilusTrader already has EMA indicators and Rust strategy facilities; migrate only after a verified native integration.

The current HTTP inspection, issuance and preview paths require this crate. Do not delete it or the EMA evaluator before wiring an equivalent Nautilus runtime and validating signal / parameter parity.

## Retired legacy

The Python strategy host is not present in the repository and has no live runtime callers. Removed the unused Python host subprocess, IPC, old ZIP contract, and generic `StrategyExecutor` dispatch layer. Strategy deployment does **not** execute uploaded Python or Rust code; the native manifest selects the compiled-in EMA evaluator.

See [the Nautilus overlap review](../../docs/STRATEGY_NAUTILUS_OVERLAP_20261123.md).

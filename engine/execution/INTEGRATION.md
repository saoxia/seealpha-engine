# SeeAlpha Execution — native-first contract

> 2026-11-23 更新：Runtime 旧订单预登记/恢复实现及 Gateway 写入/对账 POST 路由已删除。Execution 只规划调整量，Nautilus 生产发送和独立发送前授权/认证恢复仍未接通；实盘继续禁用。旧 PostgreSQL 历史订单表保留。

The old standalone Coordinator, Basket order lifecycle, checkpoint journal,
phase gate, order evidence and risk monitor have been **deleted**, together
with their research-only Runtime endpoints and checkpoint storage adapters.
Those entry points are intentionally not kept as no-op compatibility stubs.

## Execution crate scope

`seealpha-execution::target` owns only:
- Validating strategy absolute quantity / weight target publications.
- Converting member weights using explicitly positive allocations.
- Aggregating signed member target quantities and enforcing quantity steps.
- Planning account-level adjustment *only after* the caller independently
  confirms a current Nautilus account position and reconciled working orders.

No venue order, Binance parser, paper fill, order status, self-managed Flight,
OMS event log, settlement ledger or streaming subscriber belongs in Execution.

## Nautilus-native facts

- `pipeline/sources/shared/nautilus`: typed `OrderEventAny`, `OrderCore`,
  `OrderFilled`, `AccountState` and native cache/streaming.
- `pipeline/sources/binance_account`: one thread-local native subscription
  and the bridge to immutable SeeAlpha strategy allocation.
- `native_virtual_attribution`: FIFO attribution of authenticated native
  incremental fills based solely on pre-registered member claims, the latest
  native `OrderCore`, and the previous *committed* cumulative quantity.
  It does **not** maintain another venue order book.
- The caller must CAS-persist prior/new cumulative quantity, native event/trade
  ID and per-member attribution as one transaction *before* publication.
  Duplicate/out-of-order/voided fills are blocked pending native reconciliation.
- Ledger owns economic movements and verified provenance; native ACKs are
  not fills and native event observations alone are not settlement proof.

## Related modules

- `connection/binance-options::settlement`: Options exchange exercise
  evidence normalization, not synthetic cash movements.

## Operational cutover

**Breaking removal:** Runtime's old `NativePortfolioEvent`,
`save_native_portfolio`, `reconcile_native_portfolio`, and Basket checkpoint
entry points are gone. Historical stored coordinator/basket blobs are not
automatically migrated or used to resubmit orders.

The live Nautilus node/order-cache snapshot resolver, tenant-scoped durable
attribution CAS implementation, native fill-void reconciliation, and
end-to-end restart acceptance have **not** been completed by this removal.
Until those are verified, production order sending remains disabled.
The deleted research Basket phase planner / hedge gate has no live native
replacement here: multi-leg order admission must remain disabled until
a Nautilus-backed risk gate is implemented and tested. Do not silently
substitute ordinary order acceptance for protective-leg coverage.

CI requirements:
```sh
cargo check --workspace --all-targets --locked
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml
cargo test --manifest-path engine/pipeline/sources/binance_account/Cargo.toml
```

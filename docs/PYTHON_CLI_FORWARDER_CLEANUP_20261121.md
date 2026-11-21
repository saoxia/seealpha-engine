# Legacy Python CLI forwarder retirement (2026-11-21)

The default Engine application is Rust (`seealpha-engine`); Python strategy execution remains supported through `python/strategy_host/`.

This change retires three small **Python-only compatibility launchers**. Each contained only an import of an existing implementation and a `__main__` call:

| Removed compatibility command | Retained implementation command |
| --- | --- |
| `python -m engine.backtest_worker` | `python -m engine.backtesting.worker` |
| `python -m engine.feed.managed` | `python -m engine.market_data.collection.managed` |
| `python -m engine.feed.service` | `python -m engine.market_data.collection.service` |

No backtest algorithm, feed worker, trading behavior, Python strategy package, or native Rust runtime has been removed or replaced by this patch. The three former module entry points intentionally cease to work. GitHub code search did not return references to those paths; external invocations cannot be excluded without deployment inventory. Keep existing legacy worker implementations until native Rust feature parity and migration verification are complete.

**Validation scope:** inspected the contents of each removed wrapper and searched for three former module references in the repository. No local Cargo, pytest, Docker, or live-exchange tests were executed in this session. CI should establish the change's build and regression status.

For remaining Rust cutover blockers see [Rust migration audit](RUST_CUTOVER_AUDIT_20261121.md).

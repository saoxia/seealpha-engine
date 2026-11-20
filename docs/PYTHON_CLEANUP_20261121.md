# Python 清理记录（2026-11-21）

本次仅退役 `engine/pipeline/` 下 5 个无业务逻辑的旧 CLI 转发入口：

- `engine/pipeline/data_management.py`
- `engine/pipeline/pair_data.py`
- `engine/pipeline/prepare_data.py`
- `engine/pipeline/sync_binance_batch.py`
- `engine/pipeline/sync_binance_file.py`

上述文件均仅导入 `engine.market_data.repair.*.main` 然后在 `python -m` 模式调用，并未承担数据转换、交易、对账或任务持久化。对应的旧 Python 实现仍完整保留在 `engine/market_data/repair/`，本次不宣称这五项功能已被 Rust 完整替代。

如果仍需要独立 Legacy Python CLI，请从新模块路径调用：

```sh
uv run python -m engine.market_data.repair.data_management
uv run python -m engine.market_data.repair.pair_data
uv run python -m engine.market_data.repair.prepare_data
uv run python -m engine.market_data.repair.sync_binance_batch
uv run python -m engine.market_data.repair.sync_binance_file
```

Rust 是默认 Engine；`python/strategy_host/` 与 `core/` 契约保留。完整交易、Basket、Nautilus Catalog、数据恢复及 Legacy Python Engine 未得到行为等价验收前，不得据此继续批量删除其 Python 实现。

## 验证状态

- 已核对五个旧文件均为单一 import + `__main__` 转发。
- 已核对五个被转发的真实实现仍在仓库。
- GitHub Actions 按当前暂停策略未启动；本次未在本地运行 pytest/Cargo。
- 删除旧 `python -m engine.pipeline.*` 命令是刻意的 CLI 接口变更；尚未全面核对外部脚本是否使用旧入口。

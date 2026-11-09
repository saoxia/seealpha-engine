# NautilusTrader v2 本机升级记录

日期：2026-10-26。默认开发环境 `.venv` 已切换到 **nautilus-trader 2.0.0rc7**，锁定于根目录 `uv.lock`；系统 Python 未改动。

## 版本与可用性

| 组件 | 当前版本 |
|---|---|
| NautilusTrader | 2.0.0rc7（用户指定候选版） |
| 项目 Python | 3.14.9 |
| uv | 0.13.5 |
| NumPy / pandas | 2.5.4 / 3.1.0 |
| PyArrow / Polars | 26.0.0 / 2.1.0 |
| matplotlib / msgspec | 3.11.2 / 0.22.0 |
| fsspec / uvloop | 2026.9.0 / 0.23.0 |
| pytest / Ruff | 9.1.1 / 0.17.2 |
| websockets | 17.3 |
| Rust / Cargo | 1.99.1 |
| Rust Arrow / Parquet / zip | 60.0.0 / 60.0.0 / 9.1.1 |

31 个已安装 Python 包逐一经 PyPI 复核；除明确指定的 Nautilus RC 外，均为检查时的最新稳定版。rc7 不再强制依赖旧版 fsspec、uvloop。研究依赖继续显式声明，避免升级引擎后漏装。

两项可用性限制，不能表述为“所有工具均已安装最新发布版”：

- Python 3.14.10 已发布，但 uv 0.13.5 的托管构建目录及已检查的 python-build-standalone 发布尚不提供该版本；采用可安装的 3.14.9。没有修改系统 Python 或自行拼装运行时。
- crates.io API 已列出 zip 9.1.2，但官方 Cargo sparse index `https://index.crates.io/3/z/zip` 最新为 9.1.1。尝试 `cargo update -p zip --precise 9.1.2` 返回找不到包，当前锁定 9.1.1。此项是发布索引可用性限制，不是 Nautilus 的版本约束。

Python 依赖以根目录 `uv.lock` 为准，Rust 依赖以 `engine/pipeline/rust_converter/Cargo.lock` 为准。此处版本与可用性结论为升级当日记录。

## 代码适配

- 使用 v2 原生模型、策略、引擎和报告接口；撤单/改单改为传 client_order_id，订单事件使用 events()。
- 项目 `_config.py` 保留声明式策略参数、不可变参数及 JSON 往返，原生运行配置由原生构造器持有。
- 项目 `_strategy.py` 处理 rc7 显式 stop 的 Rust 可变借用冲突，先完成本策略撤单和平仓清理，再进入原生停止流程。
- `nautilus_support.py` 显式保留 v1 的订单价格取整、参考合约及手续费模型；费率可通过 instrument.info.project_fee_rates 指定，未指定时沿用项目原假设 maker 0.0002、taker 0.0004。该默认值不是实时账户费率查询。
- `engine/pipeline/catalog.py` 封装 v2 公共 Catalog API；Rust 输出改为 v2 Decimal128、UTC 纳秒时间戳和字典型买卖方向。项目指标表仍独立存储。
- Binance 合约加载使用原生 USD_M 数据客户端配置；行情层随后改为原生 LiveNode 和 BinanceDataClient，包含断线通知、重连、心跳和关闭处理。Python websockets 仅用于开发测试。
- 当前代码移除旧样式豁免并通过 Ruff；历史研究来源的允许修改有显式哈希登记，原始冻结归档仍被核验。

## 数据与历史结果

`data` 由本机文件系统链接管理；Rust 构建目录现为本机 `engine/pipeline/rust_converter/target/`。程序不检查外置盘挂载。

- 原始 ZIP：`data/raw/binance`，可继续复用。
- Catalog：`data/catalog`。
- 转换状态：`data/pipeline_state/binance`。
- 旧 Catalog 不原地改写，检测到 v1 的 data/bar 或 data/trade_tick 目录会明确拒绝。应从原始归档重建 v2 数据，不能复制旧状态冒充已完成转换。
- `_optimization` 和历史报告保持原始字节，已核对 **7,897 个文件，变化 0 个**。部分研究的可执行适配副本位于 `_research`；旧冻结命令依然代表旧版本研究约束，不应用于建立 v2 已复现的结论。

## 验证与边界

- 正式入口 `python -m pytest -q`：**1,337 项 Python 测试通过，无跳过**；68 条 pandas float_precision 弃用提示暂保留，以免无依据改变 CSV 数值解析精度。
- Rust 单元测试 **3 项通过**；release 转换器重新构建成功。
- 10 条合成逐笔数据转换为 10 根分钟线，Python 与 Rust 输出一致；原生回测生成 HTML/CSV/JSON 报告，控制台 HTTP 启动验证通过并退出。
- 使用隔离的 v1 源码/环境与 v2 环境执行两个开仓、加仓、限价退出场景，成交订单方向、数量、价格一致；见 `docs/validation/engine_scenario_comparison.json`。
- 未下载大规模行情，未连接真实账户下单，未启动长期服务。小样本验证不代表旧历史回测收益已经在 v2 全量复现；旧前向观察数据仍未迁移。

## 开发与恢复

```bash
source ./activate.sh
python -c 'import nautilus_trader; print(nautilus_trader.__version__)'
python -m pytest -q
```

重建使用 `uv sync --locked` 和 `cargo build --release --locked --manifest-path engine/pipeline/rust_converter/Cargo.toml`。
已按用户要求清理旧引擎环境、迁移候选环境、旧 Python 运行时、源码备份和下载缓存。
默认环境仅保留 `.venv`；Catalog 与状态目录统一使用无版本后缀的路径。
当前源码核验使用 `tests/baselines/current_source_baselines.json`，不再依赖旧源码 ZIP。
历史研究归档和独立策略名称中的版本编号保留其原意。清理清单见 `cleanup.json`。
原始交接 ZIP 仍由用户保存在 Downloads；项目不再提供旧环境一键回退。


上游参考：[v2 迁移指南](https://github.com/nautechsystems/nautilus_trader/blob/develop/MIGRATION_V2.md)、[rc7 包](https://pypi.org/project/nautilus-trader/2.0.0rc7/)、[WebSocket 客户端接口](https://websockets.readthedocs.io/en/17.3/reference/asyncio/client.html)。

Current implementation: [native capability adoption](NATIVE_CAPABILITY_ADOPTION.md). Python websockets is now a development-only fixture dependency.

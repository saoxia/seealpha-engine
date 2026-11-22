# SeeAlpha 行情服务（pipeline）

本目录只保留两个一级领域：`sources/`（来源适配、采集和导入）与 `catalog/`（历史存储、查询及数据完整性相关能力）。

## 当前实际目录

| 目录 | 职责 | 状态 |
| --- | --- | --- |
| `sources/binance/nautilus/` | Binance Spot/USD-M NautilusTrader 原生只读实时节点、Feather → Parquet、历史 ZIP 原生导入器 | 独立 Cargo 项目；实验性，尚未通过 READY |
| `sources/binance/archive/` | Binance 官方日 ZIP 下载、来源校验、租户采集策略清单 | `seealpha-binance-market-data-source` |
| `sources/binance/market/` | Binance 公共 REST、K 线、Options、Funding、Exercise，以及当前仍在此 crate 的通用市场模型/计算 | `seealpha-market`；通用逻辑后续再逐个拆分 |
| `catalog/` | 现有历史 Nautilus Parquet Catalog 读写、缺口检索、修复与派生 SQLite 缓存 | `seealpha-catalog`；原生读侧完整替代尚待验收 |
| `catalog/integrity/` | K 线/成交校验、来源证明、READY 门禁和实时交付安全规则 | 原 `seealpha-quality` crate 完整保留；临时放在 Catalog 下，后续按职责逐步内聚 |

这次仅整理目录，**未删除 Quality 规则，未更改 crate 名称、HTTP 契约、存储路径、策略行为或数据安全门禁**。不能因为多个来源可以互相补齐，就将收到数据等同于经过验证的数据。

## 当前调用关系

- `engine/runtime/src/sources.rs`：显式启动、停止和监测 Nautilus 行情子进程。
- `engine/runtime/src/data.rs`：管理租户授权配置、历史采集任务及策略只读订阅声明；**尚非跨进程实时推送**。
- `sources/binance/nautilus/src/main.rs`：原生 Binance 实时 TradeTick 订阅、Feather 暂存、正常关闭时 Parquet 提交。
- `sources/binance/archive/` 与 Nautilus 的 `import_archive`：官方 ZIP 下载、验证、原生历史导入。
- `catalog/`：Runtime 当前使用的历史数据读写与派生缓存。
- `catalog/integrity/`：保留完整性/来源证明相关规则；仍需接入可信协调器，不能自动报告 READY。

## 安全边界

Nautilus 实时节点仍须显式启用并独立部署；当前不提供经验证的独立策略实时推送。实时/历史衔接、缺口恢复、Native Catalog 读侧兼容和 READY 均未完成生产验收；不得放宽 `data_ready=false`、`allow_new_risk=false` 等限制。

数据文件不存放在此源码目录中。按 [数据目录约定](../../docs/architecture/DATA_LAYOUT.md) 保存在仓库根目录 `data/` 或生产挂载卷。

后续逐个文件夹优化时，再决定 `catalog/integrity/` 内哪些功能属于数据源校验、Catalog 一致性或 Runtime 交易门禁；本次先保持实现及安全约束不变。

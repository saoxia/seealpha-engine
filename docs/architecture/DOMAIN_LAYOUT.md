# SeeAlpha Engine 当前领域布局

当前源码以 `engine/` 下的 Rust crates 为主。规划领域不等于已实现功能；以代码、Workspace 和验证结果为准。

## 已落地的业务领域

| 目录 | 职责与现状 |
| --- | --- |
| `engine/pipeline/` | Nautilus 原生行情节点、补充数据源、数据完整性验证与 Catalog |
| `engine/ledger/` | 订单意图、账户事件、真实成交、对账与存储 |
| `engine/database/` | PostgreSQL 数据访问、schema 引导 |
| `engine/connection/` | Binance Options 规范化；Spot/USD-M 的 Nautilus 执行工厂仍属非实盘接入阶段 |
| `engine/gateway/` | HTTP API、研究只读入口、MCP |
| `engine/apps/` | 进程入口、服务装配与运行 |

2026-11-23 已删除仅运行内置 EMA 交叉示例的 `engine/strategy/`，以及关联的包检查、不可变下发和预览 HTTP 路由。它们当时没有独立于示例的真实生产策略使用方。该操作**不代表 Nautilus 已经接管策略执行**。

## 尚未接通的目标能力

- **策略**：计划复用 NautilusTrader 的原生 Strategy、Actor、指标与生命周期；目前不启动自研策略宿主，也不允许上传任意 Python/Rust 代码运行。
- **Risk / Execution**：原独立目标数量/净额/裁剪 crate 已删除。后续应直接验证并复用 NautilusTrader 原生订单状态机、执行、撤改单、缓存与对账，仅针对 SeeAlpha 的策略信号组合、资金分配、必要业务授权与可信成交归属补充代码；未完成端到端验收前不启用实盘。
- **回测**：旧模拟撮合和回测模块已删除，后续另行规划。

## 应用与数据边界

应用入口在 `engine/apps/`；HTTP 路由在 `engine/gateway/api/`。旧 `engine/runtime/` 已删除。行情持久化存储位于仓库根级 `data/` 或部署挂载卷，不在源码树内。

只读历史查询不等于行情 READY；真实订单 ACK 不等于成交。只有核验的真实成交证据才能更新权威账户事实。当前没有生产实盘订单发送通路，也不在本次示例退役中删除既有 PostgreSQL 数据。

不要为了形式完整创建空 crate、替代实现或兼容转发；需要新能力时先明确 Nautilus 原生 API 的边界与验证方案。

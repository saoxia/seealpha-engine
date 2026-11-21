# NautilusTrader 优先复用：SeeAlpha 架构边界与迁移门禁

状态：**架构决策已确定，Rust 接入尚未完成**（2026-11-21）。本文件是迁移约束，不是“已实现”的功能清单。

## 唯一原则

SeeAlpha 是构建在 NautilusTrader 之上的**业务扩展层**。NautilusTrader 提供通用行情、模型、事件、交易所连接、订单执行/生命周期、组合/风险基础能力和回测。SeeAlpha 不复制这些通用能力，不另造 `VenueAdapter` / `ExecutionEngine` / 通用撮合框架；不为了目录整齐添加空 crate、facade 或多余中间层。

只有在**锁定版本经过源码和行为核验确实缺失**、或者 SeeAlpha 存在上游通用语义无法表达的业务需求时，才保留最小的差异实现，并注明退出条件。要降低总代码量、状态来源数和维护面，而不是只改变目录结构。

## 以实际 Rust 仓库为准（不可把计划写成事实）

- 当前 `Cargo.toml` / `Cargo.lock` 没有 NautilusTrader Rust crate 直接依赖。
- `engine/connectivity/venue` 使用 `reqwest` + HMAC 构造 Binance 签名请求；`engine/application/runtime/src/orders.rs` 直接实例化 `VenueClient`；`engine/market_data/sources/binance` 保有独立 WS/REST 数据源。它们**不是** Nautilus 内核的适配器。
- `engine/backtest/core` 和其他模块亦需按照同一原则评估重复能力，不能仅凭 Nautilus 兼容 Parquet 就声称已原生集成。
- 此决定**没有**修改 Rust 依赖、Cargo.lock、实盘路由、订单发送默认值或运行时状态。任何转接完成之前，保留既有安全门禁。

## 基线：核对过的 NautilusTrader 上游

来源：官方 `nautechsystems/nautilus_trader` 的 **v2.0.0rc9** 标签，目标 commit `da6b57aefdd62b44f675e434ffcfefca7f4025dd`。该标签源代码 Cargo workspace 的 crate 版本为 `0.68.0`，不要把 Python 发布标签误写成 Rust crate 版本。新接入必须在确定版本、Rust toolchain 和 Cargo.lock 后进行，并检查 LGPL-3.0-only 引入的分发义务。

- [Binance crate 入口](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/crates/adapters/binance/src/lib.rs)：现货、USD-M、COIN-M 有 live data/execution clients；Options/Margin 只有共用枚举、常量、鉴权基础等，**没有完整 live client**。
- [Spot 原生执行](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/crates/adapters/binance/src/spot/execution.rs)、[Futures 原生执行](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/crates/adapters/binance/src/futures/execution.rs) 已包含订单命令、成交与订单报告相关支持；具体 OCO/Algo、对冲模式、断线恢复的可用范围仍须以离线行为矩阵核对。
- [鉴权实现](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/crates/adapters/binance/src/common/credential.rs) 同时定义 REST HMAC 与 Ed25519；原生执行客户端的 WS API 路径使用 Ed25519。**不可**将“REST 支持 HMAC”误推断成“现有 HMAC 凭据可直接驱动原生 live execution”，亦不可为规避差异复制交易协议。

## 能力归属（优先顺序，不代表迁移完成）

| 能力 | 首选责任方 | SeeAlpha 仅保留什么 | 当前差距 |
| --- | --- | --- | --- |
| Binance Spot / USD-M 普通交易、撤改、用户流和成交报告 | Nautilus `nautilus-binance` | 账户/租户绑定、意图审计、安全业务门禁 | 目前自研 `VenueClient`；**待原生迁移** |
| Binance COIN-M | Nautilus | 需要时增加业务配置；不另写协议 | 尚无 SeeAlpha 端到端集成 |
| Spot OCO、USD-M 条件单、订单恢复 | Nautilus 已有对应原生能力可核对 | 仅保留已证明未覆盖的行为和不变量 | 与旧 `spot_oco`、`conditional`、`reconciliation` 逐项对照 |
| Spot OTO / OTOCO 等特殊能力 | 先核对同一上游版本 | 确定缺口时才写局部扩展 | 尚未核实全部边界，不预设必须自研 |
| Binance Options 数据/执行、行权/结算 | 已有原生模型/基础设施优先；缺失 live client 的部分由 SeeAlpha 扩展 | 经过验证的 Options 专有传输和结算映射 | 当前本地实现不等于完整生产验收 |
| 行情 WS/REST、通用模型、Bar/Tick、Catalog | Nautilus | 来源证据、连续性/缺口门禁、合规发布与领域特征 | 现有 `market_data/sources/binance` 存在重复连接 |
| 通用订单状态机、回测/撮合、Risk/Portfolio 基础能力 | Nautilus | 多策略风险预算、目标仓位决策、业务归因、租户作用域 | Rust 自研模块需逐项 audit |
| 租户隔离、不可变意图、审计、来源证明、账户业务规则 | SeeAlpha | 仅这些项目域需求 | 保留，但不要反向复制 Nautilus Cache/OMS |

## 不增加新的架构复杂度

1. **不先设计第二套插件系统。** 对接已存在的 Nautilus 客户端、事件和模型；只在实际 API/租户契约边界设置必要的薄集成代码。
2. 不把旧 `connectivity/venue` 拆成 `adapters/binance/spot`、`futures` 再继续自行维护协议。这只是把重复实现改了文件名；优先直接退休重复代码。
3. 功能替换一项，经过对照验证后删除对应旧实现、重复类型、兼容转发，不能长期双轨运行或维护两套权威仓位/成交状态。
4. 新增项目自有功能必须在 PR 中写明：原生版本与 API、缺口证据、SeeAlpha 独有业务目标、额外代码规模、回归测试和将来删减条件。

## 安全迁移顺序（未实施即不得标完成）

1. **锁定上游**：在同一明确 release commit 上核对 API、Cargo 特性、工具链、签名方式、精度、事件、交易所支持和许可证。
2. **Spot/期货单链路样板**：仅用本地模拟交易所连接和合成订单，把 Nautilus 的指令和事件接到现有租户/账本边界；不用生产密钥；不新增自研通用 Adapter Trait。
3. **行为矩阵**：新单、重复请求、部分成交、撤改单、订单列表、条件单、Hedge Mode、乱序/丢失 user stream、重连、HTTP 超时 Unknown、REST 完整分页、服务重启、租户隔离、精度/最小金额及风险门禁；核对 Nautilus 与旧代码每一项差异。
4. **单一事实源**：由 Nautilus 提供订单/成交权威事件模型，SeeAlpha 账本保存独立验证的证据和审计；严格保留 `IntentJournal` 先登记与 Unknown 不盲重发等要求，不让 Nautilus 的一般 retry 机制跨越业务门禁。
5. **按通过范围删除**：一个产品/动作达到 parity 后，删除对应独立 REST、WS、状态机等重复实现。对未覆盖的 Options/特别订单仅留最小扩展，避免双写、双重对账。
6. **工作区验收**：更新 Cargo.lock，执行 check/fmt/clippy/test/release build、Docker 本地模拟 smoke 和跨账本场景。任何失败必须记录；未通过前保持原来的生产禁用状态。

此文档不授权真实账户连通、真实订单或自动合并；“复用优先”不能成为绕过持久化和风控约束的理由。

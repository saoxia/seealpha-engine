# Execution / Order Management

## 当前实现归属（2026-11-23）

- 原 `engine/execution/` 自研目标数量、净额、步长与裁剪 crate 已删除，不保留空壳或兼容接口。它此前不被主 Engine/HTTP 在线调用；删除不代表组合交易功能已经由 NautilusTrader 替代。
- `engine/pipeline/sources/shared/nautilus/`：独立原生 Binance Spot/USD-M 执行工厂、订单事件身份映射、虚拟成交归属纯计算及离线节点构造测试。该项目不在主 Cargo workspace 中，尚未完成生产发送及恢复验证。
- `engine/pipeline/sources/binance_account/`：原生订单事件只读观察及待接入的策略成交归属辅助；不是第二套 OMS。
- `engine/ledger/movements/verified_fills.rs`：经核验的成交证据与经济记录；不能根据 ACK 或未认证的 Nautilus 事件推导真实成交。
- `engine/gateway/api/src/routes/orders.rs`：历史订单和 Ledger 已核验成交只读查询，不是 Nautilus 实时订单状态源。
- `engine/connection/binance-options/`：Options 专属协议适配；不是 Spot/USD-M 发送通道。

## 重建原则：原生优先

1. **NautilusTrader 是唯一权威 OMS**：优先复用原生订单创建、状态机、订单事件、提交/撤改单、缓存、账户状态和对账；SeeAlpha 不建立并行的交易所订单状态机、订单缓存或通用恢复发送器。
2. **SeeAlpha 只补自身业务规则**：多策略信号整合、资金预算、组合目标、账户级风险限制、必要的发送前授权，以及真实成交后的策略虚拟归属。
3. **逐项核验原生能力，而非假定生产可用**：原生 API 存在不代表当前锁定的 Nautilus 版本、Binance 适配器或恢复路径满足生产要求；特殊订单及多腿风控须独立验收。
4. **防止重复/未知结果导致额外订单**：提交前必须有明确的授权与持久化身份，外部结果未知时只核查而不盲目重发。SeeAlpha 的补充逻辑不复制 Nautilus 的 OrderCore/OMS。
5. **分离真实交易与模拟**：只允许可信核验的真实成交影响 Ledger，不能把模拟或原生 ACK 记作已成交事实。

## 实施顺序（尚未完成）

1. 在独立、无实盘密钥的 Nautilus 环境中验证原生标准订单的提交、接受、拒绝、部分/完全成交、撤单、改单及事件流。
2. 验证原生重启恢复、账户/订单对账及未知结果行为；识别确实缺失的业务保证。
3. 仅新增所需的薄适配和 SeeAlpha 专属组合、账户授权及成交归属逻辑，再做端到端离线验收。
4. 通过单独的生产安全验收后，才讨论接通真实账户和策略入口。

当前仍无已验收的实盘订单发送、发送前原子授权、可信成交对账与重启恢复链路。删除代码不触碰 PostgreSQL 历史记录、生产凭据或实际持仓。

参见 [Ledger / Nautilus 历史重叠审计](LEDGER_NAUTILUS_OVERLAP_20261122.md)（历史快照，不作为当前运行能力证明）。

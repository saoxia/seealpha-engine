# SeeAlpha Ledger — 业务边界与功能块

本 crate 目前仍是一个 SQLite 持久化入口，而不是独立 OMS、EMS 或 NautilusTrader 订单生命周期实现。**本次只做结构归组和 SQL 外置，不改变 API、数据表或交易行为。**

**业务二级抽象已明确为 Accounts / Movements / Allocations / Reconciliation。** 当前四个源码文件夹只是迁移期的物理归组，不应被误认为最终业务二级划分。见 [账本与资金二级设计](../../docs/architecture/LEDGER_FUNDS_LEVEL2_20261122.md)。

| 功能块 | 代码位置 | 现有职责 | 是否被 Nautilus 接管 |
| --- | --- | --- | --- |
| Execution Guard | `src/execution_guard/` | 不可变发送前意图、UNKNOWN / ReconcileOnly、恢复任务 | 订单状态和执行生命周期未来由 Nautilus 承担；安全发送前持久化暂保留 |
| Exchange Evidence | `src/exchange_evidence/` | 已验证的成交、资金费/期权账户结算、账户证据、快照 | 与 Nautilus 的 OrderFilled、FillReport、AccountState 对照后逐项收敛；不假设证据等价 |
| Portfolio State | `src/portfolio_state/` | Basket/Coordinator 的 CAS 状态持久化 | 不属于 Nautilus 标准订单能力；业务规则保留在 Portfolio |
| Compatibility | `src/compatibility/` | 旧 API 视图、paper fills、gap 与历史存储兼容 | 新调用方不要依赖；逐项迁移后删除 |
| Storage | `src/lib.rs` + `src/storage/schema.sql` | Ledger API、SQLite 连接、原有完整 Schema、租户和 paper 会话 | 暂不调整语义；后续抽离 tenant / paper |

以下为**规划的业务功能块**，尚未创建空模块：

- **Allocation**：Portfolio 决定跨策略内部撮合、真实成交多对多归属、费用分摊；Ledger 只持久化分配事实和源身份，不生成伪造的交易所成交。
- **Reconciliation**：优先调用 Nautilus 的原生执行/持仓对账，SeeAlpha 只补齐多策略归属与交易所结算、余额核验的差异记录；不能由未认证报告发明财务事实。

## 不得破坏的不变量

1. 订单在外部发送前持久登记唯一意图；UNKNOWN 不自动重发。
2. ACK 和未认证 FillReport 不产生可信成交、现金入账或持仓结算。
3. 不覆盖交易所原始成交、资金结算及其账户范围；精确金额用 Decimal 表示。
4. 跨租户/账户/环境完全隔离；Basket 版本 CAS 不得绕过。
5. 回测产生模拟执行，绝不冒充交易所真实成交。
6. 功能替代后必须先证明同一场景的身份、数量、费用、重启恢复与风控等价，再删除旧实现和表；历史原始数据默认保留。

详细成熟系统对照、替换顺序与验收见 [功能块蓝图](../../docs/architecture/LEDGER_FUNCTIONAL_BLOCKS_20261122.md)，上一轮能力重叠审计见 [Nautilus 对照](../../docs/architecture/LEDGER_NAUTILUS_OVERLAP_20261122.md)。

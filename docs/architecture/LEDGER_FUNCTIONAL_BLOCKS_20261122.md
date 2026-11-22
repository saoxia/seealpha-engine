# Ledger 功能块蓝图 / Nautilus 优先替代路线（2026-11-22）

## 目标与版本边界

**二级业务架构更正（2026-11-22）：** 账本与资金作为既定一级模块，其内部二级抽象是 Accounts、Movements、Allocations、Reconciliation。本文六块清单仅供本次整理旧代码和规划移除职责使用，不能作为最终业务域划分。请以 [账本与资金二级业务设计](LEDGER_FUNDS_LEVEL2_20261122.md) 为准。

- SeeAlpha 保留 **NautilusTrader 优先** 原则：订单、执行、持仓、账户与原生恢复交给 Nautilus；不再造同等通用引擎。
- 当前 SeeAlpha 固定的 Nautilus Rust crates 是 **0.68.0 / 2.0.0rc9**。下表成熟项目的公开文档反映设计思路，不保证这一定版已经实现其所有最新功能。实际替代必须以锁定版本编译、离线回归和运行验收为准。
- 本 PR 首先归组模块、保持函数和数据库兼容，不引入新表/新业务结果，不把逻辑上不同的数据硬合并。

## 参考系统：借鉴什么、不借鉴什么

| 系统 | 已公开的划分 | 对 SeeAlpha 的启发 | 不应照搬 |
| --- | --- | --- | --- |
| NautilusTrader | ExecutionEngine、ExecutionManager / Reconciliation、Cache、Account / Position、Streaming / Event Store | 作为真实订单、执行事件、持仓和运行时账户状态的首选引擎 | 将其异步事件写入等同“发送前落盘”；把恢复合成 Fill 当成经认证的交易所财务事实 |
| QuantConnect LEAN | Portfolio、SecurityTransactionManager、Brokerage、订单事件 | 目标持仓与外部订单处理分工 | 以单策略的订单票据替代多策略成本分配 |
| vn.py | MainEngine、OmsEngine、Gateway、EventEngine | 功能引擎与适配层职责分开 | 再造一套通用网关或 OMS |
| Hummingbot V2 | Controllers、Executors、Orchestrator | 策略意图与有限生命周期执行任务分离 | 直接把 executor 绩效记录等同结算凭证 |
| Freqtrade | Trade / Order 对象与 persistence | 历史交易可查询、运行状态持久化 | 将一笔 Trade 当作唯一的机构归属单元 |
| TigerBeetle | Account、Transfer、Ledger / Double-entry | 资金变化应有可解释、可冲正的账务事实 | 在未需要复式会计前引入第二套账务引擎 |

## 过渡性代码归组（不是最终业务二级模块）

| 逻辑块 | 状态 | 当前落点 | 应持有什么 | 不负责什么 |
| --- | --- | --- | --- | --- |
| Execution Guard | **已归组** | execution_guard | 唯一发送前意图、UNKNOWN 安全恢复任务、持久化检查点 | 自研订单生命周期、交易所订单发送 |
| Exchange Evidence | **已归组** | exchange_evidence | 可信 trade ID、资金费/期权结算、账户证据与必要原始快照 | 公共费率推断现金、以 ACK 当 Fill |
| Allocation | **待设计** | Portfolio 决策、未来 Ledger 事实接口 | 内部成交与外部成交的多对多归属、来源、数量、成本分摊规则版本 | 真实交易所执行、补写不存在的 venue Fill |
| Reconciliation | **待收敛** | Nautilus 原生能力优先；当前恢复任务在 execution_guard | 账户/持仓/成交差异报告、未分配状态及修正证据 | 未验证就强制改写原始资金和成交 |
| Portfolio State | **已归组** | portfolio_state | Basket/Coordinator CAS journal 和恢复状态 | 策略交易决策、跨腿风险规则实现 |
| Storage & Compatibility | **已归组** | storage/schema.sql、compatibility、lib.rs | SQLite 读写、历史兼容、租户与 paper 旧接口 | 新增独立 OMS、模拟撮合引擎 |

## 优先顺序（一次只替换一个闭环）

### P0：模块归组（本 PR）

- 源码按职责分到目录，并通过各目录 mod.rs 重导出公开 API。
- 与旧版本 SQL Schema 字节完全一致（从 `const SCHEMA` 拆为 `include_str!("storage/schema.sql")`）。
- 不迁移 DB、不重命名任何表、不删除旧业务功能或生产证据。
- 原已有 `native_events.rs` 已在 PR #184 删除；本次不再重复造订单事件 JSON。

### P1：原生订单生命周期和原生事件持久化（下一阶段）

1. 在固定 Nautilus Rust 版本中完成执行 LiveNode、Streaming/Catalog 实际运行、重启/断线/部分成交/撤改单测试。
2. 建立唯一 AccountId、InstrumentId、client_order_id / venue_order_id / trade_id 映射。
3. 接通 SeeAlpha 原子发送前意图门禁，再允许 Nautilus 执行；UNKNOWN 必须只查询核实。
4. 切换消费者后删除 Ledger 内重复的 `order_observations` **状态维护代码**；永久证据和历史数据库行仍需保留/迁移策略。

### P2：已验证成交和账户状态

1. 对齐 Nautilus OrderFilled、FillReport 与交易所认证 Trade ID、费用、精度和所有部分成交。
2. 对齐 Nautilus AccountState、Position、Cache 与权威账户快照、现货余额及期货 Hedge/OneWay 仓位。
3. 确认 Basket/Portfolio 的账户版本、风险门禁不变后移除重复的实时读取和推导；保留不可替代的历史认证证据。
4. 单独验证资金费、期权到期、划转的签名结算完整性，不凭公共数据补造现金。

### P3：Allocation

- 定义 `run_id`、`strategy_id`、内部订单/成交 ID、Nautilus 订单/真实 Trade ID。
- 记录真实成交分配与策略内部撮合为**不同类型**，二者不得相互冒名。
- 完整外部分配需满足 `sum(allocated_qty) == venue_fill_qty`，未完成使用显式 `unallocated_qty`。
- 策略净量合计应能解释对应时点的交易所账户敞口；手续费、资金费以实际总额为约束，并保留分摊规则和版本。

### P4：Reconciliation & 财务事实

- 优先复用 Nautilus 执行对账；SeeAlpha 增加策略分配对账、跨产品资金结算差异、历史证据完整性标记。
- 如产生真实会计需求，再设计独立分录，不把业务交易对象直接变成复式总账。

### P5：删除 Legacy

- 对 compatibility/records 与 parity 的每个 API 建立消费者清单和替代位置。
- 只在 **生产调用已切换、旧历史数据可访问、断线恢复通过测试** 三者同时成立时删除；否则将其标成兼容边界而不是伪装成已迁移。
- 删除功能与删除历史表数据是两项不同的变更；除非单独审批数据迁移，绝不 DROP。

## 每个功能块的验收清单

1. 明确一个权威事实来源（Nautilus、交易所签名报告或 SeeAlpha 内部业务事件），不做双重可写主数据。
2. 支持重启重放、重复消息幂等、乱序和未知状态失败关闭。
3. 区分 live / sandbox / paper / backtest，且同账户多策略归属可审计。
4. 精度、持仓净额、资金费用守恒；拒绝无证据的现金与成交。
5. 函数 API 调用方、历史数据导出与恢复路径逐一验证。
6. 编译/测试与故障注入通过后，原有重复实现和死代码在 **同一替代 PR** 中移除；迁移记录保留。

## 参考资料

- Nautilus: https://nautilustrader.io/docs/latest/concepts/architecture/ ; https://nautilustrader.io/docs/latest/concepts/execution/reconciliation/ ; https://nautilustrader.io/docs/latest/concepts/event_sourcing/
- LEAN: https://www.quantconnect.com/docs/v2/writing-algorithms/trading-and-orders/order-management/transaction-manager
- vn.py: https://github.com/vnpy/vnpy/blob/master/vnpy/trader/engine.py
- Hummingbot: https://hummingbot.org/strategies/v2-strategies/
- Freqtrade: https://www.freqtrade.io/en/stable/advanced-setup/
- TigerBeetle: https://docs.tigerbeetle.com/coding/data-modeling/

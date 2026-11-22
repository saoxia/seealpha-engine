# Portfolio Coordinator / Basket — Rust 替代阶段二

## Rust 原生实现

- `seealpha-execution::coordinator`：多成员目标、虚拟订单（Market/Limit/StopMarket/StopLimit）、K 线后触发、成交内部撮合与归属、限额、风险退出、在途订单去重和部分成交、撤单被拒后的在途门禁、冻结、可恢复快照。
- `seealpha-execution::basket`：Spot / USD-M / Options 多腿目标，同账户隔离、合约数量步长、价格与风险预算、Funding Carry/Option Protected 和声明式 Spot/UM hedge pair 的阶段排序、每腿成交累计及终态仓位确认、事件重放。
- `risk`：跨腿漂移、期权覆盖、行情过期、当前总名义额、仓位/累计成交一致性、待确认超时。
- `order_evidence`：根据计划哈希和请求身份生成稳定客户端订单号，交易所订单/执行 ID 绑定，拒绝认可静态 ACK 成交；未知/非完整终态冻结而不是重发。
- `settlement`：已认证期权到期行权明细、合约乘数、行权价、到期价及现金账户前后余额逐项核对。
- `history`：严格 Spot/UM 同步 K 线和原始 FundingRate 时间线；不前填缺口，不从资金费率推算真实资金费结算。
- `journal` / `coordinator_snapshot`：版本化 JSON、SHA-256 损坏检测、可重放状态校验及冻结/在途订单恢复。
- `seealpha-ledger`：SQLite WAL + synchronous=FULL；Basket 和 Portfolio 均采用事务式 CAS 防止并发覆盖。
- `seealpha-runtime`：研究态阶段开始登记、Basket/Portfolio 事件处理、成交证据、风险监控、CAS 保存、重启恢复。

## 安全顺序

1. 从 Ledger 读取并验证已有状态；CAS 冲突时重新读取，不能盲重试。
2. Rust 生成新阶段身份或单一净额订单**意图**，在任何外部副作用之前持久提交。
3. 持久化成功后，独立可信的风控/执行适配器才有条件接收意图；本模块本身**没有**交易所订单出口。
4. 报单 ACK 不意味着成交；以可信委托终态、交易/执行 ID、账户快照核对。
5. 网络不确定、重启未知阶段、撤单拒绝或证据冲突时保持 pending/frozen，禁止重复提交。
6. 真实仓位、资金、到期结算记录不能靠行情推导。

## 编译与回归

本次新增 Rust 单元测试、SQLite CAS 测试和重启恢复集成测试。

需要执行：
```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
python3 -m unittest discover -s tests -t . -p '*_test.py' -v
```

**尚未执行**：当前 GitHub 连接环境不能运行 Cargo；CI 仍处于暂停状态。测试文件存在不等于测试通过。

## 与 Python 代码的关系及剩余工作

此阶段把 Portfolio/Basket 主体功能以 Rust 类型和状态机形式表达，并引入原生 Runtime/Ledger 持久化接口；Python 旧实现暂留作对照，不提前删除。

要认定“完整上线替代”，仍必须完成：
- 将具体 Nautilus/Python 事件发布路径与 Rust Runtime 的意图、虚拟成交回调逐一打通或明确弃用旧策略宿主事件 API；
- 将受认证的交易所订单、持仓、现金和资金费流水适配至此 Rust 状态机，不允许相信任意未认证调用方参数；
- 将 Rust Coordinator/Basket 接至实际策略生命周期和对外管理接口；原有 Python research/session 入口是否仍对外服务必须逐项核对；
- 使用有 Rust 工具链的运行环境编译、格式化、执行全部测试以及离线故障注入；
- 执行兼容性与长时间恢复验收，证明真实异步、部分成交、幂等和崩溃恢复闭环。

所以**代码迁移及原生接线基础已经推进，但不声称生产全链路验收完成**。生产交易授权未因迁移被扩大。

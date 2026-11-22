# Runtime — 用例编排

Runtime 负责租户检查、已授权策略包的只读预览以及订单意图预登记/恢复清单。HTTP 和 Catalog 查询由 Gateway 负责；行情、策略、组合、回测、账户与执行逻辑应由对应领域 crate 或 NautilusTrader 提供。

Gateway 直接调用 `seealpha-catalog` 读取 Nautilus v2 Parquet 数据；Runtime 不再提供 Catalog 转发、历史 Bar 写入或未使用的连续 Bar 查询入口。行情写入与修复继续由 Pipeline 负责，Catalog 本身仍执行 Parquet、时间和精度校验。

## 已退役的自定义 Paper 会话

- 移除旧的 Bar 收盘价即时成交回放，以及自定义 Paper 持仓、订单、成交和会话状态写入（原 session.rs 的模拟执行和 state_store/paper.rs）。
- 不再提供 POST /tenants/{tenant_id}/accounts/{account_id}/paper/sessions；旧 Paper smoke 改为只读市场数据验证。
- 保留 strategy-preview，但其输出仅包含已下发策略的信号和目标仓位；不读取/写入模拟仓位，也不会生成 Paper Fill。成员未发布目标时不把缺失目标当作零。
- 自定义历史 Backtest 仍属于独立的 backtest crate，本次没有修改或移除。
- 新建 PostgreSQL Schema 不再创建 paper_positions、paper_sessions，也清理无调用方的旧兼容表；共享 orders/fills、订单意图和真实成交证据表继续保留。不会在启动时自动 DROP 现有表。

## 安全边界

真实交易仍禁用：Nautilus 原生执行尚未完成原子预发送授权、风控接入、认证成交证据回写及恢复验证。订单意图的持久化、未知状态不得重发、租户隔离，以及真实成交必须独立认证的门禁均保持原样。

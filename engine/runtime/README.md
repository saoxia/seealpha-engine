# Runtime — 用例编排

Runtime 负责租户检查、已授权策略包的只读预览、Catalog 访问以及订单意图预登记/恢复清单。HTTP 由 Gateway 负责；行情、策略、组合、回测、账户与执行逻辑应由对应领域 crate 或 NautilusTrader 提供。

## 已退役的自定义 Paper 会话

- 移除旧的 Bar 收盘价即时成交回放，以及自定义 Paper 持仓、订单、成交和会话状态写入（原 session.rs 的模拟执行和 state_store/paper.rs）。
- 不再提供 POST /tenants/{tenant_id}/accounts/{account_id}/paper/sessions；旧 Paper smoke 改为只读市场数据验证。
- 保留 strategy-preview，但其输出仅包含已下发策略的信号和目标仓位；不读取/写入模拟仓位，也不会生成 Paper Fill。成员未发布目标时不把缺失目标当作零。
- 自定义历史 Backtest 仍属于独立的 backtest crate，本次没有修改或移除。
- 历史 PostgreSQL paper_positions、paper_sessions 数据不 DROP；共享 orders 和 fills 保持原样。数据库表的归档/迁移另行处理。

## 安全边界

真实交易仍禁用：Nautilus 原生执行尚未完成原子预发送授权、风控接入、认证成交证据回写及恢复验证。订单意图的持久化、未知状态不得重发、租户隔离，以及真实成交必须独立认证的门禁均保持原样。

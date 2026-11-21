# Binance Options 专属适配能力（迁移中）

本 crate 目前负责 **Options /eapi 用户成交证据、账户快照规范化及订单参数规则及行权证据规范化**，不发送交易请求，也不持有凭据；由原 `engine/connection/binance` 的签名只读查询调用。

- `normalize_option_trades` 保留交易所成交 ID、订单归属、精确小数字符串、手续费符号和时间；非目标订单被过滤，字段缺失、合约身份不符或重复成交即拒绝当作已确认成交。
- `normalize_options_account_snapshot` 将已签名 EAPI 账户与持仓的两份只读回复规范化为原 Runtime 快照契约；不新增余额估值或就绪判定。
- `build_options_order_pairs` 只生成现有 EAPI LIMIT 委托的确定性签名参数，保留字段顺序与错误语义；不会联网或持有交易凭据。
- `normalize_option_exercises` 验证 EAPI 签名行权记录的合约、交易所 ID、精度、时间和去重，不根据行权金额虚构资金流。
- Spot / USD-M 行情继续由 `integrations/nautilus_binance_data` 调用 NautilusTrader；其订单执行尚未迁移，仍位于 `engine/connection/binance`，**不要提前删除**。
- 尚未迁入：Options 的签名 HTTP、账户快照、订单写入门禁、全生命周期、Runtime / Ledger 对账。不能把当前目录出现误认为完成 Options 连接适配。

迁移原则：复用已有交易平台/交易所适配能力，不复制 Spot / USD-M 客户端。只有在 Nautilus 原生执行接线经过离线、恢复与安全门禁验收后，才移除 `engine/connection/binance` 中对应实现。

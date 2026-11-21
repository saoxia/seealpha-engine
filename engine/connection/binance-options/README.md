# Binance Options 专属适配能力（迁移中）

本 crate 目前仅负责 **Options /eapi 用户成交证据的规范化校验**，不发送交易请求，也不持有凭据；它由原 `engine/connection/binance` 的签名只读分页查询调用。

- `normalize_option_trades` 保留交易所成交 ID、订单归属、精确小数字符串、手续费符号和时间；身份不符、字段缺失或重复成交即拒绝当作已确认成交。
- Spot / USD-M 行情继续由 `integrations/nautilus_binance_data` 调用 NautilusTrader；其订单执行尚未迁移，仍位于 `engine/connection/binance`，**不要提前删除**。
- 尚未迁入：Options 的签名 HTTP、账户快照、订单写入门禁、全生命周期、Runtime / Ledger 对账。不能把当前目录出现误认为完成 Options 连接适配。

迁移原则：复用已有交易平台/交易所适配能力，不复制 Spot / USD-M 客户端。只有在 Nautilus 原生执行接线经过离线、恢复与安全门禁验收后，才移除 `engine/connection/binance` 中对应实现。

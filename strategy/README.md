# Strategy / 策略领域

Rust crate：`seealpha-strategy`。

持有策略清单校验、授权原生策略执行、信号/目标持仓发布协议。策略本身不直接发送订单或记录账户事实。本目录由 `alpha/strategy/` 迁入，crate 名及公开 API 不变。

当前代码中与 Portfolio、Risk 和 Execution 的边界见 [领域布局](../docs/architecture/DOMAIN_LAYOUT.md)。

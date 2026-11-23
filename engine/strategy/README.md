# Strategy / 策略领域

Rust crate：`seealpha-strategy`。

持有策略清单校验、授权原生策略执行、信号/目标持仓发布协议。策略本身不直接发送订单或记录账户事实。本目录由 `alpha/strategy/` 迁入，crate 名及公开 API 不变。

当前代码中与 Portfolio、Risk 和 Execution 的边界见 [领域布局](../../docs/architecture/DOMAIN_LAYOUT.md)。

## Rust 原生策略下发与预览

`src/native_issuance.rs` 统一处理原生策略包预检查（ZIP / Manifest / 参数）、不可变账户授权下发、已下发包的读取与执行前可信比对。Runtime 仅验证租户状态、组织 Market/Execution 和返回 API 结果；原生策略执行不具有发送订单权限。

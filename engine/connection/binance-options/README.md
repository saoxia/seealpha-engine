# Binance Options 离线领域规范化

> 2026-11-22：旧 `engine/connection/binance`（Venue）及 `binance-wire` 已删除。本 crate 仍为独立 Cargo workspace 成员；**Runtime、Ledger、Nautilus 执行程序未依赖它**。本目录并不是生产可用的 Binance 私有 REST 连接器。

## 当前职责

- `account.rs`：调用方注入签名只读 GET，读取官方 `/eapi/v1/marginAccount`（返回 `asset` 数组）及 `/eapi/v1/position`；输出原有 `product/balances/positions/observed_at` 格式。只接受 Options 原生 `side=LONG|SHORT`，并校验有符号数量；保留原有 `balances`/裸数组外壳与 `positionAmt` 数量别名作为离线兼容。
- `lib.rs`：规范化已认证的 Options 成交，区分全局 `id` 和 `tradeId`，保留数量和手续费原始精度、订单归属、时间及报价币种。身份不完整的订单记录不得被静默跳过。
- `trades_page.rs`：构造 `symbol/fromId/limit` 参数并最多获取 32 页签名只读结果，拒绝过大页面、重复或逆序的 `id`、回退游标与溢出；**有限分页不证明历史成交完整**。官方查询窗口仅限近三个月；`fromId` 与全局 `id` 的关联仍须经受控网络验收。
- `order.rs`：仅构建原 LIMIT 参数（`GTC/IOC/FOK/GTX` 统一大写）、按 `clientOrderId` 查询/撤单参数并核验订单响应身份，不发送订单、不签名。
- `settlement.rs`：规范化已认证行权记录，校验事件身份、金额字段、重复与时间；不自行生成钱包现金变动。

## 安全边界与未完成事项

该 crate 不持有 API 密钥，没有签名、订单写入、真正的账户查询或成交同步能力。Nautilus 的 Spot / USD-M 原生执行与 Options 独立执行适配均尚未经过实盘验收，禁止启用真实订单。Ledger 仍必须独立验证账户/事件归属、成交与结算证据；这种跨信任边界的重复校验不能视为代码冗余。

未来若通过 Nautilus Options ExecutionClient/受控网关接入，可将上述纯规范化规则作为内部实现复用；不能恢复第二套自研 Binance REST 下单通道。

## 离线验证

```sh
cargo test -p seealpha-binance-options --locked
cargo check -p seealpha-binance-options --all-targets --locked
```

上述命令为验收要求，不代表本次 GitHub-only 修改环境已经执行。分页行为和账户数据必须另以官方响应字段及受控环境核验。

官方文档：
- https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/rest-api/account
- https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/rest-api/trade

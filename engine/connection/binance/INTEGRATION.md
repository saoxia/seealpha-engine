# seealpha-venue：订单安全门禁与集成契约

> 历史增量的安全契约说明，不代表当前 Runtime/Ledger 接线状态的实时验收报告。目录重构不启用实盘。

## 当前目录归属

`engine/connection/binance/` 是 **Binance 私有 REST 订单与账户适配器**；Binance 行情采集仍在 `engine/market_data/`，不在这里复制 NautilusTrader 能力。Cargo 包名 `seealpha-venue`、`seealpha_venue::*` 公共模块路径保持不变。

| 物理目录 | 职责 |
| --- | --- |
| `src/lib.rs` | 签名 HTTP 客户端、端点白名单、公共类型及公开模块声明 |
| `src/spot/` | Spot OCO、OTO、OTOCO |
| `src/usd_m/` | USD-M 条件订单（Algo） |
| `src/options/` | Options 执行、协议规则和真实成交查询 |
| `src/orders/` | 预登记安全门禁、生命周期、真实成交证据及用户流 |
| `src/account/` | 多产品只读账户证据、账户对账及资金结算查询 |

模块声明使用 `#[path]` 直接指向实体源码文件，无空壳 crate 或转发模块。业务行为、凭据边界、账户安全门禁与 Ledger 事务归属不变。

## Options 证据代码抽离（2026-11-21）

`normalize_option_trades` 及其真实成交证据校验、期权账户快照结构校验已搬至 `engine/connection/binance-options`。当前 `src/options/trades.rs` 仅负责签名成交分页请求，`src/account/reconciliation.rs` 的 Options 分支只保留签名账户/持仓 GET 与错误映射。该迁移不改变用户成交归属、重复 ID 检查、成交数量/手续费字段或任何真实订单发送行为；不代表 Options 的整套交易网络接口已经迁移。

## EAPI 订单参数规则独立归属

原来 `VenueClient::place_order` 中期权专用的 LIMIT 类型、价格、TIF、`reduceOnly`、`postOnly` 和 `clientOrderId` 参数构造已迁至 `engine/connection/binance-options/src/order.rs`。签名、预登记门禁、未知结果判定和真实下单权限均仍由旧多产品 Venue 持有；此增量不创建新订单发送通道。

## 当前原生能力与复用范围

原有 `src/spot/oco.rs`（Spot OCO）、`src/usd_m/conditional.rs`（USD-M Algo）、`src/options/trades.rs`（Options 成交分页）仍保留，不重新实现。旧 `src/options/execution.rs` 仅用于离线原型的 `OptionsExecutor`、`src/options/rules.rs` 中未被运行时引用的测试辅助规则已删除；真实订单仍由 Venue/Runtime 使用预登记门禁处理，`binance-options` 保留实际使用的专属解析与 EAPI 订单参数规则。原有 `src/orders/lifecycle.rs` 提供数量严格校验、终态及单调状态迁移。新增：

- `src/orders/guard.rs`：按产品限定 Binance POST/PUT/DELETE 路径，提交一次之前调用持久化 `IntentJournal::admit`。相同账户与 request_id 再次出现时只允许 GET 对账；无法确证的 HTTP 状态、超时、未知响应、ACK 字段不匹配都归于 Unknown，**绝不自动补发**。
- `src/spot/oto.rs`：原生 Spot OTO、OTOCO 参数与确定性子单客户编号；交易所管理 pending legs，禁止在本地把触发当作成交，必须从订单列表和子单分别核实。
- `src/orders/user_stream.rs`：只接收 Spot `executionReport`、USD-M `ORDER_TRADE_UPDATE` 的字段契约。仅 `x=TRADE` 且拥有有效成交 ID、正数量/价格时产生 `TradeEvidence`；NEW/ACCEPTED/TRIGGERED/AMENDMENT/撤单事件不生成成交。断线、缺消息、逆序累计数量等使该订单冻结。
- `src/orders/trades.rs` + `VenueClient::query_order_trades`：Spot `myTrades` 与 USD-M `userTrades` 通过 `fromId` 循环最多 32 页，要求真实 Trade ID 严格递增且订单身份相同。即使分页结束，也必须把数量汇总与已签名查询的订单累计成交数量比较；不能以空结果证明历史完整。

## Runtime / Ledger 接线要求（未改动）

1. Runtime 先通过风控，确认账户、产品、策略/人工指令、风险预算和目标订单参数。为**每一条可能写到交易所的动作**单独分配稳定 request_id 与 clientOrderId/listClientOrderId/clientAlgoId。撤单、改单、替换单也必须预登记，不是无副作用读操作。
2. 使用 `PreparedWrite::new(account_scope, request_id, product, action, symbol, client_id, exact_pairs)` 生成完整意图及参数 SHA-256。Ledger 在 `IntentJournal::admit` 中原子提交 `(tenant,account,environment,request_id,action,payload_sha256,client_id)` 唯一键；只有事务已提交且该键首次出现，才可返回 `Admission::Fresh`。旧键、摘要冲突、重启恢复、登记库损坏、未知结果都拒绝新的写入。
3. `dispatch_live(journal, client, credentials)` 只发送一次官方签名请求；回传是**订单受理观察，不是成交**。需要由 Ledger 持久化 `observe` 结果；如观察持久化失败也必须把意图视为 Unknown。任何已登记单的超时只能用 `query_order`、`query_spot_oco`、`query_spot_trigger_list`、`query_um_conditional`、`query_order_trades` 等 **GET** 取证。
4. 用户流处理先按账户+环境+产品+symbol+orderId+tradeId 去重；`StreamReconciler` 的 checkpoint、累计数量和真实 trade ID 要在 Ledger 原子保存。重启后保持冻结，直到完整签名 GET 查单 + 全页 trades + 成交数量一致，再执行 `recover_from_rest`。交易所查询因历史保留期返回空值、超限、重叠或截断，不代表确认无成交。
5. Basket/Portfolio 必须独立评估风险敞口。OTOCO 一条腿成交、另一腿拒绝/未知时禁止自动假定仓位已对冲；仍未完成风险确认的多腿订单不能推动下一阶段开仓。

**安全迁移边界：** crate 内旧 `VenueClient` 写方法签名保留，但底层 `signed()` 现拒绝未经 `IntentJournal` 登记的 POST/PUT/DELETE。Runtime 暂未改造，原写调用会失败关闭（不发真实订单）；不能把旧 Ledger 的 `save_order_request` 本身视为已经通过新门禁。新的 `PreparedWrite::dispatch_live` 是接线后的唯一真实写出口。

## 协议与旧 Python 对照

- Python：`engine/connectors/binance/manual.py` 提供显式订单/风险执行；`manual_algo.py` 管理条件单，`options_orders.py` 的未知写结果不会自动重发。Rust 的新门禁将“预登记→最多一次发送→未知只对账”固定为可模拟的接口。
- Binance：Spot 使用 `/api/v3/order`、`/api/v3/orderList/oco|oto|otoco`、`/api/v3/order/cancelReplace`；USD-M 普通单使用 `/fapi/v1/order`，条件单保留 `/fapi/v1/algoOrder`，改单为 `PUT /fapi/v1/order`；Options 使用 `/eapi/v1/order`。各市场的查询/身份字段不互换。
- 官方依据：[Spot REST Trade](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/trade)、[Spot account trade history](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-api/account)、[USD-M change log](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log)。

## 未完成/必须阻止启用的部分

- 尚未在 Runtime/Ledger 落实 Journal 的持久化实现、事务恢复、资金和跨腿预算实际门禁。
- Spot cancelReplace 有潜在一腿成功/一腿失败的混合回执：本增量只允许按 STOP_ON_FAILURE 提交并一律标 Unknown，尚未实现逐腿安全完成判定。Spot 改单 Keep Priority 不在此增量；USD-M 修改已提供受保护的 PUT 路由，实际改后数量差异须另行核对。
- Options 原生订单传输和 `OPTIONS` 用户流/期权行权未完成同构接线；保留已有期权成交查询实现，未声称完整迁移。
- Binance 订单历史保留/缺口、用户流启动和重连、全部 LIST_STATUS 事件、极端分页超过 32 页仍需端到端集成。不能把 32 页上限当成已验证完整历史。
- 此分支仅包含 mock/offline **测试代码**。当前环境没有 Cargo、Rustc 或可运行的仓库 CI；`cargo fmt`、`cargo clippy`、`cargo test`、Python parity 均**未运行**。

## 接入后要求执行

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
python3 -m unittest discover -s tests -t . -p '*_test.py' -v
```

测试网关只能是本地模拟 HTTP/WebSocket；不得配置真实密钥、调用真实订单接口、开启生产订单路由。

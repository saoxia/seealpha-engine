# seealpha-venue：订单安全门禁与集成契约

> 历史增量的安全契约说明，不代表当前 Runtime/Ledger 接线状态的实时验收报告。目录重构不启用实盘。

## 当前目录归属

`engine/connection/binance/` 是 **Binance 私有 REST 订单与账户适配器**；Binance 行情服务统一归属 `engine/pipeline/`，不在这里复制 NautilusTrader 能力。Cargo 包名 `seealpha-venue`、`seealpha_venue::*` 公共模块路径保持不变。

| 物理目录 | 职责 |
| --- | --- |
| `src/lib.rs` | 签名 HTTP 客户端、端点白名单、公共类型及公开模块声明 |
| `src/spot/` | Spot OCO（仍被 Runtime 使用） |
| `src/usd_m/` | USD-M 条件订单（Algo） |
| `src/options/` | 已迁移并清空，Options 专属证据与分页在 `binance-options/` |
| `src/orders/` | 尚在使用的预登记安全门禁与真实成交 REST 取证；通用订单生命周期已归 `engine/execution/` |
| `src/account/` | 多产品只读账户证据、账户对账及资金结算查询 |

仍有部分模块声明通过 `#[path]` 指向实体文件；执行生命周期与 Options 成交规范化的旧公开兼容转发已删除，实际实现分别保留在 `engine/execution` 和 `engine/connection/binance-options`。业务行为、凭据边界、账户安全门禁与 Ledger 事务归属不变。

## Options 成交分页协议抽离

已删除原 `src/options/trades.rs`：有界 EAPI GET /eapi/v1/userTrades 分页的游标、1000 条饱和页、32 页上限、重复 ID 与溢出失败关闭全部迁至 `binance-options/src/trades_page.rs`。旧 `VenueClient::option_order_trades` 只注入签名 GET，不维护第二套分页逻辑。旧 `seealpha_venue::option_trades::normalize_option_trades` 兼容转发已删除，调用方应使用 `seealpha_binance_options::normalize_option_trades`。

## Options 证据代码抽离（2026-11-21）

`normalize_option_trades` 及其真实成交证据校验、期权账户快照结构校验已搬至 `engine/connection/binance-options`。旧 `src/options/trades.rs` 已删除，`VenueClient::option_order_trades` 只提供签名 GET，`src/account/reconciliation.rs` 的 Options 分支只保留签名账户/持仓 GET 与错误映射。该迁移不改变用户成交归属、重复 ID 检查、成交数量/手续费字段或任何真实订单发送行为；不代表 Options 的整套交易网络接口已经迁移。

## EAPI 订单参数规则独立归属

原来 `VenueClient::place_order` 中期权专用的 LIMIT 类型、价格、TIF、`reduceOnly`、`postOnly` 和 `clientOrderId` 参数构造，以及订单 GET/DELETE 的 `clientOrderId` 查询参数、EAPI 订单路径常量，已迁至 `engine/connection/binance-options/src/order.rs`。签名、预登记门禁、未知结果判定和真实下单权限均仍由旧多产品 Venue 持有；此增量不创建新订单发送通道。

## Options 账户仓位与余额规范化抽离

`reconciliation::position_quantities` / `asset_quantities` 的 Options 分支已改为委托 `seealpha_binance_options::account`：保持原 `quantity/positionAmt`、`marginBalance/balance/walletBalance` 回退优先级、持仓方向及重复键校验。通用对账差额算法及 Spot、USD-M 原有证据规则不变。

## Options 行权证据抽离

旧 `src/account/settlement.rs` 现仅保留签名 `GET /eapi/v1/exerciseRecord`、时间窗口和满页失败关闭，以及兼容公开函数；Options 行权记录解析和去重由 `binance-options/src/settlement.rs` 唯一实现，USD-M 资金费收入逻辑仍保留原位，未重新实现交易所网络能力。

## 执行领域状态机从连接器抽离

订单状态单调推进、终态不可回退及成交数量一致性检查已从 `src/orders/lifecycle.rs` 搬到 `engine/execution/src/lifecycle.rs`（`seealpha-execution`），成为交易所无关的 SeeAlpha 执行领域规则。Runtime 直接依赖新 crate，旧 `seealpha_venue::lifecycle` 类型转发已删除，Venue 对外错误仍映射为原来的 `Unconfirmed`。这是**搬迁单一实现**，不添加订单 HTTP/WS 客户端，且不改变 Ledger 事务归属。

## 自制用户流移除与恢复门禁

固定版本 NautilusTrader v2.0.0rc9 已提供原生 Spot/USD-M 执行事件及重连/查询能力。本仓库原 `orders/user_stream.rs` 的 `parse_order_event`、`StreamReconciler`、`TradeEvidence` 都没有 Runtime 生产调用方，仅有自包含测试。现从旧连接器删去这一份重复解析/状态机，不重写自有 WebSocket 与自有成交消息存储。

**安全边界：** 旧 Runtime 的 loopback-only 写入门禁未改变；已登记请求状态仍是 Unknown 优先，只有现有签名 REST 成交核查方能提供证据。尚未接入原生 Nautilus 订单事件到 SeeAlpha Ledger；这一删除**不表示生产流式恢复可用**。

## 当前原生能力与复用范围

原有 `src/spot/oco.rs`（Spot OCO）、`src/usd_m/conditional.rs`（USD-M Algo）仍保留。Options 成交分页已由独立 `binance-options` 模块唯一实现。旧 `src/options/execution.rs` 仅用于离线原型的 `OptionsExecutor`、`src/options/rules.rs` 中未被运行时引用的测试辅助规则已删除；真实订单仍由 Venue/Runtime 使用预登记门禁处理，`binance-options` 保留实际使用的专属解析与 EAPI 订单参数规则。`engine/execution/src/lifecycle.rs` 提供数量严格校验、终态及单调状态迁移。仍在旧适配器的能力：

- `src/orders/guard.rs`：按产品限定 Binance POST/PUT/DELETE 路径，提交一次之前调用持久化 `IntentJournal::admit`。相同账户与 request_id 再次出现时只允许 GET 对账；无法确证的 HTTP 状态、超时、未知响应、ACK 字段不匹配都归于 Unknown，**绝不自动补发**。
- `src/spot/oto.rs`：历史 OTO/OTOCO 原型**没有 Runtime 调用方**，已删除；固定版本 NautilusTrader 2.0.0rc9 的原生 Spot 列表只支持 OCO。OTO/OTOCO 在当前 SeeAlpha 不对外提供，不可默默退化为 OCO 或假定已支持。
- `src/orders/user_stream.rs`：历史 Spot/USD-M 自制 User Stream 状态机未被 Runtime 接线，现已删除；不应与 NautilusTrader 2.0.0rc9 原生用户流和执行事件重复实现。**当前 Runtime 不会自动消费原生执行流、不能宣称具备流恢复能力。**
- `src/orders/trades.rs` + `VenueClient::query_order_trades`：Spot `myTrades` 与 USD-M `userTrades` 通过 `fromId` 循环最多 32 页，要求真实 Trade ID 严格递增且订单身份相同。即使分页结束，也必须把数量汇总与已签名查询的订单累计成交数量比较；不能以空结果证明历史完整。

## Runtime / Ledger 接线要求（未改动）

1. Runtime 先通过风控，确认账户、产品、策略/人工指令、风险预算和目标订单参数。为**每一条可能写到交易所的动作**单独分配稳定 request_id 与 clientOrderId/listClientOrderId/clientAlgoId。撤单、改单、替换单也必须预登记，不是无副作用读操作。
2. 使用 `PreparedWrite::new(account_scope, request_id, product, action, symbol, client_id, exact_pairs)` 生成完整意图及参数 SHA-256。Ledger 在 `IntentJournal::admit` 中原子提交 `(tenant,account,environment,request_id,action,payload_sha256,client_id)` 唯一键；只有事务已提交且该键首次出现，才可返回 `Admission::Fresh`。旧键、摘要冲突、重启恢复、登记库损坏、未知结果都拒绝新的写入。
3. `dispatch_live(journal, client, credentials)` 只发送一次官方签名请求；回传是**订单受理观察，不是成交**。需要由 Ledger 持久化 `observe` 结果；如观察持久化失败也必须把意图视为 Unknown。任何已登记单的超时只能用 `query_order`、`query_spot_oco`、`query_spot_trigger_list`、`query_um_conditional`、`query_order_trades` 等 **GET** 取证。
4. 尚未接线的原生用户流不得参与 Ledger 记账或解除 Unknown。将来接入 NautilusTrader 2.0.0rc9 时，应按账户+环境+产品+symbol+orderId+tradeId 去重，并在 Ledger 原子存储证据检查点、累计数量与交易 ID；连接中断/缺消息/错误顺序必须冻结，只有完整签名订单/成交核验和数量一致后才能恢复。交易所查询因历史保留期返回空值、超限、重叠或截断，不代表确认无成交。
5. Basket/Portfolio 必须独立评估风险敞口。当前不支持 OTO/OTOCO；在原生能力或经验证的替代实现到位前，不能用普通下单自动拼装多腿并假定已对冲，仍未完成风险确认的多腿订单不能推动下一阶段开仓。

**安全迁移边界：** crate 内旧 `VenueClient` 写方法签名保留，但底层 `signed()` 现拒绝未经 `IntentJournal` 登记的 POST/PUT/DELETE。Runtime 暂未改造，原写调用会失败关闭（不发真实订单）；不能把旧 Ledger 的 `save_order_request` 本身视为已经通过新门禁。新的 `PreparedWrite::dispatch_live` 是接线后的唯一真实写出口。

## 协议与旧 Python 对照

- Python：`engine/connectors/binance/manual.py` 提供显式订单/风险执行；`manual_algo.py` 管理条件单，`options_orders.py` 的未知写结果不会自动重发。Rust 的新门禁将“预登记→最多一次发送→未知只对账”固定为可模拟的接口。
- Binance：Spot 使用 `/api/v3/order`、`/api/v3/orderList/oco`、`/api/v3/order/cancelReplace`；USD-M 普通单使用 `/fapi/v1/order`，条件单保留 `/fapi/v1/algoOrder`，改单为 `PUT /fapi/v1/order`；Options 使用 `/eapi/v1/order`。各市场的查询/身份字段不互换。
- 官方依据：[Spot REST Trade](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/trade)、[Spot account trade history](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-api/account)、[USD-M change log](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log)。

## 未完成/必须阻止启用的部分

- 尚未在 Runtime/Ledger 落实 Journal 的持久化实现、事务恢复、资金和跨腿预算实际门禁。
- Spot cancelReplace 有潜在一腿成功/一腿失败的混合回执：本增量只允许按 STOP_ON_FAILURE 提交并一律标 Unknown，尚未实现逐腿安全完成判定。Spot 改单 Keep Priority 不在此增量；USD-M 修改已提供受保护的 PUT 路由，实际改后数量差异须另行核对。
- Options 原生订单传输和 `OPTIONS` 用户流/期权行权未完成同构接线；保留已有期权成交查询实现，未声称完整迁移。
- Binance 订单历史保留/缺口、用户流启动和重连、全部 LIST_STATUS 事件、极端分页超过 32 页仍需端到端集成。不能把 32 页上限当成已验证完整历史。
- 本轮删除的是无 Runtime 调用方的旧模块。应由 GitHub Actions 检查 Rust/Docker 兼容性；Nautilus 事件接入、实际断线恢复、账本投影未验收。

## 接入后要求执行

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked --release
python3 -m unittest discover -s tests -t . -p '*_test.py' -v
```

测试网关只能是本地模拟 HTTP/WebSocket；不得配置真实密钥、调用真实订单接口、开启生产订单路由。

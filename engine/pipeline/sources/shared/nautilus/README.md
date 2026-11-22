# 当前执行架构（2026-11-22 目录切换）

旧 `engine/connection/binance` / `seealpha-venue` 以及未被其他 crate 使用的 `engine/connection/binance-wire` 均已从仓库和 Cargo workspace 移除；Spot / USD-M 原生执行方向仅保留此独立 NautilusTrader 0.68.0 工厂。**注意这只是源代码依赖切换，不表示功能完全迁移：** Runtime 的 `execute_order` 仍只有 Ledger 原生意图登记、不连接原生下单命令；原生执行事件采用 Nautilus 原生 Streaming/Catalog 记录类型的离线配置契约，尚无实际原生执行事件生产者或已验收的持久化运行链路。旧账户签名查询、历史订单/成交及结算查询接口已经显式 fail-closed，不能再提供私有数据，旧订单无法从原网络查询恢复。Options 没有被 Spot/USD-M 原生执行工厂取代。

**不得启用实盘交易。** 删除目录的请求不等于安全生产验收，也不等于原生账户、成交取证已实现。本次按用户要求未运行 Cargo 编译和测试。下文关于旧目录尚在或不得删除的历史迁移描述是过去阶段的记录，以本节当前状态为准。

# NautilusTrader 原生 Binance Spot / USD-M 行情节点

这是 **NautilusTrader v2.0.0rc9 / Rust crates 0.68.0** 的只读、实验性原生行情节点；目前已由管理员显式开关接入现有 Source Supervisor，但不在默认生产镜像中运行，也不提供交易执行能力。

- 数据请求：原生 `BinanceDataClientFactory` / `LiveNode` / `DataActor::subscribe_trades` / `request_trades`。
- 多交易对 WebSocket、重连与 REST 请求的限额行为由 NautilusTrader 实现，没有 SeeAlpha 自定义连接客户端。
- Spot 强制使用匿名公开 JSON `@trade`，USD-M 原生实现使用 `@aggTrade`。
- 原生 `StreamingConfig` 将 `TradeTick` 写入 Feather 暂存，并在正常关闭时迁移到 Parquet Catalog。
- 默认仅运行单产品；同时采集 Spot 和 USD-M 时运行两个独立节点。这避免一个进程中全局 Nautilus runtime 状态碰撞。
- `on_start` 对每个交易对调用一次 Nautilus 原生有限 REST 最近成交请求，最多 1,000 条。**返回不证明过去的完整区间**，也不对源缺口执行自定义补齐。

## 离线 LiveNode 执行接线构建验证（无交易）

`tests/native_execution_node.rs` 在现有固定版本的 NautilusTrader v2.0.0rc9 下分别构造 Spot 与 USD-M `LiveNode` 配置：原生 DataClient + ExecutionClient 工厂、显式 Hedging OMS、Native LiveExecutionEngine 检查间隔及 `with_reconciliation(true)`。测试只执行 `build()` 并销毁节点；不会 `run()`、访问交易所、发送订单或安装到 Engine Runtime。这是调用契约验证，不是恢复、账户安全或实盘验收。

## 替换旧 Venue 的明确目标（2026-11-22）

**最终目标不是把旧 `engine/connection/binance` 重新拆成一套自研 Binance 下单客户端。** Spot 和 USD-M 标准订单必须接入固定版本 NautilusTrader `BinanceExecutionClientFactory`、`LiveNode` 以及 Nautilus 原生 ExecutionEngine/RiskEngine；遗留 `binance-wire` 已清理，不再保留第二条自研 Binance 签名/REST 下单通道。

当前新增 Spot LiveNode 离线构建契约，与已有 USD-M Hedging 离线构建契约并列；二者**均不运行节点、不触网、不提交订单、不接入生产 Runtime**。接管顺序及验收条件：

1. 明确 Engine 账户/租户/产品到 Nautilus `AccountId`、`InstrumentId`、OMS 的稳定映射，Spot 固定 Hedging，USD-M 依交易所持仓模式明确选择 Netting/Hedging；不凭空切换模式。
2. 让 SeeAlpha 风控及 Ledger 对每一条提交、撤单、改单的**不可变指令**先做持久化预登记，且在 Nautilus 发送前确认首次允许执行；断线、超时和未知响应只查证不重发。**目前 Nautilus 自带执行客户端并不能自动满足 SeeAlpha 的持久预登记契约，未经接线不得运行真实提交。**
3. 只使用 Nautilus 原生执行报告、账户同步和必要的交易所签名成交/订单证据推进 Ledger；真实 trade ID、订单归属、累计数量、手续费、重启重复事件的去重在同一 Ledger 事务内完成。ACK 不构成成交。
4. 单独核验 Spot OCO、USD-M Algo、订单替换和撤单竞争的原生支持情况。不支持时明确拒绝，不通过普通订单进行功能降级。
5. 在 mock/sandbox 逐项验证重启恢复、断线重连、顺序错乱、重复报告、部分成交、双向仓位、限额和风控拒绝，解决 rc9 Streaming LiveNode 停止/销毁问题，完成 CI 后再考虑独立受控灰度。

Options 不属于现有 Nautilus 原生 Binance Spot/USD-M 执行覆盖范围；另行选择独立的 Nautilus ExecutionClient 适配器或受控 Options 网关，**不能**用 Spot/USD-M 原生工厂替代 EAPI。旧 Venue 和未使用的 `binance-wire` 已清理，但上述原生执行能力仍未完成实际切换；目录清理不构成生产验收。

## 原生执行路由身份约束（2026-11-22）

新增 `src/native_execution.rs::resolve_native_target`，供原生执行节点工厂使用同一份交易品种、账户 ID、合约 ID 和 OMS 映射。当前离线 Spot/USD-M LiveNode 测试已改为从此函数取得 Binance product / AccountId / OmsType，而不是各自拼接。在未核实合约、交易所仓位模式、账户身份时失败关闭：

- Spot 必须 `PositionMode::Spot`，对应原生 `OmsType::Hedging` 和 `BTCUSDT.BINANCE` 一类 InstrumentId。
- USD-M 必须显式核实 `OneWay` 或 `Hedge`，对应原生 `Netting` 或 `Hedging`；InstrumentId 采用 `BTCUSDT-PERP.BINANCE`。
- 不接受 Options/EAPI、Spot OCO、USD-M Algo、撤单替换和不认识的操作类别；这些路径不能静默降级为普通单。未核实仓位模式、非法合约/账户标识一律拒绝。

**此函数不是发送授权，不创建执行引擎、订单或凭据。** Runtime 尚未消费此路由契约；下一步要把 Ledger 不可变意图、风险判断及重启恢复与 Nautilus 原生命令调度接起来，并经过真实编译、模拟网络与故障注入验收后才能安全代替旧 Venue。

## Runtime 原生订单意图持久化增量（2026-11-22）

`engine/runtime/src/native_handoff.rs` 新增内部 `stage_native_intent`，使用**现有** `Ledger::preregister_order_intent` 原子登记不可变原生订单意图。支持经校验的 Spot/USD-M 单腿 LIMIT 订单元数据、账户/合约/持仓模式、精确 Decimal 字符串，将它们一起纳入 SHA-256 指纹并绑定 `execution_backend=nautilus_binance_native`。新的指令首次登记为 `StagedFirstAttempt`；同一请求重放为 `ReconcileOnly`，冲突请求拒绝；SQLite 重启后也不会产生第二次首次登记。

**当前该模块完全不发送订单、并且尚未接入 Runtime::execute_order。** `StagedFirstAttempt` 不是交易授权；不能被任何原生客户端直接用作实盘发送许可。后续还必须接入风控账户预算、原生 OrderFactory 与执行命令、原生事件/订单/成交证据对账，确认 Ledger 原子持久化及重启恢复无漏洞后，才能替换旧 Venue。当前不引入 Nautilus crates 到 Rust Runtime 的主工作区，避免未验收的生产依赖切换。

## Engine Runtime 原生后端入口接线（2026-11-22）

`Runtime::execute_order` 现识别显式 `values.execution_backend="nautilus_binance_native"`，在**原有 loopback-only 主机门禁和正式租户校验之后**调用 `stage_native_order_command`，从而将标准 Spot/USD-M 单腿 GTC LIMIT 的订单意图写入现有 Ledger 原子事务，而**不再调用旧 Venue 的任何下单函数**。该入口要求完整的 `request_id`、`environment`、`product`、`symbol`、`side`、`order_type`、`quantity`、`price`、`position_mode`、`time_in_force`，只接受已验证的完整字段集合，不忽略 Reduce Only、OCO、Algo 等额外字段；未知后端拒绝而不是回退旧连接器。

返回 `response.status="unknown"`、`reconciliation_only=true`、`ledger_synced=false`；同一请求重放带 `replay=true`，参数冲突拒绝。原生 Ledger 意图的恢复不得回退到旧 Venue REST 查询。离线测试覆盖首次调用、重复请求、参数冲突、未支持字段、空真实成交及凭据未持久化。

**这不是原生下单完成：** `execute_order` 还没有连接 Nautilus ExecutionEngine 的原生订单命令，首次登记也不授予实盘发送权；`reconcile_order` 对原生意图暂时拒绝，直到单独构建并验收原生证据回写。这一增量只是把 Runtime 的显式原生请求从旧 Venue 处理路径移出来；真实外部 Binance 仍被原有主机门禁禁止。

## Runtime 旧 REST 订单写入路径正式停用（2026-11-22）

`Runtime::execute_order` 已删除通过 `VenueClient::place_order` / `cancel_order` / Spot OCO / USD-M Algo 发送的旧代码分支。现在只接受显式 `execution_backend="nautilus_binance_native"` 并进入 Ledger 的原生意图登记模块；没有后端标识、未知后端或旧格式命令都会在注册订单意图和网络副作用之前失败关闭。原有 loopback-only 主机门禁、正式租户校验仍保留。

**重要：** 当前原生 Runtime 路由仍是只登记、不下单；原生 Nautilus ExecutionEngine 实际发送和 Ledger 成交事件闭环仍待接线。旧 `engine/connection/binance` 暂不可删除，因为 Runtime 的只读账户查询、历史成交核查、订单恢复以及测试中的 Journal 仍引用其公共接口；直接删除将破坏证据链。下一步必须将这些能力迁至原生执行/账户事件处理和独立的 SeeAlpha 领域接口，全部测试通过后才能移除 crate。

## 使用 Nautilus 原生执行记录持久化（离线配置验证）

NautilusTrader v2.0.0rc9 在 `NautilusRecordType` 中已有订单提交、接受、拒绝、取消、成交、冲销以及 `OrderStatusReport`、`FillReport`、`PositionStatusReport`、`AccountState`；`StreamingConfig.record_types` 可选择这些原生记录，写入 Nautilus 自带 Feather/Parquet Catalog。离线测试验证 `data_types=TradeTick`、原生执行 `record_types` 和 `.with_streaming_config(...)` 原生 Builder 接口，不创建自定义执行事件 JSON/SQLite/Parquet 写入器。注意：rc9 版本在构建**带 Streaming 的 LiveNode 后销毁节点**时，CI 曾触发 Tokio `failed to park thread: AccessError`；目前该测试仅验证 Streaming 配置与 Builder 的编译契约，单独构建**未启用 Streaming** 的原生执行节点。**Streaming 节点的运行/停止/销毁生命周期尚未验收，不能部署或宣称已接线**。

测试仅验证 **配置与节点构建**，并没有让 `LiveNode` 实际运行，因而**不宣称已写出任何执行记录**，也不代表记录已进入 SeeAlpha Ledger。后续必须实现 Catalog 读取、订单归属、去重、签名成交明细对照及 Ledger 事务投影；订单 ACK 不得冒充 `OrderFilled` 或经 `import_verified_fills` 直接入账。

## NautilusTrader rc9 原生执行能力接线检查（尚未启用交易）

当前模块增加离线 factory 契约测试：使用 **rc9 / Rust 0.68.0** 原生 `BinanceExecutionClientFactory` 和 `BinanceExecutionClientConfig`，验证 Spot 使用 Hedging OMS，USD-M 的 Netting/Hedging 选择及配置合法性。测试只构造原生执行客户端，不调用连接、下单、撤单或真实私有接口；运行时继续只启动匿名行情客户端。

**这不等于执行迁移完成。** 在替换 `engine/connection/binance` 之前，需要确保：账户凭据安全交接、订单预登记与幂等、未知结果恢复、Spot OCO/OTO/OTOCO、USD-M Algo、双向持仓、订单归属和 Ledger 真实成交证据的端到端对照。未通过这些门禁，不允许实盘或删除旧订单代码。

## 离线检查和显式运行

在编译机先运行：

```sh
cargo test --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml
cargo check --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml --all-targets
```

明确准备**既有** Feather 和 Catalog 根目录后，运行：

```sh
cargo run --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml -- \
  spot BTCUSDT,ETHUSDT /data/seealpha/feather /data/seealpha/catalog

cargo run --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml -- \
  futures_um BTCUSDT,ETHUSDT /data/seealpha/feather-um /data/seealpha/catalog-um
```

**安全与完整性限制：** 不在 Docker 镜像或 Engine Runtime 中安装/自动执行该入口；无交易所账户私钥；不注册订单客户端；不创建或变更数据采集 READY 标志；没有定期 Parquet promote，仅对正常结束触发 promote。崩溃时保留 Feather 并需通过原生 recovery 验证。

原生节点第一轮独立编译、离线单测和 Clippy 曾通过；后续增加的导入器、锁、租户任务接口必须以最新提交为准重新验证。完整 WS/REST/ZIP 覆盖、数据目录交接和下游消费者仍需验收。旧实验采集器、SQLite Journal 和自建 Parquet 转换器已从当前仓库删除；新节点仍保持显式实验开关。

## 通过现有 Engine Source Supervisor 显式试运行

在已经离线构建、检验原生 Rust 节点的主机上，由管理员先准备对应租户、产品的**真实目录**（不可使用软链接），然后设置：

```sh
export ENGINE_EXPERIMENTAL_SOURCE=1
export ENGINE_NATIVE_NAUTILUS_SOURCE_BIN=/opt/seealpha/bin/seealpha-nautilus-binance-data
export ENGINE_NATIVE_NAUTILUS_FEATHER_ROOT=/data/nautilus-feather
export ENGINE_NATIVE_NAUTILUS_CATALOG_ROOT=/data/nautilus-catalog
mkdir -p /data/nautilus-feather/42/spot /data/nautilus-catalog/42/spot
```

调用已经存在的 `/tenants/{tenant_id}/data/control/source/{product}/start` 路由时，Supervisor 将现有租户已启用的 Spot / USD-M 行情策略转换为原生 Nautilus 的多交易对订阅（实验节点每次最多 100 个交易对）；不新增第二套订阅数据库和自动调度。停止路径向子进程发送 `SIGTERM`，以支持 Nautilus 优雅关闭与 Parquet 提交；停止仍不代表历史完整性已核验。旧 SQLite K 线计算器和相关 HTTP 端点已删除，原生 Tick 不会伪装为已验证的历史 K 线。

**未启用以上环境变量时**，Source Supervisor 拒绝启动行情采集，不再回退到已删除的 `binance-native-source`。此原生入口默认不在生产镜像里；外部二进制须由维护者手动验证后部署。所有状态仍是 `DEGRADED`，禁止策略把连接状态作为完整行情就绪证据。


## 离线历史 ZIP 导入

新增 import_archive 二进制。输入 Binance 官方 ZIP 与先前校验通过的 SHA-256；按 Spot trades 或 USD-M aggTrades 格式读取，构造 NautilusTrader 原生 TradeTick 并调用 Nautilus 原生 ParquetDataCatalog 写入，不新建 Arrow/Parquet 实现。

```sh
cargo test --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml --all-targets
cargo run --manifest-path engine/pipeline/sources/shared/nautilus/Cargo.toml --bin import_archive -- \
  spot trades ETHUSDT 2026-11-20 /data/archives/ETHUSDT-trades-2026-11-20.zip \
  /data/catalog/42/spot <official-sha256> <instrument-price-precision> <instrument-size-precision>
```

Runtime archive_import 任务只接受 archive_job_id，对应同租户、已完成且验证通过的 archive_fetch 任务。管理员还需提供 ENGINE_NATIVE_NAUTILUS_IMPORT_BIN，目标 Catalog 必须已创建。实时节点和离线导入器持有同一个跨进程 Catalog writer lock，避免 SeeAlpha 受管进程同时写入。对外仍不报告 READY；跨日连续性、REST/WS 接续、Catalog 读取回测一致性以及真实 Binance 数据验收仍是 release gate。

**精度要求**：Binance ZIP 中 `price` 与 `qty` 通常以固定宽度小数字符串表示，不等于 Nautilus Instrument 的价格/数量精度。`import_archive` 现强制要求最后两个参数是已经核实的 Instrument `price_precision`、`size_precision`（0–16），使用 `Price::from_decimal_dp` / `Quantity::from_decimal_dp` 做可精确表示的转换；无法无损转换的记录将被拒绝。不要直接采用 CSV 的小数位数。此版本尚未在 Runtime 内自动从上游 Nautilus Instrument Catalog 读取精度，因此导入需要明确提供精度且不能报告 READY。

## Ledger Basket Journal 去 Venue 化（2026-11-22）

历史说明更新：旧 Venue 离线发送模拟和未接入生产的 Basket 账户快照 CAS 原型现已移除。通用订单意图仍使用 Ledger 原子预登记、重复请求仅对账且不产生伪造成交；账户余额与持仓由 Nautilus 原生状态管理。Runtime 生产账户接线尚未验收，不允许由这个测试网节点授权真实发单。

这删除了一项实际的 `seealpha-venue` Runtime 依赖，但账户与订单/成交的签名只读证据链仍在旧连接器中，不能据此删除 `engine/connection/binance`。接下来需要完成 Nautilus 原生认证执行报告/账户同步与 Ledger 原子验证，清除剩余只读依赖后再删除该目录。当前不开放原生真实下单。

## 原生订单与旧只读取证隔离（2026-11-22）

原生订单登记在 Ledger 后，`reconcile_order` 已拒绝旧 Venue 查询；本次进一步让后台 `run_one_registered_order_reconciliation` 在提取任何旧 `VenueClient` 或账户凭据前检查 `intent.execution_backend`，原生订单只返回待核实错误并由已有任务机制保持 `retry`，绝不会通过旧 REST 查询生成签名订单证据。直接 `sync_verified_order_fills` 也拒绝原生意图的历史 `order_requests.intent` 路由，避免误用旧成交格式计入 Ledger。

回归测试覆盖已登记账户凭据与原生订单同时存在时：无可信订单观察、无成交、未完成任务保持重试。原生 Nautilus `FillReport` / `OrderStatusReport` 的事件来源认证、同租户账户映射、官方成交记录对照仍须单独完成；**不能**将普通 ACK 或未验证报告作为 `signed_exchange_user_trades` 写入。此增量未触发真实网络发送。

## 原生订单事件记录交由 Nautilus（2026-11-22 第一批去重）

原 Ledger native_events.rs 仅在测试中写入未认证的事件元数据，没有任何生产 Nautilus 事件订阅器调用，因此第一批去重移除此模块及其写入 API，新建 Ledger 数据库不再创建 native_order_event_quarantine。已有 SQLite 中该表的数据**不会 DROP、清空或重写**；如有历史记录，应先独立归档、审计后再考虑数据库迁移。

执行事件持久化应使用 Nautilus 原生 StreamingConfig.record_types 与 Feather/Parquet Catalog，类型清单统一定义在 native_execution::native_execution_record_types（包括订单生命周期、OrderFilled/FillReport、PositionStatusReport 与 AccountState）。离线 tests/native_execution_node.rs 使用此清单验证固定版本的编译契约和配置可用性；**尚未启动带 Streaming 的执行节点、未验证实际写盘及重启恢复**，此前 rc9 的 Streaming LiveNode 销毁异常尚需解决。

SeeAlpha Ledger 仍保留可信签名成交、结算和不可变下单前意图的必要独立证据。Nautilus 的 OrderFilled / FillReport 不自动成为交易所认证成交，也不能直接绕过 verified_fills 的核验路径。后续接线需要核验租户/账户绑定、真实 Trade ID、重复事件、Catalog 完整性、持久化中断及重启，且在此之前 **Runtime 实盘发送仍关闭**。具体重叠能力矩阵见 [Ledger/Nautilus 边界审计](../../../../../docs/architecture/LEDGER_NAUTILUS_OVERLAP_20261122.md)。

## AccountState 原生只读报告门禁（2026-11-22）

新增 `src/native_account.rs::inspect_native_account_report`，直接使用 **固定 NautilusTrader Rust 0.68.0** 的 `AccountState` 类型，不自行设计 Binance 账户资产 JSON 模型。对输入 AccountState 检查：预期 Nautilus AccountId、AccountType、`is_reported`、事件与初始化时间有效性、观察时点未来/过期边界；只返回 AccountId、事件 ID 与纳秒时间戳的候选元数据。

**`is_reported=true` 只表示 Nautilus 此事件属于交易所上报类别，不是认证真实性的充分条件。** 接受该候选元数据并不会存入 Ledger、修改现金/仓位或赋予下单权限；必须由未来可信的原生 ExecutionClient/Report 生产者绑定 tenant/account/environment，并核验完整性和账户身份后，才可能成为 Ledger 可信快照。当前 Runtime 仍不能读取真实账户。

离线单元测试使用真实 `nautilus_model::events::account::state::AccountState` 构造器覆盖允许条件、系统本地计算事件、账户身份不匹配、类型不匹配、过期、未来时间和无效窗口。尚未验证原生账户事件的实际接收、断线重连、历史补齐或运行中 Streaming 完整性，不能据此开放生产账户或订单执行。

## Native virtual attribution projection (incremental cutover)

`native_virtual_attribution::project_native_order_observation` now links the
native typed event observation with the **post-event Nautilus OrderCore**
(`filled_qty`) and a durably registered SeeAlpha Coordinator. Neither the
observer nor its projection maintains an independent venue order-status map.
The upstream `binance_account::attach_native_virtual_attribution` reuses the
single typed native order subscription and passes the read snapshot's exact revision into an injected CAS commit before
releasing virtual member fills. A stale revision or failed commit publishes no fills;
the Binance account subscription includes explicit regression coverage for this gate.

- `OrderAccepted` is never a fill. `OrderFillVoided` freezes attribution
  pending native account/order reconciliation; it does not reverse Ledger cash.
- Partial executions require the native Core cumulative quantity to advance
  by exactly the native event's last quantity. A stale, ahead-of-event or
  contradictory cache snapshot freezes instead of constructing synthetic fills.
- Callers must supply an authenticated live-node event, a registered binding,
  an account/instrument-matched native OrderCore and an atomic durable CAS
  implementation. This module itself grants no execution permission.
- The Runtime currently retains a **development-paper-only** coordinator and
  basket journal. The separate pinned Nautilus packages remain excluded from
  the main workspace. The callback-to-Runtime production transaction, complete
  restart recovery, fill-void replay and ordered native cache acceptance tests
  are **not yet integrated or validated**. Live trading stays disabled.

When wiring the production runtime, use Nautilus as the sole OMS and keep
only SeeAlpha virtual-member allocation and immutable intent association.
Do not enable the live sender based solely on this observer.

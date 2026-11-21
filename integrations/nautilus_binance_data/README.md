# NautilusTrader 原生 Binance Spot / USD-M 行情节点

这是 **NautilusTrader v2.0.0rc9 / Rust crates 0.68.0** 的可执行验证入口，尚未接入当前 Engine 生产或实验 source supervisor。它不提供任何交易执行能力。

- 数据请求：原生 `BinanceDataClientFactory` / `LiveNode` / `DataActor::subscribe_trades` / `request_trades`。
- 多交易对 WebSocket、重连与 REST 请求的限额行为由 NautilusTrader 实现，没有 SeeAlpha 自定义连接客户端。
- Spot 强制使用匿名公开 JSON `@trade`，USD-M 原生实现使用 `@aggTrade`。
- 原生 `StreamingConfig` 将 `TradeTick` 写入 Feather 暂存，并在正常关闭时迁移到 Parquet Catalog。
- 默认仅运行单产品；同时采集 Spot 和 USD-M 时运行两个独立节点。这避免一个进程中全局 Nautilus runtime 状态碰撞。
- `on_start` 对每个交易对调用一次 Nautilus 原生有限 REST 最近成交请求，最多 1,000 条。**返回不证明过去的完整区间**，也不对源缺口执行自定义补齐。

## 离线检查和显式运行

在编译机先运行：

```sh
cargo test --manifest-path integrations/nautilus_binance_data/Cargo.toml
cargo check --manifest-path integrations/nautilus_binance_data/Cargo.toml --all-targets
```

明确准备**既有** Feather 和 Catalog 根目录后，运行：

```sh
cargo run --manifest-path integrations/nautilus_binance_data/Cargo.toml -- \
  spot BTCUSDT,ETHUSDT /data/seealpha/feather /data/seealpha/catalog

cargo run --manifest-path integrations/nautilus_binance_data/Cargo.toml -- \
  futures_um BTCUSDT,ETHUSDT /data/seealpha/feather-um /data/seealpha/catalog-um
```

**安全与完整性限制：** 不在 Docker 镜像或 Engine Runtime 中安装/自动执行该入口；无交易所账户私钥；不注册订单客户端；不创建或变更数据采集 READY 标志；没有定期 Parquet promote，仅对正常结束触发 promote。崩溃时保留 Feather 并需通过原生 recovery 验证。

下一阶段必须经 native 0.68.0 CLI 编译和自动测试验证，并打通当前 Runtime 的租户、数据根目录、订阅配置和 source journal。验证后才能移除实验性自研 Spot/USD-M websocket/REST 实现。不满足时保留现状，不进行默认行为切换。

## 通过现有 Engine Source Supervisor 显式试运行

在已经离线构建、检验原生 Rust 节点的主机上，由管理员先准备对应租户、产品的**真实目录**（不可使用软链接），然后设置：

```sh
export ENGINE_EXPERIMENTAL_SOURCE=1
export ENGINE_NATIVE_NAUTILUS_SOURCE_BIN=/opt/seealpha/bin/seealpha-nautilus-binance-data
export ENGINE_NATIVE_NAUTILUS_FEATHER_ROOT=/data/nautilus-feather
export ENGINE_NATIVE_NAUTILUS_CATALOG_ROOT=/data/nautilus-catalog
mkdir -p /data/nautilus-feather/42/spot /data/nautilus-catalog/42/spot
```

调用已经存在的 `/tenants/{tenant_id}/data/control/source/{product}/start` 路由时，Supervisor 将现有租户已启用的 Spot / USD-M 行情策略转换为原生 Nautilus 的多交易对订阅（实验节点每次最多 100 个交易对）；不新增第二套订阅数据库和自动调度。停止路径向子进程发送 `SIGTERM`，以支持 Nautilus 优雅关闭与 Parquet 提交；停止仍不代表历史完整性已核验。旧 SQLite K 线计算器无法直接消费该模式，故被显式禁用。

**未启用以上环境变量时**，原有实验性 `binance-native-source` 行为保持不变。此原生入口默认不在生产镜像里；外部二进制须由维护者手动验证后部署。所有状态仍是 `DEGRADED`，禁止策略直接将连接状态作为完整行情数据就绪证据。

# NautilusTrader 原生 Binance Spot / USD-M 行情节点

这是 **NautilusTrader v2.0.0rc9 / Rust crates 0.68.0** 的只读、实验性原生行情节点；目前已由管理员显式开关接入现有 Source Supervisor，但不在默认生产镜像中运行，也不提供交易执行能力。

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

原生节点第一轮独立编译、离线单测和 Clippy 曾通过；后续增加的导入器、锁、租户任务接口必须以最新提交为准重新验证。完整 WS/REST/ZIP 覆盖、数据目录交接和下游消费者仍需验收。正式切换之前不删除旧实验采集器。

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


## 离线历史 ZIP 导入

新增 import_archive 二进制。输入 Binance 官方 ZIP 与先前校验通过的 SHA-256；按 Spot trades 或 USD-M aggTrades 格式读取，构造 NautilusTrader 原生 TradeTick 并调用 Nautilus 原生 ParquetDataCatalog 写入，不新建 Arrow/Parquet 实现。

```sh
cargo test --manifest-path integrations/nautilus_binance_data/Cargo.toml --all-targets
cargo run --manifest-path integrations/nautilus_binance_data/Cargo.toml --bin import_archive -- \
  spot trades ETHUSDT 2026-11-20 /data/archives/ETHUSDT-trades-2026-11-20.zip \
  /data/catalog/42/spot <official-sha256> <instrument-price-precision> <instrument-size-precision>
```

Runtime archive_import 任务只接受 archive_job_id，对应同租户、已完成且验证通过的 archive_fetch 任务。管理员还需提供 ENGINE_NATIVE_NAUTILUS_IMPORT_BIN，目标 Catalog 必须已创建。实时节点和离线导入器持有同一个跨进程 Catalog writer lock，避免 SeeAlpha 受管进程同时写入。对外仍不报告 READY；跨日连续性、REST/WS 接续、Catalog 读取回测一致性以及真实 Binance 数据验收仍是 release gate。

**精度要求**：Binance ZIP 中 `price` 与 `qty` 通常以固定宽度小数字符串表示，不等于 Nautilus Instrument 的价格/数量精度。`import_archive` 现强制要求最后两个参数是已经核实的 Instrument `price_precision`、`size_precision`（0–16），使用 `Price::from_decimal_dp` / `Quantity::from_decimal_dp` 做可精确表示的转换；无法无损转换的记录将被拒绝。不要直接采用 CSV 的小数位数。此版本尚未在 Runtime 内自动从上游 Nautilus Instrument Catalog 读取精度，因此导入需要明确提供精度且不能报告 READY。

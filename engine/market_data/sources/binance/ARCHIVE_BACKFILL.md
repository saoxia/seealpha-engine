# Binance 历史归档补齐：NautilusTrader 优先

状态：**已具备归档下载、原生 TradeTick/Parquet 离线导入及实验性 Runtime 入口；数据覆盖、跨来源衔接和实盘 READY 尚未验收**。

## 边界

| 操作 | 应由谁负责 | 本次实现 |
| --- | --- | --- |
| Spot `@trade` / USD-M `@aggTrade` 实时订阅、多合约连接池、重连 | NautilusTrader `nautilus-binance` | 已删除旧 Rust WS 采集器；仅保留显式实验性 Nautilus 入口，未通过 READY 验收 |
| REST `request_trades`、`request_agg_trades` | NautilusTrader `BinanceSpotDataClient` / `BinanceFuturesDataClient` | 不增加另一套 REST 历史交易查询 |
| 日/月 ZIP 下载、官方 checksum 与来源记录 | SeeAlpha 仅做原生缺失部分 | 新增**每日** ZIP 下载 CLI；月归档和调度未实现 |
| `TradeTick`、历史持久化、流式写入和 Parquet Catalog | NautilusTrader 原生 Model / Persistence | 新增纯离线 ZIP→原生 Catalog 导入器；未替换全部旧转换器 |
| 历史覆盖、Trade ID 连续性、WS/REST/ZIP 交接与 READY | SeeAlpha 验证业务 | 仍需后续实证及跨模块验收，当前不得宣布 READY |

v2.0.0rc9 的 Nautilus Spot 实时订阅使用 `@trade`，USD-M 使用 `@aggTrade`。Binance 的 `trades` 和 `aggTrades` 是不同的数据产品；**不能把两者混同为无损一致的逐笔成交历史**。按研究需求分别下载：

- Spot 原始逐笔数据：`spot trades`，与原生 Spot `@trade` 类型对应。
- USD-M 与原生实时流同一聚合层级：`futures_um aggTrades`。
- USD-M 原始逐笔数据：`futures_um trades`，用于需要更细成交粒度的离线研究，**不应未经聚合映射就同原生 `@aggTrade` 接续**。

## 已实现：显式下载并验证单日 ZIP

首先创建一个预期由离线作业管理的**已存在**目录，然后运行：

```sh
mkdir -p /data/binance-archives
cargo run --locked -p seealpha-binance-market-data-source \
  --bin binance-trade-archive-fetch -- \
  spot trades ETHUSDT 2026-11-20 /data/binance-archives

cargo run --locked -p seealpha-binance-market-data-source \
  --bin binance-trade-archive-fetch -- \
  futures_um aggTrades ETHUSDT 2026-11-20 /data/binance-archives
```

Downloader 只访问固定的 `https://data.binance.vision` 官方路径，先获取同名 `.zip.CHECKSUM`、核对 SHA-256 与文件名，再有界流式接收 ZIP；校验唯一预期 CSV，完成 `fsync` 后原子硬链接安装，禁止覆盖已有不同内容的文件。存在的归档必须重新与官方 checksum 核验才能复用；拒绝子目录符号链接。操作不使用任何交易所账户密钥，也不生成订单。

输出是包含 `path`、`sha256`、`bytes`、`reused`、`archive` 和 `catalog_committed=false` 的 JSON。它不承诺 ZIP 已被逐行语义验证、数据完整无缺口或者 Catalog 已写入。

**数据文件位置**：`<root>/<spot|futures_um>/<trades|aggTrades>/<symbol>/<symbol>-<dataset>-<YYYY-MM-DD>.zip`。

> **已识别的旧代码问题**：现存 `src/trade_archive.rs` 对原始 Trade CSV 的列数假设为 Spot 8 列、USD-M 7 列，而 Binance 官方 `trades` 归档格式分别为 7 列和 6 列；Spot 自 2025-01-01 起时间戳还采用微秒。因此该旧聚合工具不可直接作为新下载文件的已验证导入器。本次新 CLI 只验证 ZIP 哈希与容器结构，**不调用**该解析器，也不声称已经得到完整的历史 Tick。

## 已接入 Runtime 的显式归档任务

现有 HTTP API 的 `POST /tenants/{tenant_id}/data/control/job/start` 可接收以下 payload。该接口会入队并通过**现有**任务执行机制调用同一个 ZIP 下载 CLI，没有第二套调度器。

```json
{
  "payload": {
    "action": "archive_fetch",
    "product": "spot",
    "symbol": "ETHUSDT",
    "dataset": "trades",
    "day": "2026-11-20"
  }
}
```

管理员须配置 `ENGINE_ARCHIVE_ROOT`，并提前创建 `<archive-root>/<tenant_id>/`。服务端只使用该受控路径，从请求体拒绝自定义目录；任务状态保存于已有的租户任务日志。下载完成只表示官方 SHA-256 ZIP 验证成功；回执强制 `data_ready=false`、`allow_new_risk=false` 和 `catalog_committed=false`。无自动交易；Catalog 导入必须通过独立的受控任务。

只有管理员同时设置 `ENGINE_EXPERIMENTAL_SOURCE=1` 与 `ENGINE_NATIVE_NAUTILUS_SOURCE_BIN`，并准备受控的 Nautilus Feather/Catalog 租户目录时，Runtime Source Supervisor 才会调用原生节点。**旧 `binance-native-source` 不再存在，也不再有自研 WS 回退路径**；未提供原生二进制时拒绝启动。该入口不属于默认生产切换。进程停止使用 SIGTERM，并始终保持 `data_ready=false`；旧 SQLite K 线计算器尚未与 Nautilus Catalog 兼容。

## 已实现：已验证 ZIP → 原生 Nautilus Catalog

原生 Rust 导入器位于 integrations/nautilus_binance_data/src/bin/import_archive.rs；它**不实现**自定义 TradeTick、Arrow Schema 或 Parquet Writer，而是复用 NautilusTrader v2.0.0rc9 原生模型及 ParquetDataCatalog::write_to_parquet。

对齐实时 TradeTick 粒度的组合严格限定为：

- Spot 的 trades（原始成交 ID；2025-01-01 起归档时间为微秒）
- USD-M 的 aggTrades（聚合成交 ID 与首末底层成交 ID；毫秒时间戳）

Spot aggTrades 与 USD-M 原始 trades 仍可下载，但**不能混入上述实时兼容 Catalog**。导入器完整验证 ZIP 哈希、CSV 列数、每行类型、成交 ID 连续性、聚合成交底层 ID 连续性、UTC 日范围、时间顺序及小数精度；先无副作用扫描全文件，再调用 Nautilus 原生 Catalog 分批写入。对相同 timestamp 不跨文件拆批；已有重叠区间会被 Nautilus 拒绝。大文件中途磁盘异常仍可能产生部分 Parquet，失败后必须核对并修复，不能自动重试为已完成状态。

命令行单独执行示例：

```sh
cargo run --manifest-path integrations/nautilus_binance_data/Cargo.toml --bin import_archive --   spot trades ETHUSDT 2026-11-20 /data/archives/ETHUSDT-trades-2026-11-20.zip   /data/nautilus-catalog/tenant1/spot <official-sha256> <instrument-price-precision> <instrument-size-precision>
```

现有数据任务 API 新增 archive_import 操作，只引用先前**同租户**已完成的 archive_fetch 任务编号，不接受客户端自报 ZIP 文件名或哈希值。管理员配置独立原生二进制及 Catalog 根目录：

```sh
export ENGINE_NATIVE_NAUTILUS_IMPORT_BIN=/opt/seealpha/bin/import_archive
export ENGINE_NATIVE_NAUTILUS_CATALOG_ROOT=/data/nautilus-catalog
```

已完成 archive_fetch 后，通过 POST /tenants/{tenant_id}/data/control/job/start 提交：

```json
{"payload":{"action":"archive_import","archive_job_id":"<previous-archive-fetch-job-id>","price_precision":2,"size_precision":3}}
```

现有任务队列检查同租户的下载证据与真实归档路径；当前受管数据源运行时拒绝并发导入。原生实时节点及离线导入器同时使用一个 SeeAlpha Catalog 互斥文件锁（仅进程级协调，Parquet 写入仍由 Nautilus 完成）。没有管理之外的第三方 writer 获得这层锁保证；部署时必须禁止其并发写入。导入完成仅能说明该日 ZIP 的已校验记录写入 Catalog；即使 catalog_committed=true，coverage_complete 和 data_ready 仍是 false。

## 原生 NautilusTrader Rust 节点的初步代码

参见 [integrations/nautilus_binance_data/README.md](../../../../integrations/nautilus_binance_data/README.md)。该模块使用上游 `nautilus-binance` + `nautilus-live` + `nautilus-persistence` 构建纯只读节点，而不是再写 Spot/USD-M WS/REST SDK。已接入现有 Runtime Source Supervisor 的管理员显式实验路径，但仍不在默认 Docker 中部署；需要经过最新提交的 CI、Instrument 精度、Catalog parity 与数据缺口验收后，才能切换默认行为并删除旧自研实现。

## 待完成，不能误认作已实现

1. NautilusTrader `nautilus-binance` 已有显式实验接入，旧 `ws.rs` / `protocol.rs` / `native_source.rs` 已删除。尚未通过真实市场数据端到端验收或成为默认源；不得因此将数据标为 READY。
2. ZIP→Nautilus 原生 Tick Catalog 的离线导入器已新增，仍需通过最新独立 Rust CI、离线样本及真实官方 ZIP 验证；现有 `engine/market_data/converter` 尚未清除，不能视为完整替换。
3. REST 先按交易所支持的时间、数量和分页范围获取；覆盖不足或需要官方历史归档时再调用 ZIP。**不能根据“1,000 笔”这一单次请求上限臆造固定的时间分界**。
4. 日/月文件的计划、发布滞后、缺失日期、下载重试、跨 ZIP 的 Trade ID 连续性、实时接续、ZIP 更正重验和来源证据 checkpoint；所有完整性门禁仍由 SeeAlpha 管理。
5. 运行 `cargo check --workspace --all-targets --locked`、Clippy、单元和集成测试，并在真实 Binance Spot/USD-M 数据上校验原生 WS/REST/ZIP 的接续。当前并无生产 READY 授权。
6. 旧 SQLite K 线计算及历史 Catalog 发布入口仍受 Runtime 引用，**不是可确认无引用的死代码**；在原生 Catalog 下游消费者迁移、验收前保留，但不得当作 Nautilus 的在线 Tick 采集路径。

来源：[Binance 官方公共数据文档](https://github.com/binance/binance-public-data)、[Nautilus rc9 Binance 集成说明](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/docs/integrations/binance.md)、[Nautilus rc9 Parquet Catalog](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/docs/concepts/data/catalog.md)。

**重要**：`archive_import` 的 `price_precision` 和 `size_precision` 必须来源于 Nautilus 该交易对的 Instrument 元数据，示例数值 2/3 只是 API 格式演示，并非 ETHUSDT 的可靠精度。当前没有自动验证外部来源的精度权威性，导入仍只用于隔离的实验流程；不得将其视为端到端验收通过。

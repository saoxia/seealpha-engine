# Binance 历史归档补齐：NautilusTrader 优先

状态：**仅实现归档下载与官方 checksum 校验，不代表已经完成 Nautilus 原生接入**。

## 边界

| 操作 | 应由谁负责 | 本次实现 |
| --- | --- | --- |
| Spot `@trade` / USD-M `@aggTrade` 实时订阅、多合约连接池、重连 | NautilusTrader `nautilus-binance` | 未改动现有实验性采集器；待原生替换 |
| REST `request_trades`、`request_agg_trades` | NautilusTrader `BinanceSpotDataClient` / `BinanceFuturesDataClient` | 不增加另一套 REST 历史交易查询 |
| 日/月 ZIP 下载、官方 checksum 与来源记录 | SeeAlpha 仅做原生缺失部分 | 新增**每日** ZIP 下载 CLI；月归档和调度未实现 |
| `TradeTick`、历史持久化、流式写入和 Parquet Catalog | NautilusTrader 原生 Model / Persistence | 不新增通用 Tick 存储器；当前旧转换器仍需被原生替代 |
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

管理员须配置 `ENGINE_ARCHIVE_ROOT`，并提前创建 `<archive-root>/<tenant_id>/`。服务端只使用该受控路径，从请求体拒绝自定义目录；任务状态保存于已有的租户任务日志。下载完成只表示官方 SHA-256 ZIP 验证成功；回执强制 `data_ready=false`、`allow_new_risk=false` 和 `catalog_committed=false`。无自动交易和原生 Catalog 导入。

## 原生 NautilusTrader Rust 节点的初步代码

参见 [integrations/nautilus_binance_data/README.md](../../../integrations/nautilus_binance_data/README.md)。该模块使用上游 `nautilus-binance` + `nautilus-live` + `nautilus-persistence` 构建纯只读节点，而不是再写 Spot/USD-M WS/REST SDK。当前不在 Engine Runtime 或 Docker 中启动；需要先通过独立 CI、精度与 Catalog parity、数据缺口验证后再切换和删除旧自研实现。

## 待完成，不能误认作已实现

1. 在当前 Rust Runtime 真正启用并锁定 NautilusTrader `nautilus-binance` 数据客户端，替换实验性 `ws.rs` / `native_source.rs` 的通用连接逻辑。不得以新增 SeeAlpha 自定义 Adapter Trait 代替复用。
2. 使用 Nautilus Rust `TradeTick` 和原生 Streaming/Catalog，将 ZIP 中的 trade/aggTrade 按原始精度与交易所标识导入；当前 `engine/market_data/converter` 的自研 schema 兼容实现尚不能当作上游原生引擎。
3. REST 先按交易所支持的时间、数量和分页范围获取；覆盖不足或需要官方历史归档时再调用 ZIP。**不能根据“1,000 笔”这一单次请求上限臆造固定的时间分界**。
4. 日/月文件的计划、发布滞后、缺失日期、下载重试、跨 ZIP 的 Trade ID 连续性、实时接续、ZIP 更正重验和来源证据 checkpoint；所有完整性门禁仍由 SeeAlpha 管理。
5. 运行 `cargo check --workspace --all-targets --locked`、Clippy、单元和集成测试；在未验证前不授权生产 READY。

来源：[Binance 官方公共数据文档](https://github.com/binance/binance-public-data)、[Nautilus rc9 Binance 集成说明](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/docs/integrations/binance.md)、[Nautilus rc9 Parquet Catalog](https://github.com/nautechsystems/nautilus_trader/blob/v2.0.0rc9/docs/concepts/data/catalog.md)。

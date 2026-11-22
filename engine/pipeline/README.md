# SeeAlpha 行情 Pipeline

Pipeline 只分为 `sources/`（来源、产品类型、连接、采集及行情质量校验）和 `catalog/`（通用 Nautilus/Parquet 存储、查询及存储一致性检查）。`sources/` 按 Binance 产品类型组织；共享代码不会为每个产品重复复制。

| 位置 | 现有责任 | 当前状态 |
| --- | --- | --- |
| `sources/binance_spot/` | Spot 产品身份、Instrument ID、分层历史修复入口 | 由共享 Nautilus 进程以 `spot` 模式运行；独立于 USD-M 进程 |
| `sources/binance_futures_um/` | USD-M Futures 产品身份、聚合成交 REST 对齐与 Funding 数据 | 由共享 Nautilus 进程以 `futures_um` 模式运行；尚未支持 COIN-M |
| `sources/binance_option/` | 期权合约识别、实验性 EAPI 客户端和实时消息解析 | 正式 Tick 采集与 Tick 衍生 Bar **尚未接通**，不落盘 EAPI K 线 |
| `sources/shared/nautilus/` | Spot / USD-M 共用的独立 Rust NautilusTrader LiveNode 和原生历史 ZIP 导入程序 | 编译为同一二进制，但按产品启动独立进程；实验性，未通过 READY |
| `sources/binance/archive/` | 跨 Spot / USD-M 的 Binance 官方历史 ZIP 获取、来源校验与 Manifest | 现有 `seealpha-binance-market-data-source` crate |
| `sources/shared/market/` | 当前共用的公共市场模型、REST 客户端、K 线和时间校验工具；USD-M 近期成交解析由 binance_futures_um/agg_trades.rs 承担 | 现有 `seealpha-market` crate，公共 API 不变 |
| `catalog/` | 通用 Parquet 历史数据存储、读取、文件索引和缺口查询，不解析 Binance 产品 | 原生存储适配 |

## 连接生命周期

- Spot 和 USD-M 的独立 `pipeline_worker` 进程按 PostgreSQL 的订阅配置收敛，每个产品最多运行一个原生 Nautilus 实时采集进程；同一产品的多个交易对统一订阅。Runtime 不参与这个新配置链路。
- `binance_option` 当前只存在公共 REST 与 WebSocket 消息解析函数，不表示已建立 Options WebSocket 或具备生产级实时订阅。
- `sources/shared/` 存放跨产品共用的代码，不形成一个混合 Spot / USD-M WebSocket 连接；运行时仍按产品隔离。
- 新目录不改变现有编译目标、启动参数、数据卷路径、恢复门禁及交易安全策略。

## 安全限制

Nautilus 节点仍是只读实验性采集器。REST/WS/ZIP 数据连续性、历史与实时交接、Catalog 一致性和 READY 均未通过生产验收；不得把收到行情或进程运行当作数据可用/允许开新风险。策略实时交付仍待实现。

行情数据不提交到此源码目录。参见 [数据目录约定](../../docs/architecture/DATA_LAYOUT.md)。


### Spot / USD-M / Options 历史修复

正式历史 K 线只能从已验证完整性的 TradeTick 序列聚合；交易所 REST Kline 和历史 Kline 对比器即使仍有独立工具代码，也不得作为正式 Bar 写入来源。

Spot / USD-M 保留 Nautilus 实时 TradeTick 采集与 Binance 官方历史 ZIP 下载、导入等底层能力，但“按 `history_start` 自动补齐原始 Tick → 无缺口重算所有周期 Bar”尚未完成闭环。Pipeline 现在仅执行安全的旧 Bar 保留范围清理，不会用官方 Kline 填充缺口。

Options 也采用相同规范。由于尚无期权 TradeTick 实时/历史接入，Options Worker **不再调用 EAPI 历史 Kline 写入 Catalog**；只读取订阅声明并执行既有 Bar 保留规则，不能报告行情 READY。

## 已锁定的产品契约

[Pipeline 基线](BASELINE.md) 列出 Spot、USD-M Futures、Options 的冻结范围及对应验证。此次清理不改变上述产品链路或风险门禁；删除的只是已无调用方、与现行架构重复的旧模块。

## PostgreSQL 驱动的 Spot / USD-M / Options 订阅

Docker 单容器启动三个 `seealpha-pipeline` Worker（可设置 `SEEALPHA_PIPELINE_ENABLED=0` 关闭），分别处理 `spot`、`futures_um` 和 `options`。Worker 每 30 秒读取一次 `pipeline_subscriptions`；API **只写配置**，不排队下载，也不控制实时进程。

每个租户使用相同配置格式：`product`、`symbol`、`history_start`（UTC YYYY-MM-DD）、`realtime_enabled`、`enabled`，以及可选的 `price_precision`、`size_precision`。默认由 Pipeline 从 Binance `exchangeInfo` 的 PRICE_FILTER / LOT_SIZE 读取精度；两项显式填写时必须来自经过核实的 Instrument 元数据。

```http
PUT /tenants/demo/pipeline/subscriptions/spot/ETHUSDT
Content-Type: application/json

{
  "product":"spot",
  "symbol":"ETHUSDT",
  "history_start":"2025-01-01",
  "realtime_enabled":true,
  "enabled":true
}
```

**精度**：Spot/USD-M 原生端从官方 Instrument 元数据确定精度。未实现的期权 Tick 聚合不能假定精度或报告已落盘。

`GET /tenants/demo/pipeline/subscriptions?product=spot` 查看配置；
`DELETE /tenants/demo/pipeline/subscriptions/spot/ETHUSDT` 仅禁用订阅，保留数据。
现有 Engine 租户鉴权和状态门禁照常生效。

### 历史数据和保留规则

Worker **不会**调用 Binance Kline API 为正式 Catalog 回填 Bar。历史 Tick ZIP 无缺口导入、校验与全周期 Bar 重建尚未完成，因此当前不能把配置开始日期当作已拥有完整历史 K 线的证明。

`history_start` 后移后，Catalog 只删除严格早于 UTC 日界线的完整 Bar Parquet 文件；任何跨边界的文件均拒绝删除，不裁断原始文件、不错误声明完成清理。对共享 Catalog，所有租户（**包括已禁用、仍要求保留历史的租户**）中最早的 `history_start` 才是清理下界。删除不影响其他产品、交易对或租户保留要求。

**尚未实现：** 原生 TradeTick/ZIP 自动保留期清理、跨边界 Bar Parquet 安全裁剪、Tick 断点回补、完整历史/实时交接与 READY 认证。旧 Runtime 数据任务接口已删除，不应将其重新引入。

### 实时订阅

同一产品所有 `enabled && realtime_enabled` 交易对组成一个集合，交给独立的 Nautilus 原生节点。集合发生变化时，先以 SIGTERM 关闭旧节点，确认退出后再用新列表启动；超过停止等待则拒绝并发启动。节点仅采集市场数据，不带执行客户端和策略。

Native Catalog 和 Feather 根目录可通过 `PIPELINE_NATIVE_CATALOG_ROOT` 与 `PIPELINE_FEATHER_ROOT` 设置（Worker 在目录下按产品隔离）；可通过 `PIPELINE_NATIVE_BIN` 指定原生节点二进制。数据挂载卷须持久化，初期仍属实验性采集，**不能报告 READY**。

本地分别运行：

```sh
SEEALPHA_DATABASE_URL='postgresql://...' cargo run --locked -p seealpha-market --bin pipeline_worker -- spot
SEEALPHA_DATABASE_URL='postgresql://...' cargo run --locked -p seealpha-market --bin pipeline_worker -- futures_um
SEEALPHA_DATABASE_URL='postgresql://...' cargo run --locked -p seealpha-market --bin pipeline_worker -- options
```

## 运行时数据任务清理

旧 Runtime `data.rs`、`data_policies` / `data_jobs` 的创建与调度 API 已从代码中移除。数据订阅统一使用 `/tenants/{tenant}/pipeline/subscriptions` 的 GET/PUT/DELETE；调用仅写入 PostgreSQL，Spot、USD-M、Options Worker 独立按配置运行，不再产生一次性数据任务 ID、状态队列或重试 API。

**数据迁移边界：** 已安装数据库中的旧表不会自动 DROP（避免升级时破坏历史记录），但不再创建或访问。一次性 ZIP 归档下载/导入以及旧按日修复操作不再通过 Runtime API 执行；它们的底层代码仍保留在 Pipeline。要让这些操作自动执行，需继续在各产品 Pipeline 中接入对应的配置驱动流程。

## 全产品 K 线唯一来源与实时更新规则

- 正式 OHLCV **只允许从 TradeTick 聚合**；Spot、USD-M 和 Options 均不把 Binance Kline REST/WS 数据当作正式 Bar。现有历史 Kline API 解析仅为兼容/研究代码，Pipeline 不执行其历史 Bar 写入。
- 聚合周期：`1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M`。最小固定为 **1s**，周一 UTC 起周，月线按 UTC 自然月。
- **每来一笔有效的新 Tick，都要计算并发布所有周期的当前 OHLCV**；不等待周期收盘，不遗漏首笔 Tick；周期切换时同时发上一周期最终快照以及新周期的首笔快照。
- 时间窗口内完全没有 Tick 不凭空生成零成交 Bar；重复 Tick 不应重复累计成交量；时间倒退的数据须由历史数据修复处理，不直接覆盖实时状态。
- Native `TradeSubscriber::on_trade` 已将 Bar 更新发布到同一 Nautilus LiveNode 的原生 typed Bar topic，`BarType` 为 `LAST-INTERNAL`。尚未闭合的更新使用**真实 Tick 事件时间**作为 `ts_event`，不得提前标记为未来收盘时间。
- 采集进程与策略引擎是**不同进程**。当前实现仅证明 LiveNode 内的原生消息发布，**尚未证明跨进程策略订阅可见**。需要额外打通跨进程消费和实时 Bar 状态恢复后才能宣称端到端完成。
- Nautilus 原生流持久化仍只写原始 TradeTick；不要求每笔 Tick 都重复写 Parquet。SeeAlpha Catalog 已支持通过原子文件重写修订同一时间桶的 Bar，但历史 Tick 无缺口重放/聚合、重启恢复与 Bar Catalog 全周期落盘尚未验收。

### 实时与历史 K 线聚合的分工

- **实时**：每个新的 TradeTick 直接更新所有周期（包括未闭合 K 线），不为了分层而引入一秒延迟；`1s` 本身也实时更新。
- **历史重建**：优先从经过校验、确认已经结束的 `1s` K 线批量聚合更大周期，避免将全部历史 Tick 反复逐周期处理。`aggregate_historical_seconds()` 提供这一共享纯计算能力；历史 ZIP 导入、验证覆盖范围和完整重建调度仍需接通。
- **没有成交的秒**：不凭空生成 1s 或更高周期 K 线。历史聚合接收明确的已核验数据覆盖截止时间，只有被完整覆盖的周期才标记为 `closed`，不得把最后一个未覆盖完的窗口误标为已闭合。

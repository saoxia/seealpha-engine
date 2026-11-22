
> 2026-11-21 备注：以上历史迁移记录保留用于追溯；旧 `nautilus-data-converter` / Binance SQLite Journal / 自研 WebSocket / K 线计算器已从当前 Rust workspace 移除。现行 Spot/USD-M 成交路径使用独立 Nautilus Rust 数据节点、官方 ZIP 下载与原生 Catalog Tick 导入；不得复制历史命令或当作生产 READY 证据。

# Rust 数据底座迁移：NautilusTrader v2 Catalog（2026-11-21）

> 基线：恢复的 Python Engine `f18e48e995becc0662df288058be6059c91c45b0`。本批按旧 Python 数据模型迁移至 Rust；**没有运行 Cargo 编译、测试或端到端验收**（GitHub Actions 按维护者要求暂停）。不能将本分支视为完整、可用于生产实盘的数据底座切换。

## 为什么必须对齐原 Python 行为

现有 Python Engine 的数据权威存储是 `data/catalog` 下的 NautilusTrader v2 Parquet，而非 Rust Engine 的独立 `book.sqlite`。原 Python 关键边界：

- `core/market/instruments.py`：产品区分 `spot`、`futures_um`、`options`；`InstrumentId` 和 `BarType` 要按产品匹配。
- `engine/market_data/storage/catalog.py`：原生 Parquet 的 Bar、TradeTick、QuoteTick、FundingRateUpdate、OrderBookDepth、OptionGreeks、InstrumentStatus/Close、Instrument、CustomData，附元数据精度。
- `engine/market_data/access/historical_data.py`：只读、绝不触发订阅和数据采集，按纳秒 `[start,end)` 返回，拒绝历史倒退。
- `engine/market_data/repair/data_management.py`：独立文件扫描与来源清点；仅 UTC 整日删除预览，文件指纹、symlink 防护、衍生指标后缀失效。
- `engine/market_data/collection/clock.py`：服务器时钟校验、往返与跳变限制；严格以交易所确认时间判定 K 线闭合。
- `engine/market_data/collection/service.py`：源 journal → REST 历史重叠和缺口修复 → 计算 → Catalog → 策略；某一步缺失不允许宣称 READY。

## 已落地 Rust 组件

| 组件 | 已写入实现 | 当前限制 |
| --- | --- | --- |
| `crates/seealpha-catalog/src/model.rs` | `InstrumentId`、`BarType`、UTC/周/月边界、close-minus-1ms `ts_event`、Decimal OHLCV | 其他非 K 线合约类的 schema 还未提供完整写入器 |
| `src/reader.rs` | 从**现有** `data/catalog` 只读、分批有界解码 Nautilus v2 Parquet Bar/Tick/Quote 及多种通用 Arrow 事件、精确纳秒、按产品分目录，且扫描实际数据清单 | 某些 Arrow 特殊数据结构尚未明确覆盖全部 Nautilus schema；不证明来源归档完整 |
| `src/writer.rs` | SNAPPY Arrow Parquet Bar 发布、精度/时间校验、同数据幂等重试、写锁、临时文件 + hard-link 原子发布 | 仅 Bar 写入，尚未代替全部 Tick/Greeks/L2/Instrument 生成与历史来源记录 |
| `src/cleanup.rs` | 完整 UTC 日文件清单、symlink/文件元数据/跨日期文件检查、特征后缀依赖、SHA256 删除快照 | **只读预览，不执行实际删除**，旧 Python 与 Rust writer 尚未共享一套可靠锁 |
| `seealpha-runtime/src/catalog.rs` | 统一 `read_market_bars`、`read_contiguous_market_bars`、`commit_market_bars`、通用历史事件、清单和删除预览 | 未挂载 Parquet 的开发环境才使用旧 SQLite；不将两种存储伪装成一个已同步数据源 |
| `seealpha-market/src/clock.rs` | 同 Python 的 REST 交易所时钟、最大 2s 往返、5s 跳变、120s 健康窗口与保守确认时间 | 网络断线则拒绝新历史 K 线确认 |
| `binance-source/src/native_catalog_publish.rs` | 读取原有 Rust `collection.sqlite` 中已闭合且标记完整的 K 线，独立 REST 逐根验证、发布 Parquet，**成功后**推进 journal 检查点 | 现阶段仅支持该验证器可直接读取的现货/永续固定周期；缺口或 OHLCV 不一致则拒绝写入 |
| `binance-source/src/native_source.rs` | 定期将独立、受验证的 Binance ServerClock 状态传入原始成交 journal，计算线程可用确认时间检查闭合 | **仍然 `data_ready=false`、`allow_new_risk=false`**；真实 REST handover、ws 断线缺口补齐与 READY 未验收 |
| `seealpha-runtime/src/sources.rs` | 显式实验性启动/停止 source 与独立 calculation 进程，子进程生命周期归 Engine 所有 | 不自动声明实盘 READY，不改变已配置行情源授权 |

历史回测（已下发策略、Spot/UM carry、Options protected）现在经 Runtime 优先读取**原生 Parquet Catalog**，不要求先将旧 Nautilus 文件完整转存到 `book.sqlite`。HTTP `/data/catalog/bars`、`/data/datasets`、`/data/coverage`、`/data/page`、`/data/chart` 和 `/data/latest` 已使用同一路径。

## 原生 Catalog 的路径与调用方式

可通过 `ENGINE_CATALOG_DIR` 指定 Python 配置中的 Catalog 路径，支持绝对路径及相对于 `ENGINE_PROJECT` 的路径；未配置默认 `$ENGINE_PROJECT/data/catalog`。读取端要求目录已存在，拒绝符号链接、不创建或修复旧文件。

- `POST /tenants/{tenant_id}/data/catalog/events`：传入 `{"data_type":"trades","identifier":"ETHUSDT-PERP.BINANCE","start_ns": "...", "end_ns": "...","limit":1000}`，其他支持 `quotes`、`instruments`、`funding_rate_updates`、`option_greeks`、`order_book_depths`、`instrument_statuses`、`instrument_closes`，以及指定 `feature_catalog`、`class_name` 的 `custom/supplement`。原始纳秒保留为字符串；部分 Arrow 特殊数据类型可能拒绝读取，不降级为伪造值。
- `POST /tenants/{tenant_id}/data/catalog/inventory`：按数据类型和标识查询实际文件时间范围。
- `POST /tenants/{tenant_id}/data/datasets`：从已有 Nautilus Bar 和 TradeTick 目录直接扫描产品与周期，不依赖策略订阅配置。
- `POST /tenants/{tenant_id}/data/catalog/bars`：已有 `data/catalog` 时写入原生 Parquet，要求携带 `price_precision` 和 `size_precision`（0..16，且可精确表示提交值），不能覆盖不同历史。精度目前仍需要由可信 Instrument 配置提供，尚未完成全部 schema 交叉验证。
- `POST /tenants/{tenant_id}/data/control/source/{product}/calculate/{symbol}/start` 或 `/stop`：需要显式 `ENGINE_EXPERIMENTAL_SOURCE=1`，只管理独立计算进程，不启用实时 READY。
- `POST /tenants/{tenant_id}/data/control/job/start` 可提交 `{"payload":{"action":"publish_catalog","product":"futures_um","symbol":"ETHUSDT","interval":"1m","price_precision":2,"size_precision":3}}`。先核实项目原生 Journal、精度和 Parquet 挂载，再经外部公开 REST 校验完整闭合 K 线，全部通过才原子发布和更新检查点。

独立二进制：`binance-native-catalog-publish futures_um ETHUSDT /path/to/pair /path/to/catalog 1m 2 3`；Docker Rust 镜像中已包含。进程不接受 API Key，不触发订单交易。

## 必须继续实现的能力

1. Tick/Quote/L2/Greeks/Instrument/自定义特征全类型**写入与 Nautilus 原始 schema 等价**，多类型源归档的版本与完整性证据。
2. Python Pipeline 的官方 Binance ZIP 归档与 checksum、逐笔 ID 缺口修复、历史 REST/WS handover 与旧 Catalog overlap 审计；原生 `data-converter` 已具备一部分 TradeTick→Parquet 能力，但尚未由新 Engine 全流程调度。
3. 独立 Options 实时采集进程、订单簿和 Tick/Quote 原生实时源、Funding/期权历史长期完整性。
4. 完整策略订阅读端读取已确认 Live Feed 的 READY 数据、共享生产者生命周期与可靠重热/失联恢复。
5. Python 与 Rust 数据写入器统一目录锁及文件身份协议，随后再实现 delete/trim 真正执行和修复已存在的 Parquet。
6. 统一数据质量验收：旧 Python 夹具与 Rust 产生相同记录/目录/schema/UTC 边界；完整数据目录恢复/重启、乱序、重复、断网、检查点失败、不同产品隔离，以及对接 NautilusTrader 的实际读取。

## 验收状态

- 代码已写入功能分支，仍保留所有 `engine/**/*.py`。
- GitHub Actions 继续位于 `.github/workflows-paused/`，没有重新启用。
- 由于本会话没有 Rust 编译工具链，**Cargo 编译、Rust tests、clippy、旧版 Python 数据的真实文件读写互操作和端到端验证均未执行**。添加单元测试只代表编写了测试，不能代表通过。
- 不能因为本批新增了 Catalog 和计算/发布入口，就宣称 Rust 数据底座完全等价或已达到可用于实盘策略的 `READY`。


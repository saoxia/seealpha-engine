# ClickHouse 外部历史数据契约：`binance_trade`

> 来源：项目使用者于 2026-11-22 提供的 ClickHouse DDL 和交易对截图。
> 本文件记录**已提供的结构与使用约定**，不是由 SeeAlpha 自动执行的迁移脚本；尚未连接真实数据库执行 `SHOW CREATE TABLE` 或核对数据。
> 当前用户说明此表仅存储 Binance USD-M 数据，不包含 Binance Spot 数据。

## 原始建表语句

以下 SQL 按提供内容保留字段定义、编解码、分区与排序键（仅调整缩进）：

```sql
CREATE TABLE binance_trade (
    `symbol` LowCardinality(String),
    `dt` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `trade_id` UInt64 CODEC(Delta(8), ZSTD(1)),
    `price` Decimal(32, 12),
    `qty` Decimal(32, 12),
    `quote_qty` Decimal(32, 12),
    `is_buyer_maker` Bool,
    `ingest_time` DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (symbol, dt, trade_id)
SETTINGS index_granularity = 4096;
```

DDL 没有指定数据库名；实际数据库由 `SEEALPHA_CLICKHOUSE_DATABASE` 配置（连接器默认 `default`），不应据此推断生产表所在数据库。

## 字段说明

| 字段 | ClickHouse 类型 | 含义与约束 |
| --- | --- | --- |
| `symbol` | `LowCardinality(String)` | 数据库内部的交易对标识；当前 USD-M 示例为 `ETH/USDT:USDT`。 |
| `dt` | `DateTime64(3, 'UTC')` | 成交事件时间，UTC，精度为毫秒（不是导入时间）。 |
| `trade_id` | `UInt64` | 该表的成交记录标识；是否与 Binance USD-M `aggTrades` 的聚合 ID 同义**未获证实**，不得混用。 |
| `price` | `Decimal(32, 12)` | 成交价格；查询时使用 `toString` 保留十进制精度。 |
| `qty` | `Decimal(32, 12)` | 成交数量。 |
| `quote_qty` | `Decimal(32, 12)` | 计价币种成交量；具体上游口径尚未独立校验。 |
| `is_buyer_maker` | `Bool` | 买方是否为挂单方标志；具体来源与取值应与上游采集器核验。 |
| `ingest_time` | `DateTime64(3, 'UTC')` | ClickHouse 写入时间，默认 `now64(3)`；不要当作成交事件时间使用。 |

### 存储及去重语义

- `ReplacingMergeTree` 可能在后台异步合并同排序键行；查询连接器使用 `FINAL` 读取合并后的版本。
- 排序键为 `(symbol, dt, trade_id)`，**不等于**仅按 `(symbol, trade_id)` 全局去重。同一成交 ID 若被写入不同 `dt`，不会因排序键而自动消失。
- 未指定 `ReplacingMergeTree` version 列，不能从 DDL 推导重复行保留版本的确定性；`ingest_time` 不是替换版本列。
- 按 `toYYYYMM(dt)` 月分区；`index_granularity=4096`。
- 表定义本身**不保证**逐笔 ID 连续、无漏数、时间单调、全量历史覆盖或与交易所数据一致。即使 `FINAL` 后也需要独立完整性校验。

## USD-M 交易对表示

引擎使用 Binance 原生无分隔符符号；ClickHouse 使用带结算资产的 CCXT 风格字符串。当前适配器仅映射 USDT / USDC 结算：

| SeeAlpha 符号 | 已展示的 ClickHouse 符号 |
| --- | --- |
| `ETHUSDT` | `ETH/USDT:USDT` |
| `BTCUSDT` | `BTC/USDT:USDT` |
| `SOLUSDT` | `SOL/USDT:USDT` |
| `DOGEUSDT` | `DOGE/USDT:USDT` |
| `ETHUSDC` | `ETH/USDC:USDC` |
| `BTCUSDC` | `BTC/USDC:USDC` |
| `SOLUSDC` | `SOL/USDC:USDC` |

以上仅是截图展示的符号，**不是全表可用币种清单**。如果添加其他结算资产或交割合约，需要先确认实际存储格式与产品身份，再扩展映射。

## 和 SeeAlpha 历史修复的关系

1. 当前 Binance USD-M 主流程仍然是分层 K 线检查、官方 `futures/um/daily/aggTrades` ZIP 下载、校验、Nautilus Catalog 导入。
2. **只有官方 ZIP 获取失败**时，USD-M 修复任务才会尝试 ClickHouse 备用查询；当前仅查询第一个异常分钟（至多 10,000 行），不自动导入，也不宣称完成补齐。ClickHouse 是可选依赖：未设置连接 URL、配置无效、网络不可达、查询报错、交易对不受支持或结果为空，均跳过备用查询并保留原来的 ZIP 失败原因。启动服务不需要 ClickHouse 在线。
3. `binance_trade` 当前按 **USD-M 原始逐笔成交**理解；它与 ZIP 的 `aggTrades` 聚合成交格式不同。即使 `trade_id` 看起来类似，也不可直接交由 `aggTrades` 导入器写入同一个 Catalog。
4. 当前不对 Spot 启用 ClickHouse 回退。未来如要使用这张表作为自动补齐来源，需要另行核实采集口径、原始 ID、覆盖度、去重及 Nautilus Instrument 的价格/数量精度，并建立明确的数据契约。
5. 此连接器只读；不负责创建/迁移表，不在查询时隐式启动采集器，也不改变交易安全门禁。

## 当前连接器

实现：[`src/lib.rs`](src/lib.rs)。配置与调用方式见 [`README.md`](README.md)。

- HTTP(S) + Basic Auth，通过环境变量提供凭据。
- 查询区间采用 UTC 毫秒半开区间 `[start_ms, end_ms)`，按 `(dt, trade_id)` 排序并以同一元组分页。
- 用 ClickHouse 参数绑定 symbol 和时间；不得直接拼接外部传入的 SQL 标识符。
- `Decimal(32,12)` 在响应中以字符串保留精度，不能直接经 `f64` 转换。

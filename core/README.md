# SeeAlpha 公共基础定义

本层服务于多资产量化交易系统；其中现有市场命名和分钟线模型仅覆盖当前实现，
不表示已经形成适用于股票、期货、现货、永续合约和期权的统一合约模型。
新增市场时须显式扩展合约定义和校验，不能默认套用 USDT 计价或全天交易假设。

- `market/instruments.py`：Binance USD-M 的 USDT / USDC 交易对、期权合约、周期、InstrumentId 和 BarType 命名规则；期权保留到期日/行权价/方向的连字符标识。
- `market/models.py`：闭合分钟线 `MinuteBar`、时间单位和 `DataError`。
- `paths.py`：从源码位置确定项目根目录，供各模块解析逻辑路径。

本层不依赖 connectors、feed、pipeline 或 web，不负责网络、任务编排或配置集中管理。
`engine.pipeline.models` 和 `engine.feed.models` 继续导出已有公共类型，历史研究入口保持兼容。
只供单个模块使用的工具留在其所属模块；文件哈希校验位于测试，不建立通用工具杂物目录。

周期命名包含 3d、1w、1M；`bar_window()` 提供 UTC 周一周界和自然月界。
`interval_delta("1M")` 明确拒绝固定时长换算，避免逐日转换误用。3d 按 Binance 实际 UTC 分组起点计算（Unix 纪元加一天），不按零偏移的三日桶划分。


`BINANCE_KLINE_INTERVALS` 给管理配置提供 `/fapi/v1/klines` 支持的 15 个周期（1m 至 1M）；
原 `INTERVALS` 的 1s 命名保留供既有数据及公共兼容入口使用，不属于新管理周期列表。

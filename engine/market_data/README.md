# 统一行情数据系统

持久化 `DataPolicy` 是采集任务启停的唯一依据。`control/` 负责编排；`collection/`、`calculation/`、`repair/` 为可独立恢复的维护任务；`storage/` 保存来源证据与 Catalog；`access/` 只负责授权读取与回放隔离；`quality/` 评估覆盖、连续性和服务状态。

不能通过策略或回测的订阅来隐式打开/关闭采集。原始来源、检查点、锁和数据修复证据持续保留。参见 [完整布局](../../docs/ENGINE_LAYOUT.md) 与 [数据架构决策](../../docs/MARKET_DATA_ARCHITECTURE.md)。

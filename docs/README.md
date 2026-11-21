# 文档索引

本仓库有较多 2026-11-20 至 2026-11-21 Python→Rust 迁移审计文档。为避免把历史快照误认成现行行为，新开发请优先看以下**当前入口**：

| 内容 | 路径 |
| --- | --- |
| 构建与运行入口 | [根目录 README](../README.md) |
| 当前模块及调用关系 | [ARCHITECTURE](ARCHITECTURE.md) |
| 领域与文件导航 | [architecture/MODULE_MAP](architecture/MODULE_MAP.md) |
| 一级领域与目录规划 | [architecture/DOMAIN_LAYOUT](architecture/DOMAIN_LAYOUT.md) |
| 执行与订单边界 | [architecture/execution](architecture/execution.md) |
| 风险边界 | [architecture/risk](architecture/risk.md) |
| 行情的配置/采集/读取设计目标 | [MARKET_DATA_ARCHITECTURE](MARKET_DATA_ARCHITECTURE.md) |
| Platform HTTP 集成背景 | [PLATFORM_INTEGRATION](PLATFORM_INTEGRATION.md)（含过时 Python 命令，勿当作现行启动说明） |

## 历史和迁移记录

`RUST_*.md`、`PYTHON_*.md`、`LEGACY_*.md`、`ENGINE_LAYOUT.md`、`TEST_MAP_*.md`、`NATIVE_CAPABILITY_*.md` 等部分文档记录的是过去某个分支/阶段的状态，**不是当前功能验收结论**。

这些文件暂不整体移动，以免破坏已有文档内的相对链接、代码评审引用和迁移审计证据。后续可以单独做链接检查后再归档；当前事实以本索引和当前 Cargo 源码为准。

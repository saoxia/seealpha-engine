# 行情数据目录

本目录用于存放 SeeAlpha 的**运行时行情持久化数据**，不是源代码，也不是 Cargo crate。

建议运行时建立：
- `feather/`：Nautilus Streaming 的实时暂存。
- `catalog/`：Nautilus Parquet 历史数据。
- `archives/`：已验证的 Binance 官方 ZIP 归档。

这些子目录及实际数据文件**不会进入 Git 或 Docker 镜像**；仅此 README 被版本控制。请勿提交真实成交数据、账户数据、密钥或生产状态文件。

从仓库根目录进行开发环境初始化：

```sh
mkdir -p data/feather/42/spot data/catalog/42/spot data/archives/42
export ENGINE_NATIVE_NAUTILUS_FEATHER_ROOT="$(pwd)/data/feather"
export ENGINE_NATIVE_NAUTILUS_CATALOG_ROOT="$(pwd)/data/catalog"
export ENGINE_ARCHIVE_ROOT="$(pwd)/data/archives"
```

将示例租户 ID `42` 替换为实际受控租户；生产环境建议使用独立的绝对路径及挂载卷。不要因为路径改名而自动移动、删除或重建现有 Catalog、Feather 和账本。参见 [完整数据目录约定](../docs/architecture/DATA_LAYOUT.md)。

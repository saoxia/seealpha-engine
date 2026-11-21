# 行情持久化数据目录（data）

`engine/pipeline/` 是行情服务**程序代码**；仓库根目录 `data/` 是本地行情**数据文件**（生产可使用独立挂载卷）。`data/` 被根目录的 `.gitignore` 与 `.dockerignore` 排除，**不提交数据文件、占位文件或 Git 下的空目录**。

建议布局：

```text
data/
├── feather/         Nautilus 实时增量 TradeTick 暂存
├── catalog/         Nautilus Parquet 历史数据
└── archives/        Binance 官方历史 ZIP（可选）
```

以上是运行时目录，不是 Rust crate。必须按现有隔离规则分配租户及产品子目录。Nautilus 仍须由管理员显式安装原生二进制，并开启实验开关；此次不更改默认行为，也不主动迁移任何旧数据。

## 本地初始化示例

以租户 `42` 的 Spot 数据为例，从仓库根目录运行：

```sh
mkdir -p data/feather/42/spot data/catalog/42/spot data/archives/42
export ENGINE_NATIVE_NAUTILUS_FEATHER_ROOT="$(pwd)/data/feather"
export ENGINE_NATIVE_NAUTILUS_CATALOG_ROOT="$(pwd)/data/catalog"
export ENGINE_ARCHIVE_ROOT="$(pwd)/data/archives"
```

Runtime 使用上述**管理根目录**及租户、产品标识拼出实际路径。请勿将来自 API 请求的任意绝对路径直接用于数据存储。USD-M 需要独立的 `futures_um` 受控子目录。

`ENGINE_NATIVE_NAUTILUS_SOURCE_BIN`、`ENGINE_NATIVE_NAUTILUS_IMPORT_BIN`、`ENGINE_EXPERIMENTAL_SOURCE=1` 和已验证 Instrument 精度等其他安全要求不变。当前 Native Streaming 把 TradeTick 写入 Feather，并仅在正常关闭时将其提升到 Parquet；异常退出需独立恢复验证。文件存在、进程正在运行都不是行情完整或风险 READY 的证据。

## 存量数据

不自动移动、删除或重建已有 `ENGINE_STATE`、Nautilus Catalog、Feather 或 Binance ZIP 文件。生产部署可以沿用现有绝对路径/挂载卷，数据路径调整必须另行验证历史连续性、文件锁、运行中写入进程及恢复机制。订单/账户状态库依然由 Ledger 管理，不属于此行情数据布局。

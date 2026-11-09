# 常驻交易引擎

Phase 1 把行情、连接和订单事实从业务 API 进程里拆出来。本目录是那个常驻进程。

| 路径 | 职责 |
| --- | --- |
| `engine/connectors/` | Binance 适配器与只读/手动执行客户端。不依赖 feed、pipeline、api、web。 |
| `engine/feed/` | 连续行情。管理入口 `python -m engine.feed.managed`。 |
| `engine/pipeline/` | 数据管道：历史补齐 `python -m engine.pipeline.pair_data`，一次性任务 `python -m engine.pipeline.data_management`，以及归档、Catalog 和 `rust_converter/`。 |
| `app.py` / `__main__.py` | 给业务 API 用的内部 HTTP。 |

可加载的示例在 `packages/ema_crossover/`。引擎不按策略名字保存实现；
只有业务端下发归档之后，才会按 `tenant_id` 单独加载。


## 可选 Docker 运行模式

业务 API 可在 `ENGINE_DEPLOYMENT=docker` 下，为每个活跃租户管理一个本地 Docker Engine。
所有租户容器仍运行此 `python -m engine`，通过 `ENGINE_TENANT_ID` 和
`ENGINE_TENANT_PURPOSE` 固定租户身份，拒绝跨租户请求；每个容器拥有独立资源限额和持久状态。
开发租户的历史数据各自保存，不执行跨租户共享缓存或重复回测数据预加载。
详见 [租户 Docker Engine 运行说明](https://github.com/saoxia/SeeAlpha/blob/main/docs/TENANT_DOCKER_RUNTIME.md)。
切换现有生产环境前必须迁移旧 Engine 状态，不能直接启动空 SQLite。

## 启动

在仓库根目录：

```bash
source ./activate.sh
python -m engine
```

默认监听 `127.0.0.1:8766`。状态库是 `.local/state/engine/book.sqlite`（WAL）。
可用环境变量：

| 变量 | 默认 | 作用 |
| --- | --- | --- |
| `ENGINE_HOST` | `127.0.0.1` | 监听地址 |
| `ENGINE_PORT` | `8766` | 监听端口 |
| `ENGINE_TOKEN` | `seealpha-local-engine` | 服务间 Bearer 令牌，仅限本机开发 |
| `ENGINE_STATE` | `.local/state/engine` | SQLite 目录 |
| `ENGINE_PROJECT` | 仓库根目录 | 行情锁和数据路径 |

业务 API 使用同名 `ENGINE_URL`（默认 `http://127.0.0.1:8766`）和 `ENGINE_TOKEN`。
先启动引擎，再启动 API。公开行情不需要交易所密钥。手动交易和账户查询由 API 解密后，
按 `account_id` 把密钥放进当次请求；引擎不保存主密钥，也不把密钥写入 SQLite。

内部路由都要求 `Authorization: Bearer <ENGINE_TOKEN>`，并且路径带 `account_id`：

- `GET /health`
- `POST/DELETE /accounts/{account_id}/connections`
- `POST /accounts/{account_id}/snapshots`
- `POST /accounts/{account_id}/orders` 与 `GET /accounts/{account_id}/requests/{request_id}`
- `GET /accounts/{account_id}/orders|fills|positions|gaps|events`
- `POST /accounts/{account_id}/market/workers` 启动 feed 或历史进程
- `POST /accounts/{account_id}/valuation` 估值事件流

订单以 `request_id` 幂等。重复提交返回已保存的结果，不会再次发到交易所。

## 策略加载

业务端授权并下发后，引擎才接受一个包：

- `POST /tenants/{tenant_id}/strategy-loads` 接收包版本。用途必须与租户类型一致，正式租户不接受开发授权。
- `GET /tenants/{tenant_id}/strategy-loads` 只返回该租户的记录。
- `POST /tenants/{tenant_id}/accounts/{account_id}/strategy-preview` 在模拟 venue 上喂入 K 线。
- `POST /tenants/{tenant_id}/accounts/{account_id}/backtests` 用该租户已声明的订阅，
  读取 `data/catalog` 里的历史 K 线，并以请求中的虚拟账户资金回测。仅开发租户。
- `GET /tenants/{tenant_id}/accounts/{account_id}/backtests/latest` 返回最近一次结果。

示例 `ema_crossover` 只公布目标持仓。作者说明在 `packages/ema_crossover/README.md`。
引擎用 `PortfolioCoordinator` 和 `target_netting`，先按 `risk_limit` 截断合计目标，
再对当前净持仓提交差额。预览和历史回测都不连接交易所。归档租户不能运行。
正式租户不能使用这条历史回测。

## 尚未纳入

把操作账本同步和资产校准 REST 改成纯事件消费。这两类只读连接器调用仍在 API 进程。
已下发策略的预览不是交易所实盘。管理端不再启动已删除的研究回测目录。
冻结研究快照里的 `pipeline` 导入保持原字节，不是当前可运行入口。

## Engine 镜像源码范围

独立 Engine Docker 镜像现在只包含 `engine/`、`core/`、`nautilus_support.py`、
`packages/` 和构建依赖，不包含业务后台和前端源码。跨进程错误与账户结果类型
在 `core/engine_contracts.py` 定义。仓库尚未独立；详见
[后续 API 解耦清单](../docs/STANDALONE_MIGRATION.md)。

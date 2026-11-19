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
uv sync --locked
uv run python -m engine
```

默认监听 `127.0.0.1:8766`。状态库是 `.local/state/engine/book.sqlite`（WAL）。
可用环境变量：

| 变量 | 默认 | 作用 |
| --- | --- | --- |
| `ENGINE_HOST` | `127.0.0.1` | 监听地址 |
| `ENGINE_PORT` | `8766` | 监听端口 |
| `ENGINE_AUTH_MODE` | `none` | `none`：本机无令牌；`token`：强制校验 Bearer |
| `ENGINE_TOKEN` | 空 | `token` 模式要求外部生成且至少 32 字符的私有令牌 |
| `ENGINE_STATE` | `.local/state/engine` | SQLite 目录 |
| `ENGINE_PROJECT` | 仓库根目录 | 行情锁和数据路径 |
| `ENGINE_RESEARCH_SIGNING_KEY` | 不设置（关闭） | 仅固定开发租户启用研究 HTTP v1 所需的独立签名密钥 |

单机独立 Engine 默认无鉴权，仅可监听 `127.0.0.1`、`::1` 或 `localhost`。业务 API 在独立本机模式无需配置 `ENGINE_TOKEN`；Platform 托管 Docker 必须配置 `ENGINE_AUTH_MODE=token`，将 Platform 生成的强随机 `ENGINE_TOKEN` 注入容器，客户端携带相同令牌。
先启动引擎，再启动 API。公开行情不需要交易所密钥。手动交易和账户查询由 API 解密后，
按 `account_id` 把密钥放进当次请求；引擎不保存主密钥，也不把密钥写入 SQLite。

在 `token` 模式下内部路由要求 `Authorization: Bearer <ENGINE_TOKEN>`；默认 `none` 模式无需此请求头。主要路由包括：

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

## 跨产品目标规划（研究阶段）

已增加独立纯计算模块 `engine.strategies.basket_targets`，支持按租户/账户/产品/合约组合多成员目标；研究态可将已下发策略的现货+永续目标送入原生双 Venue 模拟撮合，**不会产生真实跨产品订单**。当前 `PortfolioCoordinator` 仍只支持单品种，详见 [跨产品组合研究阶段](../docs/BASKET_RESEARCH.md)。

双市场研究数据同步器 `EngineRuntime.iter_funding_carry_history` 可在开发租户已订阅现货和永续时，从真实 Catalog 严格对齐 K 线并按原始时间插入资金费观测；不产生订单、结算或收益报告，暂不对外开放 HTTP。

开发租户还可通过内部 `/tenants/{tenant_id}/accounts/{account_id}/carry-backtests` 运行已下发 Funding Carry 的原生双 Venue CASH/MARGIN 回测，并通过 `carry-backtests/latest` 获取组合账户曲线；该研究入口不启动 Demo 自身回调、不开放真实下单，详见 [研究阶段边界](../docs/BASKET_RESEARCH.md)。

期权保护研究还需真实双翼买卖盘口历史证据。现已提供本地 Catalog `QuoteTick` 读写与 `option_history.read_option_protection_evidence` 严格门禁，但不把期权 K 线收盘价当成可成交 ask，也不声称已经具备期权多腿回测。

## Research API

单独的 `/research/v1/*` 只接收任务级研究授权，不使用管理 `ENGINE_TOKEN`。支持 Catalog 库存、后台 Parquet 快照及开发租户的已下发策略回测；当前受限于现有 Engine 的 1m/5m、单账户回测能力。启用条件、安全限制及客户端合同见 [Research API](../docs/RESEARCH_API.md)。

可选的 `ENGINE_RESEARCH_MCP_ENABLED=true` 额外启用 `/research/mcp`（MCP Streamable HTTP，JSON-only）；默认关闭，仅限配置独立研究签名密钥的固定开发租户。MCP 复用原有研究权限/任务队列，不开放实盘和管理 API；参见 [Engine 原生 MCP](../docs/MCP_RESEARCH.md)。

## 尚未纳入

把操作账本同步和资产校准 REST 改成纯事件消费。这两类只读连接器调用仍在 API 进程。
已下发策略的预览不是交易所实盘。管理端不再启动已删除的研究回测目录。
冻结研究快照里的 `pipeline` 导入保持原字节，不是当前可运行入口。

## Engine 镜像源码范围

独立 Engine Docker 镜像现在只包含 `engine/`、`core/`、`nautilus_support.py`、
`packages/` 和构建依赖，不包含业务后台和前端源码。跨进程错误与账户结果类型
在 `core/engine_contracts.py` 定义。源码已迁至独立仓库，Platform 尚未切换；详见
[后续 API 解耦清单](../docs/STANDALONE_MIGRATION.md)。


## 单一已下发策略的跨产品开发模拟（2026-11-20）

新增 `POST /tenants/{tenant_id}/accounts/{account_id}/issued-spot-perp-backtests`（`issuance_id`、`start`、`end`、`interval`、`starting_spot_balance`、`starting_perpetual_balance`、`max_gross_notional`），以及 `GET .../issued-spot-perp-backtests/latest`。仅限开发租户和已下发、归档 SHA-256 与授权身份一致的单个策略包。仍须订阅同标的 Spot/UM 的独立历史数据和经过证实的资金费率，保留原有研究最大敞口与跨腿偏差门禁。历史数据不齐则拒绝，不推断缺口。

策略在自己的 `on_start()` 中用 `subscribe_market_bundle(symbol=...,products=("spot","futures_um"),data=("closed_bars","funding_rate","account_positions"))` 声明数据需求，在 `on_market_snapshot(snapshot)` 中调用 `publish_basket(targets=[{"product":"spot","symbol":"ETHUSDT","quantity":"0.1"},{"product":"futures_um","symbol":"ETHUSDT","quantity":"-0.1"}],constraints={"atomic_hedge":True,"max_leg_drift":"0.001"})`，同一快照只能提交完整组合，未提交、重复或不满足等额对冲将拒绝回测。引擎将每个已发行实例隔离加载，读取已发布的**策略原生**目标，经现有 `BasketResearchSession` 净额与预算校验后，分别在 NautilusTrader 模拟 Spot CASH、UM MARGIN 场所产生订单与成交，报告中记录逐腿成交和权益曲线。

该接口不开放跨产品实盘下单，`atomic_hedge` 是策略提出的约束而非真实交易所的原子事务。缺失行情、腿间偏差、拒单或不完整成交须终止研究，不可自动盲重发。现阶段**未**开放任意产品对、任意多腿组合的通用单策略回测，也未开放统一 Options 行权/到期实际执行；期权保护 Demo 仍走原有专用模拟入口。生产行为、密钥与账户状态均不变。

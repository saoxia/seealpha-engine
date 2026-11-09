# SeeAlpha Engine 独立仓库：阶段一迁移边界

- 来源：`saoxia/SeeAlpha` 主分支快照 `02ba14d58c700d472584ab17494484a024a04ba1`。
- 目标：`saoxia/seealpha-engine`，独立维护 `engine/`、`core/`、示例策略、必要文档和 Engine 测试。
- 保留原 Python 包路径 `engine.*`、`core.*`，阶段一不进行重命名或改变 Engine HTTP v2 契约。
- 此次不包含原业务 API/Web/ORM/迁移；生产 Platform 尚未改为消费此镜像。
- Docker 镜像只由本仓库的源码及独立 `pyproject.toml`、`uv.lock` 构建；已移除原 Platform 的 `api` / `research` 依赖组，并保留 NautilusTrader 2.0.0rc8。锁文件可能保留旧的未引用 package 记录，待离线/在线重锁后精简。

## 切换前的阻断项

1. 新仓库 GitHub Actions 的 Python/Rust/Docker 检查均通过。
2. 独立 Engine 镜像可以在没有原 SeeAlpha 工作区的宿主机运行；隔离平台通过 v2 HTTP 回归。
3. Platform 只引用确定的 Engine 镜像 digest/tag 和契约版本，禁用本地旧 `engine/` 运行时路径；保留明确回滚。
4. 切换任何有账户、订单和状态的生产实例前，完成停机、未决订单核实、备份、恢复和幂等回归；不得同时运行新旧 Engine 控制相同账户。
5. 版本组合以及长期运行、真实账户验收单独记录，不用 CI 冒充。

## 原仓库清理

只有当跨仓库契约和部署切换通过、所有原 `engine/` 真实引用已识别并消除，才按新授权移除旧源码。原仓库的 `api/engine_client.py`、租户容器管理、轻量接口定义和跨系统测试应当保留。

## 独立服务的跨仓库接入

开发验证流程参见 [Platform 跨仓库接入预演](PLATFORM_INTEGRATION.md)。运行前必须确认容器及状态库隔离；不得复用生产账户或生产存储。

# Platform 接入独立 seealpha-engine（预演版）

目标：Platform **只依赖可运行的 Engine 服务/确定版本镜像及 HTTP v2 契约**，不导入 `engine.*`，不从旧 SeeAlpha 仓库构建 Engine 镜像。此文档只描述本地隔离环境的连接步骤，不授权切换现有生产租户。

## 当前已存在的 Platform 边界

- `saoxia/SeeAlpha/api/engine_client.py` 已使用 HTTPX 调用 `ENGINE_URL` 和 `ENGINE_TOKEN`，要求 v2 响应头；Platform 生产模块不需 import `engine.*`。
- `ENGINE_DEPLOYMENT=single`：业务 API 连接独立 Engine 服务地址。
- `ENGINE_DEPLOYMENT=docker`：业务 API 的租户管理器根据 `ENGINE_DOCKER_IMAGE` **启动新租户容器**。现有容器不会因为修改标签而自动替换；旧容器复用/版本迁移必须单独执行。
- `core/engine_contracts.py`、`core/trading_contracts.py` 仍在两个仓库保留等价副本。两端必须保持 HTTP v2 兼容，不允许擅自破坏性修改。

## 方法 A：本机独立服务（仅空白开发状态）

在 **seealpha-engine** 项目下：

```bash
uv sync --locked
export ENGINE_TOKEN='replace-with-private-local-token'
uv run python -m engine
```

在另一终端的 **SeeAlpha** 项目下，配置：

```bash
export ENGINE_DEPLOYMENT=single
export ENGINE_URL=http://127.0.0.1:8766
export ENGINE_TOKEN='replace-with-the-same-private-local-token'
# 按原 api/README.md 在隔离开发数据库中启动 Platform
```

确保本机不再同时启动原 SeeAlpha 源码目录中的 `python -m engine`。两者不能监听同一端口，也不能同时控制同一账户。开发/生产状态库必须分离。

从任意 Python 3 开发环境可运行只读协议检查：

```bash
ENGINE_URL=http://127.0.0.1:8766 ENGINE_TOKEN='replace-with-private-local-token' \
  python scripts/smoke_http_contract.py
```

脚本只测试未授权 401、正确认证下的健康状态与 v2 响应头、错误版本 426；不读取交易所密钥、不发订单、不写业务状态。令牌只能放在进程环境或受控密钥源中，不放进命令行参数或 Git。

## 方法 B：独立镜像供 Platform Docker 模式使用

在 **seealpha-engine** 仓库根目录，从明确的 Git 提交构建并标记镜像：

```bash
git rev-parse --short=12 HEAD
docker build -f docker/engine.Dockerfile -t seealpha-engine:<GIT_SHA_12> .
```

若在同一 Docker 宿主机隔离启动新的开发租户，SeeAlpha Platform 可指定：

```bash
export ENGINE_DEPLOYMENT=docker
export ENGINE_DOCKER_IMAGE=seealpha-engine:<GIT_SHA_12>
export ENGINE_TENANT_ROOT=/absolute/path/to/new-isolated-tenant-state
```

必须使用你实际构建的 Git SHA 替换占位符，不能把示例文字原样执行。正式发布推荐镜像仓库不可变 digest 而不是可变 `:latest`。本阶段没有自动发布/推送镜像。

**重要**：当前 Platform 的 `DockerTenantManager.ensure` 在容器已存在时会复用容器，并不会仅因 `ENGINE_DOCKER_IMAGE` 改变而重建。新镜像预演必须使用**全新隔离测试租户**，不能把改环境变量当作生产升级流程。

## 阶段二验收条件（仍待执行）

1. 新仓库独立构建的 Engine 镜像能脱离 SeeAlpha 源码启动并完成只读 HTTP 协议握手。
2. SeeAlpha 在物理不存在 `engine/` 的隔离 Platform 源码树里启动，HTTP 功能测试通过。
3. 全链路验收行情、数据任务、策略加载、回测、账户查询、交易请求幂等、租户隔离、异常恢复。
4. 统一版本发布和升级/回滚规则，确认跨仓库 `core` 契约无不兼容漂移。
5. 如涉及有真实订单或持仓的环境，必须单独审批停机、状态卷备份/恢复与订单核查；绝不同时运行新旧 Engine。

原 SeeAlpha 仓库的 `engine/` 和相关测试现在仍保留；只有以上条件完成，才申请后续清理 PR。

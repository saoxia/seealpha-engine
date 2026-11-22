# Engine 初始化配置

默认配置位于 [config.json](config.json)，随 Engine 源码提交并在 Docker 构建时复制到 `/app/engine/config/config.json`。

- 从仓库根目录启动时自动读取 `engine/config/config.json`；当 `ENGINE_PROJECT` 指向 `engine/` 时读取 `config/config.json`。
- 可设置 `ENGINE_CONFIG=/绝对路径/config.json` 指定文件；相对路径基于 `ENGINE_PROJECT`（默认当前工作目录）解析。显式指定但不存在或格式错误时，启动失败。
- **优先级**：`ENGINE_*` 环境变量 > `config.json` > 内置默认值。Docker 亦遵循此优先级。
- 文件支持 `host`、`port`、`auth_mode`、`state_dir`、`tenant_id`、`tenant_purpose`、`research_mcp_enabled`、`venue_base`。未填写的配置继续采用现有默认行为。
- **不要将密码、API Token、私钥或交易所凭证放在文件中。** `SEEALPHA_DATABASE_URL`、`ENGINE_TOKEN`、`ENGINE_RESEARCH_SIGNING_KEY` 必须通过环境变量或容器安全机制注入。未知字段会被拒绝。
- 默认绑定本机回环地址且关闭身份令牌认证；若 `auth_mode` 改为 `token`，须提供长度不少于 32 的 `ENGINE_TOKEN`。数据库仍必须提供 PostgreSQL 连接地址。

示例：

```sh
export SEEALPHA_DATABASE_URL='postgresql://user:password@localhost:5432/seealpha'
export ENGINE_PORT=9000   # 可选：覆盖 config.json 里的 8766
cargo run -p seealpha-engine
```

这是进程级启动参数，并非租户/账户的业务注册配置；不会在运行时自动热重载。

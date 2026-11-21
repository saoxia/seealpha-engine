# AI9 HTTP v2 / Research v1 离线契约测试报告

基线：main@83a19d857bca0ec16faf1a731ccb9835081672ab。分支：feat/http-v2-research-v1-parity-ai9-20261121。

## 验证状态（测试源码不等于实际运行）

- 新增离线测试源码：已写入分支。
- cargo test -p seealpha-http --locked：**未执行**。当前执行环境没有 cargo、rustc、rustfmt，GitHub connector 不能远程运行 Cargo。
- cargo fmt --all --check、cargo clippy --workspace --all-targets --locked -- -D warnings、cargo test --workspace --locked、cargo build --workspace --locked --release：**均未执行**。
- 跨语言 Python/Rust 端到端对照：**未执行**。仓库 Actions 存放于 .github/workflows-paused/；本分支严格不修改该目录。
- 不使用真实密钥、实盘账户、交易网络、生产数据库或旧 Catalog 的可写副本。

## 离线断言清单（预期结果，不是已通过记录）

| 用例 | 验证内容 |
| --- | --- |
| handshake_statuses_match_platform_v2（既有） | 管理无令牌 401、版本冲突 426、跨租户 403、无网关的订单请求 409 |
| research_headers_grant_tenant_and_scope_fail_closed | 无 Grant 401、HTTP 双版本头不符 426、无 snapshot 操作权限 403、实际 Catalog 缺失 409、Parquet 导出未实现 501 |
| research_grant_cannot_access_admin_and_admin_token_cannot_access_research | Research Grant 不得访问管理员 health 401；管理令牌不得访问 Research 401；固定租户不匹配 403 |
| research_body_errors_never_override_authentication_or_contract_version | 非法 JSON 不覆盖 401/426 前置检查；验签后非法请求 422；Research 失败响应始终具有双契约 Header |
| invalid_research_request_is_422_before_any_job_is_queued | 非 development/simulation 不得登记任务：422 |
| only_own_research_task_can_read_or_cancel_persisted_backtest | 持久队列真实登记；其他 task_id 查询/取消 404；本人取消 queued 返回 cancelled |
| running_research_job_cannot_be_falsely_cancelled | SQLite CAS queued→running 后取消得 409，不能伪报 cancelled |
| mcp_does_not_require_http_headers_or_advertise_fake_workers | MCP 只需独立研究 Grant，工具按 ops 过滤；未支持工具不可调用；非法 Grant 401 |
| mcp_rejects_browser_origin_and_bad_protocol | 带 Origin 403；未知 MCP-Protocol-Version 400 |
| connection_and_snapshot_routes_do_not_echo_secrets（既有） | 409 拒绝未配置交易网关；不得回显 API key/secret |
| native_backtest_denies_missing_history_without_fetching_or_synthesizing（既有） | 历史缺口不得自动下载或虚构回测 |
| native_catalog_page_and_coverage_are_read_only_and_contiguous（既有） | 只读历史连续性与重复 Bar 冲突 |

## 实现边界

- Research datasets：仅查询真实 Nautilus Catalog 库存，按 Grant 白名单过滤；无 Catalog 返回明确错误，不能伪称 READY。
- Research backtests：仅允许单产品、开发用途、已下发策略；经 Runtime 持久队列登记，GET/cancel 验证 task_id + dataset + strategy 权限；不会把 queued/accepted 说成成功回测。
- Snapshot 与候选策略：当前 Runtime 缺真实不可变 Parquet 任务及候选 worker，接口明确返回 501；Python candidate 因缺必要 Runtime 配置及能力仍不挂载。
- MCP：按操作权限过滤可用工具，拒绝浏览器 Origin/不支持的协议，绝不提供交易权限。

## 完成 PR 验收所需执行命令

    cargo fmt --all --check
    cargo clippy --workspace --all-targets --locked -- -D warnings
    cargo test -p seealpha-http --locked
    cargo test --workspace --locked
    cargo build --workspace --locked --release
    python3 -m unittest discover -s tests -t . -p '*_test.py' -v

未取得编译和真实 Cargo 测试通过证据之前不得合并。完整路由对照与其他 crate 必须实现的接口见 ROUTE_PARITY.md。

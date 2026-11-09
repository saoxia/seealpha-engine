# Binance 原生查询接口迁移

日期：2026-10-27。使用项目锁定的 NautilusTrader 2.0.0rc7。

## 调用边界

`web` → `api/exchanges/router.py` → 账户配置/汇总服务 → 引擎 → `engine/connectors/binance/queries.py`
→ NautilusTrader 公开配置、factory 和原生节点 → Binance。

新查询链路不依赖 CCXT 或适配器私有 HTTP 客户端。
行情连续性继续由 `feed` 管理；历史数据准备继续由 `pipeline` 管理。

## 已替换的前端路由

| 方法 | 路径 | 实现 |
| --- | --- | --- |
| GET | `/api/system/exchange-markets` | `load_binance_instruments`，缓存五分钟 |
| GET / POST | `/api/system/exchange-accounts` | 新 API 配置管理，凭据加密保存 |
| PUT / DELETE | `/api/system/exchange-accounts/{id}` | 修改/移除配置，失效查询缓存 |
| POST | `/api/system/exchange-accounts/{id}/test` | 原生账户与仓位读取测试，不下单 |
| GET | `/api/system/exchange-accounts/balances` | 分账户、分币种余额 |
| GET | `/api/exchange-accounts` | 首页账户导航，不解密凭据 |
| GET | `/api/exchange-accounts/{id}/portfolio` | 原生余额、账户权益和已对账仓位 |
| GET | `/api/exchange-accounts/portfolio` | 同口径账户汇总，错误账户明确排除 |
| GET | `/api/exchange-accounts/{id}/positions` | 新增独立仓位查询 |

这些路径通过 Next.js 同源 `/api` 代理转发到 `SEEALPHA_API_URL`，默认目标为
`http://127.0.0.1:8000`，不再回退到旧业务后端。
保持旧前端 JSON 字段兼容；不把缺失值改成零。

## 当前支持范围

- 首轮 UI 开放 Binance USD-M 的 USDT 结算永续市场，交易对沿用
  `BTC/USDT:USDT` 等接口表示；connector 内由原生品种定义确定身份。
- `production` 对应原生 LIVE，`sandbox` 对应 TESTNET；不同环境不共享缓存。
- 原生账户查询按需创建连接；同账户并发查询合并，成功快照缓存十秒，失败不缓存。
  LIVE portfolio 普通请求只读后台每分钟保存的展示快照，见下文。
- 原生启动使用完整 USD-M 品种范围，账户创建/编辑不配置交易对，不按历史关注列表过滤仓位。
- 新余额字段来自原生账户余额；权益使用原生 `AccountState.info` 的钱包、浮盈亏和保证金总计，
  校验 `钱包余额 + 未实现盈亏 = 总权益`。
- 纯 USDT 账户继续使用原生权益汇总。存在其他非零资产时，使用固定只读
  `/fapi/v2/account` 补充原生 Python 快照未发布的资产模式、分币种钱包和浮盈亏。
  多资产模式优先使用交易所 USD 汇总，按 USDT/USD 指数换算；单资产模式按各币种
  指数换算钱包及浮盈亏，标记 `valuation_source=estimate`，不发布估算保证金风险率。
  小额资产仍计入估值，缺价不按零处理；无法可靠汇总的账户仍明确排除并显示具体原因。
- 原生标准仓位暂未提供标记价、强平价、每仓浮盈亏、实际杠杆、逐仓模式等全部页面字段；
  返回空值及 warning，仓位数量、方向、身份和开仓均价来自对账后的原生缓存。
- 节点只装载观察策略；不配置自动杠杆/保证金修改，不发送交易命令。
  原生对账生成的本地合成条目仅用于恢复仓位缓存，不作为真实成交记录对外发布。

## 凭据与权限

配置管理、余额批量查询、连接测试和市场配置列表要求管理员；首页查询要求已登录。
凭据沿用旧版 Fernet JSON v1 格式；响应仅含 Key 尾号，数据库保存密文。
请求验证失败也不回显提交的 API Key、Secret 或密码。

API Key 和 Secret Key 必须填写同一组 API 的两个不同值；前后端拒绝相同值，
失败的编辑不会覆盖已有凭据。历史账户若已保存相同值，读取/测试会在启动原生节点前
返回具体原因，需在账户编辑页重新填写整组凭据。凭据输入框关闭密码自动填充提示，
避免将 API 凭据作为新密码与确认密码处理；浏览器仍可能自行决定填充行为。

优先读取 `SYSTEM_CREDENTIAL_KEY`，其次 `ETL_CREDENTIAL_KEY`，否则使用
`SYSTEM_CREDENTIAL_KEY_FILE` 指定文件，默认 `.local/config/api/credential_key`。
空配置首次保存凭据时创建仅当前用户可读写的私有密钥文件；如果数据库已有加密账户，
缺少原密钥时返回错误，不生成替代密钥。备份账户数据库时须单独保管原加密密钥。

API 默认无跨域访问，前端继续通过同源 `/api` 代理。

## 尚未替换

操作记录查询、同步和导出已由新 API 提供；权益 K 线现按启用后的真实估值证据生成，单账户可重算派生曲线；缺口不补造，见 [资产估值](EQUITY_VALUATION.md)。旧版账户快照倒推和人工调整入口不再使用。
当前前端不再依赖旧业务后端；旧代码及其 CCXT 运行依赖已清理。
OKX、现货、币本位、期权，以及下单/改单/撤单接口不在本轮接入范围。

本轮账户读取采用按需、短生命周期的原生节点，适合当前页面读取；后续高频推送应迁入共享的
长连接运行服务，再由 HTTP、WebSocket 和 MCP 共同消费，而不是提高前端轮询频率。

## 验证

```bash
.venv/bin/python -m pytest tests/api tests/connectors -q
```

本地原生集成测试使用虚构凭据，真实运行原生客户端连接本机模拟的 Binance HTTP/WebSocket，
记录全部请求并断言仅有读取请求及用户数据会话的建立/保活/关闭。
这不替代真实 Binance 网络、密钥权限及真实账户的验收。

公开入口参考：[NautilusTrader Binance 集成](https://nautilustrader.io/docs/latest/integrations/binance/)。

## 账户交易对配置清理（2026-10-28）

新增、编辑账户仅提交账户名称、环境及凭据，`market_symbols` 已从请求契约移除。
旧客户端继续提交的同名额外字段按既有 Pydantic 默认行为忽略，不再校验或写入。
前端不再加载市场选择器、搜索品种或要求至少选择一个交易对，账户列表不再展示已选数量。

响应中的 `market_symbols` 标记为 deprecated，仅作为历史兼容元数据保留；
现有 `market_symbols_json` 列与已有内容不删除、不迁移。新账户保存空历史列表，
编辑保留原值，不清空旧数据，也不将其纳入查询快照的缓存身份。
`/api/system/exchange-markets` 作为现有公开品种查询接口继续保留，不再由账户表单调用。

余额、仓位与操作记录仍使用原生账户查询，不启用交易命令。
这不意味着自动采集全市场 Tick；行情采集、数据准备与策略品种继续各自配置。

## 刷新资产与小额资产（2026-11-07）

LIVE 的“刷新资产”通过既有单账户或总览 portfolio GET 的 `refresh=true` 参数，
跳过原生快照、资产明细与价格缓存，原子保存展示快照后再读取；登录权限保持不变，无交易或配置写入。
响应新增 `assets` 明细（账户、币种、原始余额、USDT 价格/估值、价格时间、小额/旧价标记及错误），
以及 `valuation_source`（`exchange` / `estimate`）。设置页旧分币种余额接口保留。

价格来自固定公开 `/fapi/v1/assetIndex?symbol=币种USD`，以币种/USD ÷ USDT/USD
得到 USDT 价格，不使用带数量前缀的期货合约价格，不假定稳定币均值为一美元。
资产明细缓存十秒；一般价格缓存一分钟，绝对钱包估值严格低于 5 USDT 的非 USDT 资产
缓存一小时。价格缓存由分钟采集或手动刷新检查；余额变化在下次采集时重新取价，手动刷新立即重新取价。
USDT 换算基准保持一分钟缓存。缓存按账户配置及环境隔离，编辑/删除账户时失效。

小额资产默认折叠，仍保留精确余额并计入估值。失败价格一分钟后可重试，旧价及时间保留
供参考；旧价不用于新的估算账户总额。多资产模式若仍有可靠交易所总额及 USDT 换算基准，
个别资产缺价仅影响明细，不重复添加资产、不阻断该可靠总额。交易所资产明细本身读取失败时
不发布混币种总额。以上估值仅用于 LIVE 展示，不进入交易风控、保证金或可用资金计算。

官方字段来源：[账户信息](https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Account-Information-V2)、
[资产价格指数](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Multi-Assets-Mode-Asset-Index)。

## 首页分钟快照（2026-11-08）

单账户及总览 portfolio 的普通请求只读持久化快照，不在请求中连接交易所；
API 启动后立即检查已有账户，此后每分钟采集余额、资产估值及仓位。
快照位于原 API 本地状态目录的 `portfolios/<account_id>.json`，只保留最新展示结果，
不作为账本、资产曲线证据或交易风控输入。无需新增表或运行数据库迁移。

快照采用原子替换、仅当前用户可读写权限和逐账户进程锁；一分钟内成功采集不重复执行，
多 worker 共享同一状态目录。账户配置指纹隔离环境、凭据及名称，修改后旧快照不可见；
删除账户后不再读取其文件。文件不包含凭据。重启后直接复用匹配配置的快照。

尚无有效快照时返回空值和等待采集提示，不以零冒充账户数据。采集失败保留旧文件，
超过两分钟未更新时显示过期提示并标记资产旧值；`checked_at` 保持原始观察时间。
显式 `refresh=true` 保留同步刷新能力，失败不覆盖旧快照。设置页余额、连接测试、
独立仓位查询和手动交易继续使用原实时查询路径。


2026-11-08：首页期权持仓另由初始基准与后续实际操作推算，读取现有账本同步状态，
不加入此处的 USD-M 分钟快照或账户权益汇总。
详见[期权操作与持仓推算](OPERATIONS_API.md#期权操作与持仓推算2026-11-08)。

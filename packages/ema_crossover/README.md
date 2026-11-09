# 策略包：目标持仓

SeeAlpha 不按策略名字在引擎里登记实现。一个策略是一份可解压的 zip，由业务端登记、版本化、按租户类型授权，再下发到某一个 `tenant_id`。引擎只加载已经下发的那一份。

## 目录

```text
seealpha-package.json    名称、版本、入口、默认参数
strategy.py               入口模块
```

`seealpha-package.json` 不能写注释。`format` 固定为 `1`。`name` 是包标识（小写字母开头），`title` 是显示名，`version` 是版本字符串，`description` 是可选说明。`entry_point` 写成 `模块:类`，例如 `strategy:EmaCrossover`。

`parameters` 与 `prepare()` 接受的字段相同：`symbol`、`fast_period`、`slow_period`、`allocation`、`risk_limit`。下发时进入策略的是交给 `prepare()` 的参数；省略的键用 `strategy.py` 里的 `DEFAULTS`。zip 根目录就是这些文件，不要再套一层目录。

## 作者只公布目标持仓

行情进入策略。指标和信号留在包内：可以自己计算，也可以用 Nautilus 的指标类（本示例用 `ExponentialMovingAverage` 和 `register_indicator_for_bars`）。它们都不是平台对象。

引擎在 `on_start` 之前放好：

- `self.parameters`：`prepare()` 返回的参数
- `self.bar_type`、`self.instrument_id`：这次下发要订阅的行情
- `self.publish(symbol, quantity=None, weight=None)`：交出一个目标持仓

`quantity` 和 `weight` 二选一，类型是 `Decimal`。`quantity` 是希望持有的带符号数量；`weight` 为 `1` 表示满额 `allocation`，`0` 表示空仓。不要调用 `submit_order` 或 `close_all_positions`。

本示例的 `on_bar` 按三步阅读：

1. 读取指标。Nautilus 在进入 `on_bar` 之前，已经用收盘价更新 `register_indicator_for_bars` 登记的两条 EMA，策略只读当前值。
2. 判断包内信号：`long`（金叉）、`flat`（死叉）或 `no-cross`（没有新交叉）。这不是平台类型。两条均线第一次同时就绪时只保存上一组数值，不算交叉。
3. 换成目标数量再 `publish`：`long` 用 `allocation`，`flat` 用 `0`。`no-cross` 直接返回，不再次公布。

`prepare(parameters)` 负责本包的参数。省略 `risk_limit` 时等于本次 `allocation`。返回值必须带 `symbol`、`allocation` 和 `risk_limit`。引擎按 `symbol` 把同一账户的目标交给现有的 `PortfolioCoordinator`，先缩放到最紧的 `risk_limit`，再对当前净持仓只提交差额。订单仍由 Nautilus RiskEngine / ExecutionEngine 检查。

登记、授权和加载走业务端已有的策略商店：`/api/strategy-packages` 上传归档，版本授权分成 `development` 和 `production`，下发到一个租户的一个账户。开发授权不能加载到正式租户。开发租户声明该品种的订阅后，可以用虚拟账户资金对已下发的包做历史回测：`POST /api/tenants/{tenant_id}/accounts/{account_id}/backtests`。

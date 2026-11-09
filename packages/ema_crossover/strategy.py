"""双均线示例。金叉公布满额多头目标，死叉公布空仓。

K 线进入 ``on_bar``，内部按三步分开：读取指标、判断交叉信号、换成目标持仓。
指标用 Nautilus 已登记的 EMA，信号只留在本包内，不是平台类型。
交给引擎的唯一出口是 ``publish(...)``，这里不调用 ``submit_order``。
同一账户的目标由 ``PortfolioCoordinator`` 净额后，再对当前净持仓提交差额。
"""

from decimal import Decimal, InvalidOperation

from nautilus_trader.indicators import ExponentialMovingAverage
from nautilus_trader.trading import Strategy

# 下发参数缺省值。prepare 只接受这些键。
DEFAULTS = {
    "symbol": "ETHUSDT",
    "fast_period": 8,
    "slow_period": 21,
    "allocation": "1",
    "risk_limit": "1",
}


def prepare(parameters):
    """整理下发参数，返回引擎要的字典。

    返回值包含 ``symbol``、``fast_period``、``slow_period``、``allocation`` 和
    ``risk_limit``。``symbol`` 是交易品种；``allocation`` 是满额多头数量；
    ``risk_limit`` 是该成员目标的风险额度。``allocation`` 与 ``risk_limit`` 以十进制字符串返回。
    调用方省略 ``risk_limit`` 时，它等于本次 ``allocation``。
    未知字段在这里拒绝；密钥字段由引擎在调用本函数之前拒绝。
    """
    if not isinstance(parameters, dict):
        raise TypeError("策略参数无效。")
    unknown = set(parameters) - set(DEFAULTS)
    if unknown:
        raise ValueError("策略参数包含未知字段。")
    merged = {**DEFAULTS, **parameters}
    # 调用方没写 risk_limit 时，风险额度等于合并后的 allocation。
    if "risk_limit" not in parameters:
        merged["risk_limit"] = merged["allocation"]
    symbol = str(merged["symbol"]).upper()
    if symbol != "ETHUSDT":
        raise ValueError("双均线示例目前只交易 ETHUSDT。")
    fast = _period(merged["fast_period"], "快均线周期")
    slow = _period(merged["slow_period"], "慢均线周期")
    if fast >= slow:
        raise ValueError("快均线周期必须小于慢均线。")
    allocation = _quantity(merged["allocation"], "分配数量")
    risk_limit = _quantity(merged["risk_limit"], "风险额度")
    return {
        "symbol": symbol,
        "fast_period": fast,
        "slow_period": slow,
        "allocation": allocation,
        "risk_limit": risk_limit,
    }


def _period(value, label):
    """均线周期：2 到 500 的整数。纯数字字符串会先转成 int。"""
    if isinstance(value, bool):
        raise TypeError(f"{label}必须是整数。")
    if isinstance(value, str) and value.isdigit():
        value = int(value)
    if not isinstance(value, int) or not 2 <= value <= 500:
        raise ValueError(f"{label}必须是 2 到 500 的整数。")
    return value


def _quantity(value, label):
    """大于零的十进制数，最多 8 位小数、30 位有效数字，返回普通小数字符串。"""
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"{label}必须是大于零的十进制数。") from None
    digits = amount.as_tuple()
    if (
        isinstance(value, bool)
        or not amount.is_finite()
        or amount <= 0
        or digits.exponent < -8
        or len(digits.digits) > 30
    ):
        raise ValueError(f"{label}必须是大于零的十进制数。")
    return format(amount, "f")


# 包内信号，不是平台类型。long 是金叉，flat 是死叉，no-cross 是没有新交叉。
_LONG = "long"
_FLAT = "flat"
_NO_CROSS = "no-cross"


class EmaCrossover(Strategy):
    """金叉后目标为满额多头，死叉后目标为 0。通过 publish 交出目标持仓。"""

    def __init__(self, config) -> None:
        super().__init__(config)
        # fast / slow 在 on_start 里创建。_previous 是上一组已就绪的快慢线。
        # _quantity 是最近一次交叉换成的目标：金叉为 allocation，死叉为 0。
        self.fast = None
        self.slow = None
        self._previous = None
        self._quantity = Decimal(0)

    def on_start(self) -> None:
        # 用 prepare 得到的周期创建 Nautilus EMA。样本数达到周期后 initialized 才为真。
        self.fast = ExponentialMovingAverage(self.parameters["fast_period"])
        self.slow = ExponentialMovingAverage(self.parameters["slow_period"])
        # 登记后，每来一根该 bar_type 的 K 线，Nautilus 在 on_bar 之前用收盘价更新指标。
        self.register_indicator_for_bars(self.bar_type, self.fast)
        self.register_indicator_for_bars(self.bar_type, self.slow)
        # bar_type 由引擎在启动前放好，这里只订阅这一路行情。
        self.subscribe_bars(self.bar_type)

    def on_bar(self, bar) -> None:
        # 指标。Nautilus 在进入本方法前已经更新登记过的 EMA，这里只读取。
        current = self._indicator_values(bar)
        if current is None:
            return
        # 信号。第一根两边都就绪的 K 线只留下比较基准，不当成交叉。
        previous = self._previous
        self._previous = current
        signal = self._cross_signal(previous, current)
        # 没有新的交叉就不改目标，也不再次公布。
        if signal == _NO_CROSS:
            return
        # 目标持仓。quantity 是 Decimal：allocation 为满额多头，0 为空仓。
        # 策略不调用 submit_order。PortfolioCoordinator 汇总目标后，只对净持仓提交差额。
        self._quantity = self._target_quantity(signal)
        self.publish(symbol=self.parameters["symbol"], quantity=self._quantity)

    def _indicator_values(self, bar):
        """读取本根 K 线上已经更新过的快慢线。未就绪时不产生信号。"""
        if bar.bar_type != self.bar_type or not self.fast.initialized or not self.slow.initialized:
            return None
        return (float(self.fast.value), float(self.slow.value))

    def _cross_signal(self, previous, current):
        """比较相邻两组快慢线，返回 long、flat 或 no-cross。"""
        if previous is None:
            return _NO_CROSS
        previous_fast, previous_slow = previous
        fast, slow = current
        # 金叉：快线从不超过慢线变成高于慢线。
        if previous_fast <= previous_slow and fast > slow:
            return _LONG
        # 死叉：快线从不低于慢线变成低于慢线。
        elif previous_fast >= previous_slow and fast < slow:
            return _FLAT
        return _NO_CROSS

    def _target_quantity(self, signal):
        """long 换成 allocation，flat 换成 0。no-cross 不会进到这里。"""
        if signal == _LONG:
            return Decimal(self.parameters["allocation"])
        return Decimal(0)

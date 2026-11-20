"""Strategy base class for packages that only publish target holdings."""


class Strategy:
    def __init__(self, config=None):
        self.config = config
        self.parameters = {}
        self.bar_type = None
        self._registered = []

    def register_indicator_for_bars(self, bar_type, indicator):
        self._registered.append(indicator)

    def subscribe_bars(self, bar_type):
        self.bar_type = bar_type

    def on_start(self):
        return None

    def on_bar(self, bar):
        return None

    def submit_order(self, *args, **kwargs):
        raise RuntimeError("策略只能公布目标持仓")

    def close_all_positions(self, *args, **kwargs):
        raise RuntimeError("策略只能公布目标持仓")

    def cancel_all_orders(self, *args, **kwargs):
        raise RuntimeError("策略只能公布目标持仓")

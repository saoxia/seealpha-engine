"""Indicator surface used by issued strategy packages.

The host updates each registered indicator with the bar close before ``on_bar``.
The smoothing constant matches the common Nautilus EMA: ``2 / (period + 1)``.
The first ``period`` samples seed ``value`` with the latest close and then mark
the indicator initialized. Later samples use the exponential update.
"""


class ExponentialMovingAverage:
    def __init__(self, period):
        period = int(period)
        if period < 1:
            raise ValueError("均线周期必须为正。")
        self.period = period
        self.alpha = 2.0 / (period + 1.0)
        self.value = 0.0
        self.count = 0
        self.initialized = False

    def update_raw(self, value):
        price = float(value)
        self.count += 1
        if self.initialized:
            self.value = self.alpha * price + (1.0 - self.alpha) * self.value
        else:
            self.value = price
            if self.count >= self.period:
                self.initialized = True

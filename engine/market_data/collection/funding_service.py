"""Compatibility import for the Binance source-owned implementation."""

import sys

from engine.market_data.sources.binance.perpetual import funding as _implementation

sys.modules[__name__] = _implementation

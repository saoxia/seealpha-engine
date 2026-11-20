"""Compatibility import for the Binance source-owned implementation."""

import sys

from engine.market_data.sources.binance_perpetual import funding_history as _implementation

sys.modules[__name__] = _implementation

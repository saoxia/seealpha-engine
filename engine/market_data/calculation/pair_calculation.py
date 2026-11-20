"""Compatibility import for the Binance source-owned implementation."""

import sys

from engine.market_data.sources.binance.shared import calculation as _implementation

sys.modules[__name__] = _implementation

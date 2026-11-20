"""Compatibility module for engine.market_data.sources.binance.common.convert_trades."""

import importlib as _importlib
import sys as _sys

_impl = _importlib.import_module("engine.market_data.sources.binance.common.convert_trades")
if __name__ == "__main__":
    _impl.main()
else:
    _sys.modules[__name__] = _impl

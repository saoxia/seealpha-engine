"""Compatibility import for engine.market_data.sources.binance.common.calculation; implementation lives in the Binance source package."""

import importlib as _importlib
import sys as _sys

_impl = _importlib.import_module("engine.market_data.sources.binance.common.calculation")
_sys.modules[__name__] = _impl

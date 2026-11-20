"""Backward-compatible alias for engine.market_data.sources.binance.options.repair."""

import importlib as _importlib
import sys as _sys

_impl = _importlib.import_module("engine.market_data.sources.binance.options.repair")
_sys.modules[__name__] = _impl

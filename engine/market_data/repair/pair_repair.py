"""Backward-compatible alias for engine.market_data.sources.binance.common.reconcile."""

import importlib as _importlib
import sys as _sys

_impl = _importlib.import_module("engine.market_data.sources.binance.common.reconcile")
_sys.modules[__name__] = _impl

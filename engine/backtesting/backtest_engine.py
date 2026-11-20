"""Shared configuration for isolated, offline NautilusTrader backtests.

Keep the concrete BacktestEngine construction at each call site so tests and
specialized research paths can substitute engines without changing the policy.
"""
from nautilus_trader.config import BacktestEngineConfig, LoggerConfig


def backtest_engine_config():
    """Return a fresh config with identical logging and replay defaults."""
    return BacktestEngineConfig(logging=LoggerConfig(bypass_logging=True))

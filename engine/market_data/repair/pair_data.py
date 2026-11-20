"""Compatibility import for the Binance source-owned implementation."""

if __name__ == "__main__":
    from engine.market_data.sources.binance_shared.history import main

    main()
else:
    import sys

    from engine.market_data.sources.binance_shared import history as _implementation

    sys.modules[__name__] = _implementation

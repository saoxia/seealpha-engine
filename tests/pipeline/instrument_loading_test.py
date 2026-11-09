import asyncio

from nautilus_trader.adapters.binance import BinanceEnvironment, BinanceProductType

from engine.pipeline import prepare_data
from nautilus_support import TestInstrumentProvider


def test_native_instrument_request_uses_usdm_and_public_fee_policy(monkeypatch):
    instrument = TestInstrumentProvider.ethusdt_perp_binance()
    seen = []

    async def load(config):
        seen.append(config)
        return [instrument]

    monkeypatch.setattr(prepare_data, "load_binance_instruments", load)
    result = asyncio.run(prepare_data.load_missing_instruments(["ETHUSDT"]))
    assert result == {"ETHUSDT": instrument}
    assert seen[0].product_type == BinanceProductType.USD_M
    assert seen[0].environment == BinanceEnvironment.LIVE
    assert seen[0].instrument_provider.load_ids == [str(instrument.id)]
    assert not seen[0].instrument_provider.query_commission_rates


def test_legacy_catalog_is_rejected_instead_of_silently_empty(tmp_path):
    import pytest

    from engine.pipeline.catalog import ParquetDataCatalog

    (tmp_path / "data/bar").mkdir(parents=True)
    with pytest.raises(ValueError, match="Legacy v1"):
        ParquetDataCatalog(tmp_path)

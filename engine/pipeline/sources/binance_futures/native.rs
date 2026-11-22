//! Binance USD-M native data-product identity; COIN-M is not implemented.
use nautilus_binance::common::enums::BinanceProductType;
use nautilus_model::identifiers::InstrumentId;

pub(super) fn product_type() -> BinanceProductType {
    BinanceProductType::UsdM
}

pub(super) fn instrument_id(symbol: &str) -> InstrumentId {
    InstrumentId::from(format!("{symbol}-PERP.BINANCE").as_str())
}

pub(super) fn client_name() -> &'static str {
    "SEEALPHA-BINANCE-UM-DATA"
}

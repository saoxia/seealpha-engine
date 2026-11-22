//! Binance Spot native data-product identity; shared LiveNode creates its own process.
use nautilus_binance::common::enums::BinanceProductType;
use nautilus_model::identifiers::InstrumentId;

pub(super) fn product_type() -> BinanceProductType {
    BinanceProductType::Spot
}

pub(super) fn instrument_id(symbol: &str) -> InstrumentId {
    InstrumentId::from(format!("{symbol}.BINANCE").as_str())
}

pub(super) fn client_name() -> &'static str {
    "SEEALPHA-BINANCE-SPOT-DATA"
}

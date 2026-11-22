//! Binance product identity policy. Catalog accepts only normalized Nautilus IDs.
use crate::{MarketError, options::parse_option_symbol};
use seealpha_catalog::{CatalogBar, bar_type as storage_bar_type};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketProduct {
    Spot,
    FuturesUm,
    Options,
}
impl MarketProduct {
    pub fn parse(raw: &str) -> Result<Self, MarketError> {
        match raw {
            "spot" => Ok(Self::Spot),
            "futures_um" => Ok(Self::FuturesUm),
            "options" => Ok(Self::Options),
            _ => Err(MarketError::UnsupportedProduct),
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::FuturesUm => "futures_um",
            Self::Options => "options",
        }
    }
}

pub fn instrument_id(symbol: &str, product: MarketProduct) -> Result<String, MarketError> {
    if symbol.is_empty() || symbol.len() > 64 || symbol != symbol.trim()
        || symbol != symbol.to_ascii_uppercase()
    {
        return Err(MarketError::Symbol);
    }
    match product {
        MarketProduct::Spot => {
            if !(3..=30).contains(&symbol.len())
                || !symbol.bytes().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
            {
                return Err(MarketError::Symbol);
            }
            Ok(format!("{symbol}.BINANCE"))
        }
        MarketProduct::FuturesUm => {
            if !(5..=30).contains(&symbol.len())
                || !(symbol.ends_with("USDT") || symbol.ends_with("USDC"))
                || !symbol.bytes().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
            {
                return Err(MarketError::Symbol);
            }
            Ok(format!("{symbol}-PERP.BINANCE"))
        }
        MarketProduct::Options => {
            parse_option_symbol(symbol)?;
            Ok(format!("{symbol}.BINANCE"))
        }
    }
}

pub fn bar_type(symbol: &str, product: MarketProduct, interval: &str) -> Result<String, MarketError> {
    let id = instrument_id(symbol, product)?;
    storage_bar_type(&id, interval).map_err(|_| MarketError::Interval)
}

/// Reverse only Binance identities for presentation; generic Catalog never calls this.
pub fn parse_instrument_id(raw: &str) -> Result<(String, MarketProduct), MarketError> {
    let (symbol, product) = if let Some(symbol) = raw.strip_suffix("-PERP.BINANCE") {
        (symbol, MarketProduct::FuturesUm)
    } else if let Some(symbol) = raw.strip_suffix(".BINANCE") {
        if symbol.contains('-') {
            (symbol, MarketProduct::Options)
        } else {
            (symbol, MarketProduct::Spot)
        }
    } else {
        return Err(MarketError::Symbol);
    };
    if instrument_id(symbol, product)? != raw {
        return Err(MarketError::Symbol);
    }
    Ok((symbol.to_owned(), product))
}

/// Product-specific price rules stay in Source, not in the Parquet storage model.
pub fn validate_market_bar(bar: &CatalogBar, product: MarketProduct) -> Result<(), MarketError> {
    if product != MarketProduct::Options
        && [bar.open, bar.high, bar.low, bar.close].iter().any(|v| v.is_zero())
    {
        return Err(MarketError::Ohlc);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn product_identities_are_source_owned() {
        assert_eq!(instrument_id("ETHUSDT", MarketProduct::Spot).unwrap(), "ETHUSDT.BINANCE");
        assert_eq!(instrument_id("ETHUSDT", MarketProduct::FuturesUm).unwrap(), "ETHUSDT-PERP.BINANCE");
        assert_eq!(bar_type("BTC-271231-90000-P", MarketProduct::Options, "5m").unwrap(),
            "BTC-271231-90000-P.BINANCE-5-MINUTE-LAST-EXTERNAL");
        assert!(instrument_id("BTC-271231-90000-X", MarketProduct::Options).is_err());
        assert!(instrument_id("../ETH", MarketProduct::Spot).is_err());
    }
}

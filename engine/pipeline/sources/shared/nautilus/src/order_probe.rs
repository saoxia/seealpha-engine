//! Manual order command boundary: Nautilus owns order construction, sending and OMS.
//! No exchange-specific order sender, account budget or arbitrary quantity limit here.
use anyhow::{Context, Result, ensure};
use nautilus_binance::common::enums::BinanceEnvironment;
use nautilus_model::{
    enums::OrderSide,
    types::{Price, Quantity},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::native_execution::{
    NativeExecutionTarget, OrderCapability, PositionMode, resolve_native_target,
};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TradingEnvironment {
    Live,
    Testnet,
}

impl TradingEnvironment {
    pub const fn native(self) -> BinanceEnvironment {
        match self {
            Self::Live => BinanceEnvironment::Live,
            Self::Testnet => BinanceEnvironment::Testnet,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderKind {
    Market,
    Limit,
    TakeProfitMarket,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeOrderRequest {
    pub environment: TradingEnvironment,
    /// spot or futures_um.
    pub product: String,
    /// spot, one_way, hedge. The configured mode must match the exchange account.
    pub position_mode: String,
    pub symbol: String,
    pub account_id: String,
    /// buy or sell.
    pub side: String,
    pub kind: OrderKind,
    pub quantity: String,
    /// Limit price for limit orders; trigger price for take-profit-market; omit for market.
    pub price: Option<String>,
    #[serde(default)]
    pub reduce_only: bool,
    /// Optional native cancellation as soon as the venue accepts the order.
    #[serde(default)]
    pub cancel_on_accept: bool,
}

#[derive(Debug)]
pub struct ValidatedOrder {
    pub route: NativeExecutionTarget,
    pub environment: TradingEnvironment,
    pub side: OrderSide,
    pub kind: OrderKind,
    pub quantity: Quantity,
    pub price: Option<Price>,
    pub reduce_only: bool,
    pub cancel_on_accept: bool,
}

impl NativeOrderRequest {
    pub fn validate(&self) -> Result<ValidatedOrder> {
        let mode = match self.position_mode.as_str() {
            "spot" => PositionMode::Spot,
            "one_way" => PositionMode::OneWay,
            "hedge" => PositionMode::Hedge,
            _ => anyhow::bail!("position_mode must be spot, one_way or hedge"),
        };
        let route = resolve_native_target(
            &self.product, &self.symbol, &self.account_id,
            mode, OrderCapability::StandardSingle,
        )?;
        let side = match self.side.as_str() {
            "buy" => OrderSide::Buy,
            "sell" => OrderSide::Sell,
            _ => anyhow::bail!("side must be buy or sell"),
        };
        // Exchange protocol restriction, not an account risk limit.
        ensure!(!(self.product == "spot" && self.reduce_only),
            "Binance Spot does not support reduce_only");

        let qty_decimal = Decimal::from_str(&self.quantity)
            .context("invalid decimal quantity")?;
        ensure!(qty_decimal > Decimal::ZERO, "quantity must be positive");
        let quantity = self.quantity.parse::<Quantity>()
            .map_err(anyhow::Error::msg).context("invalid Nautilus Quantity")?;

        let price = match (self.kind, self.price.as_deref()) {
            (OrderKind::Market, None) => None,
            (OrderKind::Market, Some(_)) =>
                anyhow::bail!("market orders must not supply a price"),
            (_, None) => anyhow::bail!("limit and take-profit orders require price"),
            (_, Some(raw)) => {
                let value = Decimal::from_str(raw).context("invalid decimal price")?;
                ensure!(value > Decimal::ZERO, "price must be positive");
                Some(raw.parse::<Price>().map_err(anyhow::Error::msg)
                    .context("invalid Nautilus Price")?)
            }
        };

        Ok(ValidatedOrder {
            route,
            environment: self.environment,
            side,
            kind: self.kind,
            quantity,
            price,
            reduce_only: self.reduce_only,
            cancel_on_accept: self.cancel_on_accept,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn short_exit() -> NativeOrderRequest {
        NativeOrderRequest {
            environment: TradingEnvironment::Testnet,
            product: "futures_um".into(),
            position_mode: "one_way".into(),
            symbol: "ETHUSDT".into(),
            account_id: "BINANCE-UM-001".into(),
            side: "buy".into(),
            kind: OrderKind::Limit,
            quantity: "2".into(),
            price: Some("1000.00".into()),
            reduce_only: true,
            cancel_on_accept: false,
        }
    }

    #[test]
    fn one_way_limit_reduce_only_has_buy_short_close_intent() {
        let v = short_exit().validate().unwrap();
        assert_eq!(v.route.instrument.to_string(), "ETHUSDT-PERP.BINANCE");
        assert_eq!(v.side, OrderSide::Buy);
        assert_eq!(v.route.oms.to_string(), "NETTING");
        assert!(v.reduce_only);
        assert_eq!(v.price.unwrap().to_string(), "1000.00");
    }

    #[test]
    fn hedge_mode_uses_native_hedging_oms() {
        let mut p = short_exit();
        p.position_mode = "hedge".into();
        let v = p.validate().unwrap();
        assert_eq!(v.route.oms.to_string(), "HEDGING");
        assert!(v.reduce_only);
        // Nautilus Binance v2.0.0rc9 converts BUY+reduce_only to SHORT positionSide.
    }

    #[test]
    fn take_profit_market_has_trigger_not_execution_limit() {
        let mut p = short_exit();
        p.kind = OrderKind::TakeProfitMarket;
        let v = p.validate().unwrap();
        assert_eq!(v.kind, OrderKind::TakeProfitMarket);
        assert_eq!(v.price.unwrap().to_string(), "1000.00");
    }

    #[test]
    fn no_arbitrary_quantity_cap_or_environment_gate() {
        let mut p = short_exit();
        p.quantity = "200".into();
        p.environment = TradingEnvironment::Live;
        assert_eq!(p.validate().unwrap().environment, TradingEnvironment::Live);
    }

    #[test]
    fn regular_market_requires_no_price() {
        let mut p = short_exit();
        p.kind = OrderKind::Market;
        p.price = None;
        assert!(p.validate().is_ok());
        p.price = Some("1000".into());
        assert!(p.validate().is_err());
    }

    #[test]
    fn spot_rejects_reduce_only_due_to_venue_protocol() {
        let mut p = short_exit();
        p.product = "spot".into();
        p.position_mode = "spot".into();
        assert!(p.validate().is_err());
        p.reduce_only = false;
        assert!(p.validate().is_ok());
        p.kind = OrderKind::TakeProfitMarket;
        assert!(p.validate().is_ok());
    }

    #[test]
    fn rejects_invalid_quantities_and_routes() {
        let mut p = short_exit();
        p.quantity = "0".into();
        assert!(p.validate().is_err());
        p.quantity = "-5".into();
        assert!(p.validate().is_err());
        p = short_exit();
        p.position_mode = "unknown".into();
        assert!(p.validate().is_err());
        p = short_exit();
        p.side = "BUY".into();
        assert!(p.validate().is_err());
    }

    #[test]
    fn unknown_parameters_rejected_instead_of_silently_ignored() {
        let mut value = serde_json::to_value(short_exit()).unwrap();
        value["close_position"] = serde_json::json!(true);
        assert!(serde_json::from_value::<NativeOrderRequest>(value).is_err());
    }
}

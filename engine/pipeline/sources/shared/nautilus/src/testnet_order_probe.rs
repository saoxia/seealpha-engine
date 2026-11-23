//! A deliberately narrow, testnet-only order probe contract.
//!
//! Not a SeeAlpha OMS or an execution authorization for the production engine.
//! Nautilus creates the actual orders and owns their lifecycle.
use anyhow::{Context, Result, ensure};
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProbeKind {
    Limit,
    TakeProfitMarket,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestnetOrderProbe {
    /// Unique, caller-supplied ID: a durable claim prevents retry after a crash.
    pub request_id: String,
    /// spot or futures_um.
    pub product: String,
    /// spot, one_way or hedge. Must match the actual exchange account mode.
    pub position_mode: String,
    /// Native Binance symbol, e.g. ETHUSDT.
    pub symbol: String,
    pub account_id: String,
    /// buy or sell.
    pub side: String,
    pub kind: ProbeKind,
    pub quantity: String,
    /// For limit: limit price. For take_profit_market: trigger price, NOT a fill limit.
    pub price: String,
    pub reduce_only: bool,
}

#[derive(Debug)]
pub struct ValidatedProbe {
    pub route: NativeExecutionTarget,
    pub side: OrderSide,
    pub kind: ProbeKind,
    pub quantity: Quantity,
    pub price: Price,
    pub reduce_only: bool,
}

impl TestnetOrderProbe {
    pub fn validate(&self) -> Result<ValidatedProbe> {
        ensure!(
            (8..=48).contains(&self.request_id.len())
                && self.request_id.bytes().all(|b|
                    b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_' || b == b'-'),
            "request_id must be 8..48 uppercase ASCII letters/digits/_/-"
        );
        let mode = match self.position_mode.as_str() {
            "spot" => PositionMode::Spot,
            "one_way" => PositionMode::OneWay,
            "hedge" => PositionMode::Hedge,
            _ => anyhow::bail!("unsupported or unverified position mode"),
        };
        let route = resolve_native_target(
            &self.product,
            &self.symbol,
            &self.account_id,
            mode,
            // Routing identity is reused; conditional execution stays explicitly testnet-only.
            OrderCapability::StandardSingle,
        )?;
        let side = match self.side.as_str() {
            "buy" => OrderSide::Buy,
            "sell" => OrderSide::Sell,
            _ => anyhow::bail!("side must be buy or sell"),
        };
        ensure!(
            !(self.product == "spot" && self.reduce_only),
            "Binance spot does not support reduce_only"
        );
        ensure!(
            self.kind != ProbeKind::TakeProfitMarket ||
                (self.product == "futures_um" && self.reduce_only),
            "conditional take-profit probe requires USD-M and explicit reduce_only"
        );
        let qty_decimal = Decimal::from_str(&self.quantity).context("invalid decimal quantity")?;
        let price_decimal = Decimal::from_str(&self.price).context("invalid decimal price")?;
        // A small one-shot testnet harness, never intended for arbitrary production sizing.
        ensure!(qty_decimal > Decimal::ZERO && qty_decimal <= Decimal::ONE,
            "probe quantity must be >0 and <=1 unit");
        ensure!(price_decimal > Decimal::ZERO,
            "probe price/trigger must be positive");
        let quantity = self.quantity.parse::<Quantity>()
            .map_err(anyhow::Error::msg).context("not a Nautilus Quantity")?;
        let price = self.price.parse::<Price>()
            .map_err(anyhow::Error::msg).context("not a Nautilus Price")?;
        Ok(ValidatedProbe {
            route, side, kind: self.kind, quantity, price, reduce_only: self.reduce_only,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn short_exit() -> TestnetOrderProbe {
        TestnetOrderProbe {
            request_id: "TESTNET-EXIT-0001".into(),
            product: "futures_um".into(),
            position_mode: "one_way".into(),
            symbol: "ETHUSDT".into(),
            account_id: "BINANCE-UM-SANDBOX".into(),
            side: "buy".into(),
            kind: ProbeKind::Limit,
            quantity: "0.01".into(),
            price: "1000.00".into(),
            reduce_only: true,
        }
    }

    #[test]
    fn one_way_reduce_only_limit_is_native_buy_to_close_short() {
        let v = short_exit().validate().unwrap();
        assert_eq!(v.side, OrderSide::Buy);
        assert!(v.reduce_only);
        assert_eq!(v.route.instrument.to_string(), "ETHUSDT-PERP.BINANCE");
        assert_eq!(v.route.oms.to_string(), "NETTING");
    }

    #[test]
    fn hedge_reduce_only_uses_native_hedging_oms() {
        let mut p = short_exit();
        p.position_mode = "hedge".into();
        let v = p.validate().unwrap();
        assert_eq!(v.route.oms.to_string(), "HEDGING");
        assert!(v.reduce_only);
        // Binance's native adapter converts BUY + reduce_only to positionSide=SHORT.
    }

    #[test]
    fn conditional_take_profit_uses_market_if_touched_not_price_guarantee() {
        let mut p = short_exit();
        p.kind = ProbeKind::TakeProfitMarket;
        let v = p.validate().unwrap();
        assert_eq!(v.kind, ProbeKind::TakeProfitMarket);
        assert!(v.reduce_only);
        p.reduce_only = false;
        assert!(p.validate().is_err());
    }

    #[test]
    fn rejects_spot_reduce_only_and_conditional_close() {
        let mut p = short_exit();
        p.product = "spot".into();
        p.position_mode = "spot".into();
        assert!(p.validate().is_err());
        p.reduce_only = false;
        p.kind = ProbeKind::TakeProfitMarket;
        assert!(p.validate().is_err());
        p.kind = ProbeKind::Limit;
        assert!(p.validate().is_ok());
    }

    #[test]
    fn rejects_ambiguous_or_unsafe_requests() {
        let mut p = short_exit();
        p.request_id = "../attempt".into();
        assert!(p.validate().is_err());
        p = short_exit();
        p.quantity = "0".into();
        assert!(p.validate().is_err());
        p.quantity = "1.1".into();
        assert!(p.validate().is_err());
        p = short_exit();
        p.position_mode = "unknown".into();
        assert!(p.validate().is_err());
        p = short_exit();
        p.side = "BUY".into();
        assert!(p.validate().is_err());
    }

    #[test]
    fn rejects_extra_json_fields_instead_of_ignoring_them() {
        let mut value = serde_json::to_value(short_exit()).unwrap();
        value["close_position"] = serde_json::json!(true);
        assert!(serde_json::from_value::<TestnetOrderProbe>(value).is_err());
    }
}

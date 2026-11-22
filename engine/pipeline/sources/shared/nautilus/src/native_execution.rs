//! Fail-closed SeeAlpha -> Nautilus Binance native execution identity mapping.
//! This is a deterministic routing contract, not a sender or an authorization.
//! Runtime must commit the immutable intent before any future native submission.
use anyhow::{Result, bail, ensure};
use nautilus_binance::common::enums::BinanceProductType;
use nautilus_model::{
    data::NautilusRecordType,
    enums::OmsType,
    identifiers::{AccountId, InstrumentId},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeProduct {
    Spot,
    UsdM,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PositionMode {
    // Spot is always Hedging in the pinned native Binance execution factory.
    Spot,
    OneWay,
    Hedge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderCapability {
    StandardSingle,
    SpotOco,
    UsdMAlgo,
    CancelReplace,
    OtherUnsupported,
}

/// Route metadata only. No keys, endpoints, order contents or permissions.
#[derive(Debug)]
pub struct NativeExecutionTarget {
    pub product: BinanceProductType,
    pub oms: OmsType,
    pub account: AccountId,
    pub instrument: InstrumentId,
}

/// Nautilus-native execution records to retain in its own Streaming/Catalog
/// writer. The SeeAlpha Ledger must not duplicate the raw event stream.
/// This is a record selection only: it neither starts a node nor authenticates
/// a report or grants permission to submit an order.
pub fn native_execution_record_types() -> Vec<NautilusRecordType> {
    use NautilusRecordType as R;
    // Capture the Nautilus-native lifecycle, not a second SeeAlpha order log.
    // The downstream writer owns durability and gap/restart verification.
    vec![
        R::AccountState,
        R::OrderInitialized,
        R::OrderDenied,
        R::OrderEmulated,
        R::OrderSubmitted,
        R::OrderAccepted,
        R::OrderRejected,
        R::OrderPendingCancel,
        R::OrderCanceled,
        R::OrderCancelRejected,
        R::OrderExpired,
        R::OrderTriggered,
        R::OrderPendingUpdate,
        R::OrderReleased,
        R::OrderModifyRejected,
        R::OrderUpdated,
        R::OrderFilled,
        R::OrderFillVoided,
        R::PositionOpened,
        R::PositionChanged,
        R::PositionClosed,
        R::PositionAdjusted,
        R::OrderSnapshot,
        R::PositionSnapshot,
        R::OrderStatusReport,
        R::FillReport,
        R::PositionStatusReport,
        R::ExecutionMassStatus,
    ]
}

/// Reject unsupported market-specific behaviors rather than silently
/// downgrading contingent orders into an unprotected single order.
pub fn resolve_native_target(
    product: &str,
    symbol: &str,
    account_id: &str,
    mode: PositionMode,
    capability: OrderCapability,
) -> Result<NativeExecutionTarget> {
    ensure!(
        capability == OrderCapability::StandardSingle,
        "native execution capability not verified for this order type"
    );
    ensure!(
        !account_id.is_empty()
            && account_id.len() <= 40
            && account_id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "invalid native account identity"
    );
    let (market, oms, venue_symbol) = match product {
        "spot" => {
            ensure!(mode == PositionMode::Spot, "Spot OMS must use native Hedging mode");
            (
                NativeProduct::Spot,
                OmsType::Hedging,
                symbol.to_owned(),
            )
        }
        "futures_um" => {
            let oms = match mode {
                PositionMode::OneWay => OmsType::Netting,
                PositionMode::Hedge => OmsType::Hedging,
                PositionMode::Spot => bail!("USD-M position mode must be explicitly verified"),
            };
            let bare = symbol.strip_suffix("-PERP").unwrap_or(symbol);
            (NativeProduct::UsdM, oms, bare.to_owned())
        }
        // The pinned native Binance Spot/UsdM factory is not an EAPI client.
        _ => bail!("unsupported product for native Binance Spot/USD-M execution"),
    };
    ensure!(
        (5..=30).contains(&venue_symbol.len())
            && venue_symbol.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()),
        "invalid native Binance Spot/USD-M symbol"
    );
    let (native_product, native_instrument) = match market {
        NativeProduct::Spot => (
            BinanceProductType::Spot,
            format!("{venue_symbol}.BINANCE"),
        ),
        NativeProduct::UsdM => (
            BinanceProductType::UsdM,
            format!("{venue_symbol}-PERP.BINANCE"),
        ),
    };
    Ok(NativeExecutionTarget {
        product: native_product,
        oms,
        account: AccountId::from(account_id),
        instrument: InstrumentId::from(native_instrument.as_str()),
    })
}


/// Minimal order identity projection from a Nautilus native event.
///
/// Keep Nautilus OrderFilled as the execution model and event-store record.
/// This view is only for SeeAlpha's virtual-order association and audits.
/// It is NOT a certified exchange trade, a fill-ledger posting, or a
/// replacement for Nautilus OrderCore/ExecutionEngine state.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NativeFillLink {
    pub account_id: String,
    pub instrument_id: String,
    pub client_order_id: String,
    pub venue_order_id: String,
    pub trade_id: String,
    pub event_id: String,
    pub event_ts_ns: u64,
    pub last_quantity: String,
    pub last_price: String,
    pub position_id: Option<String>,
    pub reconciliation: bool,
}

/// Use Nautilus' typed fill and exact decimal display representations.
/// The downstream account-scoped consumer must establish trusted provenance
/// and handle repeated trade IDs / restatements before allocating economics.
pub fn link_native_fill(
    event: &nautilus_model::events::order::filled::OrderFilled,
) -> NativeFillLink {
    NativeFillLink {
        account_id: event.account_id.to_string(),
        instrument_id: event.instrument_id.to_string(),
        client_order_id: event.client_order_id.to_string(),
        venue_order_id: event.venue_order_id.to_string(),
        trade_id: event.trade_id.to_string(),
        event_id: event.event_id.to_string(),
        event_ts_ns: event.ts_event.as_u64(),
        last_quantity: event.last_qty.to_string(),
        last_price: event.last_px.to_string(),
        position_id: event.position_id.map(|id| id.to_string()),
        reconciliation: event.reconciliation,
    }
}

/// An accepted order is NOT a fill. Preserve native identities for linking,
/// but never infer executed quantity or strategy accounting from an ACK.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NativeAcceptLink {
    pub account_id: String,
    pub instrument_id: String,
    pub client_order_id: String,
    pub venue_order_id: String,
    pub event_id: String,
    pub event_ts_ns: u64,
    pub reconciliation: bool,
}

pub fn link_native_accept(
    event: &nautilus_model::events::order::accepted::OrderAccepted,
) -> NativeAcceptLink {
    NativeAcceptLink {
        account_id: event.account_id.to_string(),
        instrument_id: event.instrument_id.to_string(),
        client_order_id: event.client_order_id.to_string(),
        venue_order_id: event.venue_order_id.to_string(),
        event_id: event.event_id.to_string(),
        event_ts_ns: event.ts_event.as_u64(),
        reconciliation: event.reconciliation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spot_and_usdm_resolve_to_distinct_native_routes() {
        let spot = resolve_native_target(
            "spot", "BTCUSDT", "BINANCE-SPOT-001",
            PositionMode::Spot, OrderCapability::StandardSingle,
        ).unwrap();
        assert_eq!(spot.product, BinanceProductType::Spot);
        assert_eq!(spot.oms, OmsType::Hedging);
        assert_eq!(spot.instrument.to_string(), "BTCUSDT.BINANCE");
        assert_eq!(spot.account.to_string(), "BINANCE-SPOT-001");

        let netting = resolve_native_target(
            "futures_um", "BTCUSDT-PERP", "BINANCE-UM-001",
            PositionMode::OneWay, OrderCapability::StandardSingle,
        ).unwrap();
        assert_eq!(netting.product, BinanceProductType::UsdM);
        assert_eq!(netting.oms, OmsType::Netting);
        assert_eq!(netting.instrument.to_string(), "BTCUSDT-PERP.BINANCE");
        let hedging = resolve_native_target(
            "futures_um", "BTCUSDT", "BINANCE-UM-HEDGE-001",
            PositionMode::Hedge, OrderCapability::StandardSingle,
        ).unwrap();
        assert_eq!(hedging.oms, OmsType::Hedging);
    }

    #[test]
    fn unsupported_products_modes_orders_and_identities_fail_closed() {
        for product in ["options", "option", "delivery", "", "spot_options"] {
            assert!(resolve_native_target(
                product, "BTCUSDT", "ACCOUNT-1",
                PositionMode::Spot, OrderCapability::StandardSingle,
            ).is_err());
        }
        for capability in [
            OrderCapability::SpotOco,
            OrderCapability::UsdMAlgo,
            OrderCapability::CancelReplace,
            OrderCapability::OtherUnsupported,
        ] {
            assert!(resolve_native_target(
                "spot", "BTCUSDT", "ACCOUNT-1", PositionMode::Spot, capability,
            ).is_err());
        }
        assert!(resolve_native_target(
            "futures_um", "BTCUSDT", "ACCOUNT-1",
            PositionMode::Spot, OrderCapability::StandardSingle,
        ).is_err());
        assert!(resolve_native_target(
            "spot", "BTCUSDT", "ACCOUNT-1",
            PositionMode::Hedge, OrderCapability::StandardSingle,
        ).is_err());
        for symbol in ["btcUSDT", "BTC/USDT", "BTC-USDT", "BTCUSDT-PERP", "", "BTCUSDT.BINANCE"] {
            assert!(resolve_native_target(
                "spot", symbol, "ACCOUNT-1",
                PositionMode::Spot, OrderCapability::StandardSingle,
            ).is_err());
        }
        for account in ["", "../abc", "acct:1", "abc.def"] {
            assert!(resolve_native_target(
                "spot", "BTCUSDT", account,
                PositionMode::Spot, OrderCapability::StandardSingle,
            ).is_err());
        }
    }
    #[test]
    fn native_record_selection_retains_full_order_and_position_lifecycle() {
        use NautilusRecordType as R;
        let selected = native_execution_record_types();
        for required in [
            R::AccountState, R::OrderInitialized, R::OrderDenied,
            R::OrderSubmitted, R::OrderAccepted, R::OrderRejected,
            R::OrderPendingCancel, R::OrderCancelRejected, R::OrderCanceled,
            R::OrderPendingUpdate, R::OrderUpdated, R::OrderModifyRejected,
            R::OrderFilled, R::OrderFillVoided, R::PositionOpened,
            R::PositionChanged, R::PositionClosed, R::PositionStatusReport,
            R::OrderStatusReport, R::FillReport, R::ExecutionMassStatus,
        ] {
            assert!(selected.contains(&required), "missing {required:?}");
        }
        for (index, record) in selected.iter().enumerate() {
            assert!(!selected[..index].contains(record), "duplicate {record:?}");
        }
    }

}

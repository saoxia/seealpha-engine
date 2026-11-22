//! Nautilus-only venue execution facts with SeeAlpha-specific strategy allocation.
//!
//! OrderCore remains the sole real OMS state. Virtual allocation is a pure
//! function of the immutable preregistered member plan and the native filled
//! quantity, with its preceding committed cumulative quantity supplied by the
//! caller. No coordinator, venue Flight, synthesized fill or order transmitter.
use std::collections::BTreeSet;

use anyhow::{Context, Result, bail, ensure};
use nautilus_model::{enums::OrderSide, orders::OrderCore};
use rust_decimal::Decimal;

use crate::{
    native_execution::{NativeFillLink, NativeOrderBinding, check_native_fill_binding},
    native_order_link::NativeOrderObservation,
};

/// Ordered, immutable strategy allocation as registered before native send.
/// Claims must have one side, and their absolute quantities must sum exactly
/// to the Nautilus order quantity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberClaim {
    pub member_id: String,
    pub quantity: Decimal,
}

/// Economic attribution proposal; not an authenticated Ledger posting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VirtualFill {
    pub member_id: String,
    pub client_order_id: String,
    pub event_id: String,
    pub trade_id: String,
    pub signed_quantity: Decimal,
    pub price: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VirtualAttribution {
    pub event_id: String,
    pub trade_id: String,
    pub client_order_id: String,
    pub prior_cumulative: Decimal,
    pub cumulative: Decimal,
    pub fills: Vec<VirtualFill>,
}

/// FIFO attribution is deterministic for any split of venue fills, including
/// an execution replay after restart. The caller must persist `cumulative`
/// and the event/trade identity in one transaction with the virtual fills.
/// An identical event cannot be replayed against the same prior cumulative.
fn allocate_fifo(
    claims: &[MemberClaim],
    prior: Decimal,
    cumulative: Decimal,
    price: Decimal,
    side: OrderSide,
    fill: &NativeFillLink,
) -> Result<Vec<VirtualFill>> {
    ensure!(prior >= Decimal::ZERO && cumulative > prior, "non-increasing native fill");
    let signed = match side {
        OrderSide::Buy => Decimal::ONE,
        OrderSide::Sell => -Decimal::ONE,
    };
    let mut offset = Decimal::ZERO;
    let mut seen = BTreeSet::new();
    let mut fills = Vec::new();
    for claim in claims {
        ensure!(!claim.member_id.trim().is_empty() && seen.insert(claim.member_id.as_str())
            && claim.quantity > Decimal::ZERO, "invalid virtual member claim");
        let start = offset;
        offset = offset.checked_add(claim.quantity).context("claim quantity overflow")?;
        let after = (cumulative - start).max(Decimal::ZERO).min(claim.quantity);
        let before = (prior - start).max(Decimal::ZERO).min(claim.quantity);
        let delta = after.checked_sub(before).context("virtual fill overflow")?;
        if delta > Decimal::ZERO {
            fills.push(VirtualFill {
                member_id: claim.member_id.clone(),
                client_order_id: fill.client_order_id.clone(),
                event_id: fill.event_id.clone(),
                trade_id: fill.trade_id.clone(),
                signed_quantity: delta * signed,
                price,
            });
        }
    }
    ensure!(offset >= cumulative, "fill exceeds registered claims");
    let sum = fills.iter().try_fold(Decimal::ZERO, |acc, f| {
        acc.checked_add(f.signed_quantity.abs()).context("fill sum overflow")
    })?;
    ensure!(sum == cumulative - prior, "unattributable native execution");
    Ok(fills)
}

/// Bind a native event and its *post-event* OrderCore to a persisted immutable
/// member plan. Error means stop downstream allocation and perform native
/// order/account reconciliation; it never implies that a venue order vanished.
pub fn project_native_order_observation(
    observation: &NativeOrderObservation,
    native_order: &OrderCore,
    binding: &NativeOrderBinding<'_>,
    previous_cumulative: Decimal,
    claims: &[MemberClaim],
) -> Result<Option<VirtualAttribution>> {
    ensure!(!binding.account_id.is_empty() && !binding.instrument_id.is_empty()
        && !binding.client_order_id.is_empty(), "missing native order scope");
    ensure!(
        native_order.account_id.as_ref().is_some_and(|v| v.to_string() == binding.account_id)
        && native_order.instrument_id.to_string() == binding.instrument_id
        && native_order.client_order_id.to_string() == binding.client_order_id
        && binding.venue_order_id.is_none_or(|id| {
            native_order.venue_order_id.as_ref().is_some_and(|v| v.to_string() == id)
        }), "native OrderCore disagrees with preregistered order"
    );
    match observation {
        NativeOrderObservation::Accepted(accepted) => {
            ensure!(accepted.account_id == binding.account_id
                && accepted.instrument_id == binding.instrument_id
                && accepted.client_order_id == binding.client_order_id,
                "native accepted event identity mismatch");
            Ok(None)
        }
        NativeOrderObservation::FillVoided { .. } => {
            bail!("native fill correction requires order/account reconciliation")
        }
        NativeOrderObservation::Filled(fill) => project_fill(
            fill, native_order, binding, previous_cumulative, claims,
        ).map(Some),
    }
}

fn project_fill(
    fill: &NativeFillLink,
    native_order: &OrderCore,
    binding: &NativeOrderBinding<'_>,
    previous_cumulative: Decimal,
    claims: &[MemberClaim],
) -> Result<VirtualAttribution> {
    check_native_fill_binding(fill, binding)?;
    ensure!(native_order.venue_order_id.as_ref()
        .is_some_and(|v| v.to_string() == fill.venue_order_id),
        "native venue order identity mismatch");
    let ordered = native_order.quantity.to_string().parse::<Decimal>()
        .context("invalid native order quantity")?;
    let cumulative = native_order.filled_qty.to_string().parse::<Decimal>()
        .context("invalid native filled quantity")?;
    let last = fill.last_quantity.parse::<Decimal>()
        .context("invalid native event fill quantity")?;
    let price = fill.last_price.parse::<Decimal>()
        .context("invalid native fill price")?;
    ensure!(ordered > Decimal::ZERO && price > Decimal::ZERO
        && previous_cumulative >= Decimal::ZERO
        && cumulative <= ordered && cumulative > previous_cumulative
        && cumulative - previous_cumulative == last,
        "native cache not aligned with the reported execution");
    let total = claims.iter().try_fold(Decimal::ZERO, |acc, x| {
        acc.checked_add(x.quantity).context("virtual order plan overflow")
    })?;
    ensure!(total == ordered, "member claims disagree with native order size");
    let fills = allocate_fifo(
        claims, previous_cumulative, cumulative, price, native_order.side, fill,
    )?;
    Ok(VirtualAttribution {
        event_id: fill.event_id.clone(),
        trade_id: fill.trade_id.clone(),
        client_order_id: fill.client_order_id.clone(),
        prior_cumulative: previous_cumulative,
        cumulative,
        fills,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> NativeFillLink {
        NativeFillLink {
            account_id: "BINANCE-UM-001".into(),
            instrument_id: "ETHUSDT-PERP.BINANCE".into(),
            client_order_id: "SA123".into(),
            venue_order_id: "456".into(),
            trade_id: "789".into(),
            event_id: "event-one".into(),
            event_ts_ns: 123,
            last_quantity: "2".into(),
            last_price: "100".into(),
            position_id: None,
            reconciliation: false,
        }
    }

    #[test]
    fn fifo_claims_attribute_partial_native_fills_without_second_order_book() {
        let claims = [
            MemberClaim { member_id: "alpha".into(), quantity: Decimal::from(2) },
            MemberClaim { member_id: "beta".into(), quantity: Decimal::from(3) },
        ];
        let first = allocate_fifo(&claims, Decimal::ZERO, Decimal::from(2),
            Decimal::from(100), OrderSide::Buy, &event()).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].member_id, "alpha");
        assert_eq!(first[0].signed_quantity, Decimal::from(2));
        let second = allocate_fifo(&claims, Decimal::from(2), Decimal::from(5),
            Decimal::from(101), OrderSide::Buy, &event()).unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].member_id, "beta");
        assert_eq!(second[0].signed_quantity, Decimal::from(3));
        assert!(allocate_fifo(&claims, Decimal::from(2), Decimal::from(2),
            Decimal::ONE, OrderSide::Buy, &event()).is_err());
    }

    #[test]
    fn invalid_claims_and_unattributable_fills_fail_closed() {
        let claims = [MemberClaim { member_id: "alpha".into(), quantity: Decimal::ONE }];
        assert!(allocate_fifo(&claims, Decimal::ZERO, Decimal::from(2),
            Decimal::ONE, OrderSide::Sell, &event()).is_err());
        let duplicates = [
            MemberClaim { member_id: "alpha".into(), quantity: Decimal::ONE },
            MemberClaim { member_id: "alpha".into(), quantity: Decimal::ONE },
        ];
        assert!(allocate_fifo(&duplicates, Decimal::ZERO, Decimal::from(2),
            Decimal::ONE, OrderSide::Buy, &event()).is_err());
    }
}

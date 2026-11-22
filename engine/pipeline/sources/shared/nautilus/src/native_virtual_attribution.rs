//! Read-only Nautilus OMS projection into SeeAlpha virtual strategy attribution.
//!
//! The native OrderCore is the sole source for real order cumulative quantity.
//! This module keeps no alternative real order book and never transmits orders.
//! A caller must persist the returned coordinator with a durable CAS before
//! releasing any virtual attribution to consumers.
use anyhow::{Result, ensure};
use nautilus_model::orders::OrderCore;
use rust_decimal::Decimal;
use seealpha_execution::coordinator::{Bar, Coordinator, VirtualFill};

use crate::{
    native_execution::{NativeFillLink, NativeOrderBinding, check_native_fill_binding},
    native_order_link::NativeOrderObservation,
};

/// One immutable projection of Nautilus facts. Not a venue fill ledger entry.
pub struct VirtualAttribution {
    pub coordinator: Coordinator,
    pub fills: Vec<VirtualFill>,
}

// Tests cover only the pure fail-closed guard without a running exchange.
// Native Core snapshot and out-of-order replay acceptance need dedicated
// pinned-0.68.0 integration tests before live use.
fn fail_closed(mut coordinator: Coordinator, reason: &str) -> VirtualAttribution {
    coordinator.freeze(reason);
    VirtualAttribution { coordinator, fills: Vec::new() }
}

/// Project a native event without ever accepting a caller-provided cumulative
/// quantity or duplicating Nautilus' mutable order state. The native OrderCore
/// must represent the state *after* this event has been applied by Nautilus.
///
/// The coordinator's in-flight identity is a SeeAlpha durable intent guard,
/// not the authoritative order lifecycle. This adapter does not make an order
/// executable or verify authenticated exchange provenance on its own.
pub fn project_native_order_observation(
    previous: &Coordinator,
    observation: &NativeOrderObservation,
    native_order: &OrderCore,
    binding: &NativeOrderBinding<'_>,
    bar: Option<Bar>,
) -> Result<VirtualAttribution> {
    let mut coordinator = previous.clone();
    ensure!(!binding.account_id.is_empty()
        && !binding.instrument_id.is_empty()
        && !binding.client_order_id.is_empty(),
        "missing pre-registered native order scope");
    let core_scope_matches = native_order.account_id.as_ref()
            .is_some_and(|a| a.to_string() == binding.account_id)
        && native_order.instrument_id.to_string() == binding.instrument_id
        && native_order.client_order_id.to_string() == binding.client_order_id
        && binding.venue_order_id.is_none_or(|expected| {
            native_order.venue_order_id.as_ref()
                .is_some_and(|actual| actual.to_string() == expected)
        });
    if !core_scope_matches {
        return Ok(fail_closed(coordinator, "Nautilus order cache identity mismatch"));
    }

    match observation {
        NativeOrderObservation::Accepted(accepted) => {
            // ACK is only an order identity fact. Never assign virtual fills.
            if accepted.account_id != binding.account_id
                || accepted.instrument_id != binding.instrument_id
                || accepted.client_order_id != binding.client_order_id
                || binding.venue_order_id.is_some_and(|id| id != accepted.venue_order_id)
            {
                return Ok(fail_closed(coordinator, "Nautilus acceptance identity mismatch"));
            }
            Ok(VirtualAttribution { coordinator, fills: Vec::new() })
        }
        NativeOrderObservation::FillVoided { client_order_id } => {
            if client_order_id != binding.client_order_id {
                return Ok(fail_closed(coordinator, "Nautilus fill void identity mismatch"));
            }
            Ok(fail_closed(coordinator, "Nautilus fill correction requires native reconciliation"))
        }
        NativeOrderObservation::Filled(fill) => {
            project_native_fill(coordinator, fill, native_order, binding, bar)
        },
    }
}

fn project_native_fill(
    mut coordinator: Coordinator,
    fill: &NativeFillLink,
    native_order: &OrderCore,
    binding: &NativeOrderBinding<'_>,
    bar: Option<Bar>,
) -> Result<VirtualAttribution> {
    if check_native_fill_binding(fill, binding).is_err() {
        return Ok(fail_closed(coordinator, "Nautilus fill binding mismatch"));
    }
    let Some(inflight) = coordinator.flight.as_ref() else {
        return Ok(fail_closed(coordinator, "no registered virtual attribution intent"));
    };
    if inflight.id != fill.client_order_id {
        return Ok(fail_closed(coordinator, "native fill is not the pending SeeAlpha intent"));
    }
    // The native Core and the registered SeeAlpha intent must describe the
    // very same order, not merely reuse the same client identifier.
    let native_quantity = match native_order.quantity.to_string().parse::<Decimal>() {
        Ok(v) => v,
        Err(_) => return Ok(fail_closed(coordinator, "invalid native order quantity")),
    };
    if native_quantity != inflight.signed_requested.abs()
        || native_order.venue_order_id.as_ref()
            .is_none_or(|id| id.to_string() != fill.venue_order_id)
        || (inflight.signed_requested > Decimal::ZERO
            && native_order.side != nautilus_model::enums::OrderSide::Buy)
        || (inflight.signed_requested < Decimal::ZERO
            && native_order.side != nautilus_model::enums::OrderSide::Sell)
    {
        return Ok(fail_closed(coordinator, "Nautilus order terms disagree with durable intent"));
    }
    let cumulative = match native_order.filled_qty.to_string().parse::<Decimal>() {
        Ok(v) => v,
        Err(_) => return Ok(fail_closed(coordinator, "invalid Nautilus filled quantity")),
    };
    let last = fill.last_quantity.parse::<Decimal>()?;
    let price = fill.last_price.parse::<Decimal>()?;
    // Nautilus may already be ahead of a delayed subscriber event. Do not
    // falsely credit the latest cumulative balance to an earlier fill. Wait
    // for independently ordered replay/reconciliation instead.
    if cumulative <= inflight.filled
        || cumulative.checked_sub(inflight.filled) != Some(last)
    {
        return Ok(fail_closed(coordinator, "Nautilus fill sequence/cache snapshot mismatch"));
    }
    match coordinator.apply_venue_fill(
        &fill.client_order_id, &fill.event_id, cumulative, price, bar,
    ) {
        Ok(fills) => Ok(VirtualAttribution { coordinator, fills }),
        Err(_) => Ok(fail_closed(coordinator, "unattributable native fill")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_mismatch_freezes_without_mutating_virtual_members() {
        let mut prior = Coordinator::new(Decimal::ONE, Decimal::from(10)).unwrap();
        prior.register("alpha").unwrap();
        let result = fail_closed(prior, "native order cache identity mismatch");
        assert!(result.coordinator.frozen);
        assert!(result.fills.is_empty());
        assert_eq!(result.coordinator.member_position("alpha"), Some(Decimal::ZERO));
        assert_eq!(
            result.coordinator.freeze_reason.as_deref(),
            Some("native order cache identity mismatch"),
        );
    }
}

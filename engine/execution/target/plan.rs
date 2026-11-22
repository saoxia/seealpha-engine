//! Pure SeeAlpha multi-strategy target netting before Nautilus execution.
//!
//! This module deliberately does not own a venue order, OMS state machine,
//! account cache or execution client: those are NautilusTrader responsibilities.
//! An adapter must obtain a current account-scoped position from Nautilus,
//! certify its freshness, and check pending orders before calling this planner.
use std::collections::BTreeSet;

use rust_decimal::Decimal;
use thiserror::Error;

use super::{MemberTarget, execution_delta, net_signed_quantity, quantize_toward_zero};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetAdjustment {
    /// Signed change requested by SeeAlpha; not an order or send permit.
    pub signed_quantity: Decimal,
    /// Consolidated strategy target (one instrument and one account only).
    pub net_target: Decimal,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum PlanningError {
    #[error("unverified Nautilus account position or unresolved venue orders")]
    Unreconciled,
    #[error("invalid target, lot increment or risk limit")]
    Invalid,
    #[error("duplicate strategy target in one account/instrument batch")]
    Duplicate,
    #[error("aggregated target exceeds configured exposure limit")]
    Exposure,
    #[error("decimal arithmetic overflow")]
    Overflow,
}

/// Determine the portfolio adjustment, not how to place or manage a venue order.
/// All targets and the authoritative position MUST share one account, product
/// and instrument; their scope is established by the upstream Nautilus adapter.
/// Empty targets mean no change, not an instruction to liquidate the account.
/// A full zero target must be published explicitly by the strategy owners.
/// Neither `position_reconciled` nor `orders_reconciled` is authentication:
/// only the native adapter may assert those conditions after checking its data.
pub fn plan_adjustment(
    targets: &[MemberTarget],
    authoritative_position: Decimal,
    increment: Decimal,
    max_abs_target: Decimal,
    position_reconciled: bool,
    orders_reconciled: bool,
) -> Result<Option<NetAdjustment>, PlanningError> {
    if !position_reconciled || !orders_reconciled {
        return Err(PlanningError::Unreconciled);
    }
    if increment <= Decimal::ZERO || max_abs_target <= Decimal::ZERO
        || !(authoritative_position % increment).is_zero()
    {
        return Err(PlanningError::Invalid);
    }
    if targets.is_empty() {
        return Ok(None);
    }
    let mut seen = BTreeSet::new();
    for target in targets {
        if target.member_id.trim().is_empty()
            || target.member_id.len() > 128
            || !(target.signed_quantity % increment).is_zero()
        {
            return Err(PlanningError::Invalid);
        }
        if !seen.insert(target.member_id.as_str()) {
            return Err(PlanningError::Duplicate);
        }
    }
    let net = net_signed_quantity(targets).map_err(|_| PlanningError::Overflow)?;
    if net.abs() > max_abs_target {
        return Err(PlanningError::Exposure);
    }
    let delta = execution_delta(net, authoritative_position).map_err(|_| PlanningError::Overflow)?;
    let sized = quantize_toward_zero(delta, increment).map_err(|_| PlanningError::Overflow)?;
    if sized.is_zero() {
        return Ok(None);
    }
    Ok(Some(NetAdjustment { signed_quantity: sized, net_target: net }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(id: &str, quantity: i64) -> MemberTarget {
        MemberTarget { member_id: id.into(), signed_quantity: Decimal::from(quantity), risk_exit: false }
    }

    #[test]
    fn nets_long_and_short_without_generating_a_nautilus_order() {
        let plan = plan_adjustment(&[t("long", 6), t("short", -2)],
            Decimal::ONE, Decimal::ONE, Decimal::from(10), true, true).unwrap().unwrap();
        assert_eq!(plan.net_target, Decimal::from(4));
        assert_eq!(plan.signed_quantity, Decimal::from(3));
    }

    #[test]
    fn refuses_unverified_or_inflight_account_state() {
        assert_eq!(plan_adjustment(&[t("alpha", 2)], Decimal::ZERO,
            Decimal::ONE, Decimal::from(10), false, true), Err(PlanningError::Unreconciled));
        assert_eq!(plan_adjustment(&[t("alpha", 2)], Decimal::ZERO,
            Decimal::ONE, Decimal::from(10), true, false), Err(PlanningError::Unreconciled));
    }

    #[test]
    fn refuses_duplicate_strategies_and_excessive_or_invalid_targets() {
        assert_eq!(plan_adjustment(&[t("alpha", 1), t("alpha", -1)], Decimal::ZERO,
            Decimal::ONE, Decimal::from(10), true, true), Err(PlanningError::Duplicate));
        assert_eq!(plan_adjustment(&[t("alpha", 12)], Decimal::ZERO,
            Decimal::ONE, Decimal::from(10), true, true), Err(PlanningError::Exposure));
        assert_eq!(plan_adjustment(&[t("alpha", 1)], Decimal::ZERO,
            Decimal::ZERO, Decimal::from(10), true, true), Err(PlanningError::Invalid));
    }

    #[test]
    fn zero_net_and_empty_batch_do_not_create_orders() {
        assert_eq!(plan_adjustment(&[], Decimal::from(2),
            Decimal::ONE, Decimal::from(10), true, true).unwrap(), None);
        assert_eq!(plan_adjustment(&[t("alpha", 2)], Decimal::from(2),
            Decimal::ONE, Decimal::from(10), true, true).unwrap(), None);
        let flatten = plan_adjustment(&[t("alpha", 0)], Decimal::from(2),
            Decimal::ONE, Decimal::from(10), true, true).unwrap().unwrap();
        assert_eq!(flatten.signed_quantity, Decimal::from(-2));
    }
}

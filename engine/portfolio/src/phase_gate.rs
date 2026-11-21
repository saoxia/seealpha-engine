//! Fail-closed, read-only admission of the *next* basket phase.
//!
//! A venue may fill any subset of a multi-leg phase. Testing only its final
//! positions is insufficient: one filled leg can temporarily remove a hedge.
//! This module never transmits orders or turns a plan into trading authority.
//! The caller must CAS-persist the phase request before any external effect.
use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;

use crate::basket::{BasketError, Checkpoint, Evidence, LegId, Product, State};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhaseGateLimits {
    pub now_ns: u64,
    pub max_age_ns: u64,
    pub max_gross_notional: Decimal,
    /// Maximum absolute Spot + USD-M quantity for each declared hedge pair.
    pub max_leg_drift: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhaseGateResult {
    /// Admission is only permission to *persist an intent*, never venue authority.
    pub admitted: bool,
    pub block_reason: Option<String>,
    pub worst_case_gross: Decimal,
    pub worst_case_leg_drift: Option<Decimal>,
    pub executable: bool,
}

fn add(a: Decimal, b: Decimal) -> Result<Decimal, BasketError> {
    a.checked_add(b).ok_or(BasketError::Overflow)
}

fn mul(a: Decimal, b: Decimal) -> Result<Decimal, BasketError> {
    a.checked_mul(b).ok_or(BasketError::Overflow)
}

fn blocked(reason: &str, gross: Decimal, drift: Option<Decimal>) -> PhaseGateResult {
    PhaseGateResult {
        admitted: false,
        block_reason: Some(reason.to_owned()),
        worst_case_gross: gross,
        worst_case_leg_drift: drift,
        executable: false,
    }
}

/// Check all possible independent partial-fill endpoints of the next phase.
///
/// Quantities must be an authenticated account snapshot at the same scope as
/// the checkpoint. Every quote must have a validated timestamp and multiplier.
/// For linear positions, the maximum gross and absolute pair drift over any
/// subset of fills occur at the endpoints (no fill / full fill) of each leg.
/// This is conservative even for phases that were intended to execute together.
///
/// Do not use the returned bool to send an order directly. A successful caller
/// must first durably CAS-register Checkpoint::begin and its bound order IDs.
/// An unknown, frozen, or in-flight phase never receives new admission.
pub fn gate_next_phase(
    checkpoint: &Checkpoint,
    positions: &BTreeMap<LegId, Decimal>,
    evidence: &BTreeMap<LegId, Evidence>,
    limits: PhaseGateLimits,
    hedge_pairs: &[(LegId, LegId)],
) -> Result<PhaseGateResult, BasketError> {
    if checkpoint.plan.executable
        || limits.now_ns == 0
        || limits.max_age_ns == 0
        || limits.max_gross_notional <= Decimal::ZERO
        || limits.max_leg_drift < Decimal::ZERO
        || checkpoint.plan.targets.is_empty()
        || positions.keys().ne(checkpoint.plan.targets.keys())
        || evidence.keys().ne(checkpoint.plan.targets.keys())
    {
        return Err(BasketError::Evidence);
    }
    let first_leg = positions.keys().next().ok_or(BasketError::Evidence)?;
    if positions
        .keys()
        .any(|leg| leg.tenant != first_leg.tenant || leg.account != first_leg.account)
    {
        return Err(BasketError::Evidence);
    }
    if checkpoint.state != State::Ready {
        let message = match checkpoint.state {
            State::Frozen => "frozen basket requires independent reconciliation",
            State::AwaitingConfirmation => "in-flight or unknown phase: no second order",
            State::Completed => "completed basket has no next phase",
            State::Ready => unreachable!(),
        };
        return Ok(blocked(message, Decimal::ZERO, None));
    }
    if checkpoint.request_id.is_some() || !checkpoint.fills.is_empty() {
        return Err(BasketError::State);
    }
    if &checkpoint.positions != positions {
        return Ok(blocked(
            "authoritative positions differ from checkpoint",
            Decimal::ZERO,
            None,
        ));
    }
    let phase = checkpoint
        .plan
        .phases
        .get(checkpoint.phase_index)
        .ok_or(BasketError::State)?;
    if phase.changes.is_empty() {
        return Err(BasketError::Evidence);
    }
    let mut after = positions.clone();
    for (leg, change) in &phase.changes {
        if change.is_zero() {
            return Err(BasketError::Evidence);
        }
        let slot = after.get_mut(leg).ok_or(BasketError::Evidence)?;
        *slot = add(*slot, *change)?;
    }

    let mut gross = Decimal::ZERO;
    let mut invalid = false;
    for (leg, before) in positions {
        let quote = &evidence[leg];
        let next = after[leg];
        if quote.price <= Decimal::ZERO
            || quote.multiplier <= Decimal::ZERO
            || quote.increment <= Decimal::ZERO
        {
            return Err(BasketError::Evidence);
        }
        if quote.timestamp_ns > limits.now_ns
            || limits.now_ns - quote.timestamp_ns > limits.max_age_ns
            || !(*before % quote.increment).is_zero()
            || !(next % quote.increment).is_zero()
            || (leg.product != Product::FuturesUm
                && (*before < Decimal::ZERO || next < Decimal::ZERO))
        {
            invalid = true;
        }
        let unit = mul(quote.price, quote.multiplier)?;
        gross = add(gross, mul(before.abs().max(next.abs()), unit)?)?;
    }

    // A caller may declare several disjoint pairs. For a single Spot/UM
    // basket the same-symbol pair is mandatory even if none is passed.
    let mut pairs = hedge_pairs.to_vec();
    if pairs.is_empty() {
        let spot = positions.keys().find(|l| l.product == Product::Spot);
        let future = positions.keys().find(|l| l.product == Product::FuturesUm);
        if let (Some(s), Some(f)) = (spot, future) {
            if positions.len() != 2 || s.symbol != f.symbol {
                // Do not silently disable pair-drift checks on a larger
                // portfolio: the caller must declare the intended hedges.
                return Err(BasketError::Risk);
            }
            pairs.push((s.clone(), f.clone()));
        }
    }
    let mut seen = BTreeSet::new();
    let mut drift = None;
    for (spot, future) in &pairs {
        if spot.product != Product::Spot
            || future.product != Product::FuturesUm
            || spot.symbol != future.symbol
            || spot.tenant != future.tenant
            || spot.account != future.account
            || !positions.contains_key(spot)
            || !positions.contains_key(future)
            || !seen.insert(spot.clone())
            || !seen.insert(future.clone())
        {
            return Err(BasketError::Risk);
        }
        let mut worst = Decimal::ZERO;
        for s in [positions[spot], after[spot]] {
            for f in [positions[future], after[future]] {
                worst = worst.max(add(s, f)?.abs());
            }
        }
        drift = Some(drift.unwrap_or(Decimal::ZERO).max(worst));
    }

    let futures: Vec<_> = positions
        .keys()
        .filter(|leg| leg.product == Product::FuturesUm)
        .collect();
    let options: Vec<_> = positions
        .keys()
        .filter(|leg| leg.product == Product::Option)
        .collect();
    let mut uncovered = false;
    if !options.is_empty() {
        if futures.len() != 1 || options.len() != 2 {
            return Err(BasketError::Risk);
        }
        let future = futures[0];
        let highest_future = positions[future].abs().max(after[future].abs());
        for option in options {
            let smallest_cover = positions[option].min(after[option]);
            if mul(smallest_cover, evidence[option].multiplier)? < highest_future {
                uncovered = true;
            }
        }
    }

    if invalid {
        return Ok(blocked(
            "stale or invalid market/position evidence",
            gross,
            drift,
        ));
    }
    if gross > limits.max_gross_notional {
        return Ok(blocked(
            "independent partial fills could breach gross budget",
            gross,
            drift,
        ));
    }
    if drift.is_some_and(|v| v > limits.max_leg_drift) {
        return Ok(blocked(
            "independent leg fills could exceed hedge drift",
            gross,
            drift,
        ));
    }
    if uncovered {
        return Ok(blocked(
            "independent leg fills could remove option protection",
            gross,
            drift,
        ));
    }
    Ok(PhaseGateResult {
        admitted: true,
        block_reason: None,
        worst_case_gross: gross,
        worst_case_leg_drift: drift,
        executable: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basket::{Phase, Plan};

    fn d(value: i64) -> Decimal {
        Decimal::from(value)
    }
    fn leg(product: Product, symbol: &str) -> LegId {
        LegId {
            tenant: "tenant".into(),
            account: "account".into(),
            product,
            symbol: symbol.into(),
        }
    }
    fn quote() -> Evidence {
        Evidence {
            price: d(100),
            increment: d(1),
            multiplier: d(1),
            timestamp_ns: 10,
        }
    }
    fn limits(drift: i64) -> PhaseGateLimits {
        PhaseGateLimits {
            now_ns: 11,
            max_age_ns: 5,
            max_gross_notional: d(500),
            max_leg_drift: d(drift),
        }
    }
    #[test]
    fn one_leg_fill_can_break_hedge_even_when_final_target_is_neutral() {
        let spot = leg(Product::Spot, "ETHUSDT");
        let future = leg(Product::FuturesUm, "ETHUSDT");
        let start = [(spot.clone(), d(0)), (future.clone(), d(0))].into();
        let targets = [(spot.clone(), d(1)), (future.clone(), d(-1))].into();
        let phase = Phase {
            name: "two_venue_orders".into(),
            changes: [(spot.clone(), d(1)), (future.clone(), d(-1))].into(),
        };
        let cp = Checkpoint::new(
            Plan {
                targets,
                phases: vec![phase],
                gross_notional: d(200),
                executable: false,
            },
            start,
        )
        .unwrap();
        let quotes = [(spot.clone(), quote()), (future.clone(), quote())].into();
        let denied = gate_next_phase(&cp, &cp.positions, &quotes, limits(0), &[]).unwrap();
        assert!(!denied.admitted);
        assert_eq!(denied.worst_case_leg_drift, Some(d(1)));
        assert!(!denied.executable);
        assert!(
            gate_next_phase(&cp, &cp.positions, &quotes, limits(1), &[])
                .unwrap()
                .admitted
        );
        let mut pending = cp;
        pending.begin("req1").unwrap();
        assert!(
            !gate_next_phase(&pending, &pending.positions, &quotes, limits(1), &[])
                .unwrap()
                .admitted
        );
    }

    #[test]
    fn option_protection_must_precede_independent_futures_fill() {
        let future = leg(Product::FuturesUm, "ETHUSDT");
        let call = leg(Product::Option, "ETH-261225-3000-C");
        let put = leg(Product::Option, "ETH-261225-3000-P");
        let start = [
            (future.clone(), d(0)),
            (call.clone(), d(0)),
            (put.clone(), d(0)),
        ]
        .into();
        let targets = [
            (future.clone(), d(-1)),
            (call.clone(), d(1)),
            (put.clone(), d(1)),
        ]
        .into();
        let all_at_once = Phase {
            name: "unsafe_parallel".into(),
            changes: [
                (future.clone(), d(-1)),
                (call.clone(), d(1)),
                (put.clone(), d(1)),
            ]
            .into(),
        };
        let cp = Checkpoint::new(
            Plan {
                targets,
                phases: vec![all_at_once],
                gross_notional: d(300),
                executable: false,
            },
            start,
        )
        .unwrap();
        let quotes = [
            (future.clone(), quote()),
            (call.clone(), quote()),
            (put.clone(), quote()),
        ]
        .into();
        let denied = gate_next_phase(&cp, &cp.positions, &quotes, limits(1), &[]).unwrap();
        assert!(!denied.admitted);
        assert!(denied.block_reason.unwrap().contains("option protection"));
        // Acquiring protection on its own is safe; direction may grow after
        // authoritative confirmation of this initial phase.
        let stages = vec![
            Phase {
                name: "protection_first".into(),
                changes: [(call.clone(), d(1)), (put.clone(), d(1))].into(),
            },
            Phase {
                name: "futures_after_confirmation".into(),
                changes: [(future.clone(), d(-1))].into(),
            },
        ];
        let safe = Checkpoint::new(
            Plan {
                targets: cp.plan.targets.clone(),
                phases: stages,
                gross_notional: d(300),
                executable: false,
            },
            cp.positions.clone(),
        )
        .unwrap();
        assert!(
            gate_next_phase(&safe, &safe.positions, &quotes, limits(1), &[])
                .unwrap()
                .admitted
        );
    }

    #[test]
    fn stale_quote_or_wrong_position_does_not_admit() {
        let spot = leg(Product::Spot, "ETHUSDT");
        let starting = [(spot.clone(), d(0))].into();
        let cp = Checkpoint::new(
            Plan {
                targets: [(spot.clone(), d(1))].into(),
                phases: vec![Phase {
                    name: "buy".into(),
                    changes: [(spot.clone(), d(1))].into(),
                }],
                gross_notional: d(100),
                executable: false,
            },
            starting,
        )
        .unwrap();
        let old = [(
            spot.clone(),
            Evidence {
                timestamp_ns: 1,
                ..quote()
            },
        )]
        .into();
        assert!(
            !gate_next_phase(&cp, &cp.positions, &old, limits(0), &[])
                .unwrap()
                .admitted
        );
        assert!(
            !gate_next_phase(
                &cp,
                &[(spot.clone(), d(1))].into(),
                &[(spot, quote())].into(),
                limits(0),
                &[]
            )
            .unwrap()
            .admitted
        );
    }
}

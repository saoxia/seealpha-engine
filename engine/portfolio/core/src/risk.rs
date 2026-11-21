//! Risk observation never grants order authority. Any discrepancy freezes the journal.
use crate::basket::{BasketError, Checkpoint, Evidence, LegId, Product, State};
use rust_decimal::Decimal;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskObservation {
    pub frozen: bool,
    pub gross_notional: Decimal,
    pub leg_drift: Option<Decimal>,
    pub reason: Option<String>,
    pub checkpoint: Checkpoint,
    pub executable: bool,
}
#[derive(Debug, Clone, Copy)]
pub struct RiskLimits {
    pub now_ns: u64,
    pub max_age_ns: u64,
    pub max_pending_ns: u64,
    pub max_gross_notional: Decimal,
    pub max_leg_drift: Decimal,
}
pub fn observe_phase(
    mut checkpoint: Checkpoint,
    positions: &BTreeMap<LegId, Decimal>,
    evidence: &BTreeMap<LegId, Evidence>,
    limits: RiskLimits,
    pending_since_ns: Option<u64>,
) -> Result<RiskObservation, BasketError> {
    if checkpoint.plan.executable
        || limits.now_ns == 0
        || limits.max_age_ns == 0
        || limits.max_pending_ns == 0
        || limits.max_gross_notional <= Decimal::ZERO
        || limits.max_leg_drift < Decimal::ZERO
        || positions.keys().ne(checkpoint.plan.targets.keys())
        || evidence.keys().ne(checkpoint.plan.targets.keys())
    {
        return Err(BasketError::Evidence);
    }
    let mut reason = None;
    let mut gross = Decimal::ZERO;
    for (leg, q) in positions {
        let quote = &evidence[leg];
        if quote.price <= Decimal::ZERO
            || quote.multiplier <= Decimal::ZERO
            || quote.increment <= Decimal::ZERO
        {
            return Err(BasketError::Evidence);
        }
        if quote.timestamp_ns > limits.now_ns
            || limits.now_ns - quote.timestamp_ns > limits.max_age_ns
        {
            reason = Some("stale or future market evidence");
        }
        if !(*q % quote.increment).is_zero() {
            reason = Some("position violates quantity increment");
        }
        if leg.product != Product::FuturesUm && *q < Decimal::ZERO {
            reason = Some("unsupported short cash or naked option exposure");
        }
        let notional = q
            .abs()
            .checked_mul(quote.price)
            .and_then(|v| v.checked_mul(quote.multiplier))
            .ok_or(BasketError::Overflow)?;
        gross = gross.checked_add(notional).ok_or(BasketError::Overflow)?;
    }
    match checkpoint.state {
        State::Frozen => reason = Some("basket already frozen"),
        State::AwaitingConfirmation => {
            if !pending_since_ns.is_some_and(|t| {
                t > 0 && t <= limits.now_ns && limits.now_ns - t <= limits.max_pending_ns
            }) {
                reason = Some("unverified or expired in-flight phase");
            }
            let mut expected = checkpoint.positions.clone();
            for (leg, cumulative) in &checkpoint.fills {
                let slot = expected.get_mut(leg).ok_or(BasketError::Evidence)?;
                *slot = slot.checked_add(*cumulative).ok_or(BasketError::Overflow)?;
            }
            if &expected != positions {
                reason = Some("authoritative positions disagree with fills");
            }
        }
        State::Ready | State::Completed => {
            if &checkpoint.positions != positions {
                reason = Some("authoritative positions disagree with checkpoint");
            }
        }
    }
    let perp: Vec<_> = positions
        .keys()
        .filter(|l| l.product == Product::FuturesUm)
        .collect();
    let spots: Vec<_> = positions
        .keys()
        .filter(|l| l.product == Product::Spot)
        .collect();
    let options: Vec<_> = positions
        .keys()
        .filter(|l| l.product == Product::Option)
        .collect();
    if perp.len() != 1 {
        return Err(BasketError::Risk);
    }
    let future = perp[0];
    let drift = if spots.len() == 1 && options.is_empty() && positions.len() == 2 {
        let drift = positions[spots[0]]
            .checked_add(positions[future])
            .ok_or(BasketError::Overflow)?
            .abs();
        if drift > limits.max_leg_drift {
            reason = Some("spot/perpetual hedge drift");
        }
        Some(drift)
    } else if options.len() == 2 && spots.is_empty() && positions.len() == 3 {
        for option in options {
            let coverage = positions[option]
                .checked_mul(evidence[option].multiplier)
                .ok_or(BasketError::Overflow)?;
            if coverage < positions[future].abs() {
                reason = Some("option protection does not cover futures exposure");
            }
        }
        None
    } else {
        return Err(BasketError::Risk);
    };
    if gross > limits.max_gross_notional {
        reason = Some("gross notional budget exceeded");
    }
    if let Some(message) = reason {
        if checkpoint.state == State::Completed {
            return Err(BasketError::Risk);
        }
        if checkpoint.state != State::Frozen {
            checkpoint.freeze(message)?;
        }
        return Ok(RiskObservation {
            frozen: true,
            gross_notional: gross,
            leg_drift: drift,
            reason: checkpoint.reason.clone(),
            checkpoint,
            executable: false,
        });
    }
    Ok(RiskObservation {
        frozen: false,
        gross_notional: gross,
        leg_drift: drift,
        reason: None,
        checkpoint,
        executable: false,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::basket::{Phase, Plan, State};
    fn d(n: i64) -> Decimal {
        Decimal::from(n)
    }
    #[test]
    fn drift_freezes_and_recovery_does_not_autoresume() {
        let spot = LegId {
            tenant: "t".into(),
            account: "a".into(),
            product: Product::Spot,
            symbol: "ETHUSDT".into(),
        };
        let perp = LegId {
            product: Product::FuturesUm,
            ..spot.clone()
        };
        let positions: BTreeMap<LegId, Decimal> =
            [(spot.clone(), d(1)), (perp.clone(), d(-1))].into();
        let plan = Plan {
            targets: positions.clone(),
            phases: Vec::<Phase>::new(),
            gross_notional: d(200),
            executable: false,
        };
        let cp = Checkpoint::new(plan, positions).unwrap();
        assert_eq!(cp.state, State::Completed);
        let actual = [(spot.clone(), d(1)), (perp.clone(), d(0))].into();
        let quotes = [
            (
                spot.clone(),
                Evidence {
                    price: d(100),
                    multiplier: d(1),
                    increment: d(1),
                    timestamp_ns: 10,
                },
            ),
            (
                perp.clone(),
                Evidence {
                    price: d(100),
                    multiplier: d(1),
                    increment: d(1),
                    timestamp_ns: 10,
                },
            ),
        ]
        .into();
        assert_eq!(
            observe_phase(
                cp,
                &actual,
                &quotes,
                RiskLimits {
                    now_ns: 11,
                    max_age_ns: 10,
                    max_pending_ns: 10,
                    max_gross_notional: d(300),
                    max_leg_drift: d(0)
                },
                None
            )
            .unwrap_err(),
            BasketError::Risk
        );
    }
}

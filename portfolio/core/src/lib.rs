//! Decimal target netting for paper holdings.
//!
//! Strategies publish absolute signed targets. This crate nets them, clips the
//! aggregate to a risk limit, and sizes the remaining delta toward zero so a
//! paper fill cannot overshoot the target. It does not submit exchange orders.
pub mod basket;
pub mod coordinator;
pub mod coordinator_snapshot;
pub mod history;
pub mod journal;
pub mod order_evidence;
pub mod phase_gate;
pub mod risk;
pub mod settlement;

use rust_decimal::Decimal;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PortfolioError {
    #[error("数量步长或风险额度必须为正")]
    Increment,
    #[error("decimal overflow")]
    Overflow,
}

/// A strategy's absolute holding request. Exactly one of quantity or weight
/// must be set. A weight is relative to a positive allocation, not equity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetHolding {
    pub symbol: String,
    pub quantity: Option<Decimal>,
    pub weight: Option<Decimal>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TargetHoldingError {
    #[error("目标持仓需要数量或权重其中之一")]
    Selection,
    #[error("目标权重必须在 -1 到 1 之间")]
    Weight,
    #[error("分配数量必须大于零")]
    Allocation,
    #[error("decimal overflow")]
    Overflow,
}

impl TargetHolding {
    pub fn new(
        symbol: impl Into<String>,
        quantity: Option<Decimal>,
        weight: Option<Decimal>,
    ) -> Result<Self, TargetHoldingError> {
        if quantity.is_some() == weight.is_some() {
            return Err(TargetHoldingError::Selection);
        }
        if weight.is_some_and(|value| value < -Decimal::ONE || value > Decimal::ONE) {
            return Err(TargetHoldingError::Weight);
        }
        Ok(Self {
            symbol: symbol.into(),
            quantity,
            weight,
        })
    }

    /// Validate allocation even for an absolute-quantity target to match the
    /// legacy contract. Decimal cannot represent NaN or infinity.
    pub fn signed_quantity(&self, allocation: Decimal) -> Result<Decimal, TargetHoldingError> {
        if allocation <= Decimal::ZERO {
            return Err(TargetHoldingError::Allocation);
        }
        match (self.quantity, self.weight) {
            (Some(quantity), None) => Ok(quantity),
            (None, Some(weight)) if weight >= -Decimal::ONE && weight <= Decimal::ONE => weight
                .checked_mul(allocation)
                .ok_or(TargetHoldingError::Overflow),
            (None, Some(_)) => Err(TargetHoldingError::Weight),
            _ => Err(TargetHoldingError::Selection),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberTarget {
    pub member_id: String,
    pub signed_quantity: Decimal,
    pub risk_exit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperFill {
    pub side: String,
    pub quantity: Decimal,
    pub price: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetPublication {
    pub member_id: String,
    pub quantity: Decimal,
    pub price: Decimal,
}

pub fn net_signed_quantity(targets: &[MemberTarget]) -> Result<Decimal, PortfolioError> {
    let mut total = Decimal::ZERO;
    for target in targets {
        total = total
            .checked_add(target.signed_quantity)
            .ok_or(PortfolioError::Overflow)?;
    }
    Ok(total)
}

pub fn execution_delta(
    net_target: Decimal,
    account_position: Decimal,
) -> Result<Decimal, PortfolioError> {
    net_target
        .checked_sub(account_position)
        .ok_or(PortfolioError::Overflow)
}

pub fn quantize_toward_zero(
    quantity: Decimal,
    increment: Decimal,
) -> Result<Decimal, PortfolioError> {
    if increment <= Decimal::ZERO {
        return Err(PortfolioError::Increment);
    }
    if quantity.is_zero() {
        return Ok(Decimal::ZERO);
    }
    let steps = quantity
        .abs()
        .checked_div(increment)
        .ok_or(PortfolioError::Overflow)?
        .trunc();
    let sized = steps
        .checked_mul(increment)
        .ok_or(PortfolioError::Overflow)?;
    if quantity.is_sign_negative() {
        Ok(-sized)
    } else {
        Ok(sized)
    }
}

pub fn clip_member_targets(
    targets: &[MemberTarget],
    risk_limit: Decimal,
) -> Result<Vec<MemberTarget>, PortfolioError> {
    if risk_limit <= Decimal::ZERO {
        return Err(PortfolioError::Increment);
    }
    let net = net_signed_quantity(targets)?;
    if net.is_zero() || net.abs() <= risk_limit {
        return Ok(targets.to_vec());
    }
    let scale = risk_limit
        .checked_div(net.abs())
        .ok_or(PortfolioError::Overflow)?;
    let mut clipped = Vec::with_capacity(targets.len());
    for target in targets {
        clipped.push(MemberTarget {
            member_id: target.member_id.clone(),
            signed_quantity: target
                .signed_quantity
                .checked_mul(scale)
                .ok_or(PortfolioError::Overflow)?,
            risk_exit: target.risk_exit,
        });
    }
    Ok(clipped)
}

/// A weighted or absolute holding publication, resolved into an account
/// quantity before the existing paper netting / risk / sizing pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoldingPublication {
    pub member_id: String,
    pub holding: TargetHolding,
    pub allocation: Decimal,
    pub price: Decimal,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HoldingReplayError {
    #[error("mixed instruments cannot be netted into one position")]
    MixedInstruments,
    #[error(transparent)]
    Target(#[from] TargetHoldingError),
    #[error(transparent)]
    Portfolio(#[from] PortfolioError),
}

/// Resolve every holding before replaying any fills. Reject mixed symbols and
/// invalid weight/allocation atomically, rather than producing a partial trace.
pub fn replay_holdings(
    publications: &[HoldingPublication],
    position: Decimal,
    risk_limit: Decimal,
    increment: Decimal,
) -> Result<(Vec<PaperFill>, Decimal), HoldingReplayError> {
    let mut resolved = Vec::with_capacity(publications.len());
    let mut symbol: Option<&str> = None;
    for publication in publications {
        match symbol {
            Some(first) if first != publication.holding.symbol => {
                return Err(HoldingReplayError::MixedInstruments);
            }
            None => symbol = Some(&publication.holding.symbol),
            _ => {}
        }
        resolved.push(TargetPublication {
            member_id: publication.member_id.clone(),
            quantity: publication
                .holding
                .signed_quantity(publication.allocation)?,
            price: publication.price,
        });
    }
    replay_targets(&resolved, position, risk_limit, increment).map_err(Into::into)
}

/// Walk absolute target publications in time order and emit paper deltas.
/// The ending position is the clipped net target after the last publication.
pub fn replay_targets(
    publications: &[TargetPublication],
    mut position: Decimal,
    risk_limit: Decimal,
    increment: Decimal,
) -> Result<(Vec<PaperFill>, Decimal), PortfolioError> {
    let mut latest: Vec<(String, Decimal)> = Vec::new();
    let mut fills = Vec::new();
    for publication in publications {
        if let Some(slot) = latest
            .iter_mut()
            .find(|(member, _)| member == &publication.member_id)
        {
            slot.1 = publication.quantity;
        } else {
            latest.push((publication.member_id.clone(), publication.quantity));
        }
        let members = latest
            .iter()
            .map(|(member, quantity)| MemberTarget {
                member_id: member.clone(),
                signed_quantity: *quantity,
                risk_exit: false,
            })
            .collect::<Vec<_>>();
        let clipped = clip_member_targets(&members, risk_limit)?;
        let net = net_signed_quantity(&clipped)?;
        let delta = quantize_toward_zero(execution_delta(net, position)?, increment)?;
        if delta.is_zero() {
            continue;
        }
        fills.push(PaperFill {
            side: if delta.is_sign_positive() {
                "buy".to_string()
            } else {
                "sell".to_string()
            },
            quantity: delta.abs(),
            price: publication.price,
        });
        position = position
            .checked_add(delta)
            .ok_or(PortfolioError::Overflow)?;
    }
    Ok((fills, position))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn member(id: &str, quantity: i64) -> MemberTarget {
        MemberTarget {
            member_id: id.to_string(),
            signed_quantity: Decimal::from(quantity),
            risk_exit: false,
        }
    }

    #[test]
    fn target_holding_rejects_missing_or_ambiguous_values() {
        assert_eq!(
            TargetHolding::new("ETHUSDT", None, None).unwrap_err(),
            TargetHoldingError::Selection
        );
        assert_eq!(
            TargetHolding::new("ETHUSDT", Some(Decimal::ONE), Some(Decimal::ONE)).unwrap_err(),
            TargetHoldingError::Selection
        );
        assert_eq!(
            TargetHolding::new("ETHUSDT", None, Some(Decimal::new(1001, 3))).unwrap_err(),
            TargetHoldingError::Weight
        );
        assert_eq!(
            TargetHolding::new("ETHUSDT", None, Some(Decimal::new(-1001, 3))).unwrap_err(),
            TargetHoldingError::Weight
        );
    }

    #[test]
    fn target_holding_matches_legacy_weight_and_quantity_semantics() {
        let weight = TargetHolding::new("ETHUSDT", None, Some(Decimal::new(-25, 2))).unwrap();
        assert_eq!(
            weight.signed_quantity(Decimal::from(8)).unwrap(),
            Decimal::from(-2)
        );
        assert_eq!(
            weight.signed_quantity(Decimal::ZERO).unwrap_err(),
            TargetHoldingError::Allocation
        );
        let absolute = TargetHolding::new("ETHUSDT", Some(Decimal::new(125, 2)), None).unwrap();
        assert_eq!(
            absolute.signed_quantity(Decimal::from(8)).unwrap(),
            Decimal::new(125, 2)
        );
        assert_eq!(
            absolute.signed_quantity(Decimal::ZERO).unwrap_err(),
            TargetHoldingError::Allocation
        );
        let flat = TargetHolding::new("ETHUSDT", None, Some(Decimal::ZERO)).unwrap();
        assert_eq!(flat.signed_quantity(Decimal::ONE).unwrap(), Decimal::ZERO);
    }

    #[test]
    fn replay_holdings_nets_weights_before_paper_execution() {
        let publications = vec![
            HoldingPublication {
                member_id: "long".into(),
                holding: TargetHolding::new("ETHUSDT", None, Some(Decimal::new(75, 2))).unwrap(),
                allocation: Decimal::from(4),
                price: Decimal::from(100),
            },
            HoldingPublication {
                member_id: "short".into(),
                holding: TargetHolding::new("ETHUSDT", None, Some(Decimal::new(-25, 2))).unwrap(),
                allocation: Decimal::from(4),
                price: Decimal::from(101),
            },
        ];
        let (fills, position) = replay_holdings(
            &publications,
            Decimal::ZERO,
            Decimal::from(10),
            Decimal::ONE,
        )
        .unwrap();
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].quantity, Decimal::from(3));
        assert_eq!(fills[1].side, "sell");
        assert_eq!(fills[1].quantity, Decimal::ONE);
        assert_eq!(position, Decimal::from(2));
    }

    #[test]
    fn replay_holdings_rejects_mixed_symbols_and_bad_allocation() {
        let first = HoldingPublication {
            member_id: "a".into(),
            holding: TargetHolding::new("ETHUSDT", Some(Decimal::ONE), None).unwrap(),
            allocation: Decimal::ONE,
            price: Decimal::from(100),
        };
        let mut second = first.clone();
        second.member_id = "b".into();
        second.holding.symbol = "BTCUSDT".into();
        assert_eq!(
            replay_holdings(
                &[first.clone(), second],
                Decimal::ZERO,
                Decimal::from(10),
                Decimal::ONE
            )
            .unwrap_err(),
            HoldingReplayError::MixedInstruments
        );
        let mut bad = first;
        bad.allocation = Decimal::ZERO;
        assert_eq!(
            replay_holdings(&[bad], Decimal::ZERO, Decimal::from(10), Decimal::ONE).unwrap_err(),
            HoldingReplayError::Target(TargetHoldingError::Allocation)
        );
    }

    #[test]
    fn nets_opposing_targets_before_any_order() {
        let targets = vec![member("a", 3), member("b", -1)];
        assert_eq!(net_signed_quantity(&targets).unwrap(), Decimal::from(2));
    }

    #[test]
    fn quantize_never_crosses_zero_or_overshoots() {
        let increment = Decimal::ONE;
        assert_eq!(
            quantize_toward_zero(Decimal::new(-2505, 3), increment).unwrap(),
            Decimal::new(-2, 0)
        );
        assert_eq!(
            quantize_toward_zero(Decimal::new(5, 4), increment).unwrap(),
            Decimal::ZERO
        );
    }

    #[test]
    fn clip_scales_every_member_when_the_net_exceeds_the_limit() {
        let clipped =
            clip_member_targets(&[member("a", 3), member("b", 1)], Decimal::from(2)).unwrap();
        assert_eq!(net_signed_quantity(&clipped).unwrap(), Decimal::from(2));
    }

    #[test]
    fn replay_buys_then_flattens_to_the_latest_target() {
        let publications = vec![
            TargetPublication {
                member_id: "ema".into(),
                quantity: Decimal::ONE,
                price: Decimal::from(100),
            },
            TargetPublication {
                member_id: "ema".into(),
                quantity: Decimal::ZERO,
                price: Decimal::from(90),
            },
        ];
        let (fills, position) = replay_targets(
            &publications,
            Decimal::ZERO,
            Decimal::from(10),
            Decimal::new(1, 8),
        )
        .unwrap();
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].side, "buy");
        assert_eq!(fills[1].side, "sell");
        assert_eq!(position, Decimal::ZERO);
    }
}

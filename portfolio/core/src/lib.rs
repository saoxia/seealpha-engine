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

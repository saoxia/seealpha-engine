//! Strategy target holdings, netting and sizing; no paper execution.
pub mod plan;

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


}

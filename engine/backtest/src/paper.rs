//! Backtest and paper-session holdings replay. Never used for live venue fills.
use rust_decimal::Decimal;
use thiserror::Error;
use seealpha_execution::{
    MemberTarget, PortfolioError, TargetHolding, TargetHoldingError,
    clip_member_targets, execution_delta, net_signed_quantity, quantize_toward_zero,
};

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
    #[error("bar indices must be nondecreasing")]
    BarOrder,
    #[error("same-bar publications must share a positive close price")]
    BarPrice,
    #[error(transparent)]
    Target(#[from] TargetHoldingError),
    #[error(transparent)]
    Portfolio(#[from] PortfolioError),
}

/// One target publication with its source bar index. All publications from
/// the same bar are netted before a paper trade can occur.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedHoldingPublication {
    pub bar_index: usize,
    pub publication: HoldingPublication,
}

/// Replay bar-synchronous strategy targets without artificial same-bar churn.
///
/// Validate the entire batch before creating fills. Input must be in
/// nondecreasing bar order, with exactly the same close for a shared bar.
/// Within a bar, the last target from each member wins.
pub fn replay_holdings_by_bar(
    publications: &[IndexedHoldingPublication],
    position: Decimal,
    risk_limit: Decimal,
    increment: Decimal,
) -> Result<(Vec<PaperFill>, Decimal), HoldingReplayError> {
    use std::collections::BTreeMap;

    let mut symbol: Option<&str> = None;
    let mut previous_bar: Option<usize> = None;
    let mut batch_price: Option<Decimal> = None;
    let mut members = BTreeMap::<String, Decimal>::new();
    let mut net_publications = Vec::new();

    let flush = |members: &BTreeMap<String, Decimal>,
                 price: Decimal,
                 output: &mut Vec<TargetPublication>|
     -> Result<(), HoldingReplayError> {
        let mut net = Decimal::ZERO;
        for quantity in members.values() {
            net = net.checked_add(*quantity).ok_or(PortfolioError::Overflow)?;
        }
        output.push(TargetPublication {
            member_id: "__bar_aggregate__".into(),
            quantity: net,
            price,
        });
        Ok(())
    };

    for item in publications {
        let p = &item.publication;
        match symbol {
            Some(first) if first != p.holding.symbol => {
                return Err(HoldingReplayError::MixedInstruments);
            }
            None => symbol = Some(&p.holding.symbol),
            _ => {}
        }
        if previous_bar.is_some_and(|prior| item.bar_index < prior) {
            return Err(HoldingReplayError::BarOrder);
        }
        if p.price <= Decimal::ZERO {
            return Err(HoldingReplayError::BarPrice);
        }
        if previous_bar == Some(item.bar_index) && batch_price != Some(p.price) {
            return Err(HoldingReplayError::BarPrice);
        }
        // Resolve even if the member will be overwritten later in the bar:
        // an invalid earlier publication must not be silently accepted.
        let quantity = p.holding.signed_quantity(p.allocation)?;
        if previous_bar.is_some_and(|prior| prior != item.bar_index) {
            flush(&members, batch_price.expect("nonempty previous bar"), &mut net_publications)?;
        }
        previous_bar = Some(item.bar_index);
        batch_price = Some(p.price);
        members.insert(p.member_id.clone(), quantity);
    }
    if let Some(price) = batch_price {
        flush(&members, price, &mut net_publications)?;
    }
    replay_targets(&net_publications, position, risk_limit, increment).map_err(Into::into)
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

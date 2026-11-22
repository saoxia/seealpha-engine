//! Historical Spot/UM bar alignment and observed funding event merge for backtests.
//! Rates are observations, NOT credited or debited cash flows.
use thiserror::Error;
use rust_decimal::Decimal;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum CarryHistoryError {
    #[error("invalid or incomplete historical carry evidence")]
    Evidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CarryBar {
    pub ts_ns: u64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingObservation {
    pub ts_ns: u64,
    pub rate: Decimal,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CarryEvent {
    Bars {
        ts_ns: u64,
        spot: CarryBar,
        perpetual: CarryBar,
    },
    Funding(FundingObservation),
}
pub fn align_bars(
    spot: &[CarryBar],
    perpetual: &[CarryBar],
    step_ns: u64,
    start_ns: Option<u64>,
    end_ns: Option<u64>,
) -> Result<Vec<CarryEvent>, CarryHistoryError> {
    if !matches!(step_ns, 60_000_000_000 | 300_000_000_000)
        || spot.is_empty()
        || spot.len() != perpetual.len()
    {
        return Err(CarryHistoryError::Evidence);
    }
    let mut result = Vec::with_capacity(spot.len());
    let mut prev = None::<u64>;
    for (s, p) in spot.iter().zip(perpetual) {
        if s.ts_ns != p.ts_ns
            || s.ts_ns == 0
            || prev.is_some_and(|t| s.ts_ns.checked_sub(t) != Some(step_ns))
            || (prev.is_none() && start_ns.is_some_and(|t| t.checked_add(step_ns) != Some(s.ts_ns)))
        {
            return Err(CarryHistoryError::Evidence);
        }
        for b in [s, p] {
            if b.low <= Decimal::ZERO
                || b.high < b.low
                || b.open < b.low
                || b.open > b.high
                || b.close < b.low
                || b.close > b.high
            {
                return Err(CarryHistoryError::Evidence);
            }
        }
        prev = Some(s.ts_ns);
        result.push(CarryEvent::Bars {
            ts_ns: s.ts_ns,
            spot: s.clone(),
            perpetual: p.clone(),
        });
    }
    if end_ns.is_some_and(|t| t.checked_sub(step_ns) != prev) {
        return Err(CarryHistoryError::Evidence);
    }
    Ok(result)
}
pub fn merge_funding(
    bars: Vec<CarryEvent>,
    funding: &[FundingObservation],
    start_ns: u64,
    end_ns: u64,
    require_funding: bool,
) -> Result<Vec<CarryEvent>, CarryHistoryError> {
    if start_ns >= end_ns || funding.len() > 10_000 || (require_funding && funding.is_empty()) {
        return Err(CarryHistoryError::Evidence);
    }
    let mut previous = None::<u64>;
    for f in funding {
        if f.ts_ns < start_ns || f.ts_ns >= end_ns || previous.is_some_and(|x| x >= f.ts_ns) {
            return Err(CarryHistoryError::Evidence);
        }
        previous = Some(f.ts_ns);
    }
    let mut result = Vec::with_capacity(bars.len() + funding.len());
    let mut cursor = 0;
    for row in bars {
        let ts = match &row {
            CarryEvent::Bars { ts_ns, .. } => *ts_ns,
            CarryEvent::Funding(_) => return Err(CarryHistoryError::Evidence),
        };
        while cursor < funding.len() && funding[cursor].ts_ns < ts {
            result.push(CarryEvent::Funding(funding[cursor].clone()));
            cursor += 1;
        }
        result.push(row);
        while cursor < funding.len() && funding[cursor].ts_ns == ts {
            result.push(CarryEvent::Funding(funding[cursor].clone()));
            cursor += 1;
        }
    }
    for item in funding.iter().skip(cursor) {
        result.push(CarryEvent::Funding(item.clone()));
    }
    Ok(result)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn gap_is_not_forward_filled() {
        let row = CarryBar {
            ts_ns: 60_000_000_000,
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
        };
        let later = CarryBar {
            ts_ns: 180_000_000_000,
            ..row.clone()
        };
        assert_eq!(
            align_bars(
                &[row.clone(), later.clone()],
                &[row, later],
                60_000_000_000,
                Some(0),
                None
            ),
            Err(CarryHistoryError::Evidence)
        );
    }

    #[test]
    fn funding_observations_are_merged_without_creating_cash_flows() {
        let bar = CarryBar {
            ts_ns: 60_000_000_000,
            open: Decimal::ONE,
            high: Decimal::ONE,
            low: Decimal::ONE,
            close: Decimal::ONE,
        };
        let bars = align_bars(&[bar.clone()], &[bar], 60_000_000_000, Some(0), None).unwrap();
        let events = merge_funding(
            bars,
            &[FundingObservation {
                ts_ns: 60_000_000_000,
                rate: Decimal::new(1, 4),
            }],
            0,
            120_000_000_000,
            true,
        )
        .unwrap();
        assert!(matches!(events[0], CarryEvent::Bars { .. }));
        assert!(matches!(events[1], CarryEvent::Funding(_)));
    }
}

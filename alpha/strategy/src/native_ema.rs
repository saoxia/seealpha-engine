//! Rust-native EMA crossover signal evaluation for deterministic paper research.
//!
//! This intentionally does not replace arbitrary Python strategy execution.
//! The sample currently uses the Python host; cutover requires indicator parity.
use rust_decimal::Decimal;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EmaSignalError {
    #[error("EMA periods must satisfy 2 <= fast < slow <= 500")]
    Periods,
    #[error("EMA close must be finite")]
    NonFiniteClose,
    #[error("EMA allocation must be positive")]
    Allocation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmaTarget {
    pub bar_index: usize,
    pub quantity: Decimal,
}

/// Streaming indicator and target generator. A cross on the first initialized
/// bar is not a signal; subsequent strict crosses publish only on transitions.
#[derive(Debug, Clone)]
pub struct EmaCrossover {
    fast_period: usize,
    slow_period: usize,
    fast: Option<f64>,
    slow: Option<f64>,
    previous: Option<(f64, f64)>,
    seen: usize,
    allocation: Decimal,
}

impl EmaCrossover {
    pub fn new(
        fast_period: usize,
        slow_period: usize,
        allocation: Decimal,
    ) -> Result<Self, EmaSignalError> {
        if !(2..=500).contains(&fast_period)
            || !(2..=500).contains(&slow_period)
            || fast_period >= slow_period
        {
            return Err(EmaSignalError::Periods);
        }
        if allocation <= Decimal::ZERO {
            return Err(EmaSignalError::Allocation);
        }
        Ok(Self {
            fast_period,
            slow_period,
            fast: None,
            slow: None,
            previous: None,
            seen: 0,
            allocation,
        })
    }

    /// Feed one close, preserving its index even when no target is published.
    /// Invalid closes fail without advancing the indicator or bar index.
    pub fn on_close(&mut self, close: f64) -> Result<Option<EmaTarget>, EmaSignalError> {
        if !close.is_finite() {
            return Err(EmaSignalError::NonFiniteClose);
        }
        let index = self.seen;
        self.seen += 1;
        let update = |previous: Option<f64>, period: usize| -> f64 {
            match previous {
                None => close,
                Some(value) => value + (2.0 / (period as f64 + 1.0)) * (close - value),
            }
        };
        let fast = update(self.fast, self.fast_period);
        let slow = update(self.slow, self.slow_period);
        self.fast = Some(fast);
        self.slow = Some(slow);
        if self.seen < self.slow_period {
            return Ok(None);
        }
        let previous = self.previous.replace((fast, slow));
        let Some((previous_fast, previous_slow)) = previous else {
            return Ok(None);
        };
        let quantity = if previous_fast <= previous_slow && fast > slow {
            Some(self.allocation)
        } else if previous_fast >= previous_slow && fast < slow {
            Some(Decimal::ZERO)
        } else {
            None
        };
        Ok(quantity.map(|quantity| EmaTarget {
            bar_index: index,
            quantity,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_periods_and_nonfinite_bars_without_advancing() {
        assert_eq!(
            EmaCrossover::new(21, 8, Decimal::ONE).unwrap_err(),
            EmaSignalError::Periods
        );
        let mut evaluator = EmaCrossover::new(2, 3, Decimal::ONE).unwrap();
        assert_eq!(
            evaluator.on_close(f64::NAN).unwrap_err(),
            EmaSignalError::NonFiniteClose
        );
        assert!(evaluator.on_close(1.0).unwrap().is_none());
        assert!(evaluator.on_close(1.0).unwrap().is_none());
        assert!(evaluator.on_close(1.0).unwrap().is_none());
        let target = evaluator.on_close(10.0).unwrap().unwrap();
        assert_eq!(target.bar_index, 3);
        assert_eq!(target.quantity, Decimal::ONE);
    }

    #[test]
    fn publishes_long_and_flat_only_on_crossings() {
        let mut evaluator = EmaCrossover::new(2, 3, Decimal::new(125, 2)).unwrap();
        for price in [1.0, 1.0, 1.0] {
            assert!(evaluator.on_close(price).unwrap().is_none());
        }
        let long = evaluator.on_close(10.0).unwrap().unwrap();
        assert_eq!(long.quantity, Decimal::new(125, 2));
        assert!(evaluator.on_close(10.0).unwrap().is_none());
        let flat = evaluator.on_close(0.0).unwrap().unwrap();
        assert_eq!(flat.quantity, Decimal::ZERO);
        assert_eq!(flat.bar_index, 5);
    }
}

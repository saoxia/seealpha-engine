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


/// Run the bundled EMA strategy entirely inside Rust. No interpreter or external
/// strategy host is needed. This is an opt-in native strategy entrypoint; the
/// existing Python package protocol remains independently supported.
pub fn run_native_ema(
    parameters: &serde_json::Value,
    symbol: &str,
    bars: &[super::StrategyBar],
) -> Result<super::StrategyRun, super::StrategyError> {
    use serde_json::{Value, json};
    let bad = || super::StrategyError::Package("Rust EMA 策略参数无效。".into());
    let supplied = parameters.as_object().ok_or_else(bad)?;
    if supplied.keys().any(|key| {
        !matches!(
            key.as_str(),
            "symbol" | "fast_period" | "slow_period" | "allocation" | "risk_limit"
        )
    }) {
        return Err(bad());
    }
    let strategy_symbol = supplied.get("symbol").and_then(Value::as_str).unwrap_or("ETHUSDT");
    if strategy_symbol.to_ascii_uppercase() != "ETHUSDT" || symbol != "ETHUSDT" {
        return Err(bad());
    }
    let period = |key: &str, default: usize| -> Result<usize, super::StrategyError> {
        match supplied.get(key) {
            None => Ok(default),
            Some(Value::Number(n)) => n.as_u64().and_then(|n| usize::try_from(n).ok()).ok_or_else(bad),
            Some(Value::String(s)) if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) =>
                s.parse::<usize>().map_err(|_| bad()),
            _ => Err(bad()),
        }
    };
    let number = |key: &str, default: &str| -> Result<Decimal, super::StrategyError> {
        let text = match supplied.get(key) {
            None => default.to_string(),
            Some(Value::String(s)) => s.clone(),
            Some(Value::Number(n)) => n.to_string(),
            _ => return Err(bad()),
        };
        let amount = text.parse::<Decimal>().map_err(|_| bad())?;
        if amount <= Decimal::ZERO || amount.scale() > 8 || amount.mantissa().unsigned_abs().to_string().len() > 30 {
            return Err(bad());
        }
        Ok(amount)
    };
    let fast = period("fast_period", 8)?;
    let slow = period("slow_period", 21)?;
    let allocation = number("allocation", "1")?;
    let risk_limit = match supplied.get("risk_limit") {
        Some(_) => number("risk_limit", "1")?,
        None => allocation,
    };
    let mut evaluator = EmaCrossover::new(fast, slow, allocation).map_err(|error| {
        super::StrategyError::Package(error.to_string())
    })?;
    let mut publications = Vec::new();
    for bar in bars {
        let close = bar.close.parse::<f64>().map_err(|_| bad())?;
        if let Some(target) = evaluator
            .on_close(close)
            .map_err(|error| super::StrategyError::Failed(error.to_string()))?
        {
            publications.push(super::Publication {
                symbol: symbol.into(),
                quantity: target.quantity.to_string(),
                weight: None,
                bar_index: target.bar_index,
            });
        }
    }
    Ok(super::StrategyRun {
        prepared: json!({
            "symbol": "ETHUSDT",
            "fast_period": fast,
            "slow_period": slow,
            "allocation": allocation.to_string(),
            "risk_limit": risk_limit.to_string(),
        }),
        publications,
    })
}


#[cfg(test)]
mod native_runner_tests {
    use super::*;
    use serde_json::json;

    fn bar(close: &str) -> crate::StrategyBar {
        crate::StrategyBar {
            open_time_ms: 0,
            open: close.into(),
            high: close.into(),
            low: close.into(),
            close: close.into(),
            volume: "1".into(),
        }
    }

    #[test]
    fn native_runner_publishes_without_a_python_host() {
        let bars = ["1", "1", "1", "10", "10", "0"].map(bar);
        let run = run_native_ema(
            &json!({"fast_period": 2, "slow_period": 3, "allocation": "1.25"}),
            "ETHUSDT",
            &bars,
        )
        .unwrap();
        assert_eq!(run.prepared["risk_limit"], "1.25");
        assert_eq!(run.publications.len(), 2);
        assert_eq!(run.publications[0].bar_index, 3);
        assert_eq!(run.publications[0].quantity, "1.25");
        assert_eq!(run.publications[1].bar_index, 5);
        assert_eq!(run.publications[1].quantity, "0");
    }

    #[test]
    fn rejects_unknown_parameters_and_invalid_bars() {
        assert!(run_native_ema(&json!({"credential": "x"}), "ETHUSDT", &[]).is_err());
        assert!(run_native_ema(&json!({"fast_period": true}), "ETHUSDT", &[]).is_err());
        assert!(run_native_ema(&json!({}), "BTCUSDT", &[]).is_err());
        assert!(run_native_ema(&json!({}), "ETHUSDT", &[bar("NaN")]).is_err());
    }
}

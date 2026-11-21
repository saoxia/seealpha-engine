//! Quote evidence for protected option research. No synthetic fills, no future quotes.
use rust_decimal::Decimal;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AskQuote {
    pub ts_event_ns: i64,
    pub bid: Decimal,
    pub ask: Decimal,
    pub ask_size: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtectionAsk {
    pub decision_ns: i64,
    pub put: AskQuote,
    pub call: AskQuote,
}

/// Match the last observed put/call quote at or before each decision.
/// Reject missing, crossed, stale, unordered or undersized evidence.
pub fn align_protection_asks(
    decisions_ns: &[i64],
    puts: &[AskQuote],
    calls: &[AskQuote],
    quantity: Decimal,
    max_age_ns: i64,
) -> Result<Vec<ProtectionAsk>, &'static str> {
    if decisions_ns.is_empty()
        || puts.is_empty()
        || calls.is_empty()
        || quantity <= Decimal::ZERO
        || !(1..=300_000_000_000).contains(&max_age_ns)
        || decisions_ns.windows(2).any(|w| w[1] <= w[0])
    {
        return Err("invalid protection quote window");
    }
    for quotes in [puts, calls] {
        if quotes.len() > 250_000
            || quotes
                .windows(2)
                .any(|w| w[1].ts_event_ns <= w[0].ts_event_ns)
            || quotes.iter().any(|q| {
                q.ts_event_ns < 0
                    || q.bid < Decimal::ZERO
                    || q.ask <= Decimal::ZERO
                    || q.bid > q.ask
                    || q.ask_size <= Decimal::ZERO
            })
        {
            return Err("invalid or unordered quote evidence");
        }
    }
    let mut put_index = 0;
    let mut call_index = 0;
    let mut result = Vec::with_capacity(decisions_ns.len());
    for &decision in decisions_ns {
        if decision < 0 {
            return Err("invalid decision timestamp");
        }
        while put_index < puts.len() && puts[put_index].ts_event_ns <= decision {
            put_index += 1;
        }
        while call_index < calls.len() && calls[call_index].ts_event_ns <= decision {
            call_index += 1;
        }
        let (Some(put), Some(call)) = (
            put_index.checked_sub(1).and_then(|i| puts.get(i)),
            call_index.checked_sub(1).and_then(|i| calls.get(i)),
        ) else {
            return Err("missing historical ask evidence");
        };
        if decision - put.ts_event_ns > max_age_ns
            || decision - call.ts_event_ns > max_age_ns
            || put.ask_size < quantity
            || call.ask_size < quantity
        {
            return Err("stale or insufficient historical ask evidence");
        }
        result.push(ProtectionAsk {
            decision_ns: decision,
            put: *put,
            call: *call,
        });
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn q(ts: i64, size: i64) -> AskQuote {
        AskQuote {
            ts_event_ns: ts,
            bid: Decimal::from(1),
            ask: Decimal::from(2),
            ask_size: Decimal::from(size),
        }
    }
    #[test]
    fn as_of_join_never_borrows_future_quotes() {
        let p = [q(100, 2), q(220, 2)];
        let c = [q(110, 2), q(230, 2)];
        let aligned = align_protection_asks(&[150, 250], &p, &c, Decimal::ONE, 100).unwrap();
        assert_eq!(aligned[0].put.ts_event_ns, 100);
        assert_eq!(aligned[1].call.ts_event_ns, 230);
        assert!(align_protection_asks(&[105], &p, &c, Decimal::ONE, 100).is_err());
    }
    #[test]
    fn stale_crossed_unordered_and_short_asks_fail_closed() {
        assert!(
            align_protection_asks(&[300], &[q(100, 2)], &[q(100, 2)], Decimal::ONE, 100).is_err()
        );
        assert!(
            align_protection_asks(&[150], &[q(100, 0)], &[q(100, 2)], Decimal::ONE, 100).is_err()
        );
        assert!(
            align_protection_asks(
                &[150],
                &[q(100, 2), q(100, 2)],
                &[q(100, 2)],
                Decimal::ONE,
                100
            )
            .is_err()
        );
        let mut crossed = q(100, 2);
        crossed.bid = Decimal::from(3);
        assert!(
            align_protection_asks(&[150], &[crossed], &[q(100, 2)], Decimal::ONE, 100).is_err()
        );
    }
}

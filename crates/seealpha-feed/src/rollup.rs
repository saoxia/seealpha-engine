//! Closed-minute rollups. A partial bar is not stored. A time close is not
//! the same thing as complete coverage, and a restart clears allow_new_risk.
use rust_decimal::Decimal;
use std::collections::BTreeMap;

use crate::MINUTE_MS;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollError;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Minute {
    volume: Decimal,
    close: Decimal,
    closed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candle {
    pub interval: String,
    pub volume: Decimal,
    pub close: Decimal,
    pub is_closed: bool,
    pub is_complete: bool,
    pub allow_new_risk: bool,
    pub revision: u64,
    pub source_levels: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct Rollups {
    intervals: Vec<String>,
    minutes: BTreeMap<i64, Minute>,
    stored: BTreeMap<(String, i64), Candle>,
}

impl Rollups {
    pub fn new(intervals: &[&str]) -> Self {
        Self {
            intervals: intervals.iter().map(|item| (*item).to_owned()).collect(),
            minutes: BTreeMap::new(),
            stored: BTreeMap::new(),
        }
    }

    pub fn put_closed(&mut self, index: i64, volume: Decimal, close: Decimal) {
        self.minutes.insert(
            index,
            Minute {
                volume,
                close,
                closed: true,
            },
        );
    }

    pub fn update(
        &mut self,
        index: i64,
        volume: Decimal,
        close: Decimal,
        now_ms: i64,
        is_closed: bool,
        allow_new_risk: bool,
    ) -> Result<Option<Vec<Candle>>, RollError> {
        if index * MINUTE_MS > now_ms {
            return if is_closed { Err(RollError) } else { Ok(None) };
        }
        if !is_closed && self.covers_closed(index) {
            return Ok(None);
        }
        if is_closed {
            self.put_closed(index, volume, close);
        }
        let mut produced = Vec::new();
        for interval in self.intervals.clone() {
            let factor = factor(&interval);
            let open = index - index.rem_euclid(factor);
            let end = open + factor;
            let window_end = end * MINUTE_MS;
            let closed = is_closed && now_ms >= window_end;
            let mut total = Decimal::ZERO;
            let mut last_close = close;
            let mut present = 0_i64;
            let mut all_closed = true;
            for cursor in open..end {
                let minute = if cursor == index && !is_closed {
                    Some(Minute {
                        volume,
                        close,
                        closed: false,
                    })
                } else {
                    self.minutes.get(&cursor).cloned()
                };
                if let Some(minute) = minute {
                    total += minute.volume;
                    last_close = minute.close;
                    present += 1;
                    all_closed &= minute.closed;
                }
            }
            let complete = present == factor && all_closed;
            let sources = sources(&self.minutes, open, end, factor, is_closed);
            let candle = Candle {
                interval: interval.clone(),
                volume: total,
                close: last_close,
                is_closed: closed,
                is_complete: complete,
                allow_new_risk: allow_new_risk && complete && closed,
                revision: 0,
                source_levels: sources,
            };
            if closed {
                let key = (interval, open);
                let revision = self
                    .stored
                    .get(&key)
                    .map(|prior| prior.revision)
                    .unwrap_or(0)
                    + 1;
                let mut stored = candle.clone();
                stored.revision = revision;
                self.stored.insert(key, stored.clone());
                produced.push(stored);
            } else {
                produced.push(candle);
            }
        }
        Ok(Some(produced))
    }

    pub fn latest(&self, interval: &str) -> Option<&Candle> {
        self.stored
            .iter()
            .filter(|((name, _), _)| name == interval)
            .max_by_key(|((_, open), _)| *open)
            .map(|(_, candle)| candle)
    }

    pub fn recover(&mut self) {
        for candle in self.stored.values_mut() {
            candle.allow_new_risk = false;
        }
    }

    pub fn stored_count(&self, interval: &str) -> usize {
        self.stored
            .keys()
            .filter(|(name, _)| name == interval)
            .count()
    }

    fn covers_closed(&self, index: i64) -> bool {
        self.stored.iter().any(|((interval, open), candle)| {
            let width = factor(interval);
            candle.is_closed && index >= *open && index < open + width
        })
    }
}

fn factor(interval: &str) -> i64 {
    match interval {
        "1m" => 1,
        "5m" => 5,
        "15m" => 15,
        _ => 1,
    }
}

fn sources(
    minutes: &BTreeMap<i64, Minute>,
    open: i64,
    end: i64,
    factor: i64,
    triggering_closed: bool,
) -> Vec<String> {
    if factor < 15 {
        return vec!["1m".into()];
    }
    let mut complete_blocks = 0;
    let mut blocks = 0;
    let mut cursor = open;
    while cursor < end {
        let block_end = cursor + 5;
        let complete =
            (cursor..block_end).all(|index| minutes.get(&index).is_some_and(|m| m.closed));
        if complete {
            complete_blocks += 1;
        }
        blocks += 1;
        cursor = block_end;
    }
    if complete_blocks == blocks {
        return vec!["5m".into()];
    }
    let mut levels = Vec::new();
    if complete_blocks > 0 {
        levels.push("5m".into());
    }
    levels.push("1m".into());
    if !triggering_closed {
        levels.push("live_trades".into());
    }
    levels
}

#[cfg(test)]
mod tests {
    use super::*;

    fn two() -> Decimal {
        Decimal::from(2)
    }

    #[test]
    fn test_partial_is_ephemeral_and_final_rollup_is_idempotent() {
        let mut rollups = Rollups::new(&["1m", "5m"]);
        for index in 0..4 {
            rollups.put_closed(index, two(), Decimal::from(10));
            rollups
                .update(
                    index,
                    two(),
                    Decimal::from(10),
                    (index + 1) * MINUTE_MS,
                    true,
                    false,
                )
                .unwrap();
        }
        let partial = rollups
            .update(
                4,
                Decimal::ONE,
                Decimal::from(11),
                4 * MINUTE_MS + 1000,
                false,
                false,
            )
            .unwrap()
            .unwrap();
        assert!(
            partial
                .iter()
                .all(|value| !value.is_closed && !value.allow_new_risk)
        );
        assert_eq!(partial[1].volume, Decimal::from(9));
        assert_eq!(rollups.stored_count("5m"), 0);
        let final_candle = rollups
            .update(4, two(), Decimal::from(11), 5 * MINUTE_MS, true, true)
            .unwrap()
            .unwrap()[1]
            .clone();
        assert!(final_candle.is_closed && final_candle.is_complete && final_candle.allow_new_risk);
        assert_eq!(final_candle.volume, Decimal::from(10));
        assert_eq!(final_candle.close, Decimal::from(11));
        let repeated = rollups
            .update(4, two(), Decimal::from(11), 5 * MINUTE_MS, true, false)
            .unwrap()
            .unwrap()[1]
            .clone();
        assert_eq!(repeated.volume, Decimal::from(10));
        assert!(repeated.revision > final_candle.revision);
        assert!(
            rollups
                .update(
                    4,
                    Decimal::ONE,
                    Decimal::from(11),
                    5 * MINUTE_MS,
                    false,
                    false
                )
                .unwrap()
                .is_none()
        );
        rollups.recover();
        assert_eq!(rollups.latest("5m").unwrap().volume, Decimal::from(10));
        assert!(!rollups.latest("5m").unwrap().allow_new_risk);
    }

    #[test]
    fn test_time_close_is_distinct_from_complete_coverage_and_backfill_repairs() {
        let mut rollups = Rollups::new(&["5m"]);
        rollups.put_closed(0, two(), Decimal::from(10));
        rollups.put_closed(4, two(), Decimal::from(10));
        let value = rollups
            .update(4, two(), Decimal::from(10), 5 * MINUTE_MS, true, true)
            .unwrap()
            .unwrap()[0]
            .clone();
        assert!(value.is_closed && !value.is_complete && !value.allow_new_risk);
        for index in 1..4 {
            rollups.put_closed(index, two(), Decimal::from(10));
        }
        let corrected = rollups
            .update(3, two(), Decimal::from(10), 5 * MINUTE_MS, true, false)
            .unwrap()
            .unwrap()[0]
            .clone();
        assert!(corrected.is_complete);
        assert_eq!(corrected.volume, Decimal::from(10));
        assert!(corrected.revision > value.revision && !corrected.allow_new_risk);
        assert!(
            rollups
                .update(10, two(), Decimal::from(10), 5 * MINUTE_MS, false, false)
                .unwrap()
                .is_none()
        );
        assert!(
            rollups
                .update(10, two(), Decimal::from(10), 5 * MINUTE_MS, true, false)
                .is_err()
        );
    }

    #[test]
    fn test_auto_rollup_reuses_complete_five_minutes_and_merges_live_tail_once() {
        let mut rollups = Rollups::new(&["1m", "5m", "15m"]);
        for index in 0..13 {
            rollups.put_closed(index, two(), Decimal::from(10));
            rollups
                .update(
                    index,
                    two(),
                    Decimal::from(10),
                    (index + 1) * MINUTE_MS,
                    true,
                    false,
                )
                .unwrap();
        }
        let live = rollups
            .update(
                13,
                Decimal::ONE,
                Decimal::from(10),
                13 * MINUTE_MS + 1000,
                false,
                false,
            )
            .unwrap()
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(
            live.source_levels
                .iter()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            ["5m", "1m", "live_trades"].into_iter().collect()
        );
        assert_eq!(live.volume, Decimal::from(27));
        assert!(!live.is_closed && !live.allow_new_risk);
        let repeated = rollups
            .update(
                13,
                Decimal::ONE,
                Decimal::from(10),
                13 * MINUTE_MS + 2000,
                false,
                false,
            )
            .unwrap()
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(repeated.volume, Decimal::from(27));
        let mut final_candle = None;
        for index in [13, 14] {
            rollups.put_closed(index, two(), Decimal::from(10));
            final_candle = rollups
                .update(
                    index,
                    two(),
                    Decimal::from(10),
                    (index + 1) * MINUTE_MS,
                    true,
                    false,
                )
                .unwrap()
                .unwrap()
                .pop();
        }
        let final_candle = final_candle.unwrap();
        assert_eq!(final_candle.source_levels, vec!["5m".to_owned()]);
        assert!(final_candle.is_complete && final_candle.is_closed);
        assert_eq!(final_candle.volume, Decimal::from(30));
        rollups.recover();
        let value = rollups.latest("15m").unwrap();
        assert_eq!(value.volume, Decimal::from(30));
        assert_eq!(value.source_levels, vec!["5m".to_owned()]);
        assert!(!value.allow_new_risk);
    }

    #[test]
    fn test_auto_rollup_falls_back_when_lower_rollup_is_incomplete() {
        let mut rollups = Rollups::new(&["5m", "15m"]);
        for index in (0..15).filter(|index| *index != 2) {
            rollups.put_closed(index, two(), Decimal::from(10));
        }
        let value = rollups
            .update(4, two(), Decimal::from(10), 15 * MINUTE_MS, true, false)
            .unwrap()
            .unwrap()[0]
            .clone();
        assert!(!value.is_complete);
        let aggregate = rollups
            .update(14, two(), Decimal::from(10), 15 * MINUTE_MS, true, false)
            .unwrap()
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(aggregate.volume, Decimal::from(28));
        assert!(!aggregate.is_complete);
        assert!(aggregate.source_levels.iter().any(|level| level == "1m"));
        rollups.put_closed(2, two(), Decimal::from(10));
        let repaired = rollups
            .update(2, two(), Decimal::from(10), 15 * MINUTE_MS, true, false)
            .unwrap()
            .unwrap()
            .pop()
            .unwrap();
        assert!(repaired.is_complete);
        assert_eq!(repaired.volume, Decimal::from(30));
        assert!(!repaired.allow_new_risk);
    }
}

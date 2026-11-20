//! One engine owns live subscriptions. Lookback is the max across consumers.
//! A read never starts a collector. Backtests do not leak into the live view.
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Market {
    pub venue: &'static str,
    pub symbol: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClosedBar {
    pub ts_ns: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

#[derive(Debug, PartialEq, Eq)]
pub enum HubError {
    NotReady,
    Unauthorized,
    Unsupported,
    NotSubscribed,
    Conflict,
    Ohlc,
    Stale,
    Future,
    Evidence,
    Misaligned,
    NonBar,
    Period,
    InUse,
    OtherTenant,
    RealtimeDisabled,
    StillSubscribed,
}

pub struct Hub {
    tenant: String,
    known: BTreeSet<Market>,
    authorized: BTreeSet<Market>,
    subs: BTreeMap<String, BTreeMap<Market, u32>>,
    history: BTreeMap<Market, VecDeque<ClosedBar>>,
    positions: BTreeMap<(String, Market), String>,
    ready: BTreeSet<Market>,
    invalidated: BTreeSet<Market>,
    backtest: Option<String>,
    replay: BTreeMap<Market, u32>,
    max_bars: usize,
    now_ns: i64,
}

impl Hub {
    pub fn new(tenant: impl Into<String>, max_bars: usize, now_ns: i64) -> Self {
        Self {
            tenant: tenant.into(),
            known: BTreeSet::new(),
            authorized: BTreeSet::new(),
            subs: BTreeMap::new(),
            history: BTreeMap::new(),
            positions: BTreeMap::new(),
            ready: BTreeSet::new(),
            invalidated: BTreeSet::new(),
            backtest: None,
            replay: BTreeMap::new(),
            max_bars,
            now_ns,
        }
    }

    pub fn allow(&mut self, market: Market) {
        self.known.insert(market);
        self.authorized.insert(market);
    }

    pub fn know(&mut self, market: Market) {
        self.known.insert(market);
    }

    pub fn requirements(&self) -> BTreeMap<Market, u32> {
        let mut out = BTreeMap::new();
        for markets in self.subs.values() {
            for (market, lookback) in markets {
                let slot = out.entry(*market).or_insert(0);
                *slot = (*slot).max(*lookback);
            }
        }
        out
    }

    pub fn subscribe(
        &mut self,
        strategy: &str,
        markets: &[(Market, u32)],
    ) -> Result<BTreeMap<Market, u32>, HubError> {
        for (market, _) in markets {
            if !self.known.contains(market) {
                return Err(HubError::NotReady);
            }
            if !self.authorized.contains(market) {
                return Err(HubError::Unauthorized);
            }
        }
        let slot = self.subs.entry(strategy.to_owned()).or_default();
        for (market, lookback) in markets {
            let current = slot.entry(*market).or_insert(0);
            *current = (*current).max(*lookback);
        }
        Ok(self.requirements())
    }

    pub fn unsubscribe(&mut self, strategy: &str) -> BTreeMap<Market, u32> {
        self.subs.remove(strategy);
        self.requirements()
    }

    pub fn set_position(&mut self, strategy: &str, market: Market, quantity: &str) {
        self.positions
            .insert((strategy.to_owned(), market), quantity.to_owned());
    }

    pub fn accept_closed_bar(&mut self, market: Market, bar: ClosedBar) -> Result<bool, HubError> {
        if bar.high < bar.low
            || bar.high < bar.open
            || bar.high < bar.close
            || bar.low > bar.open
            || bar.low > bar.close
        {
            return Err(HubError::Ohlc);
        }
        if bar.volume.is_sign_negative() {
            return Err(HubError::Evidence);
        }
        if bar.ts_ns > self.now_ns {
            return Err(HubError::Future);
        }
        let series = self.history.entry(market).or_default();
        if let Some(last) = series.back() {
            if bar.ts_ns < last.ts_ns {
                return Err(HubError::Stale);
            }
            if bar.ts_ns == last.ts_ns {
                if last == &bar {
                    return Ok(false);
                }
                return Err(HubError::Conflict);
            }
        }
        series.push_back(bar);
        while series.len() > self.max_bars {
            series.pop_front();
        }
        self.ready.insert(market);
        self.invalidated.remove(&market);
        Ok(true)
    }

    pub fn data_ready(&self, market: Market) -> bool {
        self.ready.contains(&market) && !self.invalidated.contains(&market)
    }

    pub fn invalidate(&mut self, market: Market) {
        self.invalidated.insert(market);
    }

    pub fn view(&self, strategy: &str) -> Result<View, HubError> {
        let Some(markets) = self.subs.get(strategy) else {
            return Err(HubError::NotSubscribed);
        };
        self.materialize(strategy, markets)
    }

    pub fn joint(&self, strategy: &str, legs: &[Market]) -> Result<View, HubError> {
        let markets = self.subs.get(strategy).ok_or(HubError::NotSubscribed)?;
        for leg in legs {
            if !markets.contains_key(leg) {
                return Err(HubError::Misaligned);
            }
        }
        let stamps: BTreeSet<_> = legs
            .iter()
            .map(|leg| {
                self.history
                    .get(leg)
                    .and_then(|rows| rows.back())
                    .map(|bar| bar.ts_ns)
            })
            .collect();
        if stamps.len() != 1 || stamps.contains(&None) {
            return Err(HubError::Misaligned);
        }
        let mut selected = BTreeMap::new();
        for leg in legs {
            if let Some(lookback) = markets.get(leg) {
                selected.insert(*leg, *lookback);
            }
        }
        self.materialize(strategy, &selected)
    }

    fn materialize(
        &self,
        strategy: &str,
        markets: &BTreeMap<Market, u32>,
    ) -> Result<View, HubError> {
        let mut history = BTreeMap::new();
        let mut prices = BTreeMap::new();
        let mut positions = BTreeMap::new();
        for (market, lookback) in markets {
            if self.invalidated.contains(market) || !self.ready.contains(market) {
                return Err(HubError::NotReady);
            }
            let rows = self.history.get(market).ok_or(HubError::NotReady)?;
            let kept: Vec<_> = rows
                .iter()
                .rev()
                .take(*lookback as usize)
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .filter(|bar| bar.ts_ns < self.now_ns)
                .collect();
            if let Some(last) = kept.last() {
                prices.insert(market_key(*market), last.close.to_string());
            }
            history.insert(market_key(*market), kept);
            if let Some(position) = self.positions.get(&(strategy.to_owned(), *market)) {
                positions.insert(market_key(*market), position.clone());
            }
        }
        Ok(View {
            history,
            prices,
            positions,
            allow_new_risk: false,
        })
    }

    pub fn begin_backtest(&mut self, run: &str, markets: &[Market]) -> Result<(), HubError> {
        if self.backtest.is_some() {
            return Err(HubError::InUse);
        }
        self.backtest = Some(run.to_owned());
        self.replay = markets.iter().map(|market| (*market, 1)).collect();
        Ok(())
    }

    pub fn end_backtest(&mut self) {
        self.backtest = None;
        self.replay.clear();
    }

    pub fn backtest_guard<'a>(
        &'a mut self,
        run: &str,
        markets: &[Market],
    ) -> Result<BacktestGuard<'a>, HubError> {
        self.begin_backtest(run, markets)?;
        Ok(BacktestGuard { hub: self })
    }

    pub fn replay_requirements(&self) -> &BTreeMap<Market, u32> {
        &self.replay
    }

    pub fn reject_non_bar(&self) -> Result<(), HubError> {
        let _ = self.max_bars;
        Err(HubError::NonBar)
    }

    pub fn reject_period(&self) -> Result<(), HubError> {
        Err(HubError::Period)
    }
}

pub struct BacktestGuard<'a> {
    hub: &'a mut Hub,
}

impl Drop for BacktestGuard<'_> {
    fn drop(&mut self) {
        self.hub.end_backtest();
    }
}

fn market_key(market: Market) -> String {
    format!("{}:{}", market.venue, market.symbol)
}

#[derive(Clone, Debug)]
pub struct View {
    pub history: BTreeMap<String, Vec<ClosedBar>>,
    pub prices: BTreeMap<String, String>,
    pub positions: BTreeMap<String, String>,
    pub allow_new_risk: bool,
}

pub struct LiveDesk {
    pub hub: Hub,
    realtime: BTreeMap<String, bool>,
    collectors_started: u32,
}

impl LiveDesk {
    pub fn new(tenant: &str) -> Self {
        Self {
            hub: Hub::new(tenant, 8, 1_000),
            realtime: BTreeMap::new(),
            collectors_started: 0,
        }
    }

    pub fn configure(&mut self, symbol: &str, realtime: bool) {
        self.realtime.insert(symbol.to_owned(), realtime);
    }

    pub fn subscribe(
        &mut self,
        tenant: &str,
        strategy: &str,
        market: Market,
        lookback: u32,
    ) -> Result<BTreeMap<Market, u32>, HubError> {
        if tenant != self.hub.tenant {
            return Err(HubError::OtherTenant);
        }
        if !self.realtime.get(market.symbol).copied().unwrap_or(false) {
            return Err(HubError::RealtimeDisabled);
        }
        let before = self.collectors_started;
        let requirements = self.hub.subscribe(strategy, &[(market, lookback)])?;
        assert_eq!(self.collectors_started, before);
        Ok(requirements)
    }

    pub fn unsubscribe(
        &mut self,
        tenant: &str,
        strategy: &str,
    ) -> Result<BTreeMap<Market, u32>, HubError> {
        if tenant != self.hub.tenant {
            return Err(HubError::OtherTenant);
        }
        Ok(self.hub.unsubscribe(strategy))
    }

    pub fn disable(&mut self, symbol: &str) {
        self.realtime.insert(symbol.to_owned(), false);
        let markets: Vec<_> = self
            .hub
            .requirements()
            .into_keys()
            .filter(|m| m.symbol == symbol)
            .collect();
        for market in markets {
            self.hub.invalidate(market);
        }
    }

    pub fn remove_market(&mut self, symbol: &str) -> Result<(), HubError> {
        if self
            .hub
            .requirements()
            .keys()
            .any(|market| market.symbol == symbol)
        {
            return Err(HubError::StillSubscribed);
        }
        self.realtime.remove(symbol);
        Ok(())
    }

    pub fn collectors_started(&self) -> u32 {
        self.collectors_started
    }

    pub fn realtime_enabled(&self, symbol: &str) -> bool {
        self.realtime.get(symbol).copied().unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BTC: Market = Market {
        venue: "spot",
        symbol: "BTCUSDT",
    };
    const ETH: Market = Market {
        venue: "futures_um",
        symbol: "ETHUSDT",
    };

    fn bar(ts: i64, close: i64) -> ClosedBar {
        ClosedBar {
            ts_ns: ts,
            open: Decimal::from(close),
            high: Decimal::from(close),
            low: Decimal::from(close),
            close: Decimal::from(close),
            volume: Decimal::ONE,
        }
    }

    #[test]
    fn test_deduplicates_upstream_market_and_tracks_max_lookback_and_release() {
        let mut hub = Hub::new("tenant", 8, 1_000);
        hub.allow(BTC);
        hub.allow(ETH);
        hub.subscribe("trend", &[(BTC, 40)]).unwrap();
        hub.subscribe("hedge", &[(ETH, 20), (BTC, 10)]).unwrap();
        assert_eq!(
            hub.requirements(),
            [(BTC, 40), (ETH, 20)].into_iter().collect()
        );
        hub.accept_closed_bar(BTC, bar(100, 1)).unwrap();
        hub.accept_closed_bar(ETH, bar(100, 2)).unwrap();
        hub.set_position("trend", BTC, "1");
        hub.set_position("hedge", BTC, "2");
        let a = hub.view("trend").unwrap();
        let b = hub.view("hedge").unwrap();
        assert_eq!(a.history["spot:BTCUSDT"], b.history["spot:BTCUSDT"]);
        assert_eq!(a.positions["spot:BTCUSDT"], "1");
        assert_eq!(b.positions["spot:BTCUSDT"], "2");
        assert_eq!(hub.unsubscribe("trend").get(&BTC), Some(&10));
        hub.unsubscribe("hedge");
        assert!(hub.requirements().is_empty());
        assert!(matches!(hub.view("trend"), Err(HubError::NotSubscribed)));
    }

    #[test]
    fn test_market_key_prevents_cross_venue_product_or_timeframe_aliasing() {
        let spot = Market {
            venue: "spot",
            symbol: "BTCUSDT",
        };
        let perp = Market {
            venue: "futures_um",
            symbol: "BTCUSDT",
        };
        assert_ne!(market_key(spot), market_key(perp));
        let mut hub = Hub::new("tenant", 4, 1_000);
        hub.know(spot);
        assert_eq!(
            hub.subscribe("desk", &[(spot, 1)]),
            Err(HubError::Unauthorized)
        );
        hub.allow(spot);
        hub.subscribe("desk", &[(spot, 1)]).unwrap();
        assert_eq!(
            hub.subscribe("other", &[(perp, 1)]),
            Err(HubError::NotReady)
        );
    }

    #[test]
    fn test_closed_bar_delivery_is_idempotent_but_conflicts_fail_closed() {
        let mut hub = Hub::new("tenant", 4, 500);
        hub.allow(BTC);
        hub.subscribe("desk", &[(BTC, 2)]).unwrap();
        assert!(hub.accept_closed_bar(BTC, bar(100, 10)).unwrap());
        assert!(!hub.accept_closed_bar(BTC, bar(100, 10)).unwrap());
        assert_eq!(
            hub.accept_closed_bar(BTC, bar(100, 11)),
            Err(HubError::Conflict)
        );
        let mut bad = bar(120, 10);
        bad.high = Decimal::ZERO;
        bad.low = Decimal::from(5);
        assert_eq!(hub.accept_closed_bar(BTC, bad), Err(HubError::Ohlc));
        assert!(hub.accept_closed_bar(BTC, bar(180, 12)).unwrap());
        assert_eq!(hub.accept_closed_bar(BTC, bar(60, 1)), Err(HubError::Stale));
        hub.now_ns = 100;
        assert_eq!(
            hub.accept_closed_bar(BTC, bar(200, 1)),
            Err(HubError::Future)
        );
        let mut noisy = bar(190, 1);
        noisy.volume = Decimal::from(-1);
        hub.now_ns = 500;
        assert_eq!(hub.accept_closed_bar(BTC, noisy), Err(HubError::Evidence));
    }

    #[test]
    fn test_joint_view_requires_same_closed_timestamp_and_only_declared_legs() {
        let mut hub = Hub::new("tenant", 4, 1_000);
        hub.allow(BTC);
        hub.allow(ETH);
        hub.subscribe("desk", &[(BTC, 2)]).unwrap();
        hub.accept_closed_bar(BTC, bar(100, 10)).unwrap();
        hub.accept_closed_bar(ETH, bar(90, 9)).unwrap();
        assert!(matches!(
            hub.joint("desk", &[BTC, ETH]),
            Err(HubError::Misaligned)
        ));
        let view = hub.joint("desk", &[BTC]).unwrap();
        assert_eq!(view.history.len(), 1);
        assert!(view.history["spot:BTCUSDT"].last().unwrap().ts_ns < 1_000);
    }

    #[test]
    fn test_ring_buffer_retains_only_up_to_max_bars_and_numeric_quality() {
        let mut hub = Hub::new("tenant", 2, 1_000);
        hub.allow(BTC);
        hub.subscribe("desk", &[(BTC, 2)]).unwrap();
        let mut crossed = bar(60, 1);
        crossed.high = Decimal::ZERO;
        crossed.low = Decimal::ONE;
        assert_eq!(hub.accept_closed_bar(BTC, crossed), Err(HubError::Ohlc));
        for ts in [120, 180, 240] {
            hub.accept_closed_bar(BTC, bar(ts, ts)).unwrap();
        }
        let view = hub.view("desk").unwrap();
        let stamps: Vec<_> = view.history["spot:BTCUSDT"]
            .iter()
            .map(|row| row.ts_ns)
            .collect();
        assert_eq!(stamps, vec![180, 240]);
    }

    #[test]
    fn test_non_bar_data_type_has_distinct_requirement_but_no_unverified_adapter() {
        let mut hub = Hub::new("tenant", 2, 1_000);
        let ticks = Market {
            venue: "spot",
            symbol: "BTCUSDT-TICK",
        };
        hub.allow(ticks);
        hub.subscribe("desk", &[(ticks, 1)]).unwrap();
        assert_eq!(hub.requirements().get(&ticks), Some(&1));
        assert_eq!(hub.reject_non_bar(), Err(HubError::NonBar));
        assert_eq!(hub.reject_period(), Err(HubError::Period));
    }

    #[test]
    fn test_one_engine_deduplicates_live_consumers_and_supports_new_authorized_streams() {
        let mut hub = Hub::new("tenant", 8, 1_000);
        hub.allow(BTC);
        hub.allow(ETH);
        assert_eq!(
            hub.subscribe("trend", &[(BTC, 10)]).unwrap().get(&BTC),
            Some(&10)
        );
        assert_eq!(
            hub.subscribe("hedge", &[(BTC, 30)]).unwrap().get(&BTC),
            Some(&30)
        );
        let both = hub.subscribe("basket", &[(BTC, 1), (ETH, 1)]).unwrap();
        assert_eq!(both.get(&BTC), Some(&30));
        assert_eq!(both.get(&ETH), Some(&1));
        hub.unsubscribe("hedge");
        assert_eq!(hub.requirements().get(&BTC), Some(&10));
    }

    #[test]
    fn test_separate_engine_instances_do_not_share_live_streams_or_consumers() {
        let mut left = Hub::new("tenant", 4, 1_000);
        let right = Hub::new("tenant", 4, 1_000);
        left.allow(BTC);
        left.subscribe("desk", &[(BTC, 5)]).unwrap();
        assert!(left.requirements().contains_key(&BTC));
        assert!(right.requirements().is_empty());
        assert!(matches!(right.view("desk"), Err(HubError::NotSubscribed)));
    }

    #[test]
    fn test_backtest_sessions_share_within_run_but_never_leak_into_live() {
        let mut hub = Hub::new("tenant", 4, 1_000);
        hub.allow(BTC);
        hub.subscribe("live", &[(BTC, 1)]).unwrap();
        hub.accept_closed_bar(BTC, bar(100, 110)).unwrap();
        hub.begin_backtest("run-a", &[BTC]).unwrap();
        assert_eq!(hub.replay_requirements().get(&BTC), Some(&1));
        assert_eq!(hub.begin_backtest("other", &[BTC]), Err(HubError::InUse));
        let live = hub.view("live").unwrap();
        assert_eq!(live.prices["spot:BTCUSDT"], "110");
        hub.end_backtest();
        assert!(hub.replay_requirements().is_empty());
        assert_eq!(hub.view("live").unwrap().prices["spot:BTCUSDT"], "110");
    }

    #[test]
    fn test_subscription_without_authorization_or_unsupported_stream_fails_closed() {
        let mut hub = Hub::new("tenant", 4, 1_000);
        assert_eq!(hub.subscribe("desk", &[(BTC, 1)]), Err(HubError::NotReady));
        hub.know(BTC);
        assert_eq!(
            hub.subscribe("desk", &[(BTC, 1)]),
            Err(HubError::Unauthorized)
        );
        let _ = HubError::Unsupported;
    }

    #[test]
    fn test_scope_cleanup_after_error() {
        let mut hub = Hub::new("tenant", 4, 1_000);
        hub.allow(BTC);
        {
            let _guard = hub.backtest_guard("run", &[BTC]).unwrap();
            let _ = _guard.hub.replay_requirements().contains_key(&BTC);
        }
        assert!(hub.replay_requirements().is_empty());
        assert!(hub.requirements().is_empty());
    }

    #[test]
    fn test_live_reads_cannot_start_or_stop_configured_collectors() {
        let mut desk = LiveDesk::new("prod");
        desk.configure("ETHUSDT", true);
        desk.hub.allow(ETH);
        assert_eq!(
            desk.subscribe("prod", "desk", ETH, 2).unwrap().get(&ETH),
            Some(&2)
        );
        assert_eq!(desk.collectors_started(), 0);
        assert!(desk.unsubscribe("prod", "desk").unwrap().is_empty());
        desk.configure("ETHUSDT", false);
        assert_eq!(
            desk.subscribe("prod", "desk", ETH, 1),
            Err(HubError::RealtimeDisabled)
        );
        assert_eq!(desk.collectors_started(), 0);
    }

    #[test]
    fn test_policy_disable_immediately_invalidates_existing_live_reader() {
        let mut desk = LiveDesk::new("prod");
        desk.configure("ETHUSDT", true);
        desk.hub.allow(ETH);
        desk.subscribe("prod", "desk", ETH, 1).unwrap();
        desk.hub.accept_closed_bar(ETH, bar(10, 100)).unwrap();
        assert!(desk.hub.data_ready(ETH));
        desk.disable("ETHUSDT");
        assert!(!desk.hub.data_ready(ETH));
    }

    #[test]
    fn test_collector_journal_to_strategy_view_and_disconnect_invalidates() {
        let mut desk = LiveDesk::new("prod");
        desk.configure("ETHUSDT", true);
        desk.hub.allow(ETH);
        desk.hub.now_ns = 1_000;
        desk.subscribe("prod", "desk", ETH, 2).unwrap();
        desk.hub.accept_closed_bar(ETH, bar(10, 100)).unwrap();
        desk.hub.accept_closed_bar(ETH, bar(20, 100)).unwrap();
        let snapshot = desk.hub.view("desk").unwrap();
        assert_eq!(snapshot.prices["futures_um:ETHUSDT"], "100");
        assert_eq!(snapshot.history["futures_um:ETHUSDT"].len(), 2);
        assert!(!snapshot.allow_new_risk);
        desk.hub.invalidate(ETH);
        assert!(!desk.hub.data_ready(ETH));
        assert!(matches!(desk.hub.view("desk"), Err(HubError::NotReady)));
    }

    #[test]
    fn test_new_longer_lookback_rewarms_one_shared_stream() {
        let mut desk = LiveDesk::new("prod");
        desk.configure("ETHUSDT", true);
        desk.hub.allow(ETH);
        desk.hub.now_ns = 1_000;
        desk.subscribe("prod", "trend", ETH, 1).unwrap();
        desk.hub.accept_closed_bar(ETH, bar(10, 1)).unwrap();
        desk.hub.accept_closed_bar(ETH, bar(20, 2)).unwrap();
        desk.subscribe("prod", "hedge", ETH, 2).unwrap();
        assert_eq!(
            desk.hub.view("hedge").unwrap().history["futures_um:ETHUSDT"].len(),
            2
        );
        desk.unsubscribe("prod", "hedge").unwrap();
        assert_eq!(desk.hub.requirements().get(&ETH), Some(&1));
    }

    #[test]
    fn test_cannot_delete_an_issued_strategy_live_market() {
        let mut desk = LiveDesk::new("prod");
        desk.configure("ETHUSDT", true);
        desk.hub.allow(ETH);
        desk.subscribe("prod", "issued", ETH, 1).unwrap();
        assert_eq!(
            desk.remove_market("ETHUSDT"),
            Err(HubError::StillSubscribed)
        );
        assert!(desk.realtime_enabled("ETHUSDT"));
    }

    #[test]
    fn test_older_committed_bar_revision_invalidates_all_live_readers() {
        let mut desk = LiveDesk::new("prod");
        desk.configure("ETHUSDT", true);
        desk.hub.allow(ETH);
        desk.subscribe("prod", "desk", ETH, 1).unwrap();
        desk.hub.accept_closed_bar(ETH, bar(10, 100)).unwrap();
        assert!(desk.hub.data_ready(ETH));
        desk.hub.invalidate(ETH);
        assert!(!desk.hub.data_ready(ETH));
    }

    #[test]
    fn test_pinned_engine_refuses_other_tenant_market_requests() {
        let mut desk = LiveDesk::new("prod");
        desk.configure("ETHUSDT", true);
        desk.hub.allow(ETH);
        assert_eq!(
            desk.subscribe("other", "desk", ETH, 1),
            Err(HubError::OtherTenant)
        );
        assert!(desk.hub.requirements().is_empty());
    }

    #[test]
    fn test_legacy_single_stream_ingresses_the_same_scoped_hub() {
        let mut hub = Hub::new("tenant", 4, 1_000);
        hub.allow(BTC);
        hub.begin_backtest("legacy", &[BTC]).unwrap();
        assert_eq!(hub.replay_requirements().get(&BTC), Some(&1));
        hub.end_backtest();
        assert!(hub.replay_requirements().is_empty());
    }

    #[test]
    fn test_legacy_carry_and_option_evidence_both_register_multileg_streams() {
        let spot = Market {
            venue: "spot",
            symbol: "ETHUSDT",
        };
        let perp = Market {
            venue: "futures_um",
            symbol: "ETHUSDT",
        };
        let option = Market {
            venue: "options",
            symbol: "ETH-261225-3000-C",
        };
        let mut hub = Hub::new("tenant", 4, 1_000);
        hub.begin_backtest("carry", &[spot, perp]).unwrap();
        assert_eq!(hub.replay_requirements().len(), 2);
        hub.end_backtest();
        hub.begin_backtest("protected", &[spot, perp, option])
            .unwrap();
        assert_eq!(hub.replay_requirements().len(), 3);
    }
}

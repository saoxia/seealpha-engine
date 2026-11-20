//! Deterministic offline multi-instrument event replay.
//! Market bars are processed in timestamp order. Pending orders match at the
//! NEXT bar open; callbacks see the current bar close and cannot fill retroactively.
//! Funding is applied only from explicit historical events.
pub mod verified;

use std::collections::{BTreeMap, BTreeSet};
use rust_decimal::Decimal;
use crate::matching::{match_bar, Ohlc, Side, WorkingOrder};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Product { Spot, LinearPerpetual, LongOption }
#[derive(Clone, Debug)]
pub struct Instrument {
    pub id: String,
    pub product: Product,
    pub multiplier: Decimal,
    pub initial_margin_ratio: Decimal,
    pub maintenance_margin_ratio: Decimal,
    pub fee_rate: Decimal,
}
#[derive(Clone, Debug)]
pub struct MarketBar {
    pub time_ms: i64,
    pub instrument: String,
    pub ohlc: Ohlc,
}
#[derive(Clone, Debug)]
pub struct Funding {
    pub time_ms: i64,
    pub instrument: String,
    pub rate: Decimal,
    pub mark_price: Decimal,
}
#[derive(Clone, Debug)]
pub enum Event { Bar(MarketBar), Funding(Funding) }
impl Event {
    fn timestamp(&self) -> i64 {
        match self { Self::Bar(b) => b.time_ms, Self::Funding(f) => f.time_ms }
    }
}
#[derive(Clone, Debug)]
pub struct Order {
    pub id: String,
    pub instrument: String,
    pub side: Side,
    pub quantity: Decimal,
    pub limit: Option<Decimal>,
}
#[derive(Clone, Debug)]
pub struct Fill {
    pub time_ms: i64,
    pub order_id: String,
    pub instrument: String,
    pub side: Side,
    pub quantity: Decimal,
    pub price: Decimal,
    pub fee: Decimal,
}
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub time_ms: i64,
    pub cash: Decimal,
    pub equity: Decimal,
    pub initial_margin: Decimal,
    pub maintenance_margin: Decimal,
}
#[derive(Clone, Debug)]
pub struct Report {
    pub cash: Decimal,
    pub positions: BTreeMap<String, Decimal>,
    pub fills: Vec<Fill>,
    pub funding: Decimal,
    pub commissions: Decimal,
    pub curve: Vec<Snapshot>,
    pub rejected: Vec<String>,
    pub pending: Vec<Order>,
}
pub trait Strategy {
    fn on_start(&mut self, _account: &AccountView) -> Vec<Order> { Vec::new() }
    fn on_bar(&mut self, bar: &MarketBar, account: &AccountView) -> Vec<Order>;
    fn on_fill(&mut self, _fill: &Fill, _account: &AccountView) -> Vec<Order> { Vec::new() }
    fn on_funding(&mut self, _event: &Funding, _account: &AccountView) -> Vec<Order> { Vec::new() }
    fn on_stop(&mut self, _account: &AccountView) {}
}
#[derive(Clone, Debug)]
pub struct AccountView {
    pub cash: Decimal,
    pub positions: BTreeMap<String, Decimal>,
    pub marks: BTreeMap<String, Decimal>,
    pub equity: Decimal,
}
#[derive(Clone, Debug)]
struct Position { qty: Decimal, average: Decimal }
fn add(a: Decimal, b: Decimal) -> Result<Decimal, String> {
    a.checked_add(b).ok_or_else(|| "decimal overflow".into())
}
fn sub(a: Decimal, b: Decimal) -> Result<Decimal, String> {
    a.checked_sub(b).ok_or_else(|| "decimal overflow".into())
}
fn mul(a: Decimal, b: Decimal) -> Result<Decimal, String> {
    a.checked_mul(b).ok_or_else(|| "decimal overflow".into())
}
fn validation(inst: &Instrument) -> Result<(), String> {
    if inst.id.is_empty() || inst.multiplier <= Decimal::ZERO
        || inst.fee_rate < Decimal::ZERO || inst.fee_rate > Decimal::ONE
        || inst.initial_margin_ratio < Decimal::ZERO
        || inst.initial_margin_ratio > Decimal::ONE
        || inst.maintenance_margin_ratio < Decimal::ZERO
        || inst.maintenance_margin_ratio > inst.initial_margin_ratio {
        return Err("invalid instrument parameters".into());
    }
    if inst.product == Product::LinearPerpetual && inst.initial_margin_ratio == Decimal::ZERO {
        return Err("perpetual requires initial margin".into());
    }
    Ok(())
}
fn account(
    cash: Decimal, positions: &BTreeMap<String, Position>,
    marks: &BTreeMap<String, Decimal>, instruments: &BTreeMap<String, Instrument>,
) -> Result<(AccountView, Decimal, Decimal), String> {
    let mut equity = cash;
    let mut initial = Decimal::ZERO;
    let mut maintenance = Decimal::ZERO;
    let mut quantities = BTreeMap::new();
    for (id, position) in positions {
        let instrument = &instruments[id];
        let price = *marks.get(id).ok_or("missing mark for open position")?;
        let value = mul(mul(position.qty, price)?, instrument.multiplier)?;
        let notional = value.abs();
        match instrument.product {
            Product::Spot | Product::LongOption => { equity = add(equity, value)?; }
            Product::LinearPerpetual => {
                equity = add(equity, mul(mul(position.qty, sub(price, position.average)?)?, instrument.multiplier)?)?;
                initial = add(initial, mul(notional, instrument.initial_margin_ratio)?)?;
                maintenance = add(maintenance, mul(notional, instrument.maintenance_margin_ratio)?)?;
            }
        }
        quantities.insert(id.clone(), position.qty);
    }
    Ok((AccountView {cash, positions: quantities, marks: marks.clone(), equity}, initial, maintenance))
}
fn submit(
    orders: Vec<Order>, pending: &mut Vec<Order>, ids: &mut BTreeSet<String>,
    instruments: &BTreeMap<String, Instrument>, rejected: &mut Vec<String>,
) {
    for order in orders {
        if order.id.is_empty() || order.quantity <= Decimal::ZERO
            || order.limit.is_some_and(|v| v <= Decimal::ZERO)
            || !instruments.contains_key(&order.instrument)
            || !ids.insert(order.id.clone()) {
            rejected.push(order.id);
        } else {
            pending.push(order);
        }
    }
}
/// Stable total ordering: timestamp, bar before funding, instrument name,
/// then original sequence. Identical timestamp callbacks are deterministic.
pub fn replay<S: Strategy>(
    instruments: &[Instrument], events: &[Event], starting_cash: Decimal, strategy: &mut S,
) -> Result<Report, String> {
    if starting_cash <= Decimal::ZERO { return Err("starting cash must be positive".into()); }
    if events.is_empty() { return Err("empty replay".into()); }
    let mut registry = BTreeMap::new();
    for inst in instruments {
        validation(inst)?;
        if registry.insert(inst.id.clone(), inst.clone()).is_some() {
            return Err("duplicate instrument".into());
        }
    }
    let mut timeline = events.iter().enumerate().collect::<Vec<_>>();
    timeline.sort_by(|(ia,a),(ib,b)| {
        let ak = match a { Event::Bar(v) => (0, v.instrument.as_str()), Event::Funding(v) => (1, v.instrument.as_str()) };
        let bk = match b { Event::Bar(v) => (0, v.instrument.as_str()), Event::Funding(v) => (1, v.instrument.as_str()) };
        a.timestamp().cmp(&b.timestamp()).then(ak.cmp(&bk)).then(ia.cmp(ib))
    });
    let mut seen_bar = BTreeSet::new();
    let mut last_bar = BTreeMap::new();
    let mut seen_funding = BTreeSet::new();
    for (_, event) in &timeline {
        match event {
            Event::Bar(b) => {
                if !registry.contains_key(&b.instrument) || b.ohlc.open <= Decimal::ZERO
                    || b.ohlc.close <= Decimal::ZERO || b.ohlc.low <= Decimal::ZERO
                    || b.ohlc.low > b.ohlc.high || b.ohlc.open < b.ohlc.low
                    || b.ohlc.open > b.ohlc.high || b.ohlc.close < b.ohlc.low
                    || b.ohlc.close > b.ohlc.high
                    || !seen_bar.insert((b.time_ms,b.instrument.clone()))
                    || last_bar.get(&b.instrument).is_some_and(|v| *v >= b.time_ms) {
                    return Err("invalid or duplicate bar".into());
                }
                last_bar.insert(b.instrument.clone(), b.time_ms);
            }
            Event::Funding(f) => {
                if !registry.get(&f.instrument).is_some_and(|i| i.product == Product::LinearPerpetual)
                    || f.mark_price <= Decimal::ZERO || !seen_funding.insert((f.time_ms, f.instrument.clone())) {
                    return Err("invalid or duplicate funding".into());
                }
            }
        }
    }
    let mut cash = starting_cash;
    let mut positions: BTreeMap<String, Position> = BTreeMap::new();
    let mut marks = BTreeMap::new();
    let mut pending = Vec::new();
    let mut ids = BTreeSet::new();
    let mut rejected = Vec::new();
    let mut fills = Vec::new();
    let mut funding_total = Decimal::ZERO;
    let mut commissions = Decimal::ZERO;
    let mut curve = Vec::new();
    let view = account(cash, &positions, &marks, &registry)?.0;
    submit(strategy.on_start(&view), &mut pending, &mut ids, &registry, &mut rejected);
    for (_, event) in timeline {
        match event {
            Event::Bar(bar) => {
                // Match only orders submitted strictly before this bar timestamp.
                let instrument = &registry[&bar.instrument];
                let mut remaining = Vec::new();
                let mut matched = Vec::new();
                for order in pending.drain(..) {
                    if order.instrument != bar.instrument {
                        remaining.push(order);
                        continue;
                    }
                    let candidate = WorkingOrder { id: order.id.clone(), side: order.side,
                        limit: order.limit, quantity: order.quantity };
                    if let Some(m) = match_bar(&bar.ohlc, &[candidate]).into_iter().next() {
                        matched.push((order,m.price));
                    } else { remaining.push(order); }
                }
                pending = remaining;
                marks.insert(bar.instrument.clone(), bar.ohlc.open);
                for (order, price) in matched {
                    let previous = positions.get(&bar.instrument).cloned()
                        .unwrap_or(Position { qty: Decimal::ZERO, average: Decimal::ZERO });
                    let signed = if order.side == Side::Buy { order.quantity } else { -order.quantity };
                    let new_qty = add(previous.qty, signed)?;
                    if instrument.product != Product::LinearPerpetual && new_qty < Decimal::ZERO {
                        rejected.push(order.id);
                        continue;
                    }
                    let notional = mul(mul(order.quantity, price)?, instrument.multiplier)?;
                    let fee = mul(notional, instrument.fee_rate)?;
                    let mut next_cash = sub(cash, fee)?;
                    let average;
                    match instrument.product {
                        Product::Spot | Product::LongOption => {
                            next_cash = sub(next_cash, mul(signed, mul(price, instrument.multiplier)?)?)?;
                            average = Decimal::ZERO;
                        }
                        Product::LinearPerpetual => {
                            let closing = if previous.qty.signum() != signed.signum() {
                                previous.qty.abs().min(signed.abs())
                            } else { Decimal::ZERO };
                            let realized = mul(mul(closing, sub(price, previous.average)?)?,
                                previous.qty.signum())?;
                            next_cash = add(next_cash, mul(realized, instrument.multiplier)?)?;
                            average = if new_qty.is_zero() { Decimal::ZERO }
                                else if previous.qty.is_zero() || previous.qty.signum() != new_qty.signum() {
                                    price
                                } else if previous.qty.signum() == signed.signum() {
                                    add(mul(previous.qty.abs(), previous.average)?, mul(signed.abs(), price)?)?
                                        .checked_div(new_qty.abs()).ok_or("decimal overflow")?
                                } else { previous.average };
                        }
                    }
                    let mut candidate = positions.clone();
                    if new_qty.is_zero() { candidate.remove(&bar.instrument); }
                    else {candidate.insert(bar.instrument.clone(), Position {qty: new_qty, average});}
                    let (view, initial, maintenance) = account(next_cash, &candidate, &marks, &registry)?;
                    if view.equity < initial || view.equity < maintenance || view.equity < Decimal::ZERO
                        || (instrument.product != Product::LinearPerpetual && next_cash < Decimal::ZERO) {
                        rejected.push(order.id);
                        continue;
                    }
                    cash = next_cash;
                    positions = candidate;
                    commissions = add(commissions, fee)?;
                    let fill = Fill { time_ms: bar.time_ms, order_id: order.id,
                        instrument: bar.instrument.clone(), side: order.side, quantity: order.quantity, price, fee };
                    fills.push(fill.clone());
                    submit(strategy.on_fill(&fill, &view), &mut pending, &mut ids, &registry, &mut rejected);
                }
                marks.insert(bar.instrument.clone(), bar.ohlc.close);
                let (view, initial, maintenance) = account(cash, &positions, &marks, &registry)?;
                if view.equity < maintenance { return Err("maintenance margin breach: liquidation unavailable".into()); }
                submit(strategy.on_bar(bar, &view), &mut pending, &mut ids, &registry, &mut rejected);
                curve.push(Snapshot {time_ms: bar.time_ms, cash, equity: view.equity,
                    initial_margin: initial, maintenance_margin: maintenance});
            }
            Event::Funding(f) => {
                let qty = positions.get(&f.instrument).map_or(Decimal::ZERO, |p| p.qty);
                let payment = mul(mul(mul(-qty, f.mark_price)?, registry[&f.instrument].multiplier)?, f.rate)?;
                cash = add(cash, payment)?;
                funding_total = add(funding_total, payment)?;
                let (view, initial, maintenance) = account(cash, &positions, &marks, &registry)?;
                if view.equity < maintenance {return Err("maintenance margin breach after funding".into());}
                submit(strategy.on_funding(f, &view), &mut pending, &mut ids, &registry, &mut rejected);
                curve.push(Snapshot { time_ms: f.time_ms, cash, equity: view.equity,
                    initial_margin: initial, maintenance_margin: maintenance});
            }
        }
    }
    let view = account(cash, &positions, &marks, &registry)?.0;
    strategy.on_stop(&view);
    Ok(Report { cash, positions: view.positions, fills, funding: funding_total,
        commissions, curve, rejected, pending })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn d(v: i64) -> Decimal { Decimal::from(v) }
    fn inst(id: &str, product: Product) -> Instrument {
        Instrument { id: id.into(), product, multiplier: Decimal::ONE,
            initial_margin_ratio: Decimal::new(1,1), maintenance_margin_ratio: Decimal::new(5,2),
            fee_rate: Decimal::new(1,3) }
    }
    fn bar(id: &str, time: i64, price: i64) -> Event {
        Event::Bar(MarketBar {instrument: id.into(), time_ms: time,
            ohlc: Ohlc {open:d(price), high:d(price), low:d(price), close:d(price)} })
    }
    struct OneShot { fired: bool }
    impl Strategy for OneShot {
        fn on_bar(&mut self, b: &MarketBar, _: &AccountView) -> Vec<Order> {
            if self.fired { return vec![]; }
            self.fired = true;
            vec![Order {id: "entry".into(), instrument:b.instrument.clone(),
                side:Side::Buy, quantity:Decimal::ONE, limit:None}]
        }
    }
    #[test]
    fn next_bar_execution_and_fees() {
        let mut s=OneShot {fired:false};
        let report=replay(&[inst("S",Product::Spot)],
            &[bar("S",0,100),bar("S",1,110)],d(1000),&mut s).unwrap();
        assert_eq!(report.fills[0].price,d(110));
        assert_eq!(report.commissions,Decimal::new(110,3));
        assert_eq!(report.curve[0].equity,d(1000));
        assert_eq!(report.positions["S"],Decimal::ONE);
    }
    #[test]
    fn funding_and_cross_instrument_marks() {
        let mut s=OneShot {fired:false};
        let mut p=inst("P",Product::LinearPerpetual);
        p.fee_rate=Decimal::ZERO;
        let events=[bar("P",0,100),bar("P",1,100),bar("S",2,200),
            Event::Funding(Funding {time_ms:3,instrument:"P".into(),
                rate:Decimal::new(1,2),mark_price:d(100)})];
        let report=replay(&[p,inst("S",Product::Spot)],&events,d(1000),&mut s).unwrap();
        assert_eq!(report.funding,d(-1));
        assert_eq!(report.curve.last().unwrap().equity,d(999));
    }
    #[test]
    fn duplicate_bars_and_spot_short_are_rejected() {
        let mut s=OneShot {fired:false};
        assert!(replay(&[inst("S",Product::Spot)],&[bar("S",0,1),bar("S",0,2)],d(100),&mut s).is_err());
    }
}

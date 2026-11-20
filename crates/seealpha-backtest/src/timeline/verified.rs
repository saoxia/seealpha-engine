//! Evidence-only multi-product replay. This is additive to the legacy OHLC
//! timeline; an OHLC high/low is never evidence of executable liquidity.
//!
//! Only observed top-of-book quotes can cause TAKING fills. Displayed size is
//! consumed once per quote and shared by all working orders. Resting maker
//! fills, queue position, hidden liquidity and exchange acknowledgements are
//! deliberately not inferred. Every callback order is eligible only at a
//! strictly later timestamp. Trade prints can trigger stops but cannot fill.
//!
//! All contracts share one settlement currency and a conservative cash-margin
//! account. Neither omitted funding records nor omitted expiry settlements
//! imply historical completeness. This module never reports data READY.
use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;

use crate::matching::Side;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OptionRight { Call, Put }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Product {
    Spot,
    LinearPerpetual,
    CashOption {
        underlying: String,
        right: OptionRight,
        strike: Decimal,
        expiry_ms: i64,
    },
}

#[derive(Clone, Debug)]
pub struct Contract {
    pub id: String,
    pub quote_currency: String,
    pub product: Product,
    pub multiplier: Decimal,
    pub initial_margin_ratio: Decimal,
    pub maintenance_margin_ratio: Decimal,
    pub taker_fee_rate: Decimal,
    pub settlement_fee_rate: Decimal,
}

#[derive(Clone, Copy, Debug)]
pub struct Policy {
    pub starting_cash: Decimal,
    pub max_mark_age_ms: i64,
    /// Fixed, adverse taker execution adjustment: 10 bps = 0.10%.
    pub taker_slippage_bps: Decimal,
}

#[derive(Clone, Debug)]
pub struct Quote {
    pub time_ms: i64,
    pub instrument: String,
    pub bid: Decimal,
    pub ask: Decimal,
    pub bid_size: Decimal,
    pub ask_size: Decimal,
}

#[derive(Clone, Debug)]
pub struct Trade {
    pub time_ms: i64,
    pub instrument: String,
    pub price: Decimal,
}

#[derive(Clone, Debug)]
pub struct Mark {
    pub time_ms: i64,
    pub instrument: String,
    pub price: Decimal,
}

#[derive(Clone, Debug)]
pub struct FundingSettlement {
    pub time_ms: i64,
    pub instrument: String,
    /// A rate observation is NOT a settlement. An evidence ID is mandatory.
    pub evidence_id: String,
    pub rate: Decimal,
    pub mark_price: Decimal,
}

#[derive(Clone, Debug)]
pub struct OptionSettlement {
    pub time_ms: i64,
    pub instrument: String,
    /// Provenance of a verified, contract-specific cash settlement.
    pub evidence_id: String,
    pub underlying_settlement_price: Decimal,
}

#[derive(Clone, Debug)]
pub enum Event {
    Quote(Quote),
    Trade(Trade),
    Mark(Mark),
    Funding(FundingSettlement),
    OptionSettlement(OptionSettlement),
}

impl Event {
    pub fn time_ms(&self) -> i64 {
        match self {
            Self::Quote(v) => v.time_ms,
            Self::Trade(v) => v.time_ms,
            Self::Mark(v) => v.time_ms,
            Self::Funding(v) => v.time_ms,
            Self::OptionSettlement(v) => v.time_ms,
        }
    }
    fn key(&self) -> (u8, &str) {
        // Settlement is processed before a same-timestamp quote can fill.
        match self {
            Self::Funding(v) => (0, &v.instrument),
            Self::OptionSettlement(v) => (1, &v.instrument),
            Self::Mark(v) => (2, &v.instrument),
            Self::Trade(v) => (3, &v.instrument),
            Self::Quote(v) => (4, &v.instrument),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrderType {
    Market,
    Limit(Decimal),
    StopMarket(Decimal),
    StopLimit { stop: Decimal, limit: Decimal },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Order {
    pub id: String,
    pub instrument: String,
    pub side: Side,
    pub quantity: Decimal,
    pub kind: OrderType,
}
#[derive(Clone, Debug)]
pub enum Command {
    Submit(Order),
    Cancel(String),
    Replace { old_id: String, new_order: Order },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fill {
    pub time_ms: i64,
    pub order_id: String,
    pub instrument: String,
    pub side: Side,
    pub quantity: Decimal,
    pub price: Decimal,
    pub fee: Decimal,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CashSettlement {
    pub time_ms: i64,
    pub instrument: String,
    pub evidence_id: String,
    pub quantity: Decimal,
    pub gross: Decimal,
    pub fee: Decimal,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Working {
    pub order: Order,
    pub remaining: Decimal,
    pub eligible_after_ms: i64,
    pub stop_triggered_at_ms: Option<i64>,
}
#[derive(Clone, Debug)]
pub struct Account {
    pub time_ms: i64,
    pub cash: Decimal,
    pub equity: Decimal,
    /// Cash plus unrealized perpetual PnL; spot and option marks are excluded
    /// from eligible collateral to avoid cross-product margin fabrication.
    pub margin_collateral: Decimal,
    pub initial_margin: Decimal,
    pub maintenance_margin: Decimal,
    pub positions: BTreeMap<String, Decimal>,
}
#[derive(Clone, Debug)]
pub struct Report {
    pub cash: Decimal,
    pub equity: Decimal,
    pub positions: BTreeMap<String, Decimal>,
    pub fills: Vec<Fill>,
    pub settlements: Vec<CashSettlement>,
    pub funding: Decimal,
    pub commissions: Decimal,
    pub rejected: Vec<String>,
    pub pending: Vec<Working>,
    pub curve: Vec<Account>,
    /// A finite event stream cannot prove that all historical funding or
    /// quote intervals have been delivered by the upstream catalog.
    pub historical_coverage_certified: bool,
}

pub trait Strategy {
    fn on_start(&mut self, _account: &Account) -> Vec<Command> { Vec::new() }
    fn on_quote(&mut self, _quote: &Quote, _account: &Account) -> Vec<Command> { Vec::new() }
    fn on_trade(&mut self, _trade: &Trade, _account: &Account) -> Vec<Command> { Vec::new() }
    fn on_fill(&mut self, _fill: &Fill, _account: &Account) -> Vec<Command> { Vec::new() }
    fn on_funding(&mut self, _funding: &FundingSettlement, _account: &Account) -> Vec<Command> { Vec::new() }
    fn on_option_settlement(&mut self, _settlement: &CashSettlement, _account: &Account) -> Vec<Command> { Vec::new() }
    fn on_stop(&mut self, _account: &Account) {}
}

#[derive(Clone, Copy, Debug)]
struct Position { qty: Decimal, average: Decimal }
#[derive(Clone, Copy, Debug)]
struct Price { value: Decimal, at_ms: i64 }
struct Ledger {
    cash: Decimal,
    positions: BTreeMap<String, Position>,
    marks: BTreeMap<String, Price>,
    pending: Vec<Working>,
    ever_used_ids: BTreeSet<String>,
    rejected: Vec<String>,
    fills: Vec<Fill>,
    settlements: Vec<CashSettlement>,
    settled_options: BTreeSet<String>,
    funding_total: Decimal,
    fees: Decimal,
}

fn add(a: Decimal, b: Decimal) -> Result<Decimal, String> {
    a.checked_add(b).ok_or_else(|| "decimal addition overflow".into())
}
fn sub(a: Decimal, b: Decimal) -> Result<Decimal, String> {
    a.checked_sub(b).ok_or_else(|| "decimal subtraction overflow".into())
}
fn mul(a: Decimal, b: Decimal) -> Result<Decimal, String> {
    a.checked_mul(b).ok_or_else(|| "decimal multiplication overflow".into())
}
fn valid_rate(rate: Decimal) -> bool {
    rate >= Decimal::ZERO && rate <= Decimal::ONE
}
fn validate_contract(c: &Contract, currency: &str) -> Result<(), String> {
    if c.id.is_empty() || c.quote_currency != currency || c.multiplier <= Decimal::ZERO
        || !valid_rate(c.taker_fee_rate) || !valid_rate(c.settlement_fee_rate)
        || !valid_rate(c.initial_margin_ratio) || !valid_rate(c.maintenance_margin_ratio)
        || c.maintenance_margin_ratio > c.initial_margin_ratio {
        return Err("invalid contract or mixed settlement currency".into());
    }
    match &c.product {
        Product::Spot => {}
        Product::LinearPerpetual if c.initial_margin_ratio > Decimal::ZERO => {}
        Product::LinearPerpetual => return Err("perpetual requires positive initial margin".into()),
        Product::CashOption { underlying, strike, .. } if underlying.is_empty() || *strike <= Decimal::ZERO => {
            return Err("invalid option definition".into());
        }
        Product::CashOption { .. } => {}
    }
    Ok(())
}
fn order_valid(order: &Order, contracts: &BTreeMap<String, Contract>, now: i64,
    settled: &BTreeSet<String>) -> bool {
    let Some(c) = contracts.get(&order.instrument) else { return false; };
    if order.id.is_empty() || order.quantity <= Decimal::ZERO || settled.contains(&order.instrument) {
        return false;
    }
    if let Product::CashOption { expiry_ms, .. } = &c.product {
        if now >= *expiry_ms { return false; }
    }
    match &order.kind {
        OrderType::Market => true,
        OrderType::Limit(v) | OrderType::StopMarket(v) => *v > Decimal::ZERO,
        OrderType::StopLimit { stop, limit } => *stop > Decimal::ZERO && *limit > Decimal::ZERO,
    }
}
fn apply_commands(commands: Vec<Command>, at_ms: i64, state: &mut Ledger,
    contracts: &BTreeMap<String, Contract>) {
    for command in commands {
        match command {
            Command::Submit(order) => {
                if !order_valid(&order, contracts, at_ms, &state.settled_options)
                    || !state.ever_used_ids.insert(order.id.clone()) {
                    state.rejected.push(order.id);
                    continue;
                }
                state.pending.push(Working {
                    remaining: order.quantity, order, eligible_after_ms: at_ms,
                    stop_triggered_at_ms: None,
                });
            }
            Command::Cancel(id) => {
                if let Some(index) = state.pending.iter().position(|p| p.order.id == id) {
                    state.pending.remove(index);
                } else {
                    state.rejected.push(format!("cancel:{id}"));
                }
            }
            Command::Replace { old_id, new_order } => {
                let index = state.pending.iter().position(|p| p.order.id == old_id);
                if index.is_none() || !order_valid(&new_order, contracts, at_ms, &state.settled_options)
                    || new_order.id == old_id || state.ever_used_ids.contains(&new_order.id)
                    || index.is_some_and(|i| state.pending[i].order.instrument != new_order.instrument) {
                    state.rejected.push(format!("replace:{old_id}:{}", new_order.id));
                    continue;
                }
                let index = index.expect("checked working order");
                state.ever_used_ids.insert(new_order.id.clone());
                state.pending[index] = Working {
                    remaining: new_order.quantity, order: new_order, eligible_after_ms: at_ms,
                    stop_triggered_at_ms: None,
                };
            }
        }
    }
}

fn account(at_ms: i64, state: &Ledger, contracts: &BTreeMap<String, Contract>,
    max_mark_age_ms: i64) -> Result<Account, String> {
    let mut equity = state.cash;
    let mut collateral = state.cash;
    let mut initial = Decimal::ZERO;
    let mut maintenance = Decimal::ZERO;
    let mut quantities = BTreeMap::new();
    for (id, position) in &state.positions {
        if position.qty.is_zero() { continue; }
        let mark = state.marks.get(id).ok_or("open position has no historical mark")?;
        if mark.at_ms > at_ms || at_ms.saturating_sub(mark.at_ms) > max_mark_age_ms {
            return Err(format!("missing or stale mark for {id}"));
        }
        let c = &contracts[id];
        let marked_notional = mul(mul(position.qty, mark.value)?, c.multiplier)?;
        match &c.product {
            Product::Spot | Product::CashOption { .. } => {
                equity = add(equity, marked_notional)?;
            }
            Product::LinearPerpetual => {
                let pnl = mul(mul(position.qty, sub(mark.value, position.average)?)?, c.multiplier)?;
                equity = add(equity, pnl)?;
                collateral = add(collateral, pnl)?;
                let gross = marked_notional.abs();
                initial = add(initial, mul(gross, c.initial_margin_ratio)?)?;
                maintenance = add(maintenance, mul(gross, c.maintenance_margin_ratio)?)?;
            }
        }
        quantities.insert(id.clone(), position.qty);
    }
    Ok(Account { time_ms: at_ms, cash: state.cash, equity,
        margin_collateral: collateral, initial_margin: initial,
        maintenance_margin: maintenance, positions: quantities })
}

fn update_position(previous: Position, side: Side, qty: Decimal, price: Decimal,
    product: &Product, multiplier: Decimal, cash: Decimal) -> Result<(Position, Decimal), String> {
    let signed = if side == Side::Buy { qty } else { -qty };
    let next_qty = add(previous.qty, signed)?;
    if !matches!(product, Product::LinearPerpetual) && next_qty < Decimal::ZERO {
        return Err("long-only spot/option sell exceeds holdings".into());
    }
    match product {
        Product::Spot | Product::CashOption { .. } => {
            let next_cash = sub(cash, mul(mul(signed, price)?, multiplier)?)?;
            Ok((Position {qty: next_qty, average: Decimal::ZERO}, next_cash))
        }
        Product::LinearPerpetual => {
            let closing = if previous.qty.is_sign_negative() != signed.is_sign_negative() {
                previous.qty.abs().min(signed.abs())
            } else { Decimal::ZERO };
            let realized = mul(mul(closing, sub(price, previous.average)?)?,
                previous.qty.signum())?;
            let next_cash = add(cash, mul(realized, multiplier)?)?;
            let average = if next_qty.is_zero() {Decimal::ZERO}
                else if previous.qty.is_zero() || previous.qty.is_sign_negative() != next_qty.is_sign_negative() {
                    price
                } else if previous.qty.is_sign_negative() == signed.is_sign_negative() {
                    add(mul(previous.qty.abs(), previous.average)?, mul(qty, price)?)?
                        .checked_div(next_qty.abs()).ok_or("average entry overflow")?
                } else {previous.average};
            Ok((Position {qty: next_qty, average}, next_cash))
        }
    }
}

fn mark_from_quote(q: &Quote, position: Option<&Position>) -> Decimal {
    if position.is_some_and(|p| p.qty < Decimal::ZERO) {q.ask} else {q.bid}
}

fn fill_quote(q: &Quote, policy: Policy, state: &mut Ledger,
    contracts: &BTreeMap<String, Contract>) -> Result<Vec<Fill>, String> {
    if let Product::CashOption {expiry_ms, ..} = &contracts[&q.instrument].product {
        if q.time_ms >= *expiry_ms {
            if !state.settled_options.contains(&q.instrument) {
                return Err("expired option requires verified settlement".into());
            }
            return Ok(Vec::new());
        }
    }
    state.marks.insert(q.instrument.clone(), Price {
        value: mark_from_quote(q, state.positions.get(&q.instrument)), at_ms: q.time_ms,
    });
    let mut bid_liquidity = q.bid_size;
    let mut ask_liquidity = q.ask_size;
    let mut produced = Vec::new();
    let mut index = 0;
    while index < state.pending.len() {
        let working = &state.pending[index];
        if working.order.instrument != q.instrument
            || working.eligible_after_ms >= q.time_ms
            || matches!(&working.order.kind, OrderType::StopMarket(_) | OrderType::StopLimit {..})
                && working.stop_triggered_at_ms.is_none()
            || working.stop_triggered_at_ms.is_some_and(|t| t >= q.time_ms)
        {
            index += 1;
            continue;
        }
        let side = working.order.side;
        let available = if side == Side::Buy {ask_liquidity} else {bid_liquidity};
        if available <= Decimal::ZERO { index += 1; continue; }
        let quote_price = if side == Side::Buy {q.ask} else {q.bid};
        let adjustment = mul(quote_price, policy.taker_slippage_bps)?
            .checked_div(Decimal::from(10_000)).ok_or("slippage overflow")?;
        let execution_price = if side == Side::Buy {
            add(quote_price, adjustment)?
        } else { sub(quote_price, adjustment)? };
        let limit = match &working.order.kind {
            OrderType::Limit(v) => Some(*v),
            OrderType::StopLimit {limit, ..} => Some(*limit),
            _ => None,
        };
        if execution_price <= Decimal::ZERO || limit.is_some_and(|v|
            if side == Side::Buy {execution_price > v} else {execution_price < v})
        {
            index += 1;
            continue;
        }
        let quantity = available.min(working.remaining);
        let c = &contracts[&q.instrument];
        let notional = mul(mul(quantity, execution_price)?, c.multiplier)?;
        let fee = mul(notional, c.taker_fee_rate)?;
        let before = state.positions.get(&q.instrument).copied()
            .unwrap_or(Position {qty: Decimal::ZERO, average: Decimal::ZERO});
        let (next, before_fees) = match update_position(
            before, side, quantity, execution_price, &c.product, c.multiplier, state.cash
        ) {
            Ok(v) => v,
            Err(_) => {
                state.rejected.push(state.pending.remove(index).order.id);
                continue;
            }
        };
        let new_cash = sub(before_fees, fee)?;
        let old_cash = state.cash;
        let old_position = state.positions.insert(q.instrument.clone(), next);
        let old_mark = state.marks.insert(q.instrument.clone(), Price {
            value: mark_from_quote(q, Some(&next)), at_ms: q.time_ms,
        });
        state.cash = new_cash;
        let acceptable = account(q.time_ms, state, contracts, policy.max_mark_age_ms)
            .is_ok_and(|a| a.cash >= Decimal::ZERO && a.equity >= Decimal::ZERO
                && a.margin_collateral >= a.initial_margin);
        if !acceptable {
            state.cash = old_cash;
            if let Some(position) = old_position {
                state.positions.insert(q.instrument.clone(), position);
            } else {state.positions.remove(&q.instrument);}
            if let Some(mark) = old_mark {state.marks.insert(q.instrument.clone(), mark);}
            state.rejected.push(state.pending.remove(index).order.id);
            continue;
        }
        if next.qty.is_zero() {state.positions.remove(&q.instrument);}
        state.fees = add(state.fees, fee)?;
        let fill = Fill {time_ms:q.time_ms,order_id:working.order.id.clone(),
            instrument:q.instrument.clone(),side,quantity,price:execution_price,fee};
        state.fills.push(fill.clone());
        produced.push(fill);
        state.pending[index].remaining = sub(state.pending[index].remaining, quantity)?;
        if side == Side::Buy {
            ask_liquidity = sub(ask_liquidity, quantity)?;
        } else {
            bid_liquidity = sub(bid_liquidity, quantity)?;
        }
        if state.pending[index].remaining.is_zero() {
            state.pending.remove(index);
        } else {
            index += 1;
        }
    }
    Ok(produced)
}

fn validate_event(event: &Event, contracts: &BTreeMap<String, Contract>,
    proofs: &mut BTreeSet<String>, seen: &mut BTreeSet<(u8, String, i64)>) -> Result<(), String> {
    let (kind, id) = event.key();
    if !contracts.contains_key(id) || !seen.insert((kind, id.into(), event.time_ms())) {
        return Err("unknown instrument or duplicate event timestamp".into());
    }
    match event {
        Event::Quote(q) if q.bid > Decimal::ZERO && q.ask >= q.bid
            && q.ask_size >= Decimal::ZERO && q.bid_size >= Decimal::ZERO => {}
        Event::Trade(t) if t.price > Decimal::ZERO => {}
        Event::Mark(m) if m.price > Decimal::ZERO => {}
        Event::Funding(f) if matches!(&contracts[id].product, Product::LinearPerpetual)
            && f.mark_price > Decimal::ZERO && f.rate.abs() <= Decimal::ONE
            && !f.evidence_id.is_empty() && proofs.insert(format!("funding:{}",f.evidence_id)) => {}
        Event::OptionSettlement(s) if matches!(&contracts[id].product, Product::CashOption { .. })
            && s.underlying_settlement_price >= Decimal::ZERO && !s.evidence_id.is_empty()
            && proofs.insert(format!("option:{}",s.evidence_id))
            && matches!(&contracts[id].product,Product::CashOption{expiry_ms,..} if s.time_ms == *expiry_ms) => {}
        _ => return Err("invalid historical execution/settlement evidence".into()),
    }
    Ok(())
}

fn check_expired(at_ms: i64, state: &Ledger,
    contracts: &BTreeMap<String, Contract>) -> Result<(), String> {
    for (id, p) in &state.positions {
        if let Product::CashOption {expiry_ms, ..} = &contracts[id].product {
            if at_ms >= *expiry_ms && !p.qty.is_zero() {
                return Err(format!("missing verified expiry settlement for {id}"));
            }
        }
    }
    Ok(())
}

/// Replay once, fully offline. Events are stably ordered by timestamp, event
/// kind and instrument. Quotes alone consume liquidity; option settlements and
/// funding are *only* applied from caller-supplied evidence.
pub fn replay<S: Strategy>(contracts: &[Contract], events: &[Event],
    policy: Policy, strategy: &mut S) -> Result<Report, String> {
    if policy.starting_cash <= Decimal::ZERO || policy.max_mark_age_ms < 0
        || policy.taker_slippage_bps < Decimal::ZERO
        || policy.taker_slippage_bps > Decimal::from(10_000) {
        return Err("invalid replay policy".into());
    }
    if events.is_empty() || contracts.is_empty() {return Err("missing historical evidence".into());}
    let currency = &contracts[0].quote_currency;
    if currency.is_empty() {return Err("missing settlement currency".into());}
    let mut registry = BTreeMap::new();
    for c in contracts {
        validate_contract(c, currency)?;
        if registry.insert(c.id.clone(), c.clone()).is_some() {
            return Err("duplicate contract".into());
        }
    }
    let mut ordered = events.iter().enumerate().collect::<Vec<_>>();
    ordered.sort_by(|(ia, a), (ib, b)| {
        a.time_ms().cmp(&b.time_ms()).then(a.key().cmp(&b.key())).then(ia.cmp(ib))
    });
    let mut seen = BTreeSet::new();
    let mut proofs = BTreeSet::new();
    for (_, e) in &ordered {validate_event(e, &registry, &mut proofs, &mut seen)?;}
    let mut state = Ledger {
        cash: policy.starting_cash, positions: BTreeMap::new(),
        marks:BTreeMap::new(), pending:Vec::new(), ever_used_ids:BTreeSet::new(),
        rejected:Vec::new(), fills:Vec::new(), settlements:Vec::new(),
        settled_options:BTreeSet::new(), funding_total:Decimal::ZERO, fees:Decimal::ZERO,
    };
    let initial = account(i64::MIN, &state, &registry, policy.max_mark_age_ms)?;
    apply_commands(strategy.on_start(&initial), i64::MIN, &mut state, &registry);
    let mut curve = Vec::with_capacity(events.len());
    for (_, event) in ordered {
        let now = event.time_ms();
        if !matches!(event, Event::OptionSettlement(_)) {
            check_expired(now, &state, &registry)?;
        }
        match event {
            Event::Quote(q) => {
                let fills = fill_quote(q, policy, &mut state, &registry)?;
                for fill in &fills {
                    let view = account(now, &state, &registry, policy.max_mark_age_ms)?;
                    apply_commands(strategy.on_fill(fill, &view), now, &mut state, &registry);
                }
                let view = account(now, &state, &registry, policy.max_mark_age_ms)?;
                apply_commands(strategy.on_quote(q, &view), now, &mut state, &registry);
            }
            Event::Trade(t) => {
                for working in &mut state.pending {
                    if working.order.instrument != t.instrument || working.eligible_after_ms >= now
                        || working.stop_triggered_at_ms.is_some() {continue;}
                    let stop = match &working.order.kind {
                        OrderType::StopMarket(v) => Some(*v),
                        OrderType::StopLimit { stop, .. } => Some(*stop),
                        _ => None,
                    };
                    if stop.is_some_and(|v| if working.order.side == Side::Buy {t.price >= v}
                        else {t.price <= v}) {
                        working.stop_triggered_at_ms = Some(now);
                    }
                }
                let view = account(now, &state, &registry, policy.max_mark_age_ms)?;
                apply_commands(strategy.on_trade(t, &view), now, &mut state, &registry);
            }
            Event::Mark(m) => {
                state.marks.insert(m.instrument.clone(), Price {value:m.price,at_ms:now});
            }
            Event::Funding(f) => {
                state.marks.insert(f.instrument.clone(), Price {
                    value:f.mark_price, at_ms:now,
                });
                let qty = state.positions.get(&f.instrument).map_or(Decimal::ZERO, |p| p.qty);
                let cashflow = mul(mul(mul(-qty, f.mark_price)?, registry[&f.instrument].multiplier)?, f.rate)?;
                state.cash = add(state.cash, cashflow)?;
                state.funding_total = add(state.funding_total, cashflow)?;
                let view = account(now, &state, &registry, policy.max_mark_age_ms)?;
                if view.margin_collateral < view.maintenance_margin {
                    return Err("maintenance margin breach after verified funding".into());
                }
                apply_commands(strategy.on_funding(f, &view), now, &mut state, &registry);
            }
            Event::OptionSettlement(s) => {
                state.settled_options.insert(s.instrument.clone());
                state.pending.retain(|w| w.order.instrument != s.instrument);
                let c = &registry[&s.instrument];
                let Product::CashOption {right, strike, ..} = &c.product else {
                    return Err("non-option settlement".into());
                };
                let qty = state.positions.remove(&s.instrument).map_or(Decimal::ZERO, |p| p.qty);
                let intrinsic = match right {
                    OptionRight::Call => (s.underlying_settlement_price - *strike).max(Decimal::ZERO),
                    OptionRight::Put => (*strike - s.underlying_settlement_price).max(Decimal::ZERO),
                };
                let gross = mul(mul(qty, intrinsic)?, c.multiplier)?;
                let fee = mul(gross, c.settlement_fee_rate)?;
                state.cash = add(state.cash, sub(gross, fee)?)?;
                state.fees = add(state.fees, fee)?;
                state.marks.remove(&s.instrument);
                let settlement = CashSettlement {time_ms:now,instrument:s.instrument.clone(),
                    evidence_id:s.evidence_id.clone(),quantity:qty,gross,fee};
                state.settlements.push(settlement.clone());
                let view = account(now, &state, &registry, policy.max_mark_age_ms)?;
                apply_commands(strategy.on_option_settlement(&settlement, &view),
                    now, &mut state, &registry);
            }
        }
        let view = account(now, &state, &registry, policy.max_mark_age_ms)?;
        if view.margin_collateral < view.maintenance_margin || view.equity < Decimal::ZERO {
            return Err("maintenance margin breach: liquidation not modeled".into());
        }
        curve.push(view);
    }
    let final_account = curve.last().cloned().ok_or("no account curve")?;
    strategy.on_stop(&final_account);
    Ok(Report {
        cash:state.cash, equity:final_account.equity, positions:final_account.positions,
        fills:state.fills, settlements:state.settlements,
        funding:state.funding_total, commissions:state.fees,
        rejected:state.rejected, pending:state.pending, curve,
        historical_coverage_certified:false,
    })
}

#[cfg(test)]
#[path = "verified_tests.rs"]
mod tests;

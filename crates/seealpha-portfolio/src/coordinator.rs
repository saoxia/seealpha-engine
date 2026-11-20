//! Offline portfolio coordination. No venue I/O or trading authorization.
//! Orders are intents: callers must durably pre-register any external command.
use std::collections::{BTreeMap, BTreeSet};
use rust_decimal::Decimal;
use serde::{Serialize, Deserialize};
use crate::{execution_delta, net_signed_quantity, quantize_toward_zero, MemberTarget, PortfolioError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side { Buy, Sell }
impl Side {
    fn sign(self) -> Decimal { match self { Self::Buy => Decimal::ONE, Self::Sell => -Decimal::ONE } }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VirtualKind {
    Market,
    Limit { price: Decimal },
    StopMarket { trigger: Decimal },
    StopLimit { trigger: Decimal, price: Decimal, triggered: bool },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VirtualOrder {
    pub id: String,
    pub member_id: String,
    pub side: Side,
    pub quantity: Decimal,
    pub filled: Decimal,
    pub kind: VirtualKind,
    pub risk_exit: bool,
    pub armed_at: Option<u64>,
}
impl VirtualOrder {
    pub fn leaves(&self) -> Decimal { self.quantity - self.filled }
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Bar { pub time: u64, pub low: Decimal, pub high: Decimal, pub close: Decimal }
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VirtualFill {
    pub order_id: String,
    pub member_id: String,
    pub side: Side,
    pub quantity: Decimal,
    pub price: Decimal,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Submit { id: String, signed_quantity: Decimal },
    Cancel { id: String },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Flight {
    pub id: String,
    pub signed_requested: Decimal,
    pub filled: Decimal,
    pub cancel_requested: bool,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MemberBook { position: Decimal, explicit: Option<Decimal>, risk_exit: bool }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Coordinator {
    members: BTreeMap<String, MemberBook>,
    orders: BTreeMap<String, VirtualOrder>,
    pub flight: Option<Flight>,
    pub frozen: bool,
    pub freeze_reason: Option<String>,
    denied_at: Option<u64>,
    seen_fills: BTreeMap<String, (String, Decimal)>,
    increment: Decimal,
    risk_limit: Decimal,
}
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CoordinatorError {
    #[error("invalid quantity, price, or identifier")] Invalid,
    #[error("duplicate or conflicting order id")] Duplicate,
    #[error("unknown virtual order")] NotFound,
    #[error("risk exit cannot increase member exposure")] RiskExit,
    #[error("external order evidence mismatch; coordinator frozen")] Evidence,
    #[error("coordinator frozen; authoritative reconciliation required")] Frozen,
    #[error(transparent)] Portfolio(#[from] PortfolioError),
}
fn valid_qty(x: Decimal) -> bool { x >= Decimal::ZERO }
impl Coordinator {
    pub fn new(increment: Decimal, risk_limit: Decimal) -> Result<Self, CoordinatorError> {
        if increment <= Decimal::ZERO || risk_limit <= Decimal::ZERO { return Err(CoordinatorError::Invalid); }
        Ok(Self { members: BTreeMap::new(), orders: BTreeMap::new(), flight: None,
            frozen: false, freeze_reason: None, denied_at: None, seen_fills: BTreeMap::new(),
            increment, risk_limit })
    }
    pub fn register(&mut self, id: &str) -> Result<(), CoordinatorError> {
        if id.trim().is_empty() { return Err(CoordinatorError::Invalid); }
        self.members.entry(id.to_owned()).or_default(); Ok(())
    }
    pub fn report_target(&mut self, id: &str, target: Decimal, risk_exit: bool) -> Result<(), CoordinatorError> {
        self.ensure_active()?;
        let member = self.members.get_mut(id).ok_or(CoordinatorError::NotFound)?;
        let old = member.explicit.unwrap_or(member.position);
        if risk_exit && target.abs() > old.abs() { return Err(CoordinatorError::RiskExit); }
        member.explicit = Some(target); member.risk_exit = risk_exit; Ok(())
    }
    pub fn accept_order(&mut self, mut order: VirtualOrder, now: Option<u64>) -> Result<(), CoordinatorError> {
        self.ensure_active()?;
        if order.id.is_empty() || order.member_id.is_empty() || !valid_qty(order.quantity) ||
            order.quantity.is_zero() || !valid_qty(order.filled) || order.filled > order.quantity ||
            self.orders.contains_key(&order.id) || !self.members.contains_key(&order.member_id) {
            return Err(CoordinatorError::Invalid);
        }
        if self.orders.values().any(|o| o.id == order.id) { return Err(CoordinatorError::Duplicate); }
        match &order.kind {
            VirtualKind::Market => (),
            VirtualKind::Limit { price } if *price > Decimal::ZERO => (),
            VirtualKind::StopMarket { trigger } if *trigger > Decimal::ZERO => (),
            VirtualKind::StopLimit { trigger, price, .. } if *trigger > Decimal::ZERO && *price > Decimal::ZERO => (),
            _ => return Err(CoordinatorError::Invalid),
        }
        if !matches!(order.kind, VirtualKind::Market) { order.armed_at = now; }
        self.orders.insert(order.id.clone(), order); Ok(())
    }
    pub fn modify(&mut self, id: &str, quantity: Option<Decimal>, kind: Option<VirtualKind>) -> Result<(), CoordinatorError> {
        self.ensure_active()?;
        let order = self.orders.get_mut(id).ok_or(CoordinatorError::NotFound)?;
        if let Some(q) = quantity {
            if q < order.filled || q <= Decimal::ZERO { return Err(CoordinatorError::Invalid); }
            order.quantity = q;
        }
        if let Some(k) = kind {
            let valid = match &k {
                VirtualKind::Market => true,
                VirtualKind::Limit { price } => *price > Decimal::ZERO,
                VirtualKind::StopMarket { trigger } => *trigger > Decimal::ZERO,
                VirtualKind::StopLimit { trigger, price, .. } => *trigger > Decimal::ZERO && *price > Decimal::ZERO,
            };
            if !valid { return Err(CoordinatorError::Invalid); }
            order.kind = k;
        }
        Ok(())
    }
    pub fn cancel(&mut self, id: &str) -> Result<(), CoordinatorError> {
        self.ensure_active()?;
        self.orders.remove(id).ok_or(CoordinatorError::NotFound)?; Ok(())
    }
    pub fn flatten(&mut self, member: &str) -> Result<(), CoordinatorError> {
        self.ensure_active()?;
        self.orders.retain(|_, o| o.member_id != member);
        let book = self.members.get_mut(member).ok_or(CoordinatorError::NotFound)?;
        book.explicit = Some(Decimal::ZERO); book.risk_exit = true; Ok(())
    }
    fn ensure_active(&self) -> Result<(), CoordinatorError> {
        if self.frozen { Err(CoordinatorError::Frozen) } else { Ok(()) }
    }
    fn actionable(order: &mut VirtualOrder, bar: Option<Bar>) -> bool {
        if order.leaves() <= Decimal::ZERO { return false; }
        if matches!(order.kind, VirtualKind::Market) { return true; }
        let Some(b) = bar else { return false; };
        let Some(armed) = order.armed_at else { order.armed_at = Some(b.time); return false; };
        if b.time <= armed { return false; }
        let buy = order.side == Side::Buy;
        match &mut order.kind {
            VirtualKind::Market => true,
            VirtualKind::Limit {price} => if buy { b.low <= *price } else { b.high >= *price },
            VirtualKind::StopMarket {trigger} => if buy { b.high >= *trigger } else { b.low <= *trigger },
            VirtualKind::StopLimit {trigger,price,triggered} => {
                if !*triggered { *triggered = if buy { b.high >= *trigger } else { b.low <= *trigger }; }
                *triggered && if buy { b.low <= *price } else { b.high >= *price }
            }
        }
    }
    fn targets(&self, active: &BTreeSet<String>) -> Vec<MemberTarget> {
        self.members.iter().map(|(id, book)| {
            let quantity = book.explicit.unwrap_or_else(|| {
                book.position + self.orders.values().filter(|o| o.member_id == *id && active.contains(&o.id))
                    .map(|o| o.side.sign() * o.leaves()).sum::<Decimal>()
            });
            MemberTarget { member_id: id.clone(), signed_quantity: quantity, risk_exit: book.risk_exit }
        }).collect()
    }
    fn allocate(&mut self, side: Side, mut qty: Decimal, price: Decimal, active: &BTreeSet<String>) -> Result<Vec<VirtualFill>, CoordinatorError> {
        let mut fills = Vec::new();
        let ids: Vec<String> = self.orders.iter().filter(|(id,o)| active.contains(*id) && o.side == side)
            .map(|(id,_)| id.clone()).collect();
        let total: Decimal = ids.iter().map(|id| self.orders[id].leaves()).sum();
        if total <= Decimal::ZERO { return Ok(fills); }
        let initial = qty;
        for (index, id) in ids.iter().enumerate() {
            if qty <= Decimal::ZERO { break; }
            let order = &self.orders[id];
            let part = if index + 1 == ids.len() { qty.min(order.leaves()) } else {
                quantize_toward_zero(initial.checked_mul(order.leaves()).ok_or(PortfolioError::Overflow)?
                    .checked_div(total).ok_or(PortfolioError::Overflow)?, self.increment)?.min(qty).min(order.leaves())
            };
            if part <= Decimal::ZERO { continue; }
            let o = self.orders.get_mut(id).expect("id from map");
            o.filled = o.filled.checked_add(part).ok_or(PortfolioError::Overflow)?;
            let m = self.members.get_mut(&o.member_id).expect("registered member");
            m.position = m.position.checked_add(side.sign() * part).ok_or(PortfolioError::Overflow)?;
            fills.push(VirtualFill { order_id: id.clone(), member_id: o.member_id.clone(), side, quantity: part, price });
            qty -= part;
        }
        self.orders.retain(|_, o| o.leaves() > Decimal::ZERO);
        Ok(fills)
    }
    /// Caller invokes this *after all strategies have published for the bar*.
    /// The returned Submit is an intent, not a transmitted order.
    pub fn reconcile(&mut self, bar: Option<Bar>, account_position: Decimal, command_id: &str)
        -> Result<(Vec<VirtualFill>, Option<Command>), CoordinatorError> {
        self.ensure_active()?;
        if let Some(b) = bar {
            if b.low <= Decimal::ZERO || b.high < b.low || b.close <= Decimal::ZERO { return Err(CoordinatorError::Invalid); }
        }
        let mut active = BTreeSet::new();
        for (id, o) in &mut self.orders { if Self::actionable(o, bar) { active.insert(id.clone()); } }
        let buys: Decimal = self.orders.iter().filter(|(id,o)| active.contains(*id) && o.side == Side::Buy).map(|(_,o)|o.leaves()).sum();
        let sells: Decimal = self.orders.iter().filter(|(id,o)| active.contains(*id) && o.side == Side::Sell).map(|(_,o)|o.leaves()).sum();
        let crossed = quantize_toward_zero(buys.min(sells), self.increment)?;
        let mut fills = Vec::new();
        if crossed > Decimal::ZERO {
            let price = bar.ok_or(CoordinatorError::Invalid)?.close;
            fills.extend(self.allocate(Side::Buy, crossed, price, &active)?);
            fills.extend(self.allocate(Side::Sell, crossed, price, &active)?);
        }
        active.retain(|id| self.orders.contains_key(id));
        let targets = self.targets(&active);
        let net = net_signed_quantity(&targets)?;
        // No clipping of offsetting member targets; gross member risk must be checked separately.
        if net.abs() > self.risk_limit { return Err(CoordinatorError::RiskExit); }
        let need = quantize_toward_zero(execution_delta(net,account_position)?,self.increment)?;
        if let Some(flight) = &mut self.flight {
            let unfilled = flight.signed_requested - flight.signed_requested.signum() * flight.filled;
            let disagree = need == Decimal::ZERO || need.signum() != unfilled.signum() || need.abs() < unfilled.abs();
            if disagree && targets.iter().any(|t| t.risk_exit) && !flight.cancel_requested {
                flight.cancel_requested = true;
                return Ok((fills, Some(Command::Cancel { id: flight.id.clone() })));
            }
            return Ok((fills,None));
        }
        if need == Decimal::ZERO || bar.is_some_and(|b|self.denied_at==Some(b.time)) { return Ok((fills,None)); }
        if command_id.trim().is_empty() { return Err(CoordinatorError::Invalid); }
        self.flight = Some(Flight { id: command_id.into(), signed_requested: need, filled: Decimal::ZERO, cancel_requested: false });
        Ok((fills,Some(Command::Submit { id: command_id.into(), signed_quantity: need })))
    }
    /// A venue fill is uniquely identified by event id. Never infer fill from acceptance.
    pub fn apply_venue_fill(&mut self, order_id: &str, event_id: &str, cumulative: Decimal,
        price: Decimal, bar: Option<Bar>) -> Result<Vec<VirtualFill>, CoordinatorError> {
        self.ensure_active()?;
        if event_id.is_empty() || price <= Decimal::ZERO || cumulative < Decimal::ZERO { return Err(CoordinatorError::Invalid); }
        if let Some((old_order, old_cumulative)) = self.seen_fills.get(event_id) {
            if old_order == order_id && *old_cumulative == cumulative { return Ok(vec![]); }
            self.freeze("conflicting duplicate venue fill");
            return Err(CoordinatorError::Evidence);
        }
        let flight = self.flight.as_ref().ok_or(CoordinatorError::Evidence)?;
        if flight.id != order_id || cumulative < flight.filled || cumulative > flight.signed_requested.abs() {
            self.freeze("venue fill evidence mismatch");
            return Err(CoordinatorError::Evidence);
        }
        let delta = cumulative - flight.filled;
        let side = if flight.signed_requested > Decimal::ZERO { Side::Buy } else { Side::Sell };
        let mut active = BTreeSet::new();
        for (id,o) in &mut self.orders {
            if Self::actionable(o,bar) { active.insert(id.clone()); }
        }
        let fills = self.allocate(side, delta, price, &active)?;
        self.seen_fills.insert(event_id.into(), (order_id.into(), cumulative));
        if let Some(f) = &mut self.flight { f.filled = cumulative; }
        if cumulative == self.flight.as_ref().expect("flight").signed_requested.abs() { self.flight = None; }
        Ok(fills)
    }
    pub fn confirm_terminal(&mut self, id: &str, canceled: bool, denied_at: Option<u64>) -> Result<(), CoordinatorError> {
        self.ensure_active()?;
        if self.flight.as_ref().is_none_or(|f| f.id != id) { self.freeze("unexpected terminal order"); return Err(CoordinatorError::Evidence); }
        if !canceled && self.flight.as_ref().is_some_and(|f| f.filled < f.signed_requested.abs()) {
            self.freeze("unverified terminal remainder"); return Err(CoordinatorError::Evidence);
        }
        self.flight = None; self.denied_at = denied_at; Ok(())
    }
    pub fn freeze(&mut self, reason: &str) { self.frozen = true; self.freeze_reason = Some(reason.into()); }
    /// Recovered state must be validated and cannot produce automatic retransmission.
    pub fn validate_restored(&self) -> Result<(), CoordinatorError> {
        if self.increment <= Decimal::ZERO || self.risk_limit <= Decimal::ZERO {
            return Err(CoordinatorError::Invalid);
        }
        for (key, order) in &self.orders {
            if key != &order.id || !self.members.contains_key(&order.member_id) ||
                order.quantity <= Decimal::ZERO || order.filled < Decimal::ZERO ||
                order.filled >= order.quantity {
                return Err(CoordinatorError::Invalid);
            }
        }
        if let Some(flight) = &self.flight {
            if flight.id.is_empty() || flight.signed_requested == Decimal::ZERO ||
                flight.filled < Decimal::ZERO || flight.filled > flight.signed_requested.abs() {
                return Err(CoordinatorError::Invalid);
            }
        }
        if self.frozen != self.freeze_reason.is_some() {
            return Err(CoordinatorError::Invalid);
        }
        Ok(())
    }
    /// Restores state without submitting; unresolved in-flight commands remain in flight.
    pub fn restore(snapshot: Self) -> Self { snapshot }
    pub fn virtual_order(&self, id: &str) -> Option<&VirtualOrder> { self.orders.get(id) }
    pub fn member_position(&self, id: &str) -> Option<Decimal> { self.members.get(id).map(|m|m.position) }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn d(x: i64)->Decimal { Decimal::from(x) }
    fn order(id: &str, member: &str, side: Side, q: i64)->VirtualOrder {
        VirtualOrder {id:id.into(),member_id:member.into(),side,quantity:d(q),filled:Decimal::ZERO,
            kind:VirtualKind::Market,risk_exit:false,armed_at:None}
    }
    fn bar()->Bar {Bar {time:2,low:d(9),high:d(11),close:d(10)}}
    #[test] fn internal_cross_has_no_external_order() {
        let mut c=Coordinator::new(d(1),d(10)).unwrap(); c.register("a").unwrap(); c.register("b").unwrap();
        c.accept_order(order("buy","a",Side::Buy,3),None).unwrap();
        c.accept_order(order("sell","b",Side::Sell,2),None).unwrap();
        let (fills,command)=c.reconcile(Some(bar()),Decimal::ZERO,"request1").unwrap();
        assert_eq!(fills.iter().map(|f|f.quantity).sum::<Decimal>(),d(4));
        assert_eq!(command,Some(Command::Submit{id:"request1".into(),signed_quantity:d(1)}));
        assert_eq!(c.member_position("a"),Some(d(2)));
        assert_eq!(c.member_position("b"),Some(d(-2)));
    }
    #[test] fn duplicate_fill_is_idempotent_and_flight_blocks_resubmit() {
        let mut c=Coordinator::new(d(1),d(20)).unwrap(); c.register("a").unwrap();
        c.accept_order(order("v","a",Side::Buy,3),None).unwrap();
        let (_,submit)=c.reconcile(Some(bar()),Decimal::ZERO,"external").unwrap(); assert!(submit.is_some());
        c.apply_venue_fill("external","f1",d(1),d(10),Some(bar())).unwrap();
        assert!(c.apply_venue_fill("external","f1",d(1),d(10),Some(bar())).unwrap().is_empty());
        assert_eq!(c.reconcile(Some(bar()),d(1),"newid").unwrap().1,None);
        let restored=Coordinator::restore(c); assert!(restored.flight.is_some());
    }
    #[test] fn stop_cannot_trigger_on_arming_bar() {
        let mut c=Coordinator::new(d(1),d(20)).unwrap();c.register("a").unwrap();
        let mut o=order("s","a",Side::Buy,1);o.kind=VirtualKind::StopMarket{trigger:d(10)};
        c.accept_order(o,Some(2)).unwrap();
        assert!(c.reconcile(Some(bar()),Decimal::ZERO,"x").unwrap().1.is_none());
        assert!(c.reconcile(Some(Bar{time:3,..bar()}),Decimal::ZERO,"x").unwrap().1.is_some());
    }
}

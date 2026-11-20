//! Order-bound authoritative evidence validation; no order transport.
use std::collections::{BTreeMap, BTreeSet};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use crate::basket::{BasketError, Checkpoint, LegId, State};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundLegOrder {
    pub leg: LegId,
    pub client_order_id: String,
    pub buy: bool,
    pub quantity: Decimal,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundPhase {
    pub request_id: String,
    pub fingerprint: String,
    pub orders: Vec<BoundLegOrder>,
    pub executable: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueOrderStatus {
    Filled, PartiallyFilled, Canceled, Rejected, Expired, Unknown, New,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderEvidence {
    pub leg: LegId,
    pub client_order_id: String,
    pub venue_order_id: String,
    pub execution_id: String,
    pub filled_quantity: Decimal,
    pub status: VenueOrderStatus,
    pub as_of_ns: u64,
}
fn hash(input: &[u8]) -> String {
    let digest = Sha256::digest(input);
    digest.iter().map(|b|format!("{b:02x}")).collect()
}
pub fn bind_phase_orders(checkpoint: &Checkpoint) -> Result<BoundPhase,BasketError> {
    if checkpoint.plan.executable || checkpoint.state != State::AwaitingConfirmation {
        return Err(BasketError::State);
    }
    let phase = checkpoint.plan.phases.get(checkpoint.phase_index).ok_or(BasketError::State)?;
    let request = checkpoint.request_id.as_deref().ok_or(BasketError::State)?;
    let fingerprint = crate::journal::plan_fingerprint(&checkpoint.plan)?;
    let mut orders = Vec::new();
    let mut ids = BTreeSet::new();
    for (leg, change) in &phase.changes {
        if change.is_zero() { return Err(BasketError::Evidence); }
        let key = format!("{fingerprint}:{request}:{:?}",leg);
        let id = format!("SA{}",&hash(key.as_bytes())[..30]);
        if !ids.insert(id.clone()) { return Err(BasketError::Evidence); }
        orders.push(BoundLegOrder {
            leg: leg.clone(), client_order_id: id,
            buy: *change > Decimal::ZERO, quantity: change.abs(),
        });
    }
    Ok(BoundPhase { request_id: request.into(), fingerprint, orders, executable: false })
}
/// Return a frozen checkpoint for unknown/canceled/partial orders; no retry.
pub fn reconcile_order_evidence(
    mut checkpoint: Checkpoint, manifest: &BoundPhase,
    evidence: &[OrderEvidence], positions: &BTreeMap<LegId,Decimal>,
    positions_ts_ns: u64, now_ns: u64, max_age_ns: u64,
)->Result<Checkpoint,BasketError>{
    if checkpoint.state != State::AwaitingConfirmation || checkpoint.plan.executable ||
        manifest.executable || *manifest != bind_phase_orders(&checkpoint)? {
        return Err(BasketError::State);
    }
    if now_ns == 0 || max_age_ns == 0 {return Err(BasketError::Evidence);}
    if positions_ts_ns > now_ns || now_ns - positions_ts_ns > max_age_ns {
        checkpoint.freeze("stale authoritative account positions")?;
        return Ok(checkpoint);
    }
    if positions.keys().ne(checkpoint.positions.keys()) ||
       evidence.len()!=manifest.orders.len() { return Err(BasketError::Evidence); }
    let expected:BTreeMap<_,_>=manifest.orders.iter().map(|o|(o.leg.clone(),o)).collect();
    let mut visited=BTreeSet::new();
    let mut venue_ids=BTreeSet::new();
    let mut terminal=BTreeMap::new();
    let mut issue=None;
    for row in evidence {
        let original=expected.get(&row.leg).ok_or(BasketError::Evidence)?;
        if row.client_order_id!=original.client_order_id || !visited.insert(&row.leg) ||
            row.venue_order_id.len()>64 || row.execution_id.len()>64 ||
            !row.venue_order_id.bytes().all(|b|b.is_ascii_alphanumeric()) ||
            !row.execution_id.bytes().all(|b|b.is_ascii_alphanumeric()) ||
            row.filled_quantity<Decimal::ZERO || row.filled_quantity>original.quantity ||
            (row.filled_quantity>Decimal::ZERO &&
             (row.venue_order_id.is_empty()||row.execution_id.is_empty())) {
            return Err(BasketError::Evidence);
        }
        if !row.venue_order_id.is_empty() &&
            !venue_ids.insert((row.leg.product.clone(),row.leg.symbol.clone(),row.venue_order_id.clone())) {
            return Err(BasketError::Evidence);
        }
        if row.as_of_ns>now_ns || now_ns-row.as_of_ns>max_age_ns {
            issue=Some("stale exchange order snapshot");
        }
        if (row.status==VenueOrderStatus::Filled)!=(row.filled_quantity==original.quantity) {
            issue=Some("terminal status and cumulative quantity disagree");
        }
        if row.filled_quantity>Decimal::ZERO {
            let id=format!("o{}",&hash(format!("{}:{}:{}",
                row.client_order_id,row.venue_order_id,row.execution_id).as_bytes())[..32]);
            let signed=if original.buy { row.filled_quantity } else { -row.filled_quantity };
            checkpoint.fill(&manifest.request_id,&id,&row.leg,signed)?;
        }
        terminal.insert(row.leg.clone(),row.status==VenueOrderStatus::Filled);
        if row.status!=VenueOrderStatus::Filled { issue=Some("pending, unknown or failed venue order"); }
    }
    // A partially completed multi-leg phase is not an excuse to skip the
    // authoritative account check. The sum of independently witnessed fills
    // must already match the account snapshot even if another leg is unknown.
    let mut observed_from_fills = checkpoint.positions.clone();
    for (leg, filled) in &checkpoint.fills {
        let slot = observed_from_fills.get_mut(leg).ok_or(BasketError::Evidence)?;
        *slot = slot.checked_add(*filled).ok_or(BasketError::Overflow)?;
    }
    if &observed_from_fills != positions {
        checkpoint.freeze("authoritative account positions disagree with cumulative fills")?;
        return Ok(checkpoint);
    }
    if let Some(reason)=issue {
        checkpoint.freeze(reason)?;
        return Ok(checkpoint);
    }
    if checkpoint.confirm(&manifest.request_id,&terminal,positions.clone()).is_err(){
        checkpoint.freeze("verified fills or positions disagree with phase")?;
    }
    Ok(checkpoint)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::basket::{Plan,Phase,Product};
    fn d(x:i64)->Decimal {Decimal::from(x)}
    #[test]
    fn new_ack_is_not_a_fill() {
        let l=LegId{tenant:"t".into(),account:"a".into(),product:Product::Spot,symbol:"ETHUSDT".into()};
        let start=[(l.clone(),d(0))].into();
        let p=Plan {targets:[(l.clone(),d(1))].into(),phases:vec![Phase {
            name:"increase".into(),changes:[(l.clone(),d(1))].into()}],
            gross_notional:d(100),executable:false};
        let mut cp=Checkpoint::new(p,start).unwrap();cp.begin("request").unwrap();
        let manifest=bind_phase_orders(&cp).unwrap();
        let evidence=OrderEvidence{leg:l.clone(),client_order_id:manifest.orders[0].client_order_id.clone(),
            venue_order_id:"123".into(),execution_id:"".into(),filled_quantity:d(0),
            status:VenueOrderStatus::New,as_of_ns:10};
        let after=reconcile_order_evidence(cp,&manifest,&[evidence],&[(l,d(0))].into(),10,10,1).unwrap();
        assert_eq!(after.state,State::Frozen);assert!(after.fills.is_empty());
    }
}

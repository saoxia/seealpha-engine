//! Versioned, canonical, replay-verified checkpoint serialization.
//! The authoritative persistence transaction is provided by seealpha-ledger.
use std::collections::{BTreeMap,BTreeSet};
use rust_decimal::Decimal;
use serde::{Serialize,Deserialize};
use sha2::{Digest,Sha256};
use crate::basket::{BasketError,Checkpoint,JournalEvent,LegId,Plan,Phase};

#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
struct WirePlan {
    targets: Vec<(LegId,Decimal)>,
    phases: Vec<WirePhase>,
    gross_notional: Decimal,
    executable: bool,
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
struct WirePhase { name:String,changes:Vec<(LegId,Decimal)> }
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(tag="kind",rename_all="snake_case",deny_unknown_fields)]
enum WireEvent {
    Begin{request_id:String},
    Fill{request_id:String,event_id:String,leg:LegId,cumulative:Decimal},
    Confirm{request_id:String,positions:Vec<(LegId,Decimal)>},
    Freeze{reason:String},
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
struct JournalRecord {
    version:u32,
    basket_id:String,
    plan:WirePlan,
    initial:Vec<(LegId,Decimal)>,
    events:Vec<WireEvent>,
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
struct Envelope {version:u32,payload:JournalRecord,revision:String}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct StoredCheckpoint {
    pub checkpoint:Checkpoint,
    pub revision:String,
    pub basket_id:String,
}
fn id_valid(id:&str)->bool {
    id.len()>0&&id.len()<=64&&id.bytes().enumerate().all(|(i,b)|
        b.is_ascii_alphanumeric()||(i>0&&(b==b'_'||b==b'-')))
}
fn pairs(rows:&BTreeMap<LegId,Decimal>)->Vec<(LegId,Decimal)>{
    rows.iter().map(|(k,v)|(k.clone(),*v)).collect()
}
fn map(rows:Vec<(LegId,Decimal)>)->Result<BTreeMap<LegId,Decimal>,BasketError>{
    let size=rows.len();
    let result:BTreeMap<_,_>=rows.into_iter().collect();
    if result.len()!=size{return Err(BasketError::Evidence);}
    Ok(result)
}
fn wire_plan(plan:&Plan)->WirePlan {
    WirePlan{targets:pairs(&plan.targets),phases:plan.phases.iter()
        .map(|p|WirePhase{name:p.name.clone(),changes:pairs(&p.changes)}).collect(),
        gross_notional:plan.gross_notional,executable:plan.executable}
}
fn unwire_plan(wire:WirePlan)->Result<Plan,BasketError>{
    Ok(Plan{targets:map(wire.targets)?,phases:wire.phases.into_iter().map(|p|{
        Ok(Phase{name:p.name,changes:map(p.changes)?})
    }).collect::<Result<Vec<_>,BasketError>>()?,
    gross_notional:wire.gross_notional,executable:wire.executable})
}
fn wire_event(event:&JournalEvent)->WireEvent{
    match event {
        JournalEvent::Begin{request_id}=>WireEvent::Begin{request_id:request_id.clone()},
        JournalEvent::Fill{request_id,event_id,leg,cumulative}=>WireEvent::Fill{
            request_id:request_id.clone(),event_id:event_id.clone(),leg:leg.clone(),cumulative:*cumulative},
        JournalEvent::Confirm{request_id,positions}=>WireEvent::Confirm{
            request_id:request_id.clone(),positions:pairs(positions)},
        JournalEvent::Freeze{reason}=>WireEvent::Freeze{reason:reason.clone()},
    }
}
fn unwire_event(event:WireEvent)->Result<JournalEvent,BasketError>{
    Ok(match event {
        WireEvent::Begin{request_id}=>JournalEvent::Begin{request_id},
        WireEvent::Fill{request_id,event_id,leg,cumulative}=>JournalEvent::Fill{
            request_id,event_id,leg,cumulative},
        WireEvent::Confirm{request_id,positions}=>JournalEvent::Confirm{
            request_id,positions:map(positions)?},
        WireEvent::Freeze{reason}=>JournalEvent::Freeze{reason},
    })
}
fn digest(raw:&[u8])->String{
    Sha256::digest(raw).iter().map(|b|format!("{b:02x}")).collect()
}
fn encode<T:Serialize>(v:&T)->Result<Vec<u8>,BasketError>{
    serde_json::to_vec(v).map_err(|_|BasketError::Evidence)
}
/// JSON is a portable state record. SHA-256 detects corruption, not hostile tampering.
pub fn encode_checkpoint(basket_id:&str,checkpoint:&Checkpoint)->Result<StoredEncoded,BasketError>{
    if !id_valid(basket_id)||checkpoint.plan.executable||checkpoint.events.len()>4096 {
        return Err(BasketError::Evidence);
    }
    let replayed=Checkpoint::replay(checkpoint.plan.clone(),checkpoint.initial.clone(),&checkpoint.events)?;
    if replayed!=*checkpoint {return Err(BasketError::Evidence);}
    let payload=JournalRecord{
        version:1,basket_id:basket_id.into(),plan:wire_plan(&checkpoint.plan),
        initial:pairs(&checkpoint.initial),
        events:checkpoint.events.iter().map(wire_event).collect(),
    };
    let revision=digest(&encode(&payload)?);
    let envelope=Envelope{version:1,payload,revision:revision.clone()};
    let mut raw=encode(&envelope)?;
    raw.push(b'\n');
    if raw.len()>1024*1024{return Err(BasketError::Evidence);}
    let json=String::from_utf8(raw).map_err(|_|BasketError::Evidence)?;
    Ok(StoredEncoded{revision,json})
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct StoredEncoded{pub revision:String,pub json:String}
/// Deserialization always replays every journal event. Pending stages stay pending.
pub fn decode_checkpoint(basket_id:&str,raw:&str)->Result<StoredCheckpoint,BasketError>{
    if !id_valid(basket_id)||raw.len()>1024*1024{return Err(BasketError::Evidence);}
    let env:Envelope=serde_json::from_str(raw).map_err(|_|BasketError::Evidence)?;
    if env.version!=1||env.payload.version!=1||env.payload.basket_id!=basket_id||
       digest(&encode(&env.payload)?)!=env.revision{return Err(BasketError::Evidence);}
    let plan=unwire_plan(env.payload.plan)?;
    let initial=map(env.payload.initial)?;
    let events=env.payload.events.into_iter().map(unwire_event)
        .collect::<Result<Vec<_>,BasketError>>()?;
    let checkpoint=Checkpoint::replay(plan,initial,&events)?;
    let normalized=encode_checkpoint(basket_id,&checkpoint)?;
    if normalized.revision!=env.revision||normalized.json.trim_end()!=raw.trim_end(){
        return Err(BasketError::Evidence);
    }
    Ok(StoredCheckpoint{checkpoint,revision:env.revision,basket_id:basket_id.into()})
}
#[cfg(test)]
mod tests{
    use super::*;
    use crate::basket::{Product,State};
    #[test]fn pending_restores_and_replay_is_immutable(){
        let l=LegId{tenant:"t".into(),account:"a".into(),product:Product::Spot,symbol:"ETHUSDT".into()};
        let p=Plan{targets:[(l.clone(),Decimal::ONE)].into(),
            phases:vec![Phase{name:"buy".into(),changes:[(l.clone(),Decimal::ONE)].into()}],
            gross_notional:Decimal::ONE,executable:false};
        let mut cp=Checkpoint::new(p,[(l,Decimal::ZERO)].into()).unwrap();
        cp.begin("req1").unwrap();
        let raw=encode_checkpoint("basket_1",&cp).unwrap();
        let restored=decode_checkpoint("basket_1",&raw.json).unwrap();
        assert_eq!(restored.checkpoint,cp);
        assert_eq!(restored.checkpoint.state,State::AwaitingConfirmation);
        assert!(decode_checkpoint("other",&raw.json).is_err());
    }
}

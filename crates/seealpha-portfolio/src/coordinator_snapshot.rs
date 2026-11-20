//! Portable, integrity-checked coordinator snapshots for native replay.
use serde::{Serialize,Deserialize};
use sha2::{Sha256,Digest};
use crate::coordinator::{Coordinator,CoordinatorError};

#[derive(Debug,Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version:u32,
    scope:String,
    coordinator:Coordinator,
    revision:String,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct EncodedCoordinator { pub json:String, pub revision:String }
fn digest(bytes:&[u8])->String {
    Sha256::digest(bytes).iter().map(|b|format!("{b:02x}")).collect()
}
fn scope_ok(scope:&str)->bool {
    !scope.is_empty()&&scope.len()<=128&&scope.bytes().all(|b|
        b.is_ascii_alphanumeric()||b==b'_'||b==b'-'||b==b':'||b==b'.'||b==b'@'||b==b'/')
}
pub fn encode_coordinator(scope:&str,state:&Coordinator)->Result<EncodedCoordinator,CoordinatorError>{
    if !scope_ok(scope){return Err(CoordinatorError::Invalid);}
    state.validate_restored()?;
    let payload=serde_json::to_vec(&(1_u32,scope,state)).map_err(|_|CoordinatorError::Invalid)?;
    let revision=digest(&payload);
    let json=serde_json::to_string(&Envelope{
        version:1,scope:scope.into(),coordinator:state.clone(),revision:revision.clone(),
    }).map_err(|_|CoordinatorError::Invalid)?;
    if json.len()>1024*1024{return Err(CoordinatorError::Invalid);}
    Ok(EncodedCoordinator{json,revision})
}
pub fn decode_coordinator(scope:&str,raw:&str)->Result<(Coordinator,String),CoordinatorError>{
    if !scope_ok(scope)||raw.len()>1024*1024{return Err(CoordinatorError::Invalid);}
    let envelope:Envelope=serde_json::from_str(raw).map_err(|_|CoordinatorError::Invalid)?;
    if envelope.version!=1||envelope.scope!=scope{return Err(CoordinatorError::Invalid);}
    let normalized=encode_coordinator(scope,&envelope.coordinator)?;
    if normalized.revision!=envelope.revision||normalized.json!=raw{
        return Err(CoordinatorError::Invalid);
    }
    Ok((envelope.coordinator,envelope.revision))
}
#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    #[test]fn frozen_and_pending_roundtrip_without_order_side_effect(){
        let mut state=Coordinator::new(Decimal::ONE,Decimal::from(100)).unwrap();
        state.register("alpha").unwrap();
        state.freeze("unknown exchange order");
        let record=encode_coordinator("account:ETHUSDT",&state).unwrap();
        let (restored,revision)=decode_coordinator("account:ETHUSDT",&record.json).unwrap();
        assert!(restored.frozen);assert_eq!(revision,record.revision);
        assert!(decode_coordinator("other",&record.json).is_err());
    }
}

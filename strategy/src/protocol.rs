//! Versioned, fail-closed JSON envelope for the optional Python strategy host.
//!
//! A version is negotiated per invocation, not by trusting strategy package
//! metadata. This is a local stdio protocol; it is not yet a remote adapter API.

use crate::{HostResponse, StrategyError};
use serde_json::Value;

/// First explicitly versioned Rust <-> optional Python strategy-host envelope.
pub const STRATEGY_IPC_VERSION: u64 = 1;

/// Reject legacy, future, malformed, or extra-field host responses before any
/// published target can reach the portfolio or ledger.
pub(super) fn decode_host_response(bytes: &[u8]) -> Result<HostResponse, StrategyError> {
    let mut payload: Value = serde_json::from_slice(bytes)
        .map_err(|_| StrategyError::Failed("策略宿主输出无效。".into()))?;
    let object = payload
        .as_object_mut()
        .ok_or_else(|| StrategyError::Failed("策略宿主输出无效。".into()))?;
    if object.remove("protocol_version").as_ref().and_then(Value::as_u64)
        != Some(STRATEGY_IPC_VERSION)
    {
        return Err(StrategyError::Failed("策略宿主协议版本不匹配。".into()));
    }
    serde_json::from_value(payload)
        .map_err(|_| StrategyError::Failed("策略宿主输出无效。".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejects_missing_future_boolean_and_unknown_response_fields() {
        for version in [None, Some(json!(2)), Some(json!(true)), Some(json!("1"))] {
            let mut response = json!({"ok":true,"prepared":{},"publications":[]});
            if let Some(version) = version {
                response["protocol_version"] = version;
            }
            let serialized = serde_json::to_vec(&response).unwrap();
            assert!(decode_host_response(&serialized).is_err());
        }
        let unknown = json!({"protocol_version":1,"ok":true,"prepared":{},"publications":[],"orders":[]});
        assert!(decode_host_response(&serde_json::to_vec(&unknown).unwrap()).is_err());
    }

    #[test]
    fn accepts_v1_envelope_without_changing_target_contract() {
        let response = json!({
            "protocol_version":1,
            "ok":true,
            "prepared":{"symbol":"ETHUSDT","allocation":"1","risk_limit":"1"},
            "publications":[{"symbol":"ETHUSDT","quantity":"1","bar_index":0}]
        });
        let decoded = decode_host_response(&serde_json::to_vec(&response).unwrap()).unwrap();
        assert_eq!(decoded.publications[0].quantity, "1");
    }
}

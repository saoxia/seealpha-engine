//! Research grant signatures shared with the Platform research client.
//!
//! The engine verifies grants. It does not expose a route that signs them.
mod scope;
pub use scope::{
    ResearchContext, SnapshotAccess,
    authorize_operation, authorize_snapshot, authorize_snapshot_request, verify_snapshot_bytes,
};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use serde_json::{Map, Value};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use thiserror::Error;

const AUDIENCE: &str = "seealpha-engine-research";
const OPS: &[&str] = &[
    "datasets:read",
    "snapshots:create",
    "snapshots:read",
];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ResearchError {
    #[error("Research authorization rejected")]
    Rejected,
    #[error("Research operation outside granted scope")]
    Scope,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub tenant_id: String,
    pub task_id: String,
    pub ops: Vec<String>,
    pub datasets: Vec<String>,
    pub strategies: Vec<String>,
    pub not_before: i64,
    pub expires_at: i64,
}

pub fn sign_research_grant(key: &str, claims: &Value) -> Result<String, ResearchError> {
    if key.len() < 32 {
        return Err(ResearchError::Rejected);
    }
    let payload = canonical_payload(claims)?;
    let _ = parse_claims(claims, claims_now(claims)?)?;
    let signature = sign(key, &payload)?;
    Ok(format!("{payload}.{signature}"))
}

pub fn verify_research_grant(key: &str, token: &str, now: i64) -> Result<Grant, ResearchError> {
    if key.len() < 32 || token.len() > 4096 {
        return Err(ResearchError::Rejected);
    }
    let Some((payload, signature)) = token.split_once('.') else {
        return Err(ResearchError::Rejected);
    };
    if payload.is_empty() || signature.is_empty() {
        return Err(ResearchError::Rejected);
    }
    let expected = sign(key, payload)?;
    if expected.len() != signature.len()
        || !bool::from(expected.as_bytes().ct_eq(signature.as_bytes()))
    {
        return Err(ResearchError::Rejected);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| ResearchError::Rejected)?;
    if !bytes.is_ascii() {
        return Err(ResearchError::Rejected);
    }
    let claims: Value = serde_json::from_slice(&bytes).map_err(|_| ResearchError::Rejected)?;
    parse_claims(&claims, now)
}

pub fn require_op(
    grant: &Grant,
    operation: &str,
    dataset: Option<&str>,
    strategy: Option<&str>,
) -> Result<(), ResearchError> {
    if !grant.ops.iter().any(|item| item == operation) {
        return Err(ResearchError::Scope);
    }
    if let Some(dataset) = dataset
        && !grant.datasets.iter().any(|item| item == dataset)
    {
        return Err(ResearchError::Scope);
    }
    if let Some(strategy) = strategy
        && !grant.strategies.iter().any(|item| item == strategy)
    {
        return Err(ResearchError::Scope);
    }
    Ok(())
}

fn sign(key: &str, payload: &str) -> Result<String, ResearchError> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key.as_bytes()).map_err(|_| ResearchError::Rejected)?;
    mac.update(payload.as_bytes());
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

fn canonical_payload(claims: &Value) -> Result<String, ResearchError> {
    let text = serde_json::to_string(claims).map_err(|_| ResearchError::Rejected)?;
    if !text.is_ascii() {
        return Err(ResearchError::Rejected);
    }
    Ok(URL_SAFE_NO_PAD.encode(text.as_bytes()))
}

fn claims_now(claims: &Value) -> Result<i64, ResearchError> {
    claims
        .get("not_before")
        .and_then(Value::as_i64)
        .ok_or(ResearchError::Rejected)
}

fn parse_claims(claims: &Value, now: i64) -> Result<Grant, ResearchError> {
    let object = claims.as_object().ok_or(ResearchError::Rejected)?;
    let expected = [
        "version",
        "audience",
        "tenant_id",
        "task_id",
        "ops",
        "datasets",
        "strategies",
        "not_before",
        "expires_at",
    ];
    if object.len() != expected.len() || expected.iter().any(|key| !object.contains_key(*key)) {
        return Err(ResearchError::Rejected);
    }
    if object.get("version").and_then(Value::as_i64) != Some(1) {
        return Err(ResearchError::Rejected);
    }
    if object.get("audience").and_then(Value::as_str) != Some(AUDIENCE) {
        return Err(ResearchError::Rejected);
    }
    let tenant_id = required_id(object, "tenant_id")?;
    let task_id = required_id(object, "task_id")?;
    let ops = string_list(object, "ops", true)?;
    let datasets = string_list(object, "datasets", false)?;
    let strategies = string_list(object, "strategies", false)?;
    let not_before = integer(object, "not_before")?;
    let expires_at = integer(object, "expires_at")?;
    if !(expires_at > not_before
        && expires_at
            .checked_sub(not_before)
            .is_some_and(|duration| duration <= 3600)
        && not_before <= now
        && expires_at > now)
    {
        return Err(ResearchError::Rejected);
    }
    Ok(Grant {
        tenant_id,
        task_id,
        ops,
        datasets,
        strategies,
        not_before,
        expires_at,
    })
}

fn required_id(object: &Map<String, Value>, key: &str) -> Result<String, ResearchError> {
    let value = object
        .get(key)
        .and_then(Value::as_str)
        .ok_or(ResearchError::Rejected)?;
    if !identifier(value) {
        return Err(ResearchError::Rejected);
    }
    Ok(value.to_string())
}

fn integer(object: &Map<String, Value>, key: &str) -> Result<i64, ResearchError> {
    object
        .get(key)
        .and_then(Value::as_i64)
        .ok_or(ResearchError::Rejected)
}

fn string_list(
    object: &Map<String, Value>,
    key: &str,
    ops: bool,
) -> Result<Vec<String>, ResearchError> {
    let values = object
        .get(key)
        .and_then(Value::as_array)
        .ok_or(ResearchError::Rejected)?;
    if values.len() > 100 {
        return Err(ResearchError::Rejected);
    }
    let mut items = Vec::with_capacity(values.len());
    for value in values {
        let text = value.as_str().ok_or(ResearchError::Rejected)?;
        if ops {
            if !OPS.contains(&text) {
                return Err(ResearchError::Rejected);
            }
        } else if !identifier(text) {
            return Err(ResearchError::Rejected);
        }
        if items.iter().any(|existing: &String| existing == text) {
            return Err(ResearchError::Rejected);
        }
        items.push(text.to_string());
    }
    Ok(items)
}

fn identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > 128 || value.contains("..") {
        return false;
    }
    bytes[0].is_ascii_alphanumeric()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const KEY: &str = "research-signing-key-0123456789abcdef";
    const TOKEN: &str = "eyJhdWRpZW5jZSI6InNlZWFscGhhLWVuZ2luZS1yZXNlYXJjaCIsImRhdGFzZXRzIjpbImV0aHVzZHQiXSwiZXhwaXJlc19hdCI6MTcwMDAwMDkwMCwibm90X2JlZm9yZSI6MTcwMDAwMDAwMCwib3BzIjpbImRhdGFzZXRzOnJlYWQiXSwic3RyYXRlZ2llcyI6W10sInRhc2tfaWQiOiJ0YXNrLTEiLCJ0ZW5hbnRfaWQiOiI5ODc2NTQzMjEiLCJ2ZXJzaW9uIjoxfQ.7Rpfq3kBh_01URfqCvesqVGQK5MjiNdu0dJXHSM1jLY";

    #[test]
    fn verifies_a_python_signed_grant_and_rejects_tampering() {
        let grant = verify_research_grant(KEY, TOKEN, 1_700_000_400).unwrap();
        assert_eq!(grant.tenant_id, "987654321");
        require_op(&grant, "datasets:read", Some("ethusdt"), None).unwrap();
        assert_eq!(
            require_op(&grant, "snapshots:create", None, None),
            Err(ResearchError::Scope)
        );
        let mut forged = TOKEN.to_string();
        forged.pop();
        forged.push('A');
        assert_eq!(
            verify_research_grant(KEY, &forged, 1_700_000_400),
            Err(ResearchError::Rejected)
        );
    }

    #[test]
    fn malicious_integer_boundaries_never_panic_or_authorize() {
        let mut claims = json!({
            "audience": AUDIENCE,
            "datasets": [],
            "expires_at": i64::MAX,
            "not_before": i64::MIN,
            "ops": ["datasets:read"],
            "strategies": [],
            "task_id": "task-1",
            "tenant_id": "tenant-1",
            "version": 1
        });
        assert_eq!(parse_claims(&claims, 0), Err(ResearchError::Rejected));
        claims["not_before"] = json!(100);
        claims["expires_at"] = json!(100);
        assert_eq!(parse_claims(&claims, 100), Err(ResearchError::Rejected));
        claims["expires_at"] = json!(3_701);
        assert_eq!(parse_claims(&claims, 100), Err(ResearchError::Rejected));
        claims["expires_at"] = json!(3_700);
        assert!(parse_claims(&claims, 100).is_ok());
        claims["ops"] = json!(["datasets:read", "datasets:read"]);
        assert_eq!(parse_claims(&claims, 100), Err(ResearchError::Rejected));
        claims["ops"] = json!(["execute:order"]);
        assert_eq!(parse_claims(&claims, 100), Err(ResearchError::Rejected));
    }

    #[test]
    fn signed_grant_round_trips() {
        let claims = json!({
            "audience": AUDIENCE,
            "datasets": ["ethusdt"],
            "expires_at": 1_700_000_900_i64,
            "not_before": 1_700_000_000_i64,
            "ops": ["datasets:read"],
            "strategies": [],
            "task_id": "task-1",
            "tenant_id": "987654321",
            "version": 1
        });
        let token = sign_research_grant(KEY, &claims).unwrap();
        assert_eq!(token, TOKEN);
    }
}

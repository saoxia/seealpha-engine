//! SeeAlpha strategy admission and read-only target-publication contracts.
//! NautilusTrader should own generic strategy lifecycle and indicators after
//! a tested native-runtime integration. This crate retains SeeAlpha issuance.
pub mod store;
mod issued;
pub mod native_ema;
pub mod native_manifest;
pub mod native_issuance;

pub use issued::{AccountScope, IssuedStrategy, authorize_issued};

use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StrategyError {
    #[error("{0}")]
    Package(String),
    #[error("{0}")]
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageMetadata {
    pub name: String,
    pub version: String,
    pub entry_point: String,
    pub digest: String,
}

#[derive(Debug, Clone)]
pub struct StrategyBar {
    pub open_time_ms: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
}

#[derive(Debug, Clone, Copy)]
pub struct ExecutionInput<'a> {
    pub parameters: &'a Value,
    pub symbol: &'a str,
    pub interval: &'a str,
    pub bars: &'a [StrategyBar],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publication {
    pub symbol: String,
    pub quantity: String,
    pub weight: Option<String>,
    pub bar_index: usize,
}

#[derive(Debug, Clone)]
pub struct StrategyRun {
    pub prepared: Value,
    pub publications: Vec<Publication>,
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex_encode(Sha256::digest(bytes))
}

fn valid_name(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() => {}
        _ => return false,
    }
    let rest = chars.count();
    (1..=40).contains(&(rest + 1))
        && value
            .chars()
            .all(|char| char.is_ascii_lowercase() || char.is_ascii_digit() || char == '_')
}

fn valid_version(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphanumeric() => {}
        _ => return false,
    }
    value.chars().count() <= 40
        && value
            .chars()
            .all(|char| char.is_ascii_alphanumeric() || matches!(char, '.' | '_' | '-'))
}

fn valid_entry(value: &str) -> bool {
    let Some((module, attribute)) = value.split_once(':') else {
        return false;
    };
    !module.is_empty()
        && !attribute.is_empty()
        && module.split('.').all(valid_ident)
        && valid_ident(attribute)
}

fn valid_ident(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => return false,
    }
    chars.all(|char| char.is_ascii_alphanumeric() || char == '_')
}

fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

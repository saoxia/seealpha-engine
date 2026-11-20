//! Bounded, side-effect-free Binance market source policy kernel.
//!
//! This is an offline Rust candidate, NOT yet called by the running collectors.
//! It preserves the existing manifest v1 and source-specific validation rules.
//! No trading, I/O, network subscription, or storage mutation is performed here.
use std::collections::{BTreeMap, BTreeSet};

pub mod protocol;
pub mod ws;

use serde_json::Value;

pub const MAX_MANIFEST_BYTES: usize = 5 * 1024 * 1024;
pub const FUTURES_SPOT_STREAM_CAPACITY: usize = 128;
pub const OPTIONS_STREAM_CAPACITY: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Product {
    FuturesUm,
    Spot,
    Options,
}

impl Product {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "futures_um" => Some(Self::FuturesUm),
            "spot" => Some(Self::Spot),
            "options" => Some(Self::Options),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::FuturesUm => "futures_um",
            Self::Spot => "spot",
            Self::Options => "options",
        }
    }

    pub fn socket_capacity(self) -> usize {
        if self == Self::Options {
            OPTIONS_STREAM_CAPACITY
        } else {
            FUTURES_SPOT_STREAM_CAPACITY
        }
    }
}

#[derive(Debug)]
pub enum ManifestError {
    Oversized,
    Json(serde_json::Error),
    Invalid(&'static str),
}

impl std::fmt::Display for ManifestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Oversized => f.write_str("manifest exceeds 5 MiB"),
            Self::Json(e) => write!(f, "invalid JSON: {e}"),
            Self::Invalid(msg) => f.write_str(msg),
        }
    }
}
impl std::error::Error for ManifestError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LivePolicy {
    pub symbol: String,
    pub base_level: String,
    pub realtime_enabled: bool,
}

fn allowed_base(product: Product, level: &str) -> bool {
    match product {
        Product::Options => matches!(level, "tick" | "1m" | "5m" | "15m"),
        Product::FuturesUm | Product::Spot => matches!(
            level,
            "tick" | "1s" | "1m" | "3m" | "5m" | "15m" | "30m"
                | "1h" | "2h" | "4h" | "6h" | "8h" | "12h"
                | "1d" | "3d" | "1w" | "1M"
        ),
    }
}

fn is_leap(year: u32) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

fn valid_option_date(raw: &str) -> bool {
    if raw.len() != 6 || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    let year = 2000 + raw[0..2].parse::<u32>().unwrap_or(0);
    let month = raw[2..4].parse::<u32>().unwrap_or(0);
    let day = raw[4..6].parse::<u32>().unwrap_or(0);
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(year) => 29,
        2 => 28,
        _ => return false,
    };
    (1..=max_day).contains(&day)
}

/// Replicates the Binance symbol canonicalization used by the current
/// source batch collectors. Scope names like "ALL" are *not* contracts.
pub fn normalize_symbol(raw: &str, product: Product) -> Option<String> {
    let value = raw.trim().to_ascii_uppercase();
    match product {
        Product::Options => {
            let mut fields = value.split('-');
            let underlying = fields.next()?;
            let date = fields.next()?;
            let strike = fields.next()?;
            let right = fields.next()?;
            if fields.next().is_some()
                || underlying.is_empty()
                || !underlying.bytes().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
                || !valid_option_date(date)
                || !matches!(right, "C" | "P")
            {
                return None;
            }
            let mut strike_fields = strike.split('.');
            let integer = strike_fields.next()?;
            let decimal = strike_fields.next();
            if strike_fields.next().is_some()
                || integer.is_empty()
                || !integer.bytes().all(|b| b.is_ascii_digit())
                || decimal.is_some_and(|d| d.is_empty() || !d.bytes().all(|b| b.is_ascii_digit()))
                || !strike.bytes().any(|b| b.is_ascii_digit() && b != b'0')
            {
                return None;
            }
            Some(value)
        }
        Product::FuturesUm | Product::Spot => {
            let symbol: String = value.chars()
                .filter(|&c| !matches!(c, '-' | '/' | '_'))
                .collect();
            if !symbol.bytes().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()) {
                return None;
            }
            if product == Product::Spot {
                if !(3..=30).contains(&symbol.len()) {
                    return None;
                }
            } else if symbol.len() <= 4
                || !(symbol.ends_with("USDT") || symbol.ends_with("USDC"))
            {
                return None;
            }
            Some(symbol)
        }
    }
}

/// Validate the entire v1 source manifest, including disabled policies,
/// before selecting enabled collection tasks. On error, callers must retain
/// the previous committed generation rather than applying a partial update.
pub fn parse_manifest(
    bytes: &[u8],
    product: Product,
) -> Result<Vec<LivePolicy>, ManifestError> {
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(ManifestError::Oversized);
    }
    let document: Value = serde_json::from_slice(bytes).map_err(ManifestError::Json)?;
    if document.get("version").and_then(Value::as_u64) != Some(1) {
        return Err(ManifestError::Invalid("unsupported manifest version"));
    }
    let entries = document
        .get("policies")
        .and_then(Value::as_array)
        .ok_or(ManifestError::Invalid("policies must be an array"))?;
    let mut policies = Vec::with_capacity(entries.len());
    let mut seen = BTreeSet::new();
    for entry in entries {
        let object = entry.as_object()
            .ok_or(ManifestError::Invalid("policy must be an object"))?;
        if object.get("product").and_then(Value::as_str) != Some(product.as_str()) {
            return Err(ManifestError::Invalid("product mismatch"));
        }
        let symbol = object.get("symbol").and_then(Value::as_str)
            .and_then(|value| normalize_symbol(value, product))
            .ok_or(ManifestError::Invalid("invalid Binance symbol"))?;
        if !seen.insert(symbol.clone()) {
            return Err(ManifestError::Invalid("duplicate symbol"));
        }
        let base_level = object.get("base_level").and_then(Value::as_str)
            .ok_or(ManifestError::Invalid("missing base_level"))?;
        if !allowed_base(product, base_level) {
            return Err(ManifestError::Invalid("invalid source base_level"));
        }
        let realtime_enabled = object.get("realtime_enabled").and_then(Value::as_bool)
            .ok_or(ManifestError::Invalid("realtime_enabled must be boolean"))?;
        policies.push(LivePolicy {
            symbol,
            base_level: base_level.to_owned(),
            realtime_enabled,
        });
    }
    Ok(policies)
}

/// Reconciliation from a committed policy generation to another. Identical
/// base levels keep the existing collector, as in the Python source batches;
/// removing/disabling/changing base levels stops the old worker first.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct ReconcilePlan {
    pub stop: Vec<String>,
    pub start: Vec<String>,
    pub keep: Vec<String>,
}

pub fn reconcile(current: &[LivePolicy], next: &[LivePolicy]) -> ReconcilePlan {
    let existing: BTreeMap<&str, &str> = current.iter()
        .filter(|p| p.realtime_enabled)
        .map(|p| (p.symbol.as_str(), p.base_level.as_str()))
        .collect();
    let desired: BTreeMap<&str, &str> = next.iter()
        .filter(|p| p.realtime_enabled)
        .map(|p| (p.symbol.as_str(), p.base_level.as_str()))
        .collect();
    let mut plan = ReconcilePlan::default();
    for (symbol, level) in &existing {
        if desired.get(symbol).copied() != Some(*level) {
            plan.stop.push((*symbol).to_owned());
        } else {
            plan.keep.push((*symbol).to_owned());
        }
    }
    for (symbol, level) in &desired {
        if existing.get(symbol).copied() != Some(*level) {
            plan.start.push((*symbol).to_owned());
        }
    }
    plan
}

/// Deterministic cold-start socket group sizing. Existing live socket
/// assignments are managed separately to avoid reconnection churn.
pub fn cold_start_shards(policies: &[LivePolicy], product: Product) -> Vec<Vec<String>> {
    let mut enabled: Vec<String> = policies.iter().filter(|p| p.realtime_enabled)
        .map(|p| p.symbol.clone()).collect();
    enabled.sort_unstable();
    enabled.chunks(product.socket_capacity())
        .map(|chunk| chunk.to_vec()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(product: &str, policies: &str) -> String {
        format!(r#"{{"version":1,"policies":[{policies}]}}"#)
            .replace("PRODUCT", product)
    }
    fn entry(product: &str, symbol: &str, base: &str, enabled: &str) -> String {
        format!(
            r#"{{"product":"{product}","symbol":"{symbol}","base_level":"{base}","realtime_enabled":{enabled}}}"#
        )
    }
    #[test]
    fn futures_accepts_normalized_contracts_and_disabling() {
        let entries = [
            entry("futures_um", " eth/usdt ", "tick", "true"),
            entry("futures_um", "BTC_USDC", "1m", "false"),
        ].join(",");
        let out = parse_manifest(manifest("futures_um", &entries).as_bytes(), Product::FuturesUm).unwrap();
        assert_eq!(out[0].symbol, "ETHUSDT");
        assert_eq!(out[1].symbol, "BTCUSDC");
        assert_eq!(cold_start_shards(&out, Product::FuturesUm), vec![vec!["ETHUSDT"]]);
    }
    #[test]
    fn spot_is_distinct_and_accepts_non_usdt_symbols() {
        let entries = entry("spot", "eth/btc", "1s", "true");
        let out = parse_manifest(manifest("spot", &entries).as_bytes(), Product::Spot).unwrap();
        assert_eq!(out[0].symbol, "ETHBTC");
        assert!(parse_manifest(manifest("futures_um", &entries).as_bytes(), Product::FuturesUm).is_err());
    }
    #[test]
    fn options_enforce_contract_identity_and_calendar() {
        let entry = entry("options", "btc-241227-100000-c", "5m", "true");
        let out = parse_manifest(manifest("options", &entry).as_bytes(), Product::Options).unwrap();
        assert_eq!(out[0].symbol, "BTC-241227-100000-C");
        for bad in ["BTC-250229-100-C", "BTC-241227-0-C", "BTC-241227-100-X", "ALL"] {
            assert!(normalize_symbol(bad, Product::Options).is_none(), "{bad}");
        }
        assert_eq!(normalize_symbol("BTC-240229-1.5-P", Product::Options).as_deref(), Some("BTC-240229-1.5-P"));
    }
    #[test]
    fn rejects_duplicates_even_if_first_is_disabled() {
        let entries = [
            entry("spot", "eth/btc", "1m", "false"),
            entry("spot", "ETHBTC", "1m", "true"),
        ].join(",");
        assert!(parse_manifest(manifest("spot", &entries).as_bytes(), Product::Spot).is_err());
    }
    #[test]
    fn manifest_is_atomic_and_strict() {
        assert!(parse_manifest(br#"{"version":2,"policies":[]}"#, Product::Spot).is_err());
        assert!(parse_manifest(br#"{"version":1,"policies":{}}"#, Product::Spot).is_err());
        assert!(parse_manifest(br#"{"version":1,"policies":[{"product":"spot","symbol":"BTCUSDT","base_level":"1m","realtime_enabled":1}]}"#, Product::Spot).is_err());
        assert!(matches!(parse_manifest(&vec![b' '; MAX_MANIFEST_BYTES+1], Product::Spot), Err(ManifestError::Oversized)));
        assert!(parse_manifest(br#"{"version":1,"policies":[]}"#, Product::Options).unwrap().is_empty());
    }
    #[test]
    fn source_levels_do_not_cross_products() {
        assert!(parse_manifest(manifest("options", &entry("options","BTC-241227-100-C","1s","true")).as_bytes(), Product::Options).is_err());
        assert!(parse_manifest(manifest("spot", &entry("spot","ETHBTC","auto","true")).as_bytes(), Product::Spot).is_err());
    }
    #[test]
    fn reconciliation_stops_before_restarting_changed_base() {
        let previous = vec![
            LivePolicy { symbol:"ETHUSDT".into(), base_level:"tick".into(), realtime_enabled:true },
            LivePolicy { symbol:"BTCUSDT".into(), base_level:"1m".into(), realtime_enabled:true },
        ];
        let next = vec![
            LivePolicy { symbol:"ETHUSDT".into(), base_level:"1m".into(), realtime_enabled:true },
            LivePolicy { symbol:"BTCUSDT".into(), base_level:"1m".into(), realtime_enabled:false },
            LivePolicy { symbol:"SOLUSDT".into(), base_level:"tick".into(), realtime_enabled:true },
        ];
        assert_eq!(reconcile(&previous,&next), ReconcilePlan {
            stop:vec!["BTCUSDT".into(),"ETHUSDT".into()],
            start:vec!["ETHUSDT".into(),"SOLUSDT".into()],
            keep:vec![],
        });
    }
    #[test]
    fn shard_limits_are_product_specific() {
        let policies:Vec<_> = (0..260).map(|i| LivePolicy {
            symbol:format!("TEST{i:04}USDT"), base_level:"tick".into(), realtime_enabled:true,
        }).collect();
        assert_eq!(cold_start_shards(&policies, Product::FuturesUm).iter().map(Vec::len).collect::<Vec<_>>(), vec![128,128,4]);
        assert_eq!(cold_start_shards(&policies, Product::Spot).iter().map(Vec::len).collect::<Vec<_>>(), vec![128,128,4]);
        assert_eq!(cold_start_shards(&policies, Product::Options).iter().map(Vec::len).collect::<Vec<_>>(), vec![64,64,64,64,4]);
    }
}

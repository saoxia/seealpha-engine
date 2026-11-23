//! Admission groundwork for a *built-in* Rust strategy artifact.
//!
//! No arbitrary uploaded Rust code is executed. This validator only binds an
//! immutable manifest to an existing trusted issuance and account scope.
//! Runtime/HTTP issuance and production Rust execution are NOT enabled by it.

use crate::{AccountScope, IssuedStrategy, PackageMetadata, StrategyError, authorize_issued, sha256_hex};
use serde::Deserialize;

pub const NATIVE_EMA_ENTRY: &str = "native:ema_crossover";
pub const NATIVE_EMA_CODE: &str = "ema_crossover";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeManifest {
    format: u64,
    runtime: String,
    name: String,
    version: String,
    entry_point: String,
    #[serde(default)]
    description: Option<String>,
}

/// Inspect exact artifact bytes without loading executable code. The SHA-256
/// covers the *original bytes*, not a reserialized/canonicalized JSON object.
pub fn inspect_native_manifest(bytes: &[u8]) -> Result<PackageMetadata, StrategyError> {
    if bytes.is_empty() || bytes.len() > 4096 {
        return Err(StrategyError::Package("Rust 策略清单大小无效。".into()));
    }
    let manifest: NativeManifest = serde_json::from_slice(bytes)
        .map_err(|_| StrategyError::Package("Rust 策略清单无效。".into()))?;
    if manifest.format != 1
        || manifest.runtime != "rust-builtin"
        || manifest.name != NATIVE_EMA_CODE
        || manifest.version != "1"
        || manifest.entry_point != NATIVE_EMA_ENTRY
        || manifest.description.as_ref().is_some_and(|d| d.len() > 512)
    {
        return Err(StrategyError::Package("Rust 策略清单未获支持。".into()));
    }
    Ok(PackageMetadata {
        name: manifest.name,
        version: manifest.version,
        entry_point: manifest.entry_point,
        digest: sha256_hex(bytes),
    })
}

/// Bind a supported native manifest to an issuance created by the trusted
/// ledger flow. No issuer identity is inferred from manifest contents.
pub fn authorize_native_manifest(
    bytes: &[u8],
    issued: IssuedStrategy<'_>,
    scope: AccountScope<'_>,
) -> Result<(), StrategyError> {
    let metadata = inspect_native_manifest(bytes)?;
    authorize_issued(issued, scope, &metadata)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &[u8] = br#"{"format":1,"runtime":"rust-builtin","name":"ema_crossover","version":"1","entry_point":"native:ema_crossover","description":"EMA crossover"}"#;

    #[test]
    fn inspect_only_recognizes_known_builtin_and_exact_hash() {
        let metadata = inspect_native_manifest(MANIFEST).unwrap();
        assert_eq!(metadata.digest, sha256_hex(MANIFEST));
        for invalid in [
            br#"{"format":1,"runtime":"python","name":"ema_crossover","version":"1","entry_point":"native:ema_crossover"}"#.as_slice(),
            br#"{"format":1,"runtime":"rust-builtin","name":"untrusted","version":"1","entry_point":"native:ema_crossover"}"#.as_slice(),
            br#"{"format":1,"runtime":"rust-builtin","name":"ema_crossover","version":"1","entry_point":"native:ema_crossover","extra":true}"#.as_slice(),
        ] {
            assert!(inspect_native_manifest(invalid).is_err());
        }
    }

    #[test]
    fn account_and_digest_must_match_trusted_issuance() {
        let digest = sha256_hex(MANIFEST);
        let record = IssuedStrategy {
            account_id: "account-1",
            issuance_id: "issue-1",
            code_key: NATIVE_EMA_CODE,
            version: "1",
            entry_point: NATIVE_EMA_ENTRY,
            package_hash: &digest,
        };
        let scope = AccountScope {
            account_id: "account-1",
        };
        authorize_native_manifest(MANIFEST, record, scope).unwrap();
        assert!(authorize_native_manifest(MANIFEST, record, AccountScope {
            account_id: "different",
            ..scope
        }).is_err());
        assert!(authorize_native_manifest(b"{}", record, scope).is_err());
        assert!(authorize_native_manifest(MANIFEST, IssuedStrategy {
            package_hash: "0",
            ..record
        }, scope).is_err());
    }
}

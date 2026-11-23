//! Immutable issued-strategy identity and account-scope verification.
//! The artifact must be loaded from SeeAlpha's trusted StrategyStore.
use crate::{PackageMetadata, StrategyError};

#[derive(Debug, Clone, Copy)]
pub struct IssuedStrategy<'a> {
    pub account_id: &'a str,
    pub issuance_id: &'a str,
    pub code_key: &'a str,
    pub version: &'a str,
    pub entry_point: &'a str,
    pub package_hash: &'a str,
}

#[derive(Debug, Clone, Copy)]
pub struct AccountScope<'a> {
    pub account_id: &'a str,
}

/// Compare immutable issuance metadata with the verified native artifact.
/// This function is not an issuer or trading authorization.
pub fn authorize_issued(
    issued: IssuedStrategy<'_>,
    caller: AccountScope<'_>,
    metadata: &PackageMetadata,
) -> Result<(), StrategyError> {
    if !valid_identifier(issued.account_id, 40)
        || !valid_identifier(issued.issuance_id, 64)
        || issued.account_id != caller.account_id
        || !super::valid_name(issued.code_key)
        || !super::valid_version(issued.version)
        || !super::valid_entry(issued.entry_point)
        || issued.package_hash.len() != 64
        || !issued.package_hash.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        || issued.code_key != metadata.name
        || issued.version != metadata.version
        || issued.entry_point != metadata.entry_point
        || issued.package_hash != metadata.digest
    {
        return Err(StrategyError::Package("策略包与账户下发授权不一致。".into()));
    }
    Ok(())
}

fn valid_identifier(value: &str, max: usize) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= max
        && !value.contains("..")
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..]
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-' | b'_' | b':'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_issuance_is_bound_to_account_and_exact_manifest() {
        let metadata = PackageMetadata {
            name: "ema_crossover".into(),
            version: "1".into(),
            entry_point: "native:ema_crossover".into(),
            digest: "a".repeat(64),
        };
        let issued = IssuedStrategy {
            account_id: "account-1",
            issuance_id: "issue-1",
            code_key: "ema_crossover",
            version: "1",
            entry_point: "native:ema_crossover",
            package_hash: &metadata.digest,
        };
        let scope = AccountScope { account_id: "account-1" };
        assert!(authorize_issued(issued, scope, &metadata).is_ok());
        assert!(authorize_issued(issued, AccountScope { account_id: "other" }, &metadata).is_err());
        let mut tampered = metadata.clone();
        tampered.digest = "b".repeat(64);
        assert!(authorize_issued(issued, scope, &tampered).is_err());
        tampered = metadata.clone();
        tampered.entry_point = "native:another".into();
        assert!(authorize_issued(issued, scope, &tampered).is_err());
    }
}

//! Durable, write-once storage for future native Rust strategy issuances.
//! No HTTP admission or executable loading is exposed by this Ledger module.

use crate::{Ledger, LedgerError, now_ms};
use rusqlite::{TransactionBehavior, params};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeStrategyLoad {
    pub tenant_id: String,
    pub account_id: String,
    pub issuance_id: String,
    pub code_key: String,
    pub version: String,
    pub grant_purpose: String,
    pub parameters_json: String,
    pub entry_point: String,
    pub package_hash: String,
    pub manifest: Vec<u8>,
}

impl Ledger {
    /// Persist a native issuance once. Caller MUST have authenticated the
    /// issuer and verified the manifest/hash/parameters separately.
    ///
    /// The transaction also requires an active tenant of the grant purpose.
    /// Existing issuance IDs cannot be rewritten (even to different bytes).
    pub fn insert_native_strategy(&self, load: &NativeStrategyLoad) -> Result<(), LedgerError> {
        if load.account_id.is_empty()
            || load.issuance_id.is_empty()
            || load.manifest.is_empty()
            || load.manifest.len() > 4096
            || load.package_hash.len() != 64
            || !load.package_hash.bytes().all(|b| b.is_ascii_hexdigit())
            || !matches!(load.grant_purpose.as_str(), "development" | "production")
        {
            return Err(LedgerError::Conflict("Rust 策略下发字段无效。".into()));
        }
        let mut connection = self.lock()?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        let purpose: Option<(String, String)> = tx
            .query_row(
                "SELECT purpose,status FROM tenants WHERE tenant_id=?1",
                params![load.tenant_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        if purpose.as_ref().is_none_or(|(purpose, status)| {
            purpose != &load.grant_purpose || status != "active"
        }) {
            return Err(LedgerError::Conflict("Rust 策略授权租户不匹配或已停用。".into()));
        }
        tx.execute(
            "INSERT INTO native_strategy_loads (
                tenant_id,account_id,issuance_id,code_key,version,grant_purpose,
                parameters_json,entry_point,package_hash,manifest,loaded_at
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![
                load.tenant_id,load.account_id,load.issuance_id,
                load.code_key,load.version,load.grant_purpose,load.parameters_json,
                load.entry_point,load.package_hash,load.manifest,now_ms()
            ],
        )
        .map_err(|e| {
            if matches!(e, rusqlite::Error::SqliteFailure(ref error, _)
                if error.code == rusqlite::ErrorCode::ConstraintViolation)
            {
                LedgerError::Conflict("Rust 策略下发编号已存在，不允许修改。".into())
            } else {
                LedgerError::Sqlite(e.to_string())
            }
        })?;
        tx.commit().map_err(|e| LedgerError::Sqlite(e.to_string()))
    }

    /// Account-scoped read; no lookup by bare issuance_id is available.
    pub fn native_strategies(
        &self,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<Vec<NativeStrategyLoad>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "SELECT tenant_id,account_id,issuance_id,code_key,version,grant_purpose,
                    parameters_json,entry_point,package_hash,manifest
             FROM native_strategy_loads WHERE tenant_id=?1 AND account_id=?2
             ORDER BY issuance_id",
        ).map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        let rows = statement.query_map(params![tenant_id, account_id], |row| {
            Ok(NativeStrategyLoad {
                tenant_id: row.get(0)?,
                account_id: row.get(1)?,
                issuance_id: row.get(2)?,
                code_key: row.get(3)?,
                version: row.get(4)?,
                grant_purpose: row.get(5)?,
                parameters_json: row.get(6)?,
                entry_point: row.get(7)?,
                package_hash: row.get(8)?,
                manifest: row.get(9)?,
            })
        }).map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>().map_err(|e| LedgerError::Sqlite(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn native_issuance_is_scoped_immutable_and_requires_active_purpose() {
        let path = std::env::temp_dir().join(format!(
            "seealpha-native-issuance-{}-{}.sqlite",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        let ledger = Ledger::open(&path).unwrap();
        ledger.ensure_tenant("tenant-1", "development", "active").unwrap();
        let load = NativeStrategyLoad {
            tenant_id: "tenant-1".into(),
            account_id: "account-1".into(),
            issuance_id: "issue-1".into(),
            code_key: "ema_crossover".into(),
            version: "1".into(),
            grant_purpose: "development".into(),
            parameters_json: "{}".into(),
            entry_point: "native:ema_crossover".into(),
            package_hash: "a".repeat(64),
            manifest: b"test-manifest".to_vec(),
        };
        ledger.insert_native_strategy(&load).unwrap();
        assert_eq!(ledger.native_strategies("tenant-1", "account-1").unwrap(), vec![load.clone()]);
        assert!(ledger.native_strategies("tenant-1", "other").unwrap().is_empty());
        assert!(ledger.native_strategies("other", "account-1").unwrap().is_empty());
        assert!(matches!(ledger.insert_native_strategy(&load), Err(LedgerError::Conflict(_))));
        let mut tampered = load.clone();
        tampered.manifest = b"replacement".to_vec();
        assert!(matches!(ledger.insert_native_strategy(&tampered), Err(LedgerError::Conflict(_))));
        let mut wrong = load.clone();
        wrong.issuance_id = "issue-2".into();
        wrong.grant_purpose = "production".into();
        assert!(ledger.insert_native_strategy(&wrong).is_err());
        ledger.ensure_tenant("tenant-1", "development", "archived").unwrap();
        wrong.grant_purpose = "development".into();
        assert!(ledger.insert_native_strategy(&wrong).is_err());
        drop(ledger);
        let _ = std::fs::remove_file(path);
    }
}

//! Research/backtest persistent results, unrelated to trading account funds.
use super::{StateStore, LedgerError, now_ms};
use seealpha_database::{OptionalExtension, params};

impl StateStore {
    pub fn save_backtest(
        &self,
        tenant_id: &str,
        account_id: &str,
        kind: &str,
        report: &str,
    ) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO backtest_results (tenant_id, account_id, kind, report_json, updated_at)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT(tenant_id, account_id, kind) DO UPDATE SET
                   report_json=excluded.report_json, updated_at=excluded.updated_at",
                params![tenant_id, account_id, kind, report, now_ms()],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        Ok(())
    }

    pub fn latest_backtest(
        &self,
        tenant_id: &str,
        account_id: &str,
        kind: &str,
    ) -> Result<Option<String>, LedgerError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT report_json FROM backtest_results WHERE tenant_id=$1 AND account_id=$2 AND kind=$3",
                params![tenant_id, account_id, kind],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| LedgerError::Database(error.to_string()))
    }

    pub fn save_snapshot(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
        sha256: &str,
        body: &str,
    ) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO research_snapshots (tenant_id, snapshot_id, sha256, body_json, created_at)
                 VALUES ($1, $2, $3, $4, $5)",
                params![tenant_id, snapshot_id, sha256, body, now_ms()],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        Ok(())
    }

    pub fn snapshot(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
    ) -> Result<Option<(String, String)>, LedgerError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT sha256, body_json FROM research_snapshots WHERE tenant_id=$1 AND snapshot_id=$2",
                params![tenant_id, snapshot_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| LedgerError::Database(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn research_records_roundtrip_after_moving_out_of_ledger() {
        let store = StateStore::open(std::path::Path::new(":memory:")).unwrap();
        store.save_backtest("t", "a", "history", r#"{"ok":true}"#).unwrap();
        assert_eq!(
            store.latest_backtest("t", "a", "history").unwrap().as_deref(),
            Some(r#"{"ok":true}"#),
        );
        assert!(store.latest_backtest("other", "a", "history").unwrap().is_none());
        store.save_snapshot("t", "snap", "digest", r#"{"bars":1}"#).unwrap();
        assert_eq!(
            store.snapshot("t", "snap").unwrap(),
            Some(("digest".into(), r#"{"bars":1}"#.into())),
        );
        assert!(store.snapshot("other", "snap").unwrap().is_none());
    }
}

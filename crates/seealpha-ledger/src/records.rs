use crate::{Ledger, LedgerError};
use rusqlite::{OptionalExtension, params};

impl Ledger {
    pub fn get_order_request(
        &self,
        tenant_id: &str,
        account_id: &str,
        request_id: &str,
    ) -> Result<Option<String>, LedgerError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT body_json FROM order_requests WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3",
                params![tenant_id, account_id, request_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
    }

    pub fn save_order_request(
        &self,
        tenant_id: &str,
        account_id: &str,
        request_id: &str,
        body: &str,
    ) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO order_requests (tenant_id, account_id, request_id, body_json, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(tenant_id, account_id, request_id) DO UPDATE SET
                   body_json=excluded.body_json, updated_at=excluded.updated_at",
                params![tenant_id, account_id, request_id, body, now_ms()],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        Ok(())
    }

    pub fn upsert_order_row(
        &self,
        tenant_id: &str,
        account_id: &str,
        environment: &str,
        client_order_id: &str,
        order_json: &str,
    ) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO orders (tenant_id, account_id, environment, client_order_id, order_json, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(tenant_id, account_id, environment, client_order_id) DO UPDATE SET
                   order_json=excluded.order_json, updated_at=excluded.updated_at",
                params![tenant_id, account_id, environment, client_order_id, order_json, now_ms()],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        Ok(())
    }

    pub fn upsert_fill_row(
        &self,
        tenant_id: &str,
        account_id: &str,
        environment: &str,
        fill_id: &str,
        fill_json: &str,
    ) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO fills (tenant_id, account_id, environment, fill_id, fill_json, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(tenant_id, account_id, environment, fill_id) DO UPDATE SET
                   fill_json=excluded.fill_json, updated_at=excluded.updated_at",
                params![tenant_id, account_id, environment, fill_id, fill_json, now_ms()],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        Ok(())
    }

    pub fn fills(
        &self,
        tenant_id: &str,
        account_id: &str,
        environment: &str,
    ) -> Result<Vec<String>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT fill_json FROM fills WHERE tenant_id=?1 AND account_id=?2 AND environment=?3 ORDER BY fill_id",
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        let rows = statement
            .query_map(params![tenant_id, account_id, environment], |row| {
                row.get(0)
            })
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
    }

    pub fn put_gap(
        &self,
        tenant_id: &str,
        account_id: &str,
        scope: &str,
        gap_key: &str,
        gap_json: &str,
    ) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO gaps (tenant_id, account_id, scope, gap_key, gap_json, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(tenant_id, account_id, scope, gap_key) DO UPDATE SET
                   gap_json=excluded.gap_json, updated_at=excluded.updated_at",
                params![tenant_id, account_id, scope, gap_key, gap_json, now_ms()],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        Ok(())
    }

    pub fn gaps(&self, tenant_id: &str, account_id: &str) -> Result<Vec<String>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT gap_json FROM gaps WHERE tenant_id=?1 AND account_id=?2 ORDER BY gap_key",
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        let rows = statement
            .query_map(params![tenant_id, account_id], |row| row.get(0))
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
    }

    pub fn add_event(
        &self,
        tenant_id: &str,
        account_id: &str,
        kind: &str,
        body: &str,
    ) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO events (tenant_id, account_id, kind, body_json, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![tenant_id, account_id, kind, body, now_ms()],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        Ok(())
    }

    pub fn events(
        &self,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<Vec<(String, String)>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare("SELECT kind, body_json FROM events WHERE tenant_id=?1 AND account_id=?2 ORDER BY id")
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        let rows = statement
            .query_map(params![tenant_id, account_id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
    }

    pub fn import_bars(
        &self,
        tenant_id: &str,
        product: &str,
        symbol: &str,
        interval: &str,
        rows: &[CatalogBar],
    ) -> Result<usize, LedgerError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        for row in rows {
            transaction
                .execute(
                    "INSERT INTO catalog_bars (
                        tenant_id, product, symbol, interval, open_time_ms, open, high, low, close, volume
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                     ON CONFLICT(tenant_id, product, symbol, interval, open_time_ms) DO UPDATE SET
                       open=excluded.open, high=excluded.high, low=excluded.low,
                       close=excluded.close, volume=excluded.volume",
                    params![
                        tenant_id, product, symbol, interval, row.open_time_ms, row.open, row.high, row.low, row.close, row.volume
                    ],
                )
                .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        }
        transaction
            .commit()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        Ok(rows.len())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn catalog_bars(
        &self,
        tenant_id: &str,
        product: &str,
        symbol: &str,
        interval: &str,
        start_ms: i64,
        end_ms: i64,
        limit: i64,
    ) -> Result<Vec<CatalogBar>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT open_time_ms, open, high, low, close, volume FROM catalog_bars
                 WHERE tenant_id=?1 AND product=?2 AND symbol=?3 AND interval=?4
                   AND open_time_ms>=?5 AND open_time_ms<?6
                 ORDER BY open_time_ms LIMIT ?7",
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        let rows = statement
            .query_map(
                params![
                    tenant_id, product, symbol, interval, start_ms, end_ms, limit
                ],
                |row| {
                    Ok(CatalogBar {
                        open_time_ms: row.get(0)?,
                        open: row.get(1)?,
                        high: row.get(2)?,
                        low: row.get(3)?,
                        close: row.get(4)?,
                        volume: row.get(5)?,
                    })
                },
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
    }

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
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(tenant_id, account_id, kind) DO UPDATE SET
                   report_json=excluded.report_json, updated_at=excluded.updated_at",
                params![tenant_id, account_id, kind, report, now_ms()],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
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
                "SELECT report_json FROM backtest_results WHERE tenant_id=?1 AND account_id=?2 AND kind=?3",
                params![tenant_id, account_id, kind],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
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
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![tenant_id, snapshot_id, sha256, body, now_ms()],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
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
                "SELECT sha256, body_json FROM research_snapshots WHERE tenant_id=?1 AND snapshot_id=?2",
                params![tenant_id, snapshot_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogBar {
    pub open_time_ms: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0),
    )
    .unwrap_or(0)
}

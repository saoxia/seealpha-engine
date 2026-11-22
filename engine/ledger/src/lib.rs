//! SQLite runtime book for tenants, trading evidence, and paper orders.
mod order_intents;
mod native_events;
pub use native_events::UnverifiedNativeOrderEvent;
mod order_reconciliation_tasks;
mod records;
pub use order_reconciliation_tasks::ReconciliationTask;
mod account_events;
mod account_snapshots;
pub use account_snapshots::AccountSnapshot;
pub use order_intents::{BasketAdmission, IntentStart, OrderRecoveryItem};
mod basket_journal;
mod coordinator_state;
pub use basket_journal::StoredBasketJournal;
pub use coordinator_state::StoredCoordinatorState;
mod parity;
mod settlements;
mod verified_fills;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use rust_decimal::Decimal;
use std::path::Path;
use thiserror::Error;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS portfolio_coordinator_states (
 tenant_id TEXT NOT NULL,
 account_id TEXT NOT NULL,
 instrument TEXT NOT NULL,
 revision TEXT NOT NULL,
 body_json TEXT NOT NULL,
 PRIMARY KEY (tenant_id,account_id,instrument)
);
CREATE TABLE IF NOT EXISTS basket_journals (
 tenant_id TEXT NOT NULL,
 account_id TEXT NOT NULL,
 basket_id TEXT NOT NULL,
 revision TEXT NOT NULL,
 body_json TEXT NOT NULL,
 PRIMARY KEY (tenant_id, account_id, basket_id)
);
CREATE TABLE IF NOT EXISTS order_intents (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    intent_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, request_id)
);
CREATE INDEX IF NOT EXISTS order_intents_client
    ON order_intents(tenant_id, account_id, environment, client_order_id);
CREATE TABLE IF NOT EXISTS order_reconciliation_tasks (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    request_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','leased','retry','manual')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_due_ms INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_until_ms INTEGER,
    last_error_code TEXT,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY(tenant_id,account_id,request_id),
    FOREIGN KEY(tenant_id,account_id,request_id)
        REFERENCES order_intents(tenant_id,account_id,request_id)
);
CREATE INDEX IF NOT EXISTS order_reconcile_due
    ON order_reconciliation_tasks(tenant_id,state,next_due_ms);
-- Unverified Nautilus event metadata is deliberately separate from trusted
-- order_observations and fills. Rows never advance financial state.
CREATE TABLE IF NOT EXISTS native_order_event_quarantine (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_json TEXT NOT NULL,
    PRIMARY KEY (tenant_id,account_id,environment,client_order_id,event_id)
);
CREATE TABLE IF NOT EXISTS order_observations (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    observation_id TEXT NOT NULL,
    observed_at_ns INTEGER NOT NULL,
    status TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    PRIMARY KEY(tenant_id,account_id,environment,client_order_id,observation_id)
);
CREATE INDEX IF NOT EXISTS order_observations_by_time
    ON order_observations(tenant_id,account_id,environment,client_order_id,observed_at_ns);
CREATE TABLE IF NOT EXISTS account_evidence_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    kind TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    recorded_at_ms INTEGER NOT NULL,
    UNIQUE(tenant_id,account_id,environment,kind,evidence_id)
);
CREATE INDEX IF NOT EXISTS account_evidence_events_page
    ON account_evidence_events(tenant_id,account_id,environment,id);
CREATE TABLE IF NOT EXISTS account_snapshots (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    revision INTEGER NOT NULL,
    evidence_id TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    source TEXT NOT NULL,
    balances_json TEXT NOT NULL,
    PRIMARY KEY(tenant_id,account_id,environment)
);
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id TEXT PRIMARY KEY,
    purpose TEXT NOT NULL,
    status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS connections (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, environment)
);
CREATE TABLE IF NOT EXISTS order_requests (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    body_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, request_id)
);
-- Immutable request intent stays in order_requests. Confirmed results,
-- retries and reconciliation are appended as separate scoped projections.
CREATE TABLE IF NOT EXISTS order_request_views (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    body_json TEXT NOT NULL,
    recorded_at INTEGER NOT NULL,
    FOREIGN KEY(tenant_id,account_id,request_id)
        REFERENCES order_requests(tenant_id,account_id,request_id)
);
CREATE INDEX IF NOT EXISTS order_request_views_by_request
    ON order_request_views(tenant_id,account_id,request_id,id);
CREATE TABLE IF NOT EXISTS orders (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    order_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, environment, client_order_id)
);
CREATE TABLE IF NOT EXISTS fills (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    fill_id TEXT NOT NULL,
    fill_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, environment, fill_id)
);
CREATE TABLE IF NOT EXISTS quarantined_unverified_fills (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    fill_id TEXT NOT NULL,
    fill_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    quarantined_at INTEGER NOT NULL,
    PRIMARY KEY(tenant_id,account_id,environment,fill_id)
);
CREATE TABLE IF NOT EXISTS positions (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    position_id TEXT NOT NULL,
    position_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, environment, position_id)
);
CREATE TABLE IF NOT EXISTS gaps (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    scope TEXT NOT NULL,
    gap_key TEXT NOT NULL,
    gap_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, scope, gap_key)
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    body_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS paper_positions (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    quantity TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, symbol)
);
CREATE TABLE IF NOT EXISTS account_settlements (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    environment TEXT NOT NULL,
    product TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    event_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    PRIMARY KEY(tenant_id,account_id,environment,product,event_kind,event_id)
);
CREATE INDEX IF NOT EXISTS account_settlement_symbol
    ON account_settlements(tenant_id,account_id,environment,product,symbol);
CREATE TABLE IF NOT EXISTS backtest_results (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    report_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, kind)
);
CREATE TABLE IF NOT EXISTS research_snapshots (
    tenant_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    body_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, snapshot_id)
);
CREATE TABLE IF NOT EXISTS paper_sessions (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    report_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, request_id)
);
";

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("状态库不可用。{0}")]
    Sqlite(String),
    #[error("{0}")]
    Conflict(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tenant {
    pub tenant_id: String,
    pub purpose: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionStart {
    Fresh { position: Decimal },
    Replay { report: String },
    Unresolved,
    Rejected { report: String },
}

#[derive(Debug, Clone)]
pub struct StoredOrder {
    pub order_json: String,
}

pub struct SessionWrite<'a> {
    pub tenant_id: &'a str,
    pub account_id: &'a str,
    pub symbol: &'a str,
    pub request_id: &'a str,
    pub fingerprint: &'a str,
    pub report: &'a str,
    pub expected_position: Decimal,
    pub ending_position: Decimal,
}

pub struct Ledger {
    connection: std::sync::Mutex<Connection>,
}

impl Ledger {
    pub fn open(path: &Path) -> Result<Self, LedgerError> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)
                .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        }
        let connection =
            Connection::open(path).map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;",
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        connection
            .execute_batch(SCHEMA)
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        Ok(Self {
            connection: std::sync::Mutex::new(connection),
        })
    }

    /// Enumerate active tenant scopes for safe recovery of never-started jobs.
    /// This does not grant a new execution or exchange order permission.
    pub fn active_tenant_ids(&self) -> Result<Vec<String>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare("SELECT tenant_id FROM tenants WHERE status='active' ORDER BY tenant_id")
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        let tenants = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        Ok(tenants)
    }

    pub fn ensure_tenant(
        &self,
        tenant_id: &str,
        purpose: &str,
        status: &str,
    ) -> Result<Tenant, LedgerError> {
        if !matches!(purpose, "production" | "development")
            || !matches!(status, "active" | "archived")
        {
            return Err(LedgerError::Conflict("租户类型或状态无效。".into()));
        }
        let connection = self.lock()?;
        connection
            .execute(
                "INSERT INTO tenants (tenant_id, purpose, status) VALUES (?1, ?2, ?3)
                 ON CONFLICT(tenant_id) DO UPDATE SET purpose=excluded.purpose, status=excluded.status",
                params![tenant_id, purpose, status],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        self.tenant_with(&connection, tenant_id)?
            .ok_or_else(|| LedgerError::Sqlite("missing row".into()))
    }

    pub fn tenant(&self, tenant_id: &str) -> Result<Option<Tenant>, LedgerError> {
        let connection = self.lock()?;
        self.tenant_with(&connection, tenant_id)
    }

    pub fn begin_session(
        &self,
        tenant_id: &str,
        account_id: &str,
        symbol: &str,
        request_id: &str,
        fingerprint: &str,
    ) -> Result<SessionStart, LedgerError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        let existing: Option<(String, String, String)> = transaction
            .query_row(
                "SELECT fingerprint, status, report_json FROM paper_sessions
                 WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3",
                params![tenant_id, account_id, request_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        if let Some((stored_fingerprint, status, report)) = existing {
            if stored_fingerprint != fingerprint {
                return Err(LedgerError::Conflict("请求编号已用于另一笔模拟。".into()));
            }
            let start = match status.as_str() {
                "completed" => SessionStart::Replay { report },
                "rejected" => SessionStart::Rejected { report },
                _ => SessionStart::Unresolved,
            };
            transaction
                .commit()
                .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
            return Ok(start);
        }
        let position = position_of(&transaction, tenant_id, account_id, symbol)?;
        transaction
            .execute(
                "INSERT INTO paper_sessions (
                    tenant_id, account_id, request_id, fingerprint, status, report_json, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, 'unknown', '{}', ?5)",
                params![tenant_id, account_id, request_id, fingerprint, now_ms()],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        Ok(SessionStart::Fresh { position })
    }

    pub fn reject_session(&self, finish: &SessionWrite<'_>) -> Result<(), LedgerError> {
        self.finish_session(finish, "rejected", false, &[])
    }

    pub fn complete_session(
        &self,
        finish: &SessionWrite<'_>,
        orders: &[String],
    ) -> Result<(), LedgerError> {
        self.finish_session(finish, "completed", true, orders)
    }

    pub fn orders(
        &self,
        tenant_id: &str,
        account_id: &str,
        environment: &str,
    ) -> Result<Vec<StoredOrder>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT order_json FROM orders
                 WHERE tenant_id=?1 AND account_id=?2 AND environment=?3
                 ORDER BY updated_at, client_order_id",
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        let rows = statement
            .query_map(params![tenant_id, account_id, environment], |row| {
                Ok(StoredOrder {
                    order_json: row.get(0)?,
                })
            })
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
    }

    pub fn order_count(&self, tenant_id: &str, account_id: &str) -> Result<i64, LedgerError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT COUNT(*) FROM orders WHERE tenant_id=?1 AND account_id=?2",
                params![tenant_id, account_id],
                |row| row.get(0),
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
    }

    fn finish_session(
        &self,
        finish: &SessionWrite<'_>,
        status: &str,
        apply_position: bool,
        orders: &[String],
    ) -> Result<(), LedgerError> {
        let mut connection = self.lock()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        let existing: Option<(String, String)> = transaction
            .query_row(
                "SELECT fingerprint, status FROM paper_sessions
                 WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3",
                params![finish.tenant_id, finish.account_id, finish.request_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        let Some((stored_fingerprint, stored_status)) = existing else {
            return Err(LedgerError::Conflict("模拟会话不存在。".into()));
        };
        if stored_fingerprint != finish.fingerprint || stored_status != "unknown" {
            return Err(LedgerError::Conflict("模拟会话不能重复成交。".into()));
        }
        if apply_position {
            let current = position_of(
                &transaction,
                finish.tenant_id,
                finish.account_id,
                finish.symbol,
            )?;
            if current != finish.expected_position {
                return Err(LedgerError::Conflict(
                    "并发的模拟会话已更新持仓，本次未成交。".into(),
                ));
            }
            transaction
                .execute(
                    "INSERT INTO paper_positions (tenant_id, account_id, symbol, quantity, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(tenant_id, account_id, symbol) DO UPDATE SET
                        quantity=excluded.quantity, updated_at=excluded.updated_at",
                    params![
                        finish.tenant_id,
                        finish.account_id,
                        finish.symbol,
                        finish.ending_position.to_string(),
                        now_ms()
                    ],
                )
                .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
            let now = now_ms();
            for (index, order) in orders.iter().enumerate() {
                let client_order_id = format!("{}-{index}", finish.request_id);
                transaction
                    .execute(
                        "INSERT INTO orders (
                            tenant_id, account_id, environment, client_order_id, order_json, updated_at
                         ) VALUES (?1, ?2, 'paper', ?3, ?4, ?5)",
                        params![finish.tenant_id, finish.account_id, client_order_id, order, now],
                    )
                    .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
                transaction
                    .execute(
                        "INSERT INTO fills (
                            tenant_id, account_id, environment, fill_id, fill_json, updated_at
                         ) VALUES (?1, ?2, 'paper', ?3, ?4, ?5)",
                        params![
                            finish.tenant_id,
                            finish.account_id,
                            format!("fill-{client_order_id}"),
                            order,
                            now
                        ],
                    )
                    .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
            }
        }
        transaction
            .execute(
                "UPDATE paper_sessions SET status=?1, report_json=?2, updated_at=?3
                 WHERE tenant_id=?4 AND account_id=?5 AND request_id=?6",
                params![
                    status,
                    finish.report,
                    now_ms(),
                    finish.tenant_id,
                    finish.account_id,
                    finish.request_id
                ],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        Ok(())
    }

    fn tenant_with(
        &self,
        connection: &Connection,
        tenant_id: &str,
    ) -> Result<Option<Tenant>, LedgerError> {
        connection
            .query_row(
                "SELECT tenant_id, purpose, status FROM tenants WHERE tenant_id=?1",
                params![tenant_id],
                |row| {
                    Ok(Tenant {
                        tenant_id: row.get(0)?,
                        purpose: row.get(1)?,
                        status: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
    }

    pub(crate) fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, LedgerError> {
        self.connection
            .lock()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
    }
}

fn position_of(
    connection: &Connection,
    tenant_id: &str,
    account_id: &str,
    symbol: &str,
) -> Result<Decimal, LedgerError> {
    let text: Option<String> = connection
        .query_row(
            "SELECT quantity FROM paper_positions WHERE tenant_id=?1 AND account_id=?2 AND symbol=?3",
            params![tenant_id, account_id, symbol],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
    match text {
        Some(value) => value
            .parse()
            .map_err(|error: rust_decimal::Error| LedgerError::Sqlite(error.to_string())),
        None => Ok(Decimal::ZERO),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn unknown_session_is_not_applied_twice() {
        let path = std::env::temp_dir().join(format!(
            "seealpha-ledger-{}-{}.sqlite",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let ledger = Ledger::open(&path).unwrap();
        let first = ledger
            .begin_session("t", "a", "ETHUSDT", "req", "finger")
            .unwrap();
        assert!(matches!(first, SessionStart::Fresh { .. }));
        let second = ledger
            .begin_session("t", "a", "ETHUSDT", "req", "finger")
            .unwrap();
        assert!(matches!(second, SessionStart::Unresolved));
        assert_eq!(ledger.order_count("t", "a").unwrap(), 0);
        let _ = std::fs::remove_file(&path);
    }
}

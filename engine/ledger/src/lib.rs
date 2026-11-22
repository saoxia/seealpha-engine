//! PostgreSQL runtime book for tenants, trading evidence, and paper orders.
// Native Nautilus owns account/position state; this crate only persists
// SeeAlpha-specific movement evidence, attribution and reconciliation data.
// Keep src/lib.rs as the Rust crate root.
#[path = "../movements/mod.rs"]
mod movements;
#[path = "../allocations/mod.rs"]
mod allocations;
#[path = "../reconciliation/mod.rs"]
mod reconciliation;

// Existing execution, portfolio and compatibility APIs remain unchanged.
// They are not subdomains of Ledger/Funds and are not migrated in this PR.
mod order_intents;
mod order_reconciliation_tasks;
mod records;
pub use order_reconciliation_tasks::ReconciliationTask;
pub use order_intents::{IntentStart, OrderRecoveryItem};
mod basket_journal;
mod coordinator_state;
pub use basket_journal::StoredBasketJournal;
pub use coordinator_state::StoredCoordinatorState;
mod parity;
use seealpha_database::{Connection, OptionalExtension, TransactionBehavior, params};
use rust_decimal::Decimal;
use std::path::Path;
use thiserror::Error;

const SCHEMA: &str = include_str!("../../../docker/seealpha-postgres-schema.sql");

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("状态库不可用。{0}")]
    Database(String),
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
        let connection = Connection::open(path)
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        connection.execute_batch(SCHEMA)
            .map_err(|e| LedgerError::Database(e.to_string()))?;
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
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        let rows = statement
            .query_map(&[], |row| row.get::<_, String>(0))
            .map_err(|e| LedgerError::Database(e.to_string()))?;
        let tenants = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| LedgerError::Database(e.to_string()))?;
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
                "INSERT INTO tenants (tenant_id, purpose, status) VALUES ($1, $2, $3)
                 ON CONFLICT(tenant_id) DO UPDATE SET purpose=excluded.purpose, status=excluded.status",
                params![tenant_id, purpose, status],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        self.tenant_with(&connection, tenant_id)?
            .ok_or_else(|| LedgerError::Database("missing row".into()))
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
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        let existing: Option<(String, String, String)> = transaction
            .query_row(
                "SELECT fingerprint, status, report_json FROM paper_sessions
                 WHERE tenant_id=$1 AND account_id=$2 AND request_id=$3",
                params![tenant_id, account_id, request_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| LedgerError::Database(error.to_string()))?;
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
                .map_err(|error| LedgerError::Database(error.to_string()))?;
            return Ok(start);
        }
        let position = position_of(&transaction, tenant_id, account_id, symbol)?;
        transaction
            .execute(
                "INSERT INTO paper_sessions (
                    tenant_id, account_id, request_id, fingerprint, status, report_json, updated_at
                 ) VALUES ($1, $2, $3, $4, 'unknown', '{}', $5)",
                params![tenant_id, account_id, request_id, fingerprint, now_ms()],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| LedgerError::Database(error.to_string()))?;
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
                 WHERE tenant_id=$1 AND account_id=$2 AND environment=$3
                 ORDER BY updated_at, client_order_id",
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        let rows = statement
            .query_map(params![tenant_id, account_id, environment], |row| {
                Ok(StoredOrder {
                    order_json: row.get(0)?,
                })
            })
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Database(error.to_string()))
    }

    pub fn order_count(&self, tenant_id: &str, account_id: &str) -> Result<i64, LedgerError> {
        let connection = self.lock()?;
        connection
            .query_row(
                "SELECT COUNT(*) FROM orders WHERE tenant_id=$1 AND account_id=$2",
                params![tenant_id, account_id],
                |row| row.get(0),
            )
            .map_err(|error| LedgerError::Database(error.to_string()))
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
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        let existing: Option<(String, String)> = transaction
            .query_row(
                "SELECT fingerprint, status FROM paper_sessions
                 WHERE tenant_id=$1 AND account_id=$2 AND request_id=$3",
                params![finish.tenant_id, finish.account_id, finish.request_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| LedgerError::Database(error.to_string()))?;
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
                     VALUES ($1, $2, $3, $4, $5)
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
                .map_err(|error| LedgerError::Database(error.to_string()))?;
            let now = now_ms();
            for (index, order) in orders.iter().enumerate() {
                let client_order_id = format!("{}-{index}", finish.request_id);
                transaction
                    .execute(
                        "INSERT INTO orders (
                            tenant_id, account_id, environment, client_order_id, order_json, updated_at
                         ) VALUES ($1, $2, 'paper', $3, $4, $5)",
                        params![finish.tenant_id, finish.account_id, client_order_id, order, now],
                    )
                    .map_err(|error| LedgerError::Database(error.to_string()))?;
                transaction
                    .execute(
                        "INSERT INTO fills (
                            tenant_id, account_id, environment, fill_id, fill_json, updated_at
                         ) VALUES ($1, $2, 'paper', $3, $4, $5)",
                        params![
                            finish.tenant_id,
                            finish.account_id,
                            format!("fill-{client_order_id}"),
                            order,
                            now
                        ],
                    )
                    .map_err(|error| LedgerError::Database(error.to_string()))?;
            }
        }
        transaction
            .execute(
                "UPDATE paper_sessions SET status=$1, report_json=$2, updated_at=$3
                 WHERE tenant_id=$4 AND account_id=$5 AND request_id=$6",
                params![
                    status,
                    finish.report,
                    now_ms(),
                    finish.tenant_id,
                    finish.account_id,
                    finish.request_id
                ],
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        Ok(())
    }

    fn tenant_with(
        &self,
        connection: &Connection,
        tenant_id: &str,
    ) -> Result<Option<Tenant>, LedgerError> {
        connection
            .query_row(
                "SELECT tenant_id, purpose, status FROM tenants WHERE tenant_id=$1",
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
            .map_err(|error| LedgerError::Database(error.to_string()))
    }

    pub(crate) fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, LedgerError> {
        self.connection
            .lock()
            .map_err(|error| LedgerError::Database(error.to_string()))
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
            "SELECT quantity FROM paper_positions WHERE tenant_id=$1 AND account_id=$2 AND symbol=$3",
            params![tenant_id, account_id, symbol],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| LedgerError::Database(error.to_string()))?;
    match text {
        Some(value) => value
            .parse()
            .map_err(|error: rust_decimal::Error| LedgerError::Database(error.to_string())),
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
    fn native_event_quarantine_is_retired_without_destroying_existing_data() {
        // New databases must not recreate the unused self-written event store.
        let fresh = Ledger::open(Path::new(":memory:")).unwrap();
        let fresh_conn = fresh.lock().unwrap();
        let count: i64 = fresh_conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='native_order_event_quarantine'", &[],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 0);
        drop(fresh_conn);

        // Existing historical SQLite rows are not dropped or rewritten by open.
        let path = std::env::temp_dir().join(format!(
            "seealpha-retired-native-events-{}-{}.sqlite",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE native_order_event_quarantine (event_id TEXT PRIMARY KEY, event_json TEXT NOT NULL);
                 INSERT INTO native_order_event_quarantine VALUES ('old-1', '{\"evidence_status\":\"unverified_native_event\"}');",
            ).unwrap();
        }
        let reopened = Ledger::open(&path).unwrap();
        let conn = reopened.lock().unwrap();
        let original: String = conn.query_row(
            "SELECT event_json FROM native_order_event_quarantine WHERE event_id='old-1'", &[],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(original, r#"{"evidence_status":"unverified_native_event"}"#);
        drop(conn);
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }

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

//! SQLite runtime book for tenants, strategy packages, paper orders, and catalog bars.
mod records;
mod legacy;
mod order_intents;
mod account_snapshots;
mod account_events;
mod tenant_transfer;
pub use order_intents::{BasketAdmission, IntentStart};
pub use account_snapshots::AccountSnapshot;
mod basket_journal;
mod coordinator_state;
pub use coordinator_state::StoredCoordinatorState;
pub use basket_journal::StoredBasketJournal;
mod parity;
mod funding;
mod verified_fills;
mod exercises;
mod settlements;
pub use records::CatalogBar;
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
CREATE TABLE IF NOT EXISTS subscriptions (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    product TEXT NOT NULL,
    symbol TEXT NOT NULL,
    role TEXT NOT NULL,
    state TEXT NOT NULL,
    progress_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, product, symbol, role)
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    body_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS strategy_loads (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    issuance_id TEXT NOT NULL,
    code_key TEXT NOT NULL,
    version TEXT NOT NULL,
    grant_purpose TEXT NOT NULL,
    parameters_json TEXT NOT NULL,
    entry_point TEXT NOT NULL,
    package_hash TEXT NOT NULL,
    archive BLOB NOT NULL,
    loaded_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, issuance_id)
);
CREATE TABLE IF NOT EXISTS paper_positions (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    quantity TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, account_id, symbol)
);
CREATE TABLE IF NOT EXISTS catalog_bars (
    tenant_id TEXT NOT NULL,
    product TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    open_time_ms INTEGER NOT NULL,
    open TEXT NOT NULL,
    high TEXT NOT NULL,
    low TEXT NOT NULL,
    close TEXT NOT NULL,
    volume TEXT NOT NULL,
    PRIMARY KEY (tenant_id, product, symbol, interval, open_time_ms)
);
CREATE TABLE IF NOT EXISTS option_exercise_observations (
    tenant_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    expiry_time_ms INTEGER NOT NULL,
    evidence_json TEXT NOT NULL,
    PRIMARY KEY(tenant_id,symbol,expiry_time_ms)
);
CREATE INDEX IF NOT EXISTS option_exercise_scope
    ON option_exercise_observations(tenant_id,expiry_time_ms);
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
CREATE TABLE IF NOT EXISTS funding_observations (
    tenant_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    funding_time_ms INTEGER NOT NULL,
    rate_type TEXT NOT NULL,
    funding_rate TEXT NOT NULL,
    mark_price TEXT NOT NULL,
    PRIMARY KEY(tenant_id,symbol,funding_time_ms,rate_type)
);
CREATE INDEX IF NOT EXISTS funding_time_index
    ON funding_observations(tenant_id,symbol,funding_time_ms);
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
CREATE TABLE IF NOT EXISTS data_policies (
    tenant_id TEXT NOT NULL,
    product TEXT NOT NULL,
    symbol TEXT NOT NULL,
    policy_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(tenant_id,product,symbol)
);
CREATE TABLE IF NOT EXISTS strategy_market_subscriptions (
    tenant_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    issuance_id TEXT NOT NULL,
    streams_json TEXT NOT NULL,
    lookback_bars INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(tenant_id,account_id,issuance_id)
);
CREATE TABLE IF NOT EXISTS data_jobs (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    result_json TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(tenant_id,job_id)
);
CREATE INDEX IF NOT EXISTS data_jobs_status ON data_jobs(tenant_id,status,updated_at);
CREATE TABLE IF NOT EXISTS worker_settings (
    singleton_id INTEGER PRIMARY KEY CHECK(singleton_id=1),
    max_workers INTEGER NOT NULL,
    timeout_seconds INTEGER NOT NULL
);
INSERT OR IGNORE INTO worker_settings(singleton_id,max_workers,timeout_seconds) VALUES(1,1,3600);
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

#[derive(Debug, Clone)]
pub struct StrategyLoad {
    pub tenant_id: String,
    pub account_id: String,
    pub issuance_id: String,
    pub code_key: String,
    pub version: String,
    pub grant_purpose: String,
    pub parameters_json: String,
    pub entry_point: String,
    pub package_hash: String,
    pub archive: Vec<u8>,
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
        let mut connection =
            Connection::open(path).map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;",
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        legacy::initialize(&mut connection)?;
        Ok(Self {
            connection: std::sync::Mutex::new(connection),
        })
    }

    /// Enumerate active tenant scopes for safe recovery of never-started jobs.
    /// This does not grant a new execution or exchange order permission.
    pub fn active_tenant_ids(&self) -> Result<Vec<String>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(
            "SELECT tenant_id FROM tenants WHERE status='active' ORDER BY tenant_id"
        ).map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| LedgerError::Sqlite(e.to_string()))?;
        let tenants = rows.collect::<Result<Vec<_>, _>>()
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

    pub fn upsert_strategy(&self, load: &StrategyLoad) -> Result<(), LedgerError> {
        let connection = self.lock()?;
        let now = now_ms();
        connection
            .execute(
                "INSERT INTO strategy_loads (
                    tenant_id, account_id, issuance_id, code_key, version, grant_purpose,
                    parameters_json, entry_point, package_hash, archive, loaded_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                 ON CONFLICT(tenant_id, account_id, issuance_id) DO UPDATE SET
                    code_key=excluded.code_key, version=excluded.version,
                    grant_purpose=excluded.grant_purpose, parameters_json=excluded.parameters_json,
                    entry_point=excluded.entry_point, package_hash=excluded.package_hash,
                    archive=excluded.archive, loaded_at=excluded.loaded_at",
                params![
                    load.tenant_id,
                    load.account_id,
                    load.issuance_id,
                    load.code_key,
                    load.version,
                    load.grant_purpose,
                    load.parameters_json,
                    load.entry_point,
                    load.package_hash,
                    load.archive,
                    now,
                ],
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        Ok(())
    }

    pub fn strategies(
        &self,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<Vec<StrategyLoad>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT tenant_id, account_id, issuance_id, code_key, version, grant_purpose,
                        parameters_json, entry_point, package_hash, archive
                 FROM strategy_loads WHERE tenant_id=?1 AND account_id=?2 ORDER BY issuance_id",
            )
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        let rows = statement
            .query_map(params![tenant_id, account_id], |row| {
                Ok(StrategyLoad {
                    tenant_id: row.get(0)?,
                    account_id: row.get(1)?,
                    issuance_id: row.get(2)?,
                    code_key: row.get(3)?,
                    version: row.get(4)?,
                    grant_purpose: row.get(5)?,
                    parameters_json: row.get(6)?,
                    entry_point: row.get(7)?,
                    package_hash: row.get(8)?,
                    archive: row.get(9)?,
                })
            })
            .map_err(|error| LedgerError::Sqlite(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Sqlite(error.to_string()))
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

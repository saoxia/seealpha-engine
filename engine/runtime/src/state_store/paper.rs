//! Paper-only transactional simulator state and idempotent session replay.
//! No live exchange positions or account balances are stored here.
use super::{StateStore, LedgerError, now_ms};
use seealpha_database::{Connection, OptionalExtension, TransactionBehavior, params};
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionStart {
    Fresh { position: Decimal },
    Replay { report: String },
    Unresolved,
    Rejected { report: String },
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


impl StateStore {
    /// Read the existing paper position without reserving or writing a session.
    pub fn paper_position(
        &self, tenant_id: &str, account_id: &str, symbol: &str,
    ) -> Result<Decimal, LedgerError> {
        let connection = self.lock()?;
        position_of(&connection, tenant_id, account_id, symbol)
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
        self.finish_session(finish, "rejected", false, &[], &[])
    }

    pub fn complete_session(
        &self,
        finish: &SessionWrite<'_>,
        orders: &[String],
        fills: &[String],
    ) -> Result<(), LedgerError> {
        self.finish_session(finish, "completed", true, orders, fills)
    }


    #[cfg(test)]
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
        fills: &[String],
    ) -> Result<(), LedgerError> {
        if orders.len() != fills.len() {
            return Err(LedgerError::Conflict("模拟订单和成交记录数量不匹配。".into()));
        }
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
            for (index, (order, fill)) in orders.iter().zip(fills).enumerate() {
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
                            fill,
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


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_session_is_not_applied_twice() {
        let db = StateStore::open(std::path::Path::new(":memory:")).unwrap();
        let first = db.begin_session("t","a","ETHUSDT","req","finger").unwrap();
        assert!(matches!(first, SessionStart::Fresh { .. }));
        let second = db.begin_session("t","a","ETHUSDT","req","finger").unwrap();
        assert!(matches!(second, SessionStart::Unresolved));
        assert_eq!(db.order_count("t","a").unwrap(),0);
    }
}

//! Transactional, exchange-authored fill evidence. Never infer a fill
//! merely because an order was accepted, canceled, or had executedQty > 0.
use crate::{Ledger, LedgerError};
use seealpha_database::{OptionalExtension, TransactionBehavior, params};
use rust_decimal::Decimal;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

fn db_error(err: seealpha_database::Error) -> LedgerError {
    LedgerError::Database(err.to_string())
}
fn invalid() -> LedgerError {
    LedgerError::Conflict("交易所成交证据无效或与已保存记录冲突。".into())
}
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

impl Ledger {
    /// Move previously auto-generated empty 'fill-{client_order_id}' rows into
    /// a non-destructive quarantine. Historical raw evidence remains available
    /// for audit, but the trading fill table no longer claims a real execution.
    pub fn quarantine_synthetic_fills(&self) -> Result<usize, LedgerError> {
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let mut stmt = tx
            .prepare("SELECT tenant_id,account_id,environment,fill_id,fill_json FROM fills")
            .map_err(db_error)?;
        let rows = stmt
            .query_map(&[], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })
            .map_err(db_error)?;
        let mut suspected = Vec::new();
        for row in rows {
            let (tenant, account, env, id, text) = row.map_err(db_error)?;
            let Ok(value) = serde_json::from_str::<Value>(&text) else {
                continue;
            };
            let client = value["client_order_id"].as_str().unwrap_or("");
            if !client.is_empty()
                && id == format!("fill-{client}")
                && value["trade_id"].as_str() == Some(id.as_str())
                && value["quantity"].is_null()
                && value["price"].is_null()
                && value["symbol"].as_str().is_some()
            {
                suspected.push((tenant, account, env, id, text));
            }
        }
        drop(stmt);
        for (tenant, account, env, id, text) in &suspected {
            tx.execute(
                "INSERT INTO quarantined_unverified_fills
                 (tenant_id,account_id,environment,fill_id,fill_json,reason,quarantined_at)
                 VALUES($1,$2,$3,$4,$5,'synthetic_order_ack_fill',$6) ON CONFLICT DO NOTHING",
                params![tenant, account, env, id, text, now_ms()],
            )
            .map_err(db_error)?;
            tx.execute(
                "DELETE FROM fills WHERE tenant_id=$1 AND account_id=$2
                 AND environment=$3 AND fill_id=$4",
                params![tenant, account, env, id],
            )
            .map_err(db_error)?;
        }
        tx.commit().map_err(db_error)?;
        Ok(suspected.len())
    }

    pub fn import_verified_fills(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        order_id: &str,
        rows: &[Value],
    ) -> Result<usize, LedgerError> {
        if rows.len() >= 1000 || !matches!(environment, "production" | "sandbox") {
            return Err(invalid());
        }
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        for row in rows {
            let id = row["trade_id"].as_str().ok_or_else(invalid)?;
            let qty = row["quantity"]
                .as_str()
                .ok_or_else(invalid)?
                .parse::<Decimal>()
                .map_err(|_| invalid())?;
            let price = row["price"]
                .as_str()
                .ok_or_else(invalid)?
                .parse::<Decimal>()
                .map_err(|_| invalid())?;
            if !id.starts_with("binance:")
                || id.len() > 150
                || row["venue_order_id"].as_str() != Some(order_id)
                || row["source"].as_str() != Some("signed_exchange_user_trades")
                || qty <= Decimal::ZERO
                || price <= Decimal::ZERO
            {
                return Err(invalid());
            }
            let text = row.to_string();
            let prior: Option<String> = tx
                .query_row(
                    "SELECT fill_json FROM fills WHERE tenant_id=$1 AND account_id=$2
                 AND environment=$3 AND fill_id=$4",
                    params![tenant, account, environment, id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(db_error)?;
            if let Some(previous) = prior {
                if previous != text {
                    return Err(invalid());
                }
            } else {
                tx.execute(
                    "INSERT INTO fills
                     (tenant_id,account_id,environment,fill_id,fill_json,updated_at)
                     VALUES($1,$2,$3,$4,$5,$6)",
                    params![tenant, account, environment, id, text, now_ms()],
                )
                .map_err(db_error)?;
            }
        }
        tx.commit().map_err(db_error)?;
        Ok(rows.len())
    }

    pub fn quarantined_fill_count(&self, tenant: &str, account: &str) -> Result<i64, LedgerError> {
        let conn = self.lock()?;
        conn.query_row(
            "SELECT COUNT(*) FROM quarantined_unverified_fills WHERE tenant_id=$1 AND account_id=$2",
            params![tenant,account],|r|r.get(0),
        ).map_err(db_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn quarantines_fabricated_ack_without_losing_raw_evidence() {
        let path = std::env::temp_dir().join(format!(
            "seealpha-fill-{}-{}.db_error",
            std::process::id(),
            now_ms()
        ));
        let book = Ledger::open(&path).unwrap();
        let legacy = json!({"trade_id":"fill-A","client_order_id":"A","symbol":"ETHUSDT"});
        // Seed a pre-migration row directly: live raw upserts are now forbidden.
        {
            let conn = book.lock().unwrap();
            conn.execute(
                "INSERT INTO fills(tenant_id,account_id,environment,fill_id,fill_json,updated_at)
                 VALUES('t','a','production','fill-A',$1,0)",
                seealpha_database::params![legacy.to_string()],
            )
            .unwrap();
        }
        assert!(
            book.upsert_fill_row("t", "a", "production", "fill-B", "{}")
                .is_err()
        );
        assert_eq!(book.quarantine_synthetic_fills().unwrap(), 1);
        assert!(book.fills("t", "a", "production").unwrap().is_empty());
        assert_eq!(book.quarantined_fill_count("t", "a").unwrap(), 1);
        let real = json!({"trade_id":"binance:futures_um:ETHUSDT:123",
            "venue_order_id":"9","quantity":"1","price":"100",
            "source":"signed_exchange_user_trades"});
        book.import_verified_fills("t", "a", "production", "9", std::slice::from_ref(&real))
            .unwrap();
        book.import_verified_fills("t", "a", "production", "9", &[real])
            .unwrap();
        assert_eq!(book.fills("t", "a", "production").unwrap().len(), 1);
        let _ = std::fs::remove_file(path);
    }
}

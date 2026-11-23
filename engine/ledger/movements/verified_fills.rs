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
    pub fn fills(
        &self,
        account_id: &str,
        environment: &str,
    ) -> Result<Vec<String>, LedgerError> {
        let connection = self.lock()?;
        let mut statement = connection
            .prepare(
                "SELECT fill_json FROM fills WHERE account_id=$1 AND environment=$2 ORDER BY fill_id",
            )
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        let rows = statement
            .query_map(params![account_id, environment], |row| {
                row.get(0)
            })
            .map_err(|error| LedgerError::Database(error.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|error| LedgerError::Database(error.to_string()))
    }


    pub fn import_verified_fills(
        &self,
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
                    "SELECT fill_json FROM fills WHERE account_id=$1
                 AND environment=$2 AND fill_id=$3",
                    params![account, environment, id],
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
                     (account_id,environment,fill_id,fill_json,updated_at)
                     VALUES($1,$2,$3,$4,$5)",
                    params![account, environment, id, text, now_ms()],
                )
                .map_err(db_error)?;
            }
        }
        tx.commit().map_err(db_error)?;
        Ok(rows.len())
    }


}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn rejects_unverified_fills_and_preserves_verified_idempotence() {
        let path = std::env::temp_dir().join(format!(
            "seealpha-fill-{}-{}.db_error",
            std::process::id(),
            now_ms()
        ));
        let book = Ledger::open(&path).unwrap();
        // Unsigned or fabricated fill input cannot enter the verified table.
        assert!(book.import_verified_fills(
             "a", "production", "9",
            &[json!({"trade_id":"fill-B","venue_order_id":"9",
                "quantity":"1","price":"100","source":"order_ack"})],
        ).is_err());
        assert!(book.fills( "a", "production").unwrap().is_empty());
        let real = json!({"trade_id":"binance:futures_um:ETHUSDT:123",
            "venue_order_id":"9","quantity":"1","price":"100",
            "source":"signed_exchange_user_trades"});
        book.import_verified_fills( "a", "production", "9", std::slice::from_ref(&real))
            .unwrap();
        book.import_verified_fills( "a", "production", "9", &[real])
            .unwrap();
        assert_eq!(book.fills( "a", "production").unwrap().len(), 1);
        let _ = std::fs::remove_file(path);
    }
}

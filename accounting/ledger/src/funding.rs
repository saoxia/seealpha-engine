//! Immutable settled funding evidence, isolated by tenant and contract.
use crate::{Ledger, LedgerError};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};

fn error(e: rusqlite::Error) -> LedgerError {
    LedgerError::Sqlite(e.to_string())
}
fn conflict() -> LedgerError {
    LedgerError::Conflict("资金费记录与已保存的交易所结算证据冲突。".into())
}

impl Ledger {
    pub fn import_funding_events(
        &self,
        tenant: &str,
        symbol: &str,
        items: &[Value],
    ) -> Result<usize, LedgerError> {
        if items.len() > 5000 {
            return Err(LedgerError::Conflict("资金费记录数量超出上限。".into()));
        }
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(error)?;
        for row in items {
            let time = row["funding_time_ms"].as_i64().ok_or_else(conflict)?;
            let rate_type = row["rate_type"].as_str().ok_or_else(conflict)?;
            let rate = row["funding_rate"].as_str().ok_or_else(conflict)?;
            let mark = row["mark_price"].as_str().ok_or_else(conflict)?;
            if row["symbol"] != symbol
                || time < 0
                || !matches!(rate_type, "Regular" | "Special")
                || rate.parse::<rust_decimal::Decimal>().is_err()
                || mark.parse::<rust_decimal::Decimal>().is_err()
                || mark
                    .parse::<rust_decimal::Decimal>()
                    .ok()
                    .is_none_or(|x| x <= rust_decimal::Decimal::ZERO)
            {
                return Err(conflict());
            }
            let original = tx
                .query_row(
                    "SELECT funding_rate,mark_price FROM funding_observations
                 WHERE tenant_id=?1 AND symbol=?2 AND funding_time_ms=?3 AND rate_type=?4",
                    params![tenant, symbol, time, rate_type],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(error)?;
            if let Some((old_rate, old_mark)) = original {
                if old_rate != rate || old_mark != mark {
                    return Err(conflict());
                }
            } else {
                tx.execute(
                    "INSERT INTO funding_observations
                    (tenant_id,symbol,funding_time_ms,rate_type,funding_rate,mark_price)
                    VALUES(?1,?2,?3,?4,?5,?6)",
                    params![tenant, symbol, time, rate_type, rate, mark],
                )
                .map_err(error)?;
            }
        }
        tx.commit().map_err(error)?;
        Ok(items.len())
    }

    pub fn latest_funding_events(
        &self,
        tenant: &str,
        symbol: &str,
        limit: i64,
    ) -> Result<Vec<Value>, LedgerError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT funding_time_ms,rate_type,funding_rate,mark_price
             FROM funding_observations WHERE tenant_id=?1 AND symbol=?2
             ORDER BY funding_time_ms DESC,rate_type DESC LIMIT ?3",
            )
            .map_err(error)?;
        let rows = stmt
            .query_map(params![tenant, symbol, limit.clamp(1, 500)], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })
            .map_err(error)?;
        let mut items = rows
            .map(|r| {
                let (time, kind, rate, mark) = r.map_err(error)?;
                Ok(
                    json!({"symbol":symbol,"funding_time_ms":time,"rate_type":kind,
                "funding_rate":rate,"mark_price":mark,"settlement":"official_observation"}),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        items.reverse();
        Ok(items)
    }

    pub fn funding_events(
        &self,
        tenant: &str,
        symbol: &str,
        start_ms: i64,
        end_ms: i64,
        limit: i64,
    ) -> Result<Vec<Value>, LedgerError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT funding_time_ms,rate_type,funding_rate,mark_price
             FROM funding_observations WHERE tenant_id=?1 AND symbol=?2
             AND funding_time_ms>=?3 AND funding_time_ms<?4
             ORDER BY funding_time_ms,rate_type LIMIT ?5",
            )
            .map_err(error)?;
        let rows = stmt
            .query_map(
                params![tenant, symbol, start_ms, end_ms, limit.clamp(1, 5000)],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                    ))
                },
            )
            .map_err(error)?;
        rows.map(|r| {
            let (time, rate_type, rate, mark) = r.map_err(error)?;
            Ok(
                json!({"symbol":symbol,"funding_time_ms":time,"rate_type":rate_type,
                "funding_rate":rate,"mark_price":mark,"settlement":"official_observation"}),
            )
        })
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    #[test]
    fn funding_is_append_only_tenant_scoped_and_atomic() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db = std::env::temp_dir().join(format!(
            "seealpha-funding-{}-{stamp}.sqlite",
            std::process::id()
        ));
        let book = Ledger::open(&db).unwrap();
        let row = json!({"symbol":"BTCUSDT","funding_time_ms":100,
            "rate_type":"Regular","funding_rate":"0.0001","mark_price":"100"});
        assert_eq!(
            book.import_funding_events("t", "BTCUSDT", &[row.clone()])
                .unwrap(),
            1
        );
        assert_eq!(
            book.funding_events("t", "BTCUSDT", 0, 200, 100)
                .unwrap()
                .len(),
            1
        );
        assert!(
            book.funding_events("other", "BTCUSDT", 0, 200, 100)
                .unwrap()
                .is_empty()
        );
        let changed = json!({"symbol":"BTCUSDT","funding_time_ms":100,
            "rate_type":"Regular","funding_rate":"0.0002","mark_price":"100"});
        assert!(
            book.import_funding_events("t", "BTCUSDT", &[changed])
                .is_err()
        );
        assert_eq!(
            book.funding_events("t", "BTCUSDT", 0, 200, 100).unwrap()[0]["funding_rate"],
            "0.0001"
        );
        let _ = std::fs::remove_file(db);
    }
}

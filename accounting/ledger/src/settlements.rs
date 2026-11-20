//! Append-only account settlement evidence, scoped to tenant/account/environment.
//! Cash impact is never inferred from public funding rates or option greeks.
use crate::{Ledger, LedgerError};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;

fn error(e: rusqlite::Error) -> LedgerError {
    LedgerError::Sqlite(e.to_string())
}
fn invalid() -> LedgerError {
    LedgerError::Conflict("交易所结算记录无效、未完成分页或与既存证据冲突。".into())
}
fn decimal(row: &Value, key: &str) -> Result<Decimal, LedgerError> {
    let raw = row.get(key).and_then(Value::as_str).ok_or_else(invalid)?;
    Decimal::from_str(raw).map_err(|_| invalid())
}
impl Ledger {
    pub fn import_account_settlements(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        product: &str,
        rows: &[Value],
    ) -> Result<usize, LedgerError> {
        if !matches!(environment, "production" | "sandbox")
            || !matches!(product, "futures_um" | "options")
            || rows.len() >= 1000
        {
            return Err(invalid());
        }
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(error)?;
        for row in rows {
            if row.get("product").and_then(Value::as_str) != Some(product) {
                return Err(invalid());
            }
            let symbol = row
                .get("symbol")
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
                .ok_or_else(invalid)?;
            let (kind, event_id) = match product {
                "futures_um" => {
                    if row["source"] != "binance_signed_income"
                        || row["income_type"] != "FUNDING_FEE"
                        || row["time_ms"].as_i64().is_none_or(|t| t <= 0)
                        || row["asset"].as_str().is_none_or(str::is_empty)
                    {
                        return Err(invalid());
                    }
                    decimal(row, "income")?;
                    (
                        "FUNDING_FEE",
                        row["transaction_id"].as_str().ok_or_else(invalid)?,
                    )
                }
                "options" => {
                    if row["source"] != "binance_signed_exercise_record"
                        || row["account_exercise_confirmed"] != true
                        || row["raw"]["symbol"] != symbol
                        || row["raw"]["createDate"].as_i64().is_none_or(|t| t <= 0)
                    {
                        return Err(invalid());
                    }
                    for field in ["exercisePrice", "quantity", "amount", "fee"] {
                        decimal(&row["raw"], field)?;
                    }
                    ("EXERCISE", row["exercise_id"].as_str().ok_or_else(invalid)?)
                }
                _ => return Err(invalid()),
            };
            if event_id.is_empty()
                || event_id.len() > 96
                || !event_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                return Err(invalid());
            }
            let body = row.to_string();
            let previous: Option<String> = tx
                .query_row(
                    "SELECT evidence_json FROM account_settlements
                 WHERE tenant_id=?1 AND account_id=?2 AND environment=?3
                 AND product=?4 AND event_kind=?5 AND event_id=?6",
                    params![tenant, account, environment, product, kind, event_id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(error)?;
            if let Some(previous) = previous {
                if previous != body {
                    return Err(invalid());
                }
            } else {
                tx.execute(
                    "INSERT INTO account_settlements
                     (tenant_id,account_id,environment,product,event_kind,event_id,
                      symbol,evidence_json)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                    params![
                        tenant,
                        account,
                        environment,
                        product,
                        kind,
                        event_id,
                        symbol,
                        body
                    ],
                )
                .map_err(error)?;
            }
        }
        tx.commit().map_err(error)?;
        Ok(rows.len())
    }

    pub fn account_settlements(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        product: &str,
    ) -> Result<Vec<Value>, LedgerError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT evidence_json FROM account_settlements
             WHERE tenant_id=?1 AND account_id=?2 AND environment=?3 AND product=?4
             ORDER BY event_kind,event_id LIMIT 1000",
            )
            .map_err(error)?;
        let rows = stmt
            .query_map(params![tenant, account, environment, product], |r| {
                r.get::<_, String>(0)
            })
            .map_err(error)?;
        rows.map(|r| serde_json::from_str(&r.map_err(error)?).map_err(|_| invalid()))
            .collect()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn official_funding_is_scoped_deduplicated_and_conflicts_are_atomic() {
        let path =
            std::env::temp_dir().join(format!("sa-settlements-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let ledger = Ledger::open(&path).unwrap();
        let row = json!({"product":"futures_um","symbol":"ETHUSDT",
            "income_type":"FUNDING_FEE","income":"-0.1","asset":"USDT",
            "time_ms":10,"transaction_id":"11","source":"binance_signed_income"});
        ledger
            .import_account_settlements(
                "t",
                "a",
                "production",
                "futures_um",
                std::slice::from_ref(&row),
            )
            .unwrap();
        ledger
            .import_account_settlements(
                "t",
                "a",
                "production",
                "futures_um",
                std::slice::from_ref(&row),
            )
            .unwrap();
        assert_eq!(
            ledger
                .account_settlements("t", "a", "production", "futures_um")
                .unwrap()
                .len(),
            1
        );
        assert!(
            ledger
                .account_settlements("t", "a", "sandbox", "futures_um")
                .unwrap()
                .is_empty()
        );
        let mut changed = row;
        changed["income"] = json!("0.1");
        assert!(
            ledger
                .import_account_settlements("t", "a", "production", "futures_um", &[changed])
                .is_err()
        );
        let _ = std::fs::remove_file(path);
    }
}

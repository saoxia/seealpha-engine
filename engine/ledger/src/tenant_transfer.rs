//! Python Store.export_tenant format, with an atomic *create-only* import.
//! Existing target evidence is never deleted to implement a replacement import.
use crate::{Ledger, LedgerError, Tenant};
use rusqlite::{
    OptionalExtension, TransactionBehavior, params, params_from_iter,
    types::{Value as SqlValue, ValueRef},
};
use serde_json::{Map, Value, json};
use std::collections::HashSet;

const TABLES: [&str; 8] = [
    "connections",
    "order_requests",
    "orders",
    "fills",
    "positions",
    "gaps",
    "events",
    "strategy_loads",
];
const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn sqlite(e: rusqlite::Error) -> LedgerError {
    LedgerError::Sqlite(e.to_string())
}
fn invalid(msg: &str) -> LedgerError {
    LedgerError::Conflict(msg.to_string())
}

fn base64_encode(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for block in bytes.chunks(3) {
        let a = block[0];
        let b = *block.get(1).unwrap_or(&0);
        let c = *block.get(2).unwrap_or(&0);
        text.push(ALPHABET[(a >> 2) as usize] as char);
        text.push(ALPHABET[(((a & 3) << 4) | (b >> 4)) as usize] as char);
        text.push(if block.len() >= 2 {
            ALPHABET[(((b & 15) << 2) | (c >> 6)) as usize] as char
        } else {
            '='
        });
        text.push(if block.len() == 3 {
            ALPHABET[(c & 63) as usize] as char
        } else {
            '='
        });
    }
    text
}
fn base64_decode(text: &str) -> Result<Vec<u8>, LedgerError> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(4) || bytes.len() > 16 * 1024 * 1024 {
        return Err(invalid("invalid Python archive base64"));
    }
    let mut output = Vec::with_capacity(bytes.len() / 4 * 3);
    for (index, block) in bytes.as_chunks::<4>().0.iter().enumerate() {
        let last = index == bytes.len() / 4 - 1;
        let pad = usize::from(block[3] == b'=') + usize::from(block[2] == b'=');
        if (!last && pad != 0) || pad > 2 || (block[2] == b'=' && block[3] != b'=') {
            return Err(invalid("invalid archive padding"));
        }
        let mut sextets = [0u8; 4];
        for (i, b) in block.iter().enumerate() {
            if *b == b'=' && i >= 2 {
                sextets[i] = 0;
            } else {
                sextets[i] = ALPHABET
                    .iter()
                    .position(|c| c == b)
                    .and_then(|n| u8::try_from(n).ok())
                    .ok_or_else(|| invalid("invalid archive character"))?;
            }
        }
        if (pad == 1 && sextets[2] & 3 != 0) || (pad == 2 && sextets[1] & 15 != 0) {
            return Err(invalid("noncanonical archive encoding"));
        }
        output.push((sextets[0] << 2) | (sextets[1] >> 4));
        if pad < 2 {
            output.push((sextets[1] << 4) | (sextets[2] >> 2));
        }
        if pad == 0 {
            output.push((sextets[2] << 6) | sextets[3]);
        }
    }
    Ok(output)
}

fn export_value(table: &str, column: &str, value: ValueRef<'_>) -> Result<Value, LedgerError> {
    match value {
        ValueRef::Null => Ok(Value::Null),
        ValueRef::Integer(i) => Ok(json!(i)),
        ValueRef::Text(b) => std::str::from_utf8(b)
            .map(|s| json!(s))
            .map_err(|_| invalid("invalid SQLite text encoding")),
        ValueRef::Blob(b) if table == "strategy_loads" && column == "archive" => {
            Ok(json!(base64_encode(b)))
        }
        _ => Err(invalid("unsupported SQLite value in Python tenant export")),
    }
}
fn import_value(table: &str, column: &str, value: &Value) -> Result<SqlValue, LedgerError> {
    if table == "strategy_loads" && column == "archive" {
        return Ok(SqlValue::Blob(base64_decode(
            value
                .as_str()
                .ok_or_else(|| invalid("archive must be Python base64"))?,
        )?));
    }
    match value {
        Value::Null => Ok(SqlValue::Null),
        Value::String(s) => Ok(SqlValue::Text(s.clone())),
        Value::Number(n) => n
            .as_i64()
            .map(SqlValue::Integer)
            .ok_or_else(|| invalid("JSON floating point or overflowing integer in SQLite import")),
        _ => Err(invalid(
            "SQLite cell must be text, integer, null, or encoded archive",
        )),
    }
}

impl Ledger {
    /// Export exactly the Python runtime table set and column names.
    /// New Rust-only account/intent/evidence tables are deliberately NOT
    /// represented by this legacy format; use a separate v2 backup for those.
    pub fn export_tenant(&self, tenant_id: &str) -> Result<Value, LedgerError> {
        let conn = self.lock()?;
        // The Python v1 envelope cannot represent newer Rust evidence.
        // Fail closed instead of producing an apparently complete backup
        // that would silently lose irreversible trading facts on import.
        for table in [
            "portfolio_coordinator_states",
            "basket_journals",
            "order_intents",
            "order_observations",
            "account_evidence_events",
            "account_snapshots",
            "quarantined_unverified_fills",
            "paper_positions",
            "account_settlements",
            "backtest_results",
            "research_snapshots",
            "paper_sessions",
        ] {
            let present: bool = conn
                .query_row(
                    &format!("SELECT 1 FROM {table} WHERE tenant_id=?1 LIMIT 1"),
                    params![tenant_id],
                    |_| Ok(()),
                )
                .optional()
                .map_err(sqlite)?
                .is_some();
            if present {
                return Err(invalid(
                    "Python v1 export would omit Rust-only tenant evidence; use a complete v2 backup",
                ));
            }
        }
        // Older SQLite databases may retain retired market-data tables.
        // Never silently omit their rows from a Python v1 export.
        for table in ["catalog_bars", "option_exercise_observations", "funding_observations"] {
            let exists: bool = conn.query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
                params![table], |_| Ok(()),
            ).optional().map_err(sqlite)?.is_some();
            if exists {
                let present: bool = conn.query_row(
                    &format!("SELECT 1 FROM {table} WHERE tenant_id=?1 LIMIT 1"),
                    params![tenant_id], |_| Ok(()),
                ).optional().map_err(sqlite)?.is_some();
                if present {
                    return Err(invalid("legacy market data is not included in Python v1 tenant export"));
                }
            }
        }
        let tenant: Option<(String, String, String)> = conn
            .query_row(
                "SELECT tenant_id,purpose,status FROM tenants WHERE tenant_id=?1",
                params![tenant_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(sqlite)?;
        let mut tables = Map::new();
        for table in TABLES {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT * FROM {table} WHERE tenant_id=?1 ORDER BY rowid LIMIT 25001"
                ))
                .map_err(sqlite)?;
            let names: Vec<String> = stmt
                .column_names()
                .iter()
                .map(|s| (*s).to_string())
                .collect();
            let mut cursor = stmt.query(params![tenant_id]).map_err(sqlite)?;
            let mut exported = Vec::new();
            while let Some(row) = cursor.next().map_err(sqlite)? {
                if exported.len() >= 25000 {
                    return Err(invalid("tenant export exceeds per-table row limit"));
                }
                let mut item = Map::new();
                for (i, name) in names.iter().enumerate() {
                    item.insert(
                        name.clone(),
                        export_value(table, name, row.get_ref(i).map_err(sqlite)?)?,
                    );
                }
                exported.push(Value::Object(item));
            }
            tables.insert(table.to_string(), Value::Array(exported));
        }
        let tenant = tenant
            .map(|(id, purpose, status)| {
                json!({
                    "tenant_id":id,"purpose":purpose,"status":status,
                })
            })
            .unwrap_or(Value::Null);
        Ok(json!({"tenant":tenant,"tables":tables}))
    }

    /// Create a tenant in an empty scope from the Python export envelope.
    /// Never erase an existing tenant, account, order, fill, or event. Retain
    /// legacy event IDs or roll back on collision to avoid false cursor replay.
    pub fn import_tenant_new(
        &self,
        target_tenant: &str,
        purpose: &str,
        payload: &Value,
    ) -> Result<Tenant, LedgerError> {
        if target_tenant.is_empty()
            || target_tenant.len() > 128
            || !matches!(purpose, "production" | "development")
        {
            return Err(invalid("invalid tenant import identity"));
        }
        if payload
            .as_object()
            .is_none_or(|m| m.len() != 2 || !m.contains_key("tenant") || !m.contains_key("tables"))
        {
            return Err(invalid("unexpected fields in Python v1 tenant envelope"));
        }
        let tables = payload["tables"]
            .as_object()
            .ok_or_else(|| invalid("missing Python tenant tables"))?;
        if tables.keys().any(|name| !TABLES.contains(&name.as_str())) {
            return Err(invalid("unknown table in Python export"));
        }
        let origin = payload["tenant"]["tenant_id"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid("export must identify its source tenant"))?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite)?;
        if tx
            .query_row(
                "SELECT 1 FROM tenants WHERE tenant_id=?1",
                params![target_tenant],
                |_| Ok(()),
            )
            .optional()
            .map_err(sqlite)?
            .is_some()
        {
            return Err(invalid(
                "import target already exists; existing evidence must not be erased",
            ));
        }
        for table in TABLES {
            let entries = match tables.get(table) {
                Some(value) => value
                    .as_array()
                    .ok_or_else(|| invalid("export table must be an array"))?,
                None => continue,
            };
            if entries.len() > 25000 {
                return Err(invalid("import table exceeds row limit"));
            }
            let mut stmt = tx
                .prepare(&format!("PRAGMA table_info({table})"))
                .map_err(sqlite)?;
            let names = stmt
                .query_map([], |r| r.get::<_, String>(1))
                .map_err(sqlite)?
                .collect::<Result<HashSet<_>, _>>()
                .map_err(sqlite)?;
            drop(stmt);
            for item in entries {
                let obj = item
                    .as_object()
                    .ok_or_else(|| invalid("exported row must be an object"))?;
                if obj.get("tenant_id").and_then(Value::as_str) != Some(origin) {
                    return Err(invalid("mixed-source tenant export rejected"));
                }
                if obj.keys().any(|key| !names.contains(key)) {
                    return Err(invalid("unexpected Python SQLite column"));
                }
                let mut cols = Vec::new();
                let mut values = Vec::new();
                for (key, value) in obj {
                    if key == "tenant_id" {
                        continue;
                    }
                    cols.push(key.clone());
                    values.push(import_value(table, key, value)?);
                }
                cols.push("tenant_id".into());
                values.push(SqlValue::Text(target_tenant.to_string()));
                let labels = cols
                    .iter()
                    .map(|name| format!("\"{name}\""))
                    .collect::<Vec<_>>()
                    .join(",");
                let placeholders = vec!["?"; cols.len()].join(",");
                let sql_text = format!("INSERT INTO {table} ({labels}) VALUES ({placeholders})");
                tx.execute(&sql_text, params_from_iter(values))
                    .map_err(sqlite)?;
            }
        }
        tx.execute(
            "INSERT INTO tenants(tenant_id,purpose,status) VALUES(?1,?2,'active')",
            params![target_tenant, purpose],
        )
        .map_err(sqlite)?;
        tx.commit().map_err(sqlite)?;
        Ok(Tenant {
            tenant_id: target_tenant.to_string(),
            purpose: purpose.to_string(),
            status: "active".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StrategyLoad;
    use std::path::Path;

    #[test]
    fn python_archive_codec_is_byte_exact_and_strict() {
        for bytes in [
            vec![],
            vec![0],
            vec![0, 255],
            vec![0, 1, 2],
            vec![0, 1, 2, 254, 255],
        ] {
            assert_eq!(base64_decode(&base64_encode(&bytes)).unwrap(), bytes);
        }
        assert_eq!(base64_encode(&[0, 255]), "AP8=");
        assert!(base64_decode("AP9=").is_err());
        assert!(base64_decode("%%%=").is_err());
    }

    #[test]
    fn tenant_export_import_preserves_python_rows_and_rejects_overwrite() {
        let from = Ledger::open(Path::new(":memory:")).unwrap();
        from.ensure_tenant("source", "production", "active")
            .unwrap();
        from.save_order_request("source", "acct", "req", r#"{"status":"UNKNOWN"}"#)
            .unwrap();
        from.add_event("source", "acct", "ACCOUNT", r#"{"currency":"USDT"}"#)
            .unwrap();
        from.upsert_fill_row(
            "source",
            "acct",
            "paper",
            "simulation-1",
            r#"{"price":"0.1"}"#,
        )
        .unwrap();
        from.upsert_strategy(&StrategyLoad {
            tenant_id: "source".into(),
            account_id: "acct".into(),
            issuance_id: "iss".into(),
            code_key: "k".into(),
            version: "1".into(),
            grant_purpose: "research".into(),
            parameters_json: "{}".into(),
            entry_point: "".into(),
            package_hash: "".into(),
            archive: vec![0, 1, 2, 254, 255],
        })
        .unwrap();
        let transfer = from.export_tenant("source").unwrap();
        assert_eq!(
            transfer["tables"]["strategy_loads"][0]["archive"],
            "AAEC/v8="
        );
        let target = Ledger::open(Path::new(":memory:")).unwrap();
        target
            .import_tenant_new("restored", "production", &transfer)
            .unwrap();
        assert_eq!(
            target.request("restored", "acct", "req").unwrap().unwrap()["status"],
            "UNKNOWN"
        );
        assert_eq!(target.fills("restored", "acct", "paper").unwrap().len(), 1);
        assert_eq!(
            target.strategies("restored", "acct").unwrap()[0].archive,
            vec![0, 1, 2, 254, 255]
        );
        assert_eq!(
            target.event_page("restored", "acct", 0, 10).unwrap()["events"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(
            target
                .import_tenant_new("restored", "production", &transfer)
                .is_err()
        );
        let mut poisoned = transfer;
        poisoned["tables"]["fills"][0]["tenant_id"] = json!("other");
        assert!(
            target
                .import_tenant_new("new", "production", &poisoned)
                .is_err()
        );
        assert!(target.tenant("new").unwrap().is_none());
        assert_eq!(target.fills("restored", "acct", "paper").unwrap().len(), 1);
        let live_intent = json!({"action":"NEW","environment":"production",
            "client_order_id":"c1","symbol":"ETHUSDT","side":"BUY","quantity":"1"});
        from.preregister_order_intent(
            "source",
            "acct",
            "production",
            "new",
            "c1",
            &"a".repeat(64),
            &live_intent,
        )
        .unwrap();
        // A Python v1 export must never pretend to include new Rust intent evidence.
        assert!(from.export_tenant("source").is_err());
    }
}

//! Crash-atomic, non-destructive adoption of the Python SQLite runtime schema.
//! Never run this against a shared live Python writer or a production state volume.
use crate::{LedgerError, SCHEMA};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::collections::HashSet;

const RUNTIME_TABLES: [&str; 9] = [
    "connections", "order_requests", "orders", "fills", "positions",
    "gaps", "subscriptions", "events", "strategy_loads",
];

fn sql(err: rusqlite::Error) -> LedgerError {
    LedgerError::Sqlite(err.to_string())
}

fn exists(tx: &Transaction<'_>, table: &str) -> Result<bool, LedgerError> {
    tx.query_row(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
        params![table], |_| Ok(()),
    )
    .optional()
    .map(|v| v.is_some())
    .map_err(sql)
}

fn columns(tx: &Transaction<'_>, table: &str) -> Result<HashSet<String>, LedgerError> {
    // All table identifiers come from the fixed RUNTIME_TABLES whitelist.
    let mut stmt = tx.prepare(&format!("PRAGMA table_info({table})")).map_err(sql)?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1)).map_err(sql)?;
    rows.collect::<Result<HashSet<_>, _>>().map_err(sql)
}

fn legacy_copy(tx: &Transaction<'_>, table: &str) -> Result<(), LedgerError> {
    let legacy = format!("{table}_legacy");
    let statement = match table {
        "connections" => "INSERT INTO connections SELECT '1',account_id,environment,status,updated_at FROM connections_legacy".to_string(),
        "order_requests" => "INSERT INTO order_requests SELECT '1',account_id,request_id,body_json,updated_at FROM order_requests_legacy".to_string(),
        "orders" => "INSERT INTO orders SELECT '1',account_id,environment,client_order_id,order_json,updated_at FROM orders_legacy".to_string(),
        "fills" => "INSERT INTO fills SELECT '1',account_id,environment,fill_id,fill_json,updated_at FROM fills_legacy".to_string(),
        "positions" => "INSERT INTO positions SELECT '1',account_id,environment,position_id,position_json,updated_at FROM positions_legacy".to_string(),
        "gaps" => "INSERT INTO gaps SELECT '1',account_id,scope,gap_key,gap_json,updated_at FROM gaps_legacy".to_string(),
        "subscriptions" => "INSERT INTO subscriptions SELECT '1',account_id,product,symbol,role,state,progress_json,updated_at FROM subscriptions_legacy".to_string(),
        "events" => "INSERT INTO events (id,tenant_id,account_id,kind,body_json,created_at) SELECT id,'1',account_id,kind,body_json,created_at FROM events_legacy".to_string(),
        "strategy_loads" => {
            let available = columns(tx, &legacy)?;
            let optional = |name: &str, fallback: &str| {
                if available.contains(name) { name.to_string() } else { fallback.to_string() }
            };
            format!(
                "INSERT INTO strategy_loads (tenant_id,account_id,issuance_id,code_key,version,grant_purpose,parameters_json,entry_point,package_hash,archive,loaded_at)
                 SELECT '1',account_id,issuance_id,code_key,version,grant_purpose,parameters_json,{},{},{},{} FROM strategy_loads_legacy",
                optional("entry_point", "''"), optional("package_hash", "''"),
                optional("archive", "X''"), optional("loaded_at", "0"),
            )
        }
        _ => return Err(LedgerError::Conflict("unsupported legacy table".into())),
    };
    tx.execute_batch(&statement).map_err(sql)
}

/// Execute schema creation, legacy adoption and column additions as one SQLite
/// IMMEDIATE transaction. A failed conversion rolls back all renames and copies.
pub(crate) fn initialize(connection: &mut Connection) -> Result<(), LedgerError> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate).map_err(sql)?;
    let mut pending = Vec::new();
    for table in RUNTIME_TABLES {
        if exists(&tx, table)? && !columns(&tx, table)?.contains("tenant_id") {
            if exists(&tx, &format!("{table}_legacy"))? {
                return Err(LedgerError::Conflict("unfinished legacy migration; manual inspection required".into()));
            }
            pending.push(table);
        }
    }
    for table in &pending {
        tx.execute_batch(&format!("ALTER TABLE {table} RENAME TO {table}_legacy"))
            .map_err(sql)?;
    }
    tx.execute_batch(SCHEMA).map_err(sql)?;

    // Python added these fields after the original strategy_loads table.
    let strategy_columns = columns(&tx, "strategy_loads")?;
    for (name, definition) in [
        ("entry_point", "TEXT NOT NULL DEFAULT ''"),
        ("package_hash", "TEXT NOT NULL DEFAULT ''"),
        ("archive", "BLOB NOT NULL DEFAULT X''"),
    ] {
        if !strategy_columns.contains(name) {
            tx.execute_batch(&format!("ALTER TABLE strategy_loads ADD COLUMN {name} {definition}"))
                .map_err(sql)?;
        }
    }
    for table in pending {
        legacy_copy(&tx, table)?;
        tx.execute_batch(&format!("DROP TABLE {table}_legacy")).map_err(sql)?;
    }
    if tx.query_row(
        "SELECT 1 FROM orders WHERE tenant_id='1' LIMIT 1",
        [], |_| Ok(()),
    ).optional().map_err(sql)?.is_some() {
        tx.execute(
            "INSERT OR IGNORE INTO tenants(tenant_id,purpose,status) VALUES('1','production','active')",
            [],
        ).map_err(sql)?;
    }
    tx.commit().map_err(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Ledger;
    use std::path::Path;

    #[test]
    fn adopts_unscoped_python_book_and_preserves_event_cursor() {
        let mut old = Connection::open_in_memory().unwrap();
        old.execute_batch(
            "CREATE TABLE order_requests(account_id TEXT,request_id TEXT,body_json TEXT,updated_at INTEGER);
             CREATE TABLE fills(account_id TEXT,environment TEXT,fill_id TEXT,fill_json TEXT,updated_at INTEGER);
             CREATE TABLE events(id INTEGER PRIMARY KEY,account_id TEXT,kind TEXT,body_json TEXT,created_at INTEGER);
             INSERT INTO order_requests VALUES('a','r','{\"status\":\"UNKNOWN\"}',1);
             INSERT INTO fills VALUES('a','production','f','{\"trade_id\":\"f\"}',1);
             INSERT INTO events VALUES(73,'a','unknown','{}',1);"
        ).unwrap();
        initialize(&mut old).unwrap();
        let row: String = old.query_row(
            "SELECT body_json FROM order_requests WHERE tenant_id='1' AND request_id='r'",
            [], |r| r.get(0),
        ).unwrap();
        assert!(row.contains("UNKNOWN"));
        let count: i64 = old.query_row(
            "SELECT COUNT(*) FROM events WHERE id=73 AND tenant_id='1'",
            [], |r| r.get(0),
        ).unwrap();
        assert_eq!(count, 1);
        let fill: String = old.query_row(
            "SELECT fill_json FROM fills WHERE tenant_id='1' AND fill_id='f'",
            [], |r| r.get(0),
        ).unwrap();
        assert!(fill.contains("trade_id"));
        // Existing scoped records remain untouched on repeated initialization.
        initialize(&mut old).unwrap();
        assert!(exists(&old.transaction().unwrap(), "order_requests").unwrap());
    }

    #[test]
    fn failed_legacy_adoption_rolls_back_renames_and_writes() {
        let mut old = Connection::open_in_memory().unwrap();
        old.execute_batch(
            "CREATE TABLE fills(account_id TEXT,environment TEXT,fill_id TEXT,fill_json TEXT,updated_at INTEGER);
             INSERT INTO fills VALUES('a','production','f','{}',1);
             CREATE TABLE fills_legacy(x TEXT);"
        ).unwrap();
        assert!(initialize(&mut old).is_err());
        let original = columns(&old.transaction().unwrap(), "fills").unwrap();
        assert!(!original.contains("tenant_id"));
    }

    #[test]
    fn opens_new_book() {
        let book = Ledger::open(Path::new(":memory:")).unwrap();
        assert!(book.orders("t", "a", "paper").unwrap().is_empty());
    }
}

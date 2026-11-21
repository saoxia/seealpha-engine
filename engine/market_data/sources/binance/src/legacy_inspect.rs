//! Read-only inspection of pre-Nautilus SQLite source journals.
//!
//! This does not create any database, write a trade, operate a WebSocket,
//! reconcile a gap, or confer Nautilus live-data readiness. It exists only
//! because the historical Catalog inspection API still exposes this evidence.
use std::path::Path;

use rusqlite::{Connection, OpenFlags};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalInspection {
    pub last_trade_id: Option<i64>,
    pub trade_count: i64,
    pub unresolved_gaps: i64,
    pub pending_updates: i64,
    pub conflicts: i64,
}

/// Read an existing legacy pair journal, rejecting missing/corrupt/foreign
/// identities. This is never treated as a current Nautilus source checkpoint.
pub fn inspect_pair_journal(root: &Path, symbol: &str, base_level: &str) -> Result<JournalInspection> {
    let path = root.join("collection.sqlite");
    if !root.is_dir() || !path.is_file() {
        return Err("legacy source journal is absent".into());
    }
    let db = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let identity: (String, String) =
        db.query_row("SELECT symbol,base_level FROM config LIMIT 1", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?;
    if identity.0 != symbol || identity.1 != base_level {
        return Err("legacy source journal identity mismatch".into());
    }
    let count = |name: &str| -> Result<i64> {
        // Fixed SQL texts only; caller never supplies table names to SQL.
        let sql = match name {
            "trades" => "SELECT count(*) FROM trades",
            "gaps" => "SELECT count(*) FROM gaps",
            "pending_updates" => "SELECT count(*) FROM pending_updates",
            "conflicts" => "SELECT count(*) FROM conflicts",
            _ => return Err("unsupported journal table".into()),
        };
        let rows: i64 = db.query_row(sql, [], |r| r.get(0))?;
        if rows < 0 {
            return Err("invalid negative journal row count".into());
        }
        Ok(rows)
    };
    let last_trade_id: Option<i64> =
        db.query_row("SELECT max(id) FROM trades", [], |r| r.get(0))?;
    Ok(JournalInspection {
        last_trade_id,
        trade_count: count("trades")?,
        unresolved_gaps: count("gaps")?,
        pending_updates: count("pending_updates")?,
        conflicts: count("conflicts")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_journal_does_not_create_storage() {
        let dir = std::env::temp_dir().join(format!(
            "seealpha-legacy-inspection-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir(&dir).unwrap();
        assert!(inspect_pair_journal(&dir, "ETHUSDT", "1m").is_err());
        assert!(!dir.join("collection.sqlite").exists());

        let file = dir.join("collection.sqlite");
        {
            let db = Connection::open(&file).unwrap();
            db.execute_batch(
                "CREATE TABLE config(symbol TEXT,base_level TEXT);
                 INSERT INTO config VALUES ('ETHUSDT','1m');
                 CREATE TABLE trades(id INTEGER);
                 INSERT INTO trades VALUES (10);
                 CREATE TABLE gaps(first_id INTEGER);
                 CREATE TABLE pending_updates(start_ms INTEGER);
                 CREATE TABLE conflicts(detail TEXT);",
            )
            .unwrap();
        }
        let result = inspect_pair_journal(&dir, "ETHUSDT", "1m").unwrap();
        assert_eq!(result.trade_count, 1);
        assert_eq!(result.last_trade_id, Some(10));
        assert_eq!(result.unresolved_gaps, 0);
        assert!(inspect_pair_journal(&dir, "BTCUSDT", "1m").is_err());
        assert!(inspect_pair_journal(&dir, "ETHUSDT", "tick").is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}

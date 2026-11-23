//! Strategy package and issuance persistence on an immutable filesystem.
//! PostgreSQL indexes issuance metadata; executable artifacts live only in files.
use seealpha_database::{Connection, Transaction};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fs::{self, OpenOptions}, io::{Read, Write}, path::{Component, Path, PathBuf}, sync::{Mutex, atomic::{AtomicU64, Ordering}}};
use thiserror::Error;
use zip::ZipArchive;

static STAGING: AtomicU64 = AtomicU64::new(0);
const MAX_ZIP: usize = 1024 * 1024;
const MAX_FILES: usize = 128;
const MAX_UNPACKED: u64 = 4 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("strategy package storage unavailable: {0}")]
    Io(String),
    #[error("strategy issuance conflict: {0}")]
    Conflict(String),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyLoad {
    pub tenant_id: String,
    pub account_id: String,
    pub issuance_id: String,
    pub code_key: String,
    pub version: String,
    pub parameters_json: String,
    pub entry_point: String,
    /// Hash of the executable manifest bytes (legacy-compatible).
    pub package_hash: String,
    /// Exact validated native manifest bytes handed to the executor; never an arbitrary ZIP.
    #[serde(skip)]
    pub archive: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredRecord {
    load: StrategyLoad,
    /// Hash of the original ZIP when an uploaded ZIP was provided.
    zip_sha256: Option<String>,
}
pub struct StrategyStore { root: PathBuf, lock: Mutex<()>, index: Mutex<Connection> }

fn err(error: impl std::fmt::Display) -> StoreError { StoreError::Io(error.to_string()) }
fn conflict(message: &str) -> StoreError { StoreError::Conflict(message.into()) }
fn sha(bytes: &[u8]) -> String { format!("{:x}", Sha256::digest(bytes)) }
fn valid_id(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s != "." && s != ".."
        && s.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c,b'_'|b'-'|b'.'))
}
fn write_new(path: &Path, contents: &[u8]) -> Result<(), StoreError> {
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)] { use std::os::unix::fs::OpenOptionsExt; opts.mode(0o600); }
    let mut out = opts.open(path).map_err(err)?;
    out.write_all(contents).map_err(err)?;
    out.sync_all().map_err(err)
}
fn read_record(path: &Path) -> Result<StoredRecord, StoreError> {
    serde_json::from_slice(&fs::read(path).map_err(err)?).map_err(err)
}

const INDEX_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS strategy_issuances (
 tenant_id TEXT NOT NULL, account_id TEXT NOT NULL, issuance_id TEXT NOT NULL,
 code_key TEXT NOT NULL, version TEXT NOT NULL,
 parameters_json TEXT NOT NULL, entry_point TEXT NOT NULL,
 package_hash TEXT NOT NULL, zip_sha256 TEXT,
 PRIMARY KEY (tenant_id,account_id,issuance_id)
);
CREATE INDEX IF NOT EXISTS strategy_name_version ON strategy_issuances (tenant_id,code_key,version);
";
fn insert_index(tx: &mut Transaction<'_>, record: &StoredRecord) -> Result<(), StoreError> {
    let l = &record.load;
    tx.execute(
        "INSERT INTO strategy_issuances
         (tenant_id,account_id,issuance_id,code_key,version,
          parameters_json,entry_point,package_hash,zip_sha256)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING",
        &[&l.tenant_id,&l.account_id,&l.issuance_id,&l.code_key,&l.version,
                &l.parameters_json,&l.entry_point,&l.package_hash,&record.zip_sha256]
    ).map_err(err)?;
    Ok(())
}
type IndexedIdentity = (String,String,String,String,String,Option<String>);
fn read_index_row(row: &postgres::Row) -> StrategyLoad {
    StrategyLoad {
        tenant_id: row.get(0), account_id: row.get(1), issuance_id: row.get(2),
        code_key: row.get(3), version: row.get(4),
        parameters_json: row.get(5), entry_point: row.get(6),
        package_hash: row.get(7), archive: Vec::new(),
    }
}

/// Accept ZIP transport with a root manifest.json. Reject unsafe paths,
/// symlinks, encryption, duplicate names and decompression bombs.
pub type ExtractedFiles = Vec<(PathBuf, Vec<u8>)>;
pub fn unpack_native_zip(bytes: &[u8]) -> Result<(Vec<u8>, ExtractedFiles), StoreError> {
    if bytes.is_empty() || bytes.len() > MAX_ZIP { return Err(conflict("ZIP too large")); }
    let mut zip = ZipArchive::new(std::io::Cursor::new(bytes)).map_err(err)?;
    if zip.is_empty() || zip.len() > MAX_FILES { return Err(conflict("ZIP file count invalid")); }
    let mut seen = std::collections::HashSet::new();
    let mut files = Vec::new();
    let mut total = 0_u64;
    for i in 0..zip.len() {
        let item = zip.by_index(i).map_err(err)?;
        let item_size = item.size();
        let name = item.name().to_string();
        if name == "." || name == ".." { return Err(conflict("unsafe ZIP path")); }
        let rel = Path::new(&name);
        if name.contains('\\') || name.contains(':') || rel.is_absolute()
            || !rel.components().all(|c| matches!(c, Component::Normal(_)))
            || item.encrypted()
            || item.unix_mode().is_some_and(|mode| mode & 0o170000 == 0o120000)
            || !seen.insert(name.clone())
            || matches!(name.as_str(), "issuance.json" | "package.zip")
        { return Err(conflict("unsafe or duplicate ZIP entry")); }
        if item.is_dir() { continue; }
        total = total.checked_add(item_size).ok_or_else(|| conflict("ZIP size overflow"))?;
        if total > MAX_UNPACKED { return Err(conflict("ZIP expands beyond limit")); }
        let mut data = Vec::new();
        item.take(MAX_UNPACKED + 1).read_to_end(&mut data).map_err(err)?;
        if data.len() as u64 != item_size { return Err(conflict("ZIP entry size mismatch")); }
        files.push((rel.to_path_buf(), data));
    }
    let manifest = files.iter().find(|(p,_)| p == Path::new("manifest.json"))
        .map(|(_,b)| b.clone()).ok_or_else(|| conflict("missing root manifest.json"))?;
    Ok((manifest, files))
}
impl StrategyStore {
    pub fn open(root: &Path) -> Result<Self, StoreError> {
        fs::create_dir_all(root).map_err(err)?;
        if fs::symlink_metadata(root).map_err(err)?.file_type().is_symlink() {
            return Err(conflict("strategy root cannot be a symlink"));
        }
        #[cfg(unix)] {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root, fs::Permissions::from_mode(0o700)).map_err(err)?;
        }
        let connection = Connection::open(root).map_err(err)?;
        // The index and trading Ledger use the same dedicated PostgreSQL schema.
        connection.execute_batch(INDEX_SCHEMA).map_err(err)?;
        // Older databases have a non-null grant_purpose column. Keep its
        // historical values, but permit new issuance rows without that field.
        if connection.query_opt(
            "SELECT 1 FROM information_schema.columns
             WHERE table_schema=current_schema() AND table_name='strategy_issuances'
             AND column_name='grant_purpose'",
            &[],
        ).map_err(err)?.is_some() {
            connection.execute_batch(
                "ALTER TABLE strategy_issuances ALTER COLUMN grant_purpose DROP NOT NULL"
            ).map_err(err)?;
        }
        let store = Self { root: root.into(), lock: Mutex::new(()), index: Mutex::new(connection) };
        store.rebuild_index()?;
        Ok(store)
    }

    /// Reconstruct the index from published issuance directories after a crash
    /// or loss of the metadata database; no legacy Ledger strategy tables.
    fn rebuild_index(&self) -> Result<(), StoreError> {
        let _guard = self.lock.lock().map_err(err)?;
        let mut conn = self.index.lock().map_err(err)?;
        let mut tx = conn.transaction().map_err(err)?;
        // Never erase other Engine instances' metadata in a shared database.
        for tenant in fs::read_dir(&self.root).map_err(err)? {
            let tenant = tenant.map_err(err)?;
            if !tenant.file_type().map_err(err)?.is_dir() { continue; }
            for account in fs::read_dir(tenant.path()).map_err(err)? {
                let account = account.map_err(err)?;
                if !account.file_type().map_err(err)?.is_dir() { continue; }
                for item in fs::read_dir(account.path()).map_err(err)? {
                    let item = item.map_err(err)?;
                    if !item.file_type().map_err(err)?.is_dir()
                        || item.file_name().to_string_lossy().starts_with('.') { continue; }
                    let record = read_record(&item.path().join("issuance.json"))?;
                    if record.load.tenant_id != tenant.file_name().to_string_lossy()
                        || record.load.account_id != account.file_name().to_string_lossy()
                        || record.load.issuance_id != item.file_name().to_string_lossy()
                    { return Err(conflict("issuance scope mismatch on startup")); }
                    self.check_artifact(&item.path(), &record)?;
                    insert_index(&mut tx, &record)?;
                    let l = &record.load;
                    let prior = tx.query_one(
                        "SELECT code_key,version,parameters_json,entry_point,package_hash,zip_sha256
                         FROM strategy_issuances WHERE tenant_id=$1 AND account_id=$2 AND issuance_id=$3",
                        &[&l.tenant_id,&l.account_id,&l.issuance_id],
                    ).map_err(err)?;
                    let identity: IndexedIdentity = (prior.get(0),prior.get(1),prior.get(2),
                        prior.get(3),prior.get(4),prior.get(5));
                    if identity != (l.code_key.clone(),l.version.clone(),
                        l.parameters_json.clone(),l.entry_point.clone(),l.package_hash.clone(),
                        record.zip_sha256.clone()) {
                        return Err(conflict("indexed issuance differs from verified disk record"));
                    }
                }
            }
        }
        tx.commit().map_err(err)
    }

    fn check_artifact(&self, folder: &Path, record: &StoredRecord) -> Result<Vec<u8>, StoreError> {
        let manifest = folder.join("manifest.json");
        let file = if manifest.exists() { manifest } else { folder.join("legacy-archive.bin") };
        let bytes = fs::read(file).map_err(err)?;
        if sha(&bytes) != record.load.package_hash {
            return Err(conflict("stored strategy manifest was modified"));
        }
        if let Some(digest) = &record.zip_sha256 {
            let zip_bytes = fs::read(folder.join("package.zip")).map_err(err)?;
            if sha(&zip_bytes) != *digest { return Err(conflict("stored ZIP was modified")); }
            let (manifest, files) = unpack_native_zip(&zip_bytes)?;
            if manifest != bytes { return Err(conflict("ZIP manifest differs from deployed manifest")); }
            for (relative, expected) in files {
                if fs::read(folder.join(relative)).map_err(err)? != expected {
                    return Err(conflict("extracted strategy file was modified"));
                }
            }
        }
        Ok(bytes)
    }

    fn index_record(&self, record: &StoredRecord) -> Result<(), StoreError> {
        let mut conn = self.index.lock().map_err(err)?;
        let mut tx = conn.transaction().map_err(err)?;
        let l = &record.load;
        let current: Option<IndexedIdentity> = tx.query_opt(
            "SELECT code_key,version,parameters_json,entry_point,package_hash,zip_sha256
             FROM strategy_issuances WHERE tenant_id=$1 AND account_id=$2 AND issuance_id=$3",
            &[&l.tenant_id,&l.account_id,&l.issuance_id],
        ).map_err(err)?.map(|r| (r.get(0),r.get(1),r.get(2),r.get(3),r.get(4),r.get(5)));
        let expected = (l.code_key.clone(),l.version.clone(),
            l.parameters_json.clone(),l.entry_point.clone(),l.package_hash.clone(),
            record.zip_sha256.clone());
        match current {
            Some(prior) if prior != expected => return Err(conflict("indexed issuance differs")),
            Some(_) => (),
            None => {
                insert_index(&mut tx, record)?;
                let actual = tx.query_one(
                    "SELECT code_key,version,parameters_json,entry_point,package_hash,zip_sha256
                     FROM strategy_issuances WHERE tenant_id=$1 AND account_id=$2 AND issuance_id=$3",
                    &[&l.tenant_id,&l.account_id,&l.issuance_id],
                ).map_err(err)?;
                let indexed: IndexedIdentity = (actual.get(0),actual.get(1),actual.get(2),
                    actual.get(3),actual.get(4),actual.get(5));
                if indexed != expected { return Err(conflict("indexed issuance differs")); }
            },
        }
        tx.commit().map_err(err)
    }

    /// Metadata-only lookup using PostgreSQL, without walking strategy folders.
    pub fn list(&self, tenant: &str, account: &str) -> Result<Vec<StrategyLoad>, StoreError> {
        if !valid_id(tenant) || !valid_id(account) { return Err(conflict("invalid scope")); }
        let conn = self.index.lock().map_err(err)?;
        let rows = conn.query(
            "SELECT tenant_id,account_id,issuance_id,code_key,version,
                    parameters_json,entry_point,package_hash
             FROM strategy_issuances WHERE tenant_id=$1 AND account_id=$2 ORDER BY issuance_id",
            &[&tenant,&account],
        ).map_err(err)?;
        Ok(rows.iter().map(read_index_row).collect())
    }
    fn folder(&self, load: &StrategyLoad) -> PathBuf {
        self.root.join(&load.tenant_id).join(&load.account_id).join(&load.issuance_id)
    }
    /// Legacy raw native manifest is migrated without changing its identity.
    pub fn insert(&self, load: &StrategyLoad) -> Result<(), StoreError> {
        self.insert_artifact(load, None)
    }
    /// Store extracted ZIP contents and native manifest in an immutable issuance folder.
    pub fn insert_zip(&self, load: &StrategyLoad, bytes: &[u8]) -> Result<(), StoreError> {
        self.insert_artifact(load, Some(bytes))
    }
    fn insert_artifact(&self, load: &StrategyLoad, zip: Option<&[u8]>) -> Result<(), StoreError> {
        if ![&load.tenant_id,&load.account_id,&load.issuance_id,&load.code_key,
             &load.version].iter().all(|v| valid_id(v))
            || load.package_hash != sha(&load.archive)
            || serde_json::from_str::<serde_json::Value>(&load.parameters_json).is_err()
            || load.entry_point.is_empty()
        { return Err(conflict("invalid issuance identity or manifest digest")); }
        let expanded = zip.map(unpack_native_zip).transpose()?;
        if let Some((manifest,_)) = &expanded
            && manifest != &load.archive
        { return Err(conflict("ZIP manifest differs from trusted bytes")); }
        let mut stored_load = load.clone();
        stored_load.archive.clear(); // executable bytes live only in manifest.json
        let record = StoredRecord {
            load: stored_load,
            zip_sha256: zip.map(sha),
        };
        let _guard = self.lock.lock().map_err(err)?;
        let final_dir = self.folder(load);
        if final_dir.exists() {
            let prior = read_record(&final_dir.join("issuance.json"))?;
            if prior != record { return Err(conflict("immutable issuance differs")); }
            let manifest = final_dir.join("manifest.json");
            let path = if manifest.exists() { manifest } else { final_dir.join("legacy-archive.bin") };
            let current = fs::read(path).map_err(err)?;
            if current != load.archive || sha(&current) != load.package_hash {
                return Err(conflict("stored manifest was modified"));
            }
            if let Some(z) = zip
                && sha(&fs::read(final_dir.join("package.zip")).map_err(err)?) != sha(z)
            { return Err(conflict("stored ZIP was modified")); }
            self.check_artifact(&final_dir, &record)?;
            self.index_record(&record)?;
            return Ok(());
        }
        let parent = final_dir.parent().ok_or_else(|| conflict("invalid scope"))?;
        // Create and inspect each scope one level at a time. Do not follow
        // a pre-existing symlink while creating a nested tenant/account path.
        let tenant_dir = self.root.join(&load.tenant_id);
        for dir in [&tenant_dir, parent] {
            if !dir.exists() {
                match fs::create_dir(dir) {
                    Ok(()) => {},
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {},
                    Err(e) => return Err(err(e)),
                }
            }
            if fs::symlink_metadata(dir).map_err(err)?.file_type().is_symlink() {
                return Err(conflict("symlinked issuance scope"));
            }
            #[cfg(unix)] {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).map_err(err)?;
            }
        }
        let stage = parent.join(format!(".incoming-{}-{}",std::process::id(),STAGING.fetch_add(1,Ordering::Relaxed)));
        fs::create_dir(&stage).map_err(err)?;
        #[cfg(unix)] {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&stage, fs::Permissions::from_mode(0o700)).map_err(err)?;
        }
        let outcome = (|| {
            if let Some((_,files)) = &expanded {
                for (path, contents) in files {
                    let target = stage.join(path);
                    if let Some(dir) = target.parent() { fs::create_dir_all(dir).map_err(err)?; }
                    write_new(&target, contents)?;
                }
            } else {
                let file = if load.archive.starts_with(&[0x50, 0x4b, 0x03, 0x04]) { "legacy-archive.bin" } else { "manifest.json" };
                write_new(&stage.join(file), &load.archive)?;
            }
            if let Some(bytes) = zip { write_new(&stage.join("package.zip"),bytes)?; }
            write_new(&stage.join("issuance.json"), &serde_json::to_vec(&record).map_err(err)?)?;
            fs::rename(&stage,&final_dir).map_err(err)
        })();
        if outcome.is_err() { let _ = fs::remove_dir_all(stage); }
        outcome?;
        self.index_record(&record)
    }

    /// Execution: find issuance metadata in PostgreSQL, then verify on-disk bytes.
    pub fn strategies(&self, tenant: &str, account: &str) -> Result<Vec<StrategyLoad>, StoreError> {
        let indexed = self.list(tenant, account)?;
        let _guard = self.lock.lock().map_err(err)?;
        let mut output = Vec::with_capacity(indexed.len());
        for mut load in indexed {
            let dir = self.folder(&load);
            let record = read_record(&dir.join("issuance.json"))?;
            if record.load != load { return Err(conflict("indexed strategy metadata differs from disk")); }
            load.archive = self.check_artifact(&dir, &record)?;
            output.push(load);
        }
        Ok(output)
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_issuance_metadata_remains_readable_without_tenant_type() {
        let old = serde_json::json!({
            "load": {
                "tenant_id": "tenant", "account_id": "account",
                "issuance_id": "legacy", "code_key": "ema_crossover",
                "version": "1", "grant_purpose": "development",
                "parameters_json": "{}", "entry_point": "native:ema_crossover",
                "package_hash": "a".repeat(64),
            },
            "zip_sha256": null,
        });
        let record: StoredRecord = serde_json::from_value(old).unwrap();
        assert_eq!(record.load.issuance_id, "legacy");
        assert!(serde_json::to_value(&record).unwrap()["load"]
            .get("grant_purpose").is_none());
    }

    #[test]
    fn legacy_non_null_column_accepts_type_neutral_issuance() {
        if std::env::var("SEEALPHA_DATABASE_URL").is_err() { return; }
        let root = std::env::temp_dir().join(format!(
            "seealpha-legacy-purpose-{}-{}",
            std::process::id(), STAGING.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&root).unwrap();
        let connection = Connection::open(&root).unwrap();
        connection.execute_batch(INDEX_SCHEMA).unwrap();
        connection.execute_batch(
            "ALTER TABLE strategy_issuances ADD COLUMN grant_purpose TEXT NOT NULL"
        ).unwrap();
        drop(connection);
        let store = StrategyStore::open(&root).unwrap();
        let load = StrategyLoad {
            tenant_id: "tenant".into(), account_id: "account".into(),
            issuance_id: "issue".into(), code_key: "ema_crossover".into(),
            version: "1".into(), parameters_json: "{}".into(),
            entry_point: "native:ema_crossover".into(),
            package_hash: sha(b"manifest"), archive: b"manifest".to_vec(),
        };
        store.insert(&load).unwrap();
        let connection = store.index.lock().unwrap();
        let row = connection.query_one(
            "SELECT grant_purpose FROM strategy_issuances WHERE tenant_id=$1
             AND account_id=$2 AND issuance_id=$3",
            &[&"tenant", &"account", &"issue"],
        ).unwrap();
        let legacy: Option<String> = row.get(0);
        assert_eq!(legacy, None);
        drop(connection);
        assert_eq!(store.strategies("tenant", "account").unwrap(), vec![load]);
        drop(store);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn self_describing_zip_is_extracted_and_is_immutable() {
        use std::io::Write as _;
        let manifest = br#"{"format":1,"runtime":"rust-builtin","name":"ema_crossover","version":"1","entry_point":"native:ema_crossover"}"#;
        let mut buffer = std::io::Cursor::new(Vec::new());
        {
            let mut zip = zip::ZipWriter::new(&mut buffer);
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            zip.start_file("manifest.json", options).unwrap();
            zip.write_all(manifest).unwrap();
            zip.start_file("README.txt", options).unwrap();
            zip.write_all(b"native builtin manifest").unwrap();
            zip.finish().unwrap();
        }
        let bytes = buffer.into_inner();
        assert_eq!(unpack_native_zip(&bytes).unwrap().0, manifest);
        let root = std::env::temp_dir().join(format!("seealpha-zip-{}-{}",std::process::id(),STAGING.fetch_add(1,Ordering::Relaxed)));
        if std::env::var("SEEALPHA_DATABASE_URL").is_err() { return; }
        let db = StrategyStore::open(&root).unwrap();
        let load = StrategyLoad {
            tenant_id:"tenant".into(), account_id:"account".into(), issuance_id:"zip-issue".into(),
            code_key:"ema_crossover".into(), version:"1".into(),
            parameters_json:"{}".into(),entry_point:"native:ema_crossover".into(),
            package_hash:sha(manifest),archive:manifest.to_vec(),
        };
        db.insert_zip(&load,&bytes).unwrap();
        db.insert_zip(&load,&bytes).unwrap();
        let folder = db.folder(&load);
        assert!(folder.join("README.txt").exists());
        let metadata = db.list("tenant","account").unwrap();
        assert_eq!(metadata.len(),1);
        assert_eq!(metadata[0].code_key, "ema_crossover");
        assert_eq!(metadata[0].version, "1");
        assert!(metadata[0].archive.is_empty());
        assert_eq!(db.strategies("tenant","account").unwrap(),vec![load.clone()]);
        let mut changed = bytes.clone(); changed.push(0);
        assert!(db.insert_zip(&load,&changed).is_err());
        drop(db);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn zip_rejects_parent_traversal() {
        use std::io::Write as _;
        let mut buffer = std::io::Cursor::new(Vec::new());
        {
            let mut zip = zip::ZipWriter::new(&mut buffer);
            zip.start_file("../evil", zip::write::SimpleFileOptions::default()).unwrap();
            zip.write_all(b"x").unwrap();
            zip.finish().unwrap();
        }
        assert!(unpack_native_zip(&buffer.into_inner()).is_err());
    }
    #[test]
    fn persists_and_rejects_changed_issuance() {
        if std::env::var("SEEALPHA_DATABASE_URL").is_err() { return; }
        let root = std::env::temp_dir().join(format!("seealpha-fs-{}-{}",std::process::id(),STAGING.fetch_add(1,Ordering::Relaxed)));
        let load=StrategyLoad{tenant_id:"tenant".into(),account_id:"account".into(),issuance_id:"issue".into(),
            code_key:"ema_crossover".into(),version:"1".into(),
            parameters_json:"{}".into(),entry_point:"native:ema_crossover".into(),
            package_hash:sha(b"manifest"),archive:b"manifest".to_vec()};
        {let db=StrategyStore::open(&root).unwrap();db.insert(&load).unwrap();db.insert(&load).unwrap();
         let mut altered=load.clone();altered.archive=b"other".to_vec();altered.package_hash=sha(b"other");assert!(db.insert(&altered).is_err());}
        let db=StrategyStore::open(&root).unwrap();
        assert_eq!(db.strategies("tenant","account").unwrap(),vec![load]);
        let conn = db.index.lock().unwrap();
        let columns = conn.query(
            "SELECT column_name FROM information_schema.columns
             WHERE table_schema=current_schema() AND table_name='strategy_issuances'
             AND column_name IN ('archive','package_blob')", &[]
        ).unwrap();
        assert!(columns.is_empty());
        drop(conn);
        assert!(db.strategies("tenant","another").unwrap().is_empty());
        drop(db);let _=fs::remove_dir_all(root);
    }
}

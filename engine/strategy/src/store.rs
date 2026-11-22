//! Strategy package and issuance persistence on an immutable filesystem.
//! A legacy Ledger SQLite reader is retained only for one-way migration.
use rusqlite::Connection;
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
    pub grant_purpose: String,
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
pub struct StrategyStore { root: PathBuf, lock: Mutex<()> }

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
/// Accept ZIP transport with a root manifest.json. Reject unsafe paths,
/// symlinks, encryption, duplicate names and decompression bombs.
pub fn unpack_native_zip(bytes: &[u8]) -> Result<(Vec<u8>, Vec<(PathBuf, Vec<u8>)>), StoreError> {
    if bytes.is_empty() || bytes.len() > MAX_ZIP { return Err(conflict("ZIP too large")); }
    let mut zip = ZipArchive::new(std::io::Cursor::new(bytes)).map_err(err)?;
    if zip.len() == 0 || zip.len() > MAX_FILES { return Err(conflict("ZIP file count invalid")); }
    let mut seen = std::collections::HashSet::new();
    let mut files = Vec::new();
    let mut total = 0_u64;
    for i in 0..zip.len() {
        let mut item = zip.by_index(i).map_err(err)?;
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
        Ok(Self { root: root.into(), lock: Mutex::new(()) })
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
            || !matches!(load.grant_purpose.as_str(),"production"|"development")
            || load.package_hash != sha(&load.archive)
            || serde_json::from_str::<serde_json::Value>(&load.parameters_json).is_err()
            || load.entry_point.is_empty()
        { return Err(conflict("invalid issuance identity or manifest digest")); }
        let expanded = zip.map(unpack_native_zip).transpose()?;
        if let Some((manifest,_)) = &expanded {
            if manifest != &load.archive { return Err(conflict("ZIP manifest differs from trusted bytes")); }
        }
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
            if let Some(z) = zip {
                if sha(&fs::read(final_dir.join("package.zip")).map_err(err)?) != sha(z) {
                    return Err(conflict("stored ZIP was modified"));
                }
            }
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
        outcome
    }

    pub fn strategies(&self, tenant: &str, account: &str) -> Result<Vec<StrategyLoad>, StoreError> {
        if !valid_id(tenant) || !valid_id(account) { return Err(conflict("invalid scope")); }
        let _guard = self.lock.lock().map_err(err)?;
        let scope = self.root.join(tenant).join(account);
        if !scope.exists() { return Ok(Vec::new()); }
        let mut output = Vec::new();
        for entry in fs::read_dir(scope).map_err(err)? {
            let entry = entry.map_err(err)?;
            if !entry.file_type().map_err(err)?.is_dir() || entry.file_name().to_string_lossy().starts_with('.') { continue; }
            let path = entry.path();
            let record = read_record(&path.join("issuance.json"))?;
            if record.load.tenant_id != tenant || record.load.account_id != account
                || record.load.issuance_id != entry.file_name().to_string_lossy()
            { return Err(conflict("stored scope mismatch")); }
            let manifest = path.join("manifest.json");
            let artifact = if manifest.exists() { manifest } else { path.join("legacy-archive.bin") };
            let raw = fs::read(artifact).map_err(err)?;
            if sha(&raw) != record.load.package_hash { return Err(conflict("stored manifest digest mismatch")); }
            if let Some(digest) = &record.zip_sha256 {
                let zip_bytes = fs::read(path.join("package.zip")).map_err(err)?;
                if sha(&zip_bytes) != *digest { return Err(conflict("stored ZIP digest mismatch")); }
                let (_, files) = unpack_native_zip(&zip_bytes)?;
                for (relative, expected) in files {
                    if fs::read(path.join(relative)).map_err(err)? != expected {
                        return Err(conflict("extracted strategy file was modified"));
                    }
                }
            }
            let mut load = record.load;
            load.archive = raw;
            output.push(load);
        }
        output.sort_by(|a,b| a.issuance_id.cmp(&b.issuance_id));
        Ok(output)
    }
    /// Read-only one-way migration, never deletes legacy rows.
    pub fn import_legacy_ledger(&self, book: &Path) -> Result<(), StoreError> {
        let conn = Connection::open_with_flags(book,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(err)?;
        let mut stmt = conn.prepare(
            "SELECT tenant_id,account_id,issuance_id,code_key,version,grant_purpose,
                    parameters_json,entry_point,package_hash,archive
             FROM strategy_loads ORDER BY tenant_id,account_id,issuance_id"
        ).map_err(err)?;
        let rows = stmt.query_map([],|r| Ok(StrategyLoad {
            tenant_id:r.get(0)?,account_id:r.get(1)?,issuance_id:r.get(2)?,
            code_key:r.get(3)?,version:r.get(4)?,grant_purpose:r.get(5)?,
            parameters_json:r.get(6)?,entry_point:r.get(7)?,
            package_hash:r.get(8)?,archive:r.get(9)?,
        })).map_err(err)?;
        for row in rows { self.insert(&row.map_err(err)?)?; }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let db = StrategyStore::open(&root).unwrap();
        let load = StrategyLoad {
            tenant_id:"tenant".into(), account_id:"account".into(), issuance_id:"zip-issue".into(),
            code_key:"ema_crossover".into(), version:"1".into(), grant_purpose:"development".into(),
            parameters_json:"{}".into(),entry_point:"native:ema_crossover".into(),
            package_hash:sha(manifest),archive:manifest.to_vec(),
        };
        db.insert_zip(&load,&bytes).unwrap();
        db.insert_zip(&load,&bytes).unwrap();
        let folder = db.folder(&load);
        assert!(folder.join("README.txt").exists());
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
        let root = std::env::temp_dir().join(format!("seealpha-fs-{}-{}",std::process::id(),STAGING.fetch_add(1,Ordering::Relaxed)));
        let load=StrategyLoad{tenant_id:"tenant".into(),account_id:"account".into(),issuance_id:"issue".into(),
            code_key:"ema_crossover".into(),version:"1".into(),grant_purpose:"development".into(),
            parameters_json:"{}".into(),entry_point:"native:ema_crossover".into(),
            package_hash:sha(b"manifest"),archive:b"manifest".to_vec()};
        {let db=StrategyStore::open(&root).unwrap();db.insert(&load).unwrap();db.insert(&load).unwrap();
         let mut altered=load.clone();altered.archive=b"other".to_vec();altered.package_hash=sha(b"other");assert!(db.insert(&altered).is_err());}
        let db=StrategyStore::open(&root).unwrap();
        assert_eq!(db.strategies("tenant","account").unwrap(),vec![load]);
        assert!(db.strategies("tenant","another").unwrap().is_empty());
        drop(db);let _=fs::remove_dir_all(root);
    }
}

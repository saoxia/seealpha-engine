//! Binance USD-M completed-UTC-day trade archive synchronization.
//! This module performs no trading and never treats a downloaded ZIP as
//! evidence that Nautilus TradeTick conversion has succeeded.
use std::{fs::{self, File, OpenOptions}, io::{Read, Write}, path::{Path, PathBuf}, time::Duration};
use chrono::{NaiveDate, Utc};
use fs2::FileExt;
use sha2::{Digest, Sha256};

const BASE: &str = "https://data.binance.vision/";
const MAX_CHECKSUM: usize = 64 * 1024;
const MAX_DAYS: i64 = 3660;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveDay {
    pub symbol: String,
    pub day: NaiveDate,
}

impl ArchiveDay {
    pub fn remote_path(&self) -> String {
        format!("data/futures/um/daily/trades/{0}/{0}-trades-{1}.zip", self.symbol, self.day)
    }
    pub fn local_path(&self, root: &Path) -> PathBuf {
        root.join(self.remote_path().trim_start_matches("data/"))
    }
    pub fn url(&self) -> String { format!("{BASE}{}", self.remote_path()) }
}

/// Matches Python's left-closed, right-open UTC date planning. Never request
/// today's not-yet-complete ZIP from the public daily archive.
pub fn plan(symbols: &[String], start: NaiveDate, end: NaiveDate, today: NaiveDate)
    -> Result<Vec<ArchiveDay>, String>
{
    if symbols.is_empty() || start >= end || end > today {
        return Err("require nonempty [start,end) of completed UTC days".into());
    }
    let days = end.signed_duration_since(start).num_days();
    if days > MAX_DAYS || days.saturating_mul(symbols.len() as i64) > 100_000 {
        return Err("archive plan exceeds bounded request size".into());
    }
    let mut normalized = Vec::new();
    for symbol in symbols {
        let canonical = super::normalize_symbol(symbol, super::Product::FuturesUm)
            .ok_or_else(|| format!("invalid USD-M symbol: {symbol}"))?;
        if normalized.contains(&canonical) {
            return Err(format!("duplicate USD-M symbol: {canonical}"));
        }
        normalized.push(canonical);
    }
    let mut rows = Vec::new();
    for offset in 0..days {
        let date = start.checked_add_signed(chrono::Duration::days(offset))
            .ok_or("date overflow")?;
        for symbol in &normalized {
            rows.push(ArchiveDay {symbol: symbol.clone(), day: date});
        }
    }
    Ok(rows)
}

/// Official sha256sum line: checksum followed by an optional matching filename.
pub fn parse_checksum(bytes: &[u8], expected_filename: &str) -> Result<[u8; 32], String> {
    if bytes.len() > MAX_CHECKSUM { return Err("CHECKSUM exceeds 64 KiB".into()); }
    let source = std::str::from_utf8(bytes).map_err(|_| "invalid CHECKSUM UTF-8")?;
    let source = source.trim_start_matches('\u{feff}');
    let mut lines = source.lines().map(str::trim).filter(|s| !s.is_empty());
    let line = lines.next().ok_or("empty CHECKSUM")?;
    if lines.next().is_some() { return Err("CHECKSUM must contain one entry".into()); }
    let mut parts = line.split_ascii_whitespace();
    let hex = parts.next().ok_or("missing SHA256")?;
    if hex.len()!=64 || !hex.bytes().all(|b|b.is_ascii_hexdigit()) {
        return Err("invalid SHA256 digest".into());
    }
    if let Some(filename) = parts.next() {
        let filename = filename.trim_start_matches('*');
        if filename != expected_filename || parts.next().is_some() {
            return Err("CHECKSUM refers to another filename".into());
        }
    }
    let mut bytes=[0_u8;32];
    for (i, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let nibble = |b:u8| -> u8 { if b.is_ascii_digit(){b-b'0'} else {b.to_ascii_lowercase()-b'a'+10} };
        bytes[i]=(nibble(chunk[0])<<4)|nibble(chunk[1]);
    }
    Ok(bytes)
}

/// Revalidate an existing archive and its official checksum, with no network.
pub fn verify_local_archive(entry:&ArchiveDay,root:&Path)->Result<(),String>{
    let path=entry.local_path(root);
    let checksum=PathBuf::from(format!("{}.CHECKSUM",path.display()));
    let relative=path.parent().ok_or("archive parent missing")?;
    // Do not follow unexpected symlinks in the archive path.
    let mut cursor=root.to_path_buf();
    if fs::symlink_metadata(&cursor).map_err(|e|e.to_string())?.file_type().is_symlink(){
        return Err("archive root symlink".into());
    }
    for component in relative.strip_prefix(root).map_err(|_|"archive escaped root")?.components(){
        cursor.push(component.as_os_str());
        if !fs::symlink_metadata(&cursor).map_err(|e|e.to_string())?.is_dir(){
            return Err("archive path contains non-directory".into());
        }
    }
    for item in [&path,&checksum]{
        if !fs::symlink_metadata(item).map_err(|e|e.to_string())?.file_type().is_file(){
            return Err("archive file is not ordinary".into());
        }
    }
    let bytes=fs::read(&checksum).map_err(|e|e.to_string())?;
    let name=path.file_name().and_then(|v|v.to_str()).ok_or("invalid archive filename")?;
    let expected=parse_checksum(&bytes,name)?;
    if file_hash(&path)?!=expected{return Err("local archive SHA256 mismatch".into());}
    Ok(())
}

fn file_hash(path: &Path) -> Result<[u8;32], String> {
    let mut file=File::open(path).map_err(|e|e.to_string())?;
    let mut sha=Sha256::new();
    let mut buffer=[0u8;1024*1024];
    loop {
        let n=file.read(&mut buffer).map_err(|e|e.to_string())?;
        if n==0 {break;}
        sha.update(&buffer[..n]);
    }
    Ok(sha.finalize().into())
}

fn ordinary_or_missing(path:&Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_file()=>Ok(()),
        Ok(_)=>Err(format!("not an ordinary file: {}",path.display())),
        Err(e) if e.kind()==std::io::ErrorKind::NotFound=>Ok(()),
        Err(e)=>Err(e.to_string()),
    }
}

fn safe_directory(root:&Path, parent:&Path) -> Result<(),String> {
    fs::create_dir_all(root).map_err(|e|e.to_string())?;
    let relative=parent.strip_prefix(root).map_err(|_|"path outside archive root")?;
    let mut cursor=root.to_path_buf();
    if fs::symlink_metadata(&cursor).map_err(|e|e.to_string())?.file_type().is_symlink() {
        return Err("archive root is a symlink".into());
    }
    for part in relative.components() {
        cursor.push(part.as_os_str());
        match fs::symlink_metadata(&cursor) {
            Ok(meta) if meta.file_type().is_dir()=>{},
            Ok(_)=>return Err("archive path contains symlink/non-directory".into()),
            Err(e) if e.kind()==std::io::ErrorKind::NotFound=>{
                fs::create_dir(&cursor).map_err(|e|e.to_string())?;
            }
            Err(e)=>return Err(e.to_string()),
        }
    }
    Ok(())
}

async fn fetch_checksum(client:&reqwest::Client, entry:&ArchiveDay)
    -> Result<([u8;32],Vec<u8>),String>
{
    let url=format!("{}.CHECKSUM",entry.url());
    let response=client.get(&url).send().await.map_err(|e|e.to_string())?
        .error_for_status().map_err(|e|e.to_string())?;
    if response.content_length().is_some_and(|len|len>MAX_CHECKSUM as u64) {
        return Err("CHECKSUM exceeds 64 KiB".into());
    }
    let bytes=response.bytes().await.map_err(|e|e.to_string())?;
    let name=format!("{}-trades-{}.zip",entry.symbol,entry.day);
    let digest=parse_checksum(&bytes,&name)?;
    Ok((digest,bytes.to_vec()))
}

async fn download(client:&reqwest::Client, entry:&ArchiveDay, part:&Path, digest:&[u8;32])
    -> Result<(),String>
{
    // Restart instead of appending ambiguous HTTP ranges. A failed attempt
    // never becomes a published archive; staging has an exclusive lock.
    let response=client.get(entry.url()).send().await.map_err(|e|e.to_string())?
        .error_for_status().map_err(|e|e.to_string())?;
    let expected_len=response.content_length();
    let mut file=OpenOptions::new().create(true).truncate(true).write(true)
        .open(part).map_err(|e|e.to_string())?;
    let mut response=response;
    let mut hasher=Sha256::new();
    let mut length=0u64;
    while let Some(chunk)=response.chunk().await.map_err(|e|e.to_string())? {
        file.write_all(&chunk).map_err(|e|e.to_string())?;
        hasher.update(&chunk);
        length=length.checked_add(chunk.len() as u64).ok_or("download size overflow")?;
    }
    file.sync_all().map_err(|e|e.to_string())?;
    if expected_len.is_some_and(|n|n!=length) {return Err("truncated archive download".into());}
    let actual:[u8;32]=hasher.finalize().into();
    if &actual!=digest {return Err("archive SHA256 mismatch".into());}
    Ok(())
}

/// Return the verified archive. A fully verified local copy is reusable
/// without network access unless refresh=true.
pub async fn sync(client:&reqwest::Client, entry:&ArchiveDay, root:&Path, refresh:bool)
    -> Result<PathBuf,String>
{
    let target=entry.local_path(root);
    let parent=target.parent().ok_or("archive has no parent")?;
    safe_directory(root,parent)?;
    let lock_path=target.with_extension("zip.lock");
    ordinary_or_missing(&lock_path)?;
    let lock=OpenOptions::new().create(true).write(true).open(&lock_path)
        .map_err(|e|e.to_string())?;
    lock.lock_exclusive().map_err(|e|e.to_string())?;
    let result=sync_locked(client,entry,&target,refresh).await;
    let _=FileExt::unlock(&lock);
    result
}

async fn sync_locked(client:&reqwest::Client,entry:&ArchiveDay,target:&Path,refresh:bool)
    -> Result<PathBuf,String>
{
    let checksum_path=PathBuf::from(format!("{}.CHECKSUM",target.display()));
    let part=target.with_extension("zip.part");
    let checksum_part=target.with_extension("zip.CHECKSUM.part");
    for path in [target,&checksum_path,&part,&checksum_part] { ordinary_or_missing(path)?; }
    if !refresh && target.is_file() && checksum_path.is_file() {
        let data=fs::read(&checksum_path).map_err(|e|e.to_string())?;
        let name=target.file_name().and_then(|s|s.to_str()).ok_or("invalid filename")?;
        if let Ok(expected)=parse_checksum(&data,name) {
            if file_hash(target)?==expected {return Ok(target.to_path_buf());}
        }
    }
    let mut last_error=String::new();
    for attempt in 0..3 {
        if attempt>0 {tokio::time::sleep(Duration::from_secs(1<<attempt)).await;}
        match fetch_checksum(client,entry).await {
            Ok((digest,checksum))=>{
                match download(client,entry,&part,&digest).await {
                    Ok(())=>{
                        // Only install a verified file. CHECKSUM is installed
                        // afterward; missing checksum means no offline trust.
                        fs::write(&checksum_part,&checksum).map_err(|e|e.to_string())?;
                        fs::rename(&part,target).map_err(|e|e.to_string())?;
                        fs::rename(&checksum_part,&checksum_path).map_err(|e|e.to_string())?;
                        return Ok(target.to_path_buf());
                    }
                    Err(e)=>last_error=e,
                }
            }
            Err(e)=>last_error=e,
        }
        let _=fs::remove_file(&part);
        let _=fs::remove_file(&checksum_part);
    }
    Err(format!("{}: {last_error}",entry.remote_path()))
}

pub fn today_utc()->NaiveDate {Utc::now().date_naive()}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn plan_is_left_closed_right_open_and_rejects_today() {
        let start=NaiveDate::from_ymd_opt(2026,11,18).unwrap();
        let end=NaiveDate::from_ymd_opt(2026,11,20).unwrap();
        let today=NaiveDate::from_ymd_opt(2026,11,21).unwrap();
        let planned=plan(&["eth/usdt".into()],start,end,today).unwrap();
        assert_eq!(planned.len(),2);
        assert_eq!(planned[0].remote_path(),
            "data/futures/um/daily/trades/ETHUSDT/ETHUSDT-trades-2026-11-18.zip");
        assert!(plan(&["ETHUSDT".into()],start,today.succ_opt().unwrap(),today).is_err());
        assert!(plan(&["ETHUSDT".into(),"ethusdt".into()],start,end,today).is_err());
    }
    #[test]
    fn checksum_parser_rejects_wrong_filename_and_multiline() {
        let digest="a".repeat(64);
        let good=format!("{digest}  ETHUSDT-trades-2026-11-18.zip\n");
        assert_eq!(parse_checksum(good.as_bytes(),"ETHUSDT-trades-2026-11-18.zip")
            .unwrap(),[0xaa;32]);
        assert!(parse_checksum(good.as_bytes(),"BTCUSDT-trades-2026-11-18.zip").is_err());
        assert!(parse_checksum(format!("{good}{good}").as_bytes(),
            "ETHUSDT-trades-2026-11-18.zip").is_err());
        assert!(parse_checksum(b"abcdef","x.zip").is_err());
    }
}

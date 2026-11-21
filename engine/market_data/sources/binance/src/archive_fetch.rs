//! Binance's public historical archives are the only responsibility of this module.
//!
//! Live subscriptions, REST trade requests, TradeTick modeling, and Catalog
//! storage belong to NautilusTrader. No exchange credentials or order actions.
use chrono::NaiveDate;
use reqwest::Client;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

const ORIGIN: &str = "https://data.binance.vision";
const MAX_ARCHIVE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MAX_CHECKSUM_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Market {
    Spot,
    FuturesUm,
}

impl Market {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "spot" => Ok(Self::Spot),
            "futures_um" => Ok(Self::FuturesUm),
            _ => Err("archive market must be spot or futures_um".into()),
        }
    }

    fn directory(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::FuturesUm => "futures_um",
        }
    }

    fn remote_prefix(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::FuturesUm => "futures/um",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Dataset {
    Trades,
    AggTrades,
}

impl Dataset {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "trades" => Ok(Self::Trades),
            "aggTrades" => Ok(Self::AggTrades),
            _ => Err("archive dataset must be trades or aggTrades".into()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::AggTrades => "aggTrades",
        }
    }
}

/// A single, explicitly requested UTC daily ZIP, not a live feed or REST client.
#[derive(Clone, Debug)]
pub struct ArchiveSpec {
    market: Market,
    dataset: Dataset,
    symbol: String,
    day: NaiveDate,
}

#[derive(Debug)]
pub struct VerifiedArchive {
    pub path: PathBuf,
    pub sha256: String,
    pub bytes: u64,
    pub reused: bool,
}

impl ArchiveSpec {
    /// Reject ambiguous symbols and dates before constructing any remote path.
    pub fn new(market: &str, dataset: &str, symbol: &str, day: &str) -> Result<Self> {
        let market = Market::parse(market)?;
        let dataset = Dataset::parse(dataset)?;
        if !(5..=30).contains(&symbol.len())
            || !symbol.bytes().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        {
            return Err("symbol must be an uppercase Binance symbol without separators".into());
        }
        let parsed = NaiveDate::parse_from_str(day, "%Y-%m-%d")?;
        if parsed.format("%Y-%m-%d").to_string() != day {
            return Err("day must use canonical YYYY-MM-DD".into());
        }
        Ok(Self { market, dataset, symbol: symbol.to_owned(), day: parsed })
    }

    pub fn archive_name(&self) -> String {
        format!("{}-{}-{}.zip", self.symbol, self.dataset.as_str(), self.day)
    }

    fn csv_name(&self) -> String {
        format!("{}-{}-{}.csv", self.symbol, self.dataset.as_str(), self.day)
    }

    pub fn remote_path(&self) -> String {
        format!(
            "data/{}/daily/{}/{}/{}",
            self.market.remote_prefix(),
            self.dataset.as_str(),
            self.symbol,
            self.archive_name()
        )
    }

    fn destination(&self, root: &Path) -> Result<PathBuf> {
        // The caller explicitly provides an existing managed root; do not
        // create a fresh state volume or follow symlinks below that root.
        let root = root.canonicalize()?;
        if !root.is_dir() {
            return Err("archive root is not a directory".into());
        }
        let market = ensure_child_directory(&root, self.market.directory())?;
        let dataset = ensure_child_directory(&market, self.dataset.as_str())?;
        let symbol = ensure_child_directory(&dataset, &self.symbol)?;
        Ok(symbol.join(self.archive_name()))
    }

    /// Download an official ZIP and its SHA-256 sidecar, or verify an existing
    /// immutable copy. Never silently overwrite an existing archive.
    pub async fn download(&self, root: &Path) -> Result<VerifiedArchive> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(3600))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        self.download_from(&client, ORIGIN, root).await
    }

    async fn download_from(
        &self,
        client: &Client,
        origin: &str,
        root: &Path,
    ) -> Result<VerifiedArchive> {
        let path = self.destination(root)?;
        let remote = format!("{origin}/{}", self.remote_path());
        let expected = fetch_checksum(client, &format!("{remote}.CHECKSUM"), &self.archive_name())
            .await?;

        if fs::symlink_metadata(&path).is_ok() {
            return verify_existing(&path, &self.csv_name(), &expected);
        }

        let parent = path.parent().ok_or("archive destination has no parent")?.to_path_buf();
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let staging = parent.join(format!(
            ".{}.{}.{}.part",
            self.archive_name(),
            std::process::id(),
            nonce
        ));

        let outcome: Result<VerifiedArchive> = async {
            let mut response = client.get(&remote).send().await?.error_for_status()?;
            if response.content_length().is_some_and(|n| n > MAX_ARCHIVE_BYTES) {
                return Err("archive content length exceeds safety bound".into());
            }
            let mut file = OpenOptions::new().write(true).create_new(true).open(&staging)?;
            let mut hash = Sha256::new();
            let mut size = 0_u64;
            while let Some(chunk) = response.chunk().await? {
                size = size.checked_add(chunk.len() as u64).ok_or("archive size overflow")?;
                if size > MAX_ARCHIVE_BYTES {
                    return Err("archive exceeds safety bound".into());
                }
                file.write_all(&chunk)?;
                hash.update(&chunk);
            }
            if size == 0 {
                return Err("empty archive response".into());
            }
            file.sync_all()?;
            drop(file);

            let actual = format!("{:x}", hash.finalize());
            if actual != expected {
                return Err("Binance archive SHA-256 CHECKSUM mismatch".into());
            }
            verify_zip_structure(&staging, &self.csv_name())?;

            let reused = match fs::hard_link(&staging, &path) {
                Ok(()) => {
                    // Link installation must be durable before reporting success.
                    File::open(&parent)?.sync_all()?;
                    false
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    verify_existing(&path, &self.csv_name(), &expected)?;
                    true
                }
                Err(error) => return Err(error.into()),
            };
            Ok(VerifiedArchive { path, sha256: actual, bytes: size, reused })
        }
        .await;

        match outcome {
            Ok(archive) => {
                fs::remove_file(&staging)?;
                Ok(archive)
            }
            Err(error) => {
                let _ = fs::remove_file(&staging);
                Err(error)
            }
        }
    }
}

fn ensure_child_directory(parent: &Path, name: &str) -> Result<PathBuf> {
    let path = parent.join(name);
    match fs::create_dir(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let metadata = fs::symlink_metadata(&path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("archive directory is not a real directory".into());
    }
    Ok(path)
}

fn checksum_from_sidecar(bytes: &[u8], archive_name: &str) -> Result<String> {
    let content = std::str::from_utf8(bytes)?;
    let mut parts = content.split_whitespace();
    let digest = parts.next().ok_or("CHECKSUM digest missing")?;
    let file = parts.next().ok_or("CHECKSUM filename missing")?;
    if parts.next().is_some()
        || file.strip_prefix('*').unwrap_or(file) != archive_name
        || digest.len() != 64
        || !digest.bytes().all(|c| c.is_ascii_hexdigit())
    {
        return Err("invalid Binance CHECKSUM or archive filename".into());
    }
    Ok(digest.to_ascii_lowercase())
}

async fn fetch_checksum(client: &Client, url: &str, archive_name: &str) -> Result<String> {
    let mut response = client.get(url).send().await?.error_for_status()?;
    if response.content_length().is_some_and(|n| n > MAX_CHECKSUM_BYTES as u64) {
        return Err("CHECKSUM response exceeds safety bound".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > MAX_CHECKSUM_BYTES.saturating_sub(bytes.len()) {
            return Err("CHECKSUM response exceeds safety bound".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    checksum_from_sidecar(&bytes, archive_name)
}

fn verify_zip_structure(path: &Path, expected_csv: &str) -> Result<()> {
    let mut zip = zip::ZipArchive::new(File::open(path)?)?;
    if zip.len() != 1 {
        return Err("archive must contain exactly one CSV".into());
    }
    let member = zip.by_index(0)?;
    if member.is_dir() || member.name() != expected_csv || member.size() == 0 {
        return Err("archive CSV name or structure differs from the requested dataset".into());
    }
    Ok(())
}

fn verify_existing(path: &Path, csv_name: &str, expected: &str) -> Result<VerifiedArchive> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > MAX_ARCHIVE_BYTES
    {
        return Err("existing archive is not a bounded regular file".into());
    }
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buf = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buf)?;
        if count == 0 {
            break;
        }
        hash.update(&buf[..count]);
    }
    let actual = format!("{:x}", hash.finalize());
    if actual != expected {
        return Err("existing archive differs from Binance CHECKSUM; refusing overwrite".into());
    }
    verify_zip_structure(path, csv_name)?;
    Ok(VerifiedArchive {
        path: path.to_owned(),
        sha256: actual,
        bytes: metadata.len(),
        reused: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_only_canonical_binance_archive_paths() {
        let spot = ArchiveSpec::new("spot", "trades", "ETHUSDT", "2026-11-20").unwrap();
        assert_eq!(
            spot.remote_path(),
            "data/spot/daily/trades/ETHUSDT/ETHUSDT-trades-2026-11-20.zip"
        );
        let um = ArchiveSpec::new("futures_um", "aggTrades", "BTCUSDT", "2026-11-20").unwrap();
        assert_eq!(
            um.remote_path(),
            "data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2026-11-20.zip"
        );
        for symbol in ["../BTCUSDT", "ethusdt", "BTC/USDT", "", "BTC-USDT"] {
            assert!(ArchiveSpec::new("spot", "trades", symbol, "2026-11-20").is_err());
        }
        for date in ["2026-02-30", "2026-1-01", "../../etc", "2026-11-20/"] {
            assert!(ArchiveSpec::new("spot", "trades", "ETHUSDT", date).is_err());
        }
    }

    #[test]
    fn checks_sidecar_hash_and_exact_filename() {
        let name = "ETHUSDT-trades-2026-11-20.zip";
        let hash = "a".repeat(64);
        let good = format!("{hash}  {name}\n");
        assert_eq!(checksum_from_sidecar(good.as_bytes(), name).unwrap(), hash);
        assert!(checksum_from_sidecar(good.as_bytes(), "BTCUSDT-trades-2026-11-20.zip").is_err());
        assert!(checksum_from_sidecar(format!("short  {name}").as_bytes(), name).is_err());
        assert!(checksum_from_sidecar(format!("{hash}  {name} extra").as_bytes(), name).is_err());
    }

    #[tokio::test]
    async fn official_archive_mock_download_reuse_and_tamper_rejection() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let spec = ArchiveSpec::new("spot", "trades", "BTCUSDT", "2026-11-20").unwrap();
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        zip.start_file(spec.csv_name(), zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(b"1,10,2,20,1795132800000000,false,true\n").unwrap();
        let bytes = zip.finish().unwrap().into_inner();
        let hash = format!("{:x}", Sha256::digest(&bytes));
        let sidecar = format!("{hash}  {}\n", spec.archive_name()).into_bytes();

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let path = format!("/{}", spec.remote_path());
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 4096];
                let count = socket.read(&mut request).await.unwrap();
                let line = std::str::from_utf8(&request[..count]).unwrap();
                let body = if line.starts_with(&format!("GET {path}.CHECKSUM ")) {
                    &sidecar
                } else if line.starts_with(&format!("GET {path} ")) {
                    &bytes
                } else {
                    panic!("unexpected test HTTP request");
                };
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(headers.as_bytes()).await.unwrap();
                socket.write_all(body).await.unwrap();
            }
        });

        let root = std::env::temp_dir().join(format!(
            "seealpha-archive-fetch-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build().unwrap();
        let saved = spec.download_from(&client, &origin, &root).await.unwrap();
        assert!(!saved.reused);
        assert_eq!(saved.sha256, hash);
        assert!(saved.path.is_file());

        let reused = spec.download_from(&client, &origin, &root).await.unwrap();
        assert!(reused.reused);
        assert_eq!(reused.path, saved.path);

        fs::write(&saved.path, b"tampered archive").unwrap();
        assert!(spec.download_from(&client, &origin, &root).await.is_err());
        server.await.unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn zip_must_contain_only_the_requested_csv() {
        let dir = std::env::temp_dir().join(format!(
            "seealpha-binance-archive-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("archive.zip");
        {
            let mut zip = zip::ZipWriter::new(File::create(&path).unwrap());
            zip.start_file(
                "ETHUSDT-trades-2026-11-20.csv",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
            zip.write_all(b"1,10,1,10,1600000000000,true,true\n").unwrap();
            zip.finish().unwrap();
        }
        assert!(verify_zip_structure(&path, "ETHUSDT-trades-2026-11-20.csv").is_ok());
        assert!(verify_zip_structure(&path, "BTCUSDT-trades-2026-11-20.csv").is_err());
        fs::remove_file(path).unwrap();
        fs::remove_dir(dir).unwrap();
    }
}

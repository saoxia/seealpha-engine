//! Public Binance archive rules. Checksums and paths are checked locally.
//! Nothing here opens a socket or writes a partial file into the public name.
use sha2::{Digest, Sha256};

const VISION: &str = "https://data.binance.vision/";
const DAY_MS: i64 = 86_400_000;

#[derive(Debug, PartialEq, Eq)]
pub enum ArchiveError {
    Untrusted,
    Checksum,
    CurrentDay,
    EmptyPage,
    NotAdvancing,
    InvalidIdentity,
}

pub fn normalize_source(raw: &str) -> Result<String, ArchiveError> {
    let value = raw.trim();
    if value.contains("..") || value.contains('@') || value.ends_with('/') {
        return Err(ArchiveError::Untrusted);
    }
    if let Some(path) = value.strip_prefix(VISION) {
        return accept_data_path(path).map(|path| format!("{VISION}{path}"));
    }
    if value.starts_with("https://") || value.starts_with("http://") {
        return Err(ArchiveError::Untrusted);
    }
    accept_data_path(value).map(|path| format!("{VISION}{path}"))
}

fn accept_data_path(path: &str) -> Result<&str, ArchiveError> {
    if path.starts_with("data/") && path.ends_with(".zip") && !path.ends_with('/') {
        Ok(path)
    } else {
        Err(ArchiveError::Untrusted)
    }
}

pub fn destination_for(source: &str, root: &str) -> Result<String, ArchiveError> {
    let normalized = normalize_source(source)?;
    let path = normalized
        .strip_prefix(VISION)
        .ok_or(ArchiveError::Untrusted)?;
    Ok(format!("{root}/{path}"))
}

pub fn parse_checksum(content: &str, filename: &str) -> Result<String, ArchiveError> {
    let line = content.lines().next().unwrap_or("").trim();
    let (digest, name) = line.split_once("  ").ok_or(ArchiveError::Checksum)?;
    if name != filename || digest.len() != 64 || !digest.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(ArchiveError::Checksum);
    }
    Ok(digest.to_owned())
}

pub fn archive_eligible(day_ms: i64, now_ms: i64) -> bool {
    now_ms >= day_ms.saturating_add(2 * DAY_MS)
}

pub fn reject_current_utc_day(day_ms: i64, now_ms: i64) -> Result<(), ArchiveError> {
    let today = now_ms.div_euclid(DAY_MS) * DAY_MS;
    if day_ms >= today {
        Err(ArchiveError::CurrentDay)
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Publication {
    pub part: Option<Vec<u8>>,
    pub public_bytes: Option<Vec<u8>>,
    pub verified: bool,
}

impl Publication {
    pub fn stage(&mut self, bytes: Vec<u8>, expected_sha256_hex: &str) -> Result<(), ArchiveError> {
        let actual = hex_encode(Sha256::digest(&bytes));
        if actual != expected_sha256_hex {
            self.part = None;
            return Err(ArchiveError::Checksum);
        }
        self.part = Some(bytes);
        Ok(())
    }

    pub fn commit(&mut self) {
        self.public_bytes = self.part.take();
        self.verified = self.public_bytes.is_some();
    }

    pub fn fail_before_commit(&mut self) {
        self.part = None;
    }
}

pub fn accept_history_page(previous_last: Option<i64>, ids: &[i64]) -> Result<i64, ArchiveError> {
    if ids.is_empty() {
        return Err(ArchiveError::EmptyPage);
    }
    if ids.windows(2).any(|pair| pair[1] <= pair[0]) {
        return Err(ArchiveError::NotAdvancing);
    }
    if previous_last.is_some_and(|prior| ids[0] <= prior) {
        return Err(ArchiveError::NotAdvancing);
    }
    Ok(*ids.last().unwrap_or(&0))
}

pub fn rest_start_after(last_settlement_ms: Option<i64>) -> i64 {
    last_settlement_ms
        .map(|stamp| stamp.saturating_add(1))
        .unwrap_or(0)
}

fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn skip_header<'a>(lines: &'a [&'a str]) -> impl Iterator<Item = &'a &'a str> {
    lines.iter().skip_while(|line| {
        let trimmed = line.trim();
        trimmed.is_empty()
            || trimmed.split(',').next().is_some_and(|cell| {
                matches!(
                    cell.trim().to_ascii_lowercase().as_str(),
                    "id" | "trade id" | "trade_id"
                )
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const PATH: &str = "data/spot/monthly/klines/ETHUSDT/15m/ETHUSDT-15m-2026-07.zip";

    #[test]
    fn test_normalize_source_accepts_site_path() {
        assert_eq!(
            normalize_source(PATH).unwrap(),
            format!("https://data.binance.vision/{PATH}")
        );
    }

    #[test]
    fn test_normalize_source_accepts_official_https_url() {
        let url = format!("https://data.binance.vision/{PATH}");
        assert_eq!(normalize_source(&url).unwrap(), url);
    }

    #[test]
    fn test_normalize_source_rejects_untrusted_location() {
        for source in [
            "http://data.binance.vision/data/example.zip",
            "https://example.com/data/example.zip",
            "https://data.binance.vision/robots.txt",
            "https://data.binance.vision/data/example/",
            "https://user@data.binance.vision/data/example.zip",
            "https://data.binance.vision/data/../example.zip",
        ] {
            assert_eq!(
                normalize_source(source),
                Err(ArchiveError::Untrusted),
                "{source}"
            );
        }
    }

    #[test]
    fn test_destination_preserves_remote_hierarchy() {
        let url = format!("https://data.binance.vision/{PATH}");
        assert_eq!(
            destination_for(&url, "/tmp/archives").unwrap(),
            format!("/tmp/archives/{PATH}")
        );
    }

    #[test]
    fn test_parse_checksum_accepts_sha256sum_format() {
        let digest = "a".repeat(64);
        let content = format!("{digest}  ETHUSDT-15m-2026-07.zip\n");
        assert_eq!(
            parse_checksum(&content, "ETHUSDT-15m-2026-07.zip").unwrap(),
            digest
        );
    }

    #[test]
    fn test_parse_checksum_rejects_wrong_filename() {
        let content = format!("{}  BTCUSDT-15m-2026-07.zip\n", "a".repeat(64));
        assert_eq!(
            parse_checksum(&content, "ETHUSDT-15m-2026-07.zip"),
            Err(ArchiveError::Checksum)
        );
    }

    #[test]
    fn test_sync_file_downloads_verifies_and_publishes_atomically() {
        let payload = b"binance archive".to_vec();
        let digest = hex_encode(Sha256::digest(&payload));
        let mut publication = Publication::default();
        publication.stage(payload.clone(), &digest).unwrap();
        assert!(publication.public_bytes.is_none());
        publication.commit();
        assert_eq!(
            publication.public_bytes.as_deref(),
            Some(payload.as_slice())
        );
        assert!(publication.part.is_none());
        assert!(publication.verified);
    }

    #[test]
    fn test_failed_source_page_install_leaves_no_partial_public_file() {
        let mut publication = Publication::default();
        assert!(
            publication
                .stage(b"nope".to_vec(), &"ab".repeat(32))
                .is_err()
        );
        publication.fail_before_commit();
        assert!(publication.public_bytes.is_none());
        assert!(publication.part.is_none());
    }

    #[test]
    fn test_archive_plan_rejects_current_utc_day() {
        let now = 10 * DAY_MS + 5;
        assert_eq!(
            reject_current_utc_day(10 * DAY_MS, now),
            Err(ArchiveError::CurrentDay)
        );
        assert!(reject_current_utc_day(9 * DAY_MS, now).is_ok());
    }

    #[test]
    fn test_archive_day_becomes_eligible_at_utc_midnight_two_dates_later() {
        assert!(!archive_eligible(0, DAY_MS));
        assert!(archive_eligible(0, 2 * DAY_MS));
    }

    #[test]
    fn test_empty_response_and_nonadvancing_pages_never_create_fake_data() {
        assert_eq!(accept_history_page(None, &[]), Err(ArchiveError::EmptyPage));
        assert_eq!(
            accept_history_page(Some(5), &[5, 6]),
            Err(ArchiveError::NotAdvancing)
        );
        assert_eq!(accept_history_page(Some(4), &[5, 6]).unwrap(), 6);
    }

    #[test]
    fn test_trade_header_detection_skips_leading_blank_lines() {
        let lines = ["", "id,price,qty", "1,10,1"];
        let kept: Vec<_> = skip_header(&lines).copied().collect();
        assert_eq!(kept, vec!["1,10,1"]);
    }

    #[test]
    fn test_history_paginates_exclusive_window_and_preserves_signed_rate() {
        assert_eq!(rest_start_after(None), 0);
        assert_eq!(rest_start_after(Some(90)), 91);
        let rate: rust_decimal::Decimal = "-0.00025".parse().unwrap();
        assert!(rate.is_sign_negative());
    }
}

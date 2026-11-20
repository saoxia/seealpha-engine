//! Load a user strategy package and run it in the isolated Python host.
//!
//! The host is the only Python process. It may publish target holdings and
//! cannot submit orders. Package bytes are checked before the process starts.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use thiserror::Error;
use zip::ZipArchive;

const MAX_ARCHIVE_BYTES: u64 = 256 * 1024;
const MAX_MEMBERS: usize = 32;

#[derive(Debug, Error)]
pub enum StrategyError {
    #[error("{0}")]
    Package(String),
    #[error("策略宿主不可用")]
    Host,
    #[error("策略执行超时")]
    Timeout,
    #[error("{0}")]
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageMetadata {
    pub name: String,
    pub version: String,
    pub entry_point: String,
    pub digest: String,
}

#[derive(Debug, Clone, Serialize)]
struct HostBar {
    open_time_ms: i64,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
}

#[derive(Debug, Clone)]
pub struct StrategyBar {
    pub open_time_ms: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Publication {
    pub symbol: String,
    pub quantity: String,
    pub bar_index: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct HostResponse {
    ok: bool,
    #[serde(default)]
    error: String,
    #[serde(default)]
    prepared: Value,
    #[serde(default)]
    publications: Vec<Publication>,
}

#[derive(Debug, Clone)]
pub struct StrategyRun {
    pub prepared: Value,
    pub publications: Vec<Publication>,
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex_encode(Sha256::digest(bytes))
}

pub fn inspect_archive(bytes: &[u8]) -> Result<PackageMetadata, StrategyError> {
    if bytes.is_empty() || bytes.len() as u64 > MAX_ARCHIVE_BYTES {
        return Err(StrategyError::Package("策略包无效。".into()));
    }
    let digest = sha256_hex(bytes);
    let cursor = std::io::Cursor::new(bytes);
    let mut archive =
        ZipArchive::new(cursor).map_err(|_| StrategyError::Package("策略包无效。".into()))?;
    if archive.len() > MAX_MEMBERS {
        return Err(StrategyError::Package("策略包无效。".into()));
    }
    let mut total = 0_u64;
    let mut metadata: Option<Vec<u8>> = None;
    for index in 0..archive.len() {
        let mut file = archive
            .by_index(index)
            .map_err(|_| StrategyError::Package("策略包无效。".into()))?;
        if file.encrypted() {
            return Err(StrategyError::Package("策略包无效。".into()));
        }
        let name = file.name().to_string();
        reject_member(&name)?;
        total = total
            .checked_add(file.size())
            .ok_or_else(|| StrategyError::Package("策略包无效。".into()))?;
        if total > MAX_ARCHIVE_BYTES {
            return Err(StrategyError::Package("策略包无效。".into()));
        }
        if name == "seealpha-package.json" {
            let mut payload = Vec::new();
            file.read_to_end(&mut payload)
                .map_err(|_| StrategyError::Package("策略包无效。".into()))?;
            metadata = Some(payload);
        }
    }
    let payload = metadata
        .ok_or_else(|| StrategyError::Package("策略包缺少 seealpha-package.json。".into()))?;
    metadata_from_json(&payload, digest)
}

pub fn digest_dir(root: &Path) -> Result<String, StrategyError> {
    let mut files = Vec::new();
    collect_files(root, root, &mut files)?;
    files.sort();
    let mut hasher = Sha256::new();
    for (relative, path) in files {
        hasher.update(relative.as_bytes());
        hasher.update([0]);
        let bytes =
            std::fs::read(path).map_err(|_| StrategyError::Package("策略包无效。".into()))?;
        hasher.update(bytes);
    }
    Ok(hex_encode(hasher.finalize()))
}

#[derive(Clone, Copy)]
pub struct HostCommand<'a> {
    pub python: &'a Path,
    pub host: &'a Path,
    pub package_dir: Option<&'a Path>,
    pub archive_path: Option<&'a Path>,
    pub parameters: &'a Value,
    pub symbol: &'a str,
    pub interval: &'a str,
    pub bars: &'a [StrategyBar],
    pub op: &'a str,
}

pub fn run_host(command: &HostCommand<'_>) -> Result<StrategyRun, StrategyError> {
    let HostCommand {
        python,
        host,
        package_dir,
        archive_path,
        parameters,
        symbol,
        interval,
        bars,
        op,
    } = *command;
    if !host.is_file() {
        return Err(StrategyError::Host);
    }
    if bars.len() > 5_000 {
        return Err(StrategyError::Package("K 线数量超过上限。".into()));
    }
    let request = serde_json::json!({
        "op": op,
        "package_dir": package_dir,
        "archive_path": archive_path,
        "parameters": parameters,
        "symbol": symbol,
        "interval": interval,
        "bars": bars.iter().map(|bar| HostBar {
            open_time_ms: bar.open_time_ms,
            open: bar.open.clone(),
            high: bar.high.clone(),
            low: bar.low.clone(),
            close: bar.close.clone(),
            volume: bar.volume.clone(),
        }).collect::<Vec<_>>(),
    });
    let response = invoke(python, host, &request)?;
    if !response.ok {
        return Err(StrategyError::Failed(if response.error.is_empty() {
            "策略执行失败。".into()
        } else {
            response.error
        }));
    }
    Ok(StrategyRun {
        prepared: response.prepared,
        publications: response.publications,
    })
}

fn invoke(python: &Path, host: &Path, request: &Value) -> Result<HostResponse, StrategyError> {
    let mut child = Command::new(python)
        .arg(host)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| StrategyError::Host)?;
    let payload = serde_json::to_vec(request).map_err(|_| StrategyError::Host)?;
    let mut stdin = child.stdin.take().ok_or(StrategyError::Host)?;
    stdin.write_all(&payload).map_err(|_| StrategyError::Host)?;
    drop(stdin);
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|_| StrategyError::Host)? {
            break status;
        }
        if started.elapsed() > Duration::from_secs(30) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(StrategyError::Timeout);
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    let mut stdout = String::new();
    if let Some(mut pipe) = child.stdout.take() {
        pipe.read_to_string(&mut stdout)
            .map_err(|_| StrategyError::Host)?;
    }
    if !status.success() {
        return Err(StrategyError::Failed("策略宿主异常退出。".into()));
    }
    serde_json::from_str(&stdout).map_err(|_| StrategyError::Failed("策略宿主输出无效。".into()))
}

fn metadata_from_json(payload: &[u8], digest: String) -> Result<PackageMetadata, StrategyError> {
    let document: Value = serde_json::from_slice(payload)
        .map_err(|_| StrategyError::Package("策略包说明无效。".into()))?;
    if document.get("format").and_then(Value::as_i64) != Some(1) {
        return Err(StrategyError::Package("策略包说明无效。".into()));
    }
    let name = document
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| StrategyError::Package("策略包名称无效。".into()))?;
    let version = document
        .get("version")
        .and_then(Value::as_str)
        .ok_or_else(|| StrategyError::Package("策略包版本无效。".into()))?;
    let entry_point = document
        .get("entry_point")
        .and_then(Value::as_str)
        .ok_or_else(|| StrategyError::Package("策略包入口无效。".into()))?;
    if !valid_name(name) || name.contains("strategy_python") {
        return Err(StrategyError::Package("策略包名称无效。".into()));
    }
    if !valid_version(version) {
        return Err(StrategyError::Package("策略包版本无效。".into()));
    }
    if !valid_entry(entry_point) {
        return Err(StrategyError::Package("策略包入口无效。".into()));
    }
    Ok(PackageMetadata {
        name: name.to_string(),
        version: version.to_string(),
        entry_point: entry_point.to_string(),
        digest,
    })
}

fn reject_member(name: &str) -> Result<(), StrategyError> {
    if name.is_empty() || name.ends_with('/') {
        return Ok(());
    }
    if name.starts_with('/') || name.contains('\\') || name.split('/').any(|part| part == "..") {
        return Err(StrategyError::Package("策略包路径无效。".into()));
    }
    let path = Path::new(name);
    let suffix = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if !matches!(suffix.as_str(), "py" | "json" | "md" | "txt") || name.contains("__pycache__") {
        return Err(StrategyError::Package("策略包包含不支持的文件。".into()));
    }
    if path.components().any(|part| {
        part.as_os_str()
            .to_string_lossy()
            .starts_with("strategy_python")
    }) {
        return Err(StrategyError::Package(
            "不能从 strategy_python 加载策略。".into(),
        ));
    }
    Ok(())
}

fn collect_files(
    root: &Path,
    dir: &Path,
    files: &mut Vec<(String, PathBuf)>,
) -> Result<(), StrategyError> {
    let entries =
        std::fs::read_dir(dir).map_err(|_| StrategyError::Package("策略包无效。".into()))?;
    for entry in entries {
        let entry = entry.map_err(|_| StrategyError::Package("策略包无效。".into()))?;
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().and_then(|name| name.to_str()) == Some("__pycache__") {
                continue;
            }
            collect_files(root, &path, files)?;
        } else if path.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| StrategyError::Package("策略包无效。".into()))?
                .to_string_lossy()
                .replace('\\', "/");
            files.push((relative, path));
        }
    }
    Ok(())
}

fn valid_name(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() => {}
        _ => return false,
    }
    let rest = chars.count();
    (1..=40).contains(&(rest + 1))
        && value
            .chars()
            .all(|char| char.is_ascii_lowercase() || char.is_ascii_digit() || char == '_')
}

fn valid_version(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphanumeric() => {}
        _ => return false,
    }
    value.chars().count() <= 40
        && value
            .chars()
            .all(|char| char.is_ascii_alphanumeric() || matches!(char, '.' | '_' | '-'))
}

fn valid_entry(value: &str) -> bool {
    let Some((module, attribute)) = value.split_once(':') else {
        return false;
    };
    !module.is_empty()
        && !attribute.is_empty()
        && module.split('.').all(valid_ident)
        && valid_ident(attribute)
}

fn valid_ident(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => return false,
    }
    chars.all(|char| char.is_ascii_alphanumeric() || char == '_')
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_a_path_that_escapes_the_archive() {
        assert!(reject_member("../strategy.py").is_err());
        assert!(reject_member("strategy.py").is_ok());
    }
}

//! Load a user strategy package and run it in the isolated Python host.
//!
//! The host is the only Python process. It may publish target holdings and
//! cannot submit orders. Package bytes are checked before the process starts.
mod decimal;
pub mod executor;
mod host_policy;
mod host_process;
mod issued;
pub mod native_ema;

pub use issued::{AccountScope, IssuedStrategy, authorize_issued, run_issued_host};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};
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
#[serde(deny_unknown_fields)]
pub struct Publication {
    pub symbol: String,
    #[serde(default)]
    pub quantity: String,
    #[serde(default)]
    pub weight: Option<String>,
    pub bar_index: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// Inspect an uploaded archive and prepare its parameters through the bounded
/// strategy host without loading it into the resident Engine process.
///
/// This replaces the legacy `engine.strategies.inspect` worker. Preparing
/// user-authored Python still requires the isolated strategy host; the caller
/// must provide an approved host executable and interpreter.
pub fn prepare_archive(
    bytes: &[u8],
    python: &Path,
    host: &Path,
    parameters: &Value,
) -> Result<(PackageMetadata, Value), StrategyError> {
    let metadata = inspect_archive(bytes)?;
    let archive = issued::StagedArchive::create(bytes)?;
    let command = HostCommand {
        python,
        host,
        package_dir: None,
        archive_path: Some(archive.path()),
        parameters,
        symbol: "",
        interval: "1m",
        bars: &[],
        op: "prepare",
    };
    let result = run_host(&command)?;
    Ok((metadata, result.prepared))
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex_encode(Sha256::digest(bytes))
}

/// Verify the raw ZIP central directory before handing any filenames to a
/// ZIP reader. Some readers normalize or deduplicate duplicate entries, so
/// checking only the reader's exposed names is insufficient.
fn verify_central_directory_names(bytes: &[u8]) -> Result<(), StrategyError> {
    fn uint16(bytes: &[u8], offset: usize) -> Option<usize> {
        let low = *bytes.get(offset)?;
        let high = *bytes.get(offset + 1)?;
        Some(u16::from_le_bytes([low, high]) as usize)
    }
    fn uint32(bytes: &[u8], offset: usize) -> Option<usize> {
        let data: [u8; 4] = bytes.get(offset..offset + 4)?.try_into().ok()?;
        Some(u32::from_le_bytes(data) as usize)
    }

    let final_eocd = bytes.len().checked_sub(22).ok_or_else(invalid_archive)?;
    let eocd = (final_eocd.saturating_sub(u16::MAX as usize)..=final_eocd)
        .rev()
        .find(|&at| {
            bytes.get(at..at + 4) == Some(&b"PK\x05\x06"[..])
                && uint16(bytes, at + 20) == Some(bytes.len() - at - 22)
        })
        .ok_or_else(invalid_archive)?;
    let entries = uint16(bytes, eocd + 10).ok_or_else(invalid_archive)?;
    if entries == 0
        || entries > MAX_MEMBERS
        || uint16(bytes, eocd + 4) != Some(0)
        || uint16(bytes, eocd + 6) != Some(0)
        || uint16(bytes, eocd + 8) != Some(entries)
    {
        return Err(invalid_archive());
    }
    let directory_size = uint32(bytes, eocd + 12).ok_or_else(invalid_archive)?;
    let declared_offset = uint32(bytes, eocd + 16).ok_or_else(invalid_archive)?;
    let mut cursor = eocd
        .checked_sub(directory_size)
        .ok_or_else(invalid_archive)?;
    // A valid self-extracting prefix is permitted; the central directory
    // must still occupy exactly the declared length and contain each name.
    if cursor < declared_offset {
        return Err(invalid_archive());
    }
    let mut seen = std::collections::HashSet::new();
    for _ in 0..entries {
        if bytes.get(cursor..cursor + 4) != Some(&b"PK\x01\x02"[..]) {
            return Err(invalid_archive());
        }
        let name_size = uint16(bytes, cursor + 28).ok_or_else(invalid_archive)?;
        let extra_size = uint16(bytes, cursor + 30).ok_or_else(invalid_archive)?;
        let comment_size = uint16(bytes, cursor + 32).ok_or_else(invalid_archive)?;
        let name_start = cursor.checked_add(46).ok_or_else(invalid_archive)?;
        let name_end = name_start
            .checked_add(name_size)
            .ok_or_else(invalid_archive)?;
        let name = bytes
            .get(name_start..name_end)
            .ok_or_else(invalid_archive)?;
        if !seen.insert(name.to_vec()) {
            return Err(StrategyError::Package("策略包包含重复的路径。".into()));
        }
        cursor = name_end
            .checked_add(extra_size)
            .and_then(|next| next.checked_add(comment_size))
            .ok_or_else(invalid_archive)?;
        if cursor > eocd {
            return Err(invalid_archive());
        }
    }
    if cursor != eocd {
        return Err(invalid_archive());
    }
    Ok(())
}

pub fn inspect_archive(bytes: &[u8]) -> Result<PackageMetadata, StrategyError> {
    if bytes.is_empty() || bytes.len() as u64 > MAX_ARCHIVE_BYTES {
        return Err(invalid_archive());
    }
    verify_central_directory_names(bytes)?;
    let digest = sha256_hex(bytes);
    let mut archive =
        ZipArchive::new(std::io::Cursor::new(bytes)).map_err(|_| invalid_archive())?;
    if archive.is_empty() || archive.len() > MAX_MEMBERS {
        return Err(invalid_archive());
    }
    let mut seen = std::collections::HashSet::new();
    let mut total = 0_u64;
    let mut metadata: Option<Vec<u8>> = None;
    for index in 0..archive.len() {
        let mut file = archive.by_index(index).map_err(|_| invalid_archive())?;
        if file.encrypted()
            || file
                .unix_mode()
                .is_some_and(|mode| mode & 0o170000 == 0o120000)
        {
            return Err(invalid_archive());
        }
        let name = file.name().to_string();
        reject_member(&name)?;
        if !seen.insert(name.clone()) {
            return Err(StrategyError::Package("策略包包含重复的路径。".into()));
        }
        total = total.checked_add(file.size()).ok_or_else(invalid_archive)?;
        if total > MAX_ARCHIVE_BYTES {
            return Err(invalid_archive());
        }
        if file.is_dir() {
            continue;
        }
        let mut content = Vec::new();
        file.by_ref()
            .take(MAX_ARCHIVE_BYTES + 1)
            .read_to_end(&mut content)
            .map_err(|_| invalid_archive())?;
        if content.len() as u64 != file.size() {
            return Err(invalid_archive());
        }
        if name.ends_with(".py") {
            let python = std::str::from_utf8(&content).map_err(|_| invalid_archive())?;
            if python.contains("strategy_python") {
                return Err(StrategyError::Package(
                    "不能从 strategy_python 加载策略。".into(),
                ));
            }
        }
        if name == "seealpha-package.json" {
            metadata = Some(content);
        }
    }
    let payload = metadata
        .ok_or_else(|| StrategyError::Package("策略包缺少 seealpha-package.json。".into()))?;
    let meta = metadata_from_json(&payload, digest)?;
    let entry_file = meta
        .entry_point
        .split_once(':')
        .map(|(module, _)| format!("{}.py", module.replace('.', "/")))
        .ok_or_else(invalid_archive)?;
    if !seen.contains(&entry_file) {
        return Err(StrategyError::Package(
            "策略包缺少声明的 Python 入口。".into(),
        ));
    }
    Ok(meta)
}

fn invalid_archive() -> StrategyError {
    StrategyError::Package("策略包无效。".into())
}

pub fn inspect_directory(root: &Path) -> Result<PackageMetadata, StrategyError> {
    let digest = digest_dir(root)?;
    let metadata = std::fs::read(root.join("seealpha-package.json"))
        .map_err(|_| StrategyError::Package("策略包缺少 seealpha-package.json。".into()))?;
    let meta = metadata_from_json(&metadata, digest)?;
    let module = meta
        .entry_point
        .split_once(':')
        .ok_or_else(invalid_archive)?
        .0;
    let entry = root.join(format!("{}.py", module.replace('.', "/")));
    let info = std::fs::symlink_metadata(&entry).map_err(|_| invalid_archive())?;
    if !info.file_type().is_file() {
        return Err(StrategyError::Package(
            "策略包缺少声明的 Python 入口。".into(),
        ));
    }
    Ok(meta)
}

pub fn digest_dir(root: &Path) -> Result<String, StrategyError> {
    let root_info = std::fs::symlink_metadata(root).map_err(|_| invalid_archive())?;
    if !root_info.file_type().is_dir() {
        return Err(invalid_archive());
    }
    let mut files = Vec::new();
    collect_files(root, root, &mut files)?;
    if files.is_empty() || files.len() > MAX_MEMBERS {
        return Err(invalid_archive());
    }
    files.sort();
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    for (relative, path) in files {
        // Directory packages can contain an inert LICENSE file (the bundled
        // ema_crossover sample does); preserve the historical digest bytes.
        if relative != "LICENSE" {
            reject_member(&relative)?;
        }
        let bytes = std::fs::read(path).map_err(|_| invalid_archive())?;
        total = total
            .checked_add(bytes.len() as u64)
            .ok_or_else(invalid_archive)?;
        if total > MAX_ARCHIVE_BYTES {
            return Err(invalid_archive());
        }
        if relative.ends_with(".py") {
            let python = std::str::from_utf8(&bytes).map_err(|_| invalid_archive())?;
            if python.contains("strategy_python") {
                return Err(StrategyError::Package(
                    "不能从 strategy_python 加载策略。".into(),
                ));
            }
        }
        hasher.update(relative.as_bytes());
        hasher.update([0]);
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
    host_policy::check_command(command)?;
    if !command.host.is_file() {
        return Err(StrategyError::Host);
    }
    // Verify and pin archive contents in a private file before starting
    // Python, so a concurrent write to the supplied path cannot swap code.
    let staged = if let Some(path) = command.archive_path {
        let source = std::fs::read(path).map_err(|_| invalid_archive())?;
        inspect_archive(&source)?;
        Some(issued::StagedArchive::create(&source)?)
    } else {
        if let Some(root) = command.package_dir {
            inspect_directory(root)?;
        }
        None
    };
    let pinned_archive = staged.as_ref().map(issued::StagedArchive::path);
    let request = serde_json::json!({
        "op": command.op,
        "package_dir": command.package_dir,
        "archive_path": pinned_archive,
        "parameters": command.parameters,
        "symbol": command.symbol,
        "interval": command.interval,
        "bars": command.bars.iter().map(|bar| HostBar {
            open_time_ms: bar.open_time_ms,
            open: bar.open.clone(),
            high: bar.high.clone(),
            low: bar.low.clone(),
            close: bar.close.clone(),
            volume: bar.volume.clone(),
        }).collect::<Vec<_>>(),
    });
    let response = host_process::invoke(command.python, command.host, &request)?;
    host_policy::check_response(command, response)
}

fn metadata_from_json(payload: &[u8], digest: String) -> Result<PackageMetadata, StrategyError> {
    let document: Value = serde_json::from_slice(payload)
        .map_err(|_| StrategyError::Package("策略包说明无效。".into()))?;
    if document.get("format").and_then(Value::as_u64) != Some(1) {
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
    let title = document.get("title").unwrap_or(&document["name"]);
    if !title
        .as_str()
        .is_some_and(|s| !s.trim().is_empty() && s.trim().len() <= 100)
        || document.get("description").is_some_and(|value| {
            !value.is_null() && !value.as_str().is_some_and(|s| s.len() <= 500)
        })
        || !document.get("parameters").is_none_or(Value::is_object)
        || document
            .get("parameters")
            .and_then(Value::as_object)
            .is_some_and(|parameters| {
                parameters.keys().any(|key| {
                    matches!(
                        key.to_ascii_lowercase().as_str(),
                        "api_key"
                            | "secret"
                            | "passphrase"
                            | "password"
                            | "token"
                            | "credential"
                            | "credentials"
                    )
                })
            })
    {
        return Err(StrategyError::Package("策略包说明无效。".into()));
    }
    Ok(PackageMetadata {
        name: name.to_string(),
        version: version.to_string(),
        entry_point: entry_point.to_string(),
        digest,
    })
}

fn reject_member(name: &str) -> Result<(), StrategyError> {
    if name.is_empty()
        || name.starts_with('/')
        || name.contains('\\')
        || name.contains(':')
        || name.contains('\0')
        || name.chars().any(char::is_control)
        || name
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        // A directory ends in '/': its last empty component is allowed only
        // after every earlier component has passed the same checks.
        if !(name.ends_with('/')
            && !name[..name.len() - 1].is_empty()
            && name[..name.len() - 1]
                .split('/')
                .all(|p| !p.is_empty() && p != "." && p != "..")
            && !name.starts_with('/')
            && !name.contains('\\')
            && !name.contains(':')
            && !name.contains('\0')
            && !name.chars().any(char::is_control))
        {
            return Err(StrategyError::Package("策略包路径无效。".into()));
        }
    }
    if name.ends_with('/') {
        if name
            .split('/')
            .any(|part| part == "__pycache__" || part.starts_with("strategy_python"))
        {
            return Err(StrategyError::Package("策略包包含不支持的目录。".into()));
        }
        return Ok(());
    }
    let path = Path::new(name);
    let suffix = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if !matches!(suffix.as_str(), "py" | "json" | "md" | "txt")
        || path
            .components()
            .any(|p| p.as_os_str().to_string_lossy() == "__pycache__")
    {
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
    let entries = std::fs::read_dir(dir).map_err(|_| invalid_archive())?;
    for entry in entries {
        let entry = entry.map_err(|_| invalid_archive())?;
        let path = entry.path();
        let info = std::fs::symlink_metadata(&path).map_err(|_| invalid_archive())?;
        if info.file_type().is_symlink() {
            return Err(StrategyError::Package("策略包不能包含符号链接。".into()));
        }
        if info.file_type().is_dir() {
            if path.file_name().and_then(|name| name.to_str()) == Some("__pycache__") {
                continue;
            }
            collect_files(root, &path, files)?;
        } else if info.file_type().is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| invalid_archive())?
                .to_string_lossy()
                .replace('\\', "/");
            files.push((relative, path));
            if files.len() > MAX_MEMBERS {
                return Err(invalid_archive());
            }
        } else {
            return Err(invalid_archive());
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
    fn prepare_rejects_an_invalid_archive_before_invoking_python() {
        let params = serde_json::json!({});
        let result = prepare_archive(
            b"not a ZIP",
            Path::new("not-a-python"),
            Path::new("not-a-host"),
            &params,
        );
        assert!(matches!(result, Err(StrategyError::Package(_))));
    }

    #[test]
    fn rejects_a_path_that_escapes_the_archive() {
        for name in [
            "../strategy.py",
            "some/../strategy.py",
            "C:/strategy.py",
            "a\\\\b.py",
            "a//b.py",
            "strategy_python/",
            "link/../../",
        ] {
            assert!(reject_member(name).is_err(), "accepted {name}");
        }
        assert!(reject_member("strategy.py").is_ok());
        assert!(reject_member("src/").is_ok());
    }
}

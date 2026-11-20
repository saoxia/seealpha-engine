//! Read-only deletion plan for existing Nautilus v2 files. This mirrors the
//! Python data_management.deletion_plan boundaries, but intentionally does
//! NOT implement the destructive execute step until all active Python/native
//! writers share one verified exclusion lock.
use crate::reader::{filename_bounds, inspect_existing_directory};
use crate::{Catalog, CatalogError, DAY_MS, MarketProduct, bar_type, instrument_id};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, Metadata},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize)]
pub struct PlannedFile {
    pub path: String,
    pub group: String,
    pub bytes: u64,
    pub mtime_ns: String,
    pub inode: String,
    pub device: String,
}
#[derive(Debug, Clone, Serialize)]
pub struct DeletePreview {
    pub symbol: String,
    pub product: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub snapshot: String,
    pub file_count: usize,
    pub total_bytes: u64,
    pub files: Vec<PlannedFile>,
    pub can_execute: bool,
    pub warning: String,
}
fn unsafe_path() -> CatalogError {
    CatalogError::Conflict("删除预览包含符号链接、非普通文件或无法识别的文件。".into())
}
fn metadata_file(path: &Path) -> Result<Metadata, CatalogError> {
    let file = fs::symlink_metadata(path)?;
    if file.file_type().is_symlink() || !file.is_file() {
        return Err(unsafe_path());
    }
    Ok(file)
}
#[cfg(unix)]
fn identity(meta: &Metadata) -> (String, String) {
    use std::os::unix::fs::MetadataExt;
    (meta.ino().to_string(), meta.dev().to_string())
}
#[cfg(not(unix))]
fn identity(_meta: &Metadata) -> (String, String) {
    ("unavailable".to_owned(), "unavailable".to_owned())
}
impl Catalog {
    pub fn preview_delete(
        &self,
        symbol: &str,
        product: MarketProduct,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<DeletePreview, CatalogError> {
        if start_ms < 0
            || end_ms <= start_ms
            || start_ms % DAY_MS != 0
            || end_ms % DAY_MS != 0
            || end_ms - start_ms > 366 * DAY_MS
        {
            return Err(CatalogError::Invalid(
                "删除预览必须选择完整 UTC 日期，最多 366 天。".into(),
            ));
        }
        let instrument = instrument_id(symbol, product)?;
        let first_ns = start_ms
            .checked_mul(1_000_000)
            .ok_or_else(|| CatalogError::Invalid("开始时间溢出。".into()))?;
        let last_ns = end_ms
            .checked_mul(1_000_000)
            .ok_or_else(|| CatalogError::Invalid("结束时间溢出。".into()))?;
        let mut planned = Vec::new();
        let mut scan =
            |dir: PathBuf, kind: &str, deletes_suffix: bool| -> Result<(), CatalogError> {
                let mut current = self.root().to_path_buf();
                for component in dir.components() {
                    current.push(component);
                    if !inspect_existing_directory(&current)? {
                        return Ok(());
                    }
                }
                for entry in fs::read_dir(current)? {
                    let entry = entry?;
                    let path = entry.path();
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if name == ".writer.lock" {
                        continue;
                    }
                    let parquet = if let Some(raw) = name.strip_suffix(".metadata.json") {
                        format!("{raw}.parquet")
                    } else {
                        name.clone()
                    };
                    let (left, right) = filename_bounds(&parquet).map_err(|_| unsafe_path())?;
                    if right < first_ns || (!deletes_suffix && left >= last_ns) {
                        continue;
                    }
                    // Python refuses boundary-crossing Parquet files rather than
                    // silently truncating neighboring days or invalidating only
                    // part of a feature aggregate.
                    if left < first_ns || (!deletes_suffix && right >= last_ns) {
                        return Err(CatalogError::Conflict(
                            "Parquet 文件跨越所选 UTC 日期边界，拒绝执行范围删除。".into(),
                        ));
                    }
                    let stat = metadata_file(&path)?;
                    let rel = path.strip_prefix(self.root()).map_err(|_| unsafe_path())?;
                    let modified = stat
                        .modified()
                        .map_err(CatalogError::Io)?
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|_| unsafe_path())?
                        .as_nanos()
                        .to_string();
                    let (inode, device) = identity(&stat);
                    planned.push(PlannedFile {
                        path: rel.to_string_lossy().to_string(),
                        group: kind.to_owned(),
                        bytes: stat.len(),
                        mtime_ns: modified,
                        inode,
                        device,
                    });
                    if planned.len() > 10_000 {
                        return Err(CatalogError::Limit("删除范围文件数量过大。".into()));
                    }
                }
                Ok(())
            };
        scan(
            PathBuf::from("data/trades").join(&instrument),
            "ticks",
            false,
        )?;
        scan(
            PathBuf::from("data/quotes").join(&instrument),
            "quotes",
            false,
        )?;
        scan(
            PathBuf::from("supplements")
                .join(&instrument)
                .join("data/custom/TradeQuote"),
            "supplements",
            false,
        )?;
        for interval in [
            "1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d",
            "1w", "1M",
        ] {
            let kind = bar_type(symbol, product, interval)?;
            scan(PathBuf::from("data/bars").join(&kind), "bars", false)?;
            // A feature after the edited window depends on earlier bar
            // history. Invalidate the *entire suffix*, not only the dates.
            scan(
                PathBuf::from("features")
                    .join(&kind)
                    .join("data/custom/BarFeatures"),
                "features",
                true,
            )?;
        }
        planned.sort_by(|a, b| {
            let priority = |kind: &str| match kind {
                "features" => 0,
                "ticks" | "quotes" | "bars" | "supplements" => 1,
                _ => 2,
            };
            priority(&a.group)
                .cmp(&priority(&b.group))
                .then(a.path.cmp(&b.path))
        });
        let bytes = planned.iter().try_fold(0u64, |sum, file| {
            sum.checked_add(file.bytes)
                .ok_or_else(|| CatalogError::Invalid("删除数据量溢出。".into()))
        })?;
        let payload = serde_json::to_vec(&(symbol, product.as_str(), start_ms, end_ms, &planned))
            .map_err(|_| CatalogError::Invalid("删除快照序列化失败。".into()))?;
        let digest = Sha256::digest(payload);
        let snapshot = digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        Ok(DeletePreview{
            symbol:symbol.to_owned(),product:product.as_str().to_owned(),
            start_ms,end_ms,snapshot,
            file_count:planned.len(),total_bytes:bytes,files:planned,
            can_execute:false,
            warning:"此请求仅提供与 Python 一致的删除范围快照；Python/Rust 尚未共享完整写入互斥协议，因此 Rust 不执行实际删除。".into(),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partial_day_deletion_is_rejected() {
        let p = std::env::temp_dir().join(format!("sa-catalog-delete-plan-{}", std::process::id()));
        let _ = fs::create_dir(&p);
        let catalog = Catalog::open_existing(&p).unwrap();
        assert!(
            catalog
                .preview_delete("ETHUSDT", MarketProduct::Spot, 1, DAY_MS)
                .is_err()
        );
        assert_eq!(
            catalog
                .preview_delete("ETHUSDT", MarketProduct::Spot, 0, DAY_MS)
                .unwrap()
                .can_execute,
            false
        );
        let _ = fs::remove_dir(p);
    }
}

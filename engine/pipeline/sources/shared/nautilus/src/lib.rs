//! Cross-process single-writer reservation for a tenant Nautilus Catalog.
//! This is only a SeeAlpha process-coordination guard; Nautilus owns all
//! TradeTick serialization, Arrow schemas and Parquet catalog operations.

pub mod native_account;
pub mod native_account_session;
pub mod native_execution;
pub mod native_order_subscription;

use std::{
    fs::{File, OpenOptions},
    path::Path,
};

use anyhow::{Context, Result, ensure};
use fs2::FileExt;

/// The returned file must stay alive for the entire writer lifetime.
/// A busy lock fails immediately rather than risking concurrent file commits.
pub fn reserve_catalog_writer(root: &Path) -> Result<File> {
    let meta = std::fs::symlink_metadata(root)
        .context("Catalog root must already exist")?;
    ensure!(
        meta.is_dir() && !meta.file_type().is_symlink(),
        "Catalog root must be a real directory"
    );
    let lock_path = root.join(".seealpha-nautilus-writer.lock");
    if let Ok(existing) = std::fs::symlink_metadata(&lock_path) {
        ensure!(
            existing.is_file() && !existing.file_type().is_symlink(),
            "Catalog writer lock is not a real file"
        );
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        // The advisory lock must not mutate an existing lock inode.
        .truncate(false)
        .open(&lock_path)
        .context("could not open native Catalog writer lock")?;
    FileExt::try_lock_exclusive(&file)
        .context("another SeeAlpha Nautilus process is writing this Catalog")?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_one_catalog_writer_may_be_active() {
        let path = std::env::temp_dir().join(format!(
            "seealpha-nautilus-writer-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        let held = reserve_catalog_writer(&path).unwrap();
        assert!(reserve_catalog_writer(&path).is_err());
        drop(held);
        assert!(reserve_catalog_writer(&path).is_ok());
        std::fs::remove_dir_all(path).unwrap();
    }
}

//! Application bridge from *committed* historical 1m Catalog bars to the
//! native, separately persisted derived rollup cache. No exchange IO.
use crate::{Runtime, RuntimeError};
use seealpha_catalog::CatalogBar;
use seealpha_feed::persisted_rollup::{MinuteBar, PersistentRollups};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// Namespaced by tenant and product, avoiding cross-tenant/source collisions.
/// Symbol values are validated by the authoritative Catalog commit path.
fn cache_path(root: &Path, tenant: &str, product: &str, symbol: &str) -> PathBuf {
    let mut digest = Sha256::new();
    for part in [tenant, product, symbol] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    let hash = format!("{:x}", digest.finalize());
    root.join("native-calculation").join(format!("{hash}.sqlite"))
}

impl Runtime {
    /// Best-effort rebuildable derived cache. Call only after the authoritative
    /// Catalog commit succeeds. It does not publish READY or allow_new_risk.
    pub(crate) fn persist_native_minute_rollups(
        &self,
        tenant: &str,
        product: &str,
        symbol: &str,
        bars: &[CatalogBar],
    ) -> Result<(), RuntimeError> {
        if bars.is_empty() {
            return Ok(());
        }
        let path = cache_path(&self.source_state_dir, tenant, product, symbol);
        let parent = path.parent().ok_or_else(|| {
            RuntimeError::Conflict("派生聚合缓存目录无效。".into())
        })?;
        std::fs::create_dir_all(parent).map_err(|error| {
            RuntimeError::Conflict(format!("派生聚合缓存目录不可用：{error}"))
        })?;
        let mut store = PersistentRollups::open(&path).map_err(|error| {
            RuntimeError::Conflict(format!("派生聚合缓存打开失败：{error}"))
        })?;
        for bar in bars {
            store.accept_closed(&MinuteBar {
                symbol: symbol.to_owned(),
                open_ms: bar.open_time_ms,
                open: bar.open,
                high: bar.high,
                low: bar.low,
                close: bar.close,
                volume: bar.volume,
            }).map_err(|error| {
                RuntimeError::Conflict(format!("派生聚合缓存需要人工修复：{error}"))
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_cache_paths_differ_by_tenant_product_and_symbol() {
        let root = Path::new("/tmp/seealpha-test");
        let first = cache_path(root, "tenant-a", "spot", "ETHUSDT");
        assert_eq!(first, cache_path(root, "tenant-a", "spot", "ETHUSDT"));
        assert_ne!(first, cache_path(root, "tenant-b", "spot", "ETHUSDT"));
        assert_ne!(first, cache_path(root, "tenant-a", "futures_um", "ETHUSDT"));
        assert_ne!(first, cache_path(root, "tenant-a", "spot", "BTCUSDT"));
        assert!(first.starts_with(root.join("native-calculation")));
    }
}

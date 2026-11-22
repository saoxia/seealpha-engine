//! Spot hierarchical repair entrypoint: raw trade archive, not futures aggTrades.
pub use crate::repair::{DailyScan, RepairError as SpotRepairError, RepairNextStep};
use std::path::Path;

pub async fn scan_day(
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
    catalog_root: Option<&Path>,
) -> Result<DailyScan, SpotRepairError> {
    crate::repair::scan_day(crate::Product::Spot, symbol, start_ms, end_ms, catalog_root).await
}

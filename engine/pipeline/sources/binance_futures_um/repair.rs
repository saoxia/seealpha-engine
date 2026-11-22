//! USD-M hierarchical repair entrypoint: one shared algorithm, product-specific source.
pub use crate::repair::{DailyScan, RepairError as FuturesRepairError, RepairNextStep};
use std::path::Path;

pub async fn scan_day(
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
    catalog_root: Option<&Path>,
) -> Result<DailyScan, FuturesRepairError> {
    crate::repair::scan_day(crate::Product::FuturesUm, symbol, start_ms, end_ms, catalog_root).await
}

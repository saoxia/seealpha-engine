//! Seealpha's native data foundation for the existing Python/Nautilus v2
//! Catalog directory. No rewriting into SQLite or implicit subscriptions.
mod model;
mod reader;
mod cleanup;
mod writer;

use std::path::PathBuf;
use thiserror::Error;

pub use model::{
    CatalogBar,MarketProduct,bar_type,bar_window,instrument_id,interval_ms,NANO_PER_MS,DAY_MS,
};
pub use reader::RecordKind;
pub use cleanup::{DeletePreview,PlannedFile};

#[derive(Debug,Error)]
pub enum CatalogError{
    #[error("Catalog 文件 IO 失败：{0}")]
    Io(#[from] std::io::Error),
    #[error("Parquet 读写失败：{0}")]
    Parquet(String),
    #[error("Arrow 数据格式错误：{0}")]
    Arrow(String),
    #[error("Catalog 数据不兼容：{0}")]
    Invalid(String),
    #[error("Catalog 数据缺失：{0}")]
    Missing(String),
    #[error("Catalog 数据与历史证据冲突：{0}")]
    Conflict(String),
    #[error("Catalog 读取超出配额：{0}")]
    Limit(String),
}
pub struct Catalog{
    root:PathBuf,
}

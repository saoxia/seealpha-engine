//! Seealpha's native data foundation for the existing Python/Nautilus v2
//! Catalog directory. No rewriting into SQLite or implicit subscriptions.
mod model;
mod reader;
mod records;
mod writer;
mod funding;
mod control;

use std::path::PathBuf;
use thiserror::Error;

pub use model::{
    CatalogBar, DAY_MS, NANO_PER_MS, bar_type, bar_window, interval_ms, parse_bar_type,
};
pub use reader::{BarGap, RecordKind};
pub use funding::CatalogFunding;
pub use control::{ControlStore, ControlError};

#[derive(Debug, Error)]
pub enum CatalogError {
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
pub struct Catalog {
    root: PathBuf,
}

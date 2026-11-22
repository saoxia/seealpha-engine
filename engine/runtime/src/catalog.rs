//! Single historical-data facade for the Python Engine's existing
//! Nautilus v2 Parquet Catalog, without SQLite market-data fallback.
//! Python used data/catalog as the source of truth; Rust no longer requires
//! copying the entire catalog into the Engine's small control-plane book.
use crate::{Runtime, RuntimeError};
use rust_decimal::Decimal;
use seealpha_catalog::{
    Catalog, CatalogBar as NativeBar, CatalogError, MarketProduct, NANO_PER_MS, RecordKind,
};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogBar {
    pub open_time_ms: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
}
type BookBar = CatalogBar;
use serde_json::{Value, json};
use std::path::PathBuf;

fn error(e: CatalogError) -> RuntimeError {
    RuntimeError::Conflict(e.to_string())
}
fn product(raw: &str) -> Result<MarketProduct, RuntimeError> {
    MarketProduct::parse(raw).map_err(error)
}
fn one(bar: &NativeBar) -> BookBar {
    BookBar {
        open_time_ms: bar.open_time_ms,
        open: bar.open.to_string(),
        high: bar.high.to_string(),
        low: bar.low.to_string(),
        close: bar.close.to_string(),
        volume: bar.volume.to_string(),
    }
}
fn time_ns(value: i64) -> Result<i64, RuntimeError> {
    value
        .checked_mul(NANO_PER_MS)
        .ok_or_else(|| RuntimeError::Unprocessable("历史时间转换为纳秒时溢出。".into()))
}
impl Runtime {
    pub fn native_catalog_root(&self) -> PathBuf {
        // Python PipelineSettings.paths.catalog accepts project-relative
        // or absolute mount paths. This explicit process configuration
        // selects exactly one authoritative native Catalog for this Engine.
        match std::env::var("ENGINE_CATALOG_DIR") {
            Ok(value) if !value.trim().is_empty() => {
                let selected = PathBuf::from(value);
                if selected.is_absolute() {
                    selected
                } else {
                    self.project.join(selected)
                }
            }
            _ => self.project.join("data").join("catalog"),
        }
    }
    pub fn native_catalog(&self) -> Result<Option<Catalog>, RuntimeError> {
        let path = self.native_catalog_root();
        match path.symlink_metadata() {
            Ok(_) => Catalog::open_existing(&path).map(Some).map_err(error),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(RuntimeError::Conflict(format!(
                "Catalog 数据目录不可用：{e}"
            ))),
        }
    }
    /// Catalog coverage is authoritative for recorded closed bars only.
    /// It must NEVER be promoted into a live Feed READY permission without
    /// independently verified Source journal gaps and committed checkpoints.
    // Retain explicit v2 migration inputs until equivalent contract coverage is proven.
    #[allow(clippy::too_many_arguments)]
    pub fn inspect_catalog_coverage(
        &self,
        tenant: &str,
        product_name: &str,
        symbol: &str,
        interval: &str,
        start_ms: i64,
        end_ms: i64,
        limit: usize,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        if limit == 0 || limit > 50_000 || end_ms <= start_ms || start_ms < 0 {
            return Err(RuntimeError::Unprocessable(
                "历史覆盖率查询范围无效。".into(),
            ));
        }
        let catalog = self.native_catalog()?.ok_or_else(|| {
            RuntimeError::Conflict("原生 Catalog 尚不存在，不能声明数据就绪。".into())
        })?;
        let gaps = catalog
            .bar_gaps(
                symbol,
                product(product_name)?,
                interval,
                start_ms,
                end_ms,
                limit,
            )
            .map_err(error)?;
        let ranges = gaps
            .iter()
            .map(|gap| {
                json!({
                    "start_ms": gap.start_ms, "end_ms": gap.end_ms
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "product": product_name, "symbol": symbol, "interval": interval,
            "start_ms": start_ms, "end_ms": end_ms,
            "catalog_contiguous": ranges.is_empty(),
            "missing_ranges": ranges,
            "source_checkpoint_verified": false,
            "feed_ready": false,
            "allow_new_risk": false,
            "source": "nautilus_v2_parquet",
        }))
    }

    /// Read [start,end) UTC, matching HistoricalMarketData.bars() exactly:
    /// Parquet event timestamp is candle close minus 1 millisecond.
    // Retain explicit v2 migration inputs until equivalent contract coverage is proven.
    #[allow(clippy::too_many_arguments)]
    pub fn read_market_bars(
        &self,
        tenant: &str,
        product_name: &str,
        symbol: &str,
        interval: &str,
        start_ms: i64,
        end_ms: i64,
        limit: i64,
    ) -> Result<Vec<BookBar>, RuntimeError> {
        self.require_active(tenant)?;
        if start_ms < 0 || end_ms <= start_ms || !(1..=50_000).contains(&limit) {
            return Err(RuntimeError::Unprocessable(
                "历史 K 线查询范围无效。".into(),
            ));
        }
        let catalog = self.native_catalog()?.ok_or_else(|| {
            RuntimeError::NotFound("未挂载 Nautilus v2 Catalog。".into())
        })?;
        let rows = catalog
            .read_bars(
                symbol, product(product_name)?, interval,
                time_ns(start_ms)?, time_ns(end_ms)?, limit as usize,
            )
            .map_err(error)?;
        Ok(rows.iter().map(one).collect())
    }
    /// The strict backtest variant refuses missing Parquet files, partial
    /// coverage, out-of-order clocks, fabricated bars and cross-product data.
    // Retain explicit v2 migration inputs until equivalent contract coverage is proven.
    #[allow(clippy::too_many_arguments)]
    pub fn read_contiguous_market_bars(
        &self,
        tenant: &str,
        product_name: &str,
        symbol: &str,
        interval: &str,
        start_ms: i64,
        end_ms: i64,
        limit: i64,
    ) -> Result<Vec<BookBar>, RuntimeError> {
        self.require_active(tenant)?;
        if !(1..=50_000).contains(&limit) {
            return Err(RuntimeError::Unprocessable("回测范围超过上限。".into()));
        }
        let catalog = self.native_catalog()?.ok_or_else(|| {
            RuntimeError::NotFound("未挂载 Nautilus v2 Catalog。".into())
        })?;
        let rows = catalog
            .read_contiguous_bars(
                symbol, product(product_name)?, interval,
                start_ms, end_ms, limit as usize,
            )
            .map_err(error)?;
        Ok(rows.iter().map(one).collect())
    }
    /// Store historical evidence in the Python-owned Parquet layout when that
    /// directory is mounted. Never rewrite old Parquet into SQLite.
    // Retain explicit v2 migration inputs until equivalent contract coverage is proven.
    #[allow(clippy::too_many_arguments)]
    pub fn commit_market_bars(
        &self,
        tenant: &str,
        product_name: &str,
        symbol: &str,
        interval: &str,
        bars: &[BookBar],
        price_precision: u32,
        size_precision: u32,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        if bars.is_empty() || bars.len() > 50_000 {
            return Err(RuntimeError::Unprocessable(
                "历史 K 线批次为空或过大。".into(),
            ));
        }
        let product = product(product_name)?;
        if price_precision > 16 || size_precision > 16 {
            return Err(RuntimeError::Unprocessable(
                "Nautilus 价格或数量精度无效。".into(),
            ));
        }
        let mut previous = None;
        let mut native = Vec::with_capacity(bars.len());
        for b in bars {
            let row = NativeBar {
                open_time_ms: b.open_time_ms,
                open: b
                    .open
                    .parse::<Decimal>()
                    .map_err(|_| RuntimeError::Unprocessable("K 线开盘价无效。".into()))?,
                high: b
                    .high
                    .parse::<Decimal>()
                    .map_err(|_| RuntimeError::Unprocessable("K 线最高价无效。".into()))?,
                low: b
                    .low
                    .parse::<Decimal>()
                    .map_err(|_| RuntimeError::Unprocessable("K 线最低价无效。".into()))?,
                close: b
                    .close
                    .parse::<Decimal>()
                    .map_err(|_| RuntimeError::Unprocessable("K 线收盘价无效。".into()))?,
                volume: b
                    .volume
                    .parse::<Decimal>()
                    .map_err(|_| RuntimeError::Unprocessable("K 线成交量无效。".into()))?,
            };
            row.validate(interval, product).map_err(error)?;
            if [row.open, row.high, row.low, row.close]
                .iter()
                .any(|value| value.scale() > price_precision)
                || row.volume.scale() > size_precision
            {
                return Err(RuntimeError::Unprocessable(
                    "行情值精度超出声明的 Nautilus Instrument 精度。".into(),
                ));
            }
            if previous.is_some_and(|last| row.open_time_ms <= last) {
                return Err(RuntimeError::Unprocessable("K 线时间戳重复或乱序。".into()));
            }
            previous = Some(row.open_time_ms);
            native.push(row);
        }
        let catalog = self.native_catalog()?.ok_or_else(|| {
            RuntimeError::NotFound("未挂载 Nautilus v2 Catalog。".into())
        })?;
        let path = catalog
            .append_bars(symbol, product, interval, &native, price_precision, size_precision)
            .map_err(error)?;
        let published = json!({"imported":bars.len(),"verified":true,
            "backend":"nautilus_v2_parquet",
            "file":path.strip_prefix(catalog.root()).ok()
                .map(|p|p.to_string_lossy().to_string()),
            "exchange_orders":0});
        // Derived 1m rollups are a repairable cache, never authoritative
        // Catalog evidence. Failure must not recast a committed import as
        // uncommitted or elevate the live data/venue readiness gate.
        if interval == "1m"
            && matches!(product_name, "spot" | "futures_um")
            && let Err(err) =
                self.persist_native_minute_rollups(tenant, product_name, symbol, &native)
        {
            eprintln!("native derived rollup cache requires repair: {err}");
        }
        Ok(published)
    }
    /// Generic native Parquet data for instruments, TradeTick, QuoteTick,
    /// funding, depth, greeks, statuses and custom features. Requires an
    /// existing mounted Catalog; never silently reads a different product.
    /// Opt-in, historical-only reader for the separate native feature format.
    /// This is not an exchange/source READY attestation.
    pub fn read_native_trade_features(
        &self,
        tenant: &str,
        identity: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
    ) -> Result<Vec<Value>, RuntimeError> {
        self.native_market_events(
            tenant,
            &RecordKind::Feature {
                identifier: "rust-v1".to_owned(),
                name: "NativeTradeFeaturesV1".to_owned(),
            },
            identity,
            start_ns,
            end_ns,
            limit,
        )
    }
    pub fn native_market_events(
        &self,
        tenant: &str,
        kind: &RecordKind,
        identity: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
    ) -> Result<Vec<Value>, RuntimeError> {
        self.require_active(tenant)?;
        let catalog = self
            .native_catalog()?
            .ok_or_else(|| RuntimeError::NotFound("未挂载 Python Nautilus v2 Catalog。".into()))?;
        catalog
            .read_records(kind, identity, start_ns, end_ns, limit)
            .map_err(error)
    }
    pub fn native_delete_plan(
        &self,
        tenant: &str,
        product_name: &str,
        symbol: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        let catalog = self
            .native_catalog()?
            .ok_or_else(|| RuntimeError::NotFound("未挂载 Nautilus v2 Catalog。".into()))?;
        let preview = catalog
            .preview_delete(symbol, product(product_name)?, start_ms, end_ms)
            .map_err(error)?;
        serde_json::to_value(preview)
            .map_err(|_| RuntimeError::Conflict("删除预览序列化失败。".into()))
    }
    pub fn native_dataset_inventory(
        &self,
        tenant: &str,
    ) -> Result<Option<Vec<Value>>, RuntimeError> {
        self.require_active(tenant)?;
        self.native_catalog()?
            .map(|catalog| catalog.scan_inventory().map_err(error))
            .transpose()
    }
    pub fn native_market_inventory(
        &self,
        tenant: &str,
        kind: &RecordKind,
        identity: &str,
    ) -> Result<Vec<Value>, RuntimeError> {
        self.require_active(tenant)?;
        let catalog = self
            .native_catalog()?
            .ok_or_else(|| RuntimeError::NotFound("未挂载 Python Nautilus v2 Catalog。".into()))?;
        catalog.inventory(kind, identity).map_err(error)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quote_precision_is_not_lost_in_catalog_bar_conversion() {
        let b = NativeBar {
            open_time_ms: 0,
            open: Decimal::new(123456789, 8),
            high: Decimal::new(123456790, 8),
            low: Decimal::new(123456788, 8),
            close: Decimal::new(123456789, 8),
            volume: Decimal::new(15, 1),
        };
        let converted = one(&b);
        assert_eq!(converted.open, "1.23456789");
        assert_eq!(converted.volume, "1.5");
    }
}

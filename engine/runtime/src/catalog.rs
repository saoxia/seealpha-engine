//! Single historical-data facade for the Python Engine's existing
//! Nautilus v2 Parquet Catalog, without SQLite market-data fallback.
//! Python used data/catalog as the source of truth; Rust no longer requires
//! copying the entire catalog into the Engine's small control-plane book.
use crate::{Runtime, RuntimeError};
use rust_decimal::Decimal;
use seealpha_catalog::{
    Catalog, CatalogBar as NativeBar, CatalogError, NANO_PER_MS, RecordKind,
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
fn product(raw: &str) -> Result<seealpha_market::identity::MarketProduct, RuntimeError> {
    seealpha_market::identity::MarketProduct::parse(raw)
        .map_err(|e| RuntimeError::Unprocessable(e.to_string()))
}
fn market_bar_type(raw: &str, symbol: &str, interval: &str) -> Result<String, RuntimeError> {
    seealpha_market::identity::bar_type(symbol, product(raw)?, interval)
        .map_err(|e| RuntimeError::Unprocessable(e.to_string()))
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
                &market_bar_type(product_name, symbol, interval)?,
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
                &market_bar_type(product_name, symbol, interval)?, interval,
                time_ns(start_ms)?, time_ns(end_ms)?, limit as usize,
            )
            .map_err(error)?;
        // Product-specific price policy still guards historical reads.
        for row in &rows {
            seealpha_market::identity::validate_market_bar(row, product(product_name)?)
                .map_err(|e| RuntimeError::Unprocessable(e.to_string()))?;
        }
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
                &market_bar_type(product_name, symbol, interval)?, interval,
                start_ms, end_ms, limit as usize,
            )
            .map_err(error)?;
        // Product-specific price policy still guards historical reads.
        for row in &rows {
            seealpha_market::identity::validate_market_bar(row, product(product_name)?)
                .map_err(|e| RuntimeError::Unprocessable(e.to_string()))?;
        }
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
        let identity = market_bar_type(product_name, symbol, interval)?;
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
            row.validate(interval).map_err(error)?;
            seealpha_market::identity::validate_market_bar(&row, product)
                .map_err(|e| RuntimeError::Unprocessable(e.to_string()))?;
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
            .append_bars(&identity, interval, &native, price_precision, size_precision)
            .map_err(error)?;
        let published = json!({"imported":bars.len(),"verified":true,
            "backend":"nautilus_v2_parquet",
            "file":path.strip_prefix(catalog.root()).ok()
                .map(|p|p.to_string_lossy().to_string()),
            "exchange_orders":0});
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
    pub fn funding_observations(&self, tenant: &str, symbol: &str, start_ms: i64, end_ms: i64, limit: usize) -> Result<Vec<Value>, RuntimeError> {
        if start_ms < 0 || end_ms <= start_ms || !(1..=50_000).contains(&limit) {
            return Err(RuntimeError::Unprocessable("资金费查询范围无效。".into()));
        }
        let identity = seealpha_market::identity::instrument_id(symbol, seealpha_market::identity::MarketProduct::FuturesUm)
            .map_err(|e| RuntimeError::Unprocessable(e.to_string()))?;
        let from = start_ms.checked_mul(NANO_PER_MS).ok_or_else(|| RuntimeError::Unprocessable("时间溢出。".into()))?;
        let to = end_ms.checked_mul(NANO_PER_MS).ok_or_else(|| RuntimeError::Unprocessable("时间溢出。".into()))?;
        let rows = self.native_market_events(tenant, &RecordKind::FundingRate, &identity, from, to, limit)?;
        rows.into_iter().map(|record| {
            let at = record["ts_event"].as_str().and_then(|s| s.parse::<i64>().ok())
                .filter(|ns| ns % NANO_PER_MS == 0)
                .ok_or_else(|| RuntimeError::Conflict("资金费时间格式无效。".into()))? / NANO_PER_MS;
            let rate = record.get("funding_rate").or_else(|| record.get("rate")).and_then(Value::as_str)
                .ok_or_else(|| RuntimeError::Conflict("资金费率缺失。".into()))?;
            let mark = record["mark_price"].as_str()
                .ok_or_else(|| RuntimeError::Conflict("资金费标记价格缺失。".into()))?;
            let kind = record["rate_type"].as_str().unwrap_or("Unknown");
            Ok(json!({"symbol":symbol,"funding_time_ms":at,"funding_rate":rate,"mark_price":mark,
                "rate_type":kind,"settlement":"official_observation"}))
        }).collect()
    }
    pub fn native_dataset_inventory(
        &self,
        tenant: &str,
    ) -> Result<Option<Vec<Value>>, RuntimeError> {
        self.require_active(tenant)?;
        let Some(catalog) = self.native_catalog()? else { return Ok(None); };
        let mut records = catalog.scan_inventory().map_err(error)?;
        // Binance API fields are resolved in Source, not in storage.
        for record in &mut records {
            let instrument = record["instrument_id"].as_str().ok_or_else(|| {
                RuntimeError::Conflict("Catalog 数据缺失 InstrumentId。".into())
            })?;
            let (symbol, product) = seealpha_market::identity::parse_instrument_id(instrument)
                .map_err(|e| RuntimeError::Conflict(e.to_string()))?;
            record["symbol"] = json!(symbol);
            record["product"] = json!(product.as_str());
        }
        records.sort_by(|a,b| a["product"].as_str().cmp(&b["product"].as_str())
            .then(a["symbol"].as_str().cmp(&b["symbol"].as_str()))
            .then(a["level"].as_str().cmp(&b["level"].as_str())));
        Ok(Some(records))
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

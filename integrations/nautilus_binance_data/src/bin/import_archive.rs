//! Explicit, offline Binance daily ZIP to native NautilusTrader TradeTick Catalog.
//!
//! The ZIP is already downloaded by SeeAlpha's archive fetcher. This importer
//! never implements exchange REST/WebSocket, Arrow schemas, or its own storage.
//! Output is *not* a data-ready or gap-free declaration.

use anyhow::{Context, Result, bail, ensure};
use chrono::NaiveDate;
use csv::StringRecord;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::TradeTick,
    enums::AggressorSide,
    identifiers::{InstrumentId, TradeId},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{BufReader, Read},
    path::Path,
    str::FromStr,
};

const MAX_ZIP_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MAX_CSV_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const MAX_ROWS: u64 = 2_000_000_000;
const BATCH_SIZE: usize = 50_000;
const BATCH_SAME_TIMESTAMP_LIMIT: usize = 100_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Market {
    Spot,
    UsdM,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dataset {
    Trades,
    AggTrades,
}

#[derive(Debug, Clone)]
struct Request {
    market: Market,
    dataset: Dataset,
    symbol: String,
    date: NaiveDate,
    hash: String,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Stats {
    count: u64,
    first_id: Option<u64>,
    last_id: Option<u64>,
    first_ts: Option<u64>,
    last_ts: Option<u64>,
    last_raw_id: Option<u64>,
    price_precision: Option<u8>,
    size_precision: Option<u8>,
    csv_row_digest: String,
}

impl Request {
    fn parse(market: &str, dataset: &str, symbol: &str, date: &str, hash: &str, price_precision: u8, size_precision: u8) -> Result<Self> {
        let market = match market {
            "spot" => Market::Spot,
            "futures_um" => Market::UsdM,
            _ => bail!("expected spot or futures_um"),
        };
        let dataset = match dataset {
            "trades" => Dataset::Trades,
            "aggTrades" => Dataset::AggTrades,
            _ => bail!("expected trades or aggTrades"),
        };
        // Prevent combining native spot raw ticks with aggregate ticks, or
        // native futures aggregate ticks with raw futures ticks in one Catalog.
        ensure!(
            matches!(
                (market, dataset),
                (Market::Spot, Dataset::Trades) | (Market::UsdM, Dataset::AggTrades)
            ),
            "only Spot trades and USD-M aggTrades are native live-compatible; raw USD-M and Spot aggTrades require distinct research catalogs"
        );
        ensure!(
            (5..=30).contains(&symbol.len())
                && symbol.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()),
            "invalid Binance symbol"
        );
        let parsed = NaiveDate::parse_from_str(date, "%Y-%m-%d")?;
        ensure!(parsed.format("%Y-%m-%d").to_string() == date, "noncanonical date");
        ensure!(
            hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
            "expected SHA-256 must be exactly 64 hex characters"
        );
        ensure!(price_precision <= 16 && size_precision <= 16, "instrument precision must be <= 16");
        let native = match market {
            Market::Spot => format!("{symbol}.BINANCE"),
            Market::UsdM => format!("{symbol}-PERP.BINANCE"),
        };
        Ok(Self {
            market,
            dataset,
            symbol: symbol.to_owned(),
            date: parsed,
            hash: hash.to_ascii_lowercase(),
            instrument_id: InstrumentId::from(native.as_str()),
            price_precision,
            size_precision,
        })
    }

    fn csv_name(&self) -> String {
        let dataset = match self.dataset {
            Dataset::Trades => "trades",
            Dataset::AggTrades => "aggTrades",
        };
        format!("{}-{}-{}.csv", self.symbol, dataset, self.date)
    }

    fn interval_ms(&self) -> Result<(u64, u64)> {
        let start = self.date.and_hms_opt(0, 0, 0)
            .context("invalid day start")?
            .and_utc().timestamp_millis();
        let end = self.date.succ_opt()
            .context("invalid day end")?
            .and_hms_opt(0, 0, 0)
            .context("invalid day end clock")?
            .and_utc().timestamp_millis();
        Ok((u64::try_from(start)?, u64::try_from(end)?))
    }

    fn spot_micros(&self) -> bool {
        self.market == Market::Spot
            && self.date >= NaiveDate::from_ymd_opt(2025, 1, 1).expect("date exists")
    }
}

fn verify_zip_hash(path: &Path, expected: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink()
            && metadata.len() > 0 && metadata.len() <= MAX_ZIP_BYTES,
        "ZIP path must be a bounded real file"
    );
    let mut file = BufReader::new(File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    ensure!(format!("{:x}", hasher.finalize()) == expected, "archive checksum mismatch");
    Ok(())
}

struct Parsed {
    tick: TradeTick,
    id: u64,
    first_raw_id: u64,
    last_raw_id: u64,
}

fn parse_bool(field: &str) -> Result<bool> {
    match field.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => bail!("invalid maker/best-price boolean"),
    }
}

fn parse_record(request: &Request, record: &StringRecord) -> Result<Parsed> {
    let expected_cols = match (request.market, request.dataset) {
        (Market::Spot, Dataset::Trades) => 7,
        (Market::Spot, Dataset::AggTrades) => 8,
        (Market::UsdM, Dataset::Trades) => 6,
        (Market::UsdM, Dataset::AggTrades) => 7,
    };
    ensure!(record.len() == expected_cols, "unexpected CSV field count");
    let field = |i: usize| -> Result<&str> {
        Ok(record.get(i).context("CSV field missing")?.trim())
    };
    let id: u64 = field(0)?.parse()?;
    // Nautilus live data uses *Instrument* precision, not the ZIP's padded
    // decimal display width. Conversion must be exact: extra non-zero digits
    // beyond this precision are an error, never silently rounded.
    let price_decimal = rust_decimal::Decimal::from_str(field(1)?)?;
    let size_decimal = rust_decimal::Decimal::from_str(field(2)?)?;
    // Upstream from_decimal_dp can round values. An archive import must
    // preserve the exact economics of every exchange fill instead.
    ensure!(
        price_decimal.normalize().scale() <= u32::from(request.price_precision),
        "trade price cannot be represented exactly at Instrument precision"
    );
    ensure!(
        size_decimal.normalize().scale() <= u32::from(request.size_precision),
        "trade quantity cannot be represented exactly at Instrument precision"
    );
    let price = Price::from_decimal_dp(price_decimal, request.price_precision)
        .map_err(anyhow::Error::msg)?;
    let size = Quantity::from_decimal_dp(size_decimal, request.size_precision)
        .map_err(anyhow::Error::msg)?;
    ensure!(price.as_f64() > 0.0 && !size.is_zero(), "non-positive price or size");

    let (first_raw_id, last_raw_id, raw_time, maker) = match request.dataset {
        Dataset::Trades => {
            // quoteQty is part of the official raw trade schema; validate it,
            // but preserve the exact native Price/Quantity for Tick storage.
            let quote = field(3)?.parse::<rust_decimal::Decimal>()?;
            ensure!(!quote.is_sign_negative(), "negative quote quantity");
            let timestamp = field(4)?.parse::<u64>()?;
            let maker = parse_bool(field(5)?)?;
            if request.market == Market::Spot {
                parse_bool(field(6)?)?;
            }
            (id, id, timestamp, maker)
        }
        Dataset::AggTrades => {
            let first_raw_id = field(3)?.parse::<u64>()?;
            let last_raw_id = field(4)?.parse::<u64>()?;
            ensure!(first_raw_id <= last_raw_id, "inverted aggregate raw trade IDs");
            let timestamp = field(5)?.parse::<u64>()?;
            let maker = parse_bool(field(6)?)?;
            if request.market == Market::Spot {
                parse_bool(field(7)?)?;
            }
            (first_raw_id, last_raw_id, timestamp, maker)
        }
    };

    let ts_ms = if request.spot_micros() {
        raw_time / 1_000
    } else {
        raw_time
    };
    let (start_ms, end_ms) = request.interval_ms()?;
    ensure!(start_ms <= ts_ms && ts_ms < end_ms, "trade timestamp outside UTC date");
    let multiplier = if request.spot_micros() { 1_000 } else { 1_000_000 };
    let ts_ns = raw_time.checked_mul(multiplier).context("timestamp nanosecond overflow")?;
    let aggressor = if maker { AggressorSide::Sell } else { AggressorSide::Buy };
    let tick = TradeTick::new_checked(
        request.instrument_id,
        price,
        size,
        aggressor,
        TradeId::new_checked(id.to_string())?,
        UnixNanos::new(ts_ns),
        UnixNanos::new(ts_ns),
    )?;
    Ok(Parsed { tick, id, first_raw_id, last_raw_id })
}

fn is_header(record: &StringRecord) -> bool {
    record.get(0).is_some_and(|s| {
        matches!(
            s.trim().to_ascii_lowercase().replace(['_', ' '], "").as_str(),
            "id" | "tradeid" | "aggtradeid" | "aggregatetradeid"
        )
    })
}

/// Calls consume only after validating each row. Use a no-op in the first
/// complete pass, then write to the real Nautilus Catalog on the second pass.
fn scan<F>(path: &Path, request: &Request, mut consume: F) -> Result<Stats>
where
    F: FnMut(TradeTick) -> Result<()>,
{
    let mut archive = zip::ZipArchive::new(File::open(path)?)?;
    ensure!(archive.len() == 1, "ZIP must have exactly one member");
    let csv = archive.by_index(0)?;
    ensure!(
        !csv.is_dir()
            && csv.name() == request.csv_name()
            && csv.size() > 0
            && csv.size() <= MAX_CSV_BYTES,
        "ZIP CSV member identity or size invalid"
    );
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(false)
        .from_reader(csv.take(MAX_CSV_BYTES + 1));
    let mut stats = Stats::default();
    let mut row_digest = Sha256::new();
    for (index, record) in reader.records().enumerate() {
        let record = record.with_context(|| format!("invalid CSV row {}", index + 1))?;
        if index == 0 && is_header(&record) {
            continue;
        }
        // Hash all original cell bytes to detect a source change between
        // the complete validation pass and the subsequent Catalog write.
        // Length-prefix cells so distinct column boundaries never collide.
        for cell in &record {
            row_digest.update((cell.len() as u64).to_le_bytes());
            row_digest.update(cell.as_bytes());
        }
        let row = parse_record(request, &record)
            .with_context(|| format!("malformed exchange CSV row {}", index + 1))?;
        if let Some(previous) = stats.last_id {
            ensure!(
                previous.checked_add(1) == Some(row.id),
                "non-contiguous or duplicate trade/aggregate ID after {previous}"
            );
        }
        if let Some(previous) = stats.last_raw_id {
            ensure!(
                previous.checked_add(1) == Some(row.first_raw_id),
                "non-contiguous underlying trade ID after {previous}"
            );
        }
        let ns = row.tick.ts_init.as_u64();
        if let Some(previous) = stats.last_ts {
            ensure!(ns >= previous, "non-monotonic timestamps");
        }
        if let Some(precision) = stats.price_precision {
            ensure!(precision == row.tick.price.precision, "price precision changed within archive");
        }
        if let Some(precision) = stats.size_precision {
            ensure!(precision == row.tick.size.precision, "quantity precision changed within archive");
        }
        stats.count = stats.count.checked_add(1).context("row counter overflow")?;
        ensure!(stats.count <= MAX_ROWS, "too many rows");
        stats.first_id.get_or_insert(row.id);
        stats.first_ts.get_or_insert(ns);
        stats.price_precision.get_or_insert(row.tick.price.precision);
        stats.size_precision.get_or_insert(row.tick.size.precision);
        stats.last_id = Some(row.id);
        stats.last_ts = Some(ns);
        stats.last_raw_id = Some(row.last_raw_id);
        consume(row.tick)?;
    }
    ensure!(stats.count > 0, "empty daily CSV");
    stats.csv_row_digest = format!("{:x}", row_digest.finalize());
    Ok(stats)
}

fn import(request: &Request, archive: &Path, catalog_root: &Path) -> Result<serde_json::Value> {
    ensure!(catalog_root.is_dir(), "native Catalog root must already exist");
    // Held across validation and every native Catalog write, including failed
    // partial writes. No duplicate custom writer or Arrow schema.
    let _catalog_writer = seealpha_nautilus_binance_data::reserve_catalog_writer(catalog_root)?;
    verify_zip_hash(archive, &request.hash)?;
    // The first complete scan catches a late corrupt row before any new
    // Catalog files are committed. The second scan re-validates all inputs.
    let validated = scan(archive, request, |_| Ok(()))?;
    verify_zip_hash(archive, &request.hash)?;

    let catalog = ParquetDataCatalog::from_uri(
        &catalog_root.canonicalize()?.to_string_lossy(),
        None, None, None, None,
    )?;

    // Nautilus's checked writer deliberately treats an identical file name
    // as an idempotent skip, not an error. Fail closed on existing interval
    // overlap to prevent a repeated or partially recovered ZIP from being
    // falsely reported as newly imported.
    let existing = catalog.query_files(
        &nautilus_model::data::NautilusDataType::TradeTick.into(),
        Some(vec![request.instrument_id.to_string()]),
        validated.first_ts.map(UnixNanos::new),
        validated.last_ts.map(UnixNanos::new),
    )?;
    ensure!(
        existing.is_empty(),
        "historical Tick interval overlaps existing Nautilus Catalog files: {existing:?}"
    );

    let mut batch: Vec<TradeTick> = Vec::with_capacity(BATCH_SIZE);
    let mut file_paths = Vec::new();
    let written = scan(archive, request, |tick| {
        if batch.len() >= BATCH_SIZE
            && batch.last().is_some_and(|last| last.ts_init != tick.ts_init)
        {
            let path = catalog.write_to_parquet(&batch, None, None, None)
                .context("native Nautilus Catalog write rejected this batch")?;
            file_paths.push(path.display().to_string());
            batch.clear();
        }
        ensure!(
            batch.len() < BATCH_SAME_TIMESTAMP_LIMIT,
            "too many same-timestamp trades to form bounded Catalog batch"
        );
        batch.push(tick);
        Ok(())
    })?;
    ensure!(written == validated, "archive changed between CSV validation and publication");
    if !batch.is_empty() {
        let path = catalog.write_to_parquet(&batch, None, None, None)
            .context("native Nautilus Catalog write rejected final batch")?;
        file_paths.push(path.display().to_string());
    }
    Ok(serde_json::json!({
        "source": "binance_public_zip",
        "instrument_id": request.instrument_id.to_string(),
        "dataset": if request.dataset == Dataset::Trades {"trades"} else {"aggTrades"},
        "sha256": request.hash,
        "price_precision": request.price_precision,
        "size_precision": request.size_precision,
        "csv_row_digest": validated.csv_row_digest,
        "records": validated.count,
        "first_trade_id": validated.first_id,
        "last_trade_id": validated.last_id,
        "first_timestamp_ns": validated.first_ts,
        "last_timestamp_ns": validated.last_ts,
        "native_catalog_files": file_paths,
        "catalog_committed": true,
        "coverage_complete": false,
        "data_ready": false,
        "allow_new_risk": false,
        "exchange_orders": 0
    }))
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    ensure!(
        args.len() == 10,
        "usage: import_archive <spot|futures_um> <trades|aggTrades> <SYMBOL> <YYYY-MM-DD> <verified-archive.zip> <existing-native-catalog-root> <official-sha256> <instrument-price-precision> <instrument-size-precision>"
    );
    let price_precision = args[8].parse::<u8>()?;
    let size_precision = args[9].parse::<u8>()?;
    let request = Request::parse(
        &args[1], &args[2], &args[3], &args[4], &args[7],
        price_precision, size_precision,
    )?;
    let report = import(&request, Path::new(&args[5]), Path::new(&args[6]))?;
    println!("{report}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn fixture_archive(name: &str, content: &str) -> (std::path::PathBuf, String) {
        let path = std::env::temp_dir().join(format!(
            "seealpha-import-fixture-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let file = File::create(&path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        zip.start_file(name, zip::write::SimpleFileOptions::default()).unwrap();
        zip.write_all(content.as_bytes()).unwrap();
        zip.finish().unwrap();
        let mut hasher = Sha256::new();
        hasher.update(fs::read(&path).unwrap());
        (path, format!("{:x}", hasher.finalize()))
    }


    #[test]
    fn native_precision_uses_instrument_scale_not_padded_csv_width() {
        let req = Request::parse(
            "spot", "trades", "BTCUSDT", "2025-01-01", &"a".repeat(64), 2, 4,
        ).unwrap();
        let row = StringRecord::from(vec![
            "1", "100.00000000", "4.70000000", "470.00000000",
            "1735689600000000", "false", "true"
        ]);
        let parsed = parse_record(&req, &row).unwrap();
        assert_eq!(parsed.tick.price.precision, 2);
        assert_eq!(parsed.tick.size.precision, 4);
        let extra = StringRecord::from(vec![
            "1", "100.00100000", "4.70000000", "470.00470000",
            "1735689600000000", "false", "true"
        ]);
        assert!(parse_record(&req, &extra).is_err());
    }

    #[test]
    fn preserves_spot_microseconds_and_trade_precision() {
        let req = Request::parse("spot", "trades", "ETHUSDT", "2025-01-01", &"a".repeat(64), 8, 8).unwrap();
        let row = StringRecord::from(vec![
            "51175358", "17.80180000", "5.69000000", "101.29224200",
            "1735689600010866", "true", "true"
        ]);
        let parsed = parse_record(&req, &row).unwrap();
        assert_eq!(parsed.tick.ts_event.as_u64(), 1735689600010866000);
        assert_eq!(parsed.tick.trade_id.as_str(), "51175358");
        assert_eq!(parsed.tick.price.to_string(), "17.80180000");
    }

    #[test]
    fn native_granularity_requires_matching_dataset() {
        let hash = "b".repeat(64);
        assert!(Request::parse("spot", "aggTrades", "ETHUSDT", "2025-01-01", &hash, 8, 8).is_err());
        assert!(Request::parse("futures_um", "trades", "BTCUSDT", "2025-01-01", &hash, 8, 8).is_err());
        assert!(Request::parse("spot", "trades", "../BTCUSDT", "2025-01-01", &hash, 8, 8).is_err());
    }

    #[test]
    fn validates_full_aggregate_id_ranges_and_date() {
        let req = Request::parse("futures_um", "aggTrades", "BTCUSDT", "2026-11-20", &"a".repeat(64), 8, 8).unwrap();
        let row = StringRecord::from(vec![
            "26129", "100.00000000", "4.70443515", "27781", "27783",
            "1795132800000", "false"
        ]);
        let row = parse_record(&req, &row).unwrap();
        assert_eq!(row.first_raw_id, 27781);
        assert_eq!(row.last_raw_id, 27783);
        assert_eq!(row.tick.trade_id.as_str(), "26129");
        let invalid = StringRecord::from(vec![
            "26129", "100.00000000", "4.70443515", "27784", "27783",
            "1795132800000", "false"
        ]);
        assert!(parse_record(&req, &invalid).is_err());
    }

    #[test]
    fn rejects_csv_holes_before_catalog_write() {
        let day = "2026-11-20";
        let csv = "1,100.00000000,1.00000000,1,1,1795132800000,false\n\
                   3,100.00000000,1.00000000,3,3,1795132800001,false\n";
        let name = format!("BTCUSDT-aggTrades-{day}.csv");
        let (archive, hash) = fixture_archive(&name, csv);
        let req = Request::parse("futures_um", "aggTrades", "BTCUSDT", day, &hash, 8, 8).unwrap();
        assert!(scan(&archive, &req, |_| Ok(())).is_err());
        verify_zip_hash(&archive, &hash).unwrap();
        fs::remove_file(archive).unwrap();
    }

    #[test]
    fn imports_synthetic_zip_using_native_nautilus_parquet() {
        let day = "2026-11-20";
        let csv = "1,100.00000000,1.00000000,1,1,1795132800000,false\n\
                   2,101.00000000,2.00000000,2,2,1795132800001,true\n";
        let name = format!("BTCUSDT-aggTrades-{day}.csv");
        let (archive, hash) = fixture_archive(&name, csv);
        let catalog = archive.with_extension("catalog");
        fs::create_dir(&catalog).unwrap();
        let req = Request::parse("futures_um", "aggTrades", "BTCUSDT", day, &hash, 8, 8).unwrap();
        let report = import(&req, &archive, &catalog).unwrap();
        assert_eq!(report["records"], 2);
        assert_eq!(report["catalog_committed"], true);
        assert_eq!(report["data_ready"], false);
        assert_eq!(report["native_catalog_files"].as_array().unwrap().len(), 1);
        let second = import(&req, &archive, &catalog);
        assert!(second.is_err(), "overlapping archive must not be silently reimported");
        fs::remove_dir_all(catalog).unwrap();
        fs::remove_file(archive).unwrap();
    }
}

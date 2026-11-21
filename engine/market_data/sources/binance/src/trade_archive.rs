//! Offline, bounded Binance public Trade ZIP to closed OHLCV.
//! Never mutates source journals, Catalog objects or trading state.
use std::fs::File;
use std::io::{Read, Seek};
use std::path::Path;
use rust_decimal::Decimal;
use seealpha_feed::features::{aggregate_trades, AggBar, Trade};

const MAX_ARCHIVE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_CSV_BYTES: u64 = 512 * 1024 * 1024;
const MAX_TRADES: usize = 1_000_000;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TradeProduct {
    Spot,
    FuturesUm,
}

impl TradeProduct {
    pub fn parse(raw: &str) -> Result<Self> {
        match raw {
            "spot" => Ok(Self::Spot),
            "futures_um" => Ok(Self::FuturesUm),
            _ => Err("unsupported trade archive product".into()),
        }
    }
}

fn read_boolean(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err("invalid buyer-maker field".into()),
    }
}

fn parse_csv<R: Read>(reader: R, product: TradeProduct) -> Result<Vec<Trade>> {
    let mut csv = csv::ReaderBuilder::new().has_headers(false).flexible(false).from_reader(reader);
    let mut trades = Vec::new();
    let mut first = true;
    for line in csv.records() {
        let row = line?;
        if row.iter().all(|value| value.trim().is_empty()) {
            continue;
        }
        if first && matches!(row.get(0).map(str::trim), Some("id" | "trade_id" | "trade id")) {
            first = false;
            continue;
        }
        first = false;
        let expected = if product == TradeProduct::Spot { 8 } else { 7 };
        if row.len() != expected {
            return Err("unexpected Binance Trade CSV column count".into());
        }
        let col = |index: usize| -> Result<&str> {
            Ok(row.get(index).ok_or("missing trade column")?.trim())
        };
        let id: i64 = col(0)?.parse()?;
        let price: Decimal = col(1)?.parse()?;
        let quantity: Decimal = col(2)?.parse()?;
        let quote: Decimal = col(3)?.parse()?;
        let timestamp_ms: i64 = col(4)?.parse()?;
        let buyer_maker = read_boolean(col(5)?)?;
        // One record's size and match flags are still checked even if its
        // trade ID is superseded by a later duplicate.
        read_boolean(col(6)?)?;
        if product == TradeProduct::Spot {
            read_boolean(col(7)?)?;
        }
        if id < 0 || timestamp_ms < 0 || price <= Decimal::ZERO
            || quantity <= Decimal::ZERO || quote < Decimal::ZERO
        {
            return Err("invalid Binance Trade row".into());
        }
        if trades.len() >= MAX_TRADES {
            return Err("trade archive row limit exceeded".into());
        }
        trades.push(Trade { id, timestamp_ms, price, quantity, quote, buyer_maker });
    }
    Ok(trades)
}

/// Validates exactly one CSV ZIP member, finite lengths and CSV content
/// before publishing any aggregate in memory. Password-protected archives
/// and unsupported ZIP compression are rejected by the zip reader.
pub fn aggregate_archive(path: &Path, product: TradeProduct, interval_ms: i64) -> Result<Vec<AggBar>> {
    if !matches!(interval_ms, 1_000 | 60_000 | 180_000 | 300_000 | 900_000 | 3_600_000) {
        return Err("unsupported archive aggregation interval".into());
    }
    if !path.is_file() || path.metadata()?.len() > MAX_ARCHIVE_BYTES {
        return Err("archive missing or exceeds size limit".into());
    }
    let file = File::open(path)?;
    aggregate_zip(file, product, interval_ms)
}

fn aggregate_zip<R: Read + Seek>(reader: R, product: TradeProduct, interval_ms: i64) -> Result<Vec<AggBar>> {
    let mut archive = zip::ZipArchive::new(reader)?;
    if archive.len() != 1 {
        return Err("archive must contain exactly one CSV".into());
    }
    let member = archive.by_index(0)?;
    if !member.name().to_ascii_lowercase().ends_with(".csv")
        || member.name().contains('/') || member.name().contains('\\')
        || member.size() > MAX_CSV_BYTES
        || member.is_dir()
    {
        return Err("invalid or oversized archive CSV member".into());
    }
    let limited = member.take(MAX_CSV_BYTES + 1);
    let trades = parse_csv(limited, product)?;
    let bars = aggregate_trades(&trades, interval_ms)?;
    Ok(bars)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_futures_and_spot_without_fabricating_trades() {
        let futures = b"id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch\n1,10,2,20,1000,false,true\n2,11,1,11,2000,true,true\n";
        let parsed = parse_csv(&futures[..], TradeProduct::FuturesUm).unwrap();
        let result = aggregate_trades(&parsed, 60_000).unwrap();
        assert_eq!(result[0].open, Decimal::from(10));
        assert_eq!(result[0].close, Decimal::from(11));
        assert_eq!(result[0].volume, Decimal::from(3));
        let spot = b"1,10,2,20,1000,false,true,true\n";
        assert_eq!(parse_csv(&spot[..], TradeProduct::Spot).unwrap().len(), 1);
    }
    #[test]
    fn rejects_malformed_rows_and_boolean_flags() {
        assert!(parse_csv(&b"1,10,-2,-20,1000,true,true\n"[..], TradeProduct::FuturesUm).is_err());
        assert!(parse_csv(&b"1,10,2,20,1000,sometimes,true\n"[..], TradeProduct::FuturesUm).is_err());
        assert!(parse_csv(&b"1,10,2,20,1000,false\n"[..], TradeProduct::FuturesUm).is_err());
    }
}

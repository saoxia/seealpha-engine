//! Synthetic-only microbenchmark; compare with Python before speed claims.
use seealpha_quality::{Trade, validate_trades};
use std::hint::black_box;
use std::time::Instant;

fn main() {
    let count: usize = std::env::var("ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_000_000);
    let mut trades = Vec::with_capacity(count);
    for i in 0..count {
        let i = i64::try_from(i).expect("row index fits i64");
        trades.push(Trade {
            trade_id: i + 1,
            timestamp: 1_700_000_000_000 + i,
            price: 2000.0,
            quantity: 0.01,
            quote_quantity: 20.0,
            buyer_maker: i % 2 == 0,
        });
    }
    let started = Instant::now();
    let result = validate_trades(black_box(&trades), false).expect("valid synthetic rows");
    let duration = started.elapsed();
    println!(
        "rows={} elapsed_ms={:.3} rows_per_second={:.0}",
        count,
        duration.as_secs_f64() * 1000.0,
        count as f64 / duration.as_secs_f64()
    );
    black_box(result);
}

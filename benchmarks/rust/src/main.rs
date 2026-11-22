//! Same-workload hot-path measurements. Setup is outside the timer.
use rust_decimal::Decimal;
use seealpha_backtest::{HistoryBar, PublishedTarget, run_backtest};
use seealpha_market::{Bar, PublicTrade, aggregate_trades, validate_bars};
use serde_json::json;
use std::hint::black_box;
use std::time::Instant;

fn main() {
    let name = std::env::args().nth(1).unwrap_or_else(|| "aggregate".into());
    let n = std::env::args()
        .nth(2)
        .and_then(|value| value.parse().ok())
        .unwrap_or(200_000);
    let (elapsed_s, result, peak_rss_kib) = match name.as_str() {
        "net" => net(n),
        "aggregate" => aggregate(n),
        "klines" => klines(n),
        "sqlite" => sqlite(n),
        "backtest" => backtest(n),
        other => {
            eprintln!("unknown workload {other}");
            std::process::exit(2);
        }
    };
    println!(
        "{}",
        json!({
            "name": name,
            "n": n,
            "elapsed_s": elapsed_s,
            "peak_rss_kib": peak_rss_kib,
            "result": result,
        })
    );
}

fn net(n: usize) -> (f64, serde_json::Value, u64) {
    let increment = Decimal::new(1, 3);
    let started = Instant::now();
    let mut acc = Decimal::ZERO;
    let mut out = Decimal::ZERO;
    for index in 0..n {
        let qty = Decimal::from((index % 7) as i64) - Decimal::from(3);
        acc += qty;
        let sized = if acc.is_zero() {
            Decimal::ZERO
        } else {
            let steps = (acc.abs() / increment).trunc();
            let magnitude = steps * increment;
            if acc.is_sign_negative() {
                -magnitude
            } else {
                magnitude
            }
        };
        out += sized;
    }
    black_box(&out);
    let elapsed = started.elapsed().as_secs_f64();
    (elapsed, json!(out.to_string()), peak_rss_kib())
}

fn aggregate(n: usize) -> (f64, serde_json::Value, u64) {
    let trades = (0..n)
        .map(|index| PublicTrade {
            id: index as i64 + 1,
            timestamp_ms: 1_700_000_000_000 + index as i64 * 10,
            price: Decimal::from(1000 + (index % 50) as i64),
            quantity: Decimal::new(1, 2),
            buyer_maker: false,
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    let bars = aggregate_trades(black_box(&trades), 60_000).expect("bars");
    let elapsed = started.elapsed().as_secs_f64();
    (elapsed, json!(bars.len()), peak_rss_kib())
}

fn klines(n: usize) -> (f64, serde_json::Value, u64) {
    let rows = (0..n)
        .map(|index| {
            let open_time_ms = index as i64 * 60_000;
            Bar {
                open_time_ms,
                close_time_ms: open_time_ms + 59_999,
                open: Decimal::from(100),
                high: Decimal::from(100),
                low: Decimal::from(100),
                close: Decimal::from(100),
                volume: Decimal::ONE,
            }
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    validate_bars(black_box(&rows), 60_000).expect("klines");
    let elapsed = started.elapsed().as_secs_f64();
    (elapsed, json!(n), peak_rss_kib())
}

fn backtest(n: usize) -> (f64, serde_json::Value, u64) {
    let bars = (0..n)
        .map(|index| HistoryBar {
            open_time_ms: index as i64 * 60_000,
            close: Decimal::from(100 + (index % 20) as i64),
        })
        .collect::<Vec<_>>();
    let targets = (0..n)
        .step_by(50)
        .map(|index| PublishedTarget {
            member_id: "ema".into(),
            bar_index: index,
            quantity: if (index / 50) % 2 == 0 {
                Decimal::ONE
            } else {
                Decimal::ZERO
            },
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    let report = run_backtest(
        black_box(&bars),
        black_box(&targets),
        Decimal::from(1_000_000),
        Decimal::from(10),
    )
    .expect("backtest");
    let elapsed = started.elapsed().as_secs_f64();
    (elapsed, json!(report.bar_count), peak_rss_kib())
}

fn sqlite(n: usize) -> (f64, serde_json::Value, u64) {
    let path = std::env::temp_dir().join(format!("seealpha-bench-{}.sqlite", std::process::id()));
    let connection = rusqlite::Connection::open(&path).expect("sqlite");
    connection.execute_batch("PRAGMA journal_mode=WAL;").ok();
    connection
        .execute_batch(
            "CREATE TABLE order_requests (
                tenant_id TEXT, account_id TEXT, request_id TEXT, body_json TEXT, updated_at INTEGER,
                PRIMARY KEY (tenant_id, account_id, request_id))",
        )
        .expect("schema");
    connection
        .execute(
            "INSERT INTO order_requests VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                "1",
                "a",
                "req",
                r#"{"fingerprint":"abc","response":{"status":"completed"}}"#,
                1
            ],
        )
        .expect("insert");
    let started = Instant::now();
    let mut hits = 0;
    for _ in 0..n {
        let body: String = connection
            .query_row(
                "SELECT body_json FROM order_requests WHERE tenant_id=?1 AND account_id=?2 AND request_id=?3",
                rusqlite::params!["1", "a", "req"],
                |row| row.get(0),
            )
            .expect("read");
        if body.contains("\"fingerprint\":\"abc\"") {
            hits += 1;
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let _ = std::fs::remove_file(&path);
    (elapsed, json!(hits), peak_rss_kib())
}

fn peak_rss_kib() -> u64 {
    let Ok(text) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            return rest
                .split_whitespace()
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
        }
    }
    0
}

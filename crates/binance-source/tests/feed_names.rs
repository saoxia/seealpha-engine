use std::fs;

use seealpha_binance_market_data_source::calc::bar_window;
use seealpha_binance_market_data_source::protocol::{Channel, subscription_names};
use seealpha_binance_market_data_source::store::{
    FundingJournal, PairJournal, WriteReceipt, read_funding,
};
use seealpha_binance_market_data_source::{
    LivePolicy, Product, cold_start_shards, normalize_symbol,
};
use serde_json::json;

fn root(name: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "seealpha-feed-names-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&path).unwrap();
    path
}

fn mark(event: i64, next: i64, rate: &str) -> serde_json::Value {
    json!({"e":"markPriceUpdate","s":"ETHUSDT","E":event,"T":next,"r":rate})
}

#[test]
fn test_funding_store_retains_one_pending_quote_and_immutable_actuals() {
    let path = root("funding");
    let file = path.join("funding.sqlite");
    let mut store = FundingJournal::open(&file, "ETHUSDT").unwrap();
    assert!(store.estimate(&mark(100, 200, "0.0001")).unwrap());
    assert!(store.estimate(&mark(110, 200, "-0.0002")).unwrap());
    assert!(!store.estimate(&mark(100, 200, "0.0001")).unwrap());
    assert!(store.estimate(&mark(110, 200, "0.0004")).is_err());
    assert_eq!(store.current_rate().unwrap().as_deref(), Some("-0.0002"));
    assert_eq!(
        store.confirmed_settlement("ETHUSDT", 90, "0.0001").unwrap(),
        WriteReceipt::Accepted
    );
    assert_eq!(
        store
            .confirmed_settlement("ETHUSDT", 90, "0.000100")
            .unwrap(),
        WriteReceipt::Duplicate
    );
    assert!(store.confirmed_settlement("ETHUSDT", 90, "0.0005").is_err());
    assert!(store.estimate(&mark(210, 300, "0.0003")).unwrap());
    store
        .confirmed_settlement("ETHUSDT", 200, "-0.00025")
        .unwrap();
    drop(store);
    let view = read_funding(&file, "ETHUSDT", 220).unwrap();
    assert_eq!(view.next_funding_ms, Some(300));
    assert!(view.pending);
    assert_eq!(
        view.settlements,
        vec![(200, "-0.00025".to_owned()), (90, "0.0001".to_owned()),]
    );
    let _ = fs::remove_dir_all(path);
}

#[test]
fn test_funding_rejects_invalid_payloads_without_writing_history() {
    let path = root("funding-bad");
    let mut store = FundingJournal::open(&path.join("funding.sqlite"), "ETHUSDT").unwrap();
    for bad in [
        mark(100, 200, "NaN"),
        mark(100, 100, "0.1"),
        json!({"e":"markPriceUpdate","s":"BTCUSDT","E":100,"T":200,"r":"0.1"}),
        json!({"e":"markPriceUpdate","s":"ETHUSDT","E":true,"T":200,"r":"0.1"}),
    ] {
        assert!(store.estimate(&bad).is_err());
    }
    assert_eq!(store.current_rate().unwrap(), None);
    let _ = fs::remove_dir_all(path);
}

#[test]
fn test_collector_rest_reconciles_startup_then_only_new_actual_settlement() {
    let path = root("funding-rest");
    let file = path.join("funding.sqlite");
    let mut store = FundingJournal::open(&file, "ETHUSDT").unwrap();
    let first = store.next_rest_start().unwrap();
    assert_eq!(first, 0);
    store.confirmed_settlement("ETHUSDT", 90, "0.0001").unwrap();
    store.estimate(&mark(100, 200, "0.005")).unwrap();
    let second = store.next_rest_start().unwrap();
    assert_eq!(second, 91);
    store
        .confirmed_settlement("ETHUSDT", 200, "-0.00025")
        .unwrap();
    store.estimate(&mark(210, 300, "0.006")).unwrap();
    drop(store);
    let view = read_funding(&file, "ETHUSDT", 220).unwrap();
    assert_eq!(
        view.settlements
            .iter()
            .map(|(stamp, _)| *stamp)
            .collect::<Vec<_>>(),
        vec![200, 90]
    );
    assert_eq!(view.settlements[0].1, "-0.00025");
    assert_eq!(view.rate.as_deref(), Some("0.006"));
    let _ = fs::remove_dir_all(path);
}

#[test]
fn test_reader_does_not_create_missing_storage() {
    let path = root("absent");
    let file = path.join("absent.sqlite");
    let view = read_funding(&file, "ETHUSDT", 0).unwrap();
    assert!(view.settlements.is_empty());
    assert!(!file.exists());
    let _ = fs::remove_dir_all(path);
}

#[test]
fn test_calendar_windows_and_authoritative_candles() {
    assert_eq!(
        bar_window("1M", 1_709_078_400_000).unwrap(),
        (1_706_745_600_000, 1_709_251_200_000)
    );
    assert_eq!(
        bar_window("1M", 1_798_675_200_000).unwrap(),
        (1_796_083_200_000, 1_798_761_600_000)
    );
    assert_eq!(
        bar_window("1w", 1_793_232_000_000).unwrap(),
        (1_792_972_800_000, 1_793_577_600_000)
    );
    assert_eq!(
        bar_window("3d", 1_793_145_600_000).unwrap(),
        (1_792_972_800_000, 1_793_232_000_000)
    );
    assert_eq!(
        bar_window("3d", 1_793_232_000_000).unwrap(),
        (1_793_232_000_000, 1_793_491_200_000)
    );
}

#[test]
fn test_conflicting_duplicate_ids_in_one_page_never_silently_choose_one() {
    let path = root("page");
    let mut journal = PairJournal::open(&path, "ETHUSDT", "tick").unwrap();
    let now = 1_700_000_002_000;
    let page = [
        json!({"id":1,"time":1_700_000_000_000_i64,"price":"10","qty":"1","isBuyerMaker":false}),
        json!({"id":1,"time":1_700_000_000_000_i64,"price":"11","qty":"1","isBuyerMaker":false}),
    ];
    assert!(journal.ingest_page(&page, now).is_err());
    assert!(journal.conflicted().unwrap());
    assert_eq!(journal.trade_count().unwrap(), 0);
    let _ = fs::remove_dir_all(path);
}

#[test]
fn test_rust_and_python_raw_trade_journal_parity() {
    let path = root("parity");
    let mut journal = PairJournal::open(&path, "ETHUSDT", "tick").unwrap();
    let events = [
        (
            1_700_000_002_000,
            json!({"id":100,"time":1_700_000_000_100_i64,"price":"2000.010","qty":"0.100","isBuyerMaker":false}),
        ),
        (
            1_700_000_002_000,
            json!({"id":101,"time":1_700_000_001_200_i64,"price":"2001.000","qty":"0.200","isBuyerMaker":true}),
        ),
        (
            1_700_000_003_000,
            json!({"id":101,"time":1_700_000_001_200_i64,"price":"2001.000","qty":"0.200","isBuyerMaker":true}),
        ),
        (
            1_700_000_003_000,
            json!({"id":102,"time":1_700_000_002_200_i64,"price":"0","qty":"0.200","isBuyerMaker":false}),
        ),
    ];
    let mut accepted = 0;
    let mut duplicates = 0;
    let mut filtered = 0;
    for (now, row) in events {
        match journal.write_trade(&row, now).unwrap() {
            WriteReceipt::Accepted => accepted += 1,
            WriteReceipt::Duplicate => duplicates += 1,
            WriteReceipt::FilteredZero => filtered += 1,
        }
    }
    assert_eq!((accepted, duplicates, filtered), (2, 1, 1));
    assert!(!journal.conflicted().unwrap());
    assert_eq!(journal.trade_count().unwrap(), 2);
    let _ = fs::remove_dir_all(path);
}

#[test]
fn test_option_model_and_perpetual_identity_are_separate() {
    assert_eq!(
        normalize_symbol("btc-261225-85000-c", Product::Options).as_deref(),
        Some("BTC-261225-85000-C")
    );
    assert_eq!(
        normalize_symbol("BTCUSDT", Product::FuturesUm).as_deref(),
        Some("BTCUSDT")
    );
    assert!(normalize_symbol("ALL", Product::Options).is_none());
}

#[test]
fn test_trades_and_funding_use_two_category_specific_shared_connections() {
    let names = subscription_names(
        &["ETHUSDT".into(), "BTCUSDT".into()],
        Product::FuturesUm,
        Channel::Mark,
    )
    .unwrap();
    assert_eq!(
        names,
        vec![
            "ethusdt@markPrice@1s".to_owned(),
            "btcusdt@markPrice@1s".to_owned()
        ]
    );
    assert!(subscription_names(&["ETHUSDT".into()], Product::Spot, Channel::Mark).is_err());
}

#[test]
fn test_rejects_more_than_bounded_group_without_network() {
    assert!(Product::FuturesUm.socket_capacity() < 1024);
    assert_eq!(Product::Options.socket_capacity(), 64);
}

#[test]
fn test_batch_fans_out_two_pairs_without_opening_per_symbol_sockets() {
    let policies = ["ETHUSDT", "BTCUSDT"].map(|symbol| LivePolicy {
        symbol: symbol.into(),
        base_level: "tick".into(),
        realtime_enabled: true,
    });
    let shards = cold_start_shards(&policies, Product::FuturesUm);
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].len(), 2);
}

//! Standalone experimental Spot and USD-M public source collector.
//!
//! Writes the existing raw-trade/funding SQLite tables, but DOES NOT replace
//! the Python operational collector: exchange clock, REST handover, history
//! repair and native Catalog parity are still pending. Status is fail-closed.
//!
//! Usage:
//!   binance-native-source <spot|futures_um> <manifest.json> <managed-root> [seconds]
//! The managed-root must already exist on the mounted data volume.
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use seealpha_market::clock::ServerClock;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use seealpha_binance_market_data_source::{
    LivePolicy, Product, cold_start_shards, parse_manifest,
    protocol::Channel,
    store::{FundingJournal, PairJournal},
    ws::{self, WireEvent},
};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;
fn stamp_ms() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}
fn atomic_status(path: &Path, value: &Value) -> Result<()> {
    let temporary = path.with_extension("json.tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temporary)?;
        file.write_all(serde_json::to_string(value)?.as_bytes())?;
        file.sync_all()?;
    }
    fs::rename(&temporary, path)?;
    Ok(())
}
#[derive(Default)]
struct Status {
    trade_connected: bool,
    mark_connected: bool,
    last_trade_ms: Option<i64>,
    confirmed_ms: Option<i64>,
    clock_valid_until_ms: i64,
    gap_count: u64,
    last_error: Option<String>,
}
fn write_status(journal: &PairJournal, state: &Status, product: Product) -> Result<()> {
    let now = stamp_ms()?;
    // Trade-ID holes are persisted in collection.sqlite, independently of
    // this process's in-memory reconnect counter. Restart never erases them.
    let id_gaps = journal.unresolved_gap_count()?;
    let unresolved = state.gap_count.saturating_add(id_gaps);
    // The exchange clock may close calculation buckets but cannot certify
    // live feed READY before REST overlap and Catalog replay reconciliation.
    let confirmed = if state.trade_connected && now <= state.clock_valid_until_ms && unresolved == 0
    {
        state.confirmed_ms
    } else {
        None
    };
    atomic_status(
        &journal.status_file(),
        &json!({
            "state":"DEGRADED", "data_ready":false,
            "reason":state.last_error.as_deref().unwrap_or(if unresolved>0 {
                "raw_trade_gap_unverified"
            } else {
                "rust_native_handover_unverified"
            }),
            "checked_ms":now, "valid_until_ms":if confirmed.is_some(){now+10_000}else{now},
            "symbols":{journal.symbol():{
                "last_websocket_ms":state.last_trade_ms,"last_update_ms":state.last_trade_ms
            }},
            "backlog":0,"gap_count":unresolved,
            "last_error":state.last_error,"allow_new_risk":false,
            "scope":"native_public_evidence_no_strategy_readiness",
            "confirmed_ms":confirmed,
            "product":product.as_str()
        }),
    )?;
    Ok(())
}

fn writer_loop(
    product: Product,
    managed_root: PathBuf,
    policies: Vec<LivePolicy>,
    mut receiver: mpsc::Receiver<WireEvent>,
) -> Result<()> {
    let mut journals = BTreeMap::<String, PairJournal>::new();
    let mut funding = BTreeMap::<String, FundingJournal>::new();
    let mut states = BTreeMap::<String, Status>::new();
    let mut last_published = BTreeMap::<String, Instant>::new();
    for policy in &policies {
        if !policy.realtime_enabled {
            continue;
        }
        let root = managed_root.join(&policy.symbol).join(&policy.base_level);
        if !root.starts_with(&managed_root) {
            return Err("invalid managed root".into());
        }
        // Only create dataset children underneath a confirmed existing mount.
        fs::create_dir_all(&root)?;
        let journal = PairJournal::open(&root, &policy.symbol, &policy.base_level)?;
        if product == Product::FuturesUm {
            let path = managed_root.join(&policy.symbol).join("funding.sqlite");
            funding.insert(
                policy.symbol.clone(),
                FundingJournal::open(&path, &policy.symbol)?,
            );
        }
        states.insert(policy.symbol.clone(), Status::default());
        write_status(&journal, &Status::default(), product)?;
        journals.insert(policy.symbol.clone(), journal);
    }
    while let Some(item) = receiver.blocking_recv() {
        let Some(state) = states.get_mut(&item.symbol) else {
            return Err("unconfigured source symbol".into());
        };
        let journal = journals
            .get_mut(&item.symbol)
            .ok_or("missing source writer")?;
        let result: Result<()> = (|| {
            match item.event["kind"].as_str() {
                Some("connected") => {
                    let channel = item.event["channel"].as_str().unwrap_or("");
                    if channel == "Trades" {
                        state.trade_connected = true;
                    }
                    if channel == "Mark" {
                        state.mark_connected = true;
                    }
                    state.last_error = None;
                }
                Some("disconnected") => {
                    let channel = item.event["channel"].as_str().unwrap_or("");
                    if channel == "Trades" {
                        state.trade_connected = false;
                    }
                    if channel == "Mark" {
                        state.mark_connected = false;
                    }
                    state.gap_count = state.gap_count.saturating_add(1);
                    state.confirmed_ms = None;
                    state.clock_valid_until_ms = 0;
                    state.last_error = Some(
                        item.event["error"]
                            .as_str()
                            .unwrap_or("disconnected")
                            .into(),
                    );
                }
                Some("clock") => {
                    let recorded = item.event["checked_ms"]
                        .as_i64()
                        .ok_or("clock stamp missing")?;
                    let confirmed = item.event["confirmed_ms"]
                        .as_i64()
                        .ok_or("confirmed exchange clock missing")?;
                    let received = stamp_ms()?;
                    if confirmed <= 0
                        || recorded <= 0
                        || received.saturating_sub(recorded) > 15_000
                        || confirmed > received.saturating_add(10_000)
                    {
                        return Err("invalid or expired exchange clock evidence".into());
                    }
                    state.confirmed_ms = Some(confirmed);
                    state.clock_valid_until_ms = received + 15_000;
                }
                Some("clock_failed") => {
                    state.confirmed_ms = None;
                    state.clock_valid_until_ms = 0;
                    state.last_error = Some("exchange_clock_unavailable".into());
                }
                Some("trade") => {
                    if !state.trade_connected {
                        return Err("trade before confirmed subscription".into());
                    }
                    let row = &item.event["row"];
                    journal.write_trade(row, stamp_ms()?)?;
                    if let Some(stamp) = row["time"].as_i64() {
                        state.last_trade_ms =
                            Some(state.last_trade_ms.map_or(stamp, |old| old.max(stamp)));
                    }
                }
                Some("mark") => {
                    if product != Product::FuturesUm || !state.mark_connected {
                        return Err("unexpected funding estimate".into());
                    }
                    funding
                        .get_mut(&item.symbol)
                        .ok_or("funding journal unavailable")?
                        .estimate(&item.event["row"])?;
                }
                _ => return Err("unexpected native source event".into()),
            }
            Ok(())
        })();
        if let Err(error) = result {
            state.last_error = Some(error.to_string());
            write_status(journal, state, product)?;
            return Err(error); // Fail closed: no acknowledgement of rejected writes.
        }
        // Durable raw data is committed for each event; status is coalesced to
        // 1 Hz rather than fsyncing a JSON file for every exchange trade.
        let urgent = matches!(
            item.event["kind"].as_str(),
            Some("disconnected") | Some("connected")
        );
        if urgent
            || last_published
                .get(&item.symbol)
                .is_none_or(|when| when.elapsed() >= Duration::from_secs(1))
        {
            write_status(journal, state, product)?;
            last_published.insert(item.symbol.clone(), Instant::now());
        }
    }
    for (symbol, journal) in &journals {
        let state = states.get_mut(symbol).ok_or("missing source status")?;
        state.last_error = Some("rust_native_collector_stopped".into());
        write_status(journal, state, product)?;
    }
    Ok(())
}

async fn execute() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if !(4..=5).contains(&args.len()) {
        return Err("usage: binance-native-source <spot|futures_um> <manifest.json> <managed-root> [seconds]".into());
    }
    let product = Product::parse(&args[1]).ok_or("unsupported Binance product")?;
    if product == Product::Options {
        return Err("options Catalog evidence path not yet implemented; fail closed".into());
    }
    let manifest = PathBuf::from(&args[2]);
    let managed_root = PathBuf::from(&args[3]).canonicalize()?;
    if !managed_root.is_dir() {
        return Err("managed data mount unavailable".into());
    }
    let raw = fs::read(&manifest)?;
    let policies = parse_manifest(&raw, product)?;
    let mut active: Vec<LivePolicy> = policies
        .into_iter()
        .filter(|p| p.realtime_enabled)
        .collect();
    active.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    if active.is_empty() {
        return Err("no enabled source policies".into());
    }
    let (sender, receiver) = mpsc::channel::<WireEvent>(8192);
    let writer = std::thread::spawn(move || writer_loop(product, managed_root, active, receiver));
    // Collection must hold compatible writer leases before opening sockets.
    // The socket task only owns data transport and cannot bypass writer guards.
    let persisted = fs::read(&manifest)?;
    let policies = parse_manifest(&persisted, product)?;
    let mut tasks: Vec<JoinHandle<()>> = Vec::new();
    // Keep exchange-signed endpoint time separate from WebSocket message
    // receive time. The public timer holds no trading credentials.
    let symbols = policies
        .iter()
        .filter(|p| p.realtime_enabled)
        .map(|p| p.symbol.clone())
        .collect::<Vec<_>>();
    let clock_sender = sender.clone();
    tasks.push(tokio::spawn(async move {
        let Ok(client) = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(4))
            .user_agent("seealpha-native-clock/0.1")
            .build()
        else {
            return;
        };
        let mut clock = ServerClock::new();
        loop {
            let verified = clock
                .sync(&client, product.as_str())
                .await
                .and_then(|_| clock.confirmed_ms());
            let checked = stamp_ms().unwrap_or(0);
            for symbol in &symbols {
                let event = match &verified {
                    Ok(ms) => json!({"kind":"clock","confirmed_ms":ms,"checked_ms":checked}),
                    Err(_) => json!({"kind":"clock_failed","checked_ms":checked}),
                };
                if clock_sender
                    .send(WireEvent {
                        symbol: symbol.clone(),
                        event,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }));
    for members in cold_start_shards(&policies, product) {
        let members: Vec<String> = members
            .into_iter()
            .filter(|s| {
                policies
                    .iter()
                    .any(|p| p.symbol == *s && p.realtime_enabled)
            })
            .collect();
        if members.is_empty() {
            continue;
        }
        let channel = Channel::Trades;
        tasks.push(tokio::spawn(ws::run_forever(
            product,
            channel,
            members.clone(),
            sender.clone(),
        )));
        if product == Product::FuturesUm {
            tasks.push(tokio::spawn(ws::run_forever(
                product,
                Channel::Mark,
                members,
                sender.clone(),
            )));
        }
    }
    // No implicit runtime config changes: a supervisor must stop/reconcile
    // and restart after a new manifest generation. Never alter the DataPolicy.
    let initial = fs::read(&manifest)?;
    let timeout_seconds = args.get(4).map(|s| s.parse::<u64>()).transpose()?;
    let started = tokio::time::Instant::now();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if fs::read(&manifest)? != initial {
            for task in &tasks {
                task.abort();
            }
            break;
        }
        if timeout_seconds.is_some_and(|duration| started.elapsed().as_secs() >= duration) {
            for task in &tasks {
                task.abort();
            }
            break;
        }
    }
    for task in tasks {
        let _ = task.await;
    }
    drop(sender);
    // Wait until the blocking writer has consumed every accepted transport event.
    let result = tokio::task::spawn_blocking(move || writer.join())
        .await
        .map_err(|e| format!("writer join failure: {e}"))?;
    result.map_err(|_| "writer panicked")??;
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match execute().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Binance native public source failed closed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "seealpha-native-source-offline-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    fn policy() -> Vec<LivePolicy> {
        vec![LivePolicy {
            symbol: "ETHUSDT".into(),
            base_level: "tick".into(),
            realtime_enabled: true,
        }]
    }

    #[test]
    fn simulated_disconnect_and_restart_preserve_id_gap_and_fail_closed() {
        let root = temp_root();
        let (sender, receiver) = mpsc::channel(16);
        let dir = root.clone();
        let worker =
            std::thread::spawn(move || writer_loop(Product::Spot, dir, policy(), receiver));
        sender
            .blocking_send(WireEvent {
                symbol: "ETHUSDT".into(),
                event: json!({"kind":"connected","channel":"Trades"}),
            })
            .unwrap();
        for id in [100, 103] {
            sender
                .blocking_send(WireEvent {
                    symbol: "ETHUSDT".into(),
                    event: json!({"kind":"trade","row":{
                        "id":id,"time":1700000000000_i64+id,
                        "price":"10.0","qty":"0.2","isBuyerMaker":false
                    }}),
                })
                .unwrap();
        }
        sender.blocking_send(WireEvent {
            symbol: "ETHUSDT".into(),
            event: json!({"kind":"disconnected","channel":"Trades","error":"offline_network_cut"})
        }).unwrap();
        drop(sender);
        worker.join().unwrap().unwrap();

        let pair = root.join("ETHUSDT").join("tick");
        let prior: Value =
            serde_json::from_slice(&fs::read(pair.join("status.json")).unwrap()).unwrap();
        assert_eq!(prior["data_ready"], false);
        assert!(prior["gap_count"].as_u64().unwrap() >= 2);
        let (sender, receiver) = mpsc::channel(1);
        drop(sender);
        writer_loop(Product::Spot, root.clone(), policy(), receiver).unwrap();
        let restarted: Value =
            serde_json::from_slice(&fs::read(pair.join("status.json")).unwrap()).unwrap();
        assert_eq!(restarted["data_ready"], false);
        assert!(restarted["gap_count"].as_u64().unwrap() >= 1);
        let journal = PairJournal::open(&pair, "ETHUSDT", "tick").unwrap();
        assert_eq!(journal.unresolved_gap_count().unwrap(), 1);
        drop(journal);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn malformed_unsubscribed_trade_fails_without_journal_insert() {
        let root = temp_root();
        let (sender, receiver) = mpsc::channel(2);
        sender
            .blocking_send(WireEvent {
                symbol: "ETHUSDT".into(),
                event: json!({"kind":"trade","row":{
                    "id":1,"time":1,"price":"10","qty":"1","isBuyerMaker":false
                }}),
            })
            .unwrap();
        drop(sender);
        assert!(writer_loop(Product::Spot, root.clone(), policy(), receiver).is_err());
        let pair = root.join("ETHUSDT").join("tick");
        let state: Value =
            serde_json::from_slice(&fs::read(pair.join("status.json")).unwrap()).unwrap();
        assert_eq!(state["data_ready"], false);
        let journal = PairJournal::open(&pair, "ETHUSDT", "tick").unwrap();
        assert_eq!(journal.trade_count().unwrap(), 0);
        drop(journal);
        fs::remove_dir_all(root).unwrap();
    }
}


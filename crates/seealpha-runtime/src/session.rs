use crate::{Runtime, RuntimeError, map_ledger, map_strategy};
use rust_decimal::Decimal;
use seealpha_ledger::{SessionStart, SessionWrite};
use seealpha_market::{Bar, MarketSnapshot, run_pipeline};
use seealpha_portfolio::{PaperFill, TargetPublication, replay_targets};
use seealpha_strategy::{HostCommand, Publication, StrategyBar, digest_dir, run_host, sha256_hex};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum PackageBody {
    Directory(PathBuf),
    Archive(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct StrategyInput {
    pub member_id: String,
    pub package: PackageBody,
    pub parameters: Value,
}

#[derive(Debug, Clone)]
pub struct PaperRequest {
    pub tenant_id: String,
    pub account_id: String,
    pub snapshot: MarketSnapshot,
    pub strategies: Vec<StrategyInput>,
    pub request_id: Option<String>,
}

impl Runtime {
    pub fn run_paper(&self, request: PaperRequest) -> Result<Value, RuntimeError> {
        self.require_active(&request.tenant_id)?;
        if request.strategies.is_empty() {
            return Err(RuntimeError::Conflict(
                "该租户账户没有已下发的策略。".into(),
            ));
        }
        let pipeline = run_pipeline(&request.snapshot)
            .map_err(|error| RuntimeError::BadRequest(error.to_string()))?;
        let fingerprint = session_fingerprint(&request, &pipeline.klines);
        let request_id = request
            .request_id
            .clone()
            .unwrap_or_else(|| fingerprint.clone());
        let symbol = request.snapshot.symbol.clone();
        let start = self
            .ledger
            .begin_session(
                &request.tenant_id,
                &request.account_id,
                &symbol,
                &request_id,
                &fingerprint,
            )
            .map_err(map_ledger)?;
        match start {
            SessionStart::Replay { report } => mark_replay(&report),
            SessionStart::Unresolved => Err(RuntimeError::Conflict(
                "请求已登记，模拟结果待核实。请勿重复提交。".into(),
            )),
            SessionStart::Rejected { report } => Err(RuntimeError::Conflict(detail_or(
                &report,
                "策略执行已被拒绝。",
            ))),
            SessionStart::Fresh { position } => {
                match self.execute_fresh(
                    &request,
                    &pipeline.klines,
                    &pipeline,
                    position,
                    &request_id,
                    &fingerprint,
                ) {
                    Ok(report) => Ok(report),
                    Err(error) => {
                        let message = error.to_string();
                        let report = json!({ "detail": message }).to_string();
                        let _ = self.ledger.reject_session(&SessionWrite {
                            tenant_id: &request.tenant_id,
                            account_id: &request.account_id,
                            symbol: &request.snapshot.symbol,
                            request_id: &request_id,
                            fingerprint: &fingerprint,
                            report: &report,
                            expected_position: Decimal::ZERO,
                            ending_position: Decimal::ZERO,
                        });
                        Err(error)
                    }
                }
            }
        }
    }

    fn execute_fresh(
        &self,
        request: &PaperRequest,
        klines: &[Bar],
        pipeline: &seealpha_market::PipelineOutput,
        starting_position: Decimal,
        request_id: &str,
        fingerprint: &str,
    ) -> Result<Value, RuntimeError> {
        let mut tagged = Vec::new();
        let mut risk_limit: Option<Decimal> = None;
        let host_bars = klines
            .iter()
            .map(|bar| StrategyBar {
                open_time_ms: bar.open_time_ms,
                open: bar.open.to_string(),
                high: bar.high.to_string(),
                low: bar.low.to_string(),
                close: bar.close.to_string(),
                volume: bar.volume.to_string(),
            })
            .collect::<Vec<_>>();
        for strategy in &request.strategies {
            let temp = match &strategy.package {
                PackageBody::Archive(bytes) => Some(write_temp(bytes)?),
                PackageBody::Directory(_) => None,
            };
            let directory = match &strategy.package {
                PackageBody::Directory(path) => Some(path.as_path()),
                PackageBody::Archive(_) => None,
            };
            let archive = temp.as_deref();
            self.track_start();
            let outcome = run_host(&HostCommand {
                python: &self.python,
                host: &self.host,
                package_dir: directory,
                archive_path: archive,
                parameters: &strategy.parameters,
                symbol: &request.snapshot.symbol,
                interval: &request.snapshot.interval,
                bars: &host_bars,
                op: "run",
            });
            self.track_end();
            if let Some(path) = &temp {
                let _ = std::fs::remove_file(path);
            }
            let run = outcome.map_err(map_strategy)?;
            let limit = decimal_field(&run.prepared, "risk_limit")?;
            risk_limit = Some(match risk_limit {
                Some(current) => current.min(limit),
                None => limit,
            });
            for publication in run.publications {
                tagged.push((strategy.member_id.clone(), publication));
            }
        }
        let risk_limit =
            risk_limit.ok_or_else(|| RuntimeError::Conflict("策略缺少风险额度。".into()))?;
        tagged.sort_by(|left, right| {
            left.1
                .bar_index
                .cmp(&right.1.bar_index)
                .then(left.0.cmp(&right.0))
        });
        let mut publications = Vec::with_capacity(tagged.len());
        let mut views = Vec::with_capacity(tagged.len());
        for (member, publication) in &tagged {
            let price = klines
                .get(publication.bar_index)
                .ok_or_else(|| RuntimeError::Conflict("策略公布了无效的 K 线位置。".into()))?
                .close;
            let quantity = publication
                .quantity
                .parse::<Decimal>()
                .map_err(|_| RuntimeError::Conflict("目标数量无效。".into()))?;
            publications.push(TargetPublication {
                member_id: member.clone(),
                quantity,
                price,
            });
            views.push(publication_view(publication));
        }
        let (fills, ending) = replay_targets(
            &publications,
            starting_position,
            risk_limit,
            Decimal::new(1, 8),
        )
        .map_err(|_| RuntimeError::Conflict("目标净额无法计算。".into()))?;
        let order_json = fills
            .iter()
            .enumerate()
            .map(|(index, fill)| {
                json!({
                    "client_order_id": format!("{request_id}-{index}"),
                    "symbol": request.snapshot.symbol,
                    "side": fill.side,
                    "quantity": fill.quantity.to_string(),
                    "price": fill.price.to_string(),
                    "status": "FILLED",
                    "venue": "paper",
                    "exchange_orders": 0,
                })
                .to_string()
            })
            .collect::<Vec<_>>();
        let report = json!({
            "replay": false,
            "symbol": request.snapshot.symbol,
            "interval": request.snapshot.interval,
            "product": request.snapshot.product.as_str(),
            "kline_count": pipeline.klines.len(),
            "trade_count": request.snapshot.trades.len(),
            "aggregated_trade_bars": pipeline.trade_bars.len(),
            "feature_count": pipeline.features.len(),
            "depth_bid_levels": request.snapshot.depth.bids.len(),
            "depth_ask_levels": request.snapshot.depth.asks.len(),
            "publications": views,
            "paper_fills": fills.iter().map(fill_view).collect::<Vec<_>>(),
            "ending_position": ending.to_string(),
            "exchange_orders": 0,
            "venue": "paper",
            "request_id": request_id,
        });
        let report_json = report.to_string();
        self.ledger
            .complete_session(
                &SessionWrite {
                    tenant_id: &request.tenant_id,
                    account_id: &request.account_id,
                    symbol: &request.snapshot.symbol,
                    request_id,
                    fingerprint,
                    report: &report_json,
                    expected_position: starting_position,
                    ending_position: ending,
                },
                &order_json,
            )
            .map_err(map_ledger)?;
        Ok(report)
    }
}

fn write_temp(bytes: &[u8]) -> Result<PathBuf, RuntimeError> {
    let path = std::env::temp_dir().join(format!(
        "seealpha-run-{}-{}.zip",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::write(&path, bytes)
        .map_err(|_| RuntimeError::BadRequest("策略包无法写入。".into()))?;
    Ok(path)
}

fn session_fingerprint(request: &PaperRequest, bars: &[Bar]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(request.snapshot.symbol.as_bytes());
    hasher.update(request.snapshot.interval.as_bytes());
    hasher.update(request.snapshot.product.as_str().as_bytes());
    for strategy in &request.strategies {
        hasher.update(strategy.member_id.as_bytes());
        hasher.update(strategy.parameters.to_string().as_bytes());
        let digest = match &strategy.package {
            PackageBody::Archive(bytes) => sha256_hex(bytes),
            PackageBody::Directory(path) => digest_dir(path).unwrap_or_default(),
        };
        hasher.update(digest.as_bytes());
    }
    for bar in bars {
        hasher.update(bar.open_time_ms.to_le_bytes());
        hasher.update(bar.close.to_string().as_bytes());
    }
    hex_encode(hasher.finalize())
}

fn decimal_field(value: &Value, field: &str) -> Result<Decimal, RuntimeError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| RuntimeError::Conflict("策略缺少风险额度。".into()))?
        .parse()
        .map_err(|_| RuntimeError::Conflict("策略风险额度无效。".into()))
}

fn publication_view(publication: &Publication) -> Value {
    json!({
        "symbol": publication.symbol,
        "quantity": publication.quantity,
        "bar_index": publication.bar_index,
    })
}

fn fill_view(fill: &PaperFill) -> Value {
    json!({
        "side": fill.side,
        "quantity": fill.quantity.to_string(),
        "price": fill.price.to_string(),
    })
}

fn mark_replay(report: &str) -> Result<Value, RuntimeError> {
    let mut value: Value = serde_json::from_str(report)
        .map_err(|_| RuntimeError::BadRequest("模拟记录损坏。".into()))?;
    if let Some(object) = value.as_object_mut() {
        object.insert("replay".into(), Value::Bool(true));
    }
    Ok(value)
}

fn detail_or(report: &str, fallback: &str) -> String {
    serde_json::from_str::<Value>(report)
        .ok()
        .and_then(|value| {
            value
                .get("detail")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| fallback.to_string())
}

fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.as_ref().len() * 2);
    for byte in bytes.as_ref() {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, Runtime};
    use rust_decimal::Decimal;
    use seealpha_market::{DepthBook, MarketSnapshot, Product};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn sample_strategy_publishes_targets_without_exchange_orders() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let state = std::env::temp_dir().join(format!(
            "seealpha-runtime-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let config = Config {
            host: "127.0.0.1".into(),
            port: 0,
            auth_mode: crate::AuthMode::None,
            token: String::new(),
            state_dir: state.clone(),
            project: root.clone(),
            tenant_id: None,
            tenant_purpose: None,
            research_signing_key: None,
            python: PathBuf::from("python3"),
        };
        let runtime = Runtime::open(&config).unwrap();
        runtime
            .ensure_tenant("dev", "development", "active")
            .unwrap();
        let request = PaperRequest {
            tenant_id: "dev".into(),
            account_id: "paper".into(),
            snapshot: snapshot(),
            strategies: vec![StrategyInput {
                member_id: "ema".into(),
                package: PackageBody::Directory(root.join("packages/ema_crossover")),
                parameters: json!({}),
            }],
            request_id: Some("sample-paper".into()),
        };
        let report = runtime.run_paper(request.clone()).unwrap();
        assert_eq!(report["exchange_orders"], 0);
        assert_eq!(report["venue"], "paper");
        assert!(report["kline_count"].as_u64().unwrap() >= 60);
        let publications = report["publications"].as_array().unwrap();
        assert!(publications.iter().any(|item| item["quantity"] == "1"));
        assert!(publications.iter().any(|item| item["quantity"] == "0"));
        let orders = runtime.ledger.order_count("dev", "paper").unwrap();
        assert!(orders >= 1);
        let replay = runtime.run_paper(request).unwrap();
        assert_eq!(replay["replay"], true);
        assert_eq!(runtime.ledger.order_count("dev", "paper").unwrap(), orders);
        let _ = std::fs::remove_dir_all(state);
    }

    fn snapshot() -> MarketSnapshot {
        let mut closes = Vec::new();
        closes.extend(std::iter::repeat_n(100, 30));
        closes.extend(std::iter::repeat_n(200, 30));
        closes.extend(std::iter::repeat_n(50, 30));
        let klines = closes
            .into_iter()
            .enumerate()
            .map(|(index, price)| {
                let open_time_ms = index as i64 * 60_000;
                let px = Decimal::from(price);
                Bar {
                    open_time_ms,
                    close_time_ms: open_time_ms + 59_999,
                    open: px,
                    high: px,
                    low: px,
                    close: px,
                    volume: Decimal::ONE,
                }
            })
            .collect();
        MarketSnapshot {
            product: Product::Spot,
            symbol: "ETHUSDT".into(),
            interval: "1m".to_string(),
            klines,
            trades: Vec::new(),
            depth: DepthBook {
                bids: Vec::new(),
                asks: Vec::new(),
            },
        }
    }
}

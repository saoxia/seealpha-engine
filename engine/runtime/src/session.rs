use crate::{Runtime, RuntimeError, map_ledger, map_strategy};
use rust_decimal::Decimal;
use crate::state_store::{SessionStart, SessionWrite};
use seealpha_market::{Bar, MarketSnapshot, run_pipeline};
use seealpha_portfolio::{HoldingPublication, IndexedHoldingPublication, PaperFill, TargetHolding, replay_holdings_by_bar};
use seealpha_strategy::executor::{ExecutionInput, IssuedNativeEmaExecutor, NativeEmaExecutor, StrategyExecutor};
use seealpha_strategy::{
    AccountScope, IssuedStrategy, Publication, StrategyBar, digest_dir,
    sha256_hex,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub enum PackageBody {
    Directory(PathBuf),
    Archive(Vec<u8>),
    /// Built-in Rust evaluator; no Python interpreter or package files.
    NativeEma,
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
            .state_store
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
                        let _ = self.state_store.reject_session(&SessionWrite {
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
            let run = if matches!(&strategy.package, PackageBody::NativeEma) {
                // Native dispatch is development-only until the issuance and
                // immutable package authorization protocol supports Rust packages.
                let tenant = self.require_active(&request.tenant_id)?;
                if tenant.purpose != "development" {
                    return Err(RuntimeError::Conflict(
                        "正式租户不能直接执行未下发的 Rust 示例策略。".into(),
                    ));
                }
                NativeEmaExecutor
                    .execute(ExecutionInput {
                    parameters: &strategy.parameters,
                    symbol: &request.snapshot.symbol,
                    interval: &request.snapshot.interval,
                    bars: &host_bars,
                })
                    .map_err(map_strategy)?
            } else {
                match &strategy.package {
                    PackageBody::Archive(bytes) => {
                        // The archived artifact is a native JSON manifest, not a
                        // Python ZIP. Require identical bytes and parameters from
                        // trusted account issuance before strategy execution.
                        let load = self.strategy_store.strategies(&request.tenant_id, &request.account_id)
                            .map_err(crate::map_store)?
                            .into_iter()
                            .find(|record| record.issuance_id == strategy.member_id)
                            .ok_or_else(|| RuntimeError::Conflict(
                                "模拟策略没有对应账户的可信下发记录。".into()
                            ))?;
                        let stored_parameters: Value =
                            serde_json::from_str(&load.parameters_json).map_err(|_| {
                                RuntimeError::Conflict("已下发策略参数记录损坏。".into())
                            })?;
                        if load.archive.as_slice() != bytes.as_slice()
                            || stored_parameters != strategy.parameters
                        {
                            return Err(RuntimeError::Conflict(
                                "模拟策略包或参数与可信下发记录不一致。".into()
                            ));
                        }
                        let tenant = self.require_active(&request.tenant_id)?;
                        self.track_start();
                        let outcome = IssuedNativeEmaExecutor {
                            manifest: bytes,
                            issued: IssuedStrategy {
                                tenant_id: &load.tenant_id,
                                account_id: &load.account_id,
                                issuance_id: &load.issuance_id,
                                grant_purpose: &load.grant_purpose,
                                code_key: &load.code_key,
                                version: &load.version,
                                entry_point: &load.entry_point,
                                package_hash: &load.package_hash,
                            },
                            scope: AccountScope {
                                tenant_id: &request.tenant_id,
                                account_id: &request.account_id,
                                purpose: &tenant.purpose,
                            },
                        }.execute(ExecutionInput {
                            parameters: &strategy.parameters,
                            symbol: &request.snapshot.symbol,
                            interval: &request.snapshot.interval,
                            bars: &host_bars,
                        });
                        self.track_end();
                        outcome.map_err(map_strategy)?
                    }
                    PackageBody::Directory(_) => {
                        return Err(RuntimeError::Conflict(
                            "Python 目录策略已退役；请下发受支持的 Rust 策略清单。".into()
                        ));
                    }
                    PackageBody::NativeEma => unreachable!(),
                }
            };
            let limit = decimal_field(&run.prepared, "risk_limit")?;
            let allocation = allocation_field(&run.prepared)?;
            risk_limit = Some(match risk_limit {
                Some(current) => current.min(limit),
                None => limit,
            });
            for publication in run.publications {
                tagged.push((strategy.member_id.clone(), publication, allocation));
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
        for (member, publication, allocation) in &tagged {
            let price = klines
                .get(publication.bar_index)
                .ok_or_else(|| RuntimeError::Conflict("策略公布了无效的 K 线位置。".into()))?
                .close;
            let quantity = if publication.quantity.is_empty() {
                None
            } else {
                Some(publication.quantity.parse::<Decimal>()
                    .map_err(|_| RuntimeError::Conflict("目标数量无效。".into()))?)
            };
            let weight = publication.weight.as_deref().map(|value| {
                value.parse::<Decimal>()
                    .map_err(|_| RuntimeError::Conflict("目标权重无效。".into()))
            }).transpose()?;
            let holding = TargetHolding::new(&request.snapshot.symbol, quantity, weight)
                .map_err(|error| RuntimeError::Conflict(error.to_string()))?;
            publications.push(IndexedHoldingPublication {
                bar_index: publication.bar_index,
                publication: HoldingPublication {
                    member_id: member.clone(),
                    holding,
                    allocation: *allocation,
                    price,
                },
            });
            views.push(publication_view(publication));
        }
        let (fills, ending) = replay_holdings_by_bar(
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
        self.state_store
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
            PackageBody::NativeEma => "native:ema_crossover:v1".to_string(),
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

fn allocation_field(value: &Value) -> Result<Decimal, RuntimeError> {
    let allocation = value
        .get("allocation")
        .and_then(Value::as_str)
        .ok_or_else(|| RuntimeError::Conflict("策略缺少分配数量。".into()))?
        .parse::<Decimal>()
        .map_err(|_| RuntimeError::Conflict("策略分配数量无效。".into()))?;
    if allocation <= Decimal::ZERO {
        return Err(RuntimeError::Conflict("策略分配数量无效。".into()));
    }
    Ok(allocation)
}

fn publication_view(publication: &Publication) -> Value {
    if let Some(weight) = &publication.weight {
        json!({
            "symbol": publication.symbol,
            "weight": weight,
            "bar_index": publication.bar_index,
        })
    } else {
        json!({
            "symbol": publication.symbol,
            "quantity": publication.quantity,
            "bar_index": publication.bar_index,
        })
    }
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
    fn rust_ema_paper_session_requires_no_python_and_is_idempotent() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let state = std::env::temp_dir().join(format!(
            "seealpha-native-paper-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        let config = Config {
            host: "127.0.0.1".into(),
            port: 0,
            auth_mode: crate::AuthMode::None,
            token: String::new(),
            state_dir: state.clone(),
            project: root,
            tenant_id: None,
            tenant_purpose: None,
            research_signing_key: None,
            research_mcp_enabled: false,
            venue_base: None,
            python: PathBuf::from("/nonexistent/python"),
        };
        let runtime = Runtime::open(&config).unwrap();
        runtime.ensure_tenant("dev", "development", "active").unwrap();
        let request = PaperRequest {
            tenant_id: "dev".into(),
            account_id: "native-paper".into(),
            snapshot: snapshot(),
            strategies: vec![StrategyInput {
                member_id: "native-ema".into(),
                package: PackageBody::NativeEma,
                parameters: json!({}),
            }],
            request_id: Some("native-ema-request".into()),
        };
        let report = runtime.run_paper(request.clone()).unwrap();
        assert_eq!(report["exchange_orders"], 0);
        assert_eq!(report["venue"], "paper");
        let pubs = report["publications"].as_array().unwrap();
        assert!(pubs.iter().any(|item| item["quantity"] == "1"));
        assert!(pubs.iter().any(|item| item["quantity"] == "0"));
        let before = runtime.state_store.order_count("dev", "native-paper").unwrap();
        assert!(before > 0);
        assert_eq!(runtime.run_paper(request).unwrap()["replay"], true);
        assert_eq!(runtime.state_store.order_count("dev", "native-paper").unwrap(), before);
        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn native_ema_rejects_production_tenant_without_issuance() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let state = std::env::temp_dir().join(format!(
            "seealpha-native-deny-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        let config = Config {
            host: "127.0.0.1".into(),
            port: 0,
            auth_mode: crate::AuthMode::None,
            token: String::new(),
            state_dir: state.clone(),
            project: root,
            tenant_id: None,
            tenant_purpose: None,
            research_signing_key: None,
            research_mcp_enabled: false,
            venue_base: None,
            python: PathBuf::from("/nonexistent/python"),
        };
        let runtime = Runtime::open(&config).unwrap();
        runtime.ensure_tenant("prod", "production", "active").unwrap();
        let result = runtime.run_paper(PaperRequest {
            tenant_id: "prod".into(),
            account_id: "a".into(),
            snapshot: snapshot(),
            strategies: vec![StrategyInput {
                member_id: "native-ema".into(),
                package: PackageBody::NativeEma,
                parameters: json!({}),
            }],
            request_id: Some("deny-native".into()),
        });
        assert!(result.is_err());
        assert_eq!(runtime.state_store.order_count("prod", "a").unwrap(), 0);
        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn publication_view_preserves_quantity_and_weight_protocols() {
        let quantity = Publication {
            symbol: "ETHUSDT".into(),
            quantity: "1.25".into(),
            weight: None,
            bar_index: 2,
        };
        let weight = Publication {
            symbol: "ETHUSDT".into(),
            quantity: String::new(),
            weight: Some("-0.5".into()),
            bar_index: 2,
        };
        assert_eq!(publication_view(&quantity),
            json!({"symbol":"ETHUSDT","quantity":"1.25","bar_index":2}));
        assert_eq!(publication_view(&weight),
            json!({"symbol":"ETHUSDT","weight":"-0.5","bar_index":2}));
    }

    #[test]
    fn paper_allocation_validation_rejects_zero_missing_and_invalid() {
        assert_eq!(allocation_field(&json!({"allocation":"2.5"})).unwrap(),
            Decimal::new(25, 1));
        for prepared in [
            json!({}),
            json!({"allocation":"0"}),
            json!({"allocation":"-1"}),
            json!({"allocation":"not-a-decimal"}),
        ] {
            assert!(allocation_field(&prepared).is_err());
        }
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

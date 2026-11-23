//! Read-only strategy preview. No custom paper matching, fills, or positions.
use crate::{Runtime, RuntimeError, StrategyInput, map_strategy};
use rust_decimal::Decimal;
use seealpha_execution::{
    MemberTarget, TargetHolding, clip_member_targets, net_signed_quantity,
};
use seealpha_market::{MarketSnapshot, run_pipeline};
use seealpha_strategy::executor::ExecutionInput;
use seealpha_strategy::{AccountScope, Publication, StrategyBar, native_issuance::execute_issued_native};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct PreviewRequest {
    pub tenant_id: String,
    pub account_id: String,
    pub snapshot: MarketSnapshot,
    pub strategies: Vec<StrategyInput>,
    pub request_id: Option<String>,
}

impl Runtime {
    /// Run only account-issued strategies against supplied bars.
    /// Never register a session, infer paper fills, or mutate positions.
    pub fn preview_strategies(&self, request: PreviewRequest) -> Result<Value, RuntimeError> {
        let tenant = self.require_active(&request.tenant_id)?;
        if request.strategies.is_empty() {
            return Err(RuntimeError::Conflict("该租户账户没有已下发的策略。".into()));
        }
        let pipeline = run_pipeline(&request.snapshot)
            .map_err(|error| RuntimeError::BadRequest(error.to_string()))?;
        let bars = pipeline.klines.iter().map(|bar| StrategyBar {
            open_time_ms: bar.open_time_ms,
            open: bar.open.to_string(),
            high: bar.high.to_string(),
            low: bar.low.to_string(),
            close: bar.close.to_string(),
            volume: bar.volume.to_string(),
        }).collect::<Vec<_>>();
        let loads = self.strategy_store
            .strategies(&request.tenant_id, &request.account_id)
            .map_err(crate::map_store)?;

        let mut seen = BTreeSet::new();
        let mut latest = BTreeMap::<String, (usize, Decimal)>::new();
        let mut publications = Vec::new();
        let mut risk_limit: Option<Decimal> = None;

        for strategy in &request.strategies {
            if !seen.insert(strategy.member_id.as_str()) {
                return Err(RuntimeError::Conflict("策略成员重复。".into()));
            }
            let load = loads.iter()
                .find(|entry| entry.issuance_id == strategy.member_id)
                .ok_or_else(|| RuntimeError::Conflict(
                    "预览策略没有对应账户的可信下发记录。".into()
                ))?;
            self.track_start();
            let outcome = execute_issued_native(
                load,
                AccountScope {
                    tenant_id: &request.tenant_id,
                    account_id: &request.account_id,
                    purpose: &tenant.purpose,
                },
                &strategy.archive,
                ExecutionInput {
                    parameters: &strategy.parameters,
                    symbol: &request.snapshot.symbol,
                    interval: &request.snapshot.interval,
                    bars: &bars,
                },
            );
            self.track_end();
            let run = outcome.map_err(map_strategy)?;
            let limit = positive_field(&run.prepared, "risk_limit")?;
            let allocation = positive_field(&run.prepared, "allocation")?;
            risk_limit = Some(risk_limit.map_or(limit, |current| current.min(limit)));

            let mut previous_bar: Option<usize> = None;
            for publication in run.publications {
                if publication.symbol != request.snapshot.symbol
                    || publication.bar_index >= bars.len()
                    || previous_bar.is_some_and(|prior| publication.bar_index < prior)
                {
                    return Err(RuntimeError::Conflict(
                        "策略公布了无效品种或 K 线位置。".into()
                    ));
                }
                previous_bar = Some(publication.bar_index);
                let quantity = if publication.quantity.is_empty() {
                    None
                } else {
                    Some(publication.quantity.parse::<Decimal>()
                        .map_err(|_| RuntimeError::Conflict("目标数量无效。".into()))?)
                };
                let weight = publication.weight.as_deref()
                    .map(|value| value.parse::<Decimal>()
                        .map_err(|_| RuntimeError::Conflict("目标权重无效。".into())))
                    .transpose()?;
                let holding = TargetHolding::new(&request.snapshot.symbol, quantity, weight)
                    .map_err(|error| RuntimeError::Conflict(error.to_string()))?;
                let signed = holding.signed_quantity(allocation)
                    .map_err(|error| RuntimeError::Conflict(error.to_string()))?;
                latest.insert(strategy.member_id.clone(), (publication.bar_index, signed));
                publications.push(publication_view(
                    &strategy.member_id, &publication, signed,
                ));
            }
        }

        // Missing publications are unknown targets, not zero positions.
        let complete = latest.len() == request.strategies.len();
        let members = latest.iter().map(|(member_id, (_, quantity))| MemberTarget {
            member_id: member_id.clone(),
            signed_quantity: *quantity,
            risk_exit: false,
        }).collect::<Vec<_>>();
        let (net_target, unclipped_net_target, targets) = if complete {
            let raw = net_signed_quantity(&members)
                .map_err(|error| RuntimeError::Conflict(error.to_string()))?;
            let clipped = clip_member_targets(&members, risk_limit.expect("nonempty strategies"))
                .map_err(|error| RuntimeError::Conflict(error.to_string()))?;
            let net = net_signed_quantity(&clipped)
                .map_err(|error| RuntimeError::Conflict(error.to_string()))?;
            let targets = clipped.iter().map(|member| {
                json!({
                    "member_id": member.member_id,
                    "bar_index": latest[&member.member_id].0,
                    "target_quantity": member.signed_quantity.to_string(),
                })
            }).collect::<Vec<_>>();
            (json!(net.to_string()), json!(raw.to_string()), targets)
        } else {
            let targets = latest.iter().map(|(member, (bar_index, quantity))| {
                json!({
                    "member_id": member,
                    "bar_index": bar_index,
                    "target_quantity": quantity.to_string(),
                })
            }).collect::<Vec<_>>();
            (Value::Null, Value::Null, targets)
        };

        Ok(json!({
            "preview": true,
            "read_only": true,
            "symbol": request.snapshot.symbol,
            "interval": request.snapshot.interval,
            "product": request.snapshot.product.as_str(),
            "kline_count": pipeline.klines.len(),
            "trade_count": request.snapshot.trades.len(),
            "aggregated_trade_bars": pipeline.trade_bars.len(),
            "feature_count": pipeline.features.len(),
            "depth_bid_levels": request.snapshot.depth.bids.len(),
            "depth_ask_levels": request.snapshot.depth.asks.len(),
            "publications": publications,
            "latest_targets": targets,
            "target_complete": complete,
            "net_target": net_target,
            "unclipped_net_target": unclipped_net_target,
            "risk_limit": risk_limit.expect("nonempty strategies").to_string(),
            "request_id": request.request_id,
            "exchange_orders": 0,
        }))
    }
}

fn positive_field(value: &Value, field: &str) -> Result<Decimal, RuntimeError> {
    value.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| RuntimeError::Conflict(format!("策略缺少有效参数：{field}")))?
        .parse::<Decimal>()
        .ok()
        .filter(|amount| *amount > Decimal::ZERO)
        .ok_or_else(|| RuntimeError::Conflict(format!("策略参数必须为正数：{field}")))
}

fn publication_view(member_id: &str, publication: &Publication, signed: Decimal) -> Value {
    let mut view = json!({
        "member_id": member_id,
        "symbol": publication.symbol,
        "bar_index": publication.bar_index,
        "target_quantity": signed.to_string(),
    });
    if let Some(weight) = &publication.weight {
        view["weight"] = json!(weight);
    } else {
        view["quantity"] = json!(publication.quantity);
    }
    view
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_rejects_missing_zero_or_negative_budget() {
        for value in [json!({}), json!({"allocation":"0"}), json!({"allocation":"-1"}),
            json!({"allocation":"wrong"})] {
            assert!(positive_field(&value, "allocation").is_err());
        }
        assert_eq!(positive_field(&json!({"risk_limit":"2.5"}), "risk_limit").unwrap(),
            Decimal::new(25, 1));
    }

    #[test]
    fn preview_keeps_quantity_and_weight_publications_without_fills() {
        let absolute = Publication {
            symbol: "ETHUSDT".into(),
            quantity: "1.25".into(),
            weight: None,
            bar_index: 2,
        };
        let weighted = Publication {
            symbol: "ETHUSDT".into(),
            quantity: String::new(),
            weight: Some("-0.5".into()),
            bar_index: 2,
        };
        assert_eq!(publication_view("s1", &absolute, Decimal::new(125, 2)),
            json!({"member_id":"s1","symbol":"ETHUSDT","bar_index":2,
                "quantity":"1.25","target_quantity":"1.25"}));
        assert_eq!(publication_view("s2", &weighted, Decimal::new(-2, 0)),
            json!({"member_id":"s2","symbol":"ETHUSDT","bar_index":2,
                "weight":"-0.5","target_quantity":"-2"}));
    }
}

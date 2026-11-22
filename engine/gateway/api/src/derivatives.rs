//! Public derivatives read-side routes. No private credentials and no
//! order submission. Historical funding evidence is returned from Pipeline
//! Catalog, never estimated from ticker or mark-price projections.
use crate::{AppError, AppState};
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::post,
};
use seealpha_market::options::{OptionsSource, parse_option_symbol};
use serde_json::{Value, json};

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/tenants/{tenant_id}/data/funding", post(read_funding))
        .route(
            "/tenants/{tenant_id}/venue/options/contracts",
            post(option_contracts),
        )
        .route(
            "/tenants/{tenant_id}/venue/options/klines",
            post(option_klines),
        )
}
async fn read_funding(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state
        .runtime
        .require_active(&tenant)
        .map_err(AppError::from)?;
    let symbol = body["symbol"]
        .as_str()
        .ok_or_else(|| AppError::bad_request("资金费合约无效。"))?;
    seealpha_market::check_symbol(symbol).map_err(|_| AppError::bad_request("合约无效。"))?;
    let rows = if let (Some(start), Some(end)) = (
        body["start_ms"].as_i64(),
        body["end_ms"].as_i64(),
    ) {
        if start < 0 || end <= start || end - start > 365 * 86_400_000 {
            return Err(AppError::bad_request("资金费查询时间范围无效。"));
        }
        state
            .runtime
            .funding_observations(&tenant, symbol, start, end, 5000)
            .map_err(AppError::from)?
    } else {
        let limit = body["limit"].as_i64().unwrap_or(100);
        if !(1..=500).contains(&limit) {
            return Err(AppError::bad_request("资金费分页数量超出上限。"));
        }
        {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| AppError::bad_request("系统时间不可用。"))?
                .as_millis() as i64;
            let mut observations = state.runtime
                .funding_observations(&tenant, symbol, 0, now_ms, 50_000)
                .map_err(AppError::from)?;
            if observations.len() > limit as usize {
                observations.drain(..observations.len() - limit as usize);
            }
            observations
        }
    };
    Ok(Json(
        json!({"symbol":symbol,"rows":rows,"source":"pipeline_catalog_exchange_events",
        "completeness":"not_certified","no_synthetic_funding":true}),
    ))
}
async fn option_contracts(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
) -> Result<Json<Value>, AppError> {
    state
        .runtime
        .require_active(&tenant)
        .map_err(AppError::from)?;
    let source = OptionsSource::new().map_err(|e| AppError::bad_request(e.to_string()))?;
    let contracts = source
        .contracts()
        .await
        .map_err(|e| AppError::bad_request(e.to_string()))?;
    Ok(Json(json!({"items":contracts,"exchange":"binance",
        "product":"options","scope":"public_exchange_contracts",
        "exchange_orders":0})))
}
async fn option_klines(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state
        .runtime
        .require_active(&tenant)
        .map_err(AppError::from)?;
    let symbol = body["symbol"]
        .as_str()
        .ok_or_else(|| AppError::bad_request("期权合约无效。"))?;
    parse_option_symbol(symbol).map_err(|_| AppError::bad_request("期权合约无效。"))?;
    let interval = body["interval"].as_str().unwrap_or("1m");
    let start = body["start_ms"].as_i64()
        .ok_or_else(|| AppError::bad_request("开始时间无效。"))?;
    let end = body["end_ms"].as_i64()
        .ok_or_else(|| AppError::bad_request("结束时间无效。"))?;
    let source = OptionsSource::new().map_err(|e| AppError::bad_request(e.to_string()))?;
    let bars = source
        .history(symbol, interval, start, end)
        .await
        .map_err(|e| AppError::bad_request(format!("期权历史数据未完成：{e}")))?;
    let rows = bars
        .iter()
        .map(|b| {
            json!({
                "open_time_ms":b.open_time_ms,"close_time_ms":b.close_time_ms,
                "open":b.open.to_string(),"high":b.high.to_string(),
                "low":b.low.to_string(),"close":b.close.to_string(),
                "volume":b.volume.to_string(),"is_closed":true
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(
        json!({"symbol":symbol,"product":"options","interval":interval,
        "bars":rows,"exchange_orders":0,"quality":"validated_window_only"}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_explicit_option_identity_is_accepted() {
        assert!(parse_option_symbol("BTC-271231-90000-P").is_ok());
        assert!(parse_option_symbol("ALL").is_err());
    }
}
